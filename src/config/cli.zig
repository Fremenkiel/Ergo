const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const meta = std.meta;

const assert = std.debug.assert;

const root = @import("root.zig");
const Config = root.Config;
const SslMode = root.SslMode;

pub fn applyArgs(
    target: *Config,
    args: CliArgs,
    ) void {
    merge(Config, target, CliArgs, args);
}

fn merge(
comptime T: type,
target: *T,
comptime S: type,
source: S,
    ) void {
    const info = @typeInfo(T).@"struct";

    inline for (info.field_names) |name| {
        if (comptime !@hasField(S, name)) @compileError("Missing source field: " ++ name);

        const TargetField = @FieldType(T, name);
        const SourceField = @FieldType(S, name);

        switch (@typeInfo(SourceField)) {
            .optional => |opt| if (@field(source, name)) |value| mergeField(
                TargetField,
                &@field(target.*, name),
                opt.child,
                value,
            ),
            else => mergeField(
            TargetField,
            &@field(target.*, name),
            SourceField,
            @field(source, name),
        ),
        }
    }
}

fn mergeField(
comptime T: type,
target: *T,
comptime S: type,
source: S,
) void {
    if (comptime @typeInfo(S) == .@"struct") {
        merge(
        T,
        target,
        S,
        source,
    );
    } else {
        target.* = source;
    }
}

const CliArgs = struct {
    config:         []const u8 = "config.toml",

    postgres:       PostgresArgs = .{},
    clickhouse:     ClickhouseArgs = .{},
    replication:    ReplicationArgs = .{},

    help:           bool = false,
    is_test:        bool = false,

    const PostgresArgs = struct {
        host:               ?[]const u8 = null,
        port:               ?u16 = null,
        database:           ?[]const u8 = null,
        user:               ?[]const u8 = null,
        password:           ?[]const u8 = null,
        sslmode:            ?SslMode = null,
        slot:               ?[]const u8 = null,
        publication:        ?[]const u8 = null,
    };

    const ClickhouseArgs = struct {
        host:                ?[]const u8 = null,
        port:                ?u16 = null,
        database:            ?[]const u8 = null,
        user:                ?[]const u8 = null,
        password:            ?[]const u8 = null,
        secure:              ?bool = null,
    };

    const ReplicationArgs = struct {
        batch_size:         ?u16 = null,
        flush_interval_ms:  ?u16 = null,
    };
};

const Options = enum {
    config,

    @"postgres-host",
    @"postgres-port",
    @"postgres-database",
    @"postgres-user",
    @"postgres-sslmode",
    @"postgres-slot",
    @"postgres-publication",

    @"clickhouse-host",
    @"clickhouse-port",
    @"clickhouse-database",
    @"clickhouse-user",
    @"clickhouse-secure",

    @"replication-batch-size",
    @"replication-flush-interval-ms",

    help,
    @"is-test",
};

pub const ParseArgsError = error{
UnexpectedArgument,
UnknownOption,
} || NextValueError || fmt.ParseIntError || root.SslModeError;

pub fn parseArgs(argv: []const []const u8) ParseArgsError!CliArgs {
    var args: CliArgs = .{};
    var i: usize = 1;

    if (argv.len == 1) return args;

    while (true) {
        const arg = argv[i];
        i += 1;

        if (!mem.startsWith(u8, arg, "--")) return ParseArgsError.UnexpectedArgument;

        const option = meta.stringToEnum(Options, arg[2..]) orelse return ParseArgsError.UnknownOption;

        switch (option) {
            .config => args.config = try nextValue(argv, &i),
            .@"postgres-host" => args.postgres.host = try nextValue(argv, &i),
            .@"postgres-port" => args.postgres.port = try fmt.parseInt(u16, try nextValue(argv, &i), 10),
            .@"postgres-database" => args.postgres.database = try nextValue(argv, &i),
            .@"postgres-user" => args.postgres.user = try nextValue(argv, &i),
            .@"postgres-sslmode" => { 
                const value = try nextValue(argv, &i);
                var it = mem.splitScalar(u8, value, '=');

                const variant = it.next();
                assert(variant != null);

                const path = if (it.next()) |p| mem.trim(u8, p, &.{ '"' }) else null;

                assert(it.next() == null);

                args.postgres.sslmode = try SslMode.create(variant.?, path);
            },
            .@"postgres-slot" => args.postgres.slot = try nextValue(argv, &i),
            .@"postgres-publication" => args.postgres.publication = try nextValue(argv, &i),

            .@"clickhouse-host" => args.clickhouse.host = try nextValue(argv, &i),
            .@"clickhouse-port" => args.clickhouse.port = try fmt.parseInt(u16, try nextValue(argv, &i), 10),
            .@"clickhouse-database" => args.clickhouse.database = try nextValue(argv, &i),
            .@"clickhouse-user" => args.clickhouse.user = try nextValue(argv, &i),
            .@"clickhouse-secure" => args.clickhouse.secure = true,

            .@"replication-batch-size" => args.replication.batch_size = try fmt.parseInt(u16, try nextValue(argv, &i), 10),
            .@"replication-flush-interval-ms" => args.replication.flush_interval_ms = try fmt.parseInt(u16, try nextValue(argv, &i), 10),

            .help => args.help = true,
            .@"is-test" => args.is_test = true,
        }

        if (i >= argv.len) break;
    }

    return args;
}

const NextValueError = error{
MissingValue,
};

fn nextValue(argv: []const []const u8, i: *usize) NextValueError![]const u8 {
    if (i.* >= argv.len) return NextValueError.MissingValue;

    const value = argv[i.*];
    if (std.mem.startsWith(u8, value, "--"))
        return NextValueError.MissingValue;

    i.* += 1;

    return value;
}

const testing = std.testing;

test "parse: ensure connect mapping" {
    {
        const argv = &.{
            "ergo",
            "--config", "config.toml",

            "--help",
            "--is-test",

            "--postgres-host", "localhost",
            "--postgres-port", "1234",
            "--postgres-database", "db",
            "--postgres-user", "postgres",
            "--postgres-sslmode", "require",
            "--postgres-slot", "wal_slot",
            "--postgres-publication", "db_pub",

            "--clickhouse-host", "localhost",
            "--clickhouse-port", "1234",
            "--clickhouse-database", "db",
            "--clickhouse-user", "default",
            "--clickhouse-secure",

            "--replication-batch-size", "500",
            "--replication-flush-interval-ms", "360",
        };

        const args = try parseArgs(argv);

        try testing.expectEqualSlices(u8, "config.toml", args.config);
        try testing.expectEqual(true, args.help);
        try testing.expectEqual(true, args.is_test);

        try testing.expectEqualSlices(u8, "localhost", args.postgres.host.?);
        try testing.expectEqual(1234, args.postgres.port.?);
        try testing.expectEqualSlices(u8, "db", args.postgres.database.?);
        try testing.expectEqualSlices(u8, "postgres", args.postgres.user.?);
        try testing.expect(args.postgres.sslmode.? == .require);
        try testing.expectEqualSlices(u8, "wal_slot", args.postgres.slot.?);
        try testing.expectEqualSlices(u8, "db_pub", args.postgres.publication.?);

        try testing.expectEqualSlices(u8, "localhost", args.clickhouse.host.?);
        try testing.expectEqual(1234, args.clickhouse.port.?);
        try testing.expectEqualSlices(u8, "db", args.clickhouse.database.?);
        try testing.expectEqualSlices(u8, "default", args.clickhouse.user.?);
        try testing.expectEqual(true, args.clickhouse.secure.?);

        try testing.expectEqual(500, args.replication.batch_size.?);
        try testing.expectEqual(360, args.replication.flush_interval_ms.?);
    }

    {
        const argv = &.{
            "ergo",

            "--postgres-sslmode", "off",
        };

        const args = try parseArgs(argv);

        try testing.expect(args.postgres.sslmode.? == .off);
    }

    {
        const argv = &.{
            "ergo",

            "--postgres-sslmode", "verify_full",
        };

        const args = try parseArgs(argv);

        try testing.expect(args.postgres.sslmode.? == .verify_full);
        try testing.expectEqual(null, args.postgres.sslmode.?.verify_full);
    }

    {
        const argv = &.{
            "ergo",

            "--postgres-sslmode", "verify_full=\"./ca.crt\"",
        };

        const args = try parseArgs(argv);

        try testing.expect(args.postgres.sslmode.? == .verify_full);
        try testing.expectEqualSlices(u8, "./ca.crt", args.postgres.sslmode.?.verify_full.?);
    }

    {
        const argv = &.{
            "ergo",

            "--postgres-sslmode", "verify_full=./ca.crt",
        };

        const args = try parseArgs(argv);

        try testing.expect(args.postgres.sslmode.? == .verify_full);
        try testing.expectEqualSlices(u8, "./ca.crt", args.postgres.sslmode.?.verify_full.?);
    }
}

test "parse: no argv" {
    const argv = &.{
        "ergo",
    };

    const args = try parseArgs(argv);

    try testing.expectEqualSlices(u8, "config.toml", args.config);
    try testing.expectEqual(false, args.help);
    try testing.expectEqual(false, args.is_test);

    try testing.expectEqual(null, args.postgres.host);
    try testing.expectEqual(null, args.postgres.port);
    try testing.expectEqual(null, args.postgres.database);
    try testing.expectEqual(null, args.postgres.user);
    try testing.expectEqual(null, args.postgres.sslmode);
    try testing.expectEqual(null, args.postgres.slot);
    try testing.expectEqual(null, args.postgres.publication);

    try testing.expectEqual(null, args.clickhouse.host);
    try testing.expectEqual(null, args.clickhouse.port);
    try testing.expectEqual(null, args.clickhouse.database);
    try testing.expectEqual(null, args.clickhouse.user);
    try testing.expectEqual(null, args.clickhouse.secure);

    try testing.expectEqual(null, args.replication.batch_size);
    try testing.expectEqual(null, args.replication.flush_interval_ms);
}

test "parse: invalid types" {
    {
        const argv = &.{
            "ergo",
            "--postgres-host", "--postgres-port",
        };

        try testing.expectEqual(ParseArgsError.MissingValue, parseArgs(argv));
    }

    {
        const argv = &.{
            "ergo",
            "--not-valid-arg",
        };

        try testing.expectEqual(ParseArgsError.UnknownOption, parseArgs(argv));
    }

    {
        const argv = &.{
            "ergo",
            "--is-test", "not-valid",
        };

        try testing.expectEqual(ParseArgsError.UnexpectedArgument, parseArgs(argv));
    }

    {
        const argv = &.{
            "ergo",
            "--postgres-port", "NOT_VALID"
        };

        try testing.expectEqual(ParseArgsError.InvalidCharacter, parseArgs(argv));
    }
}
