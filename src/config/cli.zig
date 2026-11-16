const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = mem.Allocator;
const meta = std.meta;
const process = std.process;
const Args = process.Args;

const assert = std.debug.assert;

const root = @import("root.zig");
const Config = root.Config;
const SslMode = root.SslMode;

const ApplyArgsError = MergeError;

pub fn applyArgs(
allocator: Allocator,
target: *Config,
args: CliArgs,
    ) ApplyArgsError!void {
    try merge(allocator, Config, target, CliArgs, args);
}

const MergeError = MergeFieldError;

fn merge(
allocator: Allocator,
comptime T: type,
target: *T,
comptime S: type,
source: S,
    ) MergeError!void {
    const info = @typeInfo(T).@"struct";

    inline for (info.field_names) |name| {
        if (comptime !@hasField(S, name)) @compileError("Missing source field: " ++ name);

        const TargetField = @FieldType(T, name);
        const SourceField = @FieldType(S, name);

        switch (@typeInfo(SourceField)) {
            .optional => |opt| if (@field(source, name)) |value| try mergeField(
                allocator,
                TargetField,
                &@field(target.*, name),
                opt.child,
                value,
            ),
            else => try mergeField(
            allocator,
            TargetField,
            &@field(target.*, name),
            SourceField,
            @field(source, name),
        ),
        }
    }
}

const MergeFieldError = Allocator.Error;

fn mergeField(
allocator: Allocator,
comptime T: type,
target: *T,
comptime S: type,
source: S,
) MergeFieldError!void {
    if (comptime S == []const u8) {
        allocator.free(target.*);
        target.* = source;
    } else switch (comptime @typeInfo(S)) {
        .@"struct" => try merge(
        allocator,
        T,
        target,
        S,
        source,
    ),
        else => target.* = source,
    }
}

const CliArgs = struct {
    config:         ?[]const u8 = null,

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
} || NextValueError || fmt.ParseIntError || root.SslModeError || Allocator.Error;

pub fn parseArgs(allocator: Allocator, argv: Args) ParseArgsError!CliArgs {
    var args: CliArgs = .{};
    var i: usize = 1;

    var it = try argv.iterateAllocator(allocator);
    defer it.deinit();

    if (!it.skip()) return args;

    while (true) {
        const arg = it.next();
        if (arg) |a| {
            i += 1;

            if (!mem.startsWith(u8, a, "--")) return ParseArgsError.UnexpectedArgument;

            const option = meta.stringToEnum(Options, a[2..]) orelse return ParseArgsError.UnknownOption;

            switch (option) {
                .config => args.config = try allocator.dupe(u8, try nextValue(&it)),
                .@"postgres-host" => args.postgres.host = try allocator.dupe(u8, try nextValue(&it)),
                .@"postgres-port" => args.postgres.port = try fmt.parseInt(u16, try nextValue(&it), 10),
                .@"postgres-database" => args.postgres.database = try allocator.dupe(u8, try nextValue(&it)),
                .@"postgres-user" => args.postgres.user = try allocator.dupe(u8, try nextValue(&it)),
                .@"postgres-sslmode" => { 
                    const value = try nextValue(&it);
                    var vit = mem.splitScalar(u8, value, '=');

                    const variant = vit.next();
                    assert(variant != null);

                    const path = if (vit.next()) |p| try allocator.dupe(u8, mem.trim(u8, p, &.{ '"' })) else null;

                    assert(vit.next() == null);

                    args.postgres.sslmode = try SslMode.create(variant.?, path);
                },
                .@"postgres-slot" => args.postgres.slot = try allocator.dupe(u8, try nextValue(&it)),
                .@"postgres-publication" => args.postgres.publication = try allocator.dupe(u8, try nextValue(&it)),

                .@"clickhouse-host" => args.clickhouse.host = try allocator.dupe(u8, try nextValue(&it)),
                .@"clickhouse-port" => args.clickhouse.port = try fmt.parseInt(u16, try nextValue(&it), 10),
                .@"clickhouse-database" => args.clickhouse.database = try allocator.dupe(u8, try nextValue(&it)),
                .@"clickhouse-user" => args.clickhouse.user = try allocator.dupe(u8, try nextValue(&it)),
                .@"clickhouse-secure" => args.clickhouse.secure = true,

                .@"replication-batch-size" => args.replication.batch_size = try fmt.parseInt(u16, try nextValue(&it), 10),
                .@"replication-flush-interval-ms" => args.replication.flush_interval_ms = try fmt.parseInt(u16, try nextValue(&it), 10),

                .help => args.help = true,
                .@"is-test" => args.is_test = true,
            }
        } else break;
    }

    return args;
}

const NextValueError = error{
MissingValue,
};

fn nextValue(it: *Args.Iterator) NextValueError![]const u8 {
    if (it.next()) |val| {
        if (std.mem.startsWith(u8, val, "--")) return NextValueError.MissingValue;

        return val;
    } else return NextValueError.MissingValue;
}

const testing = std.testing;

test "parse: ensure connect mapping" {
    const allocator = testing.allocator;
    {
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
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
            }};

        const args = try parseArgs(allocator, argv);
        defer {
            allocator.free(args.config.?);

            allocator.free(args.postgres.host.?);
            allocator.free(args.postgres.database.?);
            allocator.free(args.postgres.user.?);
            allocator.free(args.postgres.slot.?);
            allocator.free(args.postgres.publication.?);

            allocator.free(args.clickhouse.host.?);
            allocator.free(args.clickhouse.database.?);
            allocator.free(args.clickhouse.user.?);
        }

        try testing.expectEqualSlices(u8, "config.toml", args.config.?);
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
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
            "ergo",

            "--postgres-sslmode", "off",
        }};

        const args = try parseArgs(allocator, argv);

        try testing.expect(args.postgres.sslmode.? == .off);
    }

    {
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
            "ergo",

            "--postgres-sslmode", "verify_full",
        }};

        const args = try parseArgs(allocator, argv);

        try testing.expect(args.postgres.sslmode.? == .verify_full);
        try testing.expectEqual(null, args.postgres.sslmode.?.verify_full);
    }

    {
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
            "ergo",

            "--postgres-sslmode", "verify_full=\"./ca.crt\"",
        }};

        const args = try parseArgs(allocator, argv);
        defer allocator.free(args.postgres.sslmode.?.verify_full.?);

        try testing.expect(args.postgres.sslmode.? == .verify_full);
        try testing.expectEqualSlices(u8, "./ca.crt", args.postgres.sslmode.?.verify_full.?);
    }

    {
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
            "ergo",

            "--postgres-sslmode", "verify_full=./ca.crt",
        }};

        const args = try parseArgs(allocator, argv);
        defer allocator.free(args.postgres.sslmode.?.verify_full.?);

        try testing.expect(args.postgres.sslmode.? == .verify_full);
        try testing.expectEqualSlices(u8, "./ca.crt", args.postgres.sslmode.?.verify_full.?);
    }
}

test "parse: no argv" {
    const allocator = testing.allocator;

    const argv: std.process.Args = .{
        .vector = &[_][*:0]const u8{ 
            "ergo",
        }};

    const args = try parseArgs(allocator, argv);

    try testing.expectEqual(null, args.config);
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
    const allocator = testing.allocator;

    {
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
            "ergo",
            "--postgres-host", "--postgres-port",
        }};

        try testing.expectEqual(ParseArgsError.MissingValue, parseArgs(allocator, argv));
    }

    {
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
            "ergo",
            "--not-valid-arg",
        }};

        try testing.expectEqual(ParseArgsError.UnknownOption, parseArgs(allocator, argv));
    }

    {
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
            "ergo",
            "--is-test", "not-valid",
        }};

        try testing.expectEqual(ParseArgsError.UnexpectedArgument, parseArgs(allocator, argv));
    }

    {
        const argv: std.process.Args = .{
            .vector = &[_][*:0]const u8{ 
            "ergo",
            "--postgres-port", "NOT_VALID"
        }};

        try testing.expectEqual(ParseArgsError.InvalidCharacter, parseArgs(allocator, argv));
    }
}
