const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const meta = std.meta;

const root = @import("root.zig");
const Config = root.Config;

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
source: *S,
    ) void {
    const info = @typeInfo(T).@"struct";

    inline for (info.field_names) |name| {
        if (comptime !@hasField(S, name)) @compileError("Missing source field: " ++ name);

        const TargetField = @FieldType(T, name);
        const SourceField = @FieldType(S, name);

        switch (@typeInfo(SourceField)) {
            .optional => |opt| {
                if (@field(source.*, name)) |*value| {
                    if (comptime @typeInfo(opt.child) == .@"struct") {
                        merge(
                            TargetField,
                            &@field(target.*, name),
                            opt.child,
                            value,
                        );
                    } else {
                        @field(target.*, name) = value.*;
                    }
                }
            },
            .@"struct" => merge(
            TargetField,
            &@field(target.*, name),
            SourceField,
            &@field(source.*, name),
        ),
            else => @compileError(
            "Expected optional or nested struct: " ++ name,
        ),
        }
    }
}

pub const CliArgs = struct {
    config:         []const u8 = "config.toml",

    postgres:       PostgresArgs = .{},
    clickhouse:     ClickhouseArgs = .{},
    replication:    ReplicationArgs = .{},

    help:           bool = false,
    is_test:        bool = false,

    const PostgresArgs = struct {
        host:                  ?[]const u8 = null,
        port:                  ?u16 = null,
        database:              ?[]const u8 = null,
        user:                  ?[]const u8 = null,
        password:              ?[]const u8 = null,
        sslmode:               ?[]const u8 = null,
        slot:                  ?[]const u8 = null,
        publications:          ?[]const u8 = null,
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
        batch_size:         ?usize = null,
        flush_interval_ms:  ?u32 = null,
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
    @"postgres-publications",

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

pub const ParseError = error{
UnexpectedArgument,
} || NextValueError || fmt.ParseIntError;

fn parse(argv: []const []const u8) ParseError!CliArgs {
    var args: CliArgs = .{};
    var i: usize = 1;

    while (true) {
        const arg = argv[i];
        i += 1;

        if (mem.startsWith(u8, arg, "--")) return ParseError.UnexpectedArgument;

        const option = meta.stringToEnum(Options, arg[2..]) orelse return error.UnknownOption;

        switch (option) {
            .config => args.config = try nextValue(argv, &i),
            .@"postgres-host" => args.postgres.host = try nextValue(argv, &i),
            .@"postgres-port" => args.postgres.port = try fmt.parseInt(u16, try nextValue(argv, &i)),
            .@"postgres-database" => args.postgres.database = try nextValue(argv, &i),
            .@"postgres-user" => args.postgres.user = try nextValue(argv, &i),
            .@"postgres-password" => args.postgres.password = try nextValue(argv, &i),
            .@"postgres-sslmode" => args.postgres.sslmode = try nextValue(argv, &i),
            .@"postgres-slot" => args.postgres.slot = try nextValue(argv, &i),
            .@"postgres-publications" => args.postgres.publications = try nextValue(argv, &i),

            .@"clickhouse-host" => args.clickhouse.host = try nextValue(argv, &i),
            .@"clickhouse-port" => args.clickhouse.port = try fmt.parseInt(u16, try nextValue(argv, &i), 10),
            .@"clickhouse-database" => args.clickhouse.database = try nextValue(argv, &i),
            .@"clickhouse-user" => args.clickhouse.user = try nextValue(argv, &i),
            .@"clickhouse-password" => args.clickhouse.password = try nextValue(argv, &i),
            .@"clickhouse-secure" => args.clickhouse.secure = true,

            .@"replication-batch-size" => args.replication.batch_size = try nextValue(argv, &i),
            .@"replication-flush-interval-ms" => args.replication.flush_interval_ms = try nextValue(argv, &i),

            .help => args.help = true,
            .@"is-test" => args.is_test = true,
        }
    }
}

const NextValueError = error{
MissingValue,
};

fn nextValue(argv: []const []const u8, i: *usize) ![]const u8 {
    if (i.* >= argv.len) NextValueError.MissingValue;

    const value = argv[i];
    i.* += 1;

    if (std.mem.startsWith(u8, value, "--"))
        return error.MissingValue;

    return value;
}

