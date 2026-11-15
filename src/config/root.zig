const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const Allocator = mem.Allocator;
const Environ = std.process.Environ;

const cli = @import("cli.zig");

const env = @import("env.zig");
const toml = @import("toml.zig");

pub const Config = struct {
    postgres:       PostgresConfig,
    clickhouse:     ClickhouseConfig,
    replication:    ReplicationConfig,

    is_test:        bool,
    
    const PostgresConfig = struct {
        host:           []const u8,
        port:           ?u16,
        database:       []const u8,
        user:           []const u8,
        password:       []const u8,
        // sslmode:        []const u8,
        sslmode:        SslMode,
        slot:           []const u8,
        publication:    []const u8,
    };

    const ClickhouseConfig = struct {
        host:       []const u8,
        port:       u16,
        database:   []const u8,
        user:       []const u8,
        password:   []const u8,
        secure:     bool,
    };

    const ReplicationConfig = struct {
        batch_size:         u16,
        flush_interval_ms:  u16,
    };

};

pub const SslModeError = error{
InvalidValue,
InvalidVariant,
};

pub const SslMode = union(enum) {
    off: void,
    require: void,
    verify_full: ?[]const u8,

    pub fn create(variant_name: []const u8, value: ?[]const u8) SslModeError!@This() {
        const info = @typeInfo(SslMode).@"union";

        inline for (
        info.field_names,
        info.field_types,
    ) |variant, Payload| {
            if (mem.eql(u8, variant, variant_name)) {
                if (Payload == void) {
                    if (value != null) return SslModeError.InvalidValue;

                    return @unionInit(SslMode, variant, {});
                } else {
                    return @unionInit(SslMode, variant, value);
                }
            }
        }
        return SslModeError.InvalidVariant;
    }
};

const LoadError = cli.ParseArgsError || toml.LoadTomlError || env.ApplyEnvError;

pub fn load(allocator: Allocator, io: Io, conf: *Config, argv: []const []const u8, environ_map: *Environ.Map) LoadError!void {
    const args = try cli.parseArgs(argv);

    try toml.loadToml(allocator, io, conf, args.config);

    try env.applyEnv(conf, environ_map);

    cli.applyArgs(conf, args);
}

test "test:main:beforeAll" {
    std.testing.refAllDecls(@This());
}
