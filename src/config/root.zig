const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const Allocator = mem.Allocator;
const Environ = std.process.Environ;

const cli = @import("cli.zig");
const CliArgs = cli.CliArgs;

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
        sslmode:        []const u8,
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
        batch_size:         usize,
        flush_interval_ms:  u32,
    };

};

const LoadError = cli.ParseError || toml.ParseError || env.ApplyEnvError;

pub fn load(allocator: Allocator, io: Io, argv: []const []const u8, environ_map: *Environ.Map) LoadError!Config {
    var conf: Config = .{};

    const args = try cli.parse(argv);

    try toml.loadToml(allocator, io, &conf, args.config);

    try env.applyEnv(&conf, environ_map);

    cli.applyArgs(&conf, args);
}

test "test:main:beforeAll" {
    std.testing.refAllDecls(@This());
}
