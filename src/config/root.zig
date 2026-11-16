const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const Allocator = mem.Allocator;
const meta = std.meta;
const process = std.process;
const Environ = process.Environ;
const Args = process.Args;

const cli = @import("cli.zig");
const env = @import("env.zig");
const toml = @import("toml.zig");
const validator = @import("validator.zig");

pub const Config = struct {
    postgres:       PostgresConfig = .{},
    clickhouse:     ClickhouseConfig = .{},
    replication:    ReplicationConfig = .{},

    is_test:        bool = false,
    
    const PostgresConfig = struct {
        host:           []const u8 = "",
        port:           ?u16 = null,
        database:       []const u8 = "",
        user:           []const u8 = "",
        password:       []const u8 = "",
        sslmode:        SslMode = undefined,
        slot:           []const u8 = "",
        publication:    []const u8 = "",
    };

    const ClickhouseConfig = struct {
        host:       []const u8 = "",
        port:       u16 = undefined,
        database:   []const u8 = "",
        user:       []const u8 = "",
        password:   []const u8 = "",
        secure:     bool = undefined,
    };

    const ReplicationConfig = struct {
        batch_size:         u16 = undefined,
        flush_interval_ms:  u16 = undefined,
    };

    pub fn init(allocator: Allocator) Allocator.Error!*@This() {
        return allocator.create(@This());
    }

    pub fn deinit(self: *@This(), allocator: Allocator) void {
        allocator.free(self.postgres.host);
        allocator.free(self.postgres.database);
        allocator.free(self.postgres.user);
        allocator.free(self.postgres.password);
        if (self.postgres.sslmode == .verify_full and self.postgres.sslmode.verify_full != null) allocator.free(self.postgres.sslmode.verify_full.?);
        allocator.free(self.postgres.slot);
        allocator.free(self.postgres.publication);

        allocator.free(self.clickhouse.host);
        allocator.free(self.clickhouse.database);
        allocator.free(self.clickhouse.user);
        allocator.free(self.clickhouse.password);

        allocator.destroy(self);
    }

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

pub fn load(allocator: Allocator, io: Io, conf: *Config, argv: Args, environ_map: *Environ.Map) LoadError!void {
    const args = try cli.parseArgs(allocator, argv);
    defer if(args.config) |path| allocator.free(path);

    if (args.config) |path| try toml.loadToml(allocator, io, conf, path);

    try env.applyEnv(allocator, conf, environ_map);

    try cli.applyArgs(allocator, conf, args);

    try validator.validate(conf);
}

const testing = std.testing;
test "test:main:beforeAll" {
    testing.refAllDecls(@This());
}

test "load: ensure toml read" {
    const allocator = testing.allocator;
    const io = testing.io;

    const argv: std.process.Args = .{
        .vector = &[_][*:0]const u8{ 
        "ergo",
        "--config", "./test/config.toml",
    }};

    var env_map = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer env_map.deinit();

    const conf = try Config.init(allocator);
    defer conf.deinit(allocator);

    conf.* = .{};

    try load(allocator, io, conf, argv, &env_map);

    try testing.expectEqualSlices(u8, "localhost", conf.postgres.host);
    try testing.expectEqual(5432, conf.postgres.port);
    try testing.expectEqualSlices(u8, "db", conf.postgres.database);
    try testing.expectEqualSlices(u8, "db_rw", conf.postgres.user);
    try testing.expectEqualSlices(u8, "12345678", conf.postgres.password);
    try testing.expect(conf.postgres.sslmode == .verify_full);
    try testing.expectEqualSlices(u8, "./ca.crt", conf.postgres.sslmode.verify_full.?);
    try testing.expectEqualSlices(u8, "wal_slot", conf.postgres.slot);
    try testing.expectEqualSlices(u8, "db_pub", conf.postgres.publication);

    try testing.expectEqualSlices(u8, "localhost", conf.clickhouse.host);
    try testing.expectEqual(8443, conf.clickhouse.port);
    try testing.expectEqualSlices(u8, "db", conf.clickhouse.database);
    try testing.expectEqualSlices(u8, "default", conf.clickhouse.user);
    try testing.expectEqualSlices(u8, "clickhouse", conf.clickhouse.password);
    try testing.expectEqual(true, conf.clickhouse.secure);

    try testing.expectEqual(500, conf.replication.batch_size);
    try testing.expectEqual(360, conf.replication.flush_interval_ms);
}

test "load: ensure env override" {
    const allocator = testing.allocator;
    const io = testing.io;

    const argv: std.process.Args = .{
        .vector = &[_][*:0]const u8{ 
            "ergo"
        }};

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
    file.close(io);

    const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
    defer allocator.free(file_path);

    var env_map = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer env_map.deinit();

    try env_map.put("ERGO_POSTGRES_HOST", "localhost");
    try env_map.put("ERGO_POSTGRES_PORT", "5432");
    try env_map.put("ERGO_POSTGRES_DATABASE", "db");
    try env_map.put("ERGO_POSTGRES_USER", "postgres");
    try env_map.put("ERGO_POSTGRES_PASSWORD", "postgres");
    try env_map.put("ERGO_POSTGRES_SSLMODE_TYPE", "verify_full");
    try env_map.put("ERGO_POSTGRES_SSLMODE_PATH", "./ca.crt");
    try env_map.put("ERGO_POSTGRES_SLOT", "wal_slot");
    try env_map.put("ERGO_POSTGRES_PUBLICATION", "db_pub");

    try env_map.put("ERGO_CLICKHOUSE_HOST", "localhost");
    try env_map.put("ERGO_CLICKHOUSE_PORT", "8443");
    try env_map.put("ERGO_CLICKHOUSE_DATABASE", "db");
    try env_map.put("ERGO_CLICKHOUSE_USER", "default");
    try env_map.put("ERGO_CLICKHOUSE_PASSWORD", "clickhouse");
    try env_map.put("ERGO_CLICKHOUSE_SECURE", "true");

    try env_map.put("ERGO_REPLICATION_BATCH_SIZE", "500");
    try env_map.put("ERGO_REPLICATION_FLUSH_INTERVAL_MS", "360");

    const conf = try Config.init(allocator);
    defer conf.deinit(allocator);

    conf.* = .{};

    try load(allocator, io, conf, argv, &env_map);

    try testing.expectEqualSlices(u8, "localhost", conf.postgres.host);
    try testing.expectEqual(5432, conf.postgres.port);
    try testing.expectEqualSlices(u8, "db", conf.postgres.database);
    try testing.expectEqualSlices(u8, "postgres", conf.postgres.user);
    try testing.expectEqualSlices(u8, "postgres", conf.postgres.password);
    try testing.expect(conf.postgres.sslmode == .verify_full);
    try testing.expectEqualSlices(u8, "./ca.crt", conf.postgres.sslmode.verify_full.?);
    try testing.expectEqualSlices(u8, "wal_slot", conf.postgres.slot);
    try testing.expectEqualSlices(u8, "db_pub", conf.postgres.publication);

    try testing.expectEqualSlices(u8, "localhost", conf.clickhouse.host);
    try testing.expectEqual(8443, conf.clickhouse.port);
    try testing.expectEqualSlices(u8, "db", conf.clickhouse.database);
    try testing.expectEqualSlices(u8, "default", conf.clickhouse.user);
    try testing.expectEqualSlices(u8, "clickhouse", conf.clickhouse.password);
    try testing.expectEqual(true, conf.clickhouse.secure);

    try testing.expectEqual(500, conf.replication.batch_size);
    try testing.expectEqual(360, conf.replication.flush_interval_ms);
}

test "load: ensure argv override" {
    const allocator = testing.allocator;
    const io = testing.io;

    const argv: std.process.Args = .{
        .vector = &[_][*:0]const u8{ 
            "ergo",
            "--config", "config.toml",

            "--help",
            "--is-test",

            "--postgres-host", "localhost",
            "--postgres-port", "5432",
            "--postgres-database", "db",
            "--postgres-user", "postgres",
            "--postgres-sslmode", "require",
            "--postgres-slot", "wal_slot",
            "--postgres-publication", "db_pub",

            "--clickhouse-host", "localhost",
            "--clickhouse-port", "8443",
            "--clickhouse-database", "db",
            "--clickhouse-user", "default",
            "--clickhouse-secure",

            "--replication-batch-size", "500",
            "--replication-flush-interval-ms", "360",
        }};

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
    file.close(io);

    const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
    defer allocator.free(file_path);

    var env_map = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer env_map.deinit();

    const conf = try Config.init(allocator);
    defer conf.deinit(allocator);

    conf.* = .{};

    try load(allocator, io, conf, argv, &env_map);

    try testing.expectEqualSlices(u8, "localhost", conf.postgres.host);
    try testing.expectEqual(5432, conf.postgres.port);
    try testing.expectEqualSlices(u8, "db", conf.postgres.database);
    try testing.expectEqualSlices(u8, "postgres", conf.postgres.user);
    try testing.expect(conf.postgres.sslmode == .require);
    try testing.expectEqualSlices(u8, "wal_slot", conf.postgres.slot);
    try testing.expectEqualSlices(u8, "db_pub", conf.postgres.publication);

    try testing.expectEqualSlices(u8, "localhost", conf.clickhouse.host);
    try testing.expectEqual(8443, conf.clickhouse.port);
    try testing.expectEqualSlices(u8, "db", conf.clickhouse.database);
    try testing.expectEqualSlices(u8, "default", conf.clickhouse.user);
    try testing.expectEqual(true, conf.clickhouse.secure);

    try testing.expectEqual(500, conf.replication.batch_size);
    try testing.expectEqual(360, conf.replication.flush_interval_ms);
}

test "load: ensure correct override order" {
    const allocator = testing.allocator;
    const io = testing.io;

    const argv: std.process.Args = .{
        .vector = &[_][*:0]const u8{ 
            "ergo",
            "--config", "./test/config.toml",

            "--help",
            "--is-test",

            "--postgres-host", "arg",
            "--postgres-port", "5555",
            "--postgres-database", "arg",
            "--postgres-user", "arg",
            "--postgres-sslmode", "require",
            "--postgres-slot", "arg",
            "--postgres-publication", "arg",

            "--clickhouse-host", "arg",
            "--clickhouse-port", "6666",
            "--clickhouse-database", "arg",
            "--clickhouse-user", "arg",
            "--clickhouse-secure",

            "--replication-batch-size", "7777",
            "--replication-flush-interval-ms", "8888",
        }};

    var env_map = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer env_map.deinit();

    try env_map.put("ERGO_POSTGRES_HOST", "env");
    try env_map.put("ERGO_POSTGRES_PORT", "1111");
    try env_map.put("ERGO_POSTGRES_DATABASE", "env");
    try env_map.put("ERGO_POSTGRES_USER", "env");
    try env_map.put("ERGO_POSTGRES_PASSWORD", "env");
    try env_map.put("ERGO_POSTGRES_SSLMODE_TYPE", "off");
    try env_map.put("ERGO_POSTGRES_SLOT", "env");
    try env_map.put("ERGO_POSTGRES_PUBLICATION", "env");

    try env_map.put("ERGO_CLICKHOUSE_HOST", "env_localhost");
    try env_map.put("ERGO_CLICKHOUSE_PORT", "2222");
    try env_map.put("ERGO_CLICKHOUSE_DATABASE", "env");
    try env_map.put("ERGO_CLICKHOUSE_USER", "env");
    try env_map.put("ERGO_CLICKHOUSE_PASSWORD", "env");
    try env_map.put("ERGO_CLICKHOUSE_SECURE", "false");

    try env_map.put("ERGO_REPLICATION_BATCH_SIZE", "3333");
    try env_map.put("ERGO_REPLICATION_FLUSH_INTERVAL_MS", "4444");

    const conf = try Config.init(allocator);
    defer conf.deinit(allocator);

    conf.* = .{};

    try load(allocator, io, conf, argv, &env_map);

    try testing.expectEqualSlices(u8, "arg", conf.postgres.host);
    try testing.expectEqual(5555, conf.postgres.port);
    try testing.expectEqualSlices(u8, "arg", conf.postgres.database);
    try testing.expectEqualSlices(u8, "arg", conf.postgres.user);
    try testing.expect(conf.postgres.sslmode == .require);
    try testing.expectEqualSlices(u8, "arg", conf.postgres.slot);
    try testing.expectEqualSlices(u8, "arg", conf.postgres.publication);

    try testing.expectEqualSlices(u8, "arg", conf.clickhouse.host);
    try testing.expectEqual(6666, conf.clickhouse.port);
    try testing.expectEqualSlices(u8, "arg", conf.clickhouse.database);
    try testing.expectEqualSlices(u8, "arg", conf.clickhouse.user);
    try testing.expectEqual(true, conf.clickhouse.secure);

    try testing.expectEqual(7777, conf.replication.batch_size);
    try testing.expectEqual(8888, conf.replication.flush_interval_ms);

    try testing.expectEqual(true, conf.is_test);
}

test "load: ensure correct validation" {
    const allocator = testing.allocator;
    const io = testing.io;

    const argv: std.process.Args = .{
        .vector = &[_][*:0]const u8{ 
            "ergo",
        }};

    var env_map = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer env_map.deinit();

    const conf = try Config.init(allocator);
    defer conf.deinit(allocator);

    conf.* = .{};

    try load(allocator, io, conf, argv, &env_map);
}
