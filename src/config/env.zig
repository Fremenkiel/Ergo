const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Environ = std.process.Environ;

const root = @import("root.zig");
const Config = root.Config;
const SslMode = root.SslMode;

pub const ApplyEnvError = error{
InvalidSslMode,
InvalidSslPath,
} || GetEnvError || root.SslModeError;

pub fn applyEnv(
        target: *Config,
        map: *Environ.Map,
    ) ApplyEnvError!void {
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_HOST")) |h| {
        target.postgres.host = h;
    }
    if (try getEnv(u16, map, "ERGO_POSTGRES_PORT")) |p| {
        target.postgres.port = p;
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_DATABASE")) |d| {
        target.postgres.database = d;
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_USER")) |u| {
        target.postgres.user = u;
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_PASSWORD")) |p| {
        target.postgres.password = p;
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_SSLMODE_TYPE")) |t| {
        const path = try getEnv([]const u8, map, "ERGO_POSTGRES_SSLMODE_PATH");

        target.postgres.sslmode = try SslMode.create(t, path);
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_SLOT")) |s| {
        target.postgres.slot = s;
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_PUBLICATION")) |p| {
        target.postgres.publication = p;
    }

    if (try getEnv([]const u8, map, "ERGO_CLICKHOUSE_HOST")) |h| {
        target.clickhouse.host = h;
    }
    if (try getEnv(u16, map, "ERGO_CLICKHOUSE_PORT")) |p| {
        target.clickhouse.port = p;
    }
    if (try getEnv([]const u8, map, "ERGO_CLICKHOUSE_DATABASE")) |d| {
        target.clickhouse.database = d;
    }
    if (try getEnv([]const u8, map, "ERGO_CLICKHOUSE_USER")) |u| {
        target.clickhouse.user = u;
    }
    if (try getEnv([]const u8, map, "ERGO_CLICKHOUSE_PASSWORD")) |p| {
        target.clickhouse.password = p;
    }
    if (try getEnv(bool, map, "ERGO_CLICKHOUSE_SECURE")) |s| {
        target.clickhouse.secure = s;
    }

    if (try getEnv(u16, map, "ERGO_REPLICATION_BATCH_SIZE")) |b| {
        target.replication.batch_size = b;
    }
    if (try getEnv(u16, map, "ERGO_REPLICATION_FLUSH_INTERVAL_MS")) |f| {
        target.replication.flush_interval_ms = f;
    }
}

const GetEnvError = error{
InvalidBoolean,
InvalidNumber,
InvalidType,
};

fn getEnv(
    comptime T: type,
    map: *Environ.Map,
    key: []const u8
    ) GetEnvError!?T {
    const val = map.get(key) orelse return null;

    if (T == []const u8) return val;

    if (T == bool) {
        if (mem.eql(u8, val, "false")) return false;
        if (mem.eql(u8, val, "true")) return true;
        return GetEnvError.InvalidBoolean;
    }

    if (T == u16) {
        return fmt.parseInt(u16, val, 10) catch return GetEnvError.InvalidNumber;
    }

    return GetEnvError.InvalidType;
}

const testing = std.testing;

test "apply env: ensure correct set" {
    const allocator = testing.allocator;
    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_POSTGRES_HOST", "localhost");
        try environ_map.put("ERGO_POSTGRES_PORT", "1234");
        try environ_map.put("ERGO_POSTGRES_DATABASE", "db");
        try environ_map.put("ERGO_POSTGRES_USER", "postgres");
        try environ_map.put("ERGO_POSTGRES_PASSWORD", "postgres");
        try environ_map.put("ERGO_POSTGRES_SSLMODE_TYPE", "require");
        try environ_map.put("ERGO_POSTGRES_SLOT", "wal_slot");
        try environ_map.put("ERGO_POSTGRES_PUBLICATION", "db_pub");

        try environ_map.put("ERGO_CLICKHOUSE_HOST", "localhost");
        try environ_map.put("ERGO_CLICKHOUSE_PORT", "1234");
        try environ_map.put("ERGO_CLICKHOUSE_DATABASE", "db");
        try environ_map.put("ERGO_CLICKHOUSE_USER", "default");
        try environ_map.put("ERGO_CLICKHOUSE_PASSWORD", "clickhouse");
        try environ_map.put("ERGO_CLICKHOUSE_SECURE", "true");

        try environ_map.put("ERGO_REPLICATION_BATCH_SIZE", "500");
        try environ_map.put("ERGO_REPLICATION_FLUSH_INTERVAL_MS", "360");

        const config = try allocator.create(Config);
        defer allocator.destroy(config);

        try applyEnv(config, &environ_map);

        try testing.expectEqualSlices(u8, "localhost", config.postgres.host);
        try testing.expectEqual(1234, config.postgres.port);
        try testing.expectEqualSlices(u8, "db", config.postgres.database);
        try testing.expectEqualSlices(u8, "postgres", config.postgres.user);
        try testing.expectEqualSlices(u8, "postgres", config.postgres.password);
        try testing.expect(config.postgres.sslmode == .require);
        try testing.expectEqualSlices(u8, "wal_slot", config.postgres.slot);
        try testing.expectEqualSlices(u8, "db_pub", config.postgres.publication);

        try testing.expectEqualSlices(u8, "localhost", config.clickhouse.host);
        try testing.expectEqual(1234, config.clickhouse.port);
        try testing.expectEqualSlices(u8, "db", config.clickhouse.database);
        try testing.expectEqualSlices(u8, "default", config.clickhouse.user);
        try testing.expectEqualSlices(u8, "clickhouse", config.clickhouse.password);
        try testing.expectEqual(true, config.clickhouse.secure);

        try testing.expectEqual(500, config.replication.batch_size);
        try testing.expectEqual(360, config.replication.flush_interval_ms);
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_POSTGRES_SSLMODE_TYPE", "off");

        const config = try allocator.create(Config);
        defer allocator.destroy(config);

        try applyEnv(config, &environ_map);

        try testing.expect(config.postgres.sslmode == .off);
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_POSTGRES_SSLMODE_TYPE", "verify_full");

        const config = try allocator.create(Config);
        defer allocator.destroy(config);

        try applyEnv(config, &environ_map);

        try testing.expect(config.postgres.sslmode == .verify_full);
        try testing.expectEqual(null, config.postgres.sslmode.verify_full);
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_POSTGRES_SSLMODE_TYPE", "verify_full");
        try environ_map.put("ERGO_POSTGRES_SSLMODE_PATH", "./ca.crt");

        const config = try allocator.create(Config);
        defer allocator.destroy(config);

        try applyEnv(config, &environ_map);

        try testing.expect(config.postgres.sslmode == .verify_full);
        try testing.expectEqualSlices(u8, "./ca.crt", config.postgres.sslmode.verify_full.?);
    }
}

test "get env: invalid type" {
    const allocator = testing.allocator;

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_POSTGRES_PORT", "NOT_VALID");

        const config = try allocator.create(Config);
        defer allocator.destroy(config);

        try testing.expectEqual(GetEnvError.InvalidNumber, getEnv(u16, &environ_map, "ERGO_POSTGRES_PORT"));
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_CLICKHOUSE_SECURE", "NOT_VALID");

        const config = try allocator.create(Config);
        defer allocator.destroy(config);

        try testing.expectEqual(GetEnvError.InvalidBoolean, getEnv(bool, &environ_map, "ERGO_CLICKHOUSE_SECURE"));
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_INVALID_TYPE", "12.34");

        const config = try allocator.create(Config);
        defer allocator.destroy(config);

        try testing.expectEqual(GetEnvError.InvalidType, getEnv(f16, &environ_map, "ERGO_INVALID_TYPE"));
    }
}
