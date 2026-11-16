const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Allocator = mem.Allocator;
const Environ = std.process.Environ;

const root = @import("root.zig");
const Config = root.Config;
const SslMode = root.SslMode;

pub const ApplyEnvError = error{
InvalidSslMode,
InvalidSslPath,
} || GetEnvError || root.SslModeError || Allocator.Error;

pub fn applyEnv(
allocator: Allocator,
target: *Config,
map: *Environ.Map,
    ) ApplyEnvError!void {
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_HOST")) |h| {
        if (target.postgres.host.len > 0) allocator.free(target.postgres.host);
        target.postgres.host = try allocator.dupe(u8, h);
    }
    if (try getEnv(u16, map, "ERGO_POSTGRES_PORT")) |p| {
        target.postgres.port = p;
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_DATABASE")) |d| {
        if (target.postgres.database.len > 0) allocator.free(target.postgres.database);
        target.postgres.database = try allocator.dupe(u8, d);
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_USER")) |u| {
        if (target.postgres.user.len > 0) allocator.free(target.postgres.user);
        target.postgres.user = try allocator.dupe(u8, u);
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_PASSWORD")) |p| {
        if (target.postgres.password.len > 0) allocator.free(target.postgres.password);
        target.postgres.password = try allocator.dupe(u8, p);
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_SSLMODE_TYPE")) |t| {
        if (target.postgres.sslmode == .verify_full and target.postgres.sslmode.verify_full != null) allocator.free(target.postgres.sslmode.verify_full.?);
        const path = if (try getEnv([]const u8, map, "ERGO_POSTGRES_SSLMODE_PATH")) |path| try allocator.dupe(u8, path) else null;

        target.postgres.sslmode = try SslMode.create(t, path);
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_SLOT")) |s| {
        if (target.postgres.slot.len > 0) allocator.free(target.postgres.slot);
        target.postgres.slot = try allocator.dupe(u8, s);
    }
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_PUBLICATION")) |p| {
        if (target.postgres.publication.len > 0) allocator.free(target.postgres.publication);
        target.postgres.publication = try allocator.dupe(u8, p);
    }

    if (try getEnv([]const u8, map, "ERGO_CLICKHOUSE_HOST")) |h| {
        if (target.clickhouse.host.len > 0) allocator.free(target.clickhouse.host);
        target.clickhouse.host = try allocator.dupe(u8, h);
    }
    if (try getEnv(u16, map, "ERGO_CLICKHOUSE_PORT")) |p| {
        target.clickhouse.port = p;
    }
    if (try getEnv([]const u8, map, "ERGO_CLICKHOUSE_DATABASE")) |d| {
        if (target.clickhouse.database.len > 0) allocator.free(target.clickhouse.database);
        target.clickhouse.database = try allocator.dupe(u8, d);
    }
    if (try getEnv([]const u8, map, "ERGO_CLICKHOUSE_USER")) |u| {
        if (target.clickhouse.user.len > 0) allocator.free(target.clickhouse.user);
        target.clickhouse.user = try allocator.dupe(u8, u);
    }
    if (try getEnv([]const u8, map, "ERGO_CLICKHOUSE_PASSWORD")) |p| {
        if (target.clickhouse.password.len > 0) allocator.free(target.clickhouse.password);
        target.clickhouse.password = try allocator.dupe(u8, p);
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

        const config = try Config.init(allocator);
        defer config.deinit(allocator);

        config.* = .{};

        try applyEnv(allocator, config, &environ_map);

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

        const config = try Config.init(allocator);
        defer config.deinit(allocator);

        config.* = .{};

        try applyEnv(allocator, config, &environ_map);

        try testing.expect(config.postgres.sslmode == .off);
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_POSTGRES_SSLMODE_TYPE", "verify_full");

        const config = try Config.init(allocator);
        defer config.deinit(allocator);

        config.* = .{};

        try applyEnv(allocator, config, &environ_map);

        try testing.expect(config.postgres.sslmode == .verify_full);
        try testing.expectEqual(null, config.postgres.sslmode.verify_full);
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_POSTGRES_SSLMODE_TYPE", "verify_full");
        try environ_map.put("ERGO_POSTGRES_SSLMODE_PATH", "./ca.crt");

        const config = try Config.init(allocator);
        defer config.deinit(allocator);

        config.* = .{};

        try applyEnv(allocator, config, &environ_map);

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

        const config = try Config.init(allocator);
        defer config.deinit(allocator);

        config.* = .{};

        try testing.expectEqual(GetEnvError.InvalidNumber, getEnv(u16, &environ_map, "ERGO_POSTGRES_PORT"));
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_CLICKHOUSE_SECURE", "NOT_VALID");

        const config = try Config.init(allocator);
        defer config.deinit(allocator);

        config.* = .{};

        try testing.expectEqual(GetEnvError.InvalidBoolean, getEnv(bool, &environ_map, "ERGO_CLICKHOUSE_SECURE"));
    }

    {
        var environ_map = try testing.environ.createMap(allocator);
        defer environ_map.deinit();

        try environ_map.put("ERGO_INVALID_TYPE", "12.34");

        const config = try Config.init(allocator);
        defer config.deinit(allocator);

        config.* = .{};

        try testing.expectEqual(GetEnvError.InvalidType, getEnv(f16, &environ_map, "ERGO_INVALID_TYPE"));
    }
}
