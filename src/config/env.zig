const std = @import("std");
const fmt = std.fmt;
const mem = std.mem;
const Environ = std.process.Environ;

const root = @import("root.zig");
const Config = root.Config;

pub const ApplyEnvError = GetEnvError;

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
    if (try getEnv([]const u8, map, "ERGO_POSTGRES_SSLMODE")) |s| {
        target.postgres.sslmode = s;
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
