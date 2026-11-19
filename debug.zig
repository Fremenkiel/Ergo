const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const process = std.process;
const debug = std.debug;

const assert = debug.assert;

const ergo = @import("src/ergo.zig");

const phantom = @import("phantom");
const Phantom = phantom.Phantom;

const TestProcess = @import("src/testing/test_process.zig").TestProcess;

const t = @import("src/t.zig");

const Config = struct {
    host: []const u8 = undefined,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;

    var database = try Phantom.init(allocator, io, "./test/template.db", "/tmp/ergo", .{
        .log_level = .{
            .fatal = .warn,
        },
    });
    defer database.deinit();

    try database.spawn();

    const socket = try allocator.print("{s}/.s.PGSQL.5432", .{ database.socket_path });
    defer allocator.free(socket);

    const db_name = try t.randomDbName(allocator, io);
    defer allocator.free(db_name);

    try t.createTestDb(allocator, io, db_name);
    defer t.teardownTestDb(allocator, io, db_name) catch {};

    var env_map = try init.minimal.environ.createMap(allocator);
    defer env_map.deinit();
    std.log.debug("setup complete", .{});

    var test_process = try TestProcess.spawn(allocator, io, &env_map, .{
        .argv = &[_][]const u8{
            "./zig-out/bin/ergo",
            "--config", "./test/config.toml",
            "--postgres-host", socket,
            "--clickhouse-database", db_name,
            "--is-test"
        },
        .readiness = .pipe,
    });
    errdefer test_process.deinit();
    std.log.debug("spawned", .{});

    try test_process.waitUntilReady(.{ .timeout_ms = 5_000 });
    std.log.debug("ready", .{});

    const term = try test_process.terminate();
    std.log.debug("terminated", .{});

    assert(term == .exited);
    assert(0 == term.exited);

    try env_map.put("PGPASSWORD", "postgres");

    const pg_assert_query = try std.fmt.allocPrint(allocator, "SELECT * FROM pg_stat_activity WHERE datname = '{s}' AND application_name = '{s}';", .{db_name, ergo.application_name});
    defer allocator.free(pg_assert_query);

    var pg_assert_argv = [_][]const u8{ 
        "psql",
        "-h", database.socket_path,
        "-U", "postgres",
        "-d", "db",
        "-q", "-t", "-A", // Formatting
        "-c", pg_assert_query,
    };
    const pg_assert_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_assert_argv,
        .environ_map = &env_map, 
    });
    defer {
        allocator.free(pg_assert_result.stdout);
        allocator.free(pg_assert_result.stderr);
    }
    std.log.debug("executed", .{});

    if (pg_assert_result.term != .exited or pg_assert_result.term.exited != 0 or pg_assert_result.stderr.len > 0) {
        std.debug.print("Error: unable to select pg data: {s}\n", .{pg_assert_result.stderr});
        return error.PgSelectError;
    }

    assert(mem.eql(u8, "", pg_assert_result.stdout));
}
