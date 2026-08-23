const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const process = std.process;
const debug = std.debug;

const assert = debug.assert;

const pg = @import("pg");
const types = @import("types");
const t = @import("src/t.zig");

const ConnectionManager = @import("src/connection_manager.zig").ConnectionManager;
const PgClient = @import("src/pg_client.zig").PgClient;
const WalStreamT = @import("src/wal_stream.zig").WalStreamT;

const Conn = pg.conn.Conn;
const PgConfig = pg.PgConfig;

var mock_is_shutting_down = std.atomic.Value(bool).init(false);

pub fn runQuery(allocator: mem.Allocator, io: Io, env: std.process.Environ, db_name: []const u8, flag: *std.atomic.Value(bool)) !void {
    var pg_env = try std.process.Environ.createMap(env, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "12345678");

    var pg_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "db_rw",
        "-d", db_name,
        "-a",
        "-f", "./test_fixtures/simple-query.sql"
    };
    const pg_result_1 = try std.process.run(allocator, io, .{ 
        .argv = &pg_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_result_1.stdout);
        allocator.free(pg_result_1.stderr);
    }

    if (pg_result_1.term != .exited or pg_result_1.term.exited != 0 or pg_result_1.stderr.len > 0) {
        std.debug.print("Error: PSQL failed: {s}\n", .{pg_result_1.stderr});
        flag.store(true, .seq_cst);
        return error.PsqlExecutionFailed;
    }

    var docker_kill_argv = [_][]const u8{
        "docker",
        "kill", "db"
    };
    const docker_kill_result = try std.process.run(allocator, io, .{ 
        .argv = &docker_kill_argv,
    });
    defer {
        allocator.free(docker_kill_result.stdout);
        allocator.free(docker_kill_result.stderr);
    }

    if (docker_kill_result.term != .exited or docker_kill_result.term.exited != 0 or docker_kill_result.stderr.len > 0) {
        std.debug.print("Error: Docker failed: {s}\n", .{docker_kill_result.stderr});
        flag.store(true, .seq_cst);
        return error.DockerKillFailed;
    }

    try Io.sleep(io, .{ .nanoseconds = 5 * std.time.ns_per_ms }, .real);

    var docker_start_argv = [_][]const u8{
        "docker",
        "start", "db"
    };
    const docker_start_result = try std.process.run(allocator, io, .{ 
        .argv = &docker_start_argv,
    });
    defer {
        allocator.free(docker_start_result.stdout);
        allocator.free(docker_start_result.stderr);
    }

    if (docker_start_result.term != .exited or docker_start_result.term.exited != 0 or docker_start_result.stderr.len > 0) {
        std.debug.print("Error: Docker failed: {s}\n", .{docker_start_result.stderr});
        flag.store(true, .seq_cst);
        return error.DockerStartFailed;
    }

    const pg_result_2 = try std.process.run(allocator, io, .{ 
        .argv = &pg_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_result_2.stdout);
        allocator.free(pg_result_2.stderr);
    }

    if (pg_result_2.term != .exited or pg_result_2.term.exited != 0 or pg_result_2.stderr.len > 0) {
        std.debug.print("Error: PSQL failed: {s}\n", .{pg_result_2.stderr});
        flag.store(true, .seq_cst);
        return error.PsqlExecutionFailed;
    }

    flag.store(true, .seq_cst);
}

pub fn main(init: process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;
    const env = init.minimal.environ;

    const pg_config: pg.PgConfig = .{
        .port = 5432,
        .host = "localhost",
        .wal = "wal_slot",
        .username = "db_rw",
        .password = "12345678",
        .database = "db",
        .application_name = "Ergo test",
        .timeout_ms = 10_000,
        .startup_parameters = null
    };

    var pg_client = try PgClient.init(allocator, io, pg_config);
    defer pg_client.deinit();
    defer pg_client.cancel();

    var ch_client = t.ChClient.init(allocator);
    defer ch_client.deinit();

    var connection_manager = ConnectionManager.init(allocator, io, pg_config);
    const prober_thread = try std.Thread.spawn(
        .{}, 
        ConnectionManager.runProber, 
        .{ &connection_manager }
    );
    defer prober_thread.detach();

    const TestWalStream = WalStreamT(t.ChClient, PgClient, ConnectionManager);
    var stream = TestWalStream.init(
        allocator,
        io, 
        &ch_client,
        &pg_client, 
        &connection_manager,
        null,
        false,
    );
    defer stream.deinit();

    std.log.info("Connection init", .{});

    try stream.startStreaming();

    mock_is_shutting_down.store(false, .seq_cst);

    const query_thread = try std.Thread.spawn(
        .{}, 
        runQuery, 
        .{ allocator, io, env, "db", &mock_is_shutting_down }
    );
    defer query_thread.detach();

    try stream.stream(&mock_is_shutting_down);
    try stream.endStreaming();

    std.log.debug("written_logs len: {d}", .{ch_client.written_logs.items.len});
    assert(2 == ch_client.written_logs.items.len);
    assert(1 == ch_client.written_logs.items[0].rows.items.len);

    const id_col = ch_client.written_logs.items[0].rows.items[0].columns.items[0];
    const address_line_1_col = ch_client.written_logs.items[0].rows.items[0].columns.items[1];
    const address_line_2_col = ch_client.written_logs.items[0].rows.items[0].columns.items[2];
    const postal_code_col = ch_client.written_logs.items[0].rows.items[0].columns.items[3];
    const city_col = ch_client.written_logs.items[0].rows.items[0].columns.items[4];
    const country_col = ch_client.written_logs.items[0].rows.items[0].columns.items[5];

    assert(mem.eql(u8, "id", id_col.column_name));
    assert(null == id_col.old_value);
    std.log.debug("{s}", .{id_col.new_value.?});
    assert(mem.eql(u8, "1", id_col.new_value.?));

    assert(mem.eql(u8, "address_line_1", address_line_1_col.column_name));
    assert(null == address_line_1_col.old_value);
    assert(mem.eql(u8, "1 Apple Park Way", address_line_1_col.new_value.?));

    assert(mem.eql(u8, "address_line_2", address_line_2_col.column_name));
    assert(null == address_line_2_col.old_value);
    assert(null == address_line_2_col.new_value);

    assert(mem.eql(u8, "postal_code", postal_code_col.column_name));
    assert(null == postal_code_col.old_value);
    assert(mem.eql(u8, "95014", postal_code_col.new_value.?));

    assert(mem.eql(u8, "city", city_col.column_name));
    assert(null == city_col.old_value);
    assert(mem.eql(u8, "Cupertino", city_col.new_value.?));

    assert(mem.eql(u8, "country", country_col.column_name));
    assert(null == country_col.old_value);
    assert(mem.eql(u8, "US", country_col.new_value.?));
}
