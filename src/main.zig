const std = @import("std");
const builtin = @import("builtin");

const Io = std.Io;
const File = Io.File;
const mem = std.mem;
const Allocator = mem.Allocator;
const process = std.process;
const posix = std.posix;

const assert = std.debug.assert;

const config = @import("config/root.zig");
const Config = config.Config;

const ergo = @import("ergo.zig");
const Ergo = ergo.Ergo;
const readiness = @import("readiness.zig");

const shutdown_str = "Shutdown signal caught. Exiting cleanly.";

pub fn main(init: std.process.Init) !void {
    const allocator: Allocator = init.arena.allocator();
    const io = init.io;
    var env_map = init.environ_map;
    errdefer readiness.notify(io, env_map, .Failed) catch {};

    const os_user = if (env_map.get("USER")) |user| user else @panic("Os user not found");

    const conf = try Config.init(allocator);
    defer conf.deinit(allocator);

    conf.* = .{};

    try config.load(allocator, io, conf, init.minimal.args, env_map);

    // try Io.File.stdout().writeStreamingAll(io, ready_str);
    // try Io.File.stdout().writeStreamingAll(io, "\n");

    var app = try Ergo.init(allocator, io, conf, os_user);
    defer app.deinit();

    try readiness.notify(io, env_map, .Ready);

    // Blocking
    try app.run();

    // try readiness.notify(io, env_map, .Exited);

    std.process.exit(0);
}

// fn spawnChildProcess(io: Io, ch_db: []const u8, socket: []const u8) !std.process.Child {
//     var child_has_error = std.atomic.Value(bool).init(false);
//
//     const argv = &[_][]const u8{
//         "./zig-out/bin/ergo",
//         "--config", "./test/config.toml",
//         "--postgres-host", socket,
//         "--clickhouse-db", ch_db,
//         "--is-test"
//     };
//     var child = try std.process.spawn(io, .{
//         .argv = argv,
//         .stdout = .pipe,
//         .stderr = .pipe,
//     });
//
//     const stderr_thread = try std.Thread.spawn(.{}, monitorStderr, .{
//         io,
//         child.stderr.?,
//         child.id.?,
//         &child_has_error
//     });
//     stderr_thread.detach();
//
//     var buffer: [1024]u8 = undefined;
//     var reader = child.stdout.?.reader(io, &buffer);
//     var r = &reader.interface;
//
//     while (true) {
//         if (try r.takeDelimiter('\n')) |line| {
//             if (std.mem.indexOf(u8, line, ready_str) != null) {
//                 break;
//             } else {
//                 std.log.err("{s}", .{ line });
//             }
//         } else {
//             return error.AppCrashedBeforeReadyError;
//         }
//     }
//
//     return child;
// }
//
// fn terminateChildProcess(io: Io, child: *std.process.Child) !std.process.Child.Term {
//     if (child.id) |pid| {
//         try std.posix.kill(pid, std.posix.SIG.TERM);
//     } else {
//         return error.ChildNotStartedError;
//     }
//
//     return try child.wait(io);
// }
//
// fn monitorStderr(io:Io, stderr: File, child_pid: std.posix.pid_t, has_error: *std.atomic.Value(bool)) void {
//     var buffer: [1024]u8 = undefined;
//
//     var reader = stderr.reader(io, &buffer);
//     var r = &reader.interface;
//
//     while (true) {
//         const maybe_line = r.takeDelimiter('\n') catch |err| switch (err) {
//             error.StreamTooLong => {
//                 _ = r.discardDelimiterInclusive('\n') catch break;
//                 continue;
//             },
//             else => break,
//         };
//
//         const line = maybe_line orelse break;
//
//         std.debug.print("Error: child process threw err: {s}\n", .{ line });
//
//         has_error.store(true, .seq_cst);
//         std.posix.kill(child_pid, std.posix.SIG.KILL) catch {};
//     }
// }

const testing = std.testing;
const t = @import("t.zig");
const phantom = @import("phantom");
const TestProcess = @import("testing/test_process.zig").TestProcess;

test "test:main:beforeAll" {
    // testing.refAllDecls(@This());
}

test "main: ensure correct shutdown" {
    const allocator = testing.allocator;
    const io = testing.io;

    var database = try phantom.Phantom.init(allocator, io, "./test/template.db", "/tmp/ergo", .{});
    defer database.deinit();

    try database.spawn();

    const socket = try allocator.print("{s}/.s.PGSQL.5432", .{ database.socket_path });
    defer allocator.free(socket);

    const db_name = try t.genereateDbName(allocator, io);
    defer allocator.free(db_name);

    try t.createTestDb(allocator, io, db_name);
    defer t.teardownTestDb(allocator, io, db_name) catch {};

    var env = try process.Environ.createMap(testing.environ, allocator);
    defer env.deinit();

    var test_process = try TestProcess.spawn(allocator, io, &env, .{
        .argv = &[_][]const u8{
            "./zig-out/bin/ergo",
            "--config", "./test/config.toml",
            "--postgres-host", socket,
            "--clickhouse-database", db_name,
            "--is-test"
        },
        .readiness = .pipe,
    });
    defer test_process.deinit();

    try test_process.waitUntilReady(.{ .timeout_ms = 1_000 });

    const term = try test_process.terminate();

    try testing.expect(term == .exited);
    try testing.expectEqual(0, term.exited);

    // make sure all connections are closed and no slots are hanging.
    var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "postgres");

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
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_assert_result.stdout);
        allocator.free(pg_assert_result.stderr);
    }

    if (pg_assert_result.term != .exited or pg_assert_result.term.exited != 0 or pg_assert_result.stderr.len > 0) {
        std.debug.print("Error: unable to select pg data: {s}\n", .{pg_assert_result.stderr});
        return error.PgSelectError;
    }

    try testing.expectEqualStrings("", pg_assert_result.stdout);
}

// test "main ensure full transaction sync on interupt" {
//     const allocator = testing.allocator;
//     const io = testing.io;
//
//     var child_has_error = std.atomic.Value(bool).init(false);
//
//     const db_name = try t.genereateDbName(allocator, io);
//     defer allocator.free(db_name);
//
//     try t.createTestDb(allocator, io, db_name);
//
//     const wal_name = try std.fmt.allocPrint(allocator, "wal_slot_{s}", .{db_name});
//     defer allocator.free(wal_name);
//
//     defer t.teardownTestDb(allocator, io, db_name, wal_name) catch {};
//
//     var child = try setupChildProcess(allocator, io, db_name, wal_name);
//     errdefer {
//         if (child.id) |pid| {
//             std.posix.kill(pid, std.posix.SIG.TERM) catch {};
//         }
//     }
//
//     const stderr_thread = try std.Thread.spawn(.{}, monitorStderr, .{
//         child.stderr.?.handle,
//         child.id.?,
//         &child_has_error
//     });
//     stderr_thread.detach();
//
//     var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
//     defer pg_env.deinit();
//     try pg_env.put("PGPASSWORD", "12345678");
//
//     var pg_argv = [_][]const u8{
//         "psql",
//         "-h", "127.0.0.1",
//         "-p", "5432",
//         "-U", "db_rw",
//         "-d", db_name,
//         "-a",
//         "-f", "./test_fixtures/shutdown-query.sql"
//     };
//     const pg_result = try std.process.run(allocator, io, .{ 
//         .argv = &pg_argv,
//         .environ_map = &pg_env, 
//     });
//     defer {
//         allocator.free(pg_result.stdout);
//         allocator.free(pg_result.stderr);
//     }
//
//     if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
//         std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
//         return error.PsqlExecutionFailed;
//     }
//
//     var buffer: [1024]u8 = undefined;
//     var reader = child.stdout.?.reader(io, &buffer);
//     const r = &reader.interface;
//
//     var stdout_acc = std.ArrayList(u8).empty;
//     defer stdout_acc.deinit(allocator);
//
//     while (true) {
//         if (try r.takeDelimiter('\n')) |line| {
//             try stdout_acc.appendSlice(allocator, line);
//
//             if (std.mem.indexOf(u8, stdout_acc.items, wal_stream.sync_marker_str) != null) {
//                 break;
//             }
//         } else {
//             if (child_has_error.load(.seq_cst)) {
//                 return error.ChildProcessError;
//             }
//             return error.ChilsExitedPrematurelyError;
//         }
//     }
//
//     const term = try terminateChildProcess(io, &child);
//
//     try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, term);
//
//     var ch_assert_argv = [_][]const u8{ 
//         "clickhouse-client", 
//         "--host", "127.0.0.1",
//         "--port", "9000",
//         "--user", "default",
//         "--password", "clickhouse",
//         "--database", db_name,
//         "--query", "SELECT action, table_name, primary_keys, changed_columns, old_values, new_values, user_id, ip_address FROM entries ORDER BY primary_keys, action DESC" 
//     };
//     const ch_assert_result = try std.process.run(allocator, io, .{ 
//         .argv = &ch_assert_argv,
//     });
//     defer {
//         allocator.free(ch_assert_result.stdout);
//         allocator.free(ch_assert_result.stderr);
//     }
//
//     if (ch_assert_result.term != .exited or ch_assert_result.term.exited != 0 or ch_assert_result.stderr.len > 0) {
//         std.debug.print("Error: unable to select ch data: {s}\n", .{ch_assert_result.stderr});
//         return error.ChSelectError;
//     }
//
//     try testing.expectEqualStrings(
//         "DELETE\tpublic.addresses\t{'id':'1'}\t['id','address_line_1','postal_code','city','country']\t{'address_line_1':'Googleplex','city':'Mountain View','id':'1','country':'US','postal_code':'94043'}\t{}\t42\t192.168.1.50\n" ++
//         "UPDATE\tpublic.addresses\t{'id':'1'}\t['address_line_1','postal_code','city']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t42\t192.168.1.50\n" ++
//         "INSERT\tpublic.addresses\t{'id':'1'}\t['id','address_line_1','postal_code','city','country']\t{}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'1','country':'US','postal_code':'95014'}\t42\t192.168.1.50\n" ++
//         "DELETE\tpublic.addresses\t{'id':'2'}\t['id','address_line_1','postal_code','city','country']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'2','country':'US','postal_code':'95014'}\t{}\t42\t192.168.1.50\n" ++
//         "UPDATE\tpublic.addresses\t{'id':'2'}\t['address_line_1','postal_code','city']\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t42\t192.168.1.50\n" ++
//         "INSERT\tpublic.addresses\t{'id':'2'}\t['id','address_line_1','postal_code','city','country']\t{}\t{'address_line_1':'Googleplex','city':'Mountain View','id':'2','country':'US','postal_code':'94043'}\t42\t192.168.1.50\n",
//         ch_assert_result.stdout,
//     );
//
//     const pg_assert_query = try std.fmt.allocPrint(allocator, "SELECT active FROM pg_replication_slots WHERE slot_name = '{s}' AND plugin = 'ergo_pgoutput' ORDER BY active LIMIT 1;", .{wal_name});
//     defer allocator.free(pg_assert_query);
//
//     var pg_assert_argv = [_][]const u8{ 
//         "psql",
//         "-h", "127.0.0.1",
//         "-p", "5432",
//         "-U", "db_rw",
//         "-d", db_name,
//         "-q", "-t", "-A", // Formatting
//         "-c", pg_assert_query,
//     };
//     const pg_assert_result = try std.process.run(allocator, io, .{ 
//         .argv = &pg_assert_argv,
//         .environ_map = &pg_env, 
//     });
//     defer {
//         allocator.free(pg_assert_result.stdout);
//         allocator.free(pg_assert_result.stderr);
//     }
//
//     if (pg_assert_result.term != .exited or pg_assert_result.term.exited != 0 or pg_assert_result.stderr.len > 0) {
//         std.debug.print("Error: unable to select pg data: {s}\n", .{pg_assert_result.stderr});
//         return error.PgSelectError;
//     }
//
//     try testing.expectEqualStrings("f\n", pg_assert_result.stdout);
// }
//
// test "main: ensure full commits are logged without interupt" {
//     const allocator = testing.allocator;
//     const io = testing.io;
//
//     var child_has_error = std.atomic.Value(bool).init(false);
//
//     const db_name = try t.genereateDbName(allocator, io);
//     defer allocator.free(db_name);
//
//     try t.createTestDb(allocator, io, db_name);
//
//     const wal_name = try std.fmt.allocPrint(allocator, "wal_slot_{s}", .{db_name});
//     defer allocator.free(wal_name);
//
//     defer t.teardownTestDb(allocator, io, db_name, wal_name) catch {};
//
//     var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
//     defer pg_env.deinit();
//     try pg_env.put("PGPASSWORD", "12345678");
//
//     var child = try setupChildProcess(allocator, io, db_name, wal_name);
//     errdefer {
//         if (child.id) |pid| {
//             std.posix.kill(pid, std.posix.SIG.TERM) catch {};
//         }
//     }
//
//     const stderr_thread = try std.Thread.spawn(.{}, monitorStderr, .{
//         child.stderr.?.handle,
//         child.id.?,
//         &child_has_error
//     });
//     stderr_thread.detach();
//
//     var pg_argv = [_][]const u8{
//         "psql",
//         "-h", "127.0.0.1",
//         "-p", "5432",
//         "-U", "db_rw",
//         "-d", db_name,
//         "-a",
//         "-f", "./test_fixtures/standard-query.sql"
//     };
//     const pg_result = try std.process.run(allocator, io, .{ 
//         .argv = &pg_argv,
//         .environ_map = &pg_env, 
//     });
//     defer {
//         allocator.free(pg_result.stdout);
//         allocator.free(pg_result.stderr);
//     }
//
//     if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
//         std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
//         return error.PsqlExecutionFailed;
//     }
//
//     var buffer: [1024]u8 = undefined;
//     var reader = child.stdout.?.reader(io, &buffer);
//     const r = &reader.interface;
//
//     var stdout_acc = std.ArrayList(u8).empty;
//     defer stdout_acc.deinit(allocator);
//
//     while (true) {
//         if (try r.takeDelimiter('\n')) |line| {
//             try stdout_acc.appendSlice(allocator, line);
//
//             if (std.mem.indexOf(u8, stdout_acc.items, wal_stream.submit_marker_str) != null) {
//                 break;
//             }
//         } else {
//             if (child_has_error.load(.seq_cst)) {
//                 return error.ChildProcessError;
//             }
//             return error.ChilsExitedPrematurelyError;
//         }
//     }
//
//     var ch_assert_argv = [_][]const u8{ 
//         "clickhouse-client", 
//         "--host", "127.0.0.1",
//         "--port", "9000",
//         "--user", "default",
//         "--password", "clickhouse",
//         "--database", db_name,
//         "--query", "SELECT action, table_name, primary_keys, changed_columns, old_values, new_values, user_id, ip_address FROM entries ORDER BY primary_keys, action DESC" 
//     };
//     const ch_assert_result = try std.process.run(allocator, io, .{ 
//         .argv = &ch_assert_argv,
//     });
//     defer {
//         allocator.free(ch_assert_result.stdout);
//         allocator.free(ch_assert_result.stderr);
//     }
//
//     if (ch_assert_result.term != .exited or ch_assert_result.term.exited != 0 or ch_assert_result.stderr.len > 0) {
//         std.debug.print("Error: unable to select ch data: {s}\n", .{ch_assert_result.stderr});
//         return error.ChSelectError;
//     }
//
//     try testing.expectEqualStrings(
//         "DELETE\tpublic.addresses\t{'id':'1'}\t['id','address_line_1','postal_code','city','country']\t{'address_line_1':'Googleplex','city':'Mountain View','id':'1','country':'US','postal_code':'94043'}\t{}\t42\t192.168.1.50\n" ++
//         "UPDATE\tpublic.addresses\t{'id':'1'}\t['address_line_1','postal_code','city']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t42\t192.168.1.50\n" ++
//         "INSERT\tpublic.addresses\t{'id':'1'}\t['id','address_line_1','postal_code','city','country']\t{}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'1','country':'US','postal_code':'95014'}\t42\t192.168.1.50\n" ++
//         "DELETE\tpublic.addresses\t{'id':'2'}\t['id','address_line_1','postal_code','city','country']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'2','country':'US','postal_code':'95014'}\t{}\t42\t192.168.1.50\n" ++
//         "UPDATE\tpublic.addresses\t{'id':'2'}\t['address_line_1','postal_code','city']\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t42\t192.168.1.50\n" ++
//         "INSERT\tpublic.addresses\t{'id':'2'}\t['id','address_line_1','postal_code','city','country']\t{}\t{'address_line_1':'Googleplex','city':'Mountain View','id':'2','country':'US','postal_code':'94043'}\t42\t192.168.1.50\n",
//         ch_assert_result.stdout,
//     );
//
//     const term = try terminateChildProcess(io, &child);
//
//     try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, term);
// }

test "correct shutdown" {
}

test "do data loss on shutdown and boot" {
}

test "receive commandComplete and recover" {
}

test "receive readyForQuery and recover" {
}

