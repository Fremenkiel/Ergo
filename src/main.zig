const std = @import("std");
const builtin = @import("builtin");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;
const posix = std.posix;

const assert = std.debug.assert;

const ch = @import("ch");
const wal_stream = @import("wal_stream.zig");
const types = @import("types");

const PgClient = @import("pg_client.zig").PgClient;
const ChClient = @import("ch_client.zig").ChClient;
const WalStream = wal_stream.WalStream;

const application_name = "Ergo";

const ready_str = "Service ready";
const shutdown_str = "Shutdown signal caught. Exiting cleanly.";

const Options = struct {
    ch_host: []const u8 = "localhost",
    ch_port: u16 = 9000,
    ch_user: []const u8 = "default",
    ch_pass: []const u8 = "clickhouse",
    ch_db: []const u8 = "audit_log",

    pg_host: []const u8 = "localhost",
    pg_port: u16 = 5432,
    pg_user: []const u8 = "db_rw",
    pg_pass: []const u8 = "12345678",
    pg_db: []const u8 = "db",
    pg_wal: []const u8 = "wal_slot",

    is_test: bool = false,
    bulk_sync_duration: u32 = 500,
    user: []const u8,

    fn init(allocator: mem.Allocator, environ_map: *std.process.Environ.Map) !Options {
        const os_user = environ_map.get("USER");

        assert(os_user != null);

        var options: Options = .{
            .ch_host = try allocator.dupe(u8, environ_map.get("CH_HOST") orelse "localhost"),
            .ch_user = try allocator.dupe(u8, environ_map.get("CH_USER") orelse "default"),
            .ch_pass = try allocator.dupe(u8, environ_map.get("CH_PASS") orelse "clickhouse"),
            .ch_db = try allocator.dupe(u8, environ_map.get("CH_DB") orelse "audit_log"),

            .pg_host = try allocator.dupe(u8, environ_map.get("PG_HOST") orelse "localhost"),
            .pg_user = try allocator.dupe(u8, environ_map.get("PG_USER") orelse "db_rw"),
            .pg_pass = try allocator.dupe(u8, environ_map.get("PG_PASS") orelse "12345678"),
            .pg_db = try allocator.dupe(u8, environ_map.get("PG_DB") orelse "db"),
            .pg_wal = try allocator.dupe(u8, environ_map.get("PG_WAL") orelse "wal_slot"),

            .user = try allocator.dupe(u8, os_user.?)
        };

        if (environ_map.get("ERGO_TEST")) |is_test| {
            options.is_test = mem.eql(u8, "1", is_test);
        }
        if (environ_map.get("BULK_SYNC_DURATION")) |bulk_sync_duration| {
            options.bulk_sync_duration = try std.fmt.parseInt(u32, bulk_sync_duration, 10);
        }

        if (environ_map.get("CH_PORT")) |ch_port| {
            options.ch_port = try std.fmt.parseInt(u16, ch_port, 10);
        }

        if (environ_map.get("PG_PORT")) |pg_port| {
            options.pg_port = try std.fmt.parseInt(u16, pg_port, 10);
        }

        return options;
    }

    fn deinit(self: *@This(), allocator: mem.Allocator) void {
        allocator.free(self.ch_host);
        allocator.free(self.ch_user);
        allocator.free(self.ch_pass);
        allocator.free(self.ch_db);

        allocator.free(self.pg_host);
        allocator.free(self.pg_user);
        allocator.free(self.pg_pass);
        allocator.free(self.pg_db);

        allocator.free(self.user);
    }
};

var is_shutting_down = std.atomic.Value(bool).init(false);

fn handleSignel(sig: std.posix.SIG) callconv(.c) void {
    _ = sig;

    is_shutting_down.store(true, .seq_cst);
}

pub fn main(init: std.process.Init) !void {
    const allocator: mem.Allocator = init.arena.allocator();
    const io = init.io;

    var act = std.posix.Sigaction{
        .handler = .{ .handler = handleSignel },
        .mask = std.posix.sigemptyset(),
        .flags = 0
    };
    std.posix.sigaction(std.posix.SIG.INT, &act, null);
    std.posix.sigaction(std.posix.SIG.TERM, &act, null);

    var options: Options = try .init(allocator, init.environ_map);
    defer options.deinit(allocator);

    try Io.File.stdout().writeStreamingAll(io, ready_str);
    try Io.File.stdout().writeStreamingAll(io, "\n");

    var ch_client = ChClient.init(allocator, io, .{
        .host = options.ch_host,
        .port = options.ch_port,
        .username = options.ch_user,
        .password = options.ch_pass,
        .database = options.ch_db,
        .application_name = application_name,
    }, options.user);
    defer ch_client.deinit();

    try ch_client.connect();
    defer ch_client.disconnect();

    var pg_client = try PgClient.init(allocator, io, .{
        .port = 5432,
        .host = options.pg_host,
        .wal = options.pg_wal,
        .username = options.pg_user,
        .password = options.pg_pass,
        .database = options.pg_db,
        .application_name = application_name,
        .timeout_ms = 10_000,
        .startup_parameters = null
    });
    defer pg_client.deinit();
    defer pg_client.cancel();

    var processor = WalStream.init(
        allocator,
        io,
        &ch_client,
        &pg_client, 
        options.bulk_sync_duration,
        options.is_test
    );
    defer processor.deinit();
    defer pg_client.cancel();

    try processor.startStreaming();
    defer {
        processor.endStreaming() catch |err| {
            const error_message = std.fmt.allocPrint(allocator, "Error: An error occurred while ending stream: {s}\n", .{@errorName(err)}) catch { 
                // At this point, we are fucked.
                @panic("All bets are off");
            };
            defer allocator.free(error_message);

            std.Io.File.stderr().writeStreamingAll(io, error_message) catch {
                @panic("Unable to print error");
            };
        };
    }

    // Blocking
    try processor.stream(&is_shutting_down);

    try Io.File.stdout().writeStreamingAll(io, shutdown_str);
    try Io.File.stdout().writeStreamingAll(io, "\n");

    std.process.exit(0);
}

fn setupChildProcess(allocator: mem.Allocator, io: Io, db_name: []const u8, wal_name: []const u8) !std.process.Child {
    var env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer env.deinit();

    try env.put("ERGO_TEST", "1");
    try env.put("CH_DB", db_name);
    try env.put("PG_DB", db_name);
    try env.put("PG_WAL", wal_name);

    const argv = &[_][]const u8{
        "./zig-out/bin/ergo",
    };
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdout = .pipe,
        .stderr = .pipe,
        .environ_map = &env,
    });

    var buffer: [1024]u8 = undefined;
    var reader = child.stdout.?.reader(io, &buffer);
    var r = &reader.interface;

    while (true) {
        if (try r.takeDelimiter('\n')) |line| {
            if (std.mem.indexOf(u8, line, ready_str) != null) {
                break;
            }
        } else {
            return error.AppCrashedBeforeReadyError;
        }
    }

    return child;
}

fn terminateChildProcess(io: Io, child: *std.process.Child) !std.process.Child.Term {
    if (child.id) |pid| {
        try std.posix.kill(pid, std.posix.SIG.TERM);
    } else {
        return error.ChildNotStartedError;
    }

    return try child.wait(io);
}

fn monitorStderr(stderr: std.posix.fd_t, child_pid: std.posix.pid_t, has_error: *std.atomic.Value(bool)) void {
    var buffer: [1024]u8 = undefined;

    const bytes_read = std.posix.read(stderr, &buffer) catch 0;

    if (bytes_read > 0) {
        std.debug.print("Error: child process threw err: {s}\n", .{buffer[0..bytes_read]});

        has_error.store(true, .seq_cst);
        std.posix.kill(child_pid, std.posix.SIG.KILL) catch {};
    }
}

const t = @import("t.zig");
test "test:main:beforeAll" {
    std.testing.refAllDecls(@This());
}
//
// test "main ensure correct shutdown" {
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
//     const term = try terminateChildProcess(io, &child);
//
//     try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, term);
//
//     // make sure all connections are closed and no slots are hanging.
//     var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
//     defer pg_env.deinit();
//     try pg_env.put("PGPASSWORD", "postgres");
//
//     const pg_assert_query = try std.fmt.allocPrint(allocator, "SELECT * FROM pg_stat_activity WHERE datname = '{s}' AND application_name = '{s}';", .{db_name, application_name});
//     defer allocator.free(pg_assert_query);
//
//     var pg_assert_argv = [_][]const u8{ 
//         "psql",
//         "-h", "127.0.0.1",
//         "-p", "5432",
//         "-U", "postgres",
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
//     try testing.expectEqualStrings("", pg_assert_result.stdout);
// }
//
// // test "main ensure full transaction sync on interupt" {
// //     const allocator = testing.allocator;
// //     const io = testing.io;
// //
// //     var child_has_error = std.atomic.Value(bool).init(false);
// //
// //     const db_name = try t.genereateDbName(allocator, io);
// //     defer allocator.free(db_name);
// //
// //     try t.createTestDb(allocator, io, db_name);
// //
// //     const wal_name = try std.fmt.allocPrint(allocator, "wal_slot_{s}", .{db_name});
// //     defer allocator.free(wal_name);
// //
// //     defer t.teardownTestDb(allocator, io, db_name, wal_name) catch {};
// //
// //     var child = try setupChildProcess(allocator, io, db_name, wal_name);
// //     errdefer {
// //         if (child.id) |pid| {
// //             std.posix.kill(pid, std.posix.SIG.TERM) catch {};
// //         }
// //     }
// //
// //     const stderr_thread = try std.Thread.spawn(.{}, monitorStderr, .{
// //         child.stderr.?.handle,
// //         child.id.?,
// //         &child_has_error
// //     });
// //     stderr_thread.detach();
// //
// //     var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
// //     defer pg_env.deinit();
// //     try pg_env.put("PGPASSWORD", "12345678");
// //
// //     var pg_argv = [_][]const u8{
// //         "psql",
// //         "-h", "127.0.0.1",
// //         "-p", "5432",
// //         "-U", "db_rw",
// //         "-d", db_name,
// //         "-a",
// //         "-f", "./test_fixtures/shutdown-query.sql"
// //     };
// //     const pg_result = try std.process.run(allocator, io, .{ 
// //         .argv = &pg_argv,
// //         .environ_map = &pg_env, 
// //     });
// //     defer {
// //         allocator.free(pg_result.stdout);
// //         allocator.free(pg_result.stderr);
// //     }
// //
// //     if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
// //         std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
// //         return error.PsqlExecutionFailed;
// //     }
// //
// //     var buffer: [1024]u8 = undefined;
// //     var reader = child.stdout.?.reader(io, &buffer);
// //     const r = &reader.interface;
// //
// //     var stdout_acc = std.ArrayList(u8).empty;
// //     defer stdout_acc.deinit(allocator);
// //
// //     while (true) {
// //         if (try r.takeDelimiter('\n')) |line| {
// //             try stdout_acc.appendSlice(allocator, line);
// //
// //             if (std.mem.indexOf(u8, stdout_acc.items, wal_stream.sync_marker_str) != null) {
// //                 break;
// //             }
// //         } else {
// //             if (child_has_error.load(.seq_cst)) {
// //                 return error.ChildProcessError;
// //             }
// //             return error.ChilsExitedPrematurelyError;
// //         }
// //     }
// //
// //     const term = try terminateChildProcess(io, &child);
// //
// //     try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, term);
// //
// //     var ch_assert_argv = [_][]const u8{ 
// //         "clickhouse-client", 
// //         "--host", "127.0.0.1",
// //         "--port", "9000",
// //         "--user", "default",
// //         "--password", "clickhouse",
// //         "--database", db_name,
// //         "--query", "SELECT action, table_name, primary_key, changed_columns, old_values, new_values, user_id, ip_address FROM entries ORDER BY primary_key, action DESC" 
// //     };
// //     const ch_assert_result = try std.process.run(allocator, io, .{ 
// //         .argv = &ch_assert_argv,
// //     });
// //     defer {
// //         allocator.free(ch_assert_result.stdout);
// //         allocator.free(ch_assert_result.stderr);
// //     }
// //
// //     if (ch_assert_result.term != .exited or ch_assert_result.term.exited != 0 or ch_assert_result.stderr.len > 0) {
// //         std.debug.print("Error: unable to select ch data: {s}\n", .{ch_assert_result.stderr});
// //         return error.ChSelectError;
// //     }
// //
// //     try testing.expectEqualStrings(
// //         "DELETE\tpublic.addresses\t1\t['address_line_1','city','id','country','postal_code']\t{'address_line_1':'Googleplex','city':'Mountain View','id':'1','country':'US','postal_code':'94043'}\t{}\t42\t192.168.1.50\n" ++
// //         "UPDATE\tpublic.addresses\t1\t['address_line_1','city','postal_code']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t42\t192.168.1.50\n" ++
// //         "INSERT\tpublic.addresses\t1\t['address_line_1','city','id','country','postal_code']\t{}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'1','country':'US','postal_code':'95014'}\t42\t192.168.1.50\n" ++
// //         "DELETE\tpublic.addresses\t2\t['address_line_1','city','id','country','postal_code']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'2','country':'US','postal_code':'95014'}\t{}\t42\t192.168.1.50\n" ++
// //         "UPDATE\tpublic.addresses\t2\t['address_line_1','city','postal_code']\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t42\t192.168.1.50\n" ++
// //         "INSERT\tpublic.addresses\t2\t['address_line_1','city','id','country','postal_code']\t{}\t{'address_line_1':'Googleplex','city':'Mountain View','id':'2','country':'US','postal_code':'94043'}\t42\t192.168.1.50\n",
// //         ch_assert_result.stdout,
// //     );
// //
// //     const pg_assert_query = try std.fmt.allocPrint(allocator, "SELECT active FROM pg_replication_slots WHERE slot_name = '{s}' AND plugin = 'pgoutput' ORDER BY active LIMIT 1;", .{wal_name});
// //     defer allocator.free(pg_assert_query);
// //
// //     var pg_assert_argv = [_][]const u8{ 
// //         "psql",
// //         "-h", "127.0.0.1",
// //         "-p", "5432",
// //         "-U", "db_rw",
// //         "-d", db_name,
// //         "-q", "-t", "-A", // Formatting
// //         "-c", pg_assert_query,
// //     };
// //     const pg_assert_result = try std.process.run(allocator, io, .{ 
// //         .argv = &pg_assert_argv,
// //         .environ_map = &pg_env, 
// //     });
// //     defer {
// //         allocator.free(pg_assert_result.stdout);
// //         allocator.free(pg_assert_result.stderr);
// //     }
// //
// //     if (pg_assert_result.term != .exited or pg_assert_result.term.exited != 0 or pg_assert_result.stderr.len > 0) {
// //         std.debug.print("Error: unable to select pg data: {s}\n", .{pg_assert_result.stderr});
// //         return error.PgSelectError;
// //     }
// //
// //     try testing.expectEqualStrings("f\n", pg_assert_result.stdout);
// // }
//
// // test "main: ensure full commits are logged without interupt" {
// //     const allocator = testing.allocator;
// //     const io = testing.io;
// //
// //     var child_has_error = std.atomic.Value(bool).init(false);
// //
// //     const db_name = try t.genereateDbName(allocator, io);
// //     defer allocator.free(db_name);
// //
// //     try t.createTestDb(allocator, io, db_name);
// //
// //     const wal_name = try std.fmt.allocPrint(allocator, "wal_slot_{s}", .{db_name});
// //     defer allocator.free(wal_name);
// //
// //     defer t.teardownTestDb(allocator, io, db_name, wal_name) catch {};
// //
// //     var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
// //     defer pg_env.deinit();
// //     try pg_env.put("PGPASSWORD", "12345678");
// //
// //     var child = try setupChildProcess(allocator, io, db_name, wal_name);
// //     errdefer {
// //         if (child.id) |pid| {
// //             std.posix.kill(pid, std.posix.SIG.TERM) catch {};
// //         }
// //     }
// //
// //     const stderr_thread = try std.Thread.spawn(.{}, monitorStderr, .{
// //         child.stderr.?.handle,
// //         child.id.?,
// //         &child_has_error
// //     });
// //     stderr_thread.detach();
// //
// //     var pg_argv = [_][]const u8{
// //         "psql",
// //         "-h", "127.0.0.1",
// //         "-p", "5432",
// //         "-U", "db_rw",
// //         "-d", db_name,
// //         "-a",
// //         "-f", "./test_fixtures/standard-query.sql"
// //     };
// //     const pg_result = try std.process.run(allocator, io, .{ 
// //         .argv = &pg_argv,
// //         .environ_map = &pg_env, 
// //     });
// //     defer {
// //         allocator.free(pg_result.stdout);
// //         allocator.free(pg_result.stderr);
// //     }
// //
// //     if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
// //         std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
// //         return error.PsqlExecutionFailed;
// //     }
// //
// //     var buffer: [1024]u8 = undefined;
// //     var reader = child.stdout.?.reader(io, &buffer);
// //     const r = &reader.interface;
// //
// //     var stdout_acc = std.ArrayList(u8).empty;
// //     defer stdout_acc.deinit(allocator);
// //
// //     while (true) {
// //         if (try r.takeDelimiter('\n')) |line| {
// //             try stdout_acc.appendSlice(allocator, line);
// //
// //             if (std.mem.indexOf(u8, stdout_acc.items, wal_stream.submit_marker_str) != null) {
// //                 break;
// //             }
// //         } else {
// //             if (child_has_error.load(.seq_cst)) {
// //                 return error.ChildProcessError;
// //             }
// //             return error.ChilsExitedPrematurelyError;
// //         }
// //     }
// //
// //     var ch_assert_argv = [_][]const u8{ 
// //         "clickhouse-client", 
// //         "--host", "127.0.0.1",
// //         "--port", "9000",
// //         "--user", "default",
// //         "--password", "clickhouse",
// //         "--database", db_name,
// //         "--query", "SELECT action, table_name, primary_key, changed_columns, old_values, new_values, user_id, ip_address FROM entries ORDER BY primary_key, action DESC" 
// //     };
// //     const ch_assert_result = try std.process.run(allocator, io, .{ 
// //         .argv = &ch_assert_argv,
// //     });
// //     defer {
// //         allocator.free(ch_assert_result.stdout);
// //         allocator.free(ch_assert_result.stderr);
// //     }
// //
// //     if (ch_assert_result.term != .exited or ch_assert_result.term.exited != 0 or ch_assert_result.stderr.len > 0) {
// //         std.debug.print("Error: unable to select ch data: {s}\n", .{ch_assert_result.stderr});
// //         return error.ChSelectError;
// //     }
// //
// //     try testing.expectEqualStrings(
// //         "DELETE\tpublic.addresses\t1\t['address_line_1','city','id','country','postal_code']\t{'address_line_1':'Googleplex','city':'Mountain View','id':'1','country':'US','postal_code':'94043'}\t{}\t42\t192.168.1.50\n" ++
// //         "UPDATE\tpublic.addresses\t1\t['address_line_1','city','postal_code']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t42\t192.168.1.50\n" ++
// //         "INSERT\tpublic.addresses\t1\t['address_line_1','city','id','country','postal_code']\t{}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'1','country':'US','postal_code':'95014'}\t42\t192.168.1.50\n" ++
// //         "DELETE\tpublic.addresses\t2\t['address_line_1','city','id','country','postal_code']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'2','country':'US','postal_code':'95014'}\t{}\t42\t192.168.1.50\n" ++
// //         "UPDATE\tpublic.addresses\t2\t['address_line_1','city','postal_code']\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t42\t192.168.1.50\n" ++
// //         "INSERT\tpublic.addresses\t2\t['address_line_1','city','id','country','postal_code']\t{}\t{'address_line_1':'Googleplex','city':'Mountain View','id':'2','country':'US','postal_code':'94043'}\t42\t192.168.1.50\n",
// //         ch_assert_result.stdout,
// //     );
// //
// //     const term = try terminateChildProcess(io, &child);
// //
// //     try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, term);
// // }
//
// test "correct shutdown" {
// }
//
// test "do data loss on shutdown and boot" {
// }
//
// test "receive commandComplete and recover" {
// }
//
// test "receive readyForQuery and recover" {
// }
//
