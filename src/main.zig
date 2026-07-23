const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const pg = @import("pg");
const ch = @import("ch");

const PgClient = @import("pg_client.zig").PgClient;
const ChClient = @import("ch_client.zig").ChClient;
const wal_processor = @import("wal_processor.zig");
const types = @import("types.zig");

const application_name = "Ergo";

const ready_str = "Service ready";
const shutdown_str = "Shutdown signal caught. Exiting cleanly.";

const Options = struct {
    ch_host: []const u8,
    ch_port: u16,
    ch_user: []const u8,
    ch_pass: []const u8,
    ch_db: []const u8,

    pg_host: []const u8,
    pg_port: u16,
    pg_user: []const u8,
    pg_pass: []const u8,
    pg_db: []const u8,

    fn init(allocator: mem.Allocator, map: std.StringHashMap([]const u8)) !Options {
        const ch_host_value = map.get("--ch-host");
        const ch_port_value = map.get("--ch-port");
        const ch_user_value = map.get("--ch-user");
        const ch_pass_value = map.get("--ch-pass");
        const ch_db_value = map.get("--ch-db");

        const pg_host_value = map.get("--pg-host");
        const pg_port_value = map.get("--pg-port");
        const pg_user_value = map.get("--pg-user");
        const pg_pass_value = map.get("--pg-pass");
        const pg_db_value = map.get("--pg-db");

        return .{
            .ch_host = try allocator.dupe(u8, if (ch_host_value != null) ch_host_value.? else "localhost"),
            .ch_port = if (ch_port_value != null) try std.fmt.parseInt(u16, ch_port_value.?, 10) else 9000,
            .ch_user = try allocator.dupe(u8, if (ch_user_value != null) ch_user_value.? else "default"),
            .ch_pass = try allocator.dupe(u8, if (ch_pass_value != null) ch_pass_value.? else "clickhouse"),
            .ch_db = try allocator.dupe(u8, if (ch_db_value != null) ch_db_value.? else "audit_log"),

            .pg_host = try allocator.dupe(u8, if (pg_host_value != null) pg_host_value.? else "localhost"),
            .pg_port = if (pg_port_value != null) try std.fmt.parseInt(u16, pg_port_value.?, 10) else 5432,
            .pg_user = try allocator.dupe(u8, if (pg_user_value != null) pg_user_value.? else "db_rp"),
            .pg_pass = try allocator.dupe(u8, if (pg_pass_value != null) pg_pass_value.? else "12345678"),
            .pg_db = try allocator.dupe(u8, if (pg_db_value != null) pg_db_value.? else "db"),
        };
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
    }
};

var is_shutting_down = std.atomic.Value(bool).init(false);

fn handleSignel(sig: std.posix.SIG) callconv(.c) void {
    _ = sig;

    is_shutting_down.store(true, .seq_cst);
}

fn parseArgs(allocator: mem.Allocator, args: []const []const u8) !Options {
    var args_map = std.StringHashMap([]const u8).init(allocator);
    defer args_map.deinit();

    if (args.len % 2 != 0) {
        std.debug.print("Error: expedted equal args pairs, got {d}\n", .{args.len});
        return error.ArgumentCountMismatchError;
    }

    try args_map.ensureUnusedCapacity(@as(u32, @truncate(args.len / 2)));

    var i: u8 = 0;
    while (i < args.len) : (i += 2) {
        const key = args[i];
        const value = args[i + 1];

        if (!mem.startsWith(u8, key, "--")) {
            std.debug.print("Error: expedted key format '--[]', got: {s}\n", .{key});
            return error.InvalidArgsKeyFormatError;
        }

        if (mem.startsWith(u8, value, "--")) {
            std.debug.print("Error: expedted value after key, got: {s}\n", .{value});
            return error.InvalidArgsValueFormatError;
        }

        args_map.putAssumeCapacity(key, value);
    }

    return try Options.init(allocator, args_map);
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

    const args = try init.minimal.args.toSlice(allocator);
    var options = try parseArgs(allocator, args[1..]);
    defer options.deinit(allocator);

    const user_env_key = if (builtin.os.tag == .windows) "USERNAME" else "USER";
    const os_user = init.environ_map.get(user_env_key);

    // used for running test in child process
    const is_test = init.environ_map.get("ERGO_TEST");

    try Io.File.stdout().writeStreamingAll(io, ready_str);
    try Io.File.stdout().writeStreamingAll(io, "\n");

    var pg_client = try PgClient.init(io, allocator, .{
        .connect = .{  
            .port = 5432,
            .host = options.pg_host,
        },
        .auth = .{
            .username = options.pg_user,
            .password = options.pg_pass,
            .database = options.pg_db,
            .application_name = "Ergo",
            .timeout_ms = 10_000,
        } 
    });
    defer pg_client.deinit();

    var ch_client = ChClient.init(allocator, io, .{
        .host = options.ch_host,
        .port = options.ch_port,
        .username = options.ch_user,
        .password = options.ch_pass,
        .database = options.ch_db,
        .application_name = application_name,
    }, os_user.?);
    defer ch_client.deinit();

    try ch_client.connect();
    defer ch_client.disconnect();

    var processor = wal_processor.WalProcessor(PgClient, ChClient){ 
        .pg_client = &pg_client, 
        .ch_client = &ch_client,
        .last_write_timestamp = std.Io.Clock.real.now(io),
        .allocator = allocator,
        .io = io,
        .is_test = is_test != null,
    };
    defer processor.deinit();

    try processor.startStreaming(&is_shutting_down);

    try Io.File.stdout().writeStreamingAll(io, shutdown_str);
    try Io.File.stdout().writeStreamingAll(io, "\n");

    std.process.exit(0);
}

fn createTestDb(allocator: mem.Allocator, io: Io, db_name: []const u8) !void {
    const query = try std.fmt.allocPrint(allocator, "CREATE DATABASE IF NOT EXISTS {s}", .{db_name});
    defer allocator.free(query);

    var ch_create_argv = [_][]const u8{ 
        "clickhouse-client", 
        "--host", "127.0.0.1",
        "--port", "9000",
        "--user", "default",
        "--password", "clickhouse",
        "--query", query
    };
    const ch_create_result = try std.process.run(allocator, io, .{ 
        .argv = &ch_create_argv,
    });
    defer {
        allocator.free(ch_create_result.stdout);
        allocator.free(ch_create_result.stderr);
    }

    if (ch_create_result.stderr.len > 0) {
        std.debug.print("Error: unable to create new ch db: {s}\n", .{ch_create_result.stderr});
        return error.CreateTestDbFailedError;
    }

    var ch_init_argv = [_][]const u8{ 
        "clickhouse-client", 
        "--host", "127.0.0.1",
        "--port", "9000",
        "--user", "default",
        "--password", "clickhouse",
	"--database", db_name,
	"--queries-file", "./infra/ch/init.sql"
    };
    const ch_init_result = try std.process.run(allocator, io, .{ 
        .argv = &ch_init_argv,
    });
    defer {
        allocator.free(ch_init_result.stdout);
        allocator.free(ch_init_result.stderr);
    }

    if (ch_init_result.stderr.len > 0) {
        std.debug.print("Error: unable to init new ch db: {s}\n", .{ch_init_result.stderr});
        return error.CreateTestDbFailedInitError;
    }
}

fn setupChildProcess(allocator: mem.Allocator, io: Io, db_name: []const u8) !std.process.Child {
    var env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer env.deinit();
    try env.put("ERGO_TEST", "1");

    const argv = &[_][]const u8{
        "./zig-out/bin/ergo",
        "--ch-db", db_name
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


test "test:main:beforeAll" {
    std.testing.refAllDecls(@This());
}

test "main ensure full transaction sync on interupt" {
    const allocator = testing.allocator;
    const io = testing.io;

    var child_has_error = std.atomic.Value(bool).init(false);

    const db_name = try std.fmt.allocPrint(allocator, "test_db_{d}", .{std.Io.Clock.real.now(io).toNanoseconds()});
    defer allocator.free(db_name);

    var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "12345678");

    try createTestDb(allocator, io, db_name);
    var child = try setupChildProcess(allocator, io, db_name);
    errdefer {
        if (child.id) |pid| {
            std.posix.kill(pid, std.posix.SIG.TERM) catch {};
        }
    }

    const stderr_thread = try std.Thread.spawn(.{}, monitorStderr, .{
        child.stderr.?.handle,
        child.id.?,
        &child_has_error
    });
    stderr_thread.detach();

    var pg_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "db_rw",
        "-d", "db",
        "-a",
        "-f", "./test_fixtures/shutdown_query.sql"
    };
    const pg_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_result.stdout);
        allocator.free(pg_result.stderr);
    }

    if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
        std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
        return error.PsqlExecutionFailed;
    }

    var buffer: [1024]u8 = undefined;
    var reader = child.stdout.?.reader(io, &buffer);
    const r = &reader.interface;

    var stdout_acc = std.ArrayList(u8).empty;
    defer stdout_acc.deinit(allocator);

    while (true) {
        if (try r.takeDelimiter('\n')) |line| {
            try stdout_acc.appendSlice(allocator, line);

            if (std.mem.indexOf(u8, stdout_acc.items, wal_processor.sync_marker_str) != null) {
                break;
            }
        } else {
            if (child_has_error.load(.seq_cst)) {
                return error.ChildProcessError;
            }
            return error.ChilsExitedPrematurelyError;
        }
    }

    const term = try terminateChildProcess(io, &child);

    try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, term);

    var ch_assert_argv = [_][]const u8{ 
        "clickhouse-client", 
        "--host", "127.0.0.1",
        "--port", "9000",
        "--user", "default",
        "--password", "clickhouse",
        "--database", db_name,
        "--query", "SELECT action, table_name, primary_key, changed_columns, old_values, new_values, user_id, ip_address FROM entries ORDER BY primary_key, action DESC" 
    };
    const ch_assert_result = try std.process.run(allocator, io, .{ 
        .argv = &ch_assert_argv,
    });
    defer {
        allocator.free(ch_assert_result.stdout);
        allocator.free(ch_assert_result.stderr);
    }

    if (ch_assert_result.term != .exited or ch_assert_result.term.exited != 0 or ch_assert_result.stderr.len > 0) {
        std.debug.print("Error: unable to select ch data: {s}\n", .{ch_assert_result.stderr});
        return error.ChSelectError;
    }

    try testing.expectEqualStrings(
        "DELETE\tpublic.addresses\t1\t['address_line_1','city','id','country','postal_code']\t{'address_line_1':'Googleplex','city':'Mountain View','id':'1','country':'US','postal_code':'94043'}\t{}\t42\t192.168.1.50\n" ++
        "UPDATE\tpublic.addresses\t1\t['address_line_1','city','postal_code']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t42\t192.168.1.50\n" ++
        "INSERT\tpublic.addresses\t1\t['address_line_1','city','id','country','postal_code']\t{}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'1','country':'US','postal_code':'95014'}\t42\t192.168.1.50\n" ++
        "DELETE\tpublic.addresses\t2\t['address_line_1','city','id','country','postal_code']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'2','country':'US','postal_code':'95014'}\t{}\t42\t192.168.1.50\n" ++
        "UPDATE\tpublic.addresses\t2\t['address_line_1','city','postal_code']\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t42\t192.168.1.50\n" ++
        "INSERT\tpublic.addresses\t2\t['address_line_1','city','id','country','postal_code']\t{}\t{'address_line_1':'Googleplex','city':'Mountain View','id':'2','country':'US','postal_code':'94043'}\t42\t192.168.1.50\n",
        ch_assert_result.stdout,
    );

    var pg_assert_argv = [_][]const u8{ 
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "db_rw",
        "-d", "db",
        "-q", "-t", "-A", // Formatting
        "-c", "SELECT active FROM pg_replication_slots WHERE slot_name = 'wal_slot' AND plugin = 'pgoutput' ORDER BY active LIMIT 1;",
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

    try testing.expectEqualStrings("f\n", pg_assert_result.stdout);
}


test "making sure full commits are logged without interupt" {
    const allocator = testing.allocator;
    const io = testing.io;

    var child_has_error = std.atomic.Value(bool).init(false);

    const db_name = try std.fmt.allocPrint(allocator, "test_db_{d}", .{std.Io.Clock.real.now(io).toNanoseconds()});
    defer allocator.free(db_name);

    var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "12345678");

    try createTestDb(allocator, io, db_name);
    var child = try setupChildProcess(allocator, io, db_name);
    errdefer {
        if (child.id) |pid| {
            std.posix.kill(pid, std.posix.SIG.TERM) catch {};
        }
    }

    const stderr_thread = try std.Thread.spawn(.{}, monitorStderr, .{
        child.stderr.?.handle,
        child.id.?,
        &child_has_error
    });
    stderr_thread.detach();

    var pg_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "db_rw",
        "-d", "db",
        "-a",
        "-f", "./test_fixtures/standard_query.sql"
    };
    const pg_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_result.stdout);
        allocator.free(pg_result.stderr);
    }

    if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
        std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
        return error.PsqlExecutionFailed;
    }

    var buffer: [1024]u8 = undefined;
    var reader = child.stdout.?.reader(io, &buffer);
    const r = &reader.interface;

    var stdout_acc = std.ArrayList(u8).empty;
    defer stdout_acc.deinit(allocator);

    while (true) {
        if (try r.takeDelimiter('\n')) |line| {
            try stdout_acc.appendSlice(allocator, line);

            if (std.mem.indexOf(u8, stdout_acc.items, wal_processor.submit_marker_str) != null) {
                break;
            }
        } else {
            if (child_has_error.load(.seq_cst)) {
                return error.ChildProcessError;
            }
            return error.ChilsExitedPrematurelyError;
        }
    }

    var ch_assert_argv = [_][]const u8{ 
        "clickhouse-client", 
        "--host", "127.0.0.1",
        "--port", "9000",
        "--user", "default",
        "--password", "clickhouse",
        "--database", db_name,
        "--query", "SELECT action, table_name, primary_key, changed_columns, old_values, new_values, user_id, ip_address FROM entries ORDER BY primary_key, action DESC" 
    };
    const ch_assert_result = try std.process.run(allocator, io, .{ 
        .argv = &ch_assert_argv,
    });
    defer {
        allocator.free(ch_assert_result.stdout);
        allocator.free(ch_assert_result.stderr);
    }

    if (ch_assert_result.term != .exited or ch_assert_result.term.exited != 0 or ch_assert_result.stderr.len > 0) {
        std.debug.print("Error: unable to select ch data: {s}\n", .{ch_assert_result.stderr});
        return error.ChSelectError;
    }

    try testing.expectEqualStrings(
        "DELETE\tpublic.addresses\t1\t['address_line_1','city','id','country','postal_code']\t{'address_line_1':'Googleplex','city':'Mountain View','id':'1','country':'US','postal_code':'94043'}\t{}\t42\t192.168.1.50\n" ++
        "UPDATE\tpublic.addresses\t1\t['address_line_1','city','postal_code']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t42\t192.168.1.50\n" ++
        "INSERT\tpublic.addresses\t1\t['address_line_1','city','id','country','postal_code']\t{}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'1','country':'US','postal_code':'95014'}\t42\t192.168.1.50\n" ++
        "DELETE\tpublic.addresses\t2\t['address_line_1','city','id','country','postal_code']\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','id':'2','country':'US','postal_code':'95014'}\t{}\t42\t192.168.1.50\n" ++
        "UPDATE\tpublic.addresses\t2\t['address_line_1','city','postal_code']\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t42\t192.168.1.50\n" ++
        "INSERT\tpublic.addresses\t2\t['address_line_1','city','id','country','postal_code']\t{}\t{'address_line_1':'Googleplex','city':'Mountain View','id':'2','country':'US','postal_code':'94043'}\t42\t192.168.1.50\n",
        ch_assert_result.stdout,
    );

    const term = try terminateChildProcess(io, &child);

    try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, term);
}

// test "correct shutdown" {
// }
//
// test "do data loss on shutdown and boot" {
// }
