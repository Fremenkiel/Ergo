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
const WalProcessor = @import("wal_processor.zig").WalProcessor;
const types = @import("types.zig");

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

    const user_env_key = if (builtin.os.tag == .windows) "USERNAME" else "USER";
    const os_user = init.environ_map.get(user_env_key);
    defer allocator.free(os_user.?);

    const is_sync_test = init.environ_map.get("ERGO_TEST_SYNC");
    defer allocator.free(is_sync_test.?);

    try Io.File.stdout().writeStreamingAll(io, "READY\n");

    var pg_client = try PgClient.init(io, allocator, .{
        .connect = .{  
            .port = 5432,
            .host = "localhost",
        },
        .auth = .{
            .username = "db_rp",
            .password = "12345678",
            .database = "db",
            .timeout = 10_000,
        } 
    });
    defer pg_client.deinit();

    var ch_client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    }, os_user.?);
    defer ch_client.deinit();

    try ch_client.connect();
    defer ch_client.disconnect();

    var processor = WalProcessor(PgClient, ChClient){ 
        .pg_client = &pg_client, 
        .ch_client = &ch_client,
        .last_write_timestamp = std.Io.Clock.real.now(io),
        .allocator = allocator,
        .io = io,
        .is_sync_test = is_sync_test != null
    };
    defer processor.deinit();

    try processor.startStreaming(&is_shutting_down);

    std.log.info("Shutdown signal caught. Exiting cleanly.\n", .{});

    std.process.exit(0);
}

fn setupChildProcess(io: Io) !std.process.Child {
    const argv = &[_][]const u8{"./zig-out/bin/ergo"};
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdout = .pipe,
    });

    var buffer: [1024]u8 = undefined;
    var reader = child.stdout.?.reader(io, &buffer);
    var r = &reader.interface;

    while (true) {
        if (try r.takeDelimiter('\n')) |line| {
            if (std.mem.indexOf(u8, line, "READY") != null) {
                break;
            }
        } else {
            return error.AppCrashedBeforeReady;
        }
    }

    return child;
}

fn terminateChildProcess(io: Io, child: *std.process.Child) !std.process.Child.Term {
    if (child.id) |pid| {
        try std.posix.kill(pid, std.posix.SIG.TERM);
    } else {
        return error.ChildNotStarted;
    }

    return try child.wait(io);
}

test "test:main:beforeAll" {
    std.testing.refAllDecls(@This());
}

test "main ensure full transaction sync on interupt" {
    const io = testing.io;

    var child = try setupChildProcess(io);
    const term = try terminateChildProcess(io, &child);

    try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, term);
}

test "making sure full commits are logged without interupt" {
}

test "correct shutdown" {
}

test "do data loss on shutdown and boot" {
}
