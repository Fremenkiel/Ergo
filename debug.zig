const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const process = std.process;
const debug = std.debug;

const assert = debug.assert;

const phantom = @import("phantom");
const Phantom = phantom.Phantom;

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;

    var database = try phantom.Phantom.init(allocator, io, "./test/template.db", "/tmp/ergo");
    defer database.deinit();

    try database.spawn();

    const socket = try allocator.print("{s}/.s.PGSQL.5432", .{ database.socket_path });
    defer allocator.free(socket);

    std.log.debug("{s}", .{ socket });

    const addr: Io.net.UnixAddress = try .init(socket);
    const stream = addr.connect(io) catch |err| {
        std.debug.print("connect failed: {s}\n", .{@errorName(err)});
        return err;
    };
    defer stream.close(io);

    std.debug.print("connected successfully\n", .{});
}
