const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const process = std.process;
const debug = std.debug;

const assert = debug.assert;

const ssl = @import("ssl");

const phantom = @import("phantom");
const Phantom = phantom.Phantom;
const Pg = @import("pg");
const Stream = Pg.stream.Stream;
const StartupMessage = Pg.protocol.StartupMessage;

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;

    var database = try phantom.Phantom.init(allocator, io, "./test/template.db", "/tmp/ergo");
    defer database.deinit();

    try database.spawn();

    const socket = try allocator.print("{s}/.s.PGSQL.5432", .{ database.socket_path });
    defer allocator.free(socket);

    std.log.debug("{s}", .{ socket });

    const opts: Pg.PgConfig = .{
        .host = socket,
        .port = null,
        .database = "db",
        .application_name = "Ergo test",
        .startup_parameters = null 
    };

    var stream = try Stream.init(allocator, io, opts);
    defer stream.deinit(allocator);

    var buf: [4096]u8 = undefined;

    try StartupMessage.write(allocator, &stream, opts);

    _ = try stream.read(&buf);

    assert('R' == buf[0]);
}
