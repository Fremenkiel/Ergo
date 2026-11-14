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

    var database = try phantom.Phantom.init(allocator, io, "./test/template.db", "/tmp/ergo", .{
        .log_level = .{
            .fatal = .warn
        },
    });
    defer database.deinit();

    try database.spawn();

    const socket = try allocator.print("{s}/.s.PGSQL.5432", .{ database.socket_path });
    defer allocator.free(socket);

    var stream = try Stream.init(allocator, io, .{ 
        .host = socket,
        .port = null,
        .database = "db",
        .application_name = "Ergo test",
        .startup_parameters = null 
    });
    defer allocator.free(stream.buffer);

    const socket_handle = stream.stream.socket.handle;
    defer Io.Threaded.closeFd(socket_handle);

    const pipes = try Io.Threaded.pipe2(.{ .CLOEXEC = true });
    defer {
        Io.Threaded.closeFd(pipes[0]);
        Io.Threaded.closeFd(pipes[1]);
    }

    stream.stream.socket.handle = pipes[0];

    var buf: [256]u8 = undefined;

    assert(error.Timeout == stream.readWithTimeout(&buf, 250));
}
