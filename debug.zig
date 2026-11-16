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

const Config = struct {
    host: []const u8 = undefined,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();

    const config = try allocator.create(Config);
    defer allocator.destroy(config);

    config.* = .{};

    std.log.debug("{d}", .{ config.host.len });
}
