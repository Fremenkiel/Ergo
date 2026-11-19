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
    _ = init;
}
