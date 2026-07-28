const std = @import("std");

const testing = std.testing;

const protocol = @import("protocol.zig");

pub const packet = @import("packet.zig");
pub const conn = @import("conn.zig");

pub const Pool = @import("pool.zig").Pool;
pub const Error = @import("error.zig").Error;

pub const Conn = conn.Conn;

const t = @import("t.zig");
test "tests:beforeAll" {
    const allocator = testing.allocator;
    const io = testing.io;

    try t.setup(allocator, io);
    std.testing.refAllDecls(@This());
}
