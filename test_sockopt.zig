const std = @import("std");

pub fn main() !void {
    const timeout_ms: u32 = 250;
    const timeout = std.posix.timeval{
        .sec = timeout_ms / 1000,
        .usec = @intCast((timeout_ms % 1000) * 1000),
    };

    std.debug.print("Set: sec: {}, usec: {}\n", .{timeout.sec, timeout.usec});
}
