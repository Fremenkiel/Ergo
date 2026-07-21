const std = @import("std");

pub fn main() void {
    const t = std.posix.timeval{
        .sec = 0,
        .usec = 250_000,
    };
    std.debug.print("sec type: {s}, usec type: {s}, size: {}\n", .{@typeName(@TypeOf(t.sec)), @typeName(@TypeOf(t.usec)), @sizeOf(std.posix.timeval)});
}
