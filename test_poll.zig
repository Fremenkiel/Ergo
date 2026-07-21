const std = @import("std");

pub fn main() !void {
    var buf: [1024]u8 = undefined;
    const bytes = try std.posix.read(0, &buf);
    _ = bytes;
}
