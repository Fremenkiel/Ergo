const std = @import("std");

pub fn main() !void {
    var buf: [10]u8 = undefined;
    var reader = std.Io.Reader.fixed(&buf);
    reader.seek = 0;
    std.debug.print("seek={}, end={}\n", .{reader.seek, reader.end});
}
