const std = @import("std");
const mem = std.mem;

pub fn main() !void {
        var len_buf: [4]u8 = undefined;
        mem.writeInt(i32, &len_buf, 4, .big);

    std.debug.print("{x}{x}\n", .{ "c", len_buf});
}
