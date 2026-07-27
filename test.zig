const std = @import("std");

pub fn main() void {
    var buf: [10]u8 = undefined;
    const bind_payload_len: usize = 42;
    std.mem.writeInt(u32, buf[1..5], @intCast(bind_payload_len), .big);
    std.debug.print("{any}\n", .{buf[1..5]});
}
