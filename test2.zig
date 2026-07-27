const std = @import("std");

pub fn main() void {
    var arr: [10]u8 = undefined;
    var buf: []u8 = &arr;
    const bind_payload_len: usize = 42;
    std.mem.writeInt(u32, buf[1..5], @intCast(bind_payload_len), .big);
    std.debug.print("{any}\n", .{buf[1..5]});
}
