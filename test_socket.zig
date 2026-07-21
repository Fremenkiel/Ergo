const std = @import("std");

pub fn main() !void {
    const io = try std.Io.init(std.heap.page_allocator, .{});
    
    // just dummy
    _ = io;
}
