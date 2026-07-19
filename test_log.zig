const std = @import("std");

test "test with debug print" {
    std.debug.print("hello debug\n", .{});
    try std.testing.expect(true);
}

test "test with std log" {
    std.log.info("hello log", .{});
    try std.testing.expect(true);
}
