const std = @import("std");

test "test with std log warn" {
    std.log.warn("hello warn", .{});
    try std.testing.expect(true);
}

test "test with std log err" {
    std.log.err("hello err", .{});
    try std.testing.expect(true);
}
