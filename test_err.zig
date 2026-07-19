const std = @import("std");
test "test err log" {
    std.log.err("this should fail the test", .{});
}
test "test warn log" {
    std.log.warn("this should not fail?", .{});
}
