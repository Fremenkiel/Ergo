const std = @import("std");
test "test debug print" {
    std.debug.print("hello\n", .{});
}
