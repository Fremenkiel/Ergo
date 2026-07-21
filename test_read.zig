const std = @import("std");
pub fn main() !void {
    const child: std.process.Child = undefined;
    const a = @TypeOf(child.stderr.?.fd);
    const b: bool = a;
    _ = b;
}
