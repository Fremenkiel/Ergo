const std = @import("std");
pub fn main() void {
    const VTable = std.Io.Writer.VTable;
    inline for (@typeInfo(VTable).@"struct".fields) |f| {
        std.debug.print("{s}\n", .{f.name});
    }
}
