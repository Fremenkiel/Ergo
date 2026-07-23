const std = @import("std");
pub const MyStruct = struct {
    public_field: u32,
    _private_field: u32,
};
pub fn main() void {
    const m = MyStruct{ .public_field = 1, ._private_field = 2 };
    _ = m;
}
