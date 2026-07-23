const MyStruct = @import("test_struct.zig").MyStruct;
pub fn main() void {
    const m = MyStruct{ .public_field = 1, ._private_field = 2 };
    _ = m._private_field;
}
