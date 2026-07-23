const std = @import("std");
const lib = @import("lib.zig");

pub const Row = lib.Row;
pub const Conn = lib.Conn;
pub const Reader = lib.Reader;
pub const Pool = lib.Pool;
pub const Stmt = lib.Stmt;
pub const Result = lib.Result;
pub const Iterator = lib.Iterator;
pub const QueryRow = lib.QueryRow;
pub const Binary = lib.Binary;

pub const Listener = @import("listener.zig").Listener;

pub const types = lib.types;
pub const Cidr = types.Cidr;
pub const Numeric = types.Numeric;
pub const Error = lib.proto.Error;
pub const printSSLError = lib.printSSLError;

const t = lib.testing;
test "tests:beforeAll" {
    try t.setup();
    std.testing.refAllDecls(@This());
}
