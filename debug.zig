const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const process = std.process;
const debug = std.debug;

const pg = @import("pg");
const types = @import("types");
const t = @import("src/t.zig");

const WalStreamT = @import("src/wal_stream.zig").WalStreamT;

const Conn = pg.conn.Conn;
const PgConfig = pg.PgConfig;

var mock_is_shutting_down = std.atomic.Value(bool).init(false);

pub fn main(init: process.Init) !void {
    _ = init;
}
