const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const process = std.process;
const debug = std.debug;

const pg = @import("pg");

const Conn = pg.conn.Conn;
const PgConfig = pg.PgConfig;

pub fn main() !void {
    const timestamp: u64 = 837427084349134;

    const ts = Timestamp.decode(timestamp);

    std.debug.print("s: {d}\n", .{ts.toNanoseconds()});
}

pub const Timestamp = struct {
    pub fn decode(pg_wal_us: u64) Io.Timestamp {
        const seconds_between_epochs: i96 = 946_684_800;
        const ns_between_epochs: i96 = seconds_between_epochs * 1_000_000_000;

        const unix_ns: i96 = @as(i96, @intCast(pg_wal_us)) * 1000 + ns_between_epochs;

        return std.Io.Timestamp.fromNanoseconds(unix_ns);
    }
};
