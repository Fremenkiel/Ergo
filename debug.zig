const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const process = std.process;
const debug = std.debug;

const pg = @import("pg");

const Conn = pg.conn.Conn;
const PgConfig = pg.PgConfig;

pub fn main(init: process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;

        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro_scram_sha256",
        .password = "12345678",
            .application_name = "Ergo test",
            .startup_parameters = .init(allocator),
        };

        var conn = try Conn.init(io, allocator, opts);
        defer conn.deinit();
        try conn.auth();
}
