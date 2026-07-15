const std = @import("std");
const Io = std.Io;

const pg = @import("pg");
const ch = @import("ch");
const types = @import("types");

const PgClient = @import("pg_client").PgClient;
const ChClient = @import("ch_client").ChClient;
const WalProcessor = @import("wal_processor.zig").WalProcessor;

pub fn main(init: std.process.Init) !void {
    std.debug.print("Initializing database connection\n", .{});

    const allocator: std.mem.Allocator = init.arena.allocator();
    const io = init.io;

    var pg_client = try PgClient.init(io, allocator, .{
        .connect = .{  
            .port = 5432,
            .host = "localhost",
        },
        .auth = .{
            .username = "db_rp",
            .password = "12345678",
            .database = "db",
            .timeout = 10_000,
        } 
    });
    errdefer pg_client.deinit();

    var ch_client = ChClient.init(allocator, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    });
    defer ch_client.deinit(io);

    try ch_client.connect(io);

    std.debug.print("calling BulkInsert.init\n", .{});

    var processor = WalProcessor(PgClient, ChClient){ 
        .pg_client = &pg_client, 
        .ch_client = &ch_client,
        .last_write_timestamp = std.Io.Clock.real.now(io),
        .allocator = allocator,
        .io = io,
    };

    try processor.startStreaming();
}
