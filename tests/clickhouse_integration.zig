const std = @import("std");

const ChClient = @import("ch_client").ChClient;
const ch = @import("ch");

const iterations = 500;

const columns = [10]ch.bulk_insert.ColumnDef{
    .{ .name = "event_time", .type_str = "DateTime64" },
    .{ .name = "transaction_id", .type_str = "UInt64" },
    .{ .name = "user_id", .type_str = "String" },
    .{ .name = "table_name", .type_str = "LowCardinality(String)" },
    .{ .name = "action", .type_str = "Enum8('INSERT' = 1, 'UPDATE' = 2, 'DELETE' = 3)" },
    .{ .name = "primary_key", .type_str = "String" },
    .{ .name = "changed_columns", .type_str = "Array(String)" },
    .{ .name = "old_values", .type_str = "Map(String, String)" },
    .{ .name = "new_values", .type_str = "Map(String, String)" },
    .{ .name = "ip_address", .type_str = "IPv4" },
};


pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;
    var timer = std.Io.Clock.awake.now(io);


    // Hot connection
    std.debug.print("starting hot test...\n", .{});
    try hotConnection(io, allocator);
    const hot_time = timer.untilNow(io, .awake);

    // Reconnect
    std.debug.print("starting cold test...\n", .{});
    try coldConnection(io, allocator);
    const cold_time = timer.untilNow(io, .awake);

    std.debug.print("Warm total: {} ns | Cold total: {} ns\n", .{hot_time, cold_time});
    // std.debug.print("Total bytes requested: {}\n", .{init.arena.total_requested_bytes});
}

fn hotConnection(io: std.Io, allocator: std.mem.Allocator) !void {
    var ch_client = ChClient.init(allocator, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    });

    try ch_client.connect(io);

    std.debug.print("calling BulkInsert.init\n", .{});

    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        try insertBulk(io, allocator, &ch_client);
        ch_client.closeStream(io) catch |err| {
            std.debug.print("close failed: {}\n", .{err});
            if (ch_client.last_error) |e| {
                std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
            }
            return err;
        };
    }

    ch_client.deinit(io);

    std.debug.print("Closed stream\n", .{});
}

fn coldConnection(io: std.Io, allocator: std.mem.Allocator) !void {
    var ch_client = ChClient.init(allocator, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    });


    std.debug.print("calling BulkInsert.init\n", .{});

    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        try ch_client.connect(io);
        try insertBulk(io, allocator, &ch_client);
        ch_client.closeStream(io) catch |err| {
            std.debug.print("close failed: {}\n", .{err});
            if (ch_client.last_error) |e| {
                std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
            }
            return err;
        };
        try ch_client.disconnect(io);
    }

    ch_client.deinit(io);

    std.debug.print("Closed stream\n", .{});
}

fn insertBulk(io: std.Io, allocator: std.mem.Allocator, ch_client: *ChClient) !void {
    var bulk = try ch.BulkInsert.init(allocator, "entries", &columns, 1000);
    defer bulk.deinit();

    var changes_columns = try std.ArrayList(ch.bulk_insert.Value).initCapacity(allocator, 3);
    changes_columns.appendAssumeCapacity(.{ .String = "name" });
    changes_columns.appendAssumeCapacity(.{ .String = "gender" });
    changes_columns.appendAssumeCapacity(.{ .String = "age" });

    var old_values = std.StringHashMap([]const u8).init(allocator);
    try old_values.ensureUnusedCapacity(3);
    old_values.putAssumeCapacity("name", "kevin");
    old_values.putAssumeCapacity("gender", "1");
    old_values.putAssumeCapacity("age", "36");

    var new_values = std.StringHashMap([]const u8).init(allocator);
    try new_values.ensureUnusedCapacity(3);
    new_values.putAssumeCapacity("name", "james");
    new_values.putAssumeCapacity("gender", "2");
    new_values.putAssumeCapacity("age", "45");

    std.debug.print("Initialized bulk insert\n", .{});

    bulk.setCompression(.LZ4);

    ch_client.startInsert(io, "INSERT INTO entries FORMAT Native") catch |err| {
        if (err == error.QueryFailed) {
            std.debug.print("Query failed: {s}\n", .{ch_client.last_error.?.message});
        }
        return err;
    };

    std.debug.print("Sending data\n", .{});

    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        const values = [_]ch.bulk_insert.Value{
            .{ .DateTime64 = 500 },
            .{ .UInt64 = 10 },
            .{ .String = "test_user" },
            .{ .LowCardinality = "users" },
            .{ .Enum8 = 1 },
            .{ .String = "1" },
            .{ .Array = changes_columns.items },
            .{ .Map = old_values },
            .{ .Map = new_values },
            .{ .IPv4 = "127.0.0.1" },
        };

        _ = try bulk.addRow(&values);
    }

    bulk.flush(io, ch_client.stream.?) catch |err| {
        std.debug.print("flush failed: {}\n", .{err});
        ch_client.processQueryResponse(io) catch {};
        if (ch_client.last_error) |e| {
            std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
        }
        return err;
    };

    std.debug.print("Executed insert\n", .{});

    changes_columns.deinit(allocator);
    old_values.deinit();
    new_values.deinit();
    bulk.deinit();

    if (ch_client.last_error) |err| {
        std.debug.print("Insert failed: {s}\n", .{err.message});
    }
}
