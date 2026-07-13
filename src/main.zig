const std = @import("std");
const Io = std.Io;

const pg = @import("pg");
const ch = @import("ch");
const types = @import("types.zig");

const PgClient = @import("pg_client.zig").PgClient;
const ChClient = @import("ch_client.zig").ChClient;

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

    var client = ChClient.init(allocator, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    });
    defer client.deinit(io);

    try client.connect(io);

    std.debug.print("calling BulkInsert.init\n", .{});

    try pg_client.startWALReader();

    var log_array = std.ArrayList([]types.AuditEntry).empty;
    defer log_array.deinit(allocator);

    const duration = std.Io.Duration.fromSeconds(1);
    var last_write_timestamp = std.Io.Clock.real.now(io);
    var uncommited_changes: bool = false;
    var transaction_array = std.ArrayList(types.AuditEntry).empty;
    errdefer transaction_array.deinit(allocator);
    while (true) {
        const response = try pg_client.readWAL();
        if (response.entry) |entry| {
            try transaction_array.append(allocator, entry);
        }

        if (response.commit_timestamp != null) {
            for (transaction_array.items) |*row| {
                row.event_time = response.commit_timestamp.?;
            }
            try log_array.append(allocator, try allocator.dupe(types.AuditEntry, transaction_array.items));

            uncommited_changes = false;
            transaction_array.clearAndFree(allocator);
        } else { 
            uncommited_changes = true;
        }

        if (last_write_timestamp.addDuration(duration).toMilliseconds() < std.Io.Clock.real.now(io).toMilliseconds() and !uncommited_changes and log_array.items.len > 0) {
            try writeLog(io, allocator, &client, log_array.items);

            log_array.clearRetainingCapacity();
            last_write_timestamp = std.Io.Clock.real.now(io);
        }
    }
}

fn writeLog(io: std.Io, allocator: std.mem.Allocator, client: *ChClient, data: [][]types.AuditEntry) !void {
    var bulk = try ch.BulkInsert.init(allocator, "entries", &columns, 1000);
    defer bulk.deinit();

    std.debug.print("Initialized bulk insert\n", .{});

    // Enable LZ4 compression
    bulk.setCompression(.LZ4);

    client.startInsert(io, "INSERT INTO entries FORMAT Native") catch |err| {
        if (err == error.QueryFailed) {
            std.debug.print("Query failed: {s}\n", .{client.last_error.?.message});
        }
        return err;
    };

    std.debug.print("Sending data\n", .{});

    var buf = std.ArrayList(ch.bulk_insert.Value).empty;
    defer buf.deinit(allocator);

    var old_values = std.StringHashMap([]const u8).init(allocator);
    defer old_values.deinit();

    var new_values = std.StringHashMap([]const u8).init(allocator);
    defer new_values.deinit();

    var i: u32 = 0;
    while (i < data.len) : (i += 1) {
        for (data[i]) |row| {
            buf.clearRetainingCapacity();
            old_values.clearRetainingCapacity();
            new_values.clearRetainingCapacity();

            try buf.ensureUnusedCapacity(allocator, row.changed_columns.capacity());
            if (row.old_values.capacity() > 0) { try old_values.ensureUnusedCapacity(row.changed_columns.capacity()); }
            if (row.new_values.capacity() > 0) { try new_values.ensureUnusedCapacity(row.changed_columns.capacity()); }

            var it = row.changed_columns.iterator();
            while (it.next()) |col| {
                if (!col.value_ptr.*.has_changes) continue;

                buf.appendAssumeCapacity(.{ .String = col.key_ptr.* });

                if (row.old_values.capacity() > 0) {
                    const val = row.old_values.get(col.key_ptr.*);
                    try old_values.put(col.key_ptr.*, val.?);
                }

                if (row.new_values.capacity() > 0) {
                    const val = row.new_values.get(col.key_ptr.*);
                    try new_values.put(col.key_ptr.*, val.?);
                }
            }

            const values = [_]ch.bulk_insert.Value{
                .{ .DateTime64 = row.event_time },
                .{ .UInt64 = row.transaction_id },
                .{ .String = row.user_id},
                .{ .LowCardinality = row.table_name },
                .{ .Enum8 = row.action },
                .{ .String = row.primary_key },
                .{ .Array = buf.items },
                .{ .Map = old_values },
                .{ .Map = new_values },
                .{ .IPv4 = row.ip_address },
            };

            if (try bulk.addRow(&values)) {
                bulk.flush(io, client.stream.?) catch |err| {
                    std.debug.print("flush failed: {}\n", .{err});
                    client.processQueryResponse(io) catch {};
                    if (client.last_error) |e| {
                        std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
                    }
                    return err;
                };
            }
        }
    }

    // Flush any remaining rows
    bulk.flush(io, client.stream.?) catch |err| {
                std.debug.print("flush failed: {}\n", .{err});
                client.processQueryResponse(io) catch {};
                if (client.last_error) |e| {
                    std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
                }
                return err;
            };

    std.debug.print("Executed insert\n", .{});

    client.closeStream(io) catch |err| {
        std.debug.print("close failed: {}\n", .{err});
        if (client.last_error) |e| {
            std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
        }
        return err;
    };

    std.debug.print("Closed stream\n", .{});

    if (client.last_error) |err| {
        std.debug.print("Insert failed: {s}\n", .{err.message});
    }


}
