const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const pg = @import("pg");

const types = @import("types.zig");
const pg_client = @import("pg_client.zig");

pub fn WalProcessor(comptime PgClient: type, comptime ChClient: type) type {
    return struct {
        duration: Io.Duration = Io.Duration.fromSeconds(1),
        last_write_timestamp: Io.Timestamp,
        log_array: std.ArrayList(types.AuditEntry) = .empty,
        transaction_array: std.ArrayList(types.AuditEntry) = .empty,
        is_sync_test: bool,

        allocator: mem.Allocator,
        io: Io,

        pg_client: *PgClient,
        ch_client: *ChClient,

        pub fn deinit(self: *@This()) void {
            for (self.log_array.items) |*entry| entry.deinit(self.allocator);
            self.log_array.deinit(self.allocator);

            for (self.transaction_array.items) |*entry| entry.deinit(self.allocator);
            self.transaction_array.deinit(self.allocator);
        }

        pub fn startStreaming(self: *@This(), flag: *std.atomic.Value(bool)) !void {
            self.pg_client.startWALReader() catch |err| switch (err) {
                pg_client.PgClientError.WalConnectionNotInitialized => {
                    self.pg_client.*.wal_conn = try PgClient.createWalConn(self.allocator, self.io, self.pg_client.*.conn_opts);
                    try self.pg_client.startWALReader();
                },
                else => return err,
            };


            // TODO: check lengts, time elapsed, transaction_array to make sure nothing is missing.
            while (!(flag.load(.seq_cst) and self.log_array.items.len == 0)) {
                var response = try self.pg_client.readWAL();

                if (response.eof) {
                    try self.ch_client.writeLog(self.log_array.items);

                    return;
                }

                if (response.entry) |entry| {
                    try self.transaction_array.append(self.allocator, entry);
                    // remove linking
                    response.entry = null;
                }

                if (response.commit_timestamp != null) {
                    for (self.transaction_array.items) |*row| {
                        row.event_time = response.commit_timestamp.?;
                    }
                    try self.log_array.appendSlice(self.allocator, self.transaction_array.items);
                    self.transaction_array.clearRetainingCapacity();

                    // Test hook
                    if (self.log_array.items.len > 0) {
                        if (std.mem.eql(u8, "test_sync_marker", self.log_array.items[self.log_array.items.len - 1].table_name)) {
                            _ = self.log_array.swapRemove(self.log_array.items.len - 1);
                            if (self.is_sync_test) {
                                try std.Io.File.stdout().writeStreamingAll(self.io, "SYNC_MARKER_REACHED\n");

                                while (!flag.load(.seq_cst)) {
                                    try Io.sleep(self.io, Io.Duration{ .nanoseconds = 10 * std.time.ns_per_ms }, .real);
                                }
                            }
                        }
                    }
                }

                if (self.last_write_timestamp.addDuration(self.duration).toMilliseconds() < Io.Clock.real.now(self.io).toMilliseconds() and self.log_array.items.len > 0) {
                    try self.ch_client.writeLog(self.log_array.items);
                    for (self.log_array.items) |*entry| entry.deinit(self.allocator);
                    self.log_array.clearRetainingCapacity();

                    self.last_write_timestamp = Io.Clock.real.now(self.io);
                }
            }
        }
    };
}

var mock_is_shutting_down = std.atomic.Value(bool).init(false);

const MockPgClient = struct {
    allocator: mem.Allocator,
    responses: []pg_client.ReadResponse,
    read_response_index: ?u8,

    conn_opts: void,
    wal_conn: *bool,

    pub fn init(allocator: mem.Allocator, responses: []pg_client.ReadResponse) !MockPgClient {
        const conn = try allocator.create(bool);
        conn.* = true;
        return .{
            .allocator = allocator,
            .responses = responses,
            .read_response_index = null,
            .conn_opts = {},
            .wal_conn = conn,
        };
    }

    pub fn deinit(self: *@This()) void {
        self.allocator.destroy(self.wal_conn);
    }

    pub fn startWALReader(self: *@This()) !void {
        if (!self.wal_conn.*) return pg_client.PgClientError.WalConnectionNotInitialized;
    }

    pub fn readWAL(self: *@This()) !pg_client.ReadResponse {
        if (self.read_response_index == null) {
            self.read_response_index = 0;
        } else {
            self.read_response_index.? += 1;
        }

        if (self.read_response_index.? == self.responses.len) {
            return pg_client.EOFReadResponse;
        }

        return self.responses[self.read_response_index.?];
    }

    pub fn createWalConn(allocator: mem.Allocator, io: Io, opts: anytype) !*bool {
        _ = io;
        _ = opts;

        const new_conn = try allocator.create(bool);
        new_conn.* = true;
        return new_conn;
    }
};

const MockChClient = struct {
    allocator: mem.Allocator,
    written_logs: std.ArrayList(types.AuditEntry) = .empty,
    log_array: std.ArrayList(types.AuditEntry) = .empty,

    pub fn init(allocator: mem.Allocator) MockChClient {
        return .{.allocator = allocator};
    }

    pub fn deinit(self: *@This()) void {
        for (self.written_logs.items) |*item| item.deinit(self.allocator);
        self.written_logs.deinit(self.allocator);
        for (self.log_array.items) |*entry| entry.deinit(self.allocator);
        self.log_array.deinit(self.allocator);
    }

    pub fn writeLog(self: *@This(), entries: []types.AuditEntry) !void {
        var copy_slice = try self.allocator.alloc(types.AuditEntry, entries.len);
        for (entries, 0..) |entry, i| {
            copy_slice[i] = entry;

            copy_slice[i].table_name = try self.allocator.dupe(u8, entry.table_name);

            copy_slice[i].changed_columns = try entry.changed_columns.clone();
            copy_slice[i].new_values = try entry.new_values.clone(self.allocator);
            copy_slice[i].old_values = try entry.old_values.clone(self.allocator);
        }

        try self.written_logs.appendSlice(self.allocator, copy_slice);
        self.allocator.free(copy_slice);
    }
};

test "startStreaming read and parse correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var changed_columns = std.StringHashMap(types.ChangedColumns).init(allocator);
    try changed_columns.ensureUnusedCapacity(1);

    try changed_columns.put("id", .{ .has_changes = false, .value = "1" });
    try changed_columns.put("address_line_1", .{ .has_changes = true, .value = "Googleplex" });
    try changed_columns.put("address_line_2", .{ .has_changes = false, .value = "" });
    try changed_columns.put("postal_code", .{ .has_changes = true, .value = "94043" });
    try changed_columns.put("city", .{ .has_changes = true, .value = "Mountain View" });
    try changed_columns.put("country", .{ .has_changes = false, .value = "US" });

    var new_values = std.StringHashMapUnmanaged([]const u8).empty;
    try new_values.ensureUnusedCapacity(allocator, 6);

    try new_values.put(allocator, "id", "1");
    try new_values.put(allocator, "address_line_1", "1 Apple Park Way");
    try new_values.put(allocator, "address_line_2", "");
    try new_values.put(allocator, "postal_code", "95014");
    try new_values.put(allocator, "city", "Cupertino");
    try new_values.put(allocator, "country", "US");

    var old_values = std.StringHashMapUnmanaged([]const u8).empty;
    try old_values.ensureUnusedCapacity(allocator, 6);

    try old_values.put(allocator, "id", "1");
    try old_values.put(allocator, "address_line_1", "Googleplex");
    try old_values.put(allocator, "address_line_2", "");
    try old_values.put(allocator, "postal_code", "94043");
    try old_values.put(allocator, "city", "Mountain View");
    try old_values.put(allocator, "country", "US");

    var res = [_]pg_client.ReadResponse{
        .{ 
            .entry = .{
                .event_time = undefined,
                .table_name = try allocator.dupe(u8, "addresses"),
                .new_values = new_values,
                .old_values = old_values,
                .action = 1,
                .changed_columns = changed_columns,
                .transaction_id = 793,
                .user_id = "42",
                .ip_address = "192.168.1.50",
                .primary_key = "1",
            },
            .commit_timestamp = 10
        },
    };

    var mock_pg_client = try MockPgClient.init(allocator, &res);
    defer mock_pg_client.deinit();

    var mock_ch_client = MockChClient.init(allocator);
    defer mock_ch_client.deinit();

    var processor = WalProcessor(MockPgClient, MockChClient){ 
        .pg_client = &mock_pg_client, 
        .ch_client = &mock_ch_client,
        .last_write_timestamp = Io.Clock.real.now(io).subDuration(
        Io.Duration.fromSeconds(2)
    ),
        .allocator = allocator,
        .io = io,
        .is_sync_test = false,
    };
    defer processor.deinit();

    try processor.startStreaming(&mock_is_shutting_down);

    try testing.expectEqual(1, mock_ch_client.written_logs.items.len);
    try testing.expectEqual(1, mock_ch_client.written_logs.items[0].action);
    try testing.expectEqualStrings("addresses", mock_ch_client.written_logs.items[0].table_name);
    try testing.expectEqualStrings("42", mock_ch_client.written_logs.items[0].user_id);
    try testing.expectEqualStrings("192.168.1.50", mock_ch_client.written_logs.items[0].ip_address);
    try testing.expectEqualStrings("1", mock_ch_client.written_logs.items[0].primary_key);
    try testing.expectEqual(10, mock_ch_client.written_logs.items[0].event_time);
    try testing.expectEqual(793, mock_ch_client.written_logs.items[0].transaction_id);

    try testing.expectEqual(6, mock_ch_client.written_logs.items[0].changed_columns.count());
    try testing.expectEqual(6, mock_ch_client.written_logs.items[0].new_values.count());
    try testing.expectEqual(6, mock_ch_client.written_logs.items[0].old_values.count());

    const id_changed_column = mock_ch_client.written_logs.items[0].changed_columns.get("id").?;
    try testing.expectEqualStrings("1", id_changed_column.value);
    try testing.expectEqual(false, id_changed_column.has_changes);
    const address_line_1_changed_column = mock_ch_client.written_logs.items[0].changed_columns.get("address_line_1").?;
    try testing.expectEqualStrings("Googleplex", address_line_1_changed_column.value);
    try testing.expectEqual(true, address_line_1_changed_column.has_changes);
    const address_line_2_changed_column = mock_ch_client.written_logs.items[0].changed_columns.get("address_line_2").?;
    try testing.expectEqualStrings("", address_line_2_changed_column.value);
    try testing.expectEqual(false, address_line_2_changed_column.has_changes);
    const postal_code_changed_column = mock_ch_client.written_logs.items[0].changed_columns.get("postal_code").?;
    try testing.expectEqualStrings("94043", postal_code_changed_column.value);
    try testing.expectEqual(true, postal_code_changed_column.has_changes);
    const city_changed_column = mock_ch_client.written_logs.items[0].changed_columns.get("city").?;
    try testing.expectEqualStrings("Mountain View", city_changed_column.value);
    try testing.expectEqual(true, city_changed_column.has_changes);
    const country_changed_column = mock_ch_client.written_logs.items[0].changed_columns.get("country").?;
    try testing.expectEqualStrings("US", country_changed_column.value);
    try testing.expectEqual(false, country_changed_column.has_changes);

    const id_new_values = mock_ch_client.written_logs.items[0].new_values.get("id").?;
    try testing.expectEqualStrings("1", id_new_values);
    const address_line_1_new_values = mock_ch_client.written_logs.items[0].new_values.get("address_line_1").?;
    try testing.expectEqualStrings("1 Apple Park Way", address_line_1_new_values);
    const address_line_2_new_values = mock_ch_client.written_logs.items[0].new_values.get("address_line_2").?;
    try testing.expectEqualStrings("", address_line_2_new_values);
    const postal_code_new_values = mock_ch_client.written_logs.items[0].new_values.get("postal_code").?;
    try testing.expectEqualStrings("95014", postal_code_new_values);
    const city_new_values = mock_ch_client.written_logs.items[0].new_values.get("city").?;
    try testing.expectEqualStrings("Cupertino", city_new_values);
    const country_new_values = mock_ch_client.written_logs.items[0].new_values.get("country").?;
    try testing.expectEqualStrings("US", country_new_values);

    const id_old_values = mock_ch_client.written_logs.items[0].old_values.get("id").?;
    try testing.expectEqualStrings("1", id_old_values);
    const address_line_1_old_values = mock_ch_client.written_logs.items[0].old_values.get("address_line_1").?;
    try testing.expectEqualStrings("Googleplex", address_line_1_old_values);
    const address_line_2_old_values = mock_ch_client.written_logs.items[0].old_values.get("address_line_2").?;
    try testing.expectEqualStrings("", address_line_2_old_values);
    const postal_code_old_values = mock_ch_client.written_logs.items[0].old_values.get("postal_code").?;
    try testing.expectEqualStrings("94043", postal_code_old_values);
    const city_old_values = mock_ch_client.written_logs.items[0].old_values.get("city").?;
    try testing.expectEqualStrings("Mountain View", city_old_values);
    const country_old_values = mock_ch_client.written_logs.items[0].old_values.get("country").?;
    try testing.expectEqualStrings("US", country_old_values);
}
