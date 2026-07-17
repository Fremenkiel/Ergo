const std = @import("std");
const types = @import("types");

const pg = @import("pg");
const pg_client = @import("pg_client");

pub fn WalProcessor(comptime PgClient: type, comptime ChClient: type) type {
    return struct {
        duration: std.Io.Duration = std.Io.Duration.fromSeconds(1),
        last_write_timestamp: std.Io.Timestamp,
        uncommited_changes: bool = false,
        log_array: types.LogArray = .empty,

        allocator: std.mem.Allocator,
        io: std.Io,

        pg_client: *PgClient,
        ch_client: *ChClient,

        pub fn deinit(self: *@This()) void {
            self.log_array.deinit(self.allocator);
        }

        pub fn startStreaming(self: *@This()) !void {
            self.pg_client.startWALReader() catch |err| switch (err) {
                pg_client.PgClientError.WalConnectionNotInitialized => {
                    self.pg_client.*.wal_conn = try PgClient.createWalConn(self.allocator, self.io, self.pg_client.*.conn_opts);
                    try self.pg_client.startWALReader();
                },
                else => return err,
            };

            var transaction_array: std.ArrayList(types.AuditEntry) = .empty;

            errdefer transaction_array.deinit(self.allocator);

            while (true) {
                var response = try self.pg_client.readWAL();

                if (response.eof) {
                    try self.ch_client.writeLog(self.io, self.log_array.items());

                    return;
                }

                if (response.entry) |entry| {
                    try transaction_array.append(self.allocator, entry);
                    response.entry = null;
                }

                if (response.commit_timestamp != null) {
                    for (transaction_array.items) |*row| {
                        row.event_time = response.commit_timestamp.?;
                    }
                    try self.log_array.append(self.allocator, try self.allocator.dupe(types.AuditEntry, transaction_array.items));

                    self.uncommited_changes = false;
                    transaction_array.clearAndFree(self.allocator);
                } else { 
                    self.uncommited_changes = true;
                }

                if (self.last_write_timestamp.addDuration(self.duration).toMilliseconds() < std.Io.Clock.real.now(self.io).toMilliseconds() and !self.uncommited_changes and self.log_array.items().len > 0) {
                    try self.ch_client.writeLog(self.io, self.log_array.items());
                    self.log_array.free(self.allocator);

                    self.last_write_timestamp = std.Io.Clock.real.now(self.io);
                }
            }
        }
    };
}

const MockPgClient = struct {
    allocator: std.mem.Allocator,
    responses: []pg_client.ReadResponse,
    read_response_index: ?u8,

    conn_opts: void,
    wal_conn: *bool,

    pub fn init(allocator: std.mem.Allocator, responses: []pg_client.ReadResponse) !MockPgClient {
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

    pub fn createWalConn(allocator: std.mem.Allocator, io: std.Io, opts: anytype) !*bool {
        _ = io;
        _ = opts;

        const new_conn = try allocator.create(bool);
        new_conn.* = true;
        return new_conn;
    }
};

const MockChClient = struct {
    allocator: std.mem.Allocator,
    written_logs: types.LogArray = .empty,

    pub fn init(allocator: std.mem.Allocator) MockChClient {
        return .{.allocator = allocator};
    }

    pub fn deinit(self: *@This()) void {
        self.written_logs.deinit(self.allocator);
    }

    pub fn writeLog(self: *@This(), io: std.Io, entries: [][]types.AuditEntry) !void {
        _ = io;

        for (entries) |slice| {
            var copy_slice = try self.allocator.alloc(types.AuditEntry, slice.len);

            for (slice, 0..) |item, i| {
                copy_slice[i] = item;

                copy_slice[i].table_name = try self.allocator.dupe(u8, item.table_name);

                copy_slice[i].changed_columns = try item.changed_columns.clone();
                copy_slice[i].new_values = try item.new_values.clone(self.allocator);
                copy_slice[i].old_values = try item.old_values.clone(self.allocator);

                try self.written_logs.append(self.allocator, copy_slice);
            }
        }
    }
};

test "startStreaming read and parse correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

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
        .last_write_timestamp = std.Io.Clock.real.now(io).subDuration(
        std.Io.Duration.fromSeconds(2)
    ),
        .allocator = allocator,
        .io = io,
    };
    defer processor.deinit();

    try processor.startStreaming();

    try std.testing.expectEqual(1, mock_ch_client.written_logs.items().len);
    try std.testing.expectEqual(1, mock_ch_client.written_logs.items()[0].len);
    try std.testing.expectEqual(1, mock_ch_client.written_logs.items()[0][0].action);
    try std.testing.expectEqualStrings("addresses", mock_ch_client.written_logs.items()[0][0].table_name);
    try std.testing.expectEqualStrings("42", mock_ch_client.written_logs.items()[0][0].user_id);
    try std.testing.expectEqualStrings("192.168.1.50", mock_ch_client.written_logs.items()[0][0].ip_address);
    try std.testing.expectEqualStrings("1", mock_ch_client.written_logs.items()[0][0].primary_key);
    try std.testing.expectEqual(10, mock_ch_client.written_logs.items()[0][0].event_time);
    try std.testing.expectEqual(793, mock_ch_client.written_logs.items()[0][0].transaction_id);

    try std.testing.expectEqual(6, mock_ch_client.written_logs.items()[0][0].changed_columns.count());
    try std.testing.expectEqual(6, mock_ch_client.written_logs.items()[0][0].new_values.count());
    try std.testing.expectEqual(6, mock_ch_client.written_logs.items()[0][0].old_values.count());

    const id_changed_column = mock_ch_client.written_logs.items()[0][0].changed_columns.get("id").?;
    try std.testing.expectEqualStrings("1", id_changed_column.value);
    try std.testing.expectEqual(false, id_changed_column.has_changes);
    const address_line_1_changed_column = mock_ch_client.written_logs.items()[0][0].changed_columns.get("address_line_1").?;
    try std.testing.expectEqualStrings("Googleplex", address_line_1_changed_column.value);
    try std.testing.expectEqual(true, address_line_1_changed_column.has_changes);
    const address_line_2_changed_column = mock_ch_client.written_logs.items()[0][0].changed_columns.get("address_line_2").?;
    try std.testing.expectEqualStrings("", address_line_2_changed_column.value);
    try std.testing.expectEqual(false, address_line_2_changed_column.has_changes);
    const postal_code_changed_column = mock_ch_client.written_logs.items()[0][0].changed_columns.get("postal_code").?;
    try std.testing.expectEqualStrings("94043", postal_code_changed_column.value);
    try std.testing.expectEqual(true, postal_code_changed_column.has_changes);
    const city_changed_column = mock_ch_client.written_logs.items()[0][0].changed_columns.get("city").?;
    try std.testing.expectEqualStrings("Mountain View", city_changed_column.value);
    try std.testing.expectEqual(true, city_changed_column.has_changes);
    const country_changed_column = mock_ch_client.written_logs.items()[0][0].changed_columns.get("country").?;
    try std.testing.expectEqualStrings("US", country_changed_column.value);
    try std.testing.expectEqual(false, country_changed_column.has_changes);

    const id_new_values = mock_ch_client.written_logs.items()[0][0].new_values.get("id").?;
    try std.testing.expectEqualStrings("1", id_new_values);
    const address_line_1_new_values = mock_ch_client.written_logs.items()[0][0].new_values.get("address_line_1").?;
    try std.testing.expectEqualStrings("1 Apple Park Way", address_line_1_new_values);
    const address_line_2_new_values = mock_ch_client.written_logs.items()[0][0].new_values.get("address_line_2").?;
    try std.testing.expectEqualStrings("", address_line_2_new_values);
    const postal_code_new_values = mock_ch_client.written_logs.items()[0][0].new_values.get("postal_code").?;
    try std.testing.expectEqualStrings("95014", postal_code_new_values);
    const city_new_values = mock_ch_client.written_logs.items()[0][0].new_values.get("city").?;
    try std.testing.expectEqualStrings("Cupertino", city_new_values);
    const country_new_values = mock_ch_client.written_logs.items()[0][0].new_values.get("country").?;
    try std.testing.expectEqualStrings("US", country_new_values);

    const id_old_values = mock_ch_client.written_logs.items()[0][0].old_values.get("id").?;
    try std.testing.expectEqualStrings("1", id_old_values);
    const address_line_1_old_values = mock_ch_client.written_logs.items()[0][0].old_values.get("address_line_1").?;
    try std.testing.expectEqualStrings("Googleplex", address_line_1_old_values);
    const address_line_2_old_values = mock_ch_client.written_logs.items()[0][0].old_values.get("address_line_2").?;
    try std.testing.expectEqualStrings("", address_line_2_old_values);
    const postal_code_old_values = mock_ch_client.written_logs.items()[0][0].old_values.get("postal_code").?;
    try std.testing.expectEqualStrings("94043", postal_code_old_values);
    const city_old_values = mock_ch_client.written_logs.items()[0][0].old_values.get("city").?;
    try std.testing.expectEqualStrings("Mountain View", city_old_values);
    const country_old_values = mock_ch_client.written_logs.items()[0][0].old_values.get("country").?;
    try std.testing.expectEqualStrings("US", country_old_values);
}
