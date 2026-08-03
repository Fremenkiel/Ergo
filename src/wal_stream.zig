const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const posix = std.posix;
const assert = std.debug.assert;

const pg = @import("pg");

const types = @import("types.zig");
const _pg_client = @import("pg_client.zig");

const ChClient = @import("ch_client.zig").ChClient;
const PgClient = _pg_client.PgClient;
const PgClientError = _pg_client.PgClientError;
const ReadResponse = _pg_client.ReadResponse;

pub const sync_marker_str = "SYNC_MARKER_REACHED";
pub const submit_marker_str = "CH_DATA_SUBMITTED_MARKER";
const read_timeout_ms = 5;

pub const WalStreamError = error{
FailedWhileShutdown,
};

const StreamStatus = enum(u8) {
    Idle = 0,
    AwaitingData = 1,
    CopyData = 2,
    CopyDone = 3,
    Failed = 4,
};

pub const WalStream = WalStreamT(ChClient, PgClient);

fn WalStreamT(comptime StreamChClient: type, comptime StreamPgClient: type) type {
    return struct {
        allocator: mem.Allocator,
        io: Io,

        ch_client: *StreamChClient,
        pg_client: *StreamPgClient,

        duration: Io.Duration,
        last_write_timestamp: Io.Timestamp,
        log_array: std.ArrayList(types.AuditEntry) = .empty,
        transaction_array: std.ArrayList(types.AuditEntry) = .empty,

        status: StreamStatus = .Idle,

        // only used for running child process tests 
        is_test: bool,

        pub fn init(allocator: mem.Allocator, io: Io, ch_client: *StreamChClient, pg_client: *StreamPgClient, duration: ?i64, is_test: bool) @This() {
            return .{
                .allocator = allocator,
                .io = io,
                .duration = Io.Duration.fromMilliseconds(duration orelse 500),
                .last_write_timestamp = Io.Clock.real.now(io),
                .ch_client = ch_client,
                .pg_client = pg_client,
                .is_test = is_test,
            };
        }

        pub fn deinit(self: *@This()) void {
            for (self.log_array.items) |*entry| entry.deinit(self.allocator);
            self.log_array.deinit(self.allocator);

            for (self.transaction_array.items) |*entry| entry.deinit(self.allocator);
            self.transaction_array.deinit(self.allocator);
        }

        pub fn startStreaming(self: *@This()) !void {
            self.pg_client.startWALReader(read_timeout_ms) catch |err| switch (err) {
                PgClientError.WalConnectionNotInitialized => {
                    self.pg_client.*.wal_conn = try StreamPgClient.createConn(self.allocator, self.io, self.pg_client.*.wal_opts);

                    try self.pg_client.startWALReader(read_timeout_ms);
                },
                else => return err,
            };
        }

        pub fn endStreaming(self: *@This()) !void {
            return self.pg_client.endWALReader();
        }

        pub fn stream(self: *@This(), flag: *std.atomic.Value(bool)) !void {
            while (true) {
                const is_shutting_down = flag.load(.seq_cst);
                const duration_passed = self.last_write_timestamp.addDuration(self.duration).toMilliseconds() < Io.Clock.real.now(self.io).toMilliseconds();
                if ((is_shutting_down or duration_passed) and self.log_array.items.len > 0) {
                    try self.flush();
                }

                if (is_shutting_down) {
                    switch (self.status) {
                        .Idle => {
                            return;
                        },
                        .AwaitingData => {},
                        .CopyData => {
                            try self.pg_client.sendCopyDone();
                            self.status = .CopyDone;
                        },
                        .Failed => {
                            return WalStreamError.FailedWhileShutdown;
                        },
                        .CopyDone => {},
                    }
                }

                var wal_response = self.pg_client.readWAL() catch |err| switch (err) {
                    error.WouldBlock, error.Timeout => {
                        continue; 
                    },
                    else => return err,
                };

                if (wal_response) |*response| {
                    switch (response.message) {
                        pg.packet.ServerPacket.XLogData => {
                            if (self.status != .CopyData) self.status = .CopyData;

                            if (response.data) |entry| {
                                try self.transaction_array.append(self.allocator, entry);
                                // remove linking
                                response.data = null;
                            }

                            if (response.timestamp != null) {
                                for (self.transaction_array.items) |*row| {
                                    row.event_time = response.timestamp.?;
                                }
                                try self.log_array.appendSlice(self.allocator, self.transaction_array.items);
                                self.transaction_array.clearRetainingCapacity();

                            }
                            // Test hook
                            if (self.transaction_array.items.len > 0 and self.is_test) {
                                var marker_idx: ?usize = null;
                                for (self.transaction_array.items, 0..) |*item, i| {
                                    if (std.mem.eql(u8, "public.test_sync_marker", item.table_name)) {
                                        marker_idx = i;
                                        break;
                                    }
                                }

                                if (marker_idx) |idx| {
                                    _ = self.transaction_array.orderedRemove(idx);
                                    try std.Io.File.stdout().writeStreamingAll(self.io, sync_marker_str);
                                    try Io.File.stdout().writeStreamingAll(self.io, "\n");

                                    while (!flag.load(.seq_cst)) {
                                        try Io.sleep(self.io, Io.Duration{ .nanoseconds = 10 * std.time.ns_per_ms }, .real);
                                    }
                                }
                            }
                        },
                        pg.packet.ServerPacket.Keepalive => {},
                        pg.packet.ServerPacket.CopyDone => {},
                        pg.packet.ServerPacket.CommandComplete => {},
                        pg.packet.ServerPacket.ReadyForQuery => {
                            self.status = .Idle;
                        },
                    }
                }
            }
        }

        pub fn flush(self: *@This()) !void {
            if (self.log_array.items.len == 0) return;

            try self.ch_client.writeLog(self.log_array.items);
            for (self.log_array.items) |*entry| entry.deinit(self.allocator);
            self.log_array.clearRetainingCapacity();

            self.last_write_timestamp = Io.Clock.real.now(self.io);

            if (self.is_test) {
                try std.Io.File.stdout().writeStreamingAll(self.io, submit_marker_str);
                try Io.File.stdout().writeStreamingAll(self.io, "\n");
            }
        }
    };
}

var mock_is_shutting_down = std.atomic.Value(bool).init(false);

const t = @import("t.zig");

test "startStreaming: read and parse correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var changed_columns = std.StringHashMap(types.ChangedColumn).init(allocator);
    try changed_columns.ensureUnusedCapacity(6);

    try changed_columns.put("id", .{ .has_changes = false, .value = "1" });
    try changed_columns.put("address_line_1", .{ .has_changes = true, .value = "Googleplex" });
    try changed_columns.put("address_line_2", .{ .has_changes = false, .value = "" });
    try changed_columns.put("postal_code", .{ .has_changes = true, .value = "94043" });
    try changed_columns.put("city", .{ .has_changes = true, .value = "Mountain View" });
    try changed_columns.put("country", .{ .has_changes = false, .value = "US" });

    var new_values = std.StringHashMapUnmanaged([]const u8).empty;
    try new_values.ensureUnusedCapacity(allocator, 6);

    try new_values.put(allocator, "id", try allocator.dupe(u8, "1"));
    try new_values.put(allocator, "address_line_1", try allocator.dupe(u8, "1 Apple Park Way"));
    try new_values.put(allocator, "address_line_2", try allocator.dupe(u8, ""));
    try new_values.put(allocator, "postal_code", try allocator.dupe(u8, "95014"));
    try new_values.put(allocator, "city", try allocator.dupe(u8, "Cupertino"));
    try new_values.put(allocator, "country", try allocator.dupe(u8, "US"));

    var old_values = std.StringHashMapUnmanaged([]const u8).empty;
    try old_values.ensureUnusedCapacity(allocator, 6);

    try old_values.put(allocator, "id", try allocator.dupe(u8, "1"));
    try old_values.put(allocator, "address_line_1", try allocator.dupe(u8, "Googleplex"));
    try old_values.put(allocator, "address_line_2", try allocator.dupe(u8, ""));
    try old_values.put(allocator, "postal_code", try allocator.dupe(u8, "94043"));
    try old_values.put(allocator, "city", try allocator.dupe(u8, "Mountain View"));
    try old_values.put(allocator, "country", try allocator.dupe(u8, "US"));

    var res = [_]ReadResponse{
        .{ 
            .data = .{
                .event_time = undefined,
                .table_name = try allocator.dupe(u8, "test.addresses"),
                .new_values = new_values,
                .old_values = old_values,
                .action = 1,
                .changed_columns = changed_columns,
                .transaction_id = 793,
                .user_id = try allocator.dupe(u8, "42"),
                .ip_address = try allocator.dupe(u8, "192.168.1.50"),
                .primary_key = try allocator.dupe(u8, "1"),
            },
            .timestamp = 10,
            .message = pg.packet.ServerPacket.XLogData,
        },
    };

    var pg_client = try t.PgClient.init(allocator, io, &res);
    defer pg_client.deinit();

    var ch_client = t.ChClient.init(allocator);
    defer ch_client.deinit();

    const TestWalStream = WalStreamT(t.ChClient, t.PgClient);

    var stream = TestWalStream.init(
        allocator,
        io, 
        &ch_client,
        &pg_client, 
        null,
        false,
    );
    defer stream.deinit();

    stream.last_write_timestamp = Io.Clock.real.now(io).subDuration(
        Io.Duration.fromSeconds(2));
    stream.status = .CopyData;

    try stream.startStreaming();

    mock_is_shutting_down.store(true, .seq_cst);

    try stream.stream(&mock_is_shutting_down);
    try stream.endStreaming();

    try testing.expectEqual(1, ch_client.written_logs.items.len);
    try testing.expectEqual(1, ch_client.written_logs.items[0].action);
    try testing.expectEqualStrings("test.addresses", ch_client.written_logs.items[0].table_name);
    try testing.expectEqualStrings("42", ch_client.written_logs.items[0].user_id);
    try testing.expectEqualStrings("192.168.1.50", ch_client.written_logs.items[0].ip_address);
    try testing.expectEqualStrings("1", ch_client.written_logs.items[0].primary_key);
    try testing.expectEqual(10, ch_client.written_logs.items[0].event_time);
    try testing.expectEqual(793, ch_client.written_logs.items[0].transaction_id);

    try testing.expectEqual(6, ch_client.written_logs.items[0].changed_columns.count());
    try testing.expectEqual(6, ch_client.written_logs.items[0].new_values.count());
    try testing.expectEqual(6, ch_client.written_logs.items[0].old_values.count());

    const id_changed_column = ch_client.written_logs.items[0].changed_columns.get("id").?;
    try testing.expectEqualStrings("1", id_changed_column.value);
    try testing.expectEqual(false, id_changed_column.has_changes);
    const address_line_1_changed_column = ch_client.written_logs.items[0].changed_columns.get("address_line_1").?;
    try testing.expectEqualStrings("Googleplex", address_line_1_changed_column.value);
    try testing.expectEqual(true, address_line_1_changed_column.has_changes);
    const address_line_2_changed_column = ch_client.written_logs.items[0].changed_columns.get("address_line_2").?;
    try testing.expectEqualStrings("", address_line_2_changed_column.value);
    try testing.expectEqual(false, address_line_2_changed_column.has_changes);
    const postal_code_changed_column = ch_client.written_logs.items[0].changed_columns.get("postal_code").?;
    try testing.expectEqualStrings("94043", postal_code_changed_column.value);
    try testing.expectEqual(true, postal_code_changed_column.has_changes);
    const city_changed_column = ch_client.written_logs.items[0].changed_columns.get("city").?;
    try testing.expectEqualStrings("Mountain View", city_changed_column.value);
    try testing.expectEqual(true, city_changed_column.has_changes);
    const country_changed_column = ch_client.written_logs.items[0].changed_columns.get("country").?;
    try testing.expectEqualStrings("US", country_changed_column.value);
    try testing.expectEqual(false, country_changed_column.has_changes);

    const id_new_values = ch_client.written_logs.items[0].new_values.get("id").?;
    try testing.expectEqualStrings("1", id_new_values);
    const address_line_1_new_values = ch_client.written_logs.items[0].new_values.get("address_line_1").?;
    try testing.expectEqualStrings("1 Apple Park Way", address_line_1_new_values);
    const address_line_2_new_values = ch_client.written_logs.items[0].new_values.get("address_line_2").?;
    try testing.expectEqualStrings("", address_line_2_new_values);
    const postal_code_new_values = ch_client.written_logs.items[0].new_values.get("postal_code").?;
    try testing.expectEqualStrings("95014", postal_code_new_values);
    const city_new_values = ch_client.written_logs.items[0].new_values.get("city").?;
    try testing.expectEqualStrings("Cupertino", city_new_values);
    const country_new_values = ch_client.written_logs.items[0].new_values.get("country").?;
    try testing.expectEqualStrings("US", country_new_values);

    const id_old_values = ch_client.written_logs.items[0].old_values.get("id").?;
    try testing.expectEqualStrings("1", id_old_values);
    const address_line_1_old_values = ch_client.written_logs.items[0].old_values.get("address_line_1").?;
    try testing.expectEqualStrings("Googleplex", address_line_1_old_values);
    const address_line_2_old_values = ch_client.written_logs.items[0].old_values.get("address_line_2").?;
    try testing.expectEqualStrings("", address_line_2_old_values);
    const postal_code_old_values = ch_client.written_logs.items[0].old_values.get("postal_code").?;
    try testing.expectEqualStrings("94043", postal_code_old_values);
    const city_old_values = ch_client.written_logs.items[0].old_values.get("city").?;
    try testing.expectEqualStrings("Mountain View", city_old_values);
    const country_old_values = ch_client.written_logs.items[0].old_values.get("country").?;
    try testing.expectEqualStrings("US", country_old_values);
}

test "startStreaming: insert all types read correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    const db_name = try t.genereateDbName(allocator, io);
    defer allocator.free(db_name);

    try t.createTestPgDb(allocator, io, db_name);

    const wal_name = try std.fmt.allocPrint(allocator, "wal_slot_{s}", .{db_name});
    defer allocator.free(wal_name);

    defer t.teardownTestPgDb(allocator, io, db_name, wal_name) catch {};

    var startup_parameters: std.StringHashMap([]const u8) = .init(allocator);
    defer startup_parameters.deinit();

    var pg_client = try PgClient.init(allocator, io, .{
        .port = 5432,
        .host = "localhost",
        .wal = wal_name,
        .username = "db_rp",
        .password = "12345678",
        .database = db_name,
        .application_name = "Ergo test",
        .timeout_ms = 10_000,
        .startup_parameters = startup_parameters
    });
    defer pg_client.deinit();
    defer pg_client.cancel();

    var ch_client = t.ChClient.init(allocator);
    defer ch_client.deinit();

    const TestWalStream = WalStreamT(t.ChClient, PgClient);
    var stream = TestWalStream.init(
        allocator,
        io, 
        &ch_client,
        &pg_client, 
        null,
        false,
    );
    defer stream.deinit();

    stream.status = .AwaitingData;

    var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "12345678");

    var pg_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "db_rw",
        "-d", db_name,
        "-a",
        "-f", "./test_fixtures/all-types-insert-query.sql"
    };
    const pg_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_result.stdout);
        allocator.free(pg_result.stderr);
    }

    if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
        std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
        return error.PsqlExecutionFailed;
    }

    try stream.startStreaming();

    mock_is_shutting_down.store(true, .seq_cst);

    try stream.stream(&mock_is_shutting_down);
    try stream.endStreaming();

    const id_new_values = ch_client.written_logs.items[0].new_values.get("id").?;
    const col_int2_value = ch_client.written_logs.items[0].new_values.get("col_int2").?;
    const col_int2_arr_value = ch_client.written_logs.items[0].new_values.get("col_int2_arr").?;
    const col_int4_value = ch_client.written_logs.items[0].new_values.get("col_int4").?;
    const col_int4_arr_value = ch_client.written_logs.items[0].new_values.get("col_int4_arr").?;
    const col_int8_value = ch_client.written_logs.items[0].new_values.get("col_int8").?;
    const col_int8_arr_value = ch_client.written_logs.items[0].new_values.get("col_int8_arr").?;
    const col_float4_value = ch_client.written_logs.items[0].new_values.get("col_float4").?;
    const col_float4_arr_value = ch_client.written_logs.items[0].new_values.get("col_float4_arr").?;
    const col_float8_value = ch_client.written_logs.items[0].new_values.get("col_float8").?;
    const col_float8_arr_value = ch_client.written_logs.items[0].new_values.get("col_float8_arr").?;
    const col_bool_value = ch_client.written_logs.items[0].new_values.get("col_bool").?;
    const col_bool_arr_value = ch_client.written_logs.items[0].new_values.get("col_bool_arr").?;
    const col_text_value = ch_client.written_logs.items[0].new_values.get("col_text").?;
    const col_text_arr_value = ch_client.written_logs.items[0].new_values.get("col_text_arr").?;
    const col_bytea_value = ch_client.written_logs.items[0].new_values.get("col_bytea").?;
    const col_bytea_arr_value = ch_client.written_logs.items[0].new_values.get("col_bytea_arr").?;
    const col_enum_value = ch_client.written_logs.items[0].new_values.get("col_enum").?;
    const col_enum_arr_value = ch_client.written_logs.items[0].new_values.get("col_enum_arr").?;
    const col_uuid_value = ch_client.written_logs.items[0].new_values.get("col_uuid").?;
    const col_uuid_arr_value = ch_client.written_logs.items[0].new_values.get("col_uuid_arr").?;
    const col_numeric_value = ch_client.written_logs.items[0].new_values.get("col_numeric").?;
    const col_numeric_arr_value = ch_client.written_logs.items[0].new_values.get("col_numeric_arr").?;
    const col_timestamp_value = ch_client.written_logs.items[0].new_values.get("col_timestamp").?;
    const col_timestamp_arr_value = ch_client.written_logs.items[0].new_values.get("col_timestamp_arr").?;
    const col_json_value = ch_client.written_logs.items[0].new_values.get("col_json").?;
    const col_json_arr_value = ch_client.written_logs.items[0].new_values.get("col_json_arr").?;
    const col_jsonb_value = ch_client.written_logs.items[0].new_values.get("col_jsonb").?;
    const col_jsonb_arr_value = ch_client.written_logs.items[0].new_values.get("col_jsonb_arr").?;
    const col_char_value = ch_client.written_logs.items[0].new_values.get("col_char").?;
    const col_char_arr_value = ch_client.written_logs.items[0].new_values.get("col_char_arr").?;
    const col_charn_value = ch_client.written_logs.items[0].new_values.get("col_charn").?;
    const col_charn_arr_value = ch_client.written_logs.items[0].new_values.get("col_charn_arr").?;
    const col_timestamptz_value = ch_client.written_logs.items[0].new_values.get("col_timestamptz").?;
    const col_timestamptz_arr_value = ch_client.written_logs.items[0].new_values.get("col_timestamptz_arr").?;
    const col_cidr_value = ch_client.written_logs.items[0].new_values.get("col_cidr").?;
    const col_cidr_arr_value = ch_client.written_logs.items[0].new_values.get("col_cidr_arr").?;
    const col_inet_value = ch_client.written_logs.items[0].new_values.get("col_inet").?;
    const col_inet_arr_value = ch_client.written_logs.items[0].new_values.get("col_inet_arr").?;
    const col_macaddr_value = ch_client.written_logs.items[0].new_values.get("col_macaddr").?;
    const col_macaddr_arr_value = ch_client.written_logs.items[0].new_values.get("col_macaddr_arr").?;
    const col_macaddr8_value = ch_client.written_logs.items[0].new_values.get("col_macaddr8").?;
    const col_macaddr8_arr_value = ch_client.written_logs.items[0].new_values.get("col_macaddr8_arr").?;

    try testing.expectEqual(1, ch_client.written_logs.items.len);
    try testing.expectEqual(43, ch_client.written_logs.items[0].new_values.count());

    try testing.expectEqualStrings("1", id_new_values);
    try testing.expectEqualStrings("-32768", col_int2_value);
    try testing.expectEqualStrings("{-32768,32767}", col_int2_arr_value);
    try testing.expectEqualStrings("-2147483648", col_int4_value);
    try testing.expectEqualStrings("{-2147483648,2147483647}", col_int4_arr_value);
    try testing.expectEqualStrings("-9223372036854775808", col_int8_value);
    try testing.expectEqualStrings("{-9223372036854775808,9223372036854775807}", col_int8_arr_value);
    try testing.expectEqualStrings("3.5555556", col_float4_value);
    try testing.expectEqualStrings("{-3.5555556,3.5555556}", col_float4_arr_value);
    try testing.expectEqualStrings("3.5555555555555554", col_float8_value);
    try testing.expectEqualStrings("{-3.5555555555555554,3.5555555555555554}", col_float8_arr_value);
    try testing.expectEqualStrings("t", col_bool_value);
    try testing.expectEqualStrings("{f,t}", col_bool_arr_value);
    try testing.expectEqualStrings("sample text", col_text_value);
    try testing.expectEqualStrings("{text1,text2}", col_text_arr_value);
    try testing.expectEqualStrings("\\xdeadbeef", col_bytea_value);
    try testing.expectEqualStrings(
        \\{"\\xdeadbeef","\\xcafebabe"}
    , col_bytea_arr_value);
    try testing.expectEqualStrings("val1", col_enum_value);
    try testing.expectEqualStrings("{val1,val2}", col_enum_arr_value);
    try testing.expectEqualStrings("123e4567-e89b-12d3-a456-426614174000", col_uuid_value);
    try testing.expectEqualStrings("{123e4567-e89b-12d3-a456-426614174000,123e4567-e89b-12d3-a456-426614174001}", col_uuid_arr_value);
    try testing.expectEqualStrings("12345.6789", col_numeric_value);
    try testing.expectEqualStrings("{12345.6789,98765.4321}", col_numeric_arr_value);
    try testing.expectEqualStrings("2024-08-02 10:00:00", col_timestamp_value);
    try testing.expectEqualStrings("{\"2024-08-02 10:00:00\",\"2024-08-03 10:00:00\"}", col_timestamp_arr_value);
    try testing.expectEqualStrings("{\"key\": \"value\"}", col_json_value);
    try testing.expectEqualStrings(
        \\{"{\"key\": \"value\"}","{\"key\": \"value2\"}"}
    , col_json_arr_value);
    try testing.expectEqualStrings("{\"key\": \"value\"}", col_jsonb_value);
    try testing.expectEqualStrings(
        \\{"{\"key\": \"value\"}","{\"key\": \"value2\"}"}
    , col_jsonb_arr_value);
    try testing.expectEqualStrings("a", col_char_value);
    try testing.expectEqualStrings("{a,b}", col_char_arr_value);
    try testing.expectEqualStrings("abc", col_charn_value);
    try testing.expectEqualStrings("{ab,cd}", col_charn_arr_value);
    try testing.expectEqualStrings("2024-08-02 10:00:00+00", col_timestamptz_value);
    try testing.expectEqualStrings("{\"2024-08-02 10:00:00+00\",\"2024-08-03 10:00:00+00\"}", col_timestamptz_arr_value);
    try testing.expectEqualStrings("192.168.100.128/25", col_cidr_value);
    try testing.expectEqualStrings("{192.168.100.128/25,10.0.0.0/8}", col_cidr_arr_value);
    try testing.expectEqualStrings("192.168.0.1", col_inet_value);
    try testing.expectEqualStrings("{192.168.0.1,10.0.0.1}", col_inet_arr_value);
    try testing.expectEqualStrings("08:00:2b:01:02:03", col_macaddr_value);
    try testing.expectEqualStrings("{08:00:2b:01:02:03,08:00:2b:01:02:04}", col_macaddr_arr_value);
    try testing.expectEqualStrings("08:00:2b:01:02:03:04:05", col_macaddr8_value);
    try testing.expectEqualStrings("{08:00:2b:01:02:03:04:05,08:00:2b:01:02:03:04:06}", col_macaddr8_arr_value);
}

test "startStreaming: update all types to null read correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    const db_name = try t.genereateDbName(allocator, io);
    defer allocator.free(db_name);

    try t.createTestPgDb(allocator, io, db_name);

    const wal_name = try std.fmt.allocPrint(allocator, "wal_slot_{s}", .{db_name});
    defer allocator.free(wal_name);

    defer t.teardownTestPgDb(allocator, io, db_name, wal_name) catch {};

    var startup_parameters: std.StringHashMap([]const u8) = .init(allocator);
    defer startup_parameters.deinit();

    var pg_client = try PgClient.init(allocator, io, .{
        .port = 5432,
        .host = "localhost",
        .wal = wal_name,
        .username = "db_rp",
        .password = "12345678",
        .database = db_name,
        .application_name = "Ergo test",
        .timeout_ms = 10_000,
        .startup_parameters = startup_parameters
    });
    defer pg_client.deinit();
    defer pg_client.cancel();

    var ch_client = t.ChClient.init(allocator);
    defer ch_client.deinit();

    const TestWalStream = WalStreamT(t.ChClient, PgClient);
    var stream = TestWalStream.init(
        allocator,
        io, 
        &ch_client,
        &pg_client, 
        null,
        false,
    );
    defer stream.deinit();

    stream.status = .AwaitingData;

    var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "12345678");

    var pg_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "db_rw",
        "-d", db_name,
        "-a",
        "-f", "./test_fixtures/all-types-update-query.sql"
    };
    const pg_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_result.stdout);
        allocator.free(pg_result.stderr);
    }

    if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
        std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
        return error.PsqlExecutionFailed;
    }

    try stream.startStreaming();

    mock_is_shutting_down.store(true, .seq_cst);

    try stream.stream(&mock_is_shutting_down);
    try stream.endStreaming();

    const id_new_values = ch_client.written_logs.items[1].new_values.get("id").?;
    const col_int2_value = ch_client.written_logs.items[1].new_values.get("col_int2");
    const col_int2_arr_value = ch_client.written_logs.items[1].new_values.get("col_int2_arr");
    const col_int4_value = ch_client.written_logs.items[1].new_values.get("col_int4");
    const col_int4_arr_value = ch_client.written_logs.items[1].new_values.get("col_int4_arr");
    const col_int8_value = ch_client.written_logs.items[1].new_values.get("col_int8");
    const col_int8_arr_value = ch_client.written_logs.items[1].new_values.get("col_int8_arr");
    const col_float4_value = ch_client.written_logs.items[1].new_values.get("col_float4");
    const col_float4_arr_value = ch_client.written_logs.items[1].new_values.get("col_float4_arr");
    const col_float8_value = ch_client.written_logs.items[1].new_values.get("col_float8");
    const col_float8_arr_value = ch_client.written_logs.items[1].new_values.get("col_float8_arr");
    const col_bool_value = ch_client.written_logs.items[1].new_values.get("col_bool");
    const col_bool_arr_value = ch_client.written_logs.items[1].new_values.get("col_bool_arr");
    const col_text_value = ch_client.written_logs.items[1].new_values.get("col_text");
    const col_text_arr_value = ch_client.written_logs.items[1].new_values.get("col_text_arr");
    const col_bytea_value = ch_client.written_logs.items[1].new_values.get("col_bytea");
    const col_bytea_arr_value = ch_client.written_logs.items[1].new_values.get("col_bytea_arr");
    const col_enum_value = ch_client.written_logs.items[1].new_values.get("col_enum");
    const col_enum_arr_value = ch_client.written_logs.items[1].new_values.get("col_enum_arr");
    const col_uuid_value = ch_client.written_logs.items[1].new_values.get("col_uuid");
    const col_uuid_arr_value = ch_client.written_logs.items[1].new_values.get("col_uuid_arr");
    const col_numeric_value = ch_client.written_logs.items[1].new_values.get("col_numeric");
    const col_numeric_arr_value = ch_client.written_logs.items[1].new_values.get("col_numeric_arr");
    const col_timestamp_value = ch_client.written_logs.items[1].new_values.get("col_timestamp");
    const col_timestamp_arr_value = ch_client.written_logs.items[1].new_values.get("col_timestamp_arr");
    const col_json_value = ch_client.written_logs.items[1].new_values.get("col_json");
    const col_json_arr_value = ch_client.written_logs.items[1].new_values.get("col_json_arr");
    const col_jsonb_value = ch_client.written_logs.items[1].new_values.get("col_jsonb");
    const col_jsonb_arr_value = ch_client.written_logs.items[1].new_values.get("col_jsonb_arr");
    const col_char_value = ch_client.written_logs.items[1].new_values.get("col_char");
    const col_char_arr_value = ch_client.written_logs.items[1].new_values.get("col_char_arr");
    const col_charn_value = ch_client.written_logs.items[1].new_values.get("col_charn");
    const col_charn_arr_value = ch_client.written_logs.items[1].new_values.get("col_charn_arr");
    const col_timestamptz_value = ch_client.written_logs.items[1].new_values.get("col_timestamptz");
    const col_timestamptz_arr_value = ch_client.written_logs.items[1].new_values.get("col_timestamptz_arr");
    const col_cidr_value = ch_client.written_logs.items[1].new_values.get("col_cidr");
    const col_cidr_arr_value = ch_client.written_logs.items[1].new_values.get("col_cidr_arr");
    const col_inet_value = ch_client.written_logs.items[1].new_values.get("col_inet");
    const col_inet_arr_value = ch_client.written_logs.items[1].new_values.get("col_inet_arr");
    const col_macaddr_value = ch_client.written_logs.items[1].new_values.get("col_macaddr");
    const col_macaddr_arr_value = ch_client.written_logs.items[1].new_values.get("col_macaddr_arr");
    const col_macaddr8_value = ch_client.written_logs.items[1].new_values.get("col_macaddr8");
    const col_macaddr8_arr_value = ch_client.written_logs.items[1].new_values.get("col_macaddr8_arr");

    try testing.expectEqual(2, ch_client.written_logs.items.len);
    try testing.expectEqual(43, ch_client.written_logs.items[0].new_values.count());

    try testing.expectEqualStrings("1", id_new_values);
    try testing.expectEqual(null, col_int2_value);
    try testing.expectEqual(null, col_int2_arr_value);
    try testing.expectEqual(null, col_int4_value);
    try testing.expectEqual(null, col_int4_arr_value);
    try testing.expectEqual(null, col_int8_value);
    try testing.expectEqual(null, col_int8_arr_value);
    try testing.expectEqual(null, col_float4_value);
    try testing.expectEqual(null, col_float4_arr_value);
    try testing.expectEqual(null, col_float8_value);
    try testing.expectEqual(null, col_float8_arr_value);
    try testing.expectEqual(null, col_bool_value);
    try testing.expectEqual(null, col_bool_arr_value);
    try testing.expectEqual(null, col_text_value);
    try testing.expectEqual(null, col_text_arr_value);
    try testing.expectEqual(null, col_bytea_value);
    try testing.expectEqual(null, col_bytea_arr_value);
    try testing.expectEqual(null, col_enum_value);
    try testing.expectEqual(null, col_enum_arr_value);
    try testing.expectEqual(null, col_uuid_value);
    try testing.expectEqual(null, col_uuid_arr_value);
    try testing.expectEqual(null, col_numeric_value);
    try testing.expectEqual(null, col_numeric_arr_value);
    try testing.expectEqual(null, col_timestamp_value);
    try testing.expectEqual(null, col_timestamp_arr_value);
    try testing.expectEqual(null, col_json_value);
    try testing.expectEqual(null, col_json_arr_value);
    try testing.expectEqual(null, col_jsonb_value);
    try testing.expectEqual(null, col_jsonb_arr_value);
    try testing.expectEqual(null, col_char_value);
    try testing.expectEqual(null, col_char_arr_value);
    try testing.expectEqual(null, col_charn_value);
    try testing.expectEqual(null, col_charn_arr_value);
    try testing.expectEqual(null, col_timestamptz_value);
    try testing.expectEqual(null, col_timestamptz_arr_value);
    try testing.expectEqual(null, col_cidr_value);
    try testing.expectEqual(null, col_cidr_arr_value);
    try testing.expectEqual(null, col_inet_value);
    try testing.expectEqual(null, col_inet_arr_value);
    try testing.expectEqual(null, col_macaddr_value);
    try testing.expectEqual(null, col_macaddr_arr_value);
    try testing.expectEqual(null, col_macaddr8_value);
    try testing.expectEqual(null, col_macaddr8_arr_value);
}

test "startStreaming: insert all types to null write correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    const db_name = try t.genereateDbName(allocator, io);
    defer allocator.free(db_name);

    try t.createTestChDb(allocator, io, db_name);
    defer t.teardownTestChDb(allocator, io, db_name) catch {};

    var changed_columns = std.StringHashMap(types.ChangedColumn).init(allocator);
    try changed_columns.ensureUnusedCapacity(6);

    try changed_columns.put("id", .{ .has_changes = false, .value = "1" });
    try changed_columns.put("address_line_1", .{ .has_changes = true, .value = "Googleplex" });
    try changed_columns.put("address_line_2", .{ .has_changes = false, .value = "" });
    try changed_columns.put("postal_code", .{ .has_changes = true, .value = "94043" });
    try changed_columns.put("city", .{ .has_changes = true, .value = "Mountain View" });
    try changed_columns.put("country", .{ .has_changes = false, .value = "US" });

    var new_values = std.StringHashMapUnmanaged([]const u8).empty;
    try new_values.ensureUnusedCapacity(allocator, 6);

    try new_values.put(allocator, "id", try allocator.dupe(u8, "1"));
    try new_values.put(allocator, "address_line_1", try allocator.dupe(u8, "1 Apple Park Way"));
    try new_values.put(allocator, "address_line_2", try allocator.dupe(u8, ""));
    try new_values.put(allocator, "postal_code", try allocator.dupe(u8, "95014"));
    try new_values.put(allocator, "city", try allocator.dupe(u8, "Cupertino"));
    try new_values.put(allocator, "country", try allocator.dupe(u8, "US"));

    var old_values = std.StringHashMapUnmanaged([]const u8).empty;
    try old_values.ensureUnusedCapacity(allocator, 6);

    try old_values.put(allocator, "id", try allocator.dupe(u8, "1"));
    try old_values.put(allocator, "address_line_1", try allocator.dupe(u8, "Googleplex"));
    try old_values.put(allocator, "address_line_2", try allocator.dupe(u8, ""));
    try old_values.put(allocator, "postal_code", try allocator.dupe(u8, "94043"));
    try old_values.put(allocator, "city", try allocator.dupe(u8, "Mountain View"));
    try old_values.put(allocator, "country", try allocator.dupe(u8, "US"));

    var res = [_]ReadResponse{
        .{ 
            .data = .{
                .event_time = undefined,
                .table_name = try allocator.dupe(u8, "test.addresses"),
                .new_values = new_values,
                .old_values = old_values,
                .action = 1,
                .changed_columns = changed_columns,
                .transaction_id = 793,
                .user_id = try allocator.dupe(u8, "42"),
                .ip_address = try allocator.dupe(u8, "192.168.1.50"),
                .primary_key = try allocator.dupe(u8, "1"),
            },
            .timestamp = 10,
            .message = pg.packet.ServerPacket.XLogData,
        },
    };

    var pg_client = try t.PgClient.init(allocator, io, &res);
    defer pg_client.deinit();

    var ch_client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = db_name,
        .application_name = "Ergo test",
    }, "Fremenkiel");
    defer ch_client.deinit();

    try ch_client.connect();
    defer ch_client.disconnect();

    const TestWalStream = WalStreamT(ChClient, t.PgClient);

    var stream = TestWalStream.init(
        allocator,
        io, 
        &ch_client,
        &pg_client, 
        null,
        false,
    );
    defer stream.deinit();

    stream.last_write_timestamp = Io.Clock.real.now(io).subDuration(
        Io.Duration.fromSeconds(2));
    stream.status = .CopyData;

    try stream.startStreaming();

    mock_is_shutting_down.store(true, .seq_cst);

    try stream.stream(&mock_is_shutting_down);
    try stream.endStreaming();

    var ch_assert_argv = [_][]const u8{ 
        "clickhouse-client", 
        "--host", "127.0.0.1",
        "--port", "9000",
        "--user", "default",
        "--password", "clickhouse",
        "--database", db_name,
        "--query", "SELECT action, table_name, primary_key, changed_columns, old_values, new_values, user_id, ip_address FROM entries ORDER BY primary_key, action DESC" 
    };
    const ch_assert_result = try std.process.run(allocator, io, .{ 
        .argv = &ch_assert_argv,
    });
    defer {
        allocator.free(ch_assert_result.stdout);
        allocator.free(ch_assert_result.stderr);
    }

    if (ch_assert_result.term != .exited or ch_assert_result.term.exited != 0 or ch_assert_result.stderr.len > 0) {
        std.debug.print("Error: unable to select ch data: {s}\n", .{ch_assert_result.stderr});
        return error.ChSelectError;
    }

    try testing.expectEqualStrings(
        "INSERT\ttest.addresses\t1\t['address_line_1','postal_code','city']\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t42\t192.168.1.50\n",
        ch_assert_result.stdout,
    );

}
