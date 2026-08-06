const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const posix = std.posix;
const assert = std.debug.assert;

const pg = @import("pg");

const types = @import("types");
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
            self.pg_client.startFlow(read_timeout_ms) catch |err| switch (err) {
                pg.PgError.WalConnectionNotInitialized => {
                    self.pg_client.*.conn = try StreamPgClient.createConn(self.allocator, self.io, self.pg_client.*.opts);

                    try self.pg_client.startFlow(read_timeout_ms);
                },
                else => return err,
            };
        }

        pub fn endStreaming(self: *@This()) !void {
            return self.pg_client.endFlow();
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

                            if (response.timestamp) |timestamp| {
                                for (self.transaction_array.items) |*row| {
                                    row.event_time = timestamp;
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

    var columns: std.ArrayList(types.ColumnChange) = .empty;
    errdefer {
        columns.clearAndFree(allocator);
        columns.deinit(allocator);
    }
    try columns.ensureUnusedCapacity(allocator, 6);

    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = try allocator.dupe(u8, "1"), .new_value = try allocator.dupe(u8, "1"), .column_name = try allocator.dupe(u8, "id"), .is_key = true });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "Googleplex"), .new_value = try allocator.dupe(u8, "1 Apple Park Way"), .column_name = try allocator.dupe(u8, "address_line_1"), .is_key = false});
    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = try allocator.dupe(u8, ""), .new_value = try allocator.dupe(u8, ""), .column_name = try allocator.dupe(u8, "address_line_2"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "94043"), .new_value = try allocator.dupe(u8, "95014"), .column_name = try allocator.dupe(u8, "postal_code"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "Mountain View"), .new_value = try allocator.dupe(u8, "Cupertino"), .column_name = try allocator.dupe(u8, "city"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = try allocator.dupe(u8, "US"), .new_value = try allocator.dupe(u8, "US"), .column_name = try allocator.dupe(u8, "country"), .is_key = false });

    var res = [_]ReadResponse{
        .{ 
            .data = .{
                .event_time = undefined,
                .table_name = try allocator.dupe(u8, "test.addresses"),
                .action = 1,
                .columns = columns,
                .transaction_id = 793,
                .user_id = try allocator.dupe(u8, "42"),
                .ip_address = try allocator.dupe(u8, "192.168.1.50"),
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
    try testing.expectEqual(true, ch_client.written_logs.items[0].columns.items[0].is_key);
    try testing.expectEqualStrings("1", ch_client.written_logs.items[0].columns.items[0].new_value.?);
    try testing.expectEqual(10, ch_client.written_logs.items[0].event_time);
    try testing.expectEqual(793, ch_client.written_logs.items[0].transaction_id);

    try testing.expectEqual(6, ch_client.written_logs.items[0].columns.items.len);

    const id_column = ch_client.written_logs.items[0].columns.items[0];
    const address_line_1_column = ch_client.written_logs.items[0].columns.items[1];
    const address_line_2_column = ch_client.written_logs.items[0].columns.items[2];
    const postal_code_column = ch_client.written_logs.items[0].columns.items[3];
    const city_column = ch_client.written_logs.items[0].columns.items[4];
    const country_column = ch_client.written_logs.items[0].columns.items[5];

    try testing.expectEqualStrings("id", id_column.column_name);
    try testing.expectEqualStrings("address_line_1", address_line_1_column.column_name);
    try testing.expectEqualStrings("address_line_2", address_line_2_column.column_name);
    try testing.expectEqualStrings("postal_code", postal_code_column.column_name);
    try testing.expectEqualStrings("city", city_column.column_name);
    try testing.expectEqualStrings("country", country_column.column_name);

    try testing.expectEqual(false, id_column.has_changes);
    try testing.expectEqual(true, address_line_1_column.has_changes);
    try testing.expectEqual(false, address_line_2_column.has_changes);
    try testing.expectEqual(true, postal_code_column.has_changes);
    try testing.expectEqual(true, city_column.has_changes);
    try testing.expectEqual(false, country_column.has_changes);

    try testing.expectEqualStrings("1", id_column.new_value.?);
    try testing.expectEqualStrings("1 Apple Park Way", address_line_1_column.new_value.?);
    try testing.expectEqualStrings("", address_line_2_column.new_value.?);
    try testing.expectEqualStrings("95014", postal_code_column.new_value.?);
    try testing.expectEqualStrings("Cupertino", city_column.new_value.?);
    try testing.expectEqualStrings("US", country_column.new_value.?);

    try testing.expectEqualStrings("1", id_column.old_value.?);
    try testing.expectEqualStrings("Googleplex", address_line_1_column.old_value.?);
    try testing.expectEqualStrings("", address_line_2_column.old_value.?);
    try testing.expectEqualStrings("94043", postal_code_column.old_value.?);
    try testing.expectEqualStrings("Mountain View", city_column.old_value.?);
    try testing.expectEqualStrings("US", country_column.old_value.?);
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

    var pg_client = try PgClient.init(allocator, io, .{
        .port = 5432,
        .host = "localhost",
        .wal = wal_name,
        .username = "db_rw",
        .password = "12345678",
        .database = db_name,
        .application_name = "Ergo test",
        .timeout_ms = 10_000,
        .startup_parameters = null
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

    const id_new_values = ch_client.written_logs.items[0].columns.items[0].new_value;
    const col_int2_value = ch_client.written_logs.items[0].columns.items[1].new_value;
    const col_int2_arr_value = ch_client.written_logs.items[0].columns.items[2].new_value;
    const col_int4_value = ch_client.written_logs.items[0].columns.items[3].new_value;
    const col_int4_arr_value = ch_client.written_logs.items[0].columns.items[4].new_value;
    const col_int8_value = ch_client.written_logs.items[0].columns.items[5].new_value;
    const col_int8_arr_value = ch_client.written_logs.items[0].columns.items[6].new_value;
    const col_float4_value = ch_client.written_logs.items[0].columns.items[7].new_value;
    const col_float4_arr_value = ch_client.written_logs.items[0].columns.items[8].new_value;
    const col_float8_value = ch_client.written_logs.items[0].columns.items[9].new_value;
    const col_float8_arr_value = ch_client.written_logs.items[0].columns.items[10].new_value;
    const col_bool_value = ch_client.written_logs.items[0].columns.items[11].new_value;
    const col_bool_arr_value = ch_client.written_logs.items[0].columns.items[12].new_value;
    const col_text_value = ch_client.written_logs.items[0].columns.items[13].new_value;
    const col_text_arr_value = ch_client.written_logs.items[0].columns.items[14].new_value;
    const col_bytea_value = ch_client.written_logs.items[0].columns.items[15].new_value;
    const col_bytea_arr_value = ch_client.written_logs.items[0].columns.items[16].new_value;
    const col_enum_value = ch_client.written_logs.items[0].columns.items[17].new_value;
    const col_enum_arr_value = ch_client.written_logs.items[0].columns.items[18].new_value;
    const col_uuid_value = ch_client.written_logs.items[0].columns.items[19].new_value;
    const col_uuid_arr_value = ch_client.written_logs.items[0].columns.items[20].new_value;
    const col_numeric_value = ch_client.written_logs.items[0].columns.items[21].new_value;
    const col_numeric_arr_value = ch_client.written_logs.items[0].columns.items[22].new_value;
    const col_timestamp_value = ch_client.written_logs.items[0].columns.items[23].new_value;
    const col_timestamp_arr_value = ch_client.written_logs.items[0].columns.items[24].new_value;
    const col_json_value = ch_client.written_logs.items[0].columns.items[25].new_value;
    const col_json_arr_value = ch_client.written_logs.items[0].columns.items[26].new_value;
    const col_jsonb_value = ch_client.written_logs.items[0].columns.items[27].new_value;
    const col_jsonb_arr_value = ch_client.written_logs.items[0].columns.items[28].new_value;
    const col_char_value = ch_client.written_logs.items[0].columns.items[29].new_value;
    const col_char_arr_value = ch_client.written_logs.items[0].columns.items[30].new_value;
    const col_charn_value = ch_client.written_logs.items[0].columns.items[31].new_value;
    const col_charn_arr_value = ch_client.written_logs.items[0].columns.items[32].new_value;
    const col_timestamptz_value = ch_client.written_logs.items[0].columns.items[33].new_value;
    const col_timestamptz_arr_value = ch_client.written_logs.items[0].columns.items[34].new_value;
    const col_cidr_value = ch_client.written_logs.items[0].columns.items[35].new_value;
    const col_cidr_arr_value = ch_client.written_logs.items[0].columns.items[36].new_value;
    const col_inet_value = ch_client.written_logs.items[0].columns.items[37].new_value;
    const col_inet_arr_value = ch_client.written_logs.items[0].columns.items[38].new_value;
    const col_macaddr_value = ch_client.written_logs.items[0].columns.items[39].new_value;
    const col_macaddr_arr_value = ch_client.written_logs.items[0].columns.items[40].new_value;
    const col_macaddr8_value = ch_client.written_logs.items[0].columns.items[41].new_value;
    const col_macaddr8_arr_value = ch_client.written_logs.items[0].columns.items[42].new_value;

    try testing.expectEqual(1, ch_client.written_logs.items.len);
    try testing.expectEqual(43, ch_client.written_logs.items[0].columns.items.len);

    try testing.expectEqualStrings("1", id_new_values.?);
    try testing.expectEqualStrings("-32768", col_int2_value.?);
    try testing.expectEqualStrings("{-32768,32767}", col_int2_arr_value.?);
    try testing.expectEqualStrings("-2147483648", col_int4_value.?);
    try testing.expectEqualStrings("{-2147483648,2147483647}", col_int4_arr_value.?);
    try testing.expectEqualStrings("-9223372036854775808", col_int8_value.?);
    try testing.expectEqualStrings("{-9223372036854775808,9223372036854775807}", col_int8_arr_value.?);
    try testing.expectEqualStrings("3.5555556", col_float4_value.?);
    try testing.expectEqualStrings("{-3.5555556,3.5555556}", col_float4_arr_value.?);
    try testing.expectEqualStrings("3.5555555555555554", col_float8_value.?);
    try testing.expectEqualStrings("{-3.5555555555555554,3.5555555555555554}", col_float8_arr_value.?);
    try testing.expectEqualStrings("t", col_bool_value.?);
    try testing.expectEqualStrings("{f,t}", col_bool_arr_value.?);
    try testing.expectEqualStrings("sample text", col_text_value.?);
    try testing.expectEqualStrings("{text1,text2}", col_text_arr_value.?);
    try testing.expectEqualStrings("\\xdeadbeef", col_bytea_value.?);
    try testing.expectEqualStrings(
        \\{"\\xdeadbeef","\\xcafebabe"}
    , col_bytea_arr_value.?);
    try testing.expectEqualStrings("val1", col_enum_value.?);
    try testing.expectEqualStrings("{val1,val2}", col_enum_arr_value.?);
    try testing.expectEqualStrings("123e4567-e89b-12d3-a456-426614174000", col_uuid_value.?);
    try testing.expectEqualStrings("{123e4567-e89b-12d3-a456-426614174000,123e4567-e89b-12d3-a456-426614174001}", col_uuid_arr_value.?);
    try testing.expectEqualStrings("12345.6789", col_numeric_value.?);
    try testing.expectEqualStrings("{12345.6789,98765.4321}", col_numeric_arr_value.?);
    try testing.expectEqualStrings("2024-08-02 10:00:00", col_timestamp_value.?);
    try testing.expectEqualStrings("{\"2024-08-02 10:00:00\",\"2024-08-03 10:00:00\"}", col_timestamp_arr_value.?);
    try testing.expectEqualStrings("{\"key\": \"value\"}", col_json_value.?);
    try testing.expectEqualStrings(
        \\{"{\"key\": \"value\"}","{\"key\": \"value2\"}"}
    , col_json_arr_value.?);
    try testing.expectEqualStrings("{\"key\": \"value\"}", col_jsonb_value.?);
    try testing.expectEqualStrings(
        \\{"{\"key\": \"value\"}","{\"key\": \"value2\"}"}
    , col_jsonb_arr_value.?);
    try testing.expectEqualStrings("a", col_char_value.?);
    try testing.expectEqualStrings("{a,b}", col_char_arr_value.?);
    try testing.expectEqualStrings("abc", col_charn_value.?);
    try testing.expectEqualStrings("{ab,cd}", col_charn_arr_value.?);
    try testing.expectEqualStrings("2024-08-02 10:00:00+00", col_timestamptz_value.?);
    try testing.expectEqualStrings("{\"2024-08-02 10:00:00+00\",\"2024-08-03 10:00:00+00\"}", col_timestamptz_arr_value.?);
    try testing.expectEqualStrings("192.168.100.128/25", col_cidr_value.?);
    try testing.expectEqualStrings("{192.168.100.128/25,10.0.0.0/8}", col_cidr_arr_value.?);
    try testing.expectEqualStrings("192.168.0.1", col_inet_value.?);
    try testing.expectEqualStrings("{192.168.0.1,10.0.0.1}", col_inet_arr_value.?);
    try testing.expectEqualStrings("08:00:2b:01:02:03", col_macaddr_value.?);
    try testing.expectEqualStrings("{08:00:2b:01:02:03,08:00:2b:01:02:04}", col_macaddr_arr_value.?);
    try testing.expectEqualStrings("08:00:2b:01:02:03:04:05", col_macaddr8_value.?);
    try testing.expectEqualStrings("{08:00:2b:01:02:03:04:05,08:00:2b:01:02:03:04:06}", col_macaddr8_arr_value.?);
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

    var pg_client = try PgClient.init(allocator, io, .{
        .port = 5432,
        .host = "localhost",
        .wal = wal_name,
        .username = "db_rw",
        .password = "12345678",
        .database = db_name,
        .application_name = "Ergo test",
        .timeout_ms = 10_000,
        .startup_parameters = null
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

    const id = ch_client.written_logs.items[1].columns.items[0];
    const col_int2 = ch_client.written_logs.items[1].columns.items[1];
    const col_int2_arr = ch_client.written_logs.items[1].columns.items[2];
    const col_int4 = ch_client.written_logs.items[1].columns.items[3];
    const col_int4_arr = ch_client.written_logs.items[1].columns.items[4];
    const col_int8 = ch_client.written_logs.items[1].columns.items[5];
    const col_int8_arr = ch_client.written_logs.items[1].columns.items[6];
    const col_float4 = ch_client.written_logs.items[1].columns.items[7];
    const col_float4_arr = ch_client.written_logs.items[1].columns.items[8];
    const col_float8 = ch_client.written_logs.items[1].columns.items[9];
    const col_float8_arr = ch_client.written_logs.items[1].columns.items[10];
    const col_bool = ch_client.written_logs.items[1].columns.items[11];
    const col_bool_arr = ch_client.written_logs.items[1].columns.items[12];
    const col_text = ch_client.written_logs.items[1].columns.items[13];
    const col_text_arr = ch_client.written_logs.items[1].columns.items[14];
    const col_bytea = ch_client.written_logs.items[1].columns.items[15];
    const col_bytea_arr = ch_client.written_logs.items[1].columns.items[16];
    const col_enum = ch_client.written_logs.items[1].columns.items[17];
    const col_enum_arr = ch_client.written_logs.items[1].columns.items[18];
    const col_uuid = ch_client.written_logs.items[1].columns.items[19];
    const col_uuid_arr = ch_client.written_logs.items[1].columns.items[20];
    const col_numeric = ch_client.written_logs.items[1].columns.items[21];
    const col_numeric_arr = ch_client.written_logs.items[1].columns.items[22];
    const col_timestamp = ch_client.written_logs.items[1].columns.items[23];
    const col_timestamp_arr = ch_client.written_logs.items[1].columns.items[24];
    const col_json = ch_client.written_logs.items[1].columns.items[25];
    const col_json_arr = ch_client.written_logs.items[1].columns.items[26];
    const col_jsonb = ch_client.written_logs.items[1].columns.items[27];
    const col_jsonb_arr = ch_client.written_logs.items[1].columns.items[28];
    const col_char = ch_client.written_logs.items[1].columns.items[29];
    const col_char_arr = ch_client.written_logs.items[1].columns.items[30];
    const col_charn = ch_client.written_logs.items[1].columns.items[31];
    const col_charn_arr = ch_client.written_logs.items[1].columns.items[32];
    const col_timestamptz = ch_client.written_logs.items[1].columns.items[33];
    const col_timestamptz_arr = ch_client.written_logs.items[1].columns.items[34];
    const col_cidr = ch_client.written_logs.items[1].columns.items[35];
    const col_cidr_arr = ch_client.written_logs.items[1].columns.items[36];
    const col_inet = ch_client.written_logs.items[1].columns.items[37];
    const col_inet_arr = ch_client.written_logs.items[1].columns.items[38];
    const col_macaddr = ch_client.written_logs.items[1].columns.items[39];
    const col_macaddr_arr = ch_client.written_logs.items[1].columns.items[40];
    const col_macaddr8 = ch_client.written_logs.items[1].columns.items[41];
    const col_macaddr8_arr = ch_client.written_logs.items[1].columns.items[42];

    try testing.expectEqual(2, ch_client.written_logs.items.len);
    try testing.expectEqual(43, ch_client.written_logs.items[0].columns.items.len);

    try testing.expectEqualStrings("id", id.column_name);
    try testing.expectEqualStrings("col_int2", col_int2.column_name);
    try testing.expectEqualStrings("col_int2_arr", col_int2_arr.column_name);
    try testing.expectEqualStrings("col_int4", col_int4.column_name);
    try testing.expectEqualStrings("col_int4_arr", col_int4_arr.column_name);
    try testing.expectEqualStrings("col_int8", col_int8.column_name);
    try testing.expectEqualStrings("col_int8_arr", col_int8_arr.column_name);
    try testing.expectEqualStrings("col_float4", col_float4.column_name);
    try testing.expectEqualStrings("col_float4_arr", col_float4_arr.column_name);
    try testing.expectEqualStrings("col_float8", col_float8.column_name);
    try testing.expectEqualStrings("col_float8_arr", col_float8_arr.column_name);
    try testing.expectEqualStrings("col_bool", col_bool.column_name);
    try testing.expectEqualStrings("col_bool_arr", col_bool_arr.column_name);
    try testing.expectEqualStrings("col_text", col_text.column_name);
    try testing.expectEqualStrings("col_text_arr", col_text_arr.column_name);
    try testing.expectEqualStrings("col_bytea", col_bytea.column_name);
    try testing.expectEqualStrings("col_bytea_arr", col_bytea_arr.column_name);
    try testing.expectEqualStrings("col_enum", col_enum.column_name);
    try testing.expectEqualStrings("col_enum_arr", col_enum_arr.column_name);
    try testing.expectEqualStrings("col_uuid", col_uuid.column_name);
    try testing.expectEqualStrings("col_uuid_arr", col_uuid_arr.column_name);
    try testing.expectEqualStrings("col_numeric", col_numeric.column_name);
    try testing.expectEqualStrings("col_numeric_arr", col_numeric_arr.column_name);
    try testing.expectEqualStrings("col_timestamp", col_timestamp.column_name);
    try testing.expectEqualStrings("col_timestamp_arr", col_timestamp_arr.column_name);
    try testing.expectEqualStrings("col_json", col_json.column_name);
    try testing.expectEqualStrings("col_json_arr", col_json_arr.column_name);
    try testing.expectEqualStrings("col_jsonb", col_jsonb.column_name);
    try testing.expectEqualStrings("col_jsonb_arr", col_jsonb_arr.column_name);
    try testing.expectEqualStrings("col_char", col_char.column_name);
    try testing.expectEqualStrings("col_char_arr", col_char_arr.column_name);
    try testing.expectEqualStrings("col_charn", col_charn.column_name);
    try testing.expectEqualStrings("col_charn_arr", col_charn_arr.column_name);
    try testing.expectEqualStrings("col_timestamptz", col_timestamptz.column_name);
    try testing.expectEqualStrings("col_timestamptz_arr", col_timestamptz_arr.column_name);
    try testing.expectEqualStrings("col_cidr", col_cidr.column_name);
    try testing.expectEqualStrings("col_cidr_arr", col_cidr_arr.column_name);
    try testing.expectEqualStrings("col_inet", col_inet.column_name);
    try testing.expectEqualStrings("col_inet_arr", col_inet_arr.column_name);
    try testing.expectEqualStrings("col_macaddr", col_macaddr.column_name);
    try testing.expectEqualStrings("col_macaddr_arr", col_macaddr_arr.column_name);
    try testing.expectEqualStrings("col_macaddr8", col_macaddr8.column_name);
    try testing.expectEqualStrings("col_macaddr8_arr", col_macaddr8_arr.column_name);

    try testing.expectEqualStrings("1", id.new_value.?);
    try testing.expectEqual(null, col_int2.new_value);
    try testing.expectEqual(null, col_int2_arr.new_value);
    try testing.expectEqual(null, col_int4.new_value);
    try testing.expectEqual(null, col_int4_arr.new_value);
    try testing.expectEqual(null, col_int8.new_value);
    try testing.expectEqual(null, col_int8_arr.new_value);
    try testing.expectEqual(null, col_float4.new_value);
    try testing.expectEqual(null, col_float4_arr.new_value);
    try testing.expectEqual(null, col_float8.new_value);
    try testing.expectEqual(null, col_float8_arr.new_value);
    try testing.expectEqual(null, col_bool.new_value);
    try testing.expectEqual(null, col_bool_arr.new_value);
    try testing.expectEqual(null, col_text.new_value);
    try testing.expectEqual(null, col_text_arr.new_value);
    try testing.expectEqual(null, col_bytea.new_value);
    try testing.expectEqual(null, col_bytea_arr.new_value);
    try testing.expectEqual(null, col_enum.new_value);
    try testing.expectEqual(null, col_enum_arr.new_value);
    try testing.expectEqual(null, col_uuid.new_value);
    try testing.expectEqual(null, col_uuid_arr.new_value);
    try testing.expectEqual(null, col_numeric.new_value);
    try testing.expectEqual(null, col_numeric_arr.new_value);
    try testing.expectEqual(null, col_timestamp.new_value);
    try testing.expectEqual(null, col_timestamp_arr.new_value);
    try testing.expectEqual(null, col_json.new_value);
    try testing.expectEqual(null, col_json_arr.new_value);
    try testing.expectEqual(null, col_jsonb.new_value);
    try testing.expectEqual(null, col_jsonb_arr.new_value);
    try testing.expectEqual(null, col_char.new_value);
    try testing.expectEqual(null, col_char_arr.new_value);
    try testing.expectEqual(null, col_charn.new_value);
    try testing.expectEqual(null, col_charn_arr.new_value);
    try testing.expectEqual(null, col_timestamptz.new_value);
    try testing.expectEqual(null, col_timestamptz_arr.new_value);
    try testing.expectEqual(null, col_cidr.new_value);
    try testing.expectEqual(null, col_cidr_arr.new_value);
    try testing.expectEqual(null, col_inet.new_value);
    try testing.expectEqual(null, col_inet_arr.new_value);
    try testing.expectEqual(null, col_macaddr.new_value);
    try testing.expectEqual(null, col_macaddr_arr.new_value);
    try testing.expectEqual(null, col_macaddr8.new_value);
    try testing.expectEqual(null, col_macaddr8_arr.new_value);
}

test "startStreaming: insert all types to null write correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    const db_name = try t.genereateDbName(allocator, io);
    defer allocator.free(db_name);

    try t.createTestChDb(allocator, io, db_name);
    defer t.teardownTestChDb(allocator, io, db_name) catch {};

    var columns: std.ArrayList(types.ColumnChange) = .empty;
    try columns.ensureUnusedCapacity(allocator, 6);

    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = try allocator.dupe(u8, "1"), .new_value = try allocator.dupe(u8, "1"), .column_name = try allocator.dupe(u8, "id"), .is_key = true });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "Googleplex"), .new_value = try allocator.dupe(u8, "1 Apple Park Way"), .column_name = try allocator.dupe(u8, "address_line_1"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = null, .new_value = null, .column_name = try allocator.dupe(u8, "address_line_2"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "94043"), .new_value = try allocator.dupe(u8, "95014"), .column_name = try allocator.dupe(u8, "postal_code"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "Mountain View"), .new_value = try allocator.dupe(u8, "Cupertino"), .column_name = try allocator.dupe(u8, "city"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = try allocator.dupe(u8, "US"), .new_value = try allocator.dupe(u8, "US"), .column_name = try allocator.dupe(u8, "country"), .is_key = false });

    var res = [_]ReadResponse{
        .{ 
            .data = .{
                .event_time = undefined,
                .table_name = try allocator.dupe(u8, "test.addresses"),
                .action = 1,
                .columns = columns,
                .transaction_id = 793,
                .user_id = try allocator.dupe(u8, "42"),
                .ip_address = try allocator.dupe(u8, "192.168.1.50"),
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
        "--query", "SELECT action, table_name, primary_keys, changed_columns, old_values, new_values, user_id, ip_address FROM entries ORDER BY primary_keys, action DESC" 
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
        "INSERT\ttest.addresses\t{'id':'1'}\t['address_line_1','postal_code','city']\t{'address_line_1':'Googleplex','city':'Mountain View','postal_code':'94043'}\t{'address_line_1':'1 Apple Park Way','city':'Cupertino','postal_code':'95014'}\t42\t192.168.1.50\n",
        ch_assert_result.stdout,
    );
}
