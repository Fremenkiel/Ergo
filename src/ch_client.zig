const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const net = Io.net;
const mem = std.mem;

const assert = std.debug.assert;

const ch = @import("ch");

const types = @import("types.zig");

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

const InsertValues = struct {
    row: types.AuditEntry,
    changed_columns: *std.ArrayList(ch.bulk_insert.Value),
    old_values: *std.StringHashMap([]const u8),
    new_values: *std.StringHashMap([]const u8),
};

pub const ChClient = struct {
    allocator: mem.Allocator,
    io: std.Io,

    config: ch.ClickHouseConfig,
    os_user: []const u8,

    stream: ?net.Stream,
    stream_reader: ?@TypeOf(@as(net.Stream, undefined).reader(@as(Io, undefined), @as(*[8192]u8, undefined))) = null,
    stream_writer: ?@TypeOf(@as(net.Stream, undefined).writer(@as(Io, undefined), @as(*[4096]u8, undefined))) = null,

    reader: ?*Io.Reader = null,
    writer: ?*Io.Writer = null,

    read_buf: [8192]u8 = undefined,
    write_buf: [4096]u8 = undefined,

    current_block: ch.block.Block,
    server_info: ?ch.server_info.ServerInfo,
    last_error: ?*ch.ch_error.Error,

    pub fn init(allocator: mem.Allocator, io: std.Io, config: ch.ClickHouseConfig, os_user: []const u8) ChClient {
        return .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .stream = null,
            .current_block = .init(allocator),
            .server_info = null,
            .last_error = null,
            .os_user = os_user,
        };
    }

    pub fn deinit(self: *ChClient) void {
        self.current_block.deinit();
        if (self.server_info) |*info| {
            info.deinit(self.allocator);
        }
        if (self.last_error) |err| {
            err.deinit();
        }
        if (self.stream) |stream| {
            stream.close(self.io);
        }
    }

    pub fn connect(self: *ChClient) !void {
        const is_unix = self.config.host.len > 0 and self.config.host[0] == '/';

        const stream = try blk: {
            if (is_unix) {
                if (comptime Io.net.has_unix_sockets == false or std.posix.AF == void) {
                    return error.UnixPathNotSupported;
                }
                const addr: Io.net.UnixAddress = try .init(self.config.host);
                break :blk addr.connect(self.io);
            }
            const hostname: Io.net.HostName = try .init(self.config.host);
            break :blk hostname.connect(self.io, self.config.port, .{ .mode = .stream });
        };
        errdefer stream.close(self.io);

        self.stream = stream;

        self.stream_reader = self.stream.?.reader(self.io, &self.read_buf);
        self.reader = &self.stream_reader.?.interface;

        self.stream_writer = self.stream.?.writer(self.io, &self.write_buf);
        self.writer = &self.stream_writer.?.interface;

        try self.sendHello();
        try self.readServerHello();
    }

    pub fn disconnect(self: *ChClient) void {
        if (self.stream) |stream| {
            stream.close(self.io);
        }
        self.stream = null;
        self.reader = null;
        self.writer = null;
        
        self.read_buf = undefined;
        self.write_buf = undefined;
    }

    fn ensureStream(self: *@This()) !void {
        if (self.reader != null and self.writer != null) return;

        if (self.stream == null) {
            try self.connect();
        }

        if (self.stream_reader == null) {
            self.read_buf = undefined;
            self.stream_reader = self.stream.?.reader(self.io, &self.read_buf);
            
            if (self.stream_reader) |*r| {
                self.reader = &r.interface;
            }
        }

        if (self.stream_writer == null) {
            self.write_buf = undefined;
            self.stream_writer = self.stream.?.writer(self.io, &self.write_buf);
            
            if (self.stream_writer) |*w| {
                self.writer = &w.interface;
            }
        }
    }

    fn sendHello(self: *ChClient) !void {
        if (self.writer == null) return ch.ClickHouseError.ConnectionFailed;

        try ch.packet.writeClientPacketHeader(self.writer.?, .Hello);
        try ch.protocol.ClientHello.write(self.writer.?);

        try self.writer.?.writeInt(u8, @as(u8, @truncate(self.config.database.len)), .little);
        try self.writer.?.writeAll(self.config.database);

        try self.writer.?.writeInt(u8, @as(u8, @truncate(self.config.username.len)), .little);
        try self.writer.?.writeAll(self.config.username);

        try self.writer.?.writeInt(u8, @as(u8, @truncate(self.config.password.len)), .little);
        try self.writer.?.writeAll(self.config.password);

        try self.writer.?.flush();
    }

    fn readServerHello(self: *ChClient) !void {
        if (self.reader == null) return ch.ClickHouseError.ConnectionFailed;

        const server_packet = try ch.protocol.readVarInt(self.reader.?);

        if (server_packet != @intFromEnum(ch.packet.ServerPacket.Hello)) {
            return ch.ClickHouseError.ProtocolError;
        }

        self.server_info = try ch.server_info.ServerInfo.read(self.allocator, self.reader.?);
    }

    pub fn readBlock(self: *ChClient) !void {
        try self.ensureStream();

        const revision = if (self.server_info) |info| info.revision else 0;

        if (revision >= 50264) {
            // table_name
            _ = try ch.protocol.readString(self.reader.?);
        }

        while (true) {
            const field_num = try ch.protocol.readVarInt(self.reader.?);
            if (field_num == 0) break;
            if (field_num == 1) {
                _ = try self.reader.?.takeByte(); // is_overflows
            } else if (field_num == 2) {
                _ = try self.reader.?.takeInt(i32, .little); // bucket_num
            }
        }

        const num_columns = try ch.protocol.readVarInt(self.reader.?);
        const num_rows = try ch.protocol.readVarInt(self.reader.?);

        self.current_block.rows = num_rows;
        for (0..num_columns) |_| {
            const col_name = try ch.protocol.readString(self.reader.?);
            const col_type_str = try ch.protocol.readString(self.reader.?);

            try self.current_block.addColumn(col_name, col_type_str);
        }
    }

    pub fn startInsert(self: *ChClient, query_str: []const u8) !void {
        try self.ensureStream();

        const address = try std.fmt.allocPrint(self.allocator, "[::ffff:127.0.0.1]:{d}", .{ self.config.port });
        defer self.allocator.free(address);

        const timestamp = std.Io.Clock.real.now(self.io).toMicroseconds();
        const query_id = try std.fmt.allocPrint(self.allocator, "ergo_bulk_{d}", .{timestamp});
        self.allocator.free(query_id);

        try ch.packet.writeClientPacketHeader(self.writer.?, .Query);
        try ch.protocol.writeString(self.writer.?, query_id);

        try ch.protocol.ClientInfo.write(self.writer.?, query_id, self.config.username, address, timestamp, self.os_user);
        try ch.protocol.writeString(self.writer.?, ""); // Empty settings
        try ch.protocol.writeString(self.writer.?, ""); // auth_hash

        try ch.protocol.writeVarInt(self.writer.?, 2); // stage: Complete
        try ch.protocol.writeVarInt(self.writer.?, self.config.settings.compression_method); // compression enabled

        try ch.protocol.writeString(self.writer.?, query_str);

        // Empty Data Block
        try ch.packet.writeClientPacketHeader(self.writer.?, .Data);
        try ch.protocol.writeString(self.writer.?, ""); // Block name
        
        try ch.protocol.writeVarInt(self.writer.?, 1); // field: is_overflows
        try self.writer.?.writeInt(u8, 0, .little); // is_overflows
        try ch.protocol.writeVarInt(self.writer.?, 2); // field: bucket_num
        try self.writer.?.writeInt(i32, -1, .little); // bucket_num
        try ch.protocol.writeVarInt(self.writer.?, 0); // END
        
        try ch.protocol.writeVarInt(self.writer.?, 0); // columns = 0
        try ch.protocol.writeVarInt(self.writer.?, 0); // rows = 0
        try self.writer.?.flush();

        while (true) {
            const packet_type = try ch.protocol.readVarInt(self.reader.?);
            switch (@as(ch.packet.ServerPacket, @enumFromInt(packet_type))) {
                .Data => {
                    try self.readBlock();
                    return; // Schema received, ready for bulk push
                },
                .TableColumns => {
                    _ = try ch.protocol.readString(self.reader.?);
                    _ = try ch.protocol.readString(self.reader.?);
                },
                .Exception => {
                    const err_code = try self.reader.?.takeInt(u32, .little);
                    const name = try ch.protocol.readString(self.reader.?);
                    _ = name;
                    const msg = try ch.protocol.readString(self.reader.?);
                    const stack = try ch.protocol.readString(self.reader.?);
                    
                    _ = try self.reader.?.takeByte();

                    self.last_error = try ch.ch_error.Error.initWithStack(
                        self.allocator,
                        ch.ch_error.ErrorCode.fromInt(err_code),
                        msg,
                        stack,
                    );

                    return ch.ClickHouseError.QueryFailed;
                },
                else => {
                }
            }
        }
    }

    pub fn processQueryResponse(self: *ChClient) !void {
        try self.ensureStream();

        while (true) {
            const packet_type = try ch.protocol.readVarInt(self.reader.?);
            
            switch (@as(ch.packet.ServerPacket, @enumFromInt(packet_type))) {
                .Data => {
                    try self.readBlock();
                },
                .Progress => {
                    _ = try self.reader.?.takeInt(u64, .little); // rows
                    _ = try self.reader.?.takeInt(u64, .little); // bytes
                    _ = try self.reader.?.takeInt(u64, .little); // total_rows
                    _ = try self.reader.?.takeInt(u64, .little); // written_rows
                    _ = try self.reader.?.takeInt(u64, .little); // written_bytes
                    _ = try self.reader.?.takeInt(u64, .little); // elapsed_ns
                },
                .TableColumns => {
                    _ = try ch.protocol.readString(self.reader.?);
                    _ = try ch.protocol.readString(self.reader.?);
                },
                .Log => {
                    // table_name - always empty
                    const table_name = try ch.protocol.readString(self.reader.?);
                    assert(std.mem.eql(u8, "", table_name));

                    const num_columns = try ch.protocol.readVarInt(self.reader.?);
                    assert(num_columns == 8);

                    const num_rows = try ch.protocol.readVarInt(self.reader.?);

                    var i: u32 = 0;
                    while (i < num_rows) : (i += 1) {
                        // event_time
                        _ = try self.reader.?.takeInt(u32, .little);
                        // event_time_microseconds
                        _ = try self.reader.?.takeInt(u32, .little);
                        // host_name
                        _ = try ch.protocol.readString(self.reader.?);
                        // query_id
                        _ = try ch.protocol.readString(self.reader.?);
                        // thread_id
                        _ = try self.reader.?.takeInt(u64, .little);
                        // priority
                        _ = try self.reader.?.takeInt(i8, .little);
                        // source
                        _ = try ch.protocol.readString(self.reader.?);
                        // text
                        _ = try ch.protocol.readString(self.reader.?);
                    }
                },
                .EndOfStream => {
                    return;
                },
                .Exception => {
                    const err_code = try self.reader.?.takeInt(u32, .little);
                    _ = try ch.protocol.readString(self.reader.?);
                    const msg = try ch.protocol.readString(self.reader.?);
                    const stack = try ch.protocol.readString(self.reader.?);
                    
                    _ = try self.reader.?.takeByte(); // has_nested

                    self.last_error = try ch.ch_error.Error.initWithStack(
                        self.allocator,
                        ch.ch_error.ErrorCode.fromInt(err_code),
                        msg,
                        stack,
                    );

                    return ch.ClickHouseError.QueryFailed;
                },
                else => {},
            }
        }
    }

    pub fn writeLog(self: *@This(), data: []types.AuditEntry) !void {
        var bulk: ch.BulkInsert = try .init(self.allocator, "entries", &columns, 1000);
        defer bulk.deinit();

        self.startInsert("INSERT INTO entries FORMAT Native") catch |err| {
            if (err == error.QueryFailed) {
                std.debug.print("Error: Query failed, {s}\n", .{self.last_error.?.message});
            }
            return err;
        };

        var buf = std.ArrayList(ch.bulk_insert.Value).empty;
        defer buf.deinit(self.allocator);

        var old_values = std.StringHashMap([]const u8).init(self.allocator);
        defer old_values.deinit();

        var new_values = std.StringHashMap([]const u8).init(self.allocator);
        defer new_values.deinit();

        for (data) |row| {
            const insert_values = try self.parseRow(row, &buf, &old_values, &new_values);

            try self.insertRow(&bulk, insert_values);
        }

        // Flush any remaining rows
        bulk.flush(self.io, self.stream.?) catch |err| {
            std.debug.print("Error: Flush failed, {}\n", .{err});
            self.processQueryResponse() catch {};
            if (self.last_error) |e| {
                std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
            }
            return err;
        };

        self.endOfStream() catch |err| {
            std.debug.print("Error: Close failed, {}\n", .{err});
            if (self.last_error) |e| {
                std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
            }
            return err;
        };

        if (self.last_error) |err| {
            std.debug.print("Error: Insert failed, {s}\n", .{err.message});
        }
    }

    pub fn parseRow(self: *@This(), row: types.AuditEntry, changed_columns: *std.ArrayList(ch.bulk_insert.Value), old_values: *std.StringHashMap([]const u8), new_values: *std.StringHashMap([]const u8)) !InsertValues {
        changed_columns.clearRetainingCapacity();
        old_values.clearRetainingCapacity();
        new_values.clearRetainingCapacity();

        try changed_columns.ensureUnusedCapacity(self.allocator, row.changed_columns.count());
        if (row.old_values.count() > 0) { try old_values.ensureUnusedCapacity(row.old_values.count()); }
        if (row.new_values.count() > 0) { try new_values.ensureUnusedCapacity(row.new_values.count()); }

        var it = row.changed_columns.iterator();
        while (it.next()) |col| {
            if (!col.value_ptr.*.has_changes) continue;

            changed_columns.appendAssumeCapacity(.{ .String = col.key_ptr.* });

            if (row.old_values.capacity() > 0) {
                if (row.old_values.get(col.key_ptr.*)) |val| {
                    try old_values.put(col.key_ptr.*, val);
                }
            }

            if (row.new_values.capacity() > 0) {
                if (row.new_values.get(col.key_ptr.*)) |val| {
                    try new_values.put(col.key_ptr.*, val);
                }
            }
        }

        // TODO: change the type to not contain row
        // Does not feal right
        return .{
            .row = row,
            .changed_columns = changed_columns,
            .old_values = old_values,
            .new_values = new_values,
        };
    }

    pub fn insertRow(self: *@This(), bulk: *ch.BulkInsert, insert_values: InsertValues) !void {
        const values = [_]ch.bulk_insert.Value{
            .{ .DateTime64 = insert_values.row.event_time },
            .{ .UInt64 = insert_values.row.transaction_id },
            .{ .String = insert_values.row.user_id},
            .{ .LowCardinality = insert_values.row.table_name },
            .{ .Enum8 = insert_values.row.action },
            .{ .String = insert_values.row.primary_key },
            .{ .Array = insert_values.changed_columns.items },
            .{ .Map = insert_values.old_values.* },
            .{ .Map = insert_values.new_values.* },
            .{ .IPv4 = insert_values.row.ip_address },
        };

        if (try bulk.addRow(&values)) {
            bulk.flush(self.io, self.stream.?) catch |err| {
                std.debug.print("Error: Flush failed, {}\n", .{err});
                self.processQueryResponse() catch {};
                if (self.last_error) |e| {
                    std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
                }
                return err;
            };
        }
    }

    pub fn endOfStream(self: *ChClient) !void {
        try self.ensureStream();

        try ch.packet.writeClientPacketHeader(self.writer.?, .Data);
        try ch.protocol.writeString(self.writer.?, ""); // block name
        try ch.protocol.writeVarInt(self.writer.?, 1); // is_overflows
        try self.writer.?.writeInt(u8, 0, .little);
        try ch.protocol.writeVarInt(self.writer.?, 2); // bucket_num
        try self.writer.?.writeInt(i32, -1, .little);
        try ch.protocol.writeVarInt(self.writer.?, 0); // end block info

        try ch.protocol.writeVarInt(self.writer.?, 0); // num_columns = 0
        try ch.protocol.writeVarInt(self.writer.?, 0); // num_rows = 0
        try self.writer.?.flush();

        try self.processQueryResponse();
    }
};

// Tests
fn setupMockClient(allocator: std.mem.Allocator, io: std.Io) !ChClient {
    return .{
        .allocator = allocator,
        .io = io,
        .config = .{
            .host = "localhost",
            .port = 9000,
            .username = "default",
            .password = "clickhouse",
            .database = "audit_log",
        },
        .stream = null,
        .stream_reader = null,
        .stream_writer = null,
        .reader = null,
        .writer = null,
        .current_block = .init(allocator),
        .server_info = null,
        .last_error = null,
        .os_user = "kswa",
    };
}

fn setupFixedReaderWriterMockClient(allocator: std.mem.Allocator, io: std.Io) !ChClient {
    var client = try setupMockClient(allocator, io);

    const buffer = try allocator.alloc(u8, 4096);

    client.reader = try allocator.create(std.Io.Reader);
    client.reader.?.* = std.Io.Reader.fixed(buffer);

    client.writer = try allocator.create(std.Io.Writer);
    client.writer.?.* = std.Io.Writer.fixed(buffer);

    return client;
}

fn teardownFixedReaderWriterMockClient(allocator: std.mem.Allocator, client: *ChClient) void {
    if (client.writer) |w| {
        allocator.free(w.buffer);
        allocator.destroy(w);
    }
    if (client.reader) |r| {
        allocator.destroy(r);
    }
    client.deinit();
}

fn writeMockDataBlock(client: *ChClient) !void {
    try ch.protocol.writeVarInt(client.writer.?, 1);
    try client.writer.?.writeByte(0); // is_overflows

    try ch.protocol.writeVarInt(client.writer.?, 2);
    try client.writer.?.writeInt(i32, 0, .little); // bucket_num
    
    try ch.protocol.writeVarInt(client.writer.?, 0);

    const num_columns = 10;
    const num_rows = 26;

    try ch.protocol.writeVarInt(client.writer.?, num_columns);
    try ch.protocol.writeVarInt(client.writer.?, num_rows);

    try ch.protocol.writeString(client.writer.?, columns[0].name);
    try ch.protocol.writeString(client.writer.?, columns[0].type_str);
    try ch.protocol.writeString(client.writer.?, columns[1].name);
    try ch.protocol.writeString(client.writer.?, columns[1].type_str);
    try ch.protocol.writeString(client.writer.?, columns[2].name);
    try ch.protocol.writeString(client.writer.?, columns[2].type_str);
    try ch.protocol.writeString(client.writer.?, columns[3].name);
    try ch.protocol.writeString(client.writer.?, columns[3].type_str);
    try ch.protocol.writeString(client.writer.?, columns[4].name);
    try ch.protocol.writeString(client.writer.?, columns[4].type_str);
    try ch.protocol.writeString(client.writer.?, columns[5].name);
    try ch.protocol.writeString(client.writer.?, columns[5].type_str);
    try ch.protocol.writeString(client.writer.?, columns[6].name);
    try ch.protocol.writeString(client.writer.?, columns[6].type_str);
    try ch.protocol.writeString(client.writer.?, columns[7].name);
    try ch.protocol.writeString(client.writer.?, columns[7].type_str);
    try ch.protocol.writeString(client.writer.?, columns[8].name);
    try ch.protocol.writeString(client.writer.?, columns[8].type_str);
    try ch.protocol.writeString(client.writer.?, columns[9].name);
    try ch.protocol.writeString(client.writer.?, columns[9].type_str);
}

test "ensureStream recover lost connect" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    try std.testing.expect(client.stream == null);
    try std.testing.expect(client.stream_reader == null);
    try std.testing.expect(client.stream_writer == null);
    try std.testing.expect(client.reader == null);
    try std.testing.expect(client.writer == null);

    try client.ensureStream();

    try std.testing.expect(client.stream != null);
    try std.testing.expect(client.stream_reader != null);
    try std.testing.expect(client.stream_writer != null);
    try std.testing.expect(client.reader != null);
    try std.testing.expect(client.writer != null);
}

test "sendHello ensure correct encoding" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupFixedReaderWriterMockClient(allocator, io);
    defer teardownFixedReaderWriterMockClient(allocator, &client);

    try client.sendHello();

    const packet_type = try ch.protocol.readVarInt(client.reader.?);
    try std.testing.expectEqual(@intFromEnum(ch.packet.ClientPacket.Hello), packet_type);

    const name = try ch.protocol.readString(client.reader.?);
    try std.testing.expectEqualStrings(ch.protocol.CLIENT_NAME, name);

    const major_version = try ch.protocol.readVarInt(client.reader.?);
    try std.testing.expectEqual(ch.protocol.CLIENT_VERSION_MAJOR, major_version);

    const minor_version = try ch.protocol.readVarInt(client.reader.?);
    try std.testing.expectEqual(ch.protocol.CLIENT_VERSION_MINOR, minor_version);

    const protocol = try ch.protocol.readVarInt(client.reader.?);
    try std.testing.expectEqual(ch.protocol.PROTOCOL_VERSION, protocol);

    const config_db_len = try client.reader.?.takeInt(u8, .little);
    try std.testing.expectEqual(client.config.database.len, config_db_len);

    const config_db = try client.reader.?.take(config_db_len);
    try std.testing.expectEqualStrings(client.config.database, config_db);

    const config_user_len = try client.reader.?.takeInt(u8, .little);
    try std.testing.expectEqual(client.config.username.len, config_user_len);

    const config_user = try client.reader.?.take(config_user_len);
    try std.testing.expectEqualStrings(client.config.username, config_user);

    const config_pass_len = try client.reader.?.takeInt(u8, .little);
    try std.testing.expectEqual(client.config.password.len, config_pass_len);

    const config_pass = try client.reader.?.take(config_pass_len);
    try std.testing.expectEqualStrings(client.config.password, config_pass);
}

test "readServerHello ensure correct decoding" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupFixedReaderWriterMockClient(allocator, io);
    defer teardownFixedReaderWriterMockClient(allocator, &client);

    const server_name = "Test ch server";
    const major_version: u64 = 1;
    const minor_version: u64 = 6;
    const revision: u64 = 6;

    const tz = "utc";
    const display = "Main";
    const version_patch = 453;

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.Hello));
    try ch.protocol.writeString(client.writer.?, server_name);
    try ch.protocol.writeVarInt(client.writer.?, major_version);
    try ch.protocol.writeVarInt(client.writer.?, minor_version);
    try ch.protocol.writeVarInt(client.writer.?, revision);
    try ch.protocol.writeString(client.writer.?, tz);
    try ch.protocol.writeString(client.writer.?, display);
    try ch.protocol.writeVarInt(client.writer.?, version_patch);

    try client.readServerHello();

    try std.testing.expect(client.server_info != null);
    try std.testing.expectEqual(client.writer.?.end, client.reader.?.seek);
    try std.testing.expectEqualStrings(server_name, client.server_info.?.name);
    try std.testing.expectEqual(major_version, client.server_info.?.major_version);
    try std.testing.expectEqual(minor_version, client.server_info.?.minor_version);
    try std.testing.expectEqual(revision, client.server_info.?.revision);
    try std.testing.expectEqualStrings(tz, client.server_info.?.timezone);
    try std.testing.expectEqualStrings(display, client.server_info.?.display_name);
    try std.testing.expectEqual(version_patch, client.server_info.?.version_patch);
}

test "startInsert ensure correct query info" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const user_env_key = if (builtin.os.tag == .windows) "USERNAME" else "USER";
    const os_user = try std.testing.environ.getAlloc(allocator, user_env_key);
    defer allocator.free(os_user);

    var client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    }, os_user);
    defer client.deinit();

    try client.connect();
    defer client.disconnect();

    try client.startInsert("INSERT INTO entries FORMAT Native");

    try std.testing.expectEqual(10, client.current_block.columns.len);
}

test "readBlock ensure correct read | non-null current_block" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupFixedReaderWriterMockClient(allocator, io);
    defer teardownFixedReaderWriterMockClient(allocator, &client);

    try writeMockDataBlock(&client);

    try client.readBlock();

    try std.testing.expectEqual(10, client.current_block.columns.len);
    try std.testing.expectEqual(26, client.current_block.rows);
    try std.testing.expectEqual(client.writer.?.end, client.reader.?.seek);

    try std.testing.expectEqualStrings(columns[0].name, client.current_block.columns[0].name);
    try std.testing.expectEqualStrings(columns[0].type_str, client.current_block.columns[0].type_name);
    try std.testing.expectEqualStrings(columns[1].name, client.current_block.columns[1].name);
    try std.testing.expectEqualStrings(columns[1].type_str, client.current_block.columns[1].type_name);
    try std.testing.expectEqualStrings(columns[2].name, client.current_block.columns[2].name);
    try std.testing.expectEqualStrings(columns[2].type_str, client.current_block.columns[2].type_name);
    try std.testing.expectEqualStrings(columns[3].name, client.current_block.columns[3].name);
    try std.testing.expectEqualStrings(columns[3].type_str, client.current_block.columns[3].type_name);
    try std.testing.expectEqualStrings(columns[4].name, client.current_block.columns[4].name);
    try std.testing.expectEqualStrings(columns[4].type_str, client.current_block.columns[4].type_name);
    try std.testing.expectEqualStrings(columns[5].name, client.current_block.columns[5].name);
    try std.testing.expectEqualStrings(columns[5].type_str, client.current_block.columns[5].type_name);
    try std.testing.expectEqualStrings(columns[6].name, client.current_block.columns[6].name);
    try std.testing.expectEqualStrings(columns[6].type_str, client.current_block.columns[6].type_name);
    try std.testing.expectEqualStrings(columns[7].name, client.current_block.columns[7].name);
    try std.testing.expectEqualStrings(columns[7].type_str, client.current_block.columns[7].type_name);
    try std.testing.expectEqualStrings(columns[8].name, client.current_block.columns[8].name);
    try std.testing.expectEqualStrings(columns[8].type_str, client.current_block.columns[8].type_name);
    try std.testing.expectEqualStrings(columns[9].name, client.current_block.columns[9].name);
    try std.testing.expectEqualStrings(columns[9].type_str, client.current_block.columns[9].type_name);
}

test "processQueryResponse ensure correct read | Data" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupFixedReaderWriterMockClient(allocator, io);
    defer teardownFixedReaderWriterMockClient(allocator, &client);

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.Data));

    try writeMockDataBlock(&client);

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.EndOfStream));

    try client.processQueryResponse();

    try std.testing.expectEqual(10, client.current_block.columns.len);
    try std.testing.expectEqual(26, client.current_block.rows);
    try std.testing.expectEqual(client.writer.?.end, client.reader.?.seek);
}

test "processQueryResponse ensure correct read | Progress" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupFixedReaderWriterMockClient(allocator, io);
    defer teardownFixedReaderWriterMockClient(allocator, &client);

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.Progress));
    try client.writer.?.writeInt(u64, 32, .little);
    try client.writer.?.writeInt(u64, 512, .little);
    try client.writer.?.writeInt(u64, 128, .little);
    try client.writer.?.writeInt(u64, 64, .little);
    try client.writer.?.writeInt(u64, 256, .little);
    try client.writer.?.writeInt(u64, 4096, .little);

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.EndOfStream));

    try client.processQueryResponse();

    try std.testing.expectEqual(0, client.current_block.columns.len);
    try std.testing.expectEqual(client.writer.?.end, client.reader.?.seek);
}

test "processQueryResponse ensure correct read | TableColumns" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupFixedReaderWriterMockClient(allocator, io);
    defer teardownFixedReaderWriterMockClient(allocator, &client);

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.TableColumns));
    try ch.protocol.writeString(client.writer.?, "Table name");
    try ch.protocol.writeString(client.writer.?, "Table desc");

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.EndOfStream));

    try client.processQueryResponse();

    try std.testing.expectEqual(0, client.current_block.columns.len);
    try std.testing.expectEqual(client.writer.?.end, client.reader.?.seek);
}

test "processQueryResponse ensure correct read | EndOfStream" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupFixedReaderWriterMockClient(allocator, io);
    defer teardownFixedReaderWriterMockClient(allocator, &client);

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.EndOfStream));

    try client.processQueryResponse();

    try std.testing.expectEqual(0, client.current_block.columns.len);
    try std.testing.expectEqual(client.writer.?.end, client.reader.?.seek);
}

test "processQueryResponse ensure correct read | Exception" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupFixedReaderWriterMockClient(allocator, io);
    defer teardownFixedReaderWriterMockClient(allocator, &client);

    try ch.protocol.writeVarInt(client.writer.?, @intFromEnum(ch.packet.ServerPacket.Exception));

    const err_code = 7;
    const err_name = "Test error name";
    const err_msg = "This is testing error handling";
    const err_stack = "PLACEHOLDER error stack";

    try client.writer.?.writeInt(u32, err_code, .little);
    try ch.protocol.writeString(client.writer.?, err_name);
    try ch.protocol.writeString(client.writer.?, err_msg);
    try ch.protocol.writeString(client.writer.?, err_stack);

    try client.writer.?.writeByte(0); // has_nested

    var return_error: ?anyerror = null;
    client.processQueryResponse() catch |err| {
        return_error = err;
    };

    try std.testing.expectEqual(0, client.current_block.columns.len);
    try std.testing.expectEqual(client.writer.?.end, client.reader.?.seek);
    try std.testing.expectEqual(@as(ch.ClickHouseError, ch.ClickHouseError.QueryFailed), return_error.?);

    try std.testing.expect(client.last_error != null);
    try std.testing.expectEqual(ch.ch_error.ErrorCode.ServerError, client.last_error.?.code);
    try std.testing.expectEqualStrings(err_msg, client.last_error.?.message);
    try std.testing.expectEqualStrings(err_stack, client.last_error.?.stack_trace.?);
}

test "writeLog" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const user_env_key = if (builtin.os.tag == .windows) "USERNAME" else "USER";
    const os_user = try std.testing.environ.getAlloc(allocator, user_env_key);
    defer allocator.free(os_user);

    var client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    }, os_user);
    defer client.deinit();

    try client.connect();
    defer client.disconnect();

    var audit_log = std.ArrayList(types.AuditEntry).empty;
    defer audit_log.deinit(allocator);
    try audit_log.ensureUnusedCapacity(allocator, 4);

    var changed_columns = std.StringHashMap(types.ChangedColumns).init(allocator);
    defer changed_columns.deinit();

    audit_log.appendSliceAssumeCapacity(&[_]types.AuditEntry{
        .{ .event_time = 53634634, .transaction_id = 10, .primary_key = "1", .user_id = try allocator.dupe(u8, "42"), .table_name = try allocator.dupe(u8, "test.addresses"), .action = 1, .changed_columns = changed_columns, .new_values = .empty, .old_values = .empty, .ip_address = try allocator.dupe(u8, "192.168.1.50") },
        .{ .event_time = 53634634, .transaction_id = 10, .primary_key = "2", .user_id = try allocator.dupe(u8, "42"), .table_name = try allocator.dupe(u8, "test.addresses"), .action = 2, .changed_columns = changed_columns, .new_values = .empty, .old_values = .empty, .ip_address = try allocator.dupe(u8, "192.168.1.50") },
        .{ .event_time = 53634634, .transaction_id = 10, .primary_key = "3", .user_id = try allocator.dupe(u8, "42"), .table_name = try allocator.dupe(u8, "test.addresses"), .action = 3, .changed_columns = changed_columns, .new_values = .empty, .old_values = .empty, .ip_address = try allocator.dupe(u8, "192.168.1.50") },
        .{ .event_time = 53634634, .transaction_id = 11, .primary_key = "4", .user_id = try allocator.dupe(u8, "42"), .table_name = try allocator.dupe(u8, "test.addresses"), .action = 1, .changed_columns = changed_columns, .new_values = .empty, .old_values = .empty, .ip_address = try allocator.dupe(u8, "192.168.1.50") }
    });

    try client.writeLog(audit_log.items);
    for (audit_log.items) |*item| item.deinit(allocator);
}

test "parseRow ensure correct output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

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

    var row: types.AuditEntry = .{
        .event_time = 10,
        .table_name = try allocator.dupe(u8, "test.addresses"),
        .new_values = new_values,
        .old_values = old_values,
        .action = 2,
        .changed_columns = changed_columns,
        .transaction_id = 793,
        .user_id = try allocator.dupe(u8, "42"),
        .ip_address = try allocator.dupe(u8, "192.168.1.50"),
        .primary_key = "1",
    };
    defer row.deinit(allocator);

    var row_changed_columns = std.ArrayList(ch.bulk_insert.Value).empty;
    defer row_changed_columns.deinit(allocator);

    var row_old_values = std.StringHashMap([]const u8).init(allocator);
    defer row_old_values.deinit();

    var row_new_values = std.StringHashMap([]const u8).init(allocator);
    defer row_new_values.deinit();

    const insert_values = try client.parseRow(row, &row_changed_columns, &row_old_values, &row_new_values);

    try std.testing.expectEqual(3, insert_values.changed_columns.items.len);
    try std.testing.expectEqual(3, insert_values.new_values.count());
    try std.testing.expectEqual(3, insert_values.old_values.count());

    try std.testing.expectEqualStrings("address_line_1", insert_values.changed_columns.items[0].String);
    try std.testing.expectEqualStrings("postal_code", insert_values.changed_columns.items[1].String);
    try std.testing.expectEqualStrings("city", insert_values.changed_columns.items[2].String);

    try std.testing.expectEqualStrings("1 Apple Park Way", insert_values.new_values.get("address_line_1").?);
    try std.testing.expectEqualStrings("95014", insert_values.new_values.get("postal_code").?);
    try std.testing.expectEqualStrings("Cupertino", insert_values.new_values.get("city").?);

    try std.testing.expectEqualStrings("Googleplex", insert_values.old_values.get("address_line_1").?);
    try std.testing.expectEqualStrings("94043", insert_values.old_values.get("postal_code").?);
    try std.testing.expectEqualStrings("Mountain View", insert_values.old_values.get("city").?);

    try std.testing.expectEqualStrings("42", insert_values.row.user_id);
    try std.testing.expectEqualStrings("192.168.1.50", insert_values.row.ip_address);
    try std.testing.expectEqualStrings("1", insert_values.row.primary_key);
    try std.testing.expectEqualStrings("test.addresses", insert_values.row.table_name);
    try std.testing.expectEqual(2, insert_values.row.action);
    try std.testing.expectEqual(793, insert_values.row.transaction_id);
    try std.testing.expectEqual(10, insert_values.row.event_time);
}

test "insertRow ensure correct insertion" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const user_env_key = if (builtin.os.tag == .windows) "USERNAME" else "USER";
    const os_user = try std.testing.environ.getAlloc(allocator, user_env_key);
    defer allocator.free(os_user);

    var client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    }, os_user);
    defer client.deinit();

    try client.connect();

    var changed_columns = std.ArrayList(ch.bulk_insert.Value).empty;
    defer changed_columns.deinit(allocator);
    try changed_columns.ensureUnusedCapacity(allocator, 3);

    changed_columns.appendAssumeCapacity(.{ .String = "address_line_1" });
    changed_columns.appendAssumeCapacity(.{ .String = "postal_code" });
    changed_columns.appendAssumeCapacity(.{ .String = "city" });

    var new_values = std.StringHashMap([]const u8).init(allocator);
    defer new_values.deinit();
    try new_values.ensureUnusedCapacity(3);

    new_values.putAssumeCapacity("address_line_1", "1 Apple Park Way");
    new_values.putAssumeCapacity("postal_code", "94043");
    new_values.putAssumeCapacity("city", "Cupertino");

    var old_values = std.StringHashMap([]const u8).init(allocator);
    defer old_values.deinit();
    try old_values.ensureUnusedCapacity(3);

    old_values.putAssumeCapacity("address_line_1", "Googleplex");
    old_values.putAssumeCapacity("postal_code", "95014");
    old_values.putAssumeCapacity("city", "Mountain View");

    const values = InsertValues{
        .changed_columns = &changed_columns,
        .new_values = &new_values,
        .old_values = &old_values,
        .row = .{
            .event_time = 10,
            .table_name = try allocator.dupe(u8, "test.addresses"),
            .new_values = undefined,
            .old_values = undefined,
            .action = 2,
            .changed_columns = undefined,
            .transaction_id = 793,
            .user_id = try allocator.dupe(u8, "42"),
            .ip_address = try allocator.dupe(u8, "192.168.1.50"),
            .primary_key = "1",
        },
    };
    defer {
        allocator.free(values.row.table_name);
        allocator.free(values.row.user_id);
        allocator.free(values.row.ip_address);
    }

    var bulk: ch.BulkInsert = try .init(allocator, "entries", &columns, 1000);
    defer bulk.deinit();

    client.startInsert("INSERT INTO entries FORMAT Native") catch |err| {
        if (err == error.QueryFailed) {
            std.debug.print("Error: Query failed, {s}\n", .{client.last_error.?.message});
        }
        return err;
    };

    try client.insertRow(&bulk, values);
    bulk.flush(io, client.stream.?) catch |err| {
        std.debug.print("Error: Flush failed, {}\n", .{err});
        client.processQueryResponse() catch {};
        if (client.last_error) |e| {
            std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
        }
        return err;
    };

    client.endOfStream() catch |err| {
        std.debug.print("Error: Close failed, {}\n", .{err});
        if (client.last_error) |e| {
            std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
        }
        return err;
    };
}

