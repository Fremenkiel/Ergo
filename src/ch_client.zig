const std = @import("std");
const builtin = @import("builtin");

const Io = std.Io;
const net = Io.net;
const mem = std.mem;

const assert = std.debug.assert;

const ch = @import("ch");
const types = @import("types");

const column_definition = [10]ch.bulk_insert.ColumnDef{
    .{ .name = "event_time", .type_str = "DateTime64" },
    .{ .name = "transaction_id", .type_str = "UInt64" },
    .{ .name = "user_id", .type_str = "String" },
    .{ .name = "table_name", .type_str = "LowCardinality(String)" },
    .{ .name = "action", .type_str = "Enum8('INSERT' = 1, 'UPDATE' = 2, 'DELETE' = 3)" },
    .{ .name = "primary_keys", .type_str = "Map(String, String)" },
    .{ .name = "changed_columns", .type_str = "Array(String)" },
    .{ .name = "old_values", .type_str = "Map(String, String)" },
    .{ .name = "new_values", .type_str = "Map(String, String)" },
    .{ .name = "ip_address", .type_str = "IPv4" },
};

const InsertValues = struct {
    allocator: mem.Allocator, 
    primary_keys: std.StringHashMap([]const u8),
    changed_columns: std.ArrayList(ch.bulk_insert.Value),
    old_values: std.StringHashMap([]const u8),
    new_values: std.StringHashMap([]const u8),

    pub fn init(allocator: mem.Allocator) !@This() {
        return .{
            .allocator = allocator,
            .primary_keys = std.StringHashMap([]const u8).init(allocator),
            .changed_columns = std.ArrayList(ch.bulk_insert.Value).empty,
            .old_values = std.StringHashMap([]const u8).init(allocator),
            .new_values = std.StringHashMap([]const u8).init(allocator),
        };
    }

    pub fn deinit(self: *@This()) void {
        self.primary_keys.deinit();
        self.changed_columns.deinit(self.allocator);

        self.old_values.clearAndFree();
        self.old_values.deinit();

        self.new_values.clearAndFree();
        self.new_values.deinit();
    }

    pub fn clearRetainingCapacity(self: *@This()) void {
        self.primary_keys.clearRetainingCapacity();
        self.changed_columns.clearRetainingCapacity();
        self.old_values.clearRetainingCapacity();
        self.new_values.clearRetainingCapacity();
    }

    pub fn parseRow(self: *@This(), row: types.AuditEntry) !void {
        try self.changed_columns.ensureUnusedCapacity(self.allocator, row.columns.items.len);
        try self.old_values.ensureUnusedCapacity(@as(u32, @truncate(row.columns.items.len)));
        try self.new_values.ensureUnusedCapacity(@as(u32, @truncate(row.columns.items.len)));

        for (row.columns.items) |col| {
            const column_name = col.column_name;
            const old_value = col.old_value;
            const new_value = col.new_value;

            if (col.is_key) {
                if (old_value) |val| {
                    try self.primary_keys.put(column_name, val);
                } else if (new_value) |val| {
                    try self.primary_keys.put(column_name, val);
                }
            }

            if (!col.has_changes) continue;

            self.changed_columns.appendAssumeCapacity(.{ .String = column_name });

            if (old_value) |val| {
                self.old_values.putAssumeCapacity(column_name, val);
            }

            if (new_value) |val| {
                self.new_values.putAssumeCapacity(column_name, val);
            }
        }
    }
};

pub const ChClient = struct {
    allocator: mem.Allocator,
    io: std.Io,

    config: ch.ChConfig,
    os_user: []const u8,

    stream: ?net.Stream,
    stream_reader: ?@TypeOf(@as(net.Stream, undefined).reader(@as(Io, undefined), @as(*[8192]u8, undefined))) = null,
    stream_writer: ?@TypeOf(@as(net.Stream, undefined).writer(@as(Io, undefined), @as(*[4096]u8, undefined))) = null,

    reader: ?*Io.Reader = null,
    writer: ?*Io.Writer = null,

    read_buf: [8192]u8 = undefined,
    write_buf: [4096]u8 = undefined,

    current_block: ch.block.Block,
    server_info: ?ch.protocol.ServerInfo,
    last_error: ?*ch.ch_error.Error,

    pub fn init(allocator: mem.Allocator, io: std.Io, config: ch.ChConfig, os_user: []const u8) ChClient {
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

        try ch.protocol.ClientHello.write(self.writer.?, self.config);
        self.server_info = try ch.protocol.ServerInfo.read(self.allocator, self.reader.?);
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

        try ch.protocol.ClientInfo.write(
            self.writer.?,
            query_id,
            self.config.username,
            address,
            timestamp,
            self.os_user,
            self.config.application_name
        );
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

                    return ch.ChError.QueryFailed;
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

                    return ch.ChError.QueryFailed;
                },
                else => {},
            }
        }
    }

    pub fn writeLog(self: *@This(), data: []types.AuditEntry) !void {
        var bulk: ch.BulkInsert = try .init(self.allocator, "entries", &column_definition, 1000);
        defer bulk.deinit();

        self.startInsert("INSERT INTO entries FORMAT Native") catch |err| {
            if (err == error.QueryFailed) {
                std.debug.print("Error: Query failed, {s}\n", .{self.last_error.?.message});
            }
            return err;
        };

        var insert_values = try InsertValues.init(self.allocator);
        defer insert_values.deinit();

        for (data) |row| {
            insert_values.clearRetainingCapacity();

            try insert_values.parseRow(row);

            try self.insertRow(&bulk, row, insert_values);
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

    pub fn insertRow(self: *@This(), bulk: *ch.BulkInsert, row: types.AuditEntry, insert_values: InsertValues) !void {

        const values = [_]ch.bulk_insert.Value{
            .{ .DateTime64 = row.event_time },
            .{ .UInt64 = row.transaction_id },
            .{ .String = row.user_id},
            .{ .LowCardinality = row.table_name },
            .{ .Enum8 = row.action },
            .{ .Map = insert_values.primary_keys },
            .{ .Array = insert_values.changed_columns.items },
            .{ .Map = insert_values.old_values },
            .{ .Map = insert_values.new_values },
            .{ .IPv4 = row.ip_address },
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

    try ch.protocol.writeString(client.writer.?, column_definition[0].name);
    try ch.protocol.writeString(client.writer.?, column_definition[0].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[1].name);
    try ch.protocol.writeString(client.writer.?, column_definition[1].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[2].name);
    try ch.protocol.writeString(client.writer.?, column_definition[2].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[3].name);
    try ch.protocol.writeString(client.writer.?, column_definition[3].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[4].name);
    try ch.protocol.writeString(client.writer.?, column_definition[4].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[5].name);
    try ch.protocol.writeString(client.writer.?, column_definition[5].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[6].name);
    try ch.protocol.writeString(client.writer.?, column_definition[6].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[7].name);
    try ch.protocol.writeString(client.writer.?, column_definition[7].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[8].name);
    try ch.protocol.writeString(client.writer.?, column_definition[8].type_str);
    try ch.protocol.writeString(client.writer.?, column_definition[9].name);
    try ch.protocol.writeString(client.writer.?, column_definition[9].type_str);
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

test "startInsert ensure correct query info" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const user_env_key = "USER";
    const os_user = try std.testing.environ.getAlloc(allocator, user_env_key);
    defer allocator.free(os_user);

    var client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
        .application_name = "Ergo test",
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

    try std.testing.expectEqualStrings(column_definition[0].name, client.current_block.columns[0].name);
    try std.testing.expectEqualStrings(column_definition[0].type_str, client.current_block.columns[0].type_name);
    try std.testing.expectEqualStrings(column_definition[1].name, client.current_block.columns[1].name);
    try std.testing.expectEqualStrings(column_definition[1].type_str, client.current_block.columns[1].type_name);
    try std.testing.expectEqualStrings(column_definition[2].name, client.current_block.columns[2].name);
    try std.testing.expectEqualStrings(column_definition[2].type_str, client.current_block.columns[2].type_name);
    try std.testing.expectEqualStrings(column_definition[3].name, client.current_block.columns[3].name);
    try std.testing.expectEqualStrings(column_definition[3].type_str, client.current_block.columns[3].type_name);
    try std.testing.expectEqualStrings(column_definition[4].name, client.current_block.columns[4].name);
    try std.testing.expectEqualStrings(column_definition[4].type_str, client.current_block.columns[4].type_name);
    try std.testing.expectEqualStrings(column_definition[5].name, client.current_block.columns[5].name);
    try std.testing.expectEqualStrings(column_definition[5].type_str, client.current_block.columns[5].type_name);
    try std.testing.expectEqualStrings(column_definition[6].name, client.current_block.columns[6].name);
    try std.testing.expectEqualStrings(column_definition[6].type_str, client.current_block.columns[6].type_name);
    try std.testing.expectEqualStrings(column_definition[7].name, client.current_block.columns[7].name);
    try std.testing.expectEqualStrings(column_definition[7].type_str, client.current_block.columns[7].type_name);
    try std.testing.expectEqualStrings(column_definition[8].name, client.current_block.columns[8].name);
    try std.testing.expectEqualStrings(column_definition[8].type_str, client.current_block.columns[8].type_name);
    try std.testing.expectEqualStrings(column_definition[9].name, client.current_block.columns[9].name);
    try std.testing.expectEqualStrings(column_definition[9].type_str, client.current_block.columns[9].type_name);
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
    try std.testing.expectEqual(@as(ch.ChError, ch.ChError.QueryFailed), return_error.?);

    try std.testing.expect(client.last_error != null);
    try std.testing.expectEqual(ch.ch_error.ErrorCode.ServerError, client.last_error.?.code);
    try std.testing.expectEqualStrings(err_msg, client.last_error.?.message);
    try std.testing.expectEqualStrings(err_stack, client.last_error.?.stack_trace.?);
}

test "writeLog" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const user_env_key = "USER";
    const os_user = try std.testing.environ.getAlloc(allocator, user_env_key);
    defer allocator.free(os_user);

    var client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
        .application_name = "Ergo test",
    }, os_user);
    defer client.deinit();

    try client.connect();
    defer client.disconnect();

    var audit_log = std.ArrayList(types.AuditEntry).empty;
    defer audit_log.deinit(allocator);
    try audit_log.ensureUnusedCapacity(allocator, 4);

    var columns: std.ArrayList(types.ChangedColumn) = .empty;
    defer columns.deinit(allocator);

    audit_log.appendSliceAssumeCapacity(&[_]types.AuditEntry{
        .{ .event_time = 53634634, .transaction_id = 10, .user_id = try allocator.dupe(u8, "42"), .table_name = try allocator.dupe(u8, "test.addresses"), .action = 1, .columns = columns,.ip_address = try allocator.dupe(u8, "192.168.1.50") },
        .{ .event_time = 53634634, .transaction_id = 10, .user_id = try allocator.dupe(u8, "42"), .table_name = try allocator.dupe(u8, "test.addresses"), .action = 2, .columns = columns,.ip_address = try allocator.dupe(u8, "192.168.1.50") },
        .{ .event_time = 53634634, .transaction_id = 10, .user_id = try allocator.dupe(u8, "42"), .table_name = try allocator.dupe(u8, "test.addresses"), .action = 3, .columns = columns,.ip_address = try allocator.dupe(u8, "192.168.1.50") },
        .{ .event_time = 53634634, .transaction_id = 11, .user_id = try allocator.dupe(u8, "42"), .table_name = try allocator.dupe(u8, "test.addresses"), .action = 1, .columns = columns,.ip_address = try allocator.dupe(u8, "192.168.1.50") }
    });

    try client.writeLog(audit_log.items);
    for (audit_log.items) |*item| item.deinit(allocator);
}

test "parseRow ensure correct output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    var columns: std.ArrayList(types.ChangedColumn) = .empty;

    try columns.ensureUnusedCapacity(allocator, 6);

    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = try allocator.dupe(u8, "1"), .new_value = try allocator.dupe(u8, "1"), .column_name = try allocator.dupe(u8, "id"), .is_key = true });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "1 Apple Park Way"), .new_value = try allocator.dupe(u8, "Googleplex"), .column_name = try allocator.dupe(u8, "address_line_1"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = try allocator.dupe(u8, ""), .new_value = try allocator.dupe(u8, ""), .column_name = try allocator.dupe(u8, "address_line_2"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "95014"), .new_value = try allocator.dupe(u8, "94043"), .column_name = try allocator.dupe(u8, "postal_code"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = true, .old_value = try allocator.dupe(u8, "Cupertino"), .new_value = try allocator.dupe(u8, "Mountain View"), .column_name = try allocator.dupe(u8, "city"), .is_key = false });
    columns.appendAssumeCapacity(.{ .has_changes = false, .old_value = try allocator.dupe(u8, "US"), .new_value = try allocator.dupe(u8, "US"), .column_name = try allocator.dupe(u8, "country"), .is_key = false });

    var row: types.AuditEntry = .{
        .event_time = 10,
        .table_name = try allocator.dupe(u8, "test.addresses"),
        .action = 2,
        .columns = columns,
        .transaction_id = 793,
        .user_id = try allocator.dupe(u8, "42"),
        .ip_address = try allocator.dupe(u8, "192.168.1.50"),
    };
    defer row.deinit(allocator);

    var insert_values = try InsertValues.init(allocator);
    defer insert_values.deinit();

    try insert_values.parseRow(row);

    try std.testing.expectEqual(3, insert_values.changed_columns.items.len);
    try std.testing.expectEqual(3, insert_values.new_values.count());
    try std.testing.expectEqual(3, insert_values.old_values.count());

    try std.testing.expectEqualStrings("address_line_1", insert_values.changed_columns.items[0].String);
    try std.testing.expectEqualStrings("postal_code", insert_values.changed_columns.items[1].String);
    try std.testing.expectEqualStrings("city", insert_values.changed_columns.items[2].String);

    try std.testing.expectEqualStrings("Googleplex", insert_values.new_values.get("address_line_1").?);
    try std.testing.expectEqualStrings("94043", insert_values.new_values.get("postal_code").?);
    try std.testing.expectEqualStrings("Mountain View", insert_values.new_values.get("city").?);

    try std.testing.expectEqualStrings("1 Apple Park Way", insert_values.old_values.get("address_line_1").?);
    try std.testing.expectEqualStrings("95014", insert_values.old_values.get("postal_code").?);
    try std.testing.expectEqualStrings("Cupertino", insert_values.old_values.get("city").?);

    try std.testing.expectEqual(1, insert_values.primary_keys.count());
    try std.testing.expectEqualStrings("1", insert_values.primary_keys.get("id").?);

    try std.testing.expectEqualStrings("42", row.user_id);
    try std.testing.expectEqualStrings("192.168.1.50", row.ip_address);
    try std.testing.expectEqualStrings("test.addresses", row.table_name);
    try std.testing.expectEqual(2, row.action);
    try std.testing.expectEqual(793, row.transaction_id);
    try std.testing.expectEqual(10, row.event_time);
}

test "insertRow ensure correct insertion" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const user_env_key = "USER";
    const os_user = try std.testing.environ.getAlloc(allocator, user_env_key);
    defer allocator.free(os_user);

    var client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
        .application_name = "Ergo test",
    }, os_user);
    defer client.deinit();

    try client.connect();

    var columns = std.ArrayList(ch.bulk_insert.Value).empty;
    defer columns.deinit(allocator);
    try columns.ensureUnusedCapacity(allocator, 3);

    columns.appendAssumeCapacity(.{ .String = "address_line_1" });
    columns.appendAssumeCapacity(.{ .String = "postal_code" });
    columns.appendAssumeCapacity(.{ .String = "city" });

    var primary_keys = std.StringHashMap([]const u8).init(allocator);
    defer primary_keys.deinit();
    try primary_keys.ensureUnusedCapacity(2);

    primary_keys.putAssumeCapacity("id", "1");
    primary_keys.putAssumeCapacity("user_id", "1");

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
        .allocator = allocator,
        .changed_columns = columns,
        .primary_keys = primary_keys,
        .new_values = new_values,
        .old_values = old_values,
    };
    const row: types.AuditEntry = .{
            .event_time = 10,
            .table_name = try allocator.dupe(u8, "test.addresses"),
            .action = 2,
            .columns = undefined,
            .transaction_id = 793,
            .user_id = try allocator.dupe(u8, "42"),
            .ip_address = try allocator.dupe(u8, "192.168.1.50"),
        };

    defer {
        allocator.free(row.table_name);
        allocator.free(row.user_id);
        allocator.free(row.ip_address);
    }

    var bulk: ch.BulkInsert = try .init(allocator, "entries", &column_definition, 1000);
    defer bulk.deinit();

    client.startInsert("INSERT INTO entries FORMAT Native") catch |err| {
        if (err == error.QueryFailed) {
            std.debug.print("Error: Query failed, {s}\n", .{client.last_error.?.message});
        }
        return err;
    };

    try client.insertRow(&bulk, row, values);
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

