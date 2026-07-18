const std = @import("std");
const Io = std.Io;
const net = Io.net;
const mem = std.mem;
const ch = @import("ch");
const types = @import("types");

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

    stream: ?net.Stream,
    stream_reader: ?@TypeOf(@as(net.Stream, undefined).reader(@as(Io, undefined), @as(*[8192]u8, undefined))) = null,
    stream_writer: ?@TypeOf(@as(net.Stream, undefined).writer(@as(Io, undefined), @as(*[4096]u8, undefined))) = null,

    reader: ?*Io.Reader = null,
    writer: ?*Io.Writer = null,

    read_buf: [8192]u8 = undefined,
    write_buf: [4096]u8 = undefined,

    current_block: ?ch.block.Block,
    current_result: ?ch.results.QueryResult,
    server_info: ?ch.server_info.ServerInfo,
    query_info: ?ch.query_info.QueryInfo,
    last_error: ?*ch.ch_error.Error,

    pub fn init(allocator: mem.Allocator, io: std.Io, config: ch.ClickHouseConfig) ChClient {
        return .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .stream = null,
            .current_block = null,
            .current_result = null,
            .server_info = null,
            .query_info = null,
            .last_error = null,
        };
    }

    pub fn deinit(self: *ChClient) void {
        if (self.current_result) |*result| {
            result.deinit();
        }
        if (self.current_block) |*b| {
            b.deinit();
        }
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

    pub fn disconnect(self: *ChClient, io: std.Io) !void {
        if (self.stream) |stream| {
            stream.close(io);
        }
        self.stream = null;
        self.reader = null;
        self.writer = null;
        
        self.read_buf = undefined;
        self.write_buf = undefined;
    }

    fn ensureStream(self: *@This()) !void {
        if (self.stream == null) {
            return ch.ClickHouseError.ConnectionFailed;
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
        try self.ensureStream();

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
        try self.ensureStream();

        const server_packet = try ch.protocol.readVarInt(self.reader.?);

        std.debug.print("Got {d}, expected {d}\n", .{server_packet, @intFromEnum(ch.packet.ServerPacket.Hello)});
        if (server_packet != @intFromEnum(ch.packet.ServerPacket.Hello)) {
            return ch.ClickHouseError.ProtocolError;
        }

        self.server_info = try ch.server_info.ServerInfo.read(self.allocator, self.reader.?);
    }

    pub fn startInsert(self: *ChClient, query_str: []const u8) !void {
        try self.ensureStream();

        if (self.current_result) |*result| {
            result.deinit();
            self.current_result = null;
        }

        if (self.query_info) |*info| {
            _ = info;
            self.query_info = null;
        }

        self.query_info = ch.query_info.QueryInfo.init();

        var address_buf: [256]u8 = undefined;
        const address = try std.fmt.bufPrint(&address_buf, "[::ffff:127.0.0.1]:{d}", .{ self.config.port });

        try ch.packet.writeClientPacketHeader(self.writer.?, .Query);
        try ch.protocol.ClientInfo.write(self.writer.?, "", "ClickHouse Zig", self.config.username, address);
        try ch.protocol.writeString(self.writer.?, ""); // Empty settings

        try ch.protocol.writeVarInt(self.writer.?, 2); // stage: Complete
        try ch.protocol.writeVarInt(self.writer.?, self.config.settings.compression_method); // compression

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
            const packet_type = try ch.protocol.readVarInt(self.reader.?); std.debug.print("startInsert packet: {d}\n", .{packet_type});
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

    pub fn readBlock(self: *ChClient) !void {
        try self.ensureStream();

        std.debug.print("entering readBlock\n", .{});
        
        const revision = if (self.server_info) |info| info.revision else 0;
        std.debug.print("revision: {d}\n", .{revision});

        if (revision >= 50264) {
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

        if (self.current_block) |*b| {
            b.rows = num_rows;
        }

        for (0..num_columns) |_| {
            const col_name = try ch.protocol.readString(self.reader.?);
            const col_type_str = try ch.protocol.readString(self.reader.?);

            if (self.current_block) |*b| {
                const ch_type = try ch.ClickHouseType.fromStr(col_type_str);
                try b.addColumn(col_name, col_type_str);
                
                const col_idx = b.columns.len - 1;
                
                if (num_rows > 0) {
                    _ = ch_type; _ = col_idx;
                }
            }
        }
    }

    pub fn processQueryResponse(self: *ChClient) !void {
        try self.ensureStream();

        while (true) {
            const packet_type = try ch.protocol.readVarInt(self.reader.?); std.debug.print("startInsert packet: {d}\n", .{packet_type});
            
            switch (@as(ch.packet.ServerPacket, @enumFromInt(packet_type))) {
                .Data => {
                    if (self.current_block == null) {
                        self.current_block = ch.block.Block.init(self.allocator);
                    }
                    try self.readBlock();
                    
                    if (self.current_block) |*b| {
                        self.current_result = try ch.results.QueryResult.init(self.allocator, b);
                    }
                },
                .Progress => {
                    const prog = try ch.progress.Progress.read(self.reader.?);
                    if (self.query_info) |*info| {
                        info.updateProgress(prog);
                    }
                },
                .TableColumns => {
                    const table_name = try ch.protocol.readString(self.reader.?);
                    const cols_desc = try ch.protocol.readString(self.reader.?);
                    self.allocator.free(table_name);
                    self.allocator.free(cols_desc);
                },
                .EndOfStream => {
                    std.debug.print("returning from processQueryResponse!\n", .{});
                    return;
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
                else => {},
            }
        }
    }

    pub fn writeLog(self: *@This(), data: [][]types.AuditEntry) !void {
        var bulk: ch.BulkInsert = try .init(self.allocator, "entries", &columns, 1000);
        defer bulk.deinit();

        self.startInsert("INSERT INTO entries FORMAT Native") catch |err| {
            if (err == error.QueryFailed) {
                std.debug.print("Query failed: {s}\n", .{self.last_error.?.message});
            }
            return err;
        };

        var buf = std.ArrayList(ch.bulk_insert.Value).empty;
        defer buf.deinit(self.allocator);

        var old_values = std.StringHashMap([]const u8).init(self.allocator);
        defer old_values.deinit();

        var new_values = std.StringHashMap([]const u8).init(self.allocator);
        defer new_values.deinit();

        var i: u32 = 0;
        while (i < data.len) : (i += 1) {
            for (data[i]) |row| {
                const insert_values = self.parseRow(row, &buf, &old_values, &new_values);

                try self.insertRow(&bulk, insert_values);
            }
        }

        // Flush any remaining rows
        bulk.flush(self.io, self.stream.?) catch |err| {
            std.debug.print("flush failed: {}\n", .{err});
            self.processQueryResponse(self.io) catch {};
            if (self.last_error) |e| {
                std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
            }
            return err;
        };

        std.debug.print("Executed insert\n", .{});

        self.closeStream(self.io) catch |err| {
            std.debug.print("close failed: {}\n", .{err});
            if (self.last_error) |e| {
                std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
            }
            return err;
        };

        std.debug.print("Closed stream\n", .{});

        if (self.last_error) |err| {
            std.debug.print("Insert failed: {s}\n", .{err.message});
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
                const val = row.old_values.get(col.key_ptr.*);
                try old_values.put(col.key_ptr.*, val.?);
            }

            if (row.new_values.capacity() > 0) {
                const val = row.new_values.get(col.key_ptr.*);
                try new_values.put(col.key_ptr.*, val.?);
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
                std.debug.print("flush failed: {}\n", .{err});
                self.processQueryResponse() catch {};
                if (self.last_error) |e| {
                    std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
                }
                return err;
            };
        }
    }

    pub fn closeStream(self: *ChClient) !void {
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

        try self.processQueryResponse(self.io);
    }
};

fn setupMockClient(allocator: std.mem.Allocator, io: std.Io) !ChClient {
    return .{
        .allocator = allocator,
        .io = io,
        .config = undefined,
        .stream = null,
        .reader = null,
        .read_buf = undefined,
        .current_block = null,
        .current_result = null,
        .server_info = null,
        .query_info = null,
        .last_error = null,
    };
}

test "parseRow ensure correct output" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    client.deinit();

    var changed_columns = std.StringHashMap(types.ChangedColumns).init(allocator);
    defer changed_columns.deinit();
    try changed_columns.ensureUnusedCapacity(1);

    try changed_columns.put("id", .{ .has_changes = false, .value = "1" });
    try changed_columns.put("address_line_1", .{ .has_changes = true, .value = "Googleplex" });
    try changed_columns.put("address_line_2", .{ .has_changes = false, .value = "" });
    try changed_columns.put("postal_code", .{ .has_changes = true, .value = "94043" });
    try changed_columns.put("city", .{ .has_changes = true, .value = "Mountain View" });
    try changed_columns.put("country", .{ .has_changes = false, .value = "US" });

    var new_values = std.StringHashMapUnmanaged([]const u8).empty;
    defer new_values.deinit(allocator);
    try new_values.ensureUnusedCapacity(allocator, 6);

    try new_values.put(allocator, "id", "1");
    try new_values.put(allocator, "address_line_1", "1 Apple Park Way");
    try new_values.put(allocator, "address_line_2", "");
    try new_values.put(allocator, "postal_code", "95014");
    try new_values.put(allocator, "city", "Cupertino");
    try new_values.put(allocator, "country", "US");

    var old_values = std.StringHashMapUnmanaged([]const u8).empty;
    defer old_values.deinit(allocator);
    try old_values.ensureUnusedCapacity(allocator, 6);

    try old_values.put(allocator, "id", "1");
    try old_values.put(allocator, "address_line_1", "Googleplex");
    try old_values.put(allocator, "address_line_2", "");
    try old_values.put(allocator, "postal_code", "94043");
    try old_values.put(allocator, "city", "Mountain View");
    try old_values.put(allocator, "country", "US");

    const row: types.AuditEntry = .{
        .event_time = 10,
        .table_name = try allocator.dupe(u8, "addresses"),
        .new_values = new_values,
        .old_values = old_values,
        .action = 2,
        .changed_columns = changed_columns,
        .transaction_id = 793,
        .user_id = "42",
        .ip_address = "192.168.1.50",
        .primary_key = "1",
    };
    defer allocator.free(row.table_name);

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
    try std.testing.expectEqualStrings("addresses", insert_values.row.table_name);
    try std.testing.expectEqual(2, insert_values.row.action);
    try std.testing.expectEqual(793, insert_values.row.transaction_id);
    try std.testing.expectEqual(10, insert_values.row.event_time);
}

test "insertRow ensure correct insertion" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var ch_client = ChClient.init(allocator, io, .{
        .host = "localhost",
        .port = 9000,
        .username = "default",
        .password = "clickhouse",
        .database = "audit_log",
    });
    defer ch_client.deinit();

    try ch_client.connect();

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
            .table_name = try allocator.dupe(u8, "addresses"),
            .new_values = undefined,
            .old_values = undefined,
            .action = 2,
            .changed_columns = undefined,
            .transaction_id = 793,
            .user_id = "42",
            .ip_address = "192.168.1.50",
            .primary_key = "1",
        },
    };
    defer allocator.free(values.row.table_name);

    var bulk: ch.BulkInsert = try .init(allocator, "entries", &columns, 1000);
    defer bulk.deinit();

    ch_client.startInsert("INSERT INTO entries FORMAT Native") catch |err| {
        if (err == error.QueryFailed) {
            std.debug.print("Query failed: {s}\n", .{ch_client.last_error.?.message});
        }
        return err;
    };

    try ch_client.insertRow(&bulk, values);
}

test "sendHello ensure correct format" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    client.deinit();

    client.stream = undefined;
    client.writer = undefined;

    const buffer = try allocator.alloc(u8, 1024);
    defer allocator.free(buffer);

    var writer = std.Io.Writer.fixed(buffer);
    client.writer = &writer;

    try client.sendHello();
}
