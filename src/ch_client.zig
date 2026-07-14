const std = @import("std");
const Io = std.Io;
const net = Io.net;
const mem = std.mem;
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

pub const ChClient = struct {
    config: ch.ClickHouseConfig,
    stream: ?net.Stream,
    allocator: mem.Allocator,
    current_block: ?ch.block.Block,
    current_result: ?ch.results.QueryResult,
    server_info: ?ch.server_info.ServerInfo,
    query_info: ?ch.query_info.QueryInfo,
    last_error: ?*ch.ch_error.Error,
    read_buf: [8192]u8 = undefined,
    stream_reader: ?net.Stream.Reader = null,

    pub fn init(allocator: mem.Allocator, config: ch.ClickHouseConfig) ChClient {
        return .{
            .config = config,
            .stream = null,
            .allocator = allocator,
            .current_block = null,
            .current_result = null,
            .server_info = null,
            .query_info = null,
            .last_error = null,
        };
    }

    pub fn connect(self: *ChClient, io: Io) !void {
        const is_unix = self.config.host.len > 0 and self.config.host[0] == '/';

        const stream = try blk: {
            if (is_unix) {
                if (comptime Io.net.has_unix_sockets == false or std.posix.AF == void) {
                    return error.UnixPathNotSupported;
                }
                const addr: Io.net.UnixAddress = try .init(self.config.host);
                break :blk addr.connect(io);
            }
            const hostname: Io.net.HostName = try .init(self.config.host);
            break :blk hostname.connect(io, self.config.port, .{ .mode = .stream });
        };
        errdefer stream.close(io);

        self.stream = stream;
        self.stream_reader = stream.reader(io, &self.read_buf);

        try self.sendHello(io);
        try self.readServerHello(io);
    }

    pub fn disconnect(self: *ChClient, io: std.Io) !void {
        if (self.stream) |stream| {
            stream.close(io);
        }
    }

    fn sendHello(self: *ChClient, io: Io) !void {
        var buf: [1024]u8 = undefined;
        var writer = self.stream.?.writer(io, &buf);
        var w = &writer.interface;
        
        try ch.packet.writeClientPacketHeader(w, .Hello);
        try ch.protocol.ClientHello.write(w);
        
        try w.writeInt(u8, @as(u8, @truncate(self.config.database.len)), .little);
        try w.writeAll(self.config.database);
        
        try w.writeInt(u8, @as(u8, @truncate(self.config.username.len)), .little);
        try w.writeAll(self.config.username);
        
        try w.writeInt(u8, @as(u8, @truncate(self.config.password.len)), .little);
        try w.writeAll(self.config.password);

        try w.flush();
    }

    fn readServerHello(self: *ChClient, io: Io) !void {
        _ = io;
        const r = &self.stream_reader.?.interface;
        
        const server_packet = try ch.protocol.readVarInt(r);

        std.debug.print("Got {d}, expected {d}\n", .{server_packet, @intFromEnum(ch.packet.ServerPacket.Hello)});
        if (server_packet != @intFromEnum(ch.packet.ServerPacket.Hello)) {
            return ch.ClickHouseError.ProtocolError;
        }

        self.server_info = try ch.server_info.ServerInfo.read(r);
    }

    pub fn startInsert(self: *ChClient, io: std.Io, query_str: []const u8) !void {
        if (self.stream == null) {
            return ch.ClickHouseError.ConnectionFailed;
        }

        if (self.current_result) |*result| {
            result.deinit();
            self.current_result = null;
        }

        if (self.query_info) |*info| {
            _ = info;
            self.query_info = null;
        }

        self.query_info = ch.query_info.QueryInfo.init();

        var write_buf: [4096]u8 = undefined;
        var writer = self.stream.?.writer(io, &write_buf);
        var w = &writer.interface;

        var address_buf: [256]u8 = undefined;
        const address = try std.fmt.bufPrint(&address_buf, "[::ffff:127.0.0.1]:{d}", .{ self.config.port });

        try ch.packet.writeClientPacketHeader(w, .Query);
        try ch.protocol.ClientInfo.write(w, "", "ClickHouse Zig", self.config.username, address);
        try ch.protocol.writeString(w, ""); // Empty settings

        try ch.protocol.writeVarInt(w, 2); // stage: Complete
        try ch.protocol.writeVarInt(w, self.config.settings.compression_method); // compression

        try ch.protocol.writeString(w, query_str);


        // Empty Data Block
        try ch.packet.writeClientPacketHeader(w, .Data);
        try ch.protocol.writeString(w, ""); // Block name
        
        try ch.protocol.writeVarInt(w, 1); // field: is_overflows
        try w.writeInt(u8, 0, .little); // is_overflows
        try ch.protocol.writeVarInt(w, 2); // field: bucket_num
        try w.writeInt(i32, -1, .little); // bucket_num
        try ch.protocol.writeVarInt(w, 0); // END
        
        try ch.protocol.writeVarInt(w, 0); // columns = 0
        try ch.protocol.writeVarInt(w, 0); // rows = 0
        try w.flush();

        var r = &self.stream_reader.?.interface;

        while (true) {
            const packet_type = try ch.protocol.readVarInt(r); std.debug.print("startInsert packet: {d}\n", .{packet_type});
            switch (@as(ch.packet.ServerPacket, @enumFromInt(packet_type))) {
                .Data => {
                    try self.readBlock(io);
                    return; // Schema received, ready for bulk push
                },
                .TableColumns => {
                    const table_name = try ch.protocol.readString(r);
                    const cols_desc = try ch.protocol.readString(r);
                    self.allocator.free(table_name);
                    self.allocator.free(cols_desc);
                },
                .Exception => {
                    const err_code = try r.takeInt(u32, .little);
                    const name = try ch.protocol.readString(r);
                    _ = name;
                    const msg = try ch.protocol.readString(r);
                    const stack = try ch.protocol.readString(r);
                    
                    _ = try r.takeByte();

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

    pub fn readBlock(self: *ChClient, io: std.Io) !void {
        std.debug.print("entering readBlock\n", .{});
        _ = io;
        var r = &self.stream_reader.?.interface;
        
        const revision = if (self.server_info) |info| info.revision else 0;
        std.debug.print("revision: {d}\n", .{revision});

        if (revision >= 50264) {
            const table_name = try ch.protocol.readString(r);
            self.allocator.free(table_name);
        }

        while (true) {
            const field_num = try ch.protocol.readVarInt(r);
            if (field_num == 0) break;
            if (field_num == 1) {
                _ = try r.takeByte(); // is_overflows
            } else if (field_num == 2) {
                _ = try r.takeInt(i32, .little); // bucket_num
            }
        }

        const num_columns = try ch.protocol.readVarInt(r);
        const num_rows = try ch.protocol.readVarInt(r);

        if (self.current_block) |*b| {
            b.rows = num_rows;
        }

        for (0..num_columns) |_| {
            const col_name = try ch.protocol.readString(r);
            const col_type_str = try ch.protocol.readString(r);

            if (self.current_block) |*b| {
                const ch_type = try ch.ClickHouseType.fromStr(col_type_str);
                try b.addColumn(col_name, col_type_str);
                
                const col_idx = b.columns.len - 1;
                
                if (num_rows > 0) {
                    _ = ch_type; _ = col_idx;
                }
            }
            
            self.allocator.free(col_name);
            self.allocator.free(col_type_str);
        }
    }

    pub fn processQueryResponse(self: *ChClient, io: std.Io) !void {
        var r = &self.stream_reader.?.interface;
        
        while (true) {
            const packet_type = try ch.protocol.readVarInt(r); std.debug.print("startInsert packet: {d}\n", .{packet_type});
            
            switch (@as(ch.packet.ServerPacket, @enumFromInt(packet_type))) {
                .Data => {
                    if (self.current_block == null) {
                        self.current_block = ch.block.Block.init(self.allocator);
                    }
                    try self.readBlock(io);
                    
                    if (self.current_block) |*b| {
                        self.current_result = try ch.results.QueryResult.init(self.allocator, b);
                    }
                },
                .Progress => {
                    const prog = try ch.progress.Progress.read(r);
                    if (self.query_info) |*info| {
                        info.updateProgress(prog);
                    }
                },
                .TableColumns => {
                    const table_name = try ch.protocol.readString(r);
                    const cols_desc = try ch.protocol.readString(r);
                    self.allocator.free(table_name);
                    self.allocator.free(cols_desc);
                },
                .EndOfStream => {
                    std.debug.print("returning from processQueryResponse!\n", .{});
                    return;
                },
                .Exception => {
                    const err_code = try r.takeInt(u32, .little);
                    const name = try ch.protocol.readString(r);
                    _ = name;
                    const msg = try ch.protocol.readString(r);
                    const stack = try ch.protocol.readString(r);
                    
                    _ = try r.takeByte();

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

    pub fn writeLog(self: *@This(), io: std.Io, data: [][]types.AuditEntry) !void {
        var bulk = try ch.BulkInsert.init(self.allocator, "entries", &columns, 1000);
        defer bulk.deinit();

        std.debug.print("Initialized bulk insert\n", .{});

        bulk.setCompression(.LZ4);

        self.startInsert(io, "INSERT INTO entries FORMAT Native") catch |err| {
            if (err == error.QueryFailed) {
                std.debug.print("Query failed: {s}\n", .{self.last_error.?.message});
            }
            return err;
        };

        std.debug.print("Sending data\n", .{});

        var buf = std.ArrayList(ch.bulk_insert.Value).empty;
        defer buf.deinit(self.allocator);

        var old_values = std.StringHashMap([]const u8).init(self.allocator);
        defer old_values.deinit();

        var new_values = std.StringHashMap([]const u8).init(self.allocator);
        defer new_values.deinit();

        var i: u32 = 0;
        while (i < data.len) : (i += 1) {
            for (data[i]) |row| {
                buf.clearRetainingCapacity();
                old_values.clearRetainingCapacity();
                new_values.clearRetainingCapacity();

                try buf.ensureUnusedCapacity(self.allocator, row.changed_columns.capacity());
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
                    bulk.flush(io, self.stream.?) catch |err| {
                        std.debug.print("flush failed: {}\n", .{err});
                        self.processQueryResponse(io) catch {};
                        if (self.last_error) |e| {
                            std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
                        }
                        return err;
                    };
                }
            }
        }

        // Flush any remaining rows
        bulk.flush(io, self.stream.?) catch |err| {
            std.debug.print("flush failed: {}\n", .{err});
            self.processQueryResponse(io) catch {};
            if (self.last_error) |e| {
                std.debug.print("SERVER EXCEPTION: {s}\n", .{e.message});
            }
            return err;
        };

        std.debug.print("Executed insert\n", .{});

        self.closeStream(io) catch |err| {
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

    pub fn closeStream(self: *ChClient, io: std.Io) !void {
        var buf: [1024]u8 = undefined;
        var writer = self.stream.?.writer(io, &buf);
        var w = &writer.interface;

        try ch.packet.writeClientPacketHeader(w, .Data);
        try ch.protocol.writeString(w, ""); // block name
        try ch.protocol.writeVarInt(w, 1); // is_overflows
        try w.writeInt(u8, 0, .little);
        try ch.protocol.writeVarInt(w, 2); // bucket_num
        try w.writeInt(i32, -1, .little);
        try ch.protocol.writeVarInt(w, 0); // end block info

        try ch.protocol.writeVarInt(w, 0); // num_columns = 0
        try ch.protocol.writeVarInt(w, 0); // num_rows = 0
        try w.flush();

        try self.processQueryResponse(io);
    }


    pub fn deinit(self: *ChClient, io: Io) void {
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
            stream.close(io);
        }
    }
};
