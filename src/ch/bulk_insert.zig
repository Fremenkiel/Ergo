const std = @import("std");
const block = @import("block.zig");
const types = @import("types.zig");
const compression = @import("compression.zig");
const protocol = @import("protocol.zig");
const packet = @import("packet.zig");

pub const BulkInsert = struct {
    allocator: std.mem.Allocator,
    table: []const u8,
    columns: []Column,
    batch_size: usize,
    current_row: usize,
    compression_method: compression.CompressionMethod,
    
    pub const Column = struct {
        name: []const u8,
        type_info: types.TypeInfo,
        lc_keys: std.StringHashMap(u16),
        key_count: u16,
        array_offset: std.ArrayList(u8),
        map_keys: std.ArrayList(u8),
        offset_count: u64,
        data: std.ArrayList(u8),
    };

    pub fn init(
        allocator: std.mem.Allocator,
        table: []const u8,
        column_defs: []const ColumnDef,
        batch_size: usize,
    ) !BulkInsert {
        var columns = try allocator.alloc(Column, column_defs.len);
        
        for (column_defs, 0..) |def, i| {
            columns[i] = .{
                .name = try allocator.dupe(u8, def.name),
                .type_info = try types.TypeInfo.parse(allocator, def.type_str),
                .lc_keys = std.StringHashMap(u16).init(allocator),
                .key_count = 0,
                .array_offset = std.ArrayList(u8).empty,
                .map_keys = std.ArrayList(u8).empty,
                .offset_count = 0,
                .data = std.ArrayList(u8).empty,
            };
        }

        var bulk = BulkInsert{
            .allocator = allocator,
            .table = try allocator.dupe(u8, table),
            .columns = columns,
            .batch_size = batch_size,
            .current_row = 0,
            .compression_method = .None,
        };

        bulk.setCompression(.LZ4);

        return bulk;
    }

    pub fn deinit(self: *BulkInsert) void {
        for (self.columns) |*column| {
            self.allocator.free(column.name);

            column.data.clearRetainingCapacity();
            column.data.deinit(self.allocator);
            
            column.lc_keys.clearRetainingCapacity();
            column.lc_keys.deinit();

            column.map_keys.clearRetainingCapacity();
            column.map_keys.deinit(self.allocator);

            column.array_offset.clearRetainingCapacity();
            column.array_offset.deinit(self.allocator);

            column.key_count = 0;
            column.type_info.deinit(self.allocator);
        }
        self.allocator.free(self.columns);
        self.allocator.free(self.table);
    }

    pub fn setCompression(self: *BulkInsert, method: compression.CompressionMethod) void {
        self.compression_method = method;
    }

    pub fn addRow(self: *BulkInsert, values: []const Value) !bool {
        if (values.len != self.columns.len) return error.ColumnCountMismatch;

        for (values, 0..) |value, i| {
            try self.addValue(&self.columns[i], value);
        }

        self.current_row += 1;
        return self.current_row >= self.batch_size;
    }

    pub fn flush(self: *BulkInsert, io: std.Io, stream: std.Io.net.Stream) !void {
        if (self.current_row == 0) return;

        var block_data = try self.createBlock();
        defer block_data.deinit();

        var buf: [1024]u8 = undefined;
        var writer = stream.writer(io, &buf);
        var w = &writer.interface;

        try packet.writeClientPacketHeader(w, .Data);

        try protocol.writeString(w, "");

        if (self.compression_method != .None) {
            const serialized_block = try block_data.serialize();
            defer self.allocator.free(serialized_block);
            
            var compressed = try compression.CompressedData.compress(
                self.allocator,
                serialized_block,
                self.compression_method,
            );
            defer compressed.deinit(self.allocator);

            try w.writeAll(serialized_block);
        } else {
            const serialized_block = try block_data.serialize();
            defer self.allocator.free(serialized_block);
            try w.writeAll(serialized_block);
        }

        try w.flush();

        self.reset();
    }

    fn writeVarIntToList(self: *BulkInsert, list: *std.ArrayList(u8), value: u64) !void {
        var v = value;
        var buf: [10]u8 = undefined;
        var i: usize = 0;
        while (true) {
            var byte: u8 = @truncate(v);
            byte &= 0x7F;
            v >>= 7;
            if (v != 0) {
                byte |= 0x80;
                buf[i] = byte;
                i += 1;
            } else {
                buf[i] = byte;
                i += 1;
                break;
            }
        }
        try list.appendSlice(self.allocator, buf[0..i]);
    }

    fn addValue(self: *BulkInsert, column: *Column, value: Value) anyerror!void {
        var buf: [8]u8 = undefined;
        switch (value) {
            .UInt64 => |v| { std.mem.writeInt(u64, &buf[0..8].*, v, .little); try column.data.appendSlice(self.allocator, buf[0..8]); },
            .String => |v| {
                try self.writeVarIntToList(&column.data, v.len);
                try column.data.appendSlice(self.allocator, v);
            },
            .LowCardinality => |v| { 
                const res = try column.lc_keys.getOrPut(v);

                if (!res.found_existing) { res.value_ptr.* = column.key_count; column.key_count += 1; }
                std.mem.writeInt(u16, &buf[0..2].*, res.value_ptr.*, .little);
                try column.data.appendSlice(self.allocator, buf[0..2]);
            },
            .DateTime64 => |v| { std.mem.writeInt(i64, &buf[0..8].*, @intCast(v), .little); try column.data.appendSlice(self.allocator, buf[0..8]); },
            .Enum8 => |v| { std.mem.writeInt(i8, &buf[0..1].*, v, .little); try column.data.appendSlice(self.allocator, buf[0..1]); },
            .Array => |v| {
                column.offset_count += v.len;
                std.mem.writeInt(u64, &buf[0..8].*, column.offset_count, .little);
                try column.array_offset.appendSlice(self.allocator, buf[0..8]);
                try self.addArray(column, v);
            },
            .Map => |v| {
                var it = v.keyIterator();
                while (it.next()) |key_ptr| {
                    try self.writeVarIntToList(&column.map_keys, key_ptr.*.len);
                    try column.map_keys.appendSlice(self.allocator, key_ptr.*);

                    const res = v.get(key_ptr.*);

                    try self.writeVarIntToList(&column.data, res.?.len);
                    try column.data.appendSlice(self.allocator, res.?);

                    column.offset_count += 1;
                }
                std.mem.writeInt(u64, &buf[0..8].*, column.offset_count, .little);
                try column.array_offset.appendSlice(self.allocator, buf[0..8]);
            },
            .IPv4 => |v| {
                const ip_bytes = try parseIp4ToBytes(v);
                try column.data.appendSlice(self.allocator, &ip_bytes);
            }
        }
    }

    fn addArray(self: *BulkInsert, column: *Column, values: []const Value) anyerror!void {
        for (values) |value| {
            try self.addValue(column, value);
        }
    }

    fn createBlock(self: *BulkInsert) !block.Block {
        var result = block.Block.init(self.allocator);
        
        for (self.columns) |column| {
            var type_name: []const u8 = undefined;
            defer self.allocator.free(type_name);
            switch (column.type_info.base_type) {
                .LowCardinality => type_name = try std.fmt.allocPrint(self.allocator, "{s}({s})", .{@tagName(column.type_info.base_type), @tagName(column.type_info.key_type.?.base_type)}),
                .Enum8 => type_name = try std.fmt.allocPrint(self.allocator, "{s}({s})", .{@tagName(column.type_info.base_type), column.type_info.enum_values.?}),
                .Array => type_name = try std.fmt.allocPrint(self.allocator, "{s}({s})", .{@tagName(column.type_info.base_type), @tagName(column.type_info.key_type.?.base_type)}),
                .Map => type_name = try std.fmt.allocPrint(self.allocator, "{s}({s}, {s})", .{@tagName(column.type_info.base_type), @tagName(column.type_info.key_type.?.base_type), @tagName(column.type_info.value_type.?.base_type)}),
                else => type_name = try self.allocator.dupe(u8, @tagName(column.type_info.base_type)),
            }
            try result.addColumn(column.name, type_name);

            const col_idx = result.columns.len - 1;
            result.columns[col_idx].data = try self.allocator.dupe(u8, column.data.items);
            result.columns[col_idx].lc_keys = column.lc_keys;
            result.columns[col_idx].key_count = column.key_count;
            result.columns[col_idx].array_offset = try self.allocator.dupe(u8, column.array_offset.items);
            result.columns[col_idx].map_keys = try self.allocator.dupe(u8, column.map_keys.items);
        }
        
        result.rows = self.current_row;
        return result;
    }

    fn reset(self: *BulkInsert) void {
        for (self.columns) |*column| {
            column.data.clearRetainingCapacity();
        }
        self.current_row = 0;
    }

    fn parseIp4ToBytes(ip: []const u8) ![4]u8 {
        var bytes: [4]u8 = undefined;
        var current_octet: u16 = 0; // u16 prevents overflow when multiplying by 10
        var index: u8 = 3;

        for (ip) |c| {
            switch (c) {
                '.' => {
                    if (index < 0) return error.InvalidIP;
                    bytes[index] = @as(u8, @intCast(current_octet));
                    current_octet = 0;
                    index -= 1;
                },
                '0'...'9' => {
                    current_octet = current_octet * 10 + (c - '0');
                    if (current_octet > 255) return error.InvalidIP;
                },
                else => return error.InvalidCharacter,
            }
        }

        if (index != 0) return error.InvalidIP;
        bytes[0] = @as(u8, @intCast(current_octet));

        return bytes;
    }
};

pub const ColumnDef = struct {
    name: []const u8,
    type_str: []const u8,
};

pub const Value = union(enum) {
    UInt64: u64,
    String: []const u8,
    LowCardinality: []const u8,
    DateTime64: i64,
    Enum8: i8,
    Array: []const Value,
    Map: std.hash_map.StringHashMap([]const u8),
    IPv4: []const u8,
};
