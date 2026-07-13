const std = @import("std");
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
pub const Column = struct {
    name: []const u8,
    type_name: []const u8,
    type: types.ClickHouseType,
    lc_keys: std.StringHashMap(u16),
    key_count: u16,
    array_offset: []u8,
    map_keys: []u8,
    data: []const u8,
    
    pub fn init(allocator: Allocator, name: []const u8, type_str: []const u8) !Column {
        const ch_type = try types.ClickHouseType.fromStr(type_str);
        return Column{
            .name = try allocator.dupe(u8, name),
            .type_name = try allocator.dupe(u8, type_str),
            .type = ch_type,
            .lc_keys = std.StringHashMap(u16).init(allocator),
            .key_count = undefined,
            .array_offset = &[_]u8{},
            .map_keys = &[_]u8{},
            .data = &[_]u8{},
        };
    }
};
pub const Block = struct {
    columns: []Column,
    rows: usize,
    allocator: Allocator,
    pub fn init(allocator: Allocator) Block {
        return Block{
            .columns = &[_]Column{},
            .rows = 0,
            .allocator = allocator,
        };
    }
    pub fn addColumn(self: *Block, name: []const u8, type_str: []const u8) !void {
        std.debug.print("Add column name: {s}, type: {s}\n", .{name,type_str});
        const column = try Column.init(self.allocator, name, type_str);
        const new_columns = try self.allocator.realloc(self.columns, self.columns.len + 1);
        new_columns[new_columns.len - 1] = column;
        self.columns = new_columns;
    }
    pub fn serialize(self: *Block) ![]u8 {
        var arr = std.ArrayList(u8).empty;
        var aw = std.Io.Writer.Allocating.fromArrayList(self.allocator, &arr);
        defer aw.deinit();

        const protocol = @import("protocol.zig");
        // 1. Block info
        try protocol.writeVarInt(&aw.writer, 1);
        try aw.writer.writeInt(u8, 0, .little);
        try protocol.writeVarInt(&aw.writer, 2);
        try aw.writer.writeInt(i32, -1, .little);
        try protocol.writeVarInt(&aw.writer, 0);

        // 2. Columns and rows
        try protocol.writeVarInt(&aw.writer, self.columns.len);
        try protocol.writeVarInt(&aw.writer, self.rows);

        // 3. For each column
        for (self.columns) |col| {
            try protocol.writeString(&aw.writer, col.name);
            try protocol.writeString(&aw.writer, col.type_name);

            switch (col.type) {
                .LowCardinality => {
                    try aw.writer.writeInt(u64, 1, .little);
                    try aw.writer.writeInt(u64, @intCast(0x0601), .little);
                    try aw.writer.writeInt(u64, col.key_count, .little);
                    var it = col.lc_keys.keyIterator();
                    while (it.next()) |key_ptr| {
                        try protocol.writeString(&aw.writer, key_ptr.*);
                    }
                    try aw.writer.writeInt(u64, self.rows, .little);
                    try aw.writer.writeAll(col.data);
                },
                .Array => {
                    try aw.writer.writeAll(col.array_offset);
                    try aw.writer.writeAll(col.data);
                },
                .Map => {
                    try aw.writer.writeAll(col.array_offset);
                    try aw.writer.writeAll(col.map_keys);
                    try aw.writer.writeAll(col.data);
                },
                else => {
                    try aw.writer.writeAll(col.data);
                }
            }
        }

        var final_arr = aw.toArrayList();
        return final_arr.toOwnedSlice(self.allocator);
    }

    pub fn deinit(self: *Block) void {
        for (self.columns) |*column| {
            self.allocator.free(column.name);
            self.allocator.free(column.type_name);
            column.lc_keys.clearRetainingCapacity();
            column.key_count = 0;
            self.allocator.free(column.data);
            self.allocator.free(column.array_offset);
            self.allocator.free(column.map_keys);
        }
        self.allocator.free(self.columns);
    }
};
