const std = @import("std");
const types = @import("types.zig");
const block = @import("block.zig");

pub const QueryResult = struct {
    allocator: std.mem.Allocator,
    columns: []Column,
    rows: usize,
    current_row: usize,

    pub const Column = struct {
        name: []const u8,
        type: types.ClickHouseType,
        data: ColumnData,
    };

    pub const ColumnData = union(enum) {
        Int8: []i8,
        Int16: []i16,
        Int32: []i32,
        Int64: []i64,
        UInt8: []u8,
        UInt16: []u16,
        UInt32: []u32,
        UInt64: []u64,
        Float32: []f32,
        Float64: []f64,
        String: [][]const u8,
        DateTime64: []i64,
        Date: []i32,
        UUID: [][16]u8,
        Nullable: struct {
            nulls: []bool,
            data: *const ColumnData,
        },
    };

    pub fn init(allocator: std.mem.Allocator, cur_block: *block.Block) !QueryResult {
        var columns = try allocator.alloc(Column, cur_block.columns.len);
        var total_rows: usize = 0;

        for (cur_block.columns, 0..) |col, i| {
            columns[i] = try parseColumn(allocator, col);
            if (i == 0) total_rows = columnLength(columns[i].data);
        }

        return QueryResult{
            .allocator = allocator,
            .columns = columns,
            .rows = total_rows,
            .current_row = 0,
        };
    }

    fn parseColumn(allocator: std.mem.Allocator, col: block.Column) !Column {
        const parsed_data = try parseColumnData(allocator, col);
        return Column{
            .name = try allocator.dupe(u8, col.name),
            .type = col.type,
            .data = parsed_data,
        };
    }

    fn parseColumnData(allocator: std.mem.Allocator, col: block.Column) !ColumnData {
        return switch (col.type) {
            .UInt64 => .{ .UInt64 = try parseNumericColumn(u64, allocator, col.data) },
            .String => .{ .String = try parseStringColumn(allocator, col.data) },
            .DateTime64 => .{ .DateTime64 = try parseNumericColumn(i64, allocator, col.data) },
            else => return error.UnsupportedType,
        };
    }

    fn parseNumericColumn(comptime T: type, allocator: std.mem.Allocator, data: []const u8) ![]T {
        const count = @divExact(data.len, @sizeOf(T));
        const result = try allocator.alloc(T, count);
        @memcpy(std.mem.sliceAsBytes(result), data);
        return result;
    }

    fn parseStringColumn(allocator: std.mem.Allocator, data: []const u8) ![][]const u8 {
        var strings = std.ArrayList([]const u8).empty;
        defer strings.deinit(allocator);

        var i: usize = 0;
        while (i < data.len) {
            const len = std.mem.readInt(u64, data[i..][0..8], .little);
            i += 8;
            const str = try allocator.dupe(u8, data[i..][0..len]);
            try strings.append(allocator, str);
            i += len;
        }

        return strings.toOwnedSlice(allocator);
    }

    fn parseUUIDColumn(allocator: std.mem.Allocator, data: []const u8) ![][16]u8 {
        const count = @divExact(data.len, 16);
        var result = try allocator.alloc([16]u8, count);
        var i: usize = 0;
        while (i < count) : (i += 1) {
            @memcpy(&result[i], data[i * 16 .. (i + 1) * 16]);
        }
        return result;
    }

    fn columnLength(data: ColumnData) usize {
        return switch (data) {
            .Int8 => |v| v.len,
            .Int16 => |v| v.len,
            .Int32 => |v| v.len,
            .Int64 => |v| v.len,
            .UInt8 => |v| v.len,
            .UInt16 => |v| v.len,
            .UInt32 => |v| v.len,
            .UInt64 => |v| v.len,
            .Float32 => |v| v.len,
            .Float64 => |v| v.len,
            .String => |v| v.len,
            .DateTime64 => |v| v.len,
            .Date => |v| v.len,
            .UUID => |v| v.len,
            .Nullable => |v| v.nulls.len,
        };
    }

    pub fn next(self: *QueryResult) bool {
        if (self.current_row >= self.rows) return false;
        self.current_row += 1;
        return true;
    }

    pub fn getValue(self: QueryResult, column_index: usize) !ValueRef {
        if (column_index >= self.columns.len) return error.InvalidColumnIndex;
        if (self.current_row == 0 or self.current_row > self.rows) return error.InvalidRowIndex;

        const col = self.columns[column_index];
        const row_idx = self.current_row - 1;

        return switch (col.data) {
            .Int8 => |v| .{ .Int8 = v[row_idx] },
            .Int16 => |v| .{ .Int16 = v[row_idx] },
            .Int32 => |v| .{ .Int32 = v[row_idx] },
            .Int64 => |v| .{ .Int64 = v[row_idx] },
            .UInt8 => |v| .{ .UInt8 = v[row_idx] },
            .UInt16 => |v| .{ .UInt16 = v[row_idx] },
            .UInt32 => |v| .{ .UInt32 = v[row_idx] },
            .UInt64 => |v| .{ .UInt64 = v[row_idx] },
            .Float32 => |v| .{ .Float32 = v[row_idx] },
            .Float64 => |v| .{ .Float64 = v[row_idx] },
            .String => |v| .{ .String = v[row_idx] },
            .DateTime64 => |v| .{ .DateTime64 = v[row_idx] },
            .Date => |v| .{ .Date = v[row_idx] },
            .UUID => |v| .{ .UUID = v[row_idx] },
            .Nullable => |v| if (v.nulls[row_idx]) .Null else try self.getValue(column_index),
        };
    }

    pub const ValueRef = union(enum) {
        Int8: i8,
        Int16: i16,
        Int32: i32,
        Int64: i64,
        UInt8: u8,
        UInt16: u16,
        UInt32: u32,
        UInt64: u64,
        Float32: f32,
        Float64: f64,
        String: []const u8,
        DateTime64: i64,
        Date: i32,
        UUID: [16]u8,
        Null,
    };

    pub fn deinit(self: *QueryResult) void {
        for (self.columns) |col| {
            self.allocator.free(col.name);
            self.freeColumnData(col.data);
        }
        self.allocator.free(self.columns);
    }

    fn freeColumnData(self: *QueryResult, data: ColumnData) void {
        switch (data) {
            .String => |v| {
                for (v) |str| {
                    self.allocator.free(str);
                }
                self.allocator.free(v);
            },
            .Nullable => |v| {
                self.allocator.free(v.nulls);
                self.freeColumnData(v.data.*);
                self.allocator.destroy(@constCast(v.data));
            },
            inline else => |v| self.allocator.free(v),
        }
    }
};
