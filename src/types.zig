const std = @import("std");

const Io = std.Io;
const mem = std.mem;

pub const ColumnChange = struct {
    column_name: []const u8,
    old_value: ?[]const u8,
    new_value: ?[]const u8,
    has_changes: bool,
    is_key: bool,

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        allocator.free(self.column_name);
        if (self.old_value) |val| {
            allocator.free(val);
        }

        if (self.new_value) |val| {
            allocator.free(val);
        }
    }

    pub fn copy(self: *@This(), allocator: mem.Allocator) !@This() {
        return .{
            .column_name = try allocator.dupe(u8, self.column_name),
            .old_value = if (self.old_value) |old_value| try allocator.dupe(u8, old_value) else null,
            .new_value = if (self.new_value) |new_value| try allocator.dupe(u8, new_value) else null,
            .has_changes = self.has_changes,
            .is_key = self.is_key,
        };
    }
};

pub const Row = struct {
    table_name: []const u8,
    action: i8,
    columns: std.ArrayList(ColumnChange),

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |*col| { 
            col.deinit(allocator);
        }

        self.columns.clearAndFree(allocator);
        self.columns.deinit(allocator);
    }

    pub fn copy(self: *@This(), allocator: mem.Allocator) !@This() {
        var copy_columns: std.ArrayList(ColumnChange) = .empty;
        try copy_columns.ensureUnusedCapacity(allocator, self.columns.items.len);

        for (self.columns.items) |*col| {
            copy_columns.appendAssumeCapacity(try col.copy(allocator));
        }

        return .{
            .table_name = try allocator.dupe(u8, self.table_name),
            .action = self.action,
            .columns = copy_columns,
        };
    }
};

pub const TransactionMeta = struct {
    event_time: Io.Timestamp,
    transaction_id: u64,
    user_id: []const u8,
    ip_address: []const u8,

    pub const empty: @This() = .{
        .event_time = undefined,
        .transaction_id = undefined,
        .user_id = undefined,
        .ip_address = undefined,
    };

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        if (self.user_id.len > 0) allocator.free(self.user_id);
        if (self.ip_address.len > 0) allocator.free(self.ip_address);
    }

    pub fn copy(self: *@This(), allocator: mem.Allocator) !@This() {
        return .{
            .event_time = self.event_time,
            .transaction_id = self.transaction_id,
            .user_id = try allocator.dupe(u8, self.user_id),
            .ip_address = try allocator.dupe(u8, self.ip_address),
        };
    }
};

pub const Transaction = struct {
    meta: TransactionMeta,
    rows: std.ArrayList(Row),

    pub const empty: @This() = .{
        .meta = .empty,
        .rows = .empty,
    };

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        self.meta.deinit(allocator);

        for (self.rows.items) |*row| { 
            row.deinit(allocator);
        }
        self.rows.deinit(allocator);
    }

    pub fn clearAndFree(self: *@This(), allocator: mem.Allocator) void {
        for (self.rows.items) |*row| { 
            for (row.columns.items) |*col| { 
                col.deinit(allocator);
            }
            row.columns.clearAndFree(allocator);
        }
        self.rows.clearAndFree(allocator);
    }

    pub fn copy(self: *@This(), allocator: mem.Allocator) !@This() {
        var copy_rows: std.ArrayList(Row) = .empty;
        try copy_rows.ensureUnusedCapacity(allocator, self.rows.items.len);

        for (self.rows.items) |*row| {
            copy_rows.appendAssumeCapacity(try row.copy(allocator));
        }

        return .{
            .meta = try self.meta.copy(allocator),
            .rows = copy_rows,
        };
    }
};

pub const AuditEntry = struct {
    event_time: i64,
    transaction_id: u64,
    user_id: []const u8,
    table_name: []const u8,
    action: i8,
    columns: std.ArrayList(ColumnChange),
    ip_address: []const u8,

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        allocator.free(self.table_name);
        if (self.user_id.len > 0) allocator.free(self.user_id);
        if (self.ip_address.len > 0) allocator.free(self.ip_address);

        self.clearAndFree(allocator);
        self.columns.deinit(allocator);
    }

    pub fn clearAndFree(self: *@This(), allocator: mem.Allocator) void {
        for (self.columns.items) |*col| { 
            col.deinit(allocator);
        }
        self.columns.clearAndFree(allocator);
    }
};

test "AuditEntry ensure correct deinit" {
    const allocator = std.testing.allocator;

    var columns: std.ArrayList(ColumnChange) = .empty;
    try columns.ensureUnusedCapacity(allocator, 3);

    for (columns.items) |*col| {
        col.column_name = try allocator.dupe(u8, "the_col_name");
        col.new_value = try allocator.dupe(u8, "old value");
        col.old_value = try allocator.dupe(u8, "new value"); 
        col.has_changes = true;
        col.is_key = false;
    }

    var entry = AuditEntry{
        .event_time = 1244,
        .transaction_id = 10,
        .user_id = try allocator.dupe(u8, "43"),
        .table_name = try allocator.dupe(u8, "test"),
        .action = 1,
        .columns = columns,
        .ip_address = try allocator.dupe(u8, "192.168.1.50"),
    };
    entry.deinit(allocator);
}
