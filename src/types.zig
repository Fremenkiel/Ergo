const std = @import("std");

pub const ChangedColumns = struct {
    value: []const u8,
    has_changes: bool,
};

pub const AuditEntry = struct {
    event_time: i64,
    transaction_id: u64,
    primary_key: []const u8,
    user_id: []const u8,
    table_name: []const u8,
    action: i8,
    changed_columns: std.StringHashMap(ChangedColumns),
    old_values: std.StringHashMapUnmanaged([]const u8) = .empty,
    new_values: std.StringHashMapUnmanaged([]const u8) = .empty,
    ip_address: []const u8,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        defer allocator.free(self.table_name);
        defer self.changed_columns.deinit();
        defer self.old_values.deinit(allocator);
        defer self.new_values.deinit(allocator);
    }
};

pub const LogArray = struct {
    base: std.ArrayList([]AuditEntry),

    pub const empty: @This() = .{
        .base = .empty
    };

    pub fn initCapacity(allocator: std.mem.Allocator, size: usize) LogArray {
        return .{
            .base = try .initCapacity(allocator, size)
        };
    }

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        for (self.base.items) |entry| {
            for (entry) |*item| item.deinit(allocator);
            allocator.free(entry);
        }
        self.base.deinit(allocator);
    }

    pub fn free(self: *@This(), allocator: std.mem.Allocator) void {
        for (self.base.items) |entry| {
            for (entry) |*item| item.deinit(allocator);
            allocator.free(entry);
        }
        self.base.clearRetainingCapacity();
    }

    pub fn items(self: *@This()) [][]AuditEntry {
        return self.base.items;
    }

    pub fn append(self: *@This(), allocator: std.mem.Allocator, item: []AuditEntry) !void {
        try self.base.append(allocator, item);
    }

    pub fn ensureUnusedCapacity(self: *@This(), allocator: std.mem.Allocator, size: usize) !void {
        try self.base.ensureUnusedCapacity(allocator, size);
    }
};
