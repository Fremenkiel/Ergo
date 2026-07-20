const std = @import("std");
const mem = std.mem;

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

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        defer allocator.free(self.table_name);
        defer self.changed_columns.deinit();
        defer self.old_values.deinit(allocator);
        defer self.new_values.deinit(allocator);
    }
};

test "AuditEntry ensure correct deinit" {
    const allocator = std.testing.allocator;

    var changed_columns = std.StringHashMap(ChangedColumns).init(allocator);
    try changed_columns.ensureUnusedCapacity(3);

    var old_values = std.StringHashMapUnmanaged([]const u8).empty;
    try old_values.ensureUnusedCapacity(allocator, 3);

    var new_values = std.StringHashMapUnmanaged([]const u8).empty;
    try new_values.ensureUnusedCapacity(allocator, 3);

    var entry = AuditEntry{
        .event_time = 1244,
        .transaction_id = 10,
        .primary_key = "24",
        .user_id = "43",
        .table_name = try allocator.dupe(u8, "test"),
        .action = 1,
        .changed_columns = changed_columns,
        .old_values = old_values,
        .new_values = new_values,
        .ip_address = "192.168.1.50",
    };
    defer entry.deinit(allocator);
}
