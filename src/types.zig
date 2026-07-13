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
    changed_columns: std.StringHashMapUnmanaged(ChangedColumns) = .empty,
    old_values: std.StringHashMapUnmanaged([]const u8) = .empty,
    new_values: std.StringHashMapUnmanaged([]const u8) = .empty,
    ip_address: []const u8,
};
