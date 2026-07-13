const std = @import("std");
const protocol = @import("protocol.zig");

pub const Settings = struct {
    // Connection settings
    max_block_size: u64 = 65536,
    connect_timeout_ms: u64 = 10000,
    receive_timeout_ms: u64 = 10000,
    send_timeout_ms: u64 = 10000,
    tcp_keep_alive: bool = true,
    tcp_nodelay: bool = true,
    compression_method: u8 = 0,
    decompress_response: bool = true,
    
    // Query settings
    max_insert_block_size: u64 = 1048576,
    max_threads: u32 = 8,
    max_memory_usage: u64 = 0,
    prefer_localhost_replica: bool = true,
    totals_mode: TotalsMode = .AfterHavingGroupBy,
    quota_key: ?[]const u8 = null,
    priority: u32 = 0,
    load_balancing: LoadBalancing = .Random,
    max_execution_time: u64 = 0,
    max_rows_to_read: u64 = 0,
    max_bytes_to_read: u64 = 0,
    max_result_rows: u64 = 0,
    max_result_bytes: u64 = 0,
    result_overflow_mode: OverflowMode = .Break,
    
    pub const TotalsMode = enum {
        BeforeHavingGroupBy,
        AfterHavingGroupBy,
        OnlyFinal,
    };

    pub const LoadBalancing = enum {
        Random,
        NearestHost,
        InOrder,
        FirstOrRandom,
    };

    pub const OverflowMode = enum {
        Break,
        Throw,
        Any,
    };

    pub fn write(self: Settings, allocator: std.mem.Allocator, writer: *std.Io.Writer) !void {
        var settings_buf = std.ArrayList(u8).empty;
        defer settings_buf.deinit(allocator);

        // Write each setting
        inline for (std.meta.fields(Settings)) |field| {
            const value = @field(self, field.name);
            if (shouldWriteSetting(field.type, value)) {
                try writeSettingValue(allocator, &settings_buf, field.name, value);
            }
        }
        
        // Write settings buffer
        try writer.writeAll(settings_buf.items);
        
        // Settings block is terminated by an empty string
        try protocol.writeString(writer, "");
    }

    fn shouldWriteSetting(comptime T: type, value: T) bool {
        return switch (@typeInfo(T)) {
            .optional => value != null,
            .int, .float => value != 0,
            .bool => value != false,
            .enum_literal => true,
            else => false,
        };
    }

    fn writeSettingValue(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), name: []const u8, value: anytype) !void {
        var w = std.Io.Writer.Allocating.fromArrayList(allocator, buf);
        defer w.deinit();

        try w.writer.writeInt(u8, @as(u8, @intCast(name.len)), .little);
        try w.writer.writeAll(name);

        const T = @TypeOf(value);
        switch (@typeInfo(T)) {
            .int => |info| {
                try w.writer.writeInt(u8, if (info.bits <= 8) 0 else 1, .little);
                if (info.bits <= 8) {
                    try w.writer.writeInt(u8, @as(u8, @intCast(value)), .little);
                } else {
                    try w.writer.writeInt(u64, value, .little);
                }
            },
            .float => {
                try w.writer.writeInt(u8, 3, .little);
                try w.writer.writeInt(f64, value, .little);
            },
            .bool => {
                try w.writer.writeInt(u8, 0, .little);
                try w.writer.writeInt(u8, @intFromBool(value), .little);
            },
            .enum_literal => {
                try w.writer.writeInt(u8, 2, .little);
                try w.writer.writeAll(@tagName(value));
            },
            .optional => if (value) |v| {
                try writeSettingValue(allocator, buf, name, v);
            },
            else => unreachable,
        }

        buf.* = w.toArrayList();
    }
};
