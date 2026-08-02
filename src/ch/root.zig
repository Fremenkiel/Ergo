const std = @import("std");

pub const protocol = @import("protocol.zig");
pub const packet = @import("packet.zig");
pub const block = @import("block.zig");
pub const bulk_insert = @import("bulk_insert.zig");
pub const compression = @import("compression.zig");
pub const ch_error = @import("error.zig");

pub const ClickHouseType = @import("types.zig").ClickHouseType;

pub const BulkInsert = @import("bulk_insert.zig").BulkInsert;

pub const ChError = error{
    ConnectionFailed,
    QueryFailed,
    InvalidResponse,
    OutOfMemory,
    ProtocolError,
    CompressionError,
    TypeMismatch,
    QueryCancelled,
};

pub const ChConfig = struct {
    host: []const u8,
    port: u16 = 9000,
    username: []const u8 = "default",
    password: []const u8 = "",
    database: []const u8 = "default",
    application_name: []const u8 = "Ergo",
    settings: Settings = .{},

    const Settings = struct {
        max_block_size: u64 = 65536,
        connect_timeout_ms: u64 = 10000,
        receive_timeout_ms: u64 = 10000,
        send_timeout_ms: u64 = 10000,
        tcp_keep_alive: bool = true,
        tcp_nodelay: bool = true,
        compression_method: u8 = 0,
        decompress_response: bool = true,
    };
};

test "run all ch module tests" {
    std.testing.refAllDecls(@This());
}
