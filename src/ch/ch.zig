pub const protocol = @import("protocol.zig");
pub const packet = @import("packet.zig");
pub const block = @import("block.zig");
pub const bulk_insert = @import("bulk_insert.zig");
pub const settings = @import("settings.zig");
pub const compression = @import("compression.zig");
pub const results = @import("results.zig");
pub const ch_error = @import("error.zig");
pub const server_info = @import("server_info.zig");
pub const query_info = @import("query_info.zig");
pub const progress = @import("progress.zig");
pub const statistics = @import("statistics.zig");
pub const profile = @import("profile_info.zig");

pub const ClickHouseType = @import("types.zig").ClickHouseType;

pub const BulkInsert = @import("bulk_insert.zig").BulkInsert;

pub const ClickHouseError = error{
    ConnectionFailed,
    QueryFailed,
    InvalidResponse,
    OutOfMemory,
    ProtocolError,
    CompressionError,
    TypeMismatch,
    QueryCancelled,
};

pub const ClickHouseConfig = struct {
    host: []const u8,
    port: u16 = 9000,
    username: []const u8 = "default",
    password: []const u8 = "",
    database: []const u8 = "default",
    settings: settings.Settings = .{},
};

