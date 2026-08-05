const std = @import("std");

const testing = std.testing;

pub const protocol = @import("protocol.zig");

pub const packet = @import("packet.zig");
pub const conn = @import("conn.zig");

// pub const Pool = @import("pool.zig").Pool;
pub const Error = @import("error.zig").Error;

pub const Conn = conn.Conn;

pub const PgError = error {
UnableToAuthenticate,
UnexpectedDBMessage,
InvalidSASLFlow,
InvalidData,
InvalidMessageLength,
InvalidBufferLength,
InvalidMessageType,
InvalidStringDelimitor,
InvalidAffectedCount,
AuthNotSupported,
InvalidResponseCode,
InvalidWrite,
InvalidType,
UnexpectedNull,
UnknownColumnName,
};

pub const PgConfig = struct {
    host: []const u8,
    port: ?u16 = null,
    wal: []const u8 = "wal_slot",
    write_buffer: ?u16 = null,
    read_buffer: ?u16 = null,
    result_state_size: u16 = 32,
    tls: TLS = .off,
    hostz: ?[:0]const u8 = null,

    // tcp keepalive settings (null timer = OS default)
    keepalive: bool = true,
    keepalive_idle: ?u32 = 30,
    keepalive_interval: ?u32 = 10,
    keepalive_count: ?u32 = 3,

    // auth
    username: []const u8 = "postgres",
    password: ?[]const u8 = null,
    database: []const u8,
    timeout_ms: i32 = 500,
    application_name: []const u8,
    startup_parameters: ?std.hash_map.StringHashMap([]const u8),

    pub const TLS = union(enum) {
        off: void,
        require: void,
        verify_full: ?[]const u8,
    };
};

test "tests:beforeAll" {
    std.testing.refAllDecls(@This());
}
