const std = @import("std");
const builtin = @import("builtin");

const compression = @import("compression.zig");

pub const CLIENT_NAME = "ClickHouse Zig Client";
pub const CLIENT_VERSION_MAJOR: u64 = 1;
pub const CLIENT_VERSION_MINOR: u64 = 0;
pub const CLIENT_VERSION_PATCH: u64 = 0;

pub const PROTOCOL_VERSION: u64 = 54449;

pub const ClientHello = struct {
    pub fn write(writer: *std.Io.Writer) !void {
        try writeString(writer, CLIENT_NAME);
        try writeVarInt(writer, CLIENT_VERSION_MAJOR);
        try writeVarInt(writer, CLIENT_VERSION_MINOR);
        try writeVarInt(writer, PROTOCOL_VERSION);
    }
};

pub const ClientInfo = struct {
    pub fn write(writer: *std.Io.Writer, query_id: []const u8, initial_user: []const u8, initial_address: []const u8, initial_timestamp: i64, os_user: []const u8) !void {
        var hostname_buf: [std.posix.HOST_NAME_MAX]u8 = undefined;
        const hostname = try std.posix.gethostname(&hostname_buf);

        try writer.writeInt(u8, 1, .little); // query_kind marker = 1 (init)
        try writeString(writer, initial_user);
        try writeString(writer, query_id);
        try writeString(writer, initial_address);
        try writer.writeInt(i64, initial_timestamp, .little);

        try writer.writeInt(u8, 1, .little); // query_interface = 1 (TCP)

        try writeString(writer, os_user);
        try writeString(writer, hostname);
        try writeString(writer, CLIENT_NAME);
        try writeVarInt(writer, CLIENT_VERSION_MAJOR);
        try writeVarInt(writer, CLIENT_VERSION_MINOR);
        try writeVarInt(writer, PROTOCOL_VERSION);
        try writeString(writer, ""); // quota_key
        try writeVarInt(writer, 0); // distributed_depth
        try writeVarInt(writer, CLIENT_VERSION_PATCH);
        try writer.writeInt(u8, 0, .little); // open_telemetry = off
    }
};

pub fn writeVarInt(writer: *std.Io.Writer, value: u64) !void {
    var v = value;
    while (true) {
        var byte: u8 = @truncate(v);
        byte &= 0x7F; // Keep 7 bits
        v >>= 7;
        
        if (v != 0) {
            byte |= 0x80; // Set continuation bit
            try writer.writeByte(byte);
        } else {
            try writer.writeByte(byte);
            break;
        }
    }
}

pub fn readVarInt(reader: *std.Io.Reader) !u64 {
    var result: u64 = 0;
    var shift: u6 = 0;
    while (true) {
        const byte = try reader.takeByte();
        const val = @as(u64, byte & 0x7F);
        result |= (val << shift);
        
        if ((byte & 0x80) == 0) break;
        shift += 7;
    }
    return result;
}

pub fn writeString(writer: *std.Io.Writer, str: []const u8) !void {
    try writeVarInt(writer, str.len);
    try writer.writeAll(str);
}

pub fn readString(reader: *std.Io.Reader) ![]u8 {
    const len = try readVarInt(reader);
    return try reader.take(len);
}

test "writeVarInt ensure correct encoding" {
    const allocator = std.testing.allocator;

    const write_buffer = try allocator.alloc(u8, 512);
    defer allocator.free(write_buffer);

    var writer = std.Io.Writer.fixed(write_buffer);
    const w = &writer;

    // zero
    const zero: u8 = 0;
    const zero_hex = "00";

    const zero_bytes = try allocator.alloc(u8, zero_hex.len / 2);
    defer allocator.free(zero_bytes);
    _ = try std.fmt.hexToBytes(zero_bytes, zero_hex);

    try writeVarInt(w, zero);
    const zero_written_data = w.buffer[0..w.end];
    try std.testing.expectEqualStrings(zero_bytes, zero_written_data);
    w.end = 0;

    // one
    const one: u8 = 1;
    const one_hex = "01";

    const one_bytes = try allocator.alloc(u8, one_hex.len / 2);
    defer allocator.free(one_bytes);
    _ = try std.fmt.hexToBytes(one_bytes, one_hex);

    try writeVarInt(w, one);
    const one_written_data = w.buffer[0..w.end];
    try std.testing.expectEqualStrings(one_bytes, one_written_data);
    w.end = 0;
    
    // u8
    const unsigned8: u8 = 7;
    const unsigned8_hex = "07";

    const unsigned8_bytes = try allocator.alloc(u8, unsigned8_hex.len / 2);
    defer allocator.free(unsigned8_bytes);
    _ = try std.fmt.hexToBytes(unsigned8_bytes, unsigned8_hex);

    try writeVarInt(w, unsigned8);
    const unsigned8_written_data = w.buffer[0..w.end];
    try std.testing.expectEqualStrings(unsigned8_bytes, unsigned8_written_data);
    w.end = 0;

    // u16
    const unsigned16: u16 = 24090;
    const unsigned16_hex = "9abc01";

    const unsigned16_bytes = try allocator.alloc(u8, unsigned16_hex.len / 2);
    defer allocator.free(unsigned16_bytes);
    _ = try std.fmt.hexToBytes(unsigned16_bytes, unsigned16_hex);

    try writeVarInt(w, unsigned16);
    const unsigned16_written_data = w.buffer[0..w.end];
    try std.testing.expectEqualStrings(unsigned16_bytes, unsigned16_written_data);
    w.end = 0;

    // u32
    const unsigned32: u32 = 1475382682;
    const unsigned32_hex = "9a9bc2bf05";

    const unsigned32_bytes = try allocator.alloc(u8, unsigned32_hex.len / 2);
    defer allocator.free(unsigned32_bytes);
    _ = try std.fmt.hexToBytes(unsigned32_bytes, unsigned32_hex);

    try writeVarInt(w, unsigned32);
    const unsigned32_written_data = w.buffer[0..w.end];
    try std.testing.expectEqualStrings(unsigned32_bytes, unsigned32_written_data);
    w.end = 0;

    // u64
    const unsigned64: u64 = 15596884590815070553;
    const unsigned64_hex = "d992afb2e4fcd0b9d801";

    const unsigned64_bytes = try allocator.alloc(u8, unsigned64_hex.len / 2);
    defer allocator.free(unsigned64_bytes);
    _ = try std.fmt.hexToBytes(unsigned64_bytes, unsigned64_hex);

    try writeVarInt(w, unsigned64);
    const unsigned64_written_data = w.buffer[0..w.end];
    try std.testing.expectEqualStrings(unsigned64_bytes, unsigned64_written_data);
    w.end = 0;
}

test "readVarInt ensure correct decoding" {
    const allocator = std.testing.allocator;

    const read_buffer = try allocator.alloc(u8, 512);
    defer allocator.free(read_buffer);

    var reader = std.Io.Reader.fixed(read_buffer);
    const r = &reader;

    // zero
    const zero: u8 = 0;
    const zero_hex = "00";

    _ = try std.fmt.hexToBytes(read_buffer, zero_hex);

    const read_zero = try readVarInt(r);
    try std.testing.expectEqual(zero, read_zero);
    r.seek = 0;

    // one
    const one: u8 = 1;
    const one_hex = "01";

    _ = try std.fmt.hexToBytes(read_buffer, one_hex);

    const read_one = try readVarInt(r);
    try std.testing.expectEqual(one, read_one);
    r.seek = 0;

    // u8
    const unsigned8: u8 = 7;
    const unsigned8_hex = "07";

    _ = try std.fmt.hexToBytes(read_buffer, unsigned8_hex);

    const read_unsigned8 = try readVarInt(r);
    try std.testing.expectEqual(unsigned8, read_unsigned8);
    r.seek = 0;

    // u16
    const unsigned16: u16 = 24090;
    const unsigned16_hex = "9abc01";

    _ = try std.fmt.hexToBytes(read_buffer, unsigned16_hex);

    const read_unsigned16 = try readVarInt(r);
    try std.testing.expectEqual(unsigned16, read_unsigned16);
    r.seek = 0;

    // u32
    const unsigned32: u32 = 1475382682;
    const unsigned32_hex = "9a9bc2bf05";

    _ = try std.fmt.hexToBytes(read_buffer, unsigned32_hex);

    const read_unsigned32 = try readVarInt(r);
    try std.testing.expectEqual(unsigned32, read_unsigned32);
    r.seek = 0;

    // u64
    const unsigned64: u64 = 15596884590815070553;
    const unsigned64_hex = "d992afb2e4fcd0b9d801";

    _ = try std.fmt.hexToBytes(read_buffer, unsigned64_hex);

    const read_unsigned64 = try readVarInt(r);
    try std.testing.expectEqual(unsigned64, read_unsigned64);
    r.seek = 0;
}

test "writeString ensure correct decoding" {
    const allocator = std.testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = std.Io.Writer.fixed(buffer);
    const w = &writer;

    var reader = std.Io.Reader.fixed(buffer);
    const r = &reader;

    // empty
    const empty_str = "";

    try writeString(w, empty_str);

    const empty_len = try readVarInt(r);
    const empty_written_data = try r.take(empty_len);
    try std.testing.expectEqualStrings(empty_str, empty_written_data);
    w.end = 0;
    r.seek = 0;

    // normal
    const normal_str = "this is a test";

    try writeString(w, normal_str);

    const normal_len = try readVarInt(r);
    const normal_written_data = try r.take(normal_len);
    try std.testing.expectEqualStrings(normal_str, normal_written_data);
    w.end = 0;
    r.seek = 0;
}

test "readString ensure correct decoding" {
    const allocator = std.testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = std.Io.Writer.fixed(buffer);
    const w = &writer;

    var reader = std.Io.Reader.fixed(buffer);
    const r = &reader;

    // empty
    const empty_str = "";

    try writeVarInt(w, empty_str.len);
    try writer.writeAll(empty_str);

    const read_empty_str = try readString(r);
    try std.testing.expectEqualStrings(empty_str, read_empty_str);
    w.end = 0;
    r.seek = 0;

    // normal
    const normal_str = "this is a test";

    try writeVarInt(w, normal_str.len);
    try writer.writeAll(normal_str);

    const read_normal_str = try readString(r);
    try std.testing.expectEqualStrings(normal_str, read_normal_str);
    w.end = 0;
    r.seek = 0;
}
