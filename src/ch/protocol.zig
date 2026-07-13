const std = @import("std");
const compression = @import("compression.zig");

pub const ClientHello = struct {
    const CLIENT_NAME = "ClickHouse Zig Client";
    const CLIENT_VERSION_MAJOR: u64 = 1;
    const CLIENT_VERSION_MINOR: u64 = 0;
    const PROTOCOL_VERSION: u64 = 54429;

    pub fn write(writer: *std.Io.Writer) !void {
        try writeString(writer, "ClickHouseClient");
        try writeVarInt(writer, CLIENT_VERSION_MAJOR);
        try writeVarInt(writer, CLIENT_VERSION_MINOR);
        try writeVarInt(writer, PROTOCOL_VERSION);
    }
};

pub const ClientInfo = struct {
    pub fn write(writer: *std.Io.Writer, query_id: []const u8, client_name: []const u8, initial_user: []const u8, initial_address: []const u8) !void {
        _ = client_name;
        _ = initial_user;
        _ = initial_address;
        
        // Query ID
        try writeString(writer, query_id);

        // Client info block
        try writeVarInt(writer, 0); // client_info marker = 0 (empty)
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
