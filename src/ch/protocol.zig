const std = @import("std");
const builtin = @import("builtin");

const Io = std.Io;

const compression = @import("compression.zig");

const packet = @import("packet.zig");
const root = @import("root.zig");

const ChConfig = root.ChConfig;
const ChError = root.ChError;

pub const CLIENT_VERSION_MAJOR: u64 = 1;
pub const CLIENT_VERSION_MINOR: u64 = 0;
pub const CLIENT_VERSION_PATCH: u64 = 0;

pub const PROTOCOL_VERSION: u64 = 54449;

pub const ClientHello = struct {
    pub fn write(writer: *Io.Writer, config: ChConfig) !void {
        try packet.writeClientPacketHeader(writer, .Hello);

        try writeString(writer, config.application_name);
        try writeVarInt(writer, CLIENT_VERSION_MAJOR);
        try writeVarInt(writer, CLIENT_VERSION_MINOR);
        try writeVarInt(writer, PROTOCOL_VERSION);

        try writer.writeInt(u8, @as(u8, @truncate(config.database.len)), .little);
        try writer.writeAll(config.database);

        try writer.writeInt(u8, @as(u8, @truncate(config.username.len)), .little);
        try writer.writeAll(config.username);

        try writer.writeInt(u8, @as(u8, @truncate(config.password.len)), .little);
        try writer.writeAll(config.password);

        try writer.flush();
    }
};

pub const ClientInfo = struct {
    pub fn write(writer: *Io.Writer, query_id: []const u8, initial_user: []const u8, initial_address: []const u8, initial_timestamp: i64, os_user: []const u8, application_name: []const u8) !void {
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
        try writeString(writer, application_name);
        try writeVarInt(writer, CLIENT_VERSION_MAJOR);
        try writeVarInt(writer, CLIENT_VERSION_MINOR);
        try writeVarInt(writer, PROTOCOL_VERSION);
        try writeString(writer, ""); // quota_key
        try writeVarInt(writer, 0); // distributed_depth
        try writeVarInt(writer, CLIENT_VERSION_PATCH);
        try writer.writeInt(u8, 0, .little); // open_telemetry = off
    }
};

pub const ServerInfo = struct {
    allocator: std.mem.Allocator,

    name: []const u8,
    major_version: u64,
    minor_version: u64,
    revision: u64,
    timezone: []const u8,
    display_name: []const u8,
    version_patch: u64,

    pub fn read(allocator: std.mem.Allocator, reader: *Io.Reader) !@This() {
        const server_packet = try readVarInt(reader);

        if (server_packet != @intFromEnum(packet.ServerPacket.Hello)) {
            return ChError.ProtocolError;
        }

        const name = try allocator.dupe(u8, try readString(reader));

        const major_version = try readVarInt(reader);
        const minor_version = try readVarInt(reader);
        const revision = try readVarInt(reader);

        const tz = try allocator.dupe(u8, try readString(reader));

        const display = try allocator.dupe(u8, try readString(reader));

        const version_patch = try readVarInt(reader);

        return .{
            .allocator = allocator,
            .name = name,
            .major_version = major_version,
            .minor_version = minor_version,
            .revision = revision,
            .timezone = tz,
            .display_name = display,
            .version_patch = version_patch,
        };
    }

    pub fn deinit(self: *ServerInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.timezone);
        allocator.free(self.display_name);
    }
};

pub fn writeVarInt(writer: *Io.Writer, value: u64) !void {
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

pub fn readVarInt(reader: *Io.Reader) !u64 {
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

pub fn writeString(writer: *Io.Writer, str: []const u8) !void {
    try writeVarInt(writer, str.len);
    try writer.writeAll(str);
}

pub fn readString(reader: *Io.Reader) ![]u8 {
    const len = try readVarInt(reader);
    return try reader.take(len);
}

const t = @import("t.zig");
test "writeVarInt: ensure correct encoding" {
    const allocator = std.testing.allocator;

    const write_buffer = try allocator.alloc(u8, 512);
    defer allocator.free(write_buffer);

    var writer = Io.Writer.fixed(write_buffer);
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

test "readVarInt: ensure correct decoding" {
    const allocator = std.testing.allocator;

    const read_buffer = try allocator.alloc(u8, 512);
    defer allocator.free(read_buffer);

    var reader = Io.Reader.fixed(read_buffer);
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

test "writeString: ensure correct decoding" {
    const allocator = std.testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);
    const w = &writer;

    var reader = Io.Reader.fixed(buffer);
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

test "readString: ensure correct decoding" {
    const allocator = std.testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);
    const w = &writer;

    var reader = Io.Reader.fixed(buffer);
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


test "sendHello: ensure correct encoding" {
    const allocator = std.testing.allocator;

    const buffer = try allocator.alloc(u8, 1024);
    defer allocator.free(buffer);
    
    var writer = Io.Writer.fixed(buffer);
    var reader = Io.Reader.fixed(buffer);

    try ClientHello.write(&writer, t.default_config);

    const packet_type = try readVarInt(&reader);
    try std.testing.expectEqual(@intFromEnum(packet.ClientPacket.Hello), packet_type);

    const name = try readString(&reader);
    try std.testing.expectEqualStrings(t.default_config.application_name, name);

    const major_version = try readVarInt(&reader);
    try std.testing.expectEqual(CLIENT_VERSION_MAJOR, major_version);

    const minor_version = try readVarInt(&reader);
    try std.testing.expectEqual(CLIENT_VERSION_MINOR, minor_version);

    const protocol = try readVarInt(&reader);
    try std.testing.expectEqual(PROTOCOL_VERSION, protocol);

    const config_db_len = try reader.takeInt(u8, .little);
    try std.testing.expectEqual(t.default_config.database.len, config_db_len);

    const config_db = try reader.take(config_db_len);
    try std.testing.expectEqualStrings(t.default_config.database, config_db);

    const config_user_len = try reader.takeInt(u8, .little);
    try std.testing.expectEqual(t.default_config.username.len, config_user_len);

    const config_user = try reader.take(config_user_len);
    try std.testing.expectEqualStrings(t.default_config.username, config_user);

    const config_pass_len = try reader.takeInt(u8, .little);
    try std.testing.expectEqual(t.default_config.password.len, config_pass_len);

    const config_pass = try reader.take(config_pass_len);
    try std.testing.expectEqualStrings(t.default_config.password, config_pass);
}

test "readServerHello: ensure correct decoding" {
    const allocator = std.testing.allocator;

    const buffer = try allocator.alloc(u8, 1024);
    defer allocator.free(buffer);
    
    var writer = Io.Writer.fixed(buffer);
    var reader = Io.Reader.fixed(buffer);

    const server_name = "Test ch server";
    const major_version: u64 = 1;
    const minor_version: u64 = 6;
    const revision: u64 = 6;

    const tz = "utc";
    const display = "Main";
    const version_patch = 453;

    try writeVarInt(&writer, @intFromEnum(packet.ServerPacket.Hello));
    try writeString(&writer, server_name);
    try writeVarInt(&writer, major_version);
    try writeVarInt(&writer, minor_version);
    try writeVarInt(&writer, revision);
    try writeString(&writer, tz);
    try writeString(&writer, display);
    try writeVarInt(&writer, version_patch);

    const server_info = try ServerInfo.read(allocator, &reader);

    try std.testing.expectEqual(writer.end, reader.seek);
    try std.testing.expectEqualStrings(server_name, server_info.name);
    try std.testing.expectEqual(major_version, server_info.major_version);
    try std.testing.expectEqual(minor_version, server_info.minor_version);
    try std.testing.expectEqual(revision, server_info.revision);
    try std.testing.expectEqualStrings(tz, server_info.timezone);
    try std.testing.expectEqualStrings(display, server_info.display_name);
    try std.testing.expectEqual(version_patch, server_info.version_patch);
}
