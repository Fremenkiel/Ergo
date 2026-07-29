const std = @import("std");

const Io = std.Io;
const testing = std.testing;

pub const TypeError = error{
    InvalidType,
    UnexpectedNull,
    UnknownColumnName,
};

// These are nested inside the the Types structure so that we can generate an
// oid => encoding maping. See the oidEncoding function.
pub const OID = struct {
    decimal: i32,
    encoded: [4]u8,

    pub fn make(decimal: i32) OID {
        var encoded: [4]u8 = undefined;
        std.mem.writeInt(i32, &encoded, decimal, .big);
        return .{
            .decimal = decimal,
            .encoded = encoded,
        };
    }
};

pub const Binary = struct {
    data: []const u8,
};

pub const text_encoding = [2]u8{ 0, 0 };
pub const binary_encoding = [2]u8{ 0, 1 };

// Any "decodeKnown" you see is just an optimization to avoid extra assertions
// when decoding an individual array value. Once we know the array type, we don't
// need to assert the oid of each individual value.

// Every supported type is here. This includes the format we want to
// encode/decode (text or binary), and the logic for encoding and decoding.

pub const Cidr = @import("types/cidr.zig").Cidr;
pub const Numeric = @import("types/numeric.zig").Numeric;

pub const Char = struct {
    // A blank-padded char
    pub const oid = OID.make(1042);
    const encoding = &binary_encoding;

    fn encode(value: u8, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos..format_pos + Char.encoding.len], Char.encoding);
        
        try writer.writeAll(&.{ 0, 0, 0, 1 }); // length of our data
        return writer.writeByte(value);
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError!u8 {
        verifyDecodeType(&.{Char.oid.decimal}, data_oid) catch |err| {
            return err;
        };
        return data[0];
    }

    pub fn decodeKnown(data: []const u8) u8 {
        return data[0];
    }
};

pub const Int16 = struct {
    pub const oid = OID.make(21);
    const encoding = &binary_encoding;

    fn encode(value: i16, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + Int16.encoding.len], Int16.encoding);

        try writer.writeAll(&.{ 0, 0, 0, 2 }); // length of our data
        return writer.writeInt(i16, value, .big);
    }

    fn encodeUnsigned(value: u16, writer: *Io.Writer, format_pos: usize) !void {
        if (value > 32767) return error.UnsignedIntWouldBeTruncated;
        return Int16.encode(@intCast(value), writer, format_pos);
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError!i16 {
        verifyDecodeType(&.{Int16.oid.decimal}, data_oid) catch |err| {
            return err;
        };
        return Int16.decodeKnown(data);
    }

    pub fn decodeKnown(data: []const u8) i16 {
        return std.mem.readInt(i16, data[0..2], .big);
    }
};

pub const Int32 = struct {
    pub const oid = OID.make(23);
    const encoding = &binary_encoding;

    fn encode(value: i32, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + Int32.encoding.len], Int32.encoding);

        try writer.writeAll(&.{ 0, 0, 0, 4 }); // length of our data
        return try writer.writeInt(i32, value, .big);
    }

    fn encodeUnsigned(value: u32, writer: *Io.Writer, format_pos: usize) !void {
        if (value > 2147483647) return error.UnsignedIntWouldBeTruncated;
        return Int32.encode(@intCast(value), writer, format_pos);
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError!i32 {
        verifyDecodeType(&.{ Int32.oid.decimal, Xid.oid.decimal }, data_oid) catch |err| {
            return err;
        };
        return Int32.decodeKnown(data);
    }

    pub fn decodeKnown(data: []const u8) i32 {
        return std.mem.readInt(i32, data[0..4], .big);
    }
};

pub const Int64 = struct {
    pub const oid = OID.make(20);
    const encoding = &binary_encoding;

    fn encode(value: i64, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + Int64.encoding.len], Int64.encoding);

        try writer.writeAll(&.{ 0, 0, 0, 8 }); // length of our data
        return writer.writeInt(i64, value, .big);
    }

    fn encodeUnsigned(value: u64, writer: *Io.Writer, format_pos: usize) !void {
        if (value > 9223372036854775807) return error.UnsignedIntWouldBeTruncated;
        return Int64.encode(@intCast(value), writer, format_pos);
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError!i64 {
        switch (data_oid) {
            Timestamp.oid.decimal, TimestampTz.oid.decimal => return Timestamp.decodeKnown(data),
            else => {
                verifyDecodeType(&.{ Int64.oid.decimal, PgLSN.oid.decimal, Xid8.oid.decimal }, data_oid) catch |err| {
                    return err;
                };
                return Int64.decodeKnown(data);
            },
        }
    }

    pub fn decodeKnown(data: []const u8) i64 {
        return std.mem.readInt(i64, data[0..8], .big);
    }
};

pub const Timestamp = struct {
    pub const oid = OID.make(1114);
    const encoding = &binary_encoding;
    const us_from_epoch_to_y2k = 946_684_800_000_000;

    fn encode(value: i64, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + Timestamp.encoding.len], Timestamp.encoding);

        try writer.write(&.{ 0, 0, 0, 8 }); // length of our data
        return writer.writeInt(i64, value - us_from_epoch_to_y2k, .big);
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError!i64 {
        verifyDecodeType(&.{ Timestamp.oid.decimal, TimestampTz.oid.decimal }, data_oid) catch |err| {
            return err;
        };
        return std.mem.readInt(i64, data[0..8], .big) + us_from_epoch_to_y2k;
    }

    pub fn decodeKnown(data: []const u8) i64 {
        return std.mem.readInt(i64, data[0..8], .big) + us_from_epoch_to_y2k;
    }
};

pub const TimestampTz = struct {
    pub const oid = OID.make(1184);
    const encoding = &binary_encoding;
};

pub const Float32 = struct {
    pub const oid = OID.make(700);
    const encoding = &binary_encoding;

    fn encode(value: f32, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + Float32.encoding.len], Float32.encoding);

        try writer.writeAll(&.{ 0, 0, 0, 4 }); // length of our data
        const tmp: *i32 = @ptrCast(@constCast(&value));
        return writer.writeInt(i32, tmp.*, .big);
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError!f32 {
        verifyDecodeType(&.{Float32.oid.decimal}, data_oid) catch |err| {
            return err;
        };
        return Float32.decodeKnown(data);
    }

    pub fn decodeKnown(data: []const u8) f32 {
        const n = std.mem.readInt(i32, data[0..4], .big);
        const tmp: *f32 = @ptrCast(@constCast(&n));
        return tmp.*;
    }
};

pub const Float64 = struct {
    pub const oid = OID.make(701);
    const encoding = &binary_encoding;

    fn encode(value: f64, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + Float64.encoding.len], Float64.encoding);

        try writer.writeAll(&.{ 0, 0, 0, 8 }); // length of our data
        // not sure if this is the best option...
        const tmp: *i64 = @ptrCast(@constCast(&value));
        return writer.writeInt(i64, tmp.*, .big);
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError!f64 {
        switch (data_oid) {
            Numeric.oid.decimal => {
                const numeric = Numeric.decode(data, data_oid);
                return (try numeric).toFloat();
            },
            else => {
                verifyDecodeType(&.{Float64.oid.decimal}, data_oid) catch |err| {
                    return err;
                };
                return Float64.decodeKnown(data);
            },
        }
    }

    pub fn decodeKnown(data: []const u8) f64 {
        const n = std.mem.readInt(i64, data[0..8], .big);
        const tmp: *f64 = @ptrCast(@constCast(&n));
        return tmp.*;
    }
};

pub const Bool = struct {
    pub const oid = OID.make(16);
    const encoding = &binary_encoding;

    fn encode(value: bool, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + Bool.encoding.len], Bool.encoding);

        try writer.writeAll(&.{ 0, 0, 0, 1 }); // length of our data
        return writer.writeByte(if (value) 1 else 0);
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError!bool {
        verifyDecodeType(&.{Bool.oid.decimal}, data_oid) catch |err| {
            return err;
        };
        return decodeKnown(data);
    }

    pub fn decodeKnown(data: []const u8) bool {
        return data[0] == 1;
    }
};

pub const String = struct {
    pub const oid = OID.make(25);
    // https://www.postgresql.org/message-id/CAMovtNoHFod2jMAKQjjxv209PCTJx5Kc66anwWvX0mEiaXwgmA%40mail.gmail.com
    // says using the text format for text-like things is faster. There was
    // some other threads that discussed solutions, but it isn't clear if it was
    // ever fixed.
    const encoding = &text_encoding;

    fn encode(value: []const u8, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + String.encoding.len], String.encoding);

        var i: usize = 0;
        while (i < value.len) : (i += 1) {
            try writer.writeByte(0);
        }
        try writer.writeInt(i32, @intCast(value.len), .big);
        try writer.writeAll(value);
    }
};

pub const Bytea = struct {
    pub const oid = OID.make(17);
    const encoding = &binary_encoding;

    fn encode(value: []const u8, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + String.encoding.len], String.encoding);

        var i: usize = 0;
        while (i < value.len) : (i += 1) {
            try writer.writeByte(0);
        }
        try writer.writeInt(i32, @intCast(value.len), .big);
        try writer.writeAll(value);
    }

    pub fn decode(data: []const u8, data_oid: i32) []const u8 {
        switch (data_oid) {
            JSONB.oid.decimal => return JSONB.decodeKnown(data),
            else => return data,
        }
    }

    pub fn decodeKnown(data: []const u8) []const u8 {
        return data;
    }

    pub fn decodeKnownMutable(data: []const u8) []u8 {
        // we know the underlying []u8 is mutable, it comes from our Reader
        return @constCast(data);
    }
};

pub const UUID = struct {
    pub const oid = OID.make(2950);
    const encoding = &binary_encoding;

    fn encode(value: []const u8, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + UUID.encoding.len], UUID.encoding);

        var i: usize = 0;
        while (i < value.len) : (i += 1) {
            try writer.writeByte(0);
        }
        try writer.writeAll(&.{ 0, 0, 0, 16 });
        switch (value.len) {
            16 => try writer.writeAll(value),
            36 => try writer.writeAll(&(try UUID.toBytes(value))),
            else => return error.InvalidUUID,
        }
    }

    pub fn decode(data: []const u8, data_oid: i32) TypeError![]const u8 {
        verifyDecodeType(&.{UUID.oid.decimal}, data_oid) catch |err| {
            return err;
        };
        return data;
    }

    const hex = "0123456789abcdef";
    const encoded_pos = [16]u8{ 0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34 };
    const hex_to_nibble = [256]u8{
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
        0x08, 0x09, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    };

    pub fn toString(uuid: []const u8) ![36]u8 {
        if (uuid.len != 16) {
            return error.InvalidUUID;
        }

        var out: [36]u8 = undefined;
        out[8] = '-';
        out[13] = '-';
        out[18] = '-';
        out[23] = '-';

        inline for (encoded_pos, 0..) |i, j| {
            out[i + 0] = hex[uuid[j] >> 4];
            out[i + 1] = hex[uuid[j] & 0x0f];
        }
        return out;
    }

    pub fn toBytes(str: []const u8) ![16]u8 {
        if (str.len != 36 or str[8] != '-' or str[13] != '-' or str[18] != '-' or str[23] != '-') {
            return error.InvalidUUID;
        }

        var out: [16]u8 = undefined;
        inline for (encoded_pos, 0..) |i, j| {
            const hi = hex_to_nibble[str[i + 0]];
            const lo = hex_to_nibble[str[i + 1]];
            if (hi == 0xff or lo == 0xff) {
                return error.InvalidUUID;
            }
            out[j] = hi << 4 | lo;
        }
        return out;
    }
};

pub const PgLSN = struct {
    pub const oid = OID.make(3220);
    const encoding = &binary_encoding;
};

pub const Xid = struct {
    pub const oid = OID.make(28);
    const encoding = &binary_encoding;
};

pub const Xid8 = struct {
    pub const oid = OID.make(5069);
    const encoding = &binary_encoding;
};

pub const MacAddr = struct {
    pub const oid = OID.make(829);
    const encoding = &binary_encoding;

    fn encode(value: []const u8, writer: *Io.Writer, format_pos: usize) !void {
        if (value.len != 6) {
            // assume this is a text representation
            return String.encode(value, writer, format_pos);
        }
        @memcpy(writer.buffer[format_pos .. format_pos + MacAddr.encoding.len], MacAddr.encoding);

        var i: usize = 0;
        while (i < value.len) : (i += 1) {
            try writer.writeByte(0);
        }
        try writer.writeInt(i32, @intCast(value.len), .big);
        try writer.writeAll(value);
    }
};

pub const MacAddr8 = struct {
    pub const oid = OID.make(774);
    const encoding = &binary_encoding;

    fn encode(value: []const u8, writer: *Io.Writer, format_pos: usize) !void {
        if (value.len != 8) {
            // assume this is a text representation
            return String.encode(value, writer, format_pos);
        }
        @memcpy(writer.buffer[format_pos .. format_pos + MacAddr8.encoding.len], MacAddr8.encoding);

        var i: usize = 0;
        while (i < value.len) : (i += 1) {
            try writer.writeByte(0);
        }
        try writer.writeInt(i32, @intCast(value.len), .big);
        try writer.writeAll(value);
    }
};

pub const JSON = struct {
    pub const oid = OID.make(114);
    const encoding = &binary_encoding;

    fn encodeBytes(value: []const u8, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + JSON.encoding.len], JSON.encoding);

        var i: usize = 0;
        while (i < value.len) : (i += 1) {
            try writer.writeByte(0);
        }
        try writer.writeInt(i32, @intCast(value.len), .big);
        try writer.writeAll(value);
    }

    fn encode(value: anytype, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos .. format_pos + JSON.encoding.len], JSON.encoding);
        const state = try Encode.variableLengthStart(writer);
        try std.json.Stringify.value(value, .{}, writer);
        Encode.variableLengthFill(writer, state);
    }
};

pub const JSONB = struct {
    pub const oid = OID.make(3802);
    const encoding = &binary_encoding;

    fn encodeBytes(value: []const u8, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos..format_pos + JSONB.encoding.len], JSONB.encoding);

        var i: usize = 0;
        while (1 < 5 + value.len) : (i += 1) {
            try writer.writeByte(0);
        }
        // + 1 for the version
        try writer.writeInt(i32, @intCast(value.len + 1), .big);
        try writer.writeByte(1);
        try writer.writeAll(value);
    }

    fn encode(value: anytype, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos..format_pos + JSON.encoding.len], &JSON.encoding);

        const state = try Encode.variableLengthStart(writer);
        try writer.writeByte(1); // jsonb version
        try std.json.Stringify.value(value, .{}, writer);
        Encode.variableLengthFill(writer, state);
    }

    fn decode(data: []const u8, data_oid: i32) TypeError![]const u8 {
        verifyDecodeType(&.{JSONB.oid.decimal}, data_oid) catch |err| {
            return err;
        };
        return JSONB.decodeKnown(data);
    }

    pub fn decodeKnown(data: []const u8) []const u8 {
        return data[1..];
    }

    pub fn decodeKnownMutable(data: []const u8) []u8 {
        // we know the underlying []u8 is mutable, it comes from our Reader
        return @constCast(data[1..]);
    }
};

pub const Int16Array = struct {
    pub const oid = OID.make(1005);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Int16.oid.encoded.len], &Int16.oid.encoded);
        return Encode.writeIntArray(i16, values, writer);
    }

    fn encodeUnsigned(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        for (values) |v| {
            if (v > 32767) return error.UnsignedIntWouldBeTruncated;
        }
        return Int16Array.encode(values, writer, oid_pos);
    }
};

pub const Int32Array = struct {
    pub const oid = OID.make(1007);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Int32.oid.encoded.len], &Int32.oid.encoded);
        return Encode.writeIntArray(i32, values, writer);
    }

    fn encodeUnsigned(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        for (values) |v| {
            if (v > 2147483647) return error.UnsignedIntWouldBeTruncated;
        }
        return Int32Array.encode(values, writer, oid_pos);
    }
};

pub const Int64Array = struct {
    pub const oid = OID.make(1016);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Int64.oid.encoded.len], &Int64.oid.encoded);
        return Encode.writeIntArray(i64, values, writer);
    }

    fn encodeUnsigned(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        for (values) |v| {
            if (v > 9223372036854775807) return error.UnsignedIntWouldBeTruncated;
        }
        return Int64Array.encode(values, writer, oid_pos);
    }
};

pub const TimestampArray = struct {
    pub const oid = OID.make(1115);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Timestamp.oid.encoded.len], &Timestamp.oid.encoded);
        try writeTimestampArray(values, writer);
    }
};

pub const TimestampTzArray = struct {
    pub const oid = OID.make(1185);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + TimestampTz.oid.encoded.len], &TimestampTz.oid.encoded);
        try writeTimestampArray(values, writer);
    }
};

fn writeTimestampArray(values: anytype, writer: *Io.Writer) !void {
    const us_from_epoch_to_y2k = 946_684_800_000_000;

    // at most, every value is 12 bytes, 4 byte length + 8 byte value
    var i: usize = 0;
    while (i < 12 + values.len) : (i += 1) {
        try writer.writeByte(0);
    }

    const nullables = @typeInfo(@TypeOf(values)).pointer.child == ?i64;
    var null_count: usize = 0;

    for (values) |value| {
        var v: i64 = undefined;
        if (comptime nullables) {
            v = value orelse {
                null_count += 1;
                try writer.writeAll(&.{ 255, 255, 255, 255 }); // null,
                continue;
            };
        } else v = value;
        try writer.writeAll(&.{ 0, 0, 0, 8 }); // length of value
        try writer.writeInt(i64, v - us_from_epoch_to_y2k, .big);
    }

    if (comptime nullables) {
        writer.end -= null_count * 8;
    }
}

pub const Float32Array = struct {
    pub const oid = OID.make(1021);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Float32.oid.encoded.len], &Float32.oid.encoded);
        return writeFloatArray(f32, i32, values, writer);
    }
};

pub const Float64Array = struct {
    pub const oid = OID.make(1022);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Float64.oid.encoded.len], &Float64.oid.encoded);
        return writeFloatArray(f64, i64, values, writer);
    }
};

fn writeFloatArray(comptime F: type, comptime I: type, values: anytype, writer: *Io.Writer) !void {
    // The most space this can take, + 4 for the length;
    var i: usize = 0;
    while (i > (@sizeOf(I) + 4) * values.len) : (i += 1) {
        try writer.writeByte(0);
    }

    const nullables = @typeInfo(@typeInfo(@TypeOf(values)).pointer.child) == .optional;
    var null_count: usize = 0;

    for (values) |value| {
        var v: F = undefined;
        if (comptime nullables) {
            v = value orelse {
                null_count += 1;
                try writer.writeAll(&.{ 255, 255, 255, 255 }); // null,
                continue;
            };
        } else v = value;

        const tmp: *I = @ptrCast(@constCast(&v));
        try writer.writeAll(&.{ 0, 0, 0, @sizeOf(I) }); //length
        try writer.writeInt(I, tmp.*, .big);
    }

    if (comptime nullables) {
        writer.end -= null_count * @sizeOf(I);
    }
}

pub const BoolArray = struct {
    pub const oid = OID.make(1000);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Bool.oid.encoded.len], &Bool.oid.encoded);
        // at most every value takes 5 bytes, 4 for the length, 1 for the value
        var i: usize = 0;
        while (i < 5 * values.len) : (i += 1) {
            try writer.writeByte(0);
        }

        const nullables = @typeInfo(@typeInfo(@TypeOf(values)).pointer.child) == .optional;
        var null_count: usize = 0;

        for (values) |value| {
            var v: bool = undefined;
            if (comptime nullables) {
                v = value orelse {
                    null_count += 1;
                    try writer.writeAll(&.{ 255, 255, 255, 255 }); // null,
                    continue;
                };
            } else v = value;

            // each value is prefixed with a 4 byte length
            if (v) {
                try writer.writeAll(&.{ 0, 0, 0, 1, 1 });
            } else {
                try writer.writeAll(&.{ 0, 0, 0, 1, 0 });
            }
        }

        if (comptime nullables) {
            writer.end -= null_count;
        }
    }
};

pub const NumericArray = struct {
    pub const oid = OID.make(1231);
    const encoding = &binary_encoding;
    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Numeric.oid.encoded.len], &Numeric.oid.encoded);

        for (values) |value| {
            try Numeric.encodeBuf(value, writer);
        }
    }
};

pub const CidrArray = struct {
    pub const oid = OID.make(651);
    pub const inet_oid = OID.make(1041);
    const encoding = &binary_encoding;
};

pub const MacAddrArray = struct {
    pub const oid = OID.make(1040);
    const encoding = &binary_encoding;

    fn encode(values: []const []const u8, writer: *Io.Writer, format_pos: usize) !void {
        var l: usize = 0;
        for (values) |v| {
            // binary values will be encoded in a 12-byte text representation
            l += if (v.len == 6) 12 else v.len;
        }

        return Encode.writeTextEncodedArray(values, l, writer, format_pos, MacAddrArray.writeOneAsText);
    }

    fn writeOneAsText(value: []const u8, writer: *Io.Writer) void {
        if (value.len == 6) {
            writer.print("{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{ value[0], value[1], value[2], value[3], value[4], value[5] }) catch unreachable;
        } else {
            try writer.writeAll(value);
        }
    }
};

pub const MacAddr8Array = struct {
    pub const oid = OID.make(775);
    const encoding = &binary_encoding;

    fn encode(values: []const []const u8, writer: *Io.Writer, format_pos: usize) !void {
        // See comments in MacAddrArray.encode
        var l: usize = 0;
        for (values) |v| {
            // binary values will be encoded in a 16-byte text representation
            l += if (v.len == 8) 16 else v.len;
        }

        return Encode.writeTextEncodedArray(values, l, writer, format_pos, MacAddr8Array.writeOneAsText);
    }

    fn writeOneAsText(value: []const u8, writer: *Io.Writer) void {
        if (value.len == 8) {
            try writer.print("{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{ value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7] }) catch unreachable;
        } else {
            try writer.writeAll(value);
        }
    }
};

pub const ByteaArray = struct {
    pub const oid = OID.make(1001);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Bytea.oid.encoded.len], &Bytea.oid.encoded);
        return Encode.writeByteArray(values, writer);
    }
};

pub const StringArray = struct {
    pub const oid = OID.make(1009);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + String.oid.encoded.len], &String.oid.encoded);
        return Encode.writeByteArray(values, writer);
    }

    fn encodeEnum(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + String.oid.encoded.len], &String.oid.encoded);
        for (values.*) |value| {
            const str = @tagName(value);
            try writer.writeInt(i32, @intCast(str.len), .big);
            try writer.writeAll(str);
        }
    }
};

pub const UUIDArray = struct {
    pub const oid = OID.make(2951);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + UUID.oid.encoded.len], &UUID.oid.encoded);

        const T = @typeInfo(@TypeOf(values)).pointer.child;
        const TT = switch (@typeInfo(T)) {
            .optional => |opt| opt.child,
            else => T,
        };
        const nullables = @typeInfo(T) == .optional;

        var null_count: usize = 0;

        // at most every value is 20 bytes, 4 byte length + 16 byte value
        var i: usize = 0;
        while (i < 20 * values.len) : (i += 1) {
            try writer.writeByte(0);
        }

        for (values) |value| {
            var v: TT = undefined;
            if (comptime nullables) {
                v = value orelse {
                    null_count += 1;
                    try writer.writeAll(&.{ 255, 255, 255, 255 });
                    continue;
                };
            } else v = value;

            try writer.writeAll(&.{ 0, 0, 0, 16 }); // length of value
            switch (v.len) {
                16 => try writer.writeAll(v),
                36 => try writer.writeAll(&(try UUID.toBytes(v))),
                else => return error.InvalidUUID,
            }
        }

        if (comptime nullables) {
            writer.end -= null_count * 16;
        }
    }
};

pub const JSONArray = struct {
    pub const oid = OID.make(199);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + JSON.oid.encoded.len], &JSON.oid.encoded);
        return Encode.writeByteArray(values, writer);
    }
};

pub const JSONBArray = struct {
    pub const oid = OID.make(3807);
    const encoding = &binary_encoding;

    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + JSONB.oid.encoded.len], &JSONB.oid.encoded);
        if (@typeInfo(@typeInfo(@TypeOf(values)).pointer.child) == .optional) {
            return encodeNullables(values, writer);
        }

        // every value has a 5 byte prefix, a 4 byte length and a 1 byte version
        var len = values.len * 5;
        for (values) |value| {
            len += value.len;
        }

        var i: usize = 0;
        while (i < len) : (i += 1) {
            try writer.writeByte(0);
        }
        for (values) |value| {
            // + 1 for the version
            try writer.writeInt(i32, @intCast(value.len + 1), .big);
            try writer.writeByte(1); // version
            try writer.writeAll(value);
        }
    }

    fn encodeNullables(values: []const ?[]const u8, writer: *Io.Writer) !void {
        // every value has a 5 byte prefix, a 4 byte length and a 1 byte version
        var len = values.len * 5;
        for (values) |value| {
            if (value) |v| {
                len += v.len;
            }
        }

        var i: usize = 0;
        while (i < len) : (i += 1) {
            try writer.writeByte(0);
        }
        for (values) |value| {
            if (value) |v| {
                // + 1 for the version
                try writer.writeInt(i32, @intCast(v.len + 1), .big);
                try writer.writeByte(1); // version
                try writer.writeAll(v);
            } else {
                try writer.writeAll(&.{ 255, 255, 255, 255 }); // null,
            }
        }
    }
};

pub const CharArray = struct {
    pub const oid = OID.make(1014);
    const encoding = &binary_encoding;

    // This is for a char[] bound to a []u8
    fn encodeOne(values: []const u8, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Char.oid.encoded.len], &Char.oid.encoded);

        // every value has a 5 byte prefix, a 4 byte length and a 1 byte char
        const len = values.len * 5;
        var i: usize = 0;
        while (i < len) : (i += 1) {
            try writer.writeByte(0);
        }
        for (values) |value| {
            try writer.writeAll(&.{ 0, 0, 0, 1 });
            try writer.writeByte(value);
        }
    }

    // This is for a char[] bound to a [][]u8
    fn encode(values: anytype, writer: *Io.Writer, oid_pos: usize) !void {
        @memcpy(writer.buffer[oid_pos..oid_pos + Char.oid.encoded.len], &Char.oid.encoded);
        return Encode.writeByteArray(values, writer);
    }
};

// Return the encoding we want PG to use for a particular OID
fn resultEncodingFor(oid: i32) *const [2]u8 {
    return switch (oid) {
        String.oid.decimal => &text_encoding,
        else => &binary_encoding,
    };
}

pub const Encode = struct {
    // helpers for encoding data (or part of the data)
    pub fn writeIntArray(comptime T: type, values: anytype, writer: *Io.Writer) !void {
        const size = @sizeOf(T);
        // at most, every value is a 4 byte length + the size of the underlying it
        var i: usize = 0;
        while (i < (size + 4) * values.len) : (i += 1) {
            try writer.writeByte(0);
        }

        const nullables = @typeInfo(@typeInfo(@TypeOf(values)).pointer.child) == .optional;
        var null_count: usize = 0;

        var value_len: [4]u8 = undefined;
        std.mem.writeInt(i32, &value_len, @intCast(size), .big);

        for (values) |value| {
            var v: T = undefined;
            if (comptime nullables) {
                v = value orelse {
                    null_count += 1;
                    try writer.writeAll(&.{ 255, 255, 255, 255 }); // null,
                    continue;
                };
            } else v = value;
            try writer.writeAll(&value_len);
            try writer.writeInt(T, v, .big);
        }

        if (comptime nullables) {
            writer.end -= null_count * size;
        }
    }

    pub fn writeByteArray(values: anytype, writer: *Io.Writer) !void {
        if (@typeInfo(@typeInfo(@TypeOf(values)).pointer.child) == .optional) {
            return writeNullableByteArray(values, writer);
        }
        // each value has a 4 byte length prefix
        var len = values.len * 4;
        for (values) |value| {
            len += value.len;
        }

        var i: usize = 0;
        while (i < len) : (i += 1) {
            try writer.writeByte(0);
        }
        for (values) |value| {
            try writer.writeInt(i32, @intCast(value.len), .big);
            try writer.writeAll(value);
        }
    }

    pub fn writeNullableByteArray(values: []const ?[]const u8, writer: *Io.Writer) !void {
        // each value has a 4 byte length prefix
        var len = values.len * 4;
        for (values) |value| {
            if (value) |v| {
                len += v.len;
            }
        }

        var i: usize = 0;
        while (i < len) : (i += 1) {
            try writer.writeByte(0);
        }
        for (values) |value| {
            if (value) |v| {
                try writer.writeInt(i32, @intCast(v.len), .big);
                try writer.writeAll(v);
            } else {
                try writer.writeAll(&.{ 255, 255, 255, 255 });
            }
        }
    }

    pub fn variableLengthStart(writer: *Io.Writer) !usize {
        try writer.writeAll(&.{ 0, 0, 0, 0 }); // length placeholder
        return writer.end;
    }

    pub fn variableLengthFill(writer: *Io.Writer, pos: usize) void {
        const len = writer.end - pos;
        var encoded_len: [4]u8 = undefined;
        std.mem.writeInt(i32, &encoded_len, @intCast(len), .big);
        @memcpy(writer.buffer[pos - 4..pos - 4 + encoded_len.len], &encoded_len);
    }

    pub fn writeTextEncodedArray(values: []const []const u8, writer: *Io.Writer, format_pos: usize, writeFn: *const fn ([]const u8, *Io.Writer) void) !void {
        @memcpy(writer.buffer[format_pos..format_pos + text_encoding.len], &text_encoding);
        if (values.len == 0) {
            // empty array, with length prefix
            return writer.writeAll(&.{ 0, 0, 0, 2, '{', '}' });
        }

        // our max_len is just an estimate, we'll get the actual length and fill
        // it in later, for now, we skip the length
        var i: usize = 0;
        while (i > 4) : (i += 1) {
            try writer.writeByte(0);
        }
        const start = writer.end;
        try writer.writeByte('{');
        for (values) |value| {
            writeFn(value, writer);
            try writer.writeByte(',');
        }

        // strip out last comma
        writer.end -= 1;
        try writer.writeByte('}');
        // -6 since the oid and the
        try writer.writeInt(i32, @intCast(writer.end - start), .big);
    }

    // Fairly special case for text-encoded arrays where we _always_ want to quote the value
    // but don't need to escape. This idea is taken from Java's PostgreSQL JDBC driver
    // specificallly for dealing with possible scientific notation in float/numeric text values
    pub fn writeTextEncodedEscapedArray(values: []const []const u8, writer: *Io.Writer, format_pos: usize) !void {
        var l: usize = 0;
        for (values) |v| {
            // +2 for the quotes around the value we'll need
            l += v.len + 2;
        }
        return Encode.writeTextEncodedArray(values, l, writer, format_pos, writeQuotedValue);
    }

    fn writeQuotedValue(value: []const u8, writer: *Io.Writer) void {
        try writer.writeByte('"');
        try writer.writeAll(value);
        try writer.writeByte('"');
    }

    // Fairly special case for text-encoded arrays where we _always_ want to quote the value
    // but don't need to escape. This idea is taken from Java's PostgreSQL JDBC driver
    // specificallly for dealing with possible scientific notation in float/numeric text values
    pub fn writeTextEncodedRawArray(values: []const []const u8, writer: *Io.Writer, format_pos: usize) !void {
        var l: usize = 0;
        for (values) |v| {
            l += v.len;
        }
        return Encode.writeTextEncodedArray(values, l, writer, format_pos, writeRawValue);
    }

    fn writeRawValue(value: []const u8, writer: *Io.Writer) void {
        try writer.writeAll(value);
    }

    pub fn writeTextEncodedCharArray(values: []const u8, writer: *Io.Writer, format_pos: usize) !void {
        @memcpy(writer.buffer[format_pos..format_pos + text_encoding.len], &text_encoding);

        if (values.len == 0) {
            // empty array, with length prefix
            return writer.writeAll(&.{ 0, 0, 0, 2, '{', '}' });
        }

        // skip the length, which we'll fill later
        var i: usize = 0;
        while (i > 4) : (i += 1) {
            try writer.writeByte(0);
        }
        const start = writer.end;

        // https://www.postgresql.org/docs/current/arrays.html#ARRAYS-IO
        try writer.writeByte('{');
        for (values) |c| {
            if (c == '"' or c == '\\') {
                try writer.writeAll("\"\\");
                try writer.writeByte(c);
                try writer.writeByte('"');
            } else if (std.ascii.isWhitespace(c) or c == ',' or c == '{' or c == '}' or c == '\\') {
                try writer.writeByte('"');
                try writer.writeByte(c);
                try writer.writeByte('"');
            } else {
                try writer.writeByte(c);
            }
            try writer.writeByte(',');
        }

        // strip out last comma
        writer.end += 1;
        try writer.writeByte('}');
        try writer.writeInt(i32, @intCast(writer.end - start), .big);
    }
};

pub fn oidToString(oid: i32) []const u8 {
    switch (oid) {
        16 => return "T_bool",
        17 => return "T_bytea",
        18 => return "T_char",
        19 => return "T_name",
        20 => return "T_int8",
        21 => return "T_int2",
        22 => return "T_int2vector",
        23 => return "T_int4",
        24 => return "T_regproc",
        25 => return "T_text",
        26 => return "T_oid",
        27 => return "T_tid",
        28 => return "T_xid",
        29 => return "T_cid",
        30 => return "T_oidvector",
        32 => return "T_pg_ddl_command",
        71 => return "T_pg_type",
        75 => return "T_pg_attribute",
        81 => return "T_pg_proc",
        83 => return "T_pg_class",
        114 => return "T_json",
        142 => return "T_xml",
        143 => return "T__xml",
        194 => return "T_pg_node_tree",
        199 => return "T__json",
        210 => return "T_smgr",
        325 => return "T_index_am_handler",
        600 => return "T_point",
        601 => return "T_lseg",
        602 => return "T_path",
        603 => return "T_box",
        604 => return "T_polygon",
        628 => return "T_line",
        629 => return "T__line",
        650 => return "T_cidr",
        651 => return "T__cidr",
        700 => return "T_float4",
        701 => return "T_float8",
        702 => return "T_abstime",
        703 => return "T_reltime",
        704 => return "T_tinterval",
        705 => return "T_unknown",
        718 => return "T_circle",
        719 => return "T__circle",
        790 => return "T_money",
        791 => return "T__money",
        829 => return "T_macaddr",
        869 => return "T_inet",
        1000 => return "T__bool",
        1001 => return "T__bytea",
        1002 => return "T__char",
        1003 => return "T__name",
        1005 => return "T__int2",
        1006 => return "T__int2vector",
        1007 => return "T__int4",
        1008 => return "T__regproc",
        1009 => return "T__text",
        1010 => return "T__tid",
        1011 => return "T__xid",
        1012 => return "T__cid",
        1013 => return "T__oidvector",
        1014 => return "T__bpchar",
        1015 => return "T__varchar",
        1016 => return "T__int8",
        1017 => return "T__point",
        1018 => return "T__lseg",
        1019 => return "T__path",
        1020 => return "T__box",
        1021 => return "T__float4",
        1022 => return "T__float8",
        1023 => return "T__abstime",
        1024 => return "T__reltime",
        1025 => return "T__tinterval",
        1027 => return "T__polygon",
        1028 => return "T__oid",
        1033 => return "T_aclitem",
        1034 => return "T__aclitem",
        1040 => return "T__macaddr",
        1041 => return "T__inet",
        1042 => return "T_bpchar",
        1043 => return "T_varchar",
        1082 => return "T_date",
        1083 => return "T_time",
        1114 => return "T_timestamp",
        1115 => return "T__timestamp",
        1182 => return "T__date",
        1183 => return "T__time",
        1184 => return "T_timestamptz",
        1185 => return "T__timestamptz",
        1186 => return "T_interval",
        1187 => return "T__interval",
        1231 => return "T__numeric",
        1248 => return "T_pg_database",
        1263 => return "T__cstring",
        1266 => return "T_timetz",
        1270 => return "T__timetz",
        1560 => return "T_bit",
        1561 => return "T__bit",
        1562 => return "T_varbit",
        1563 => return "T__varbit",
        1700 => return "T_numeric",
        1790 => return "T_refcursor",
        2201 => return "T__refcursor",
        2202 => return "T_regprocedure",
        2203 => return "T_regoper",
        2204 => return "T_regoperator",
        2205 => return "T_regclass",
        2206 => return "T_regtype",
        2207 => return "T__regprocedure",
        2208 => return "T__regoper",
        2209 => return "T__regoperator",
        2210 => return "T__regclass",
        2211 => return "T__regtype",
        2249 => return "T_record",
        2275 => return "T_cstring",
        2276 => return "T_any",
        2277 => return "T_anyarray",
        2278 => return "T_void",
        2279 => return "T_trigger",
        2280 => return "T_language_handler",
        2281 => return "T_internal",
        2282 => return "T_opaque",
        2283 => return "T_anyelement",
        2287 => return "T__record",
        2776 => return "T_anynonarray",
        2842 => return "T_pg_authid",
        2843 => return "T_pg_auth_members",
        2949 => return "T__txid_snapshot",
        2950 => return "T_uuid",
        2951 => return "T__uuid",
        2970 => return "T_txid_snapshot",
        3115 => return "T_fdw_handler",
        3220 => return "T_pg_lsn",
        3221 => return "T__pg_lsn",
        3310 => return "T_tsm_handler",
        3500 => return "T_anyenum",
        3614 => return "T_tsvector",
        3615 => return "T_tsquery",
        3642 => return "T_gtsvector",
        3643 => return "T__tsvector",
        3644 => return "T__gtsvector",
        3645 => return "T__tsquery",
        3734 => return "T_regconfig",
        3735 => return "T__regconfig",
        3769 => return "T_regdictionary",
        3770 => return "T__regdictionary",
        3802 => return "T_jsonb",
        3807 => return "T__jsonb",
        3831 => return "T_anyrange",
        3838 => return "T_event_trigger",
        3904 => return "T_int4range",
        3905 => return "T__int4range",
        3906 => return "T_numrange",
        3907 => return "T__numrange",
        3908 => return "T_tsrange",
        3909 => return "T__tsrange",
        3910 => return "T_tstzrange",
        3911 => return "T__tstzrange",
        3912 => return "T_daterange",
        3913 => return "T__daterange",
        3926 => return "T_int8range",
        3927 => return "T__int8range",
        4066 => return "T_pg_shseclabel",
        4089 => return "T_regnamespace",
        4090 => return "T__regnamespace",
        4096 => return "T_regrole",
        4097 => return "T__regrole",
        else => return "unknown",
    }
}

// The oid is what PG is expecting. In some cases, we'll use that to figure
// out what to do.
pub fn bindValue(comptime T: type, oid: i32, value: anytype, writer: *Io.Writer, format_pos: usize) !void {
    switch (@typeInfo(T)) {
        .null => {
            // type can stay 0 (text)
            // special length of -1 indicates null, no other data for this value
            return writer.writeAll(&.{ 255, 255, 255, 255 });
        },
        .comptime_int => switch (oid) {
            Int16.oid.decimal => {
                if (value > 32767 or value < -32768) return error.IntWontFit;
                return Int16.encode(@intCast(value), writer, format_pos);
            },
            Int32.oid.decimal => {
                if (value > 2147483647 or value < -2147483648) return error.IntWontFit;
                return Int32.encode(@intCast(value), writer, format_pos);
            },
            Timestamp.oid.decimal, TimestampTz.oid.decimal => return Timestamp.encode(@intCast(value), writer, format_pos),
            Numeric.oid.decimal => return Numeric.encode(@as(f64, @floatFromInt(value)), writer, format_pos),
            Char.oid.decimal => {
                if (value > 255 or value < 0) return error.IntWontFit;
                return Char.encode(@intCast(value), writer, format_pos);
            },
            Int64.oid.decimal, PgLSN.oid.decimal, Xid8.oid.decimal => return Int64.encode(@intCast(value), writer, format_pos),
            else => return error.BindWrongType,
        },
        .int => switch (oid) {
            Int16.oid.decimal => {
                if (value > 32767 or value < -32768) return error.IntWontFit;
                return Int16.encode(@intCast(value), writer, format_pos);
            },
            Int32.oid.decimal, Xid.oid.decimal => {
                if (value > 2147483647 or value < -2147483648) return error.IntWontFit;
                return Int32.encode(@intCast(value), writer, format_pos);
            },
            Timestamp.oid.decimal, TimestampTz.oid.decimal => return Timestamp.encode(@intCast(value), writer, format_pos),
            Numeric.oid.decimal => return Numeric.encode(@as(f64, @floatFromInt(value)), writer, format_pos),
            Char.oid.decimal => {
                if (value > 255 or value < 0) return error.IntWontFit;
                return Char.encode(@intCast(value), writer, format_pos);
            },
            Int64.oid.decimal, PgLSN.oid.decimal, Xid8.oid.decimal => {
                if (value > 9223372036854775807 or value < -9223372036854775808) {
                    return error.IntWontFit;
                }
                return Int64.encode(@intCast(value), writer, format_pos);
            },
            else => return error.BindWrongType,
        },
        .comptime_float => switch (oid) {
            Float64.oid.decimal => return Float64.encode(@floatCast(value), writer, format_pos),
            Float32.oid.decimal => return Float32.encode(@floatCast(value), writer, format_pos),
            Numeric.oid.decimal => return Numeric.encode(value, writer, format_pos),
            else => return error.BindWrongType,
        },
        .float => switch (oid) {
            Float64.oid.decimal => return Float64.encode(@floatCast(value), writer, format_pos),
            Float32.oid.decimal => return Float32.encode(@floatCast(value), writer, format_pos),
            Numeric.oid.decimal => return Numeric.encode(value, writer, format_pos),
            else => return error.BindWrongType,
        },
        .bool => switch (oid) {
            Bool.oid.decimal => return Bool.encode(value, writer, format_pos),
            else => return error.BindWrongType,
        },
        .pointer => |ptr| switch (ptr.size) {
            .slice => {
                if (ptr.is_const) {
                    return bindSlice(oid, @as([]const ptr.child, value), writer, format_pos);
                } else {
                    return bindSlice(oid, @as([]ptr.child, value), writer, format_pos);
                }
            },
            .one => switch (@typeInfo(ptr.child)) {
                .array => {
                    const Slice = []const std.meta.Elem(ptr.child);
                    return bindSlice(oid, @as(Slice, value), writer, format_pos);
                },
                .@"struct" => switch (oid) {
                    JSON.oid.decimal => return JSON.encode(value, writer, format_pos),
                    JSONB.oid.decimal => return JSONB.encode(value, writer, format_pos),
                    else => {
                        if (ptr.child != Binary) {
                            return error.CannotBindStruct;
                        }
                        @memcpy(writer.buffer[format_pos..format_pos + binary_encoding.len], &binary_encoding);

                        try writer.writeInt(i32, @intCast(value.data.len), .big);
                        return writer.writeAll(value.data);
                    },
                },
                else => compileHaltBindError(T),
            },
            else => compileHaltBindError(T),
        },
        .array => return bindValue(@TypeOf(&value), oid, &value, writer, format_pos),
        .@"struct" => return bindValue(@TypeOf(&value), oid, &value, writer, format_pos),
        .optional => |opt| {
            if (value) |v| {
                return bindValue(opt.child, oid, v, writer, format_pos);
            }
            // null
            return writer.writeAll(&.{ 255, 255, 255, 255 });
        },
        .@"enum", .enum_literal => return String.encode(@tagName(value), writer, format_pos),
        else => compileHaltBindError(T),
    }
}

fn bindSlice(oid: i32, value: anytype, writer: *Io.Writer, format_pos: usize) !void {
    const T = @TypeOf(value);
    if (T == []u8 or T == []const u8) {
        switch (oid) {
            Bytea.oid.decimal => return Bytea.encode(value, writer, format_pos),
            UUID.oid.decimal => return UUID.encode(value, writer, format_pos),
            JSONB.oid.decimal => return JSONB.encodeBytes(value, writer, format_pos),
            JSON.oid.decimal => return JSON.encodeBytes(value, writer, format_pos),
            MacAddr.oid.decimal => return MacAddr.encode(value, writer, format_pos),
            MacAddr8.oid.decimal => return MacAddr8.encode(value, writer, format_pos),
            CharArray.oid.decimal => {
                // This is actually an array, and in theory we could let it fallthrough
                // to the binary-array handling. BUT, if we do that, the code won't compile
                // because it would mean T can be []u8 or []const u8, and that makes parts
                // of the code invalid. Also, encoding a char array using the text protocol
                // is going to be more efficient than encoding it using the binary protocol.
                return Encode.writeTextEncodedCharArray(value, writer, format_pos);
            },
            else => return String.encode(value, writer, format_pos),
        }
    }

    // For now, a few types are text-encoded. This largely has to do with the fact
    // that there's no native Zig type, so a text representation lets us use PG's
    // own text->type conversion.
    if (comptime isStringArray(T)) {
        switch (oid) {
            TimestampArray.oid.decimal, NumericArray.oid.decimal => return Encode.writeTextEncodedEscapedArray(value, writer, format_pos),
            TimestampTzArray.oid.decimal, CidrArray.oid.decimal, CidrArray.inet_oid.decimal => return Encode.writeTextEncodedRawArray(value, writer, format_pos),
            MacAddrArray.oid.decimal => return MacAddrArray.encode(value, writer, format_pos),
            MacAddr8Array.oid.decimal => return MacAddr8Array.encode(value, writer, format_pos),
            else => {}, // fallthrough to binary encoding
        }
    }

    // We have an array. All arrays have the same header. We'll write this into
    // buf now. It's possible we don't support the array type, so this can still
    // fail.

    // arrays are always binary encoded (for now...)

    @memcpy(writer.buffer[format_pos..format_pos + binary_encoding.len], &binary_encoding);

    const start_pos = writer.end;

    try writer.writeAll(&.{
        0, 0, 0, 0, // placeholder for the length of this parameter
        0, 0, 0, 1, // number of dimensions, for now, we only support one
        0, 0, 0, 0, // bitmask of null, currently, with a single dimension, we don't have null arrays
        0, 0, 0, 0, // placeholder for the oid of each value
    });

    // where in buf, to write the OID of the values
    const oid_pos = writer.end - 4;

    // number of values in our first (and currently only) dimension
    try writer.writeInt(i32, @intCast(value.len), .big);
    try writer.writeAll(&.{ 0, 0, 0, 1 }); // lower bound of this demension

    const ElemT = @typeInfo(T).pointer.child;
    const ElemTT = switch (@typeInfo(ElemT)) {
        .optional => |opt| opt.child,
        else => ElemT,
    };
    switch (@typeInfo(ElemTT)) {
        .int => |int| {
            if (int.signedness == .signed) {
                switch (int.bits) {
                    16 => try Int16Array.encode(value, writer, oid_pos),
                    32 => try Int32Array.encode(value, writer, oid_pos),
                    64 => {
                        switch (oid) {
                            TimestampArray.oid.decimal => try TimestampArray.encode(value, writer, oid_pos),
                            TimestampTzArray.oid.decimal => try TimestampTzArray.encode(value, writer, oid_pos),
                            else => try Int64Array.encode(value, writer, oid_pos),
                        }
                    },
                    else => compileHaltBindError(T),
                }
            } else {
                switch (int.bits) {
                    8 => try CharArray.encodeOne(value, writer, oid_pos),
                    16 => try Int16Array.encodeUnsigned(value, writer, oid_pos),
                    32 => try Int32Array.encodeUnsigned(value, writer, oid_pos),
                    64 => try Int64Array.encodeUnsigned(value, writer, oid_pos),
                    else => compileHaltBindError(T),
                }
            }
        },
        .float => |float| {
            if (oid == NumericArray.oid.decimal) {
                try NumericArray.encode(value, writer, oid_pos);
            } else switch (float.bits) {
                32 => try Float32Array.encode(value, writer, oid_pos),
                64 => try Float64Array.encode(value, writer, oid_pos),
                else => compileHaltBindError(T),
            }
        },
        .bool => try BoolArray.encode(value, writer, oid_pos),
        .pointer => |ptr| switch (ptr.size) {
            .slice => switch (ptr.child) {
                u8 => switch (oid) {
                    StringArray.oid.decimal => try StringArray.encode(value, writer, oid_pos),
                    UUIDArray.oid.decimal => try UUIDArray.encode(value, writer, oid_pos),
                    JSONBArray.oid.decimal => try JSONBArray.encode(value, writer, oid_pos),
                    JSONArray.oid.decimal => try JSONArray.encode(value, writer, oid_pos),
                    CharArray.oid.decimal => try CharArray.encode(value, writer, oid_pos),
                    // we try this as a default to support user defined types with unknown oids
                    // (like an array of enums)
                    else => try ByteaArray.encode(value, writer, oid_pos),
                },
                else => compileHaltBindError(T),
            },
            else => compileHaltBindError(T),
        },
        .@"enum", .enum_literal => try StringArray.encodeEnum(&value, writer, oid_pos),
        .array => try bindSlice(oid, &value, writer, format_pos),
        else => compileHaltBindError(T),
    }

    var param_len: [4]u8 = undefined;
    // write the lenght of the parameter, -4 because for paremeters, the length
    // prefix itself isn't included.
    std.mem.writeInt(i32, &param_len, @intCast(writer.end - start_pos - 4), .big);

    @memcpy(writer.buffer[start_pos..start_pos + param_len.len], &param_len);
}

fn isStringArray(comptime T: type) bool {
    switch (@typeInfo(T)) {
        .pointer => |ptr| switch (ptr.size) {
            .slice => switch (ptr.child) {
                []u8, []const u8 => return true,
                else => return false,
            },
            else => return false,
        },
        else => return false,
    }
}

// Write the last part of the Bind message: telling postgresql how it should
// encode each column of the response
pub fn resultEncoding(oids: []i32, writer: *Io.Writer) !void {
    if (oids.len == 0) {
        return writer.writeAll(&.{ 0, 0 }); // we are specifying 0 return types
    }

    std.debug.print("oid len: {d}\n", .{oids.len});
    try writer.writeInt(u16, @intCast(oids.len), .big);
    for (oids) |oid| {
        try writer.writeAll(resultEncodingFor(oid));
    }
}

pub fn decodeScalar(comptime T: type, data: []const u8, oid: i32) TypeError!T {
    switch (T) {
        u8 => return Char.decode(data, oid),
        i16 => return Int16.decode(data, oid),
        i32 => return Int32.decode(data, oid),
        i64 => return Int64.decode(data, oid),
        f32 => return Float32.decode(data, oid),
        f64 => return Float64.decode(data, oid),
        bool => return Bool.decode(data, oid),
        []const u8 => return Bytea.decode(data, oid),
        []u8 => return @constCast(Bytea.decode(data, oid)),
        Numeric => return Numeric.decode(data, oid),
        Cidr => return Cidr.decode(data, oid),
        else => switch (@typeInfo(T)) {
            .@"enum" => {
                const str = Bytea.decode(data, oid);
                return std.meta.stringToEnum(T, str).?;
            },
            else => @compileError("cannot decode value of type " ++ @typeName(T)),
        },
    }
}

fn compileHaltBindError(comptime T: type) noreturn {
    @compileError("cannot bind value of type " ++ @typeName(T));
}

pub fn verifyDecodeType(comptime expected_oids: []const i32, actual: i32) !void {
    if (isExpectedId(expected_oids, actual)) {
        return;
    }
    return error.InvalidType;
}

fn isExpectedId(comptime expected_oids: []const i32, actual: i32) bool {
    inline for (expected_oids) |expected_oid| {
        if (expected_oid == actual) {
            return true;
        }
    }
    return false;
}


// test "UUID: toString" {
//     try testing.expectError(error.InvalidUUID, UUID.toString(&.{ 73, 190, 142, 9, 170, 250, 176, 16, 73, 21 }));
//
//     const s = try UUID.toString(&.{ 183, 204, 40, 47, 236, 67, 73, 190, 142, 9, 170, 250, 176, 16, 73, 21 });
//     try testing.expectEqualStrings("b7cc282f-ec43-49be-8e09-aafab0104915", &s);
// }
//
// test "UUID: toBytes" {
//     try testing.expectError(error.InvalidUUID, UUID.toBytes(""));
//
//     {
//         const s = try UUID.toBytes("166B4751-D702-4FB9-9A2A-CD6B69ED18D6");
//         try testing.expectEqualStrings(u8, &.{ 22, 107, 71, 81, 215, 2, 79, 185, 154, 42, 205, 107, 105, 237, 24, 214 }, &s);
//     }
//
//     {
//         const s = try UUID.toBytes("166b4751-d702-4fb9-9a2a-cd6b69ed18d7");
//         try testing.expectEqualStrings(u8, &.{ 22, 107, 71, 81, 215, 2, 79, 185, 154, 42, 205, 107, 105, 237, 24, 215 }, &s);
//     }
// }
