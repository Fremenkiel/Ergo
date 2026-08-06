const std = @import("std");

const Io = std.Io;
const math = std.math;
const testing = std.testing;

const assert = std.debug.assert;

const PgError = @import("root.zig").PgError;

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

pub const text_encoding = [2]u8{ 0, 0 };
pub const binary_encoding = [2]u8{ 0, 1 };

pub const Bytea = struct {
    pub const oid = OID.make(17);
    const encoding = &binary_encoding;

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
        return @constCast(data);
    }
};

pub const JSONB = struct {
    pub const oid = OID.make(3802);
    const encoding = &binary_encoding;

    fn decode(data: []const u8, data_oid: i32) PgError![]const u8 {
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

pub const String = struct {
    pub const oid = OID.make(25);
    const encoding = &text_encoding;
};

pub const Timestamp = struct {
    pub fn decode(pg_wal_us: u64) Io.Timestamp {
        const seconds_between_epochs: i96 = 946_684_800;
        const ns_between_epochs: i96 = seconds_between_epochs * 1_000_000_000;

        const unix_ns: i96 = @as(i96, @intCast(pg_wal_us)) * 1000 + ns_between_epochs;

        return std.Io.Timestamp.fromNanoseconds(unix_ns);
    }
};

// Return the encoding we want PG to use for a particular OID
fn resultEncodingFor(oid: i32) *const [2]u8 {
    return switch (oid) {
        String.oid.decimal => &text_encoding,
        else => &binary_encoding,
    };
}

// Write the last part of the Bind message: telling postgresql how it should
// encode each column of the response
pub fn resultEncoding(oids: []i32, writer: *Io.Writer) !void {
    if (oids.len == 0) {
        return writer.writeAll(&.{ 0, 0 }); // we are specifying 0 return types
    }

    try writer.writeInt(u16, @intCast(oids.len), .big);
    for (oids) |oid| {
        try writer.writeAll(resultEncodingFor(oid));
    }
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
