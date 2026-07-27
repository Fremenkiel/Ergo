const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const conn = @import("conn.zig");

pub const StartupMessage = struct {
    const startup_message_protocol: []const u8 = &[_]u8{ 0, 3, 0, 0 };

    pub fn write(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, opts: conn.Opts) !void {
        var payload_len: u64 = 4 + 1; // len + end zero
        payload_len += startup_message_protocol.len;
        payload_len += "user".len + opts.username.len + 2;
        payload_len += "database".len + opts.database.len + 2;
        payload_len += "application_name".len + opts.application_name.len + 2;

        var it = opts.startup_parameters.iterator();
        while (it.next()) |kv| {
            // +2 because both key and value are null-terminated
            payload_len += kv.key_ptr.len + kv.value_ptr.len + 2;
        }

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, payload_len);
        defer allocator.free(buf);

        var writer = stream.writer(io, buf);
        const w = &writer.interface;

        try w.writeInt(u32, @intCast(payload_len), .big);
        try w.writeAll(startup_message_protocol);

        try w.writeAll("user");
        try w.writeByte(0);
        try w.writeAll(opts.username);
        try w.writeByte(0);

        try w.writeAll("database");
        try w.writeByte(0);
        try w.writeAll(opts.database);
        try w.writeByte(0);

        try w.writeAll("application_name");
        try w.writeByte(0);
        try w.writeAll(opts.application_name);
        try w.writeByte(0);

        it = opts.startup_parameters.iterator();
        while (it.next()) |kv| {
            try w.writeAll(kv.key_ptr.*);
            try w.writeByte(0);
            try w.writeAll(kv.value_ptr.*);
            try w.writeByte(0);
        }
        try w.writeByte(0);

        try w.flush();
    }
};

pub const SASLResponse = struct {
    pub fn write(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, data: []const u8) !void {
        // 4 +   N
        // len + $data
        const payload_len = 4 + data.len;

        const total_length = payload_len + 1;

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, total_length);
        allocator.free(buf);

        var writer = stream.writer(io, buf);
        const w = &writer.interface;

        try w.writeByte('p');
        try w.writeInt(u32, @intCast(payload_len), .big);
        try w.writeAll(data);

        try w.flush();
    }
};

pub const SASLInitialResponse = struct {
    pub fn write(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, response: []const u8, mechanism: []const u8) !void {
        // 4 +   M          + 1 + 4             + R
        // len + $mechanism + 0 + $response.len + $response
        const payload_len = 9 + mechanism.len + response.len;

        const total_length = payload_len + 1;

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, total_length);
        defer allocator.free(buf);

        var writer = stream.writer(io, buf);
        const w = &writer.interface;

        try w.writeByte('p');
        try w.writeInt(u32, @intCast(payload_len), .big);
        try w.writeAll(mechanism);
        try w.writeByte(0);
        try w.writeInt(u32, @intCast(response.len), .big);
        try w.writeAll(response);

        try w.flush();
    }
};

pub const Query = struct {
    pub fn write(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, sql: []const u8) !void {
        // 4   + S    + 1
        // len + $sql + 0
        const payload_len = 5 + sql.len;

        // +1 for the type field, 'Q'
        const total_length = payload_len + 1;

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, total_length);
        defer allocator.free(buf);

        var writer = stream.writer(io, buf);
        var w = &writer.interface;

        try w.writeByte('Q');
        try w.writeInt(u32, @intCast(payload_len), .big);
        try w.writeAll(sql);
        try w.writeByte(0);

        try w.flush();
    }
};

pub const PasswordMessage = struct {
    pub fn write(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, password: []const u8) !void {
        // +4 since the payload length includes the length itself
        // +1 for null terminated string
        const payload_len = password.len + 5;

        // +1 for the type field, 'p'
        const total_length = payload_len + 1;

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, total_length);
        defer allocator.free(buf);

        var writer = stream.writer(io, buf);
        var w = &writer.interface;

        try w.writeByte('p');
        try w.writeInt(u32, @intCast(payload_len), .big);
        try w.writeAll(password);
        try w.writeByte(0);

        try w.flush();
    }
};

const CommandComplete = struct {
    pub fn parse(buf: []const u8) ![]const u8 {
        assert(buf[buf.len - 1] == 0);

        const end = buf.len - 2;
        var i: usize = end;
        while (i >= 0) : (i -= 1) {
            const b = buf[i];
            if (b < '0' or b > '9') {
                break;
            }
        }

        if (i == end) {
            return null;
        }

        return std.fmt.parseInt(i64, buf[(i + 1)..], 10) catch unreachable;
    }
};

pub const AuthenticationRequest = union(enum) {
    ok: void,
    password: void,
    md5: []const u8,
    sasl: SASL,

    pub const SASL = struct {
        scram_sha_256: bool = false,
        scram_sha_256_plus: bool = false,
    };

    pub fn parse(buf: []const u8) !AuthenticationRequest {
        const code = std.mem.readInt(u32, buf[0..4][0..4], .big);
        switch (code) {
            0 => return .{ .ok = {} }, // authentication ok
            3 => return .{ .password = {} }, // authentication requires a plain-text password
            5 => {
                // 4           + 4 + 4
                // payload_len + 5 + $salt
                if (buf.len != 8) {
                    return error.NoMoreData;
                }
                return .{ .md5 = buf[5..] };
            },
            10 => {
                var sasl = SASL{};
                while (readOptionalString(buf[5..])) |auth_mechanism| {
                    if (std.ascii.eqlIgnoreCase(auth_mechanism, "SCRAM-SHA-256")) {
                        sasl.scram_sha_256 = true;
                    } else if (std.ascii.eqlIgnoreCase(auth_mechanism, "SCRAM-SHA-256-PLUS")) {
                        sasl.scram_sha_256_plus = true;
                    }
                }
                return .{ .sasl = sasl };
            },
            else => return error.AuthNotSupported,
        }
    }
};

pub const AuthenticationSASLFinal = struct {
    pub fn parse(buf: []const u8) ![]const u8 {
        const code = std.mem.readInt(u32, buf[0..4][0..4], .big);

        assert(code == 12);

        return buf[5..];
    }
};

pub const AuthenticationSASLContinue = struct {
    pub fn parse(buf: []const u8) ![]const u8 {
        const code = std.mem.readInt(u32, buf[0..4][0..4], .big);

        assert(code == 11);

        return buf[5..];
    }
};

pub fn readOptionalString(buf: []const u8) ?[]const u8 {
    const index = std.mem.indexOfScalarPos(u8, buf, 0, 0) orelse return null;

    const value = buf[0..index];
    return value;
}

// test "StartupMessage: write" {
//     const allocator = testing.allocator;
        //
        // var buf: []u8 = undefined;
        // buf = try allocator.alloc(u8, total_length);
//     defer allocator.free(buf);
//
//     const s = @This(){ .username = "leto", .database = "ghanima" };
//     try s.write(allocator, &buf);
//
//     var reader = Reader.init(buf.string());
//     try testing.expectEqual(36, try reader.int32()); // payload length
//     try testing.expectEqual(196608, try reader.int32()); // protocol version
//     try testing.expectEqualStrings("user", try reader.string());
//     try testing.expectEqualStrings("leto", try reader.string());
//     try testing.expectEqualStrings("database", try reader.string());
//     try testing.expectEqualStrings("ghanima", try reader.string());
//     try testing.expectEqualSlices(u8, &.{0}, reader.rest());
// }
//
// test "SASLResponse: write" {
//     const io = testing.io;
//
//     const s = writeSASLResponse(io, stream, "the response");
//
//     var reader = Reader.init(buf.string());
//     try testing.expectEqual('p', try reader.byte());
//     try testing.expectEqual(16, try reader.int32()); // payload length
//     try testing.expectEqualStrings("the response", reader.rest());
// }
//
// test "SASLInitialResponse: write" {
//     const io = testing.io;
//
//     const s = writeSASLInitialResponse(io, stream, "a sasl response", "SCRAM-SHA-256");
//
//     var reader = Reader.init(buf.string());
//     try t.expectEqual('p', try reader.byte());
//     try t.expectEqual(37, try reader.int32()); // payload length
//     try t.expectString("SCRAM-SHA-256", try reader.string());
//     try t.expectEqual(15, try reader.int32()); // length of response
//     try t.expectString("a sasl response", reader.rest());
// }
//
// test "Query: write" {
//     const allocator = testing.allocator;
//     const io = testing.io;
//
//     const q = writeQuery(allocator, io, stream, "select 1");
//
//     var reader = Reader.init(buf.string());
//     try testing.expectEqual('Q', try reader.byte());
//     try testing.expectEqual(13, try reader.int32()); // payload length
//     try testing.expectEqualStrings("select 1", try reader.restAsString());
// }
//
// test "PasswordMessage: write" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     const pw = PasswordMessage{ .password = "gh@nim@" };
//     try pw.write(&buf);
//
//     var reader = Reader.init(buf.string());
//     try t.expectEqual('p', try reader.byte());
//     try t.expectEqual(12, try reader.int32()); // payload length
//     try t.expectString("gh@nim@", try reader.string());
// }
//
// test "CommandComplete: parse" {
//     {
//         // not a string (not null terminated)
//         try buf.write("123");
//         try t.expectError(error.NotAString, CommandComplete.parse(buf.string()));
//     }
//
//     {
//         // success
//         try buf.write("CREATE ROLE");
//         try buf.writeByte(0);
//
//         const c = try CommandComplete.parse(buf.string());
//         try t.expectString("CREATE ROLE", c.tag);
//     }
// }
//
// test "CommandComplete: rowsAffected" {
//     {
//         const c = CommandComplete{ .tag = "DROP ROLE" };
//         try t.expectEqual(null, c.rowsAffected());
//     }
//
//     {
//         const c = CommandComplete{ .tag = "INSERT 392 1" };
//         try t.expectEqual(1, c.rowsAffected());
//     }
//
//     {
//         const c = CommandComplete{ .tag = "DELETE 9392" };
//         try t.expectEqual(9392, c.rowsAffected());
//     }
// }
//
// test "AuthenticationRequest: invalid" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     {
//         // empty
//         try t.expectError(error.NoMoreData, AuthenticationRequest.parse(buf.string()));
//     }
//
//     {
//         // less than minimum length
//         try buf.write("123");
//         try t.expectError(error.NoMoreData, AuthenticationRequest.parse(buf.string()));
//     }
//
//     {
//         // unknown auth type
//         buf.reset();
//         try buf.writeIntBig(u32, 99);
//         try t.expectError(error.AuthNotSupported, AuthenticationRequest.parse(buf.string()));
//     }
// }
//
// test "AuthenticationRequest: ok" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     try buf.writeIntBig(u32, 0);
//     const request = try AuthenticationRequest.parse(buf.string());
//     try t.expectEqual({}, request.ok);
// }
//
// test "AuthenticationRequest: password" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     try buf.writeIntBig(u32, 3);
//     const request = try AuthenticationRequest.parse(buf.string());
//     try t.expectEqual({}, request.password);
// }
//
// test "AuthenticationRequest: md5" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     try buf.writeIntBig(u32, 5);
//     try buf.write("s@Lt");
//     const request = try AuthenticationRequest.parse(buf.string());
//     try t.expectString("s@Lt", request.md5);
// }
//
// test "AuthenticationRequest: sasl with 1 mechanism" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     {
//         try buf.writeIntBig(u32, 10);
//         try buf.write("SCRAM-SHA-256");
//         try buf.writeByte(0);
//
//         const request = try AuthenticationRequest.parse(buf.string());
//         try t.expectEqual(true, request.sasl.scram_sha_256);
//         try t.expectEqual(false, request.sasl.scram_sha_256_plus);
//     }
//
//     {
//         buf.reset();
//         try buf.writeIntBig(u32, 10);
//         try buf.write("SCRAM-SHA-256-PLUS");
//         try buf.writeByte(0);
//
//         const request = try AuthenticationRequest.parse(buf.string());
//         try t.expectEqual(false, request.sasl.scram_sha_256);
//         try t.expectEqual(true, request.sasl.scram_sha_256_plus);
//     }
// }
//
// test "AuthenticationRequest: sasl with multiple including unknown" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     try buf.writeIntBig(u32, 10);
//     try buf.write("SCRAM-SHA-256-PLUS");
//     try buf.writeByte(0);
//     try buf.write("SCRAM-SHA-256");
//     try buf.writeByte(0);
//     try buf.write("SCRAM-MD5");
//     try buf.writeByte(0);
//
//     const request = try AuthenticationRequest.parse(buf.string());
//     try t.expectEqual(true, request.sasl.scram_sha_256);
//     try t.expectEqual(true, request.sasl.scram_sha_256_plus);
// }
//
// test "AuthenticationSASLFinal: parse" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     {
//         // too short
//         try t.expectError(error.NoMoreData, AuthenticationSASLFinal.parse(buf.string()));
//
//         try buf.write("123");
//         try t.expectError(error.NoMoreData, AuthenticationSASLFinal.parse(buf.string()));
//     }
//
//     {
//         // wrong special sasl type
//         buf.reset();
//         try buf.writeIntBig(u32, 13);
//         try t.expectError(error.NotSASLChallenge, AuthenticationSASLFinal.parse(buf.string()));
//     }
//
//     {
//         // success
//         buf.reset();
//         try buf.writeIntBig(u32, 12);
//         try buf.write("some server data");
//
//         const final = try AuthenticationSASLFinal.parse(buf.string());
//         try t.expectString("some server data", final.data);
//     }
// }
//
// test "AuthenticationSASLContinue: parse" {
//     var buf = try proto.Buffer.init(t.allocator, 128);
//     defer buf.deinit();
//
//     {
//         // too short
//         try t.expectError(error.NoMoreData, AuthenticationSASLContinue.parse(buf.string()));
//
//         try buf.write("123");
//         try t.expectError(error.NoMoreData, AuthenticationSASLContinue.parse(buf.string()));
//     }
//
//     {
//         // wrong special sasl type
//         buf.reset();
//         try buf.writeIntBig(u32, 12);
//         try t.expectError(error.NotSASLChallenge, AuthenticationSASLContinue.parse(buf.string()));
//     }
//
//     {
//         // success
//         buf.reset();
//         try buf.writeIntBig(u32, 11);
//         try buf.write("r=a-nounce,s=the-S@lt,i=4096");
//
//         const c = try AuthenticationSASLContinue.parse(buf.string());
//         try t.expectString("r=a-nounce,s=the-S@lt,i=4096", c.data);
//     }
// }
//
