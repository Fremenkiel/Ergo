const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const conn = @import("conn.zig");
const Stream = @import("stream.zig").Stream;

const user_key = "user";
const database_key = "database";
const application_name_key = "application_name";

pub const StartupMessage = struct {
    const startup_message_protocol: []const u8 = &[_]u8{ 0, 3, 0, 0 };

    pub fn write(allocator: mem.Allocator, stream: *Stream, opts: conn.Opts) !void {
        var payload_len: u64 = 4 + 1; // len + end zero
        payload_len += startup_message_protocol.len;
        payload_len += user_key.len + opts.username.len + 2;
        payload_len += database_key.len + opts.database.len + 2;
        payload_len += application_name_key.len + opts.application_name.len + 2;

        var it = opts.startup_parameters.iterator();
        while (it.next()) |kv| {
            // +2 because both key and value are null-terminated
            payload_len += kv.key_ptr.len + kv.value_ptr.len + 2;
        }

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, payload_len);
        defer allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeInt(u32, @intCast(payload_len), .big);
        try writer.writeAll(startup_message_protocol);

        try writer.writeAll(user_key);
        try writer.writeByte(0);
        try writer.writeAll(opts.username);
        try writer.writeByte(0);

        try writer.writeAll(database_key);
        try writer.writeByte(0);
        try writer.writeAll(opts.database);
        try writer.writeByte(0);

        try writer.writeAll(application_name_key);
        try writer.writeByte(0);
        try writer.writeAll(opts.application_name);
        try writer.writeByte(0);

        it = opts.startup_parameters.iterator();
        while (it.next()) |kv| {
            try writer.writeAll(kv.key_ptr.*);
            try writer.writeByte(0);
            try writer.writeAll(kv.value_ptr.*);
            try writer.writeByte(0);
        }
        try writer.writeByte(0);

        try stream.writeAll(buf);
    }
};

pub const SASLResponse = struct {
    pub fn write(allocator: mem.Allocator, stream: *Stream, data: []const u8) !void {
        // 4 +   N
        // len + $data
        const payload_len = 4 + data.len;

        const total_length = payload_len + 1;

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, total_length);
        allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeByte('p');
        try writer.writeInt(u32, @intCast(payload_len), .big);
        try writer.writeAll(data);

        assert(total_length == @sizeOf(u8) + @sizeOf(u32) + data.len);
        assert(buf.len == total_length);
        assert(buf[0] == 'p');

        try stream.writeAll(buf);
    }
};

pub const SASLInitialResponse = struct {
    pub fn write(allocator: mem.Allocator, stream: *Stream, response: []const u8, mechanism: []const u8) !void {
        // 4 +   M          + 1 + 4             + R
        // len + $mechanism + 0 + $response.len + $response
        const payload_len = 9 + mechanism.len + response.len;

        const total_length = payload_len + 1;

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, total_length);
        defer allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeByte('p');
        try writer.writeInt(u32, @intCast(payload_len), .big);
        try writer.writeAll(mechanism);
        try writer.writeByte(0);
        try writer.writeInt(u32, @intCast(response.len), .big);
        try writer.writeAll(response);

        assert(total_length == @sizeOf(u8) + @sizeOf(u32) + mechanism.len + @sizeOf(u8) + @sizeOf(u32) + response.len);
        assert(buf.len == total_length);
        assert(buf[0] == 'p');

        // -1 for index offset, +1 for the next byte.
        assert(buf[@sizeOf(u8) + @sizeOf(u32) + mechanism.len] == 0);

        try stream.writeAll(buf);
    }
};

pub const Query = struct {
    pub fn write(allocator: mem.Allocator, stream: *Stream, sql: []const u8) !void {
        // 4   + S    + 1
        // len + $sql + 0
        const payload_len = 5 + sql.len;

        // +1 for the type field, 'Q'
        const total_length = payload_len + 1;

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, total_length);
        defer allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeByte('Q');
        try writer.writeInt(u32, @intCast(payload_len), .big);
        try writer.writeAll(sql);
        try writer.writeByte(0);

        assert(total_length == @sizeOf(u8) + @sizeOf(u32) + sql.len + @sizeOf(u8));
        assert(buf.len == total_length);
        assert(buf[0] == 'Q');
        assert(buf[buf.len - 1] == 0);

        try stream.writeAll(buf);
    }
};

pub const PasswordMessage = struct {
    pub fn write(allocator: mem.Allocator, stream: *Stream, password: []const u8) !void {
        // +4 since the payload length includes the length itself
        // +1 for null terminated string
        const payload_len = password.len + 5;

        // +1 for the type field, 'p'
        const total_length = payload_len + 1;

        var buf: []u8 = undefined;
        buf = try allocator.alloc(u8, total_length);
        defer allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeByte('p');
        try writer.writeInt(u32, @intCast(payload_len), .big);
        try writer.writeAll(password);
        try writer.writeByte(0);

        assert(total_length == @sizeOf(u8) + @sizeOf(u32) + password.len + @sizeOf(u8));
        assert(buf.len == total_length);
        assert(buf[0] == 'p');
        assert(buf[buf.len - 1] == 0);

        try stream.writeAll(buf);
    }
};

pub const CommandComplete = struct {
    pub fn parse(data: []const u8) !?i64 {
        assert(data.len > 0);

        if (data[data.len - 1] != 0) {
            return error.NotAString;
        }

        const buf = data[0 .. data.len - 1];

        const end = buf.len - 1;
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
        assert(buf.len >= 4);

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
                const md5 = buf[4..];

                assert(@sizeOf(@TypeOf(code)) + md5.len == buf.len);

                return .{ .md5 = md5 };
            },
            10 => {
                var sasl = SASL{};
                const str = buf[4..];

                assert(@sizeOf(@TypeOf(code)) + str.len == buf.len);

                if (readOptionalString(str)) |auth_mechanism| {
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
        assert(buf.len > 4);

        const code = std.mem.readInt(u32, buf[0..4][0..4], .big);

        assert(code == 12);

        const msg = buf[4..];

        assert(@sizeOf(@TypeOf(code)) + msg.len == buf.len);

        return msg;
    }
};

pub const AuthenticationSASLContinue = struct {
    pub fn parse(buf: []const u8) ![]const u8 {
        assert(buf.len > 4);

        const code = std.mem.readInt(u32, buf[0..4][0..4], .big);

        assert(code == 11);

        const msg = buf[4..];

        assert(@sizeOf(@TypeOf(code)) + msg.len == buf.len);

        return msg;
    }
};

pub fn readOptionalString(buf: []const u8) ?[]const u8 {
    assert(buf.len > 0);
    const index = std.mem.indexOfScalarPos(u8, buf, 0, 0) orelse return null;

    assert(index > 0);

    const value = buf[0..index];

    assert(value.len <= buf.len);

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
test "CommandComplete: parse" {
    const allocator = testing.allocator;
    {
        // not a string (not null terminated)
        try testing.expectError(error.NotAString, CommandComplete.parse("123"));
    }

    {
        // success
        const buf = try allocator.alloc(u8,  12);
        defer allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeAll("CREATE ROLE");
        try writer.writeByte(0);

        try testing.expectEqual(null, try CommandComplete.parse(buf));
    }
}

test "CommandComplete: rowsAffected" {
    const allocator = testing.allocator;

    {
        const buf = try allocator.alloc(u8,  10);
        defer allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeAll("DROP ROLE");
        try writer.writeByte(0);
        
        try testing.expectEqual(null, try CommandComplete.parse(buf));
    }

    {
        const buf = try allocator.alloc(u8,  13);
        defer allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeAll("INSERT 392 1");
        try writer.writeByte(0);
        
        try testing.expectEqual(1, try CommandComplete.parse(buf));
    }

    {
        const buf = try allocator.alloc(u8,  12);
        defer allocator.free(buf);

        var writer = Io.Writer.fixed(buf);

        try writer.writeAll("DELETE 9392");
        try writer.writeByte(0);
        
        try testing.expectEqual(9392, try CommandComplete.parse(buf));
    }
}
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
