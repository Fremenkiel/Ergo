const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const conn = @import("conn.zig");
const root = @import("root.zig");

const PgConfig = root.PgConfig;
const PgError = root.PgError;
const Stream = @import("stream.zig").Stream;

const user_key = "user";
const database_key = "database";
const application_name_key = "application_name";

// Custom assert to ensure correct error testing. 
// Should be replaced with native zig handling if implemented.
fn assert(check: bool, err: anyerror) !void {
    if (!check) return err;
}

pub const StartupMessage = StartupMessageT(Stream);

pub fn StartupMessageT(comptime ProtocolStream: type) type {
    return struct {
        const startup_message_protocol: []const u8 = &[_]u8{ 0, 3, 0, 0 };

        pub fn write(allocator: mem.Allocator, stream: *ProtocolStream, opts: PgConfig) !void {
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
}

pub const SASLResponse = SASLResponseT(Stream);

pub fn SASLResponseT(comptime ProtocolStream: type) type {
    return struct {
        pub fn write(allocator: mem.Allocator, stream: *ProtocolStream, data: []const u8) !void {
            // 4 +   N
            // len + $data
            const payload_len = 4 + data.len;

            const total_length = payload_len + 1;
            try assert(total_length == @sizeOf(u8) + @sizeOf(u32) + data.len, PgError.InvalidMessageLength);

            var buf: []u8 = undefined;
            buf = try allocator.alloc(u8, total_length);
            defer allocator.free(buf);
            try assert(buf.len == total_length, PgError.InvalidBufferLength);

            var writer = Io.Writer.fixed(buf);

            try writer.writeByte('p');
            try writer.writeInt(u32, @intCast(payload_len), .big);
            try writer.writeAll(data);

            try assert(buf[0] == 'p', PgError.InvalidMessageType);

            try stream.writeAll(buf);
        }
    };
}

pub const SASLInitialResponse = SASLInitialResponseT(Stream);

pub fn SASLInitialResponseT(comptime ProtocolStream: type) type {
    return struct {
        pub fn write(allocator: mem.Allocator, stream: *ProtocolStream, response: []const u8, mechanism: []const u8) !void {
            // 4 +   M          + 1 + 4             + R
            // len + $mechanism + 0 + $response.len + $response
            const payload_len = 9 + mechanism.len + response.len;

            const total_length = payload_len + 1;
            try assert(total_length == @sizeOf(u8) + @sizeOf(u32) + mechanism.len + @sizeOf(u8) + @sizeOf(u32) + response.len, PgError.InvalidMessageLength);

            var buf: []u8 = undefined;
            buf = try allocator.alloc(u8, total_length);
            defer allocator.free(buf);
            try assert(buf.len == total_length, PgError.InvalidBufferLength);

            var writer = Io.Writer.fixed(buf);

            try writer.writeByte('p');
            try writer.writeInt(u32, @intCast(payload_len), .big);
            try writer.writeAll(mechanism);
            try writer.writeByte(0);
            try writer.writeInt(u32, @intCast(response.len), .big);
            try writer.writeAll(response);

            try assert(buf[0] == 'p', PgError.InvalidMessageType);

            // -1 for index offset, +1 for the next byte.
            try assert(buf[@sizeOf(u8) + @sizeOf(u32) + mechanism.len] == 0, PgError.InvalidStringDelimitor);

            try assert(writer.end == total_length, PgError.InvalidWrite);

            try stream.writeAll(buf);
        }
    };
}

pub const Query = QueryT(Stream);

pub fn QueryT(comptime ProtocolStream: type) type {
    return struct {
        pub fn write(allocator: mem.Allocator, stream: *ProtocolStream, sql: []const u8) !void {
            try assert(sql.len > 0, PgError.InvalidData);

            // 4   + S    + 1
            // len + $sql + 0
            const payload_len = 5 + sql.len;

            // +1 for the type field, 'Q'
            const total_length = payload_len + 1;
            try assert(total_length == @sizeOf(u8) + @sizeOf(u32) + sql.len + @sizeOf(u8), PgError.InvalidMessageLength);

            var buf: []u8 = undefined;
            buf = try allocator.alloc(u8, total_length);
            defer allocator.free(buf);
            try assert(buf.len == total_length, PgError.InvalidBufferLength);

            var writer = Io.Writer.fixed(buf);

            try writer.writeByte('Q');
            try writer.writeInt(u32, @intCast(payload_len), .big);
            try writer.writeAll(sql);
            try writer.writeByte(0);

            try assert(buf[0] == 'Q', PgError.InvalidMessageType);
            try assert(buf[buf.len - 1] == 0, PgError.InvalidStringDelimitor);

            try stream.writeAll(buf);
        }
    };
}

pub const PasswordMessage = PasswordMessageT(Stream);

pub fn PasswordMessageT(comptime ProtocolStream: type) type {
    return struct {
        pub fn write(allocator: mem.Allocator, stream: *ProtocolStream, password: []const u8) !void {
            // +4 since the payload length includes the length itself
            // +1 for null terminated string
            const payload_len = password.len + 5;

            // +1 for the type field, 'p'
            const total_length = payload_len + 1;
            try assert(total_length == @sizeOf(u8) + @sizeOf(u32) + password.len + @sizeOf(u8), PgError.InvalidMessageLength);

            var buf: []u8 = undefined;
            buf = try allocator.alloc(u8, total_length);
            defer allocator.free(buf);
            try assert(buf.len == total_length, PgError.InvalidBufferLength);

            var writer = Io.Writer.fixed(buf);

            try writer.writeByte('p');
            try writer.writeInt(u32, @intCast(payload_len), .big);
            try writer.writeAll(password);
            try writer.writeByte(0);

            try assert(buf[0] == 'p', PgError.InvalidMessageType);
            try assert(buf[buf.len - 1] == 0, PgError.InvalidStringDelimitor);

            try stream.writeAll(buf);
        }
    };
}

pub const CopyDone = CopyDoneT(Stream);

fn CopyDoneT(comptime ProtocolStream: type) type {
    return struct {
        pub fn write(stream: *ProtocolStream) !void {
            var len_buf: [4]u8 = undefined;
            mem.writeInt(i32, &len_buf, 4, .big);

            try stream.writeAll("c");
            try stream.writeAll(&len_buf);
        }
    };
}

pub const CommandComplete = struct {
    pub fn parse(data: []const u8) !?i64 {
        try assert(data.len > 0, PgError.InvalidData);

        try assert(data[data.len - 1] == 0, PgError.InvalidStringDelimitor);

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

        return std.fmt.parseInt(i64, buf[(i + 1)..], 10) catch PgError.InvalidAffectedCount;
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

    pub fn parse(data: []const u8) !AuthenticationRequest {
        try assert(data.len >= 4, PgError.InvalidData);

        var reader = Io.Reader.fixed(data);

        const code = try reader.takeInt(u32, .big);
        switch (code) {
            0 => return .{ .ok = {} }, // authentication ok
            3 => return .{ .password = {} }, // authentication requires a plain-text password
            5 => {
                // 4           + 4 + 4
                // payload_len + 5 + $salt
                try assert(data.len == 8, PgError.InvalidData);
                const md5 = try reader.take(data.len - reader.seek);

                try assert(@sizeOf(@TypeOf(code)) + md5.len == data.len, PgError.InvalidMessageLength);

                return .{ .md5 = md5 };
            },
            10 => {
                var sasl = SASL{};

                try assert(@sizeOf(@TypeOf(code)) - reader.seek == 0, PgError.InvalidMessageLength);

                while (try readOptionalString(&reader)) |auth_mechanism| {
                    if (std.ascii.eqlIgnoreCase(auth_mechanism, "SCRAM-SHA-256")) {
                        sasl.scram_sha_256 = true;
                    } else if (std.ascii.eqlIgnoreCase(auth_mechanism, "SCRAM-SHA-256-PLUS")) {
                        sasl.scram_sha_256_plus = true;
                    }
                }
                return .{ .sasl = sasl };
            },
            else => return PgError.AuthNotSupported,
        }
    }
};

pub const AuthenticationSASLFinal = struct {
    pub fn parse(data: []const u8) ![]const u8 {
        try assert(data.len > 4, PgError.InvalidData);

        const code = std.mem.readInt(u32, data[0..4][0..4], .big);

        try assert(code == 12, PgError.InvalidResponseCode);

        const msg = data[4..];

        try assert(@sizeOf(@TypeOf(code)) + msg.len == data.len, PgError.InvalidMessageLength);

        return msg;
    }
};

pub const AuthenticationSASLContinue = struct {
    pub fn parse(data: []const u8) ![]const u8 {
        try assert(data.len > 4, PgError.InvalidData);

        const code = std.mem.readInt(u32, data[0..4][0..4], .big);

        try assert(code == 11, PgError.InvalidResponseCode);

        const msg = data[4..];

        try assert(@sizeOf(@TypeOf(code)) + msg.len == data.len, PgError.InvalidMessageLength);

        return msg;
    }
};

pub fn readOptionalString(reader: *Io.Reader) !?[]const u8 {
    if (reader.bufferedLen() == 0) return null;

    const value = reader.takeDelimiter(0) catch {
        return null;
    };

    try assert(value == null or value.?.len <= reader.end, PgError.InvalidData);

    return value;
}

const t = @import("t.zig");
test "StartupMessage: write" {
    const allocator = testing.allocator;

    var mock_stream = try t.Stream.init(allocator, null);
    defer mock_stream.deinit();

    const TestStartupMessage = StartupMessageT(t.Stream);
    try TestStartupMessage.write(allocator, &mock_stream, .{ .username = "james", .database = "doe", .host = "localhost", .application_name = "Ergo test", .startup_parameters = .init(allocator) });

    var reader = Io.Reader.fixed(mock_stream.toString());
    const startup_protocol_length = try reader.takeInt(u32, .big);
    const startup_protocol_version = try reader.takeInt(u32, .big);
    const startup_user_key = try reader.takeDelimiter(0);
    const startup_user_value = try reader.takeDelimiter(0);
    const startup_database_key = try reader.takeDelimiter(0);
    const startup_database_value = try reader.takeDelimiter(0);
    const startup_application_name_key = try reader.takeDelimiter(0);
    const startup_application_name_value = try reader.takeDelimiter(0);

    try testing.expectEqual(60, startup_protocol_length);
    try testing.expectEqual(196608, startup_protocol_version);
    try testing.expectEqualStrings(user_key, startup_user_key.?);
    try testing.expectEqualStrings("james", startup_user_value.?);
    try testing.expectEqualStrings(database_key, startup_database_key.?);
    try testing.expectEqualStrings("doe", startup_database_value.?);
    try testing.expectEqualStrings(application_name_key, startup_application_name_key.?);
    try testing.expectEqualStrings("Ergo test", startup_application_name_value.?);
    try testing.expectEqualSlices(u8, &.{0}, try reader.take(1));
    try testing.expectError(error.EndOfStream, reader.take(1));
}

test "SASLResponse: write" {
    const allocator = testing.allocator;

    var mock_stream = try t.Stream.init(allocator, null);
    defer mock_stream.deinit();

    const TestSASLResponse = SASLResponseT(t.Stream);
    try TestSASLResponse.write(allocator, &mock_stream, "the response");

    var reader = Io.Reader.fixed(mock_stream.toString());

    const message_type = try reader.takeByte();
    const payload_len = try reader.takeInt(u32, .big);
    const payload = try reader.take(@as(usize, @intCast(payload_len)) - @sizeOf(@TypeOf(payload_len)));

    try testing.expectEqual('p', message_type);
    try testing.expectEqual(16, payload_len);
    try testing.expectEqualStrings("the response", payload);
    try testing.expectError(error.EndOfStream, reader.takeByte());
}

test "SASLInitialResponse: write" {
    const allocator = testing.allocator;

    var mock_stream = try t.Stream.init(allocator, null);
    defer mock_stream.deinit();

    const TestSASLInitialResponse = SASLInitialResponseT(t.Stream);
    try TestSASLInitialResponse.write(allocator, &mock_stream, "a sasl response", "SCRAM-SHA-256");

    var reader = Io.Reader.fixed(mock_stream.toString());

    const message_type = try reader.takeByte();
    const payload_len = try reader.takeInt(u32, .big);
    const mechanism = try reader.takeDelimiter(0);
    const response_len = try reader.takeInt(u32, .big);
    const response = try reader.take(response_len);

    try testing.expectEqual('p', message_type);
    try testing.expectEqual(37, payload_len);
    try testing.expectEqualStrings("SCRAM-SHA-256", mechanism.?);
    try testing.expectEqual(15, response_len);
    try testing.expectEqualStrings("a sasl response", response);
    try testing.expectError(error.EndOfStream, reader.takeByte());

    try testing.expectEqual(@sizeOf(@TypeOf(payload_len)) + mechanism.?.len + @sizeOf(u8) + @sizeOf(@TypeOf(response_len)) + response.len, payload_len);
}

test "Query: write" {
    const allocator = testing.allocator;

    var mock_stream = try t.Stream.init(allocator, null);
    defer mock_stream.deinit();

    const TestQuery = QueryT(t.Stream);
    try TestQuery.write(allocator, &mock_stream, "select 1");

    var reader = Io.Reader.fixed(mock_stream.toString());

    const message_type = try reader.takeByte();
    const payload_len = try reader.takeInt(u32, .big);
    const query = try reader.takeDelimiter(0);

    try testing.expectEqual('Q', message_type);
    try testing.expectEqual(13, payload_len);
    try testing.expectEqualStrings("select 1", query.?);
    try testing.expectError(error.EndOfStream, reader.takeByte());
}

test "PasswordMessage: write" {
    const allocator = testing.allocator;

    var mock_stream = try t.Stream.init(allocator, null);
    defer mock_stream.deinit();

    const TestPasswordMessage = PasswordMessageT(t.Stream);
    try TestPasswordMessage.write(allocator, &mock_stream, "gh@nim@");

    var reader = Io.Reader.fixed(mock_stream.toString());

    const message_type = try reader.takeByte();
    const payload_len = try reader.takeInt(u32, .big);
    const password = try reader.takeDelimiter(0);

    try testing.expectEqual('p', message_type);
    try testing.expectEqual(12, payload_len);
    try testing.expectEqualStrings("gh@nim@", password.?);
    try testing.expectError(error.EndOfStream, reader.takeByte());

}

test "CommandComplete: parse" {
    const allocator = testing.allocator;
    {
        // not a string (not null terminated)
        try testing.expectError(PgError.InvalidStringDelimitor, CommandComplete.parse("123"));
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

    const buf = try allocator.alloc(u8,  512);
    defer allocator.free(buf);

    var writer = Io.Writer.fixed(buf);

    {
        try testing.expectError(PgError.InvalidData, CommandComplete.parse(buf[0 .. writer.end]));
    }

    writer.end = 0;

    {
        try writer.writeAll("DROP ROLE");

        try testing.expectError(PgError.InvalidStringDelimitor, CommandComplete.parse(buf[0 .. writer.end]));
    }

    {

        try writer.writeAll("DROP ROLE");
        try writer.writeByte(0);

        try testing.expectEqual(null, try CommandComplete.parse(buf[0 .. writer.end]));
    }

    writer.end = 0;

    {
        try writer.writeAll("INSERT 392 1");
        try writer.writeByte(0);

        try testing.expectEqual(1, try CommandComplete.parse(buf[0 .. writer.end]));
    }

    writer.end = 0;

    {
        try writer.writeAll("DELETE 9392");
        try writer.writeByte(0);

        try testing.expectEqual(9392, try CommandComplete.parse(buf[0 .. writer.end]));
    }
}

test "AuthenticationRequest: invalid" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);

    {
        // empty
        try testing.expectError(PgError.InvalidData, AuthenticationRequest.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        // less than minimum length
        try writer.writeAll("123");
        try testing.expectError(PgError.InvalidData, AuthenticationRequest.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        // unknown auth type
        try writer.writeInt(u32, 99, .big);
        try testing.expectError(error.AuthNotSupported, AuthenticationRequest.parse(buffer[0 .. writer.end]));
    }
}

test "AuthenticationRequest: ok" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);

    try writer.writeInt(u32, 0, .big);
    const request = try AuthenticationRequest.parse(buffer[0 .. writer.end]);
    try testing.expectEqual({}, request.ok);
}

test "AuthenticationRequest: password" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);

    try writer.writeInt(u32, 3, .big);
    const request = try AuthenticationRequest.parse(buffer[0 .. writer.end]);
    try testing.expectEqual({}, request.password);
}

test "AuthenticationRequest: md5" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);

    {
        try writer.writeInt(u32, 5, .big);
        try writer.writeAll("s@L");
        try testing.expectError(PgError.InvalidData, AuthenticationRequest.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        try writer.writeInt(u32, 5, .big);
        try writer.writeAll("s@Lt");
        const request = try AuthenticationRequest.parse(buffer[0 .. writer.end]);
        try testing.expectEqualStrings("s@Lt", request.md5);
    }

}

test "AuthenticationRequest: sasl with 1 mechanism" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);

    {
        try writer.writeInt(u32, 10, .big);
        try writer.writeAll("SCRAM-SHA-256");
        try writer.writeByte(0);

        const request = try AuthenticationRequest.parse(buffer[0 .. writer.end]);
        try testing.expectEqual(true, request.sasl.scram_sha_256);
        try testing.expectEqual(false, request.sasl.scram_sha_256_plus);
    }

    writer.end = 0;

    {
        try writer.writeInt(u32, 10, .big);
        try writer.writeAll("SCRAM-SHA-256-PLUS");
        try writer.writeByte(0);

        const request = try AuthenticationRequest.parse(buffer[0 .. writer.end]);
        try testing.expectEqual(false, request.sasl.scram_sha_256);
        try testing.expectEqual(true, request.sasl.scram_sha_256_plus);
    }
}

test "AuthenticationRequest: sasl with multiple including unknown" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);

    try writer.writeInt(u32, 10, .big);
    try writer.writeAll("SCRAM-SHA-256-PLUS");
    try writer.writeByte(0);
    try writer.writeAll("SCRAM-SHA-256");
    try writer.writeByte(0);
    try writer.writeAll("SCRAM-MD5");
    try writer.writeByte(0);

    const request = try AuthenticationRequest.parse(buffer[0 .. writer.end]);
    try testing.expectEqual(true, request.sasl.scram_sha_256);
    try testing.expectEqual(true, request.sasl.scram_sha_256_plus);
}

test "AuthenticationSASLFinal: parse" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);

    {
        // too short
        try testing.expectError(PgError.InvalidData, AuthenticationSASLFinal.parse(buffer[0 .. writer.end]));

        try writer.writeAll("123");
        try testing.expectError(PgError.InvalidData, AuthenticationSASLFinal.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        // wrong special sasl type
        try writer.writeInt(u32, 13, .big);
        try testing.expectError(PgError.InvalidData, AuthenticationSASLFinal.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        // wrong special sasl type
        try writer.writeAll("13425");
        try testing.expectError(PgError.InvalidResponseCode, AuthenticationSASLFinal.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        // success
        try writer.writeInt(u32, 12, .big);
        try writer.writeAll("some server data");

        const final = try AuthenticationSASLFinal.parse(buffer[0 .. writer.end]);
        try testing.expectEqualStrings("some server data", final);
    }
}

test "AuthenticationSASLContinue: parse" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);

    var writer = Io.Writer.fixed(buffer);

    {
        // too short
        try testing.expectError(PgError.InvalidData, AuthenticationSASLContinue.parse(buffer[0 .. writer.end]));

        try writer.writeAll("123");
        try testing.expectError(PgError.InvalidData, AuthenticationSASLContinue.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        // wrong special sasl type
        try writer.writeInt(u32, 12, .big);
        try testing.expectError(PgError.InvalidData, AuthenticationSASLContinue.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        // wrong special sasl type
        try writer.writeInt(u32, 1234, .big);
        try testing.expectError(PgError.InvalidData, AuthenticationSASLContinue.parse(buffer[0 .. writer.end]));
    }

    writer.end = 0;

    {
        // success
        try writer.writeInt(u32, 11, .big);
        try writer.writeAll("r=a-nounce,s=the-S@lt,i=4096");

        const c = try AuthenticationSASLContinue.parse(buffer[0 .. writer.end]);
        try testing.expectEqualStrings("r=a-nounce,s=the-S@lt,i=4096", c);
    }
}

