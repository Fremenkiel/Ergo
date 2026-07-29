const std = @import("std");

const mem = std.mem;
const Io = std.Io;
const testing = std.testing;

const conn = @import("conn.zig");
const protocol = @import("protocol.zig");

const Reader = @import("reader.zig").Reader;
const Stream = @import("stream.zig").Stream;

// Todo: Redo this
// Weird return (but Zig has no error payloads, so..)
// null on success
// a []const on a PG error
//   - can be be passed to  proto.Error.parse(owned)
//   - is only valid until the next call to reader.read()
//     (we expect our caller to clone the value)
// a normal zig error on any other error

pub const AuthError = error {
UnableToAuthenticate,
UnexpectedDBMessage,
InvalidSASLFlow,
};

pub const Auth = struct {
    allocator: mem.Allocator,
    io: Io,

    reader: *Reader,
    stream: *Stream,

    opts: conn.Opts,

    err_data: ?[]const u8,

    pub fn init(allocator: mem.Allocator, io: Io, reader: *Reader, opts: conn.Opts) Auth {
        return .{
            .allocator = allocator,
            .io = io,
            .reader = reader,
            .stream = &reader.stream,
            .opts = opts,
            .err_data = null,
        };
    }

    pub fn auth(self: *@This()) !void {
        try self.reader.startFlow(self.opts.timeout_ms);

        // ignore errors on endFlow, because it's troublesome to handle, and only
        // something really bad (like OOM) can happen, and that'll surface again
        // as soon as the app tries to use the connection.
        defer self.reader.endFlow() catch |err| {
            std.debug.print("Error: unable to end flow: {s}\n", .{@errorName(err)});
        };

        // write our startup message
        try protocol.StartupMessage.write(self.allocator, self.stream, self.opts);

        const init_msg = try self.reader.next();
        switch (init_msg.type) {
            'R' => {},
            'E' => {
                self.err_data = init_msg.data;
                return AuthError.UnableToAuthenticate;
            },
            else => return AuthError.UnexpectedDBMessage,
        }

        switch (try protocol.AuthenticationRequest.parse(init_msg.data)) {
            .ok => return,
            .sasl => |sasl| self.saslAuth(sasl) catch |err| {
                return err;
            },
            .md5 => |salt| try self.md5PasswordAuth(salt),
            .password => try self.passwordAuth(self.opts.password orelse ""),
        }

        const final_msg = try self.reader.next();
        switch (final_msg.type) {
            'R' => {},
            'E' => {
                self.err_data = final_msg.data;
                return AuthError.UnableToAuthenticate;
            },
            else => return AuthError.UnexpectedDBMessage,
        }

        switch (try protocol.AuthenticationRequest.parse(final_msg.data)) {
            .ok => return,
            else => return AuthError.UnexpectedDBMessage,
        }
    }

    fn saslAuth(self: *@This(), req: protocol.AuthenticationRequest.SASL) !void {
        if (!req.scram_sha_256) {
            return AuthError.UnexpectedDBMessage;
        }
        var sasl = try SASL.init(self.allocator, self.io);
        defer sasl.deinit();

        // send the client initial response
        try protocol.SASLInitialResponse.write(self.allocator, self.stream, sasl.client_first_message, "SCRAM-SHA-256");

        // read the server continue response
        const init_msg = try self.reader.next();
        switch (init_msg.type) {
            'R' => {},
            'E' => {
                self.err_data = init_msg.data;
                return AuthError.UnableToAuthenticate;
            },
            else => return AuthError.InvalidSASLFlow,
        }
        const continue_data = try protocol.AuthenticationSASLContinue.parse(init_msg.data);
        try sasl.serverResponse(continue_data);

        // send the client final response
        const client_final_message = try sasl.clientFinalMessage(self.opts.password orelse "");
        defer self.allocator.free(client_final_message);
        try protocol.SASLResponse.write(self.allocator, self.stream, client_final_message);

        // read the server final response
        const msg = try self.reader.next();
        switch (msg.type) {
            'R' => {},
            'E' => {
                self.err_data = msg.data;
                return AuthError.UnableToAuthenticate;
            },
            else => return AuthError.InvalidSASLFlow,
        }
        const final_data = try protocol.AuthenticationSASLFinal.parse(msg.data);
        try sasl.verifyServerFinal(final_data);
    }

    fn md5PasswordAuth(self: *@This(), salt: []const u8) !void {
        var hash: [16]u8 = undefined;

        var hasher = std.crypto.hash.Md5.init(.{});
        hasher.update(self.opts.password orelse "");
        hasher.update(self.opts.username);
        hasher.final(&hash);

        const hex_hash = std.fmt.bytesToHex(&hash, .lower);
        var hex_hasher = std.crypto.hash.Md5.init(.{});
        hex_hasher.update(&hex_hash);
        hex_hasher.update(salt);
        hex_hasher.final(&hash);

        var hashed_password: [35]u8 = undefined;
        const password = try std.fmt.bufPrint(&hashed_password, "md5{s}", .{&std.fmt.bytesToHex(&hash, .lower)});

        try self.passwordAuth(password);
    }

    fn passwordAuth(self: *@This(), password: []const u8) !void {
        try protocol.PasswordMessage.write(self.allocator, self.stream, password);
    }
};

const SASL = struct {
    allocator: mem.Allocator,
    client_first_message: []u8,
    auth_message: ?[]const u8 = null,
    salted_password: ?[32]u8 = null,
    server_response: ?ServerResponse = null,

    const Base64Encoder = std.base64.standard.Encoder;
    const Base64Decoder = std.base64.standard.Decoder;

    pub fn init(allocator: mem.Allocator, io: Io) !SASL {
        var nonce: [18]u8 = undefined;
        std.Io.random(io, &nonce);

        var client_first_message = try allocator.alloc(u8, 32);
        client_first_message[0] = 'n';
        client_first_message[1] = ',';
        client_first_message[2] = ',';
        client_first_message[3] = 'n';
        client_first_message[4] = '=';
        client_first_message[5] = ',';
        client_first_message[6] = 'r';
        client_first_message[7] = '=';
        _ = Base64Encoder.encode(client_first_message[8..], &nonce);

        return .{
            .allocator = allocator,
            .client_first_message = client_first_message,
        };
    }

    pub fn deinit(self: *@This()) void {
        self.allocator.free(self.client_first_message);

        if (self.server_response) |server_response| {
            defer self.allocator.free(server_response.raw);
        }

        if (self.auth_message) |auth_message| {
            defer self.allocator.free(auth_message);
        }
    }

    pub fn serverResponse(self: *SASL, data: []const u8) !void {
        if (data.len < 8) {
            return error.InvalidLength;
        }

        // Specification states the attribute positions are fixed, so we expect r=X,s=Y,i=Z
        if (data[0] != 'r' or data[1] != '=') {
            return error.InvalidNoncePrefix;
        }

        const owned = try self.allocator.dupe(u8, data);

        var res = ServerResponse{
            .raw = owned,
            .nonce = undefined,
            .base64_salt = undefined,
            .iterations = undefined,
        };

        var pos: usize = 2;
        {
            const sep = std.mem.indexOfScalarPos(u8, owned, pos, ',') orelse return error.MissingSalt;
            res.nonce = owned[2..sep];
            pos = sep + 1;
        }

        {
            const value_start = pos + 2;
            if (owned.len < value_start or owned[pos] != 's' or owned[pos + 1] != '=') {
                return error.InvalidSaltPrefix;
            }
            pos = value_start;

            const sep = std.mem.indexOfScalarPos(u8, owned, pos, ',') orelse return error.MissingIterations;
            res.base64_salt = owned[pos..sep];
            pos = sep + 1;
        }

        {
            const value_start = pos + 2;
            if (owned.len < value_start or owned[pos] != 'i' or owned[pos + 1] != '=') {
                return error.InvalidIterationPrefix;
            }
            pos = value_start;
            const sep = std.mem.indexOfScalarPos(u8, owned, pos, ',') orelse owned.len;
            res.iterations = std.fmt.parseInt(u32, owned[pos..sep], 10) catch return error.InvalidIteration;
        }

        self.server_response = res;
    }

    pub fn clientFinalMessage(self: *SASL, password: []const u8) ![]const u8 {
        const sr = self.server_response orelse return error.MissingServerResponse;
        const allocator = self.allocator;

        const salt = try allocator.alloc(u8, try Base64Decoder.calcSizeForSlice(sr.base64_salt));
        defer allocator.free(salt);
        try Base64Decoder.decode(salt, sr.base64_salt);

        const unproved = try std.fmt.allocPrint(allocator, "c=biws,r={s}", .{sr.nonce});
        defer allocator.free(unproved);

        const auth_message = try std.fmt.allocPrint(allocator, "{s},{s},{s}", .{ self.client_first_message[3..], sr.raw, unproved });

        const salted_password = blk: {
            var buf: [32]u8 = undefined;
            try std.crypto.pwhash.pbkdf2(&buf, password, salt, sr.iterations, std.crypto.auth.hmac.sha2.HmacSha256);
            break :blk buf;
        };

        const proof = blk: {
            var client_key: [32]u8 = undefined;
            std.crypto.auth.hmac.sha2.HmacSha256.create(&client_key, "Client Key", &salted_password);

            var stored_key: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(&client_key, &stored_key, .{});

            var client_signature: [32]u8 = undefined;
            std.crypto.auth.hmac.sha2.HmacSha256.create(&client_signature, auth_message, &stored_key);

            var proof: [32]u8 = undefined;
            for (client_key, client_signature, 0..) |ck, cs, i| {
                proof[i] = ck ^ cs;
            }

            var encoded_proof: [44]u8 = undefined;
            _ = Base64Encoder.encode(&encoded_proof, &proof);
            break :blk encoded_proof;
        };

        self.auth_message = auth_message;
        self.salted_password = salted_password;
        return std.fmt.allocPrint(allocator, "{s},p={s}", .{ unproved, proof });
    }

    pub fn verifyServerFinal(self: *SASL, data: []const u8) !void {
        if (data.len < 46) {
            return error.InvalidLength;
        }
        const auth_message = self.auth_message orelse return error.MissingAutMessage;
        const salted_password = if (self.salted_password) |*sp| sp else return error.MissingSaltedPassword;

        const computed_signature = blk: {
            var server_key: [32]u8 = undefined;
            std.crypto.auth.hmac.sha2.HmacSha256.create(&server_key, "Server Key", salted_password);

            var server_signature: [32]u8 = undefined;
            std.crypto.auth.hmac.sha2.HmacSha256.create(&server_signature, auth_message, &server_key);

            var encoded_signature: [44]u8 = undefined;
            _ = Base64Encoder.encode(&encoded_signature, &server_signature);
            break :blk encoded_signature;
        };

        // don't tell me about timing leaks unless there's also something in std to deal with it
        if (std.mem.eql(u8, &computed_signature, data[2..]) == false) {
            return error.InvalidServerSignature;
        }
    }
};

pub const ServerResponse = struct {
    raw: []const u8,
    nonce: []const u8,
    base64_salt: []const u8,
    iterations: u32,
};

// const t = @import("t.zig");
// test "SASL: init" {
//     defer t.reset();
//     var sasl1 = try SASL.init(t.io, t.arena.allocator());
//
//     try testing.expectEqualStrings("n,,n=,r=", sasl1.client_first_message[0..8]);
//
//     var sasl2 = try SASL.init(t.io, t.arena.allocator());
//     try testing.expectEqualStrings("n,,n=,r=", sasl2.client_first_message[0..8]);
//
//     var sasl3 = try SASL.init(t.io, t.arena.allocator());
//     try testing.expectEqualStrings("n,,n=,r=", sasl3.client_first_message[0..8]);
//
//     var sasl4 = try SASL.init(t.io, t.arena.allocator());
//     try testing.expectEqualStrings("n,,n=,r=", sasl4.client_first_message[0..8]);
//
//     // TODO: Redo this
//     // The nonce should be random. It's unlikely that if we generate 4, we'd get
//     // the same value at a given byte.
//     const nonce1 = sasl1.client_first_message[8..];
//     const nonce2 = sasl2.client_first_message[8..];
//     const nonce3 = sasl3.client_first_message[8..];
//     const nonce4 = sasl4.client_first_message[8..];
//     for (0..18) |i| {
//         try testing.expectEqual(true, nonce1[i] != nonce2[i] or
//             nonce2[i] != nonce3[i] or
//             nonce1[i] != nonce3[i] or
//             nonce3[i] != nonce4[i] or
//             nonce1[i] != nonce4[i] or
//             nonce2[i] != nonce4[i]);
//     }
// }
//
// test "SASL: serverResponse invalid" {
//     //invalid response
//     const InvalidTest = struct {
//         input: []const u8,
//         expected: anyerror,
//     };
//
//     const test_cases = [_]InvalidTest{
//         .{ .input = "", .expected = error.InvalidLength },
//         .{ .input = "r", .expected = error.InvalidLength },
//         .{ .input = "r=", .expected = error.InvalidLength },
//         .{ .input = "s=abc,r=123,i=32", .expected = error.InvalidNoncePrefix },
//         .{ .input = "r=abc123,i=32,s=aaa", .expected = error.InvalidSaltPrefix },
//         .{ .input = "r=abc123,s=aaa,x=32", .expected = error.InvalidIterationPrefix },
//         .{ .input = "r=abc123", .expected = error.MissingSalt },
//         .{ .input = "r=abc123,s=aaaa", .expected = error.MissingIterations },
//         .{ .input = "r=abc123,s=aaaa,i=123a", .expected = error.InvalidIteration },
//     };
//
//     defer t.reset();
//     var sasl = try SASL.init(t.io, t.arena.allocator());
//
//     for (test_cases) |tc| {
//         try testing.expectError(tc.expected, sasl.serverResponse(tc.input));
//         try testing.expectEqual(null, sasl.server_response);
//     }
// }
//
// test "SASL: serverResponse" {
//     defer t.reset();
//     var sasl = try SASL.init(t.io, t.arena.allocator());
//
//     try sasl.serverResponse("r=abc123,s=aaaaxa,i=4096");
//     try testing.expectEqualStrings("abc123", sasl.server_response.?.nonce);
//     try testing.expectEqualStrings("aaaaxa", sasl.server_response.?.base64_salt);
//     try testing.expectEqual(4096, sasl.server_response.?.iterations);
// }
