const std = @import("std");
const builtin = @import("builtin");

const os = std.os;
const mem = std.mem;
const Io = std.Io;
const testing = std.testing;

const openssl = @import("openssl");
const protocol = @import("protocol.zig");
const root = @import("root.zig");
const ssl = @import("ssl.zig");
const types = @import("types.zig");

const Auth = @import("auth.zig").Auth;
const Error = @import("error.zig").Error;
const Message = @import("reader.zig").Message;
const PgConfig = root.PgConfig;
const PgError = root.PgError;
const Reader = @import("reader.zig").Reader;
const Stream = @import("stream.zig").Stream;

const sendTerminate = @import("stream.zig").sendTerminate;

pub const State = enum {
    idle,

    // something bad happened
    fail,

    // we're doing a query
    query,

    // we're in a transaction
    transaction,
};

pub const Conn = struct {
    // If we own the ssl context (which only happens if the connection is
    // created directly and NOT through a pool), then we have to free it
    ssl_ctx: ?*openssl.SSL_CTX,

    // If we get a postgreSQL error, this will be set.
    err: ?Error,

    // The underlying data for err
    err_data: ?[]const u8,

    stream: Stream,

    // The current transation state, this is whatever the last ReadyForQuery
    // message told us
    state: State,

    // Used to read data from PG. Has its own buffer which can grow dynamically
    reader: Reader,

    allocator: mem.Allocator,

    io: Io,

    opts: PgConfig,

    pub const QueryOpts = struct {
        timeout_ms: ?i32 = null,
        column_names: bool = true,

        allocator: ?mem.Allocator = null,

        // When not null, the prepared statement will be cached and re-used
        // by subsequent queries using the same name.
        cache_name: ?[]const u8 = null,
    };

    pub fn init(io: Io, allocator: mem.Allocator, opts: PgConfig) !@This() {
        var ssl_ctx: ?*openssl.SSL_CTX = null;
        switch (opts.tls) {
            .off => {},
            else => |tls_config| {
                ssl_ctx = try ssl.initializeSSLContext(tls_config);
            },
        }
        errdefer ssl.freeSSLContext(ssl_ctx);

        var stream = try Stream.init(allocator, io, opts, ssl_ctx);
        errdefer stream.deinit(allocator);

        const reader = try Reader.init(allocator, opts.read_buffer orelse 4096, stream);
        errdefer reader.deinit();

        return .{
            .err = null,
            .ssl_ctx = ssl_ctx,
            .reader = reader,
            .stream = stream,
            .err_data = null,
            .state = .idle,
            .allocator = allocator,
            .io = io,
            .opts = opts,
        };
    }

    pub fn cancel(self: *@This()) void {
        self.stream.shutdown(.recv) catch {};
    }

    pub fn deinit(self: *@This()) void {
        if (self.err_data) |err_data| {
            self.allocator.free(err_data);
        }
        self.reader.deinit();

        sendTerminate(&self.stream, self.io);
        ssl.freeSSLContext(self.ssl_ctx);
        self.stream.deinit(self.allocator);
    }

    pub fn auth(self: *@This()) !void {
        var conn_auth = Auth.init(self.allocator, self.io, &self.reader, self.opts);

        conn_auth.auth() catch |err| {
            if (!builtin.is_test) {
                std.log.err("auth error: {s}", .{@errorName(err)});
            }
            if (conn_auth.err_data) |err_data| {
                return self.setErr(err_data);
            }
            return PgError.UnexpectedDBMessage;
        };

        while (true) {
            const msg = try self.read();
            switch (msg.type) {
                'Z' => return,
                'K' => {}, // TODO: BackendKeyData
                else => return self.unexpectedDBMessage(),
            }
        }
    }

    pub fn read(self: *@This()) !Message {
        var reader = &self.reader;
        while (true) {
            const msg = reader.next() catch |err| {
                self.state = .fail;
                return err;
            };
            switch (msg.type) {
                'Z' => {
                    self.state = switch (msg.data[0]) {
                        'I' => .idle,
                        'T' => .transaction,
                        'E' => .fail,
                        else => unreachable,
                    };
                    return msg;
                },
                'S' => {}, // TODO: ParameterStatus,
                'N' => {}, // TODO: NoticeResponse
                'E' => {
                    return self.setErr(msg.data);
                },
                else => return msg,
            }
        }
    }

    pub fn write(self: *@This(), data: []const u8) !void {
        self.stream.writeAll(data) catch |err| {
            self.state = .fail;
            return err;
        };
    }

    pub fn sendStandbyStatusUpdate(self: *@This(), last_lsn: u64, server_timestamp: i64) !void {
        var buf: [34]u8 = undefined;
        buf[0] = 'r';
        std.mem.writeInt(u64, buf[1..9], last_lsn, .big); // received
        std.mem.writeInt(u64, buf[9..17], last_lsn, .big); // flushed
        std.mem.writeInt(u64, buf[17..25], last_lsn, .big); // applied

        // Echo the server's timestamp to avoid needing OS clock functions
        std.mem.writeInt(i64, buf[25..33], server_timestamp, .big);
        buf[33] = 0; // reply requested

        var msg_lenbuf: [4]u8 = undefined;
        std.mem.writeInt(u32, &msg_lenbuf, 34 + 4, .big);
        try self.write("d");
        try self.write(&msg_lenbuf);
        try self.write(&buf);
    }


    fn setErr(self: *@This(), data: []const u8) error{ PG, OutOfMemory } {
        const allocator = self.allocator;

        // That means clearing out any previous duped error data we had
        if (self.err_data) |err_data| {
            allocator.free(err_data);
        }

        const owned = try allocator.dupe(u8, data);
        self.err_data = owned;
        self.err = Error.init(owned);
        return error.PG;
    }

    pub fn unexpectedDBMessage(self: *@This()) error{UnexpectedDBMessage} {
        self.state = .fail;
        return error.UnexpectedDBMessage;
    }

    fn canQuery(self: *const @This()) bool {
        const state = self.state;
        if (state == .idle or state == .transaction) {
            return true;
        }
        return false;
    }

    pub fn readyForQuery(self: *@This()) !void {
        const msg = try self.read();
        if (msg.type != 'Z') {
            return self.unexpectedDBMessage();
        }
    }

    // Drain the trailing ReadyForQuery after a server error so the connection
    // stays usable. Best-effort, but never swallow a cancellation.
    pub fn recoverFromError(self: *@This()) error{Canceled}!void {
        self.readyForQuery() catch |err| {
            if (err == error.Canceled) return error.Canceled;
        };
    }
};

const t = @import("t.zig");
test "Conn: auth trust (no pass)" {
    const allocator = testing.allocator;
    const io = testing.io;

    const opts: PgConfig = .{
        .host = "localhost",
        .database = "db",
        .username = "db_np",
        .application_name = "Ergo test",
        .startup_parameters = null,
    };

    var conn = try Conn.init(io, allocator, opts);
    defer conn.deinit();
    try conn.auth();
}

test "Conn: auth unknown user" {
    const allocator = testing.allocator;
    const io = testing.io;

    const opts: PgConfig = .{
        .host = "localhost",
        .database = "db",
        .username = "does_not_exist",
        .application_name = "Ergo test",
        .startup_parameters = null,
    };

    var conn = try Conn.init(io, allocator, opts);
    defer conn.deinit();
    try testing.expectError(error.PG, conn.auth());
    try testing.expectEqual(true, std.mem.find(u8, conn.err.?.message, "user \"does_not_exist\"") != null);
}

test "Conn: auth cleartext password" {
    const allocator = testing.allocator;
    const io = testing.io;

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro",
            .application_name = "Ergo test",
            .startup_parameters = null,
        };

        var conn = try Conn.init(io, allocator, opts);
        defer conn.deinit();
        try testing.expectError(error.PG, conn.auth());
        try testing.expectEqualStrings("empty password returned by client", conn.err.?.message);
    }

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro",
            .password = "wrong",
            .application_name = "Ergo test",
            .startup_parameters = null,
        };

        var conn = try Conn.init(io, allocator, opts);
        defer conn.deinit();
        try testing.expectError(error.PG, conn.auth());
        try testing.expectEqualStrings("password authentication failed for user \"db_ro\"", conn.err.?.message);
    }

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro",
            .password = "12345678",
            .application_name = "Ergo test",
            .startup_parameters = null,
        };

        var conn = try Conn.init(io, allocator, opts);
        defer conn.deinit();
        try conn.auth();
    }
}

test "Conn: auth scram-sha-256 password" {
    const allocator = testing.allocator;
    const io = testing.io;

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro_scram_sha256",
            .application_name = "Ergo test",
            .startup_parameters = null,
        };

        var conn = try Conn.init(io, allocator, opts);
        defer conn.deinit();
        try testing.expectError(error.PG, conn.auth());
        try testing.expectEqualStrings("password authentication failed for user \"db_ro_scram_sha256\"", conn.err.?.message);
    }

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro_scram_sha256",
            .password = "wrong",
            .application_name = "Ergo test",
            .startup_parameters = null,
        };

        var conn = try Conn.init(io, allocator, opts);
        defer conn.deinit();
        try testing.expectError(error.PG, conn.auth());
        try testing.expectEqualStrings("password authentication failed for user \"db_ro_scram_sha256\"", conn.err.?.message);
    }

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro_scram_sha256",
            .password = "12345678",
            .application_name = "Ergo test",
            .startup_parameters = null,
        };

        var conn = try Conn.init(io, allocator, opts);
        defer conn.deinit();
        try conn.auth();
    }
}

test "Conn: TLS required" {
    const allocator = testing.allocator;
    const io = testing.io;

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro_ssl",
            .application_name = "Ergo test",
            .startup_parameters = null,
            .tls = .off,
        };

        var c = try Conn.init(io, allocator, opts);
        defer c.deinit();
        try testing.expectError(error.PG, c.auth());
        try testing.expectEqual(true, std.mem.find(u8, c.err.?.message, "no encryption") != null);
    }

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro_ssl",
            .password = "12345678",
            .application_name = "Ergo test",
            .startup_parameters = null,
            .tls = .require,
        };

        var c = try t.connect(allocator, io, opts);
        defer c.deinit();
    }
}

test "Conn: TLS verify-full" {
    const allocator = testing.allocator;
    const io = testing.io;

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .application_name = "Ergo test",
            .startup_parameters = null,
            .tls = PgConfig.TLS{ .verify_full = null },
        };

        try testing.expectError(error.SSLCertificationVerificationError, Conn.init(io, allocator, opts));
    }

    {
        const opts: PgConfig = .{
            .host = "localhost",
            .database = "db",
            .username = "db_ro_ssl",
            .password = "12345678",
            .application_name = "Ergo test",
            .startup_parameters = null,
            .tls = PgConfig.TLS{ .verify_full = "infra/postgres/certs/ca.crt" },
        };

        var c = try t.connect(allocator, io, opts);
        defer c.deinit();
    }
}

// test "Conn: query is cancelable" {
//     const allocator = testing.allocator;
//     const io = testing.io;
//
//     const S = struct {
//         fn sleepQuery(c: *Conn) !void {
//             var result = try c.query("select pg_sleep(3)", .{});
//             result.deinit(allocator);
//         }
//     };
//
//     const opts: PgConfig = .{
//         .host = "localhost",
//         .database = "db",
//         .username = "postgres",
//         .password = "postgres",
//         .application_name = "Ergo test",
//         .startup_parameters = null,
//     };
//
//     var c = try t.connect(allocator, io, opts);
//     defer c.deinit();
//
//     // Run the query concurrently, let it reach its blocking read, then cancel.
//     var future = try io.concurrent(S.sleepQuery, .{&c});
//     try io.sleep(.fromMilliseconds(50), .awake);
//
//     const start = std.Io.Clock.Timestamp.now(io, .awake);
//     const result = future.cancel(io);
//     const elapsed_ms = start.untilNow(io).raw.toMilliseconds();
//
//     try testing.expectError(error.Timeout, result);
//     try testing.expectEqual(true, elapsed_ms < 1500); // prompt, not blocked until pg_sleep ends
//     try testing.expectEqual(State.fail, c.state);
// }
