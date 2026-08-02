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
const Result = @import("result.zig").Result;
const Stream = @import("stream.zig").Stream;
const Stmt = @import("stmt.zig").Stmt;

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

    // Holds information describing the query that we're executing. If the query
    // returns more columns than an appropriately sized ResultState is created as
    // needed.
    result_state: Result.State,

    // Holds information describing the parameters that PG is expecting. If the
    // query has more parameters, than an appropriately sized one is created.
    // This is separate from result_state because:
    //   (a) they are populated separately
    //   (b) have distinct lifetimes
    //   (c) they likely have different lengths;
    param_oids: []i32,

    // cache_name => data necessary to re-execute previously prepared statement.
    prepared_statements: std.hash_map.StringHashMapUnmanaged(Stmt.Describe),

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

        const result_state = try Result.State.init(allocator, opts.result_state_size);
        errdefer result_state.deinit(allocator);

        const param_oids = try allocator.alloc(i32, opts.result_state_size);
        errdefer param_oids.free(allocator);

        return .{
            .err = null,
            .ssl_ctx = ssl_ctx,
            .reader = reader,
            .stream = stream,
            .err_data = null,
            .state = .idle,
            .allocator = allocator,
            .io = io,
            .param_oids = param_oids,
            .result_state = result_state,
            .prepared_statements = .{},
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
        self.allocator.free(self.param_oids);
        self.result_state.deinit(self.allocator);

        sendTerminate(&self.stream, self.io);
        ssl.freeSSLContext(self.ssl_ctx);
        self.stream.deinit(self.allocator);

        self.prepared_statements.deinit(self.allocator);
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

    pub fn query(self: *@This(), sql: []const u8, opts: QueryOpts) !*Result {
        if (self.canQuery() == false) {
            return error.ConnectionBusy;
        }

        var cached = false;
        var stmt: Stmt = undefined;
        defer stmt.deinit();

        const name = opts.cache_name;

        const buf = try self.allocator.alloc(u8, 1028);
        defer self.allocator.free(buf);

        if (name) |n| {
            if (self.prepared_statements.getPtr(n)) |describe| {
                cached = true;
                stmt = try Stmt.fromDescribe(self.allocator, self, describe, opts);
                errdefer stmt.endStmt();

                try self.reader.startFlow(opts.timeout_ms);
                // Send a "SYNC" command
                try self.write(&.{ 'S', 0, 0, 0, 4 });
                try stmt.prepareForBind(@intCast(describe.param_oids.len));
            }
        }

        if (cached == false) {
            // either this isn't supposed to be cached, or it is, but we don't
            // have it in our cache
            stmt = Stmt.init(self.allocator, self, opts) catch |err| {
                return err;
            };
            errdefer stmt.endStmt();

            if (name) |n| {
                try stmt.prepare(sql);

                const owned_name = try self.allocator.dupe(u8, n);
                try self.prepared_statements.put(self.allocator, owned_name, .{
                    .param_oids = stmt.param_oids,
                    .result_state = stmt.result_state,
                });
            } else {
                stmt.prepare(sql) catch |err| {
                    if (self.err_data) |err_msg| {
                        std.debug.print("Error: {s}\n", .{err_msg});
                    }

                    return err;
                };
            }
        }

        return stmt.execute() catch |err| {
            stmt.endStmt();
            return err;
        };
    }

    pub fn peekForError(self: *@This()) !void {
        const data = (try self.reader.peekForError()) orelse return;
        try self.readyForQuery();
        return self.setErr(data);
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
        .startup_parameters = .init(allocator),
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
        .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
        };

        var conn = try Conn.init(io, allocator, opts);
        defer conn.deinit();
        try conn.auth();
    }
}

test "PG: query column names" {
    const allocator = testing.allocator;
    const io = testing.io;

    const opts: PgConfig = .{
        .host = "localhost",
        .database = "db",
        .username = "db_rw",
        .password = "12345678",
        .application_name = "Ergo test",
        .startup_parameters = .init(allocator),
    };

    var c = try t.connect(allocator, io, opts);
    defer c.deinit();
    {
        var result = try c.query(
        \\ SELECT
        \\   a.attname AS column_name,
        \\   COALESCE((SELECT string_agg(c.contype::text, '') FROM pg_constraint c WHERE a.attnum = ANY(c.conkey) AND c.conrelid = a.attrelid), '') AS constraint_types
        \\ FROM pg_attribute a
        \\ WHERE a.attrelid = 'addresses'::regclass AND a.attnum > 0 AND NOT a.attisdropped
        \\ ORDER BY a.attnum;
        , .{ .column_names = true });
        try testing.expectEqual(2, result.column_names.len);
        try result.drain();
        result.deinit(allocator);
    }

    {
        var result = try c.query("select 1 as id, 'leto' as name", .{ .column_names = true });
        defer result.deinit(allocator);
        try testing.expectEqual(2, result.column_names.len);
        try testing.expectEqualStrings("id", result.column_names[0]);
        try testing.expectEqualStrings("name", result.column_names[1]);
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
            .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
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
            .startup_parameters = .init(allocator),
            .tls = PgConfig.TLS{ .verify_full = "infra/postgres/certs/ca.crt" },
        };

        var c = try t.connect(allocator, io, opts);
        defer c.deinit();
    }
}

test "Conn: query is cancelable" {
    const allocator = testing.allocator;
    const io = testing.io;

    const S = struct {
        fn sleepQuery(c: *Conn) !void {
            var result = try c.query("select pg_sleep(3)", .{});
            result.deinit(allocator);
        }
    };

    const opts: PgConfig = .{
        .host = "localhost",
        .database = "db",
        .username = "postgres",
        .password = "postgres",
        .application_name = "Ergo test",
        .startup_parameters = .init(allocator),
    };

    var c = try t.connect(allocator, io, opts);
    defer c.deinit();

    // Run the query concurrently, let it reach its blocking read, then cancel.
    var future = try io.concurrent(S.sleepQuery, .{&c});
    try io.sleep(.fromMilliseconds(50), .awake);

    const start = std.Io.Clock.Timestamp.now(io, .awake);
    const result = future.cancel(io);
    const elapsed_ms = start.untilNow(io).raw.toMilliseconds();

    try testing.expectError(error.Timeout, result);
    try testing.expectEqual(true, elapsed_ms < 1500); // prompt, not blocked until pg_sleep ends
    try testing.expectEqual(State.fail, c.state);
}
