const std = @import("std");
const lib = @import("lib.zig");
const Buffer = @import("buffer").Buffer;

const proto = lib.proto;
const types = lib.types;
const Pool = lib.Pool;
const Stmt = lib.Stmt;
const SSLCtx = lib.SSLCtx;
const Reader = lib.Reader;
const Result = lib.Result;
const Stream = lib.Stream;
const Timeout = lib.Timeout;
const QueryRow = lib.QueryRow;
const QueryRowUnsafe = lib.QueryRowUnsafe;
const has_openssl = lib.has_openssl;

const os = std.os;
const Allocator = std.mem.Allocator;
const ArenaAllocator = std.heap.ArenaAllocator;
const Io = std.Io;

pub const Conn = struct {
    // If we own the ssl context (which only happens if the connection is
    // created directly and NOT through a pool), then we have to free it
    ssl_ctx: ?*SSLCtx,

    // If we get a postgreSQL error, this will be set.
    err: ?proto.Error,

    // The underlying data for err
    err_data: ?[]const u8,

    stream: Stream,

    pool: ?*Pool = null,

    // The current transation state, this is whatever the last ReadyForQuery
    // message told us
    state: State,

    // A buffer used for writing to PG. This can grow dynamically as needed.
    buf: Buffer,

    // Used to read data from PG. Has its own buffer which can grow dynamically
    reader: Reader,

    allocator: Allocator,

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

    const State = enum {
        idle,

        // something bad happened
        fail,

        // we're doing a query
        query,

        // we're in a transaction
        transaction,
    };

    pub const Opts = struct {
        host: ?[]const u8 = null,
        port: ?u16 = null,
        writebuffer: ?u16 = null,
        readbuffer: ?u16 = null,
        result_state_size: u16 = 32,
        tls: TLS = .off,
        _hostz: ?[:0]const u8 = null,

        // tcp keepalive settings (null timer = OS default)
        keepalive: bool = true,
        keepalive_idle: ?u32 = 30,
        keepalive_interval: ?u32 = 10,
        keepalive_count: ?u32 = 3,

        pub const TLS = union(enum) {
            off: void,
            require: void,
            verify_full: ?[]const u8,
        };
    };

    pub const AuthOpts = struct {
        username: []const u8 = "postgres",
        password: ?[]const u8 = null,
        database: ?[]const u8 = null,
        timeout_ms: i32 = 10_000,
        application_name: ?[]const u8 = null,
        startup_parameters: ?std.hash_map.StringHashMap([]const u8) = null,
    };

    pub const ConnOpts = struct {
        auth: AuthOpts = .{},
        connect: Opts = .{},
    };

    pub const QueryOpts = struct {
        timeout_ms: ?i32 = null,
        column_names: bool = lib.default_column_names,

        allocator: ?Allocator = null,
        // Whether a call to result.deinit() should automatically release the
        // connection back to the pool. Meant to be used internally by pool.query()
        // and the other pool utility wrappers, but applications might find it useful
        // to use in their own helpers
        release_conn: bool = false,

        // When not null, the prepared statement will be cached and re-used
        // by subsequent queries using the same name.
        cache_name: ?[]const u8 = null,
    };

    pub fn openAndAuth(io: Io, allocator: Allocator, opts: Opts, ao: AuthOpts) !Conn {
        var conn = try open(io, allocator, opts);
        errdefer conn.deinit();

        try conn.auth(ao);
        return conn;
    }

    pub fn open(io: Io, allocator: Allocator, opts: Opts) !Conn {
        var ssl_ctx: ?*SSLCtx = null;
        switch (opts.tls) {
            .off => {},
            else => |tls_config| {
                if (comptime lib.has_openssl == false) {
                    return error.OpenSSLNotConfigured;
                }
                ssl_ctx = try lib.initializeSSLContext(tls_config);
            },
        }
        errdefer lib.freeSSLContext(ssl_ctx);
        var conn = try openWithContext(io, allocator, opts, ssl_ctx);
        conn.ssl_ctx = ssl_ctx;
        return conn;
    }

    pub fn openWithContext(io: Io, allocator: Allocator, opts: Opts, ssl_ctx: ?*SSLCtx) !Conn {
        var stream = try Stream.connect(io, allocator, opts, ssl_ctx);
        errdefer stream.close();

        const buf = try Buffer.init(allocator, @max(opts.writebuffer orelse 2048, 128));
        errdefer buf.deinit();

        const reader = try Reader.init(allocator, opts.readbuffer orelse 4096, stream);
        errdefer reader.deinit();

        const result_state = try Result.State.init(allocator, opts.result_state_size);
        errdefer result_state.deinit(allocator);

        const param_oids = try allocator.alloc(i32, opts.result_state_size);
        errdefer param_oids.deinit(allocator);

        return .{
            .err = null,
            .buf = buf,
            .ssl_ctx = null,
            .reader = reader,
            .stream = stream,
            .err_data = null,
            .state = .idle,
            .allocator = allocator,
            .io = io,
            .param_oids = param_oids,
            .result_state = result_state,
            .prepared_statements = .{},
        };
    }

    pub fn cancel(self: *Conn) void {
        self.stream.shutdown(.recv) catch {};
    }

    pub fn deinit(self: *Conn) void {
        const allocator = self.allocator;
        if (self.err_data) |err_data| {
            allocator.free(err_data);
        }
        self.buf.deinit();
        self.reader.deinit();
        allocator.free(self.param_oids);
        self.result_state.deinit(allocator);

        lib.sendTerminate(&self.stream, self.io);
        lib.freeSSLContext(self.ssl_ctx);
        self.stream.close();

        self.prepared_statements.deinit(self.allocator);
    }

    pub fn release(self: *Conn) void {
        var pool = self.pool orelse {
            self.deinit();
            return;
        };
        self.err = null;
        pool.release(self);
    }

    pub fn auth(self: *Conn, opts: AuthOpts) !void {
        if (try lib.auth.auth(self.io, &self.stream, &self.buf, &self.reader, opts)) |raw_pg_err| {
            return self.setErr(raw_pg_err);
        }

        while (true) {
            const msg = try self.read();
            switch (msg.type) {
                'Z' => return,
                'K' => {}, // TODO: BackendKeyData
                else => return self.unexpectedDBMessage(),
            }
        }
    }

    pub fn prepare(self: *Conn, sql: []const u8) !Stmt {
        return self.prepareOpts(sql, .{});
    }

    pub fn prepareOpts(self: *Conn, sql: []const u8, opts: QueryOpts) !Stmt {
        var stmt = try Stmt.init(self, opts);
        errdefer stmt.deinit();
        try stmt.prepare(sql, null);
        return stmt;
    }

    pub fn query(self: *Conn, sql: []const u8, values: anytype) !*Result {
        return self.queryOpts(sql, values, .{});
    }

    pub fn queryOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !*Result {
        if (self.canQuery() == false) {
            self.maybeRelease(opts.release_conn);
            return error.ConnectionBusy;
        }

        var cached = false;
        var stmt: Stmt = undefined;
        const name = opts.cache_name;

        if (name) |n| {
            if (self.prepared_statements.getPtr(n)) |describe| {
                cached = true;
                stmt = try Stmt.fromDescribe(self.allocator, self, describe, opts);
                errdefer stmt.deinit();

                try self.reader.startFlow(self.allocator, opts.timeout_ms);
                // Send a "SYNC" command
                try self.write(&.{ 'S', 0, 0, 0, 4 });
                stmt.buf.reset();
                try stmt.prepareForBind(@intCast(describe.param_oids.len));
            }
        }

        if (cached == false) {
            // either this isn't supposed to be cached, or it is, but we don't
            // have it in our cache
            stmt = Stmt.init(self, opts) catch |err| {
                self.maybeRelease(opts.release_conn);
                return err;
            };

            errdefer stmt.deinit();
            if (name) |n| {
                try stmt.prepare(sql, self.allocator);

                const owned_name = try self.allocator.dupe(u8, n);
                try self.prepared_statements.put(self.allocator, owned_name, .{
                    .param_oids = stmt.param_oids,
                    .result_state = stmt.result_state,
                });
            } else {
                try stmt.prepare(sql, null);
            }
        }

        {
            errdefer stmt.deinit();
            if (values.len != stmt.param_count) {
                return error.WrongNumberOfParameters;
            }

            inline for (values) |value| {
                try stmt.bind(value);
            }
        }

        return stmt.execute() catch |err| {
            stmt.deinit();
            self.maybeRelease(opts.release_conn);
            return err;
        };
    }

    // Execute a query that does not return rows
    pub fn exec(self: *Conn, sql: []const u8, values: anytype) !?i64 {
        return self.execOpts(sql, values, .{});
    }

    pub fn execOpts(self: *Conn, sql: []const u8, values: anytype, opts: QueryOpts) !?i64 {
        if (self.canQuery() == false) {
            return error.ConnectionBusy;
        }
        var buf = &self.buf;
        buf.reset();

        if (values.len == 0) {
            try self.reader.startFlow(opts.allocator, opts.timeout_ms);
            defer self.reader.endFlow() catch {
                // this can only fail in extreme conditions (OOM) and it will only impact
                // the next query (and if the app is using the pool, the pool will try to
                // recover from this anyways)
                self.state = .fail;
            };
            const simple_query = proto.Query{ .sql = sql };
            try simple_query.write(buf);
            // no longer idle, we're now in a query
            lib.metrics.query();
            self.state = .query;
            try self.write(buf.string());
        } else {
            // TODO: there's some optimization opportunities here, since we know
            // we aren't expecting any result. We don't have to ask PG to DESCRIBE
            // the returned columns (there should be none). This is very significant
            // as it would remove 1 back-and-forth. We could just:
            //    Parse + Bind + Exec + Sync
            // Instead of having to do:
            //    Parse + Describe + Sync  ... read response ...  Bind + Exec + Sync
            const result = try self.queryOpts(sql, values, opts);
            result.deinit();
        }

        // affected can be null, so we need a separate boolean to track if we
        // actually have a response.
        var affected: ?i64 = null;
        while (true) {
            const msg = self.read() catch |err| {
                if (err == error.PG) try self.recoverFromError();
                return err;
            };
            switch (msg.type) {
                'C' => {
                    const cc = try proto.CommandComplete.parse(msg.data);
                    affected = cc.rowsAffected();
                },
                'Z' => return affected,
                'T' => affected = 0,
                'D' => affected = (affected orelse 0) + 1,
                else => return self.unexpectedDBMessage(),
            }
        }
    }

    pub fn begin(self: *Conn) !void {
        self.state = .transaction;
        _ = try self.execOpts("begin", .{}, .{});
    }

    pub fn commit(self: *Conn) !void {
        _ = try self.execOpts("commit", .{}, .{});
    }

    // We don't use `execOpts` here because rollback can be called at any point
    // and we want to send this command even if the conn is in a fail state.
    // So we issue the rollback, no matter what state we're in.
    // It's also possible rollback was called while we were reading results,
    // so we need to keep reading replies until we get a ready to query state,
    // just skipping over any data rows or any other in-flight messages there
    // might be.
    pub fn rollback(self: *Conn) !void {
        return self.execIgnoringState("rollback");
    }

    pub fn tryRollback(self: *Conn) !void {
        if (self.state != .idle) {
            try self.rollback();
        }
    }

    pub fn execIgnoringState(self: *Conn, sql: []const u8) !void {
        var buf = &self.buf;
        buf.reset();

        const state = self.state;

        const simple_query = proto.Query{ .sql = sql };
        try simple_query.write(buf);
        try self.write(buf.string());
        while (true) {
            const msg = self.read() catch |err| {
                if (state != .fail and err == error.PG) try self.recoverFromError();
                return err;
            };
            switch (msg.type) {
                'Z' => return,
                'C', 'T', 'D', 'n' => {},
                else => return self.unexpectedDBMessage(),
            }
        }
    }

    pub fn deallocate(self: *Conn, cache_name: []const u8) !void {
        const allocator = self.allocator;
        const sql = try std.fmt.allocPrint(allocator, "deallocate {s}", .{cache_name});
        defer allocator.free(sql);
        _ = try self.execOpts(sql, .{}, .{});
    }

    // Should not be called directly
    pub fn peekForError(self: *Conn) !void {
        const data = (try self.reader.peekForError()) orelse return;
        try self.readyForQuery();
        return self.setErr(data);
    }

    // Should not be called directly
    pub fn read(self: *Conn) !lib.Message {
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
                'E' => return self.setErr(msg.data),
                else => return msg,
            }
        }
    }

    pub fn write(self: *Conn, data: []const u8) !void {
        self.stream.writeAll(data) catch |err| {
            self.state = .fail;
            return err;
        };
    }

    pub fn sendStandbyStatusUpdate(self: *Conn, last_lsn: u64, server_timestamp: i64) !void {
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


    fn setErr(self: *Conn, data: []const u8) error{ PG, OutOfMemory } {
        const allocator = self.allocator;

        // The proto.Error that we're about to create is going to reference data.
        // But data is owned by our Reader and its lifetime doesn't necessarily match
        // what we want here. So we're going to dupe it and make the connection own
        // the data so it can tie its lifecycle to the error.

        // That means clearing out any previous duped error data we had
        if (self.err_data) |err_data| {
            allocator.free(err_data);
        }

        const owned = try allocator.dupe(u8, data);
        self.err_data = owned;
        self.err = proto.Error.parse(owned);
        return error.PG;
    }

    pub fn unexpectedDBMessage(self: *Conn) error{UnexpectedDBMessage} {
        self.state = .fail;
        return error.UnexpectedDBMessage;
    }

    fn canQuery(self: *const Conn) bool {
        const state = self.state;
        if (state == .idle or state == .transaction) {
            return true;
        }
        return false;
    }

    inline fn maybeRelease(self: *Conn, rel: bool) void {
        if (rel) {
            self.release();
        }
    }

    // should not be called directly
    pub fn readyForQuery(self: *Conn) !void {
        const msg = try self.read();
        if (msg.type != 'Z') {
            return self.unexpectedDBMessage();
        }
    }

    // Drain the trailing ReadyForQuery after a server error so the connection
    // stays usable. Best-effort, but never swallow a cancellation.
    pub fn recoverFromError(self: *Conn) error{Canceled}!void {
        self.readyForQuery() catch |err| {
            if (err == error.Canceled) return error.Canceled;
        };
    }
};

const t = lib.testing;
test "Conn: auth trust (no pass)" {
    var conn = try Conn.open(t.io, t.allocator, .{});
    defer conn.deinit();
    try conn.auth(.{ .username = "db_np", .database = "postgres" });
}

test "Conn: auth unknown user" {
    var conn = try Conn.open(t.io, t.allocator, .{});
    defer conn.deinit();
    try t.expectError(error.PG, conn.auth(.{ .username = "does_not_exist" }));
    try t.expectEqual(true, std.mem.find(u8, conn.err.?.message, "user \"does_not_exist\"") != null);
}

test "Conn: auth cleartext password" {
    {
        var conn = try Conn.open(t.io, t.allocator, .{});
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "db_ro" }));
        try t.expectString("empty password returned by client", conn.err.?.message);
    }

    {
        var conn = try Conn.open(t.io, t.allocator, .{});
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "db_ro", .password = "wrong" }));
        try t.expectString("password authentication failed for user \"db_ro\"", conn.err.?.message);
    }

    {
        var conn = try Conn.open(t.io, t.allocator, .{});
        defer conn.deinit();
        try conn.auth(.{ .username = "db_ro", .password = "12345678", .database = "postgres" });
    }
}

test "Conn: auth scram-sha-256 password" {
    {
        var conn = try Conn.open(t.io, t.allocator, .{});
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "db_ro_scram_sha256" }));
        try t.expectString("password authentication failed for user \"db_ro_scram_sha256\"", conn.err.?.message);
    }

    {
        var conn = try Conn.open(t.io, t.allocator, .{});
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "db_ro_scram_sha256", .password = "wrong" }));
        try t.expectString("password authentication failed for user \"db_ro_scram_sha256\"", conn.err.?.message);
    }

    {
        var conn = try Conn.open(t.io, t.allocator, .{});
        defer conn.deinit();
        try conn.auth(.{ .username = "db_ro_scram_sha256", .password = "12345678", .database = "postgres" });
    }
}

test "Conn: exec rowsAffected" {
    var c = try t.connect(.{});
    defer c.deinit();

    {
        const n = try c.exec("insert into simple_table values ('exec_insert_a'), ('exec_insert_b')", .{});
        try t.expectEqual(2, n.?);
    }

    {
        const n = try c.exec("update simple_table set value = 'exec_insert_a' where value = 'exec_insert_a'", .{});
        try t.expectEqual(1, n.?);
    }

    {
        const n = try c.exec("delete from simple_table where value like 'exec_insert%'", .{});
        try t.expectEqual(2, n.?);
    }

    {
        try t.expectEqual(null, try c.exec("begin", .{}));
        try t.expectEqual(null, try c.exec("end", .{}));
    }
}

test "Conn: exec with values rowsAffected" {
    var c = try t.connect(.{});
    defer c.deinit();

    {
        const n = try c.exec("insert into simple_table values ($1), ($2)", .{ "exec_insert_args_a", "exec_insert_args_b" });
        try t.expectEqual(2, n.?);
    }
}

test "Conn: exec query that returns rows" {
    var c = try t.connect(.{});
    defer c.deinit();
    _ = try c.exec("insert into simple_table values ('exec_sel_1'), ('exec_sel_2')", .{});
    try t.expectEqual(0, c.exec("select * from simple_table where value = 'none'", .{}));
    try t.expectEqual(2, c.exec("select * from simple_table where value like $1", .{"exec_sel_%"}));
}

test "PG: query column names" {
    var c = try t.connect(.{});
    defer c.deinit();
    {
        var result = try c.query("select 1 as id, 'leto' as name", .{});
        try t.expectEqual(0, result.column_names.len);
        try result.drain();
        result.deinit();
    }

    {
        var result = try c.queryOpts("select 1 as id, 'leto' as name", .{}, .{ .column_names = true });
        defer result.deinit();
        try t.expectEqual(2, result.column_names.len);
        try t.expectString("id", result.column_names[0]);
        try t.expectString("name", result.column_names[1]);
    }
}

test "PG: eager error" {
    var c = try t.connect(.{});
    defer c.deinit();

    {
        // Some errors happen when the prepared statement is executed
        try t.expectError(error.PG, c.query("select * from invalid", .{}));
        try t.expectString("relation \"invalid\" does not exist", c.err.?.message);
    }

    {
        // some errors only happen when the result is read
        try c.begin();
        defer c.rollback() catch {};
        const sql = "create temp table test1 (id int) on commit drop";
        _ = try c.exec(sql, .{});
        try t.expectError(error.PG, c.query(sql, .{}));
    }
}

// https://github.com/karlseguin/pg.zig/issues/44
test "PG: eager error conn state" {
    var pool = try lib.Pool.init(t.io, t.allocator, .{ .size = 1, .auth = t.authOpts(.{}) });
    defer pool.deinit();

    {
        var c = try pool.acquire();
        defer c.release();

        // duplicate it
        _ = try c.exec("insert into all_types (id) values ($1)", .{2000});
        try t.expectError(error.PG, c.exec("insert into all_types (id) values ($1)", .{2000}));
    }

    {
        // only 1 connection in our pool, so the fact that the above fails and
        // this one succeeds, means we're properly handling the failure
        var c = try pool.acquire();
        defer c.release();
        _ = try c.exec("insert into all_types (id) values ($1)", .{2001});
    }
}

test "Conn: TLS required" {
    {
        var conn = try Conn.open(t.io, t.allocator, .{ .tls = .off });
        defer conn.deinit();
        try t.expectError(error.PG, conn.auth(.{ .username = "db_ro_ssl" }));
        try t.expectEqual(true, std.mem.find(u8, conn.err.?.message, "no encryption") != null);
    }

    {
        var conn = try t.connect(.{ .tls = Conn.Opts.TLS.require, .username = "db_ro_ssl", .password = "12345678" });
        defer conn.deinit();
    }
}

test "Conn: TLS verify-full" {
    try t.expectError(error.SSLCertificationVerificationError, Conn.open(t.io, t.allocator, .{ .tls = .{ .verify_full = null } }));

    {
        var conn = try t.connect(.{ .tls = Conn.Opts.TLS{ .verify_full = "infra/postgres/certs/ca.crt" }, .username = "db_ro_ssl", .password = "12345678" });
        defer conn.deinit();
    }
}

test "Conn: query is cancelable" {
    const S = struct {
        fn sleepQuery(c: *Conn) !void {
            var result = try c.query("select pg_sleep(3)", .{});
            result.deinit();
        }
    };

    var conn = try t.connect(.{});
    defer conn.deinit();

    // Run the query concurrently, let it reach its blocking read, then cancel.
    var future = try t.io.concurrent(S.sleepQuery, .{&conn});
    try t.io.sleep(.fromMilliseconds(50), .awake);

    const start = std.Io.Clock.Timestamp.now(t.io, .awake);
    const result = future.cancel(t.io);
    const elapsed_ms = start.untilNow(t.io).raw.toMilliseconds();

    try t.expectError(error.Canceled, result);
    try t.expectEqual(true, elapsed_ms < 1500); // prompt, not blocked until pg_sleep ends
    try t.expectEqual(Conn.State.fail, conn.state);
    try t.expectError(error.ConnectionBusy, conn.exec("select 1", .{}));
}

fn expectNumeric(numeric: types.Numeric, expected: []const u8) !void {
    var strbuf: [50]u8 = undefined;
    try t.expectString(expected, try numeric.toString(&strbuf));

    const a = try t.allocator.alloc(u8, numeric.estimatedStringLen());
    defer t.allocator.free(a);
    try t.expectString(expected, try numeric.toString(a));

    if (std.mem.eql(u8, expected, "nan")) {
        try t.expectEqual(true, std.math.isNan(numeric.toFloat()));
    } else if (std.mem.eql(u8, expected, "inf")) {
        try t.expectEqual(true, std.math.isInf(numeric.toFloat()));
    } else if (std.mem.eql(u8, expected, "-inf")) {
        try t.expectEqual(true, std.math.isNegativeInf(numeric.toFloat()));
    } else {
        try t.expectDelta(try std.fmt.parseFloat(f64, expected), numeric.toFloat(), 0.000001);
    }
}

const DummyStruct = struct {
    id: i32,
    name: []const u8,
};

const DummyEnum = enum {
    val1,
    val2,
};
