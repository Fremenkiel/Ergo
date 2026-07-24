const std = @import("std");
const lib = @import("lib.zig");

const log = lib.log;
const Conn = lib.Conn;
const Result = lib.Result;
const SSLCtx = lib.SSLCtx;

const Thread = std.Thread;
const Allocator = std.mem.Allocator;

const Io = std.Io;

pub const Pool = struct {
    io: Io,
    opts: Opts,
    timeout_ms: i32,
    conns: []*Conn,
    available: usize,
    missing: usize,
    allocator: Allocator,
    mutex: Io.Mutex,
    cond: Io.Condition,
    ssl_ctx: ?*lib.SSLCtx,
    reconnector: Reconnector,

    pub const Opts = struct {
        size: u16 = 10,
        auth: Conn.AuthOpts = .{},
        connect: Conn.Opts = .{},
        timeout_ms: i32 = 10 * std.time.ms_per_s,
        connect_on_init_count: ?u16 = null,
    };

    pub const Stats = struct {
        size: usize,
        available: usize,
        missing: usize,
        in_use: usize,
    };

    pub fn init(io: Io, allocator: Allocator, opts: Opts) !*Pool {
        const pool = try allocator.create(Pool);
        const size = opts.size;
        const conns = try allocator.alloc(*Conn, size);

        // Copy every caller-provided string into our arena so the pool owns them
        // outright. Callers (including initUri) don't need to keep `opts`'s strings
        // alive past this call.
        var opts_copy = opts;
        opts_copy.auth.username = try allocator.dupe(u8, opts.auth.username);
        if (opts.auth.password) |v| opts_copy.auth.password = try allocator.dupe(u8, v);
        if (opts.auth.database) |v| opts_copy.auth.database = try allocator.dupe(u8, v);
        if (opts.auth.application_name) |v| opts_copy.auth.application_name = try allocator.dupe(u8, v);
        if (opts.connect.host) |v| opts_copy.connect.host = try allocator.dupe(u8, v);
        // Note: auth.startup_parameters (a StringHashMap) is not deep-copied; it is
        // currently unused, but if it ever gets wired up it must be owned here too.

        var ssl_ctx: ?*SSLCtx = null;
        if (comptime lib.has_openssl) {
            switch (opts.connect.tls) {
                .off => {},
                else => |tls_config| {
                    if (opts_copy.connect.host) |h| {
                        opts_copy.connect.hostz = try allocator.dupeZ(u8, h);
                    }
                    // the cert path is re-read on every (re)connect, so own it too
                    switch (tls_config) {
                        .verify_full => |path| if (path) |p| {
                            opts_copy.connect.tls = .{ .verify_full = try allocator.dupe(u8, p) };
                        },
                        else => {},
                    }
                    ssl_ctx = try lib.initializeSSLContext(tls_config);
                },
            }
        }
        errdefer lib.freeSSLContext(ssl_ctx);
        const connect_on_init_count = opts.connect_on_init_count orelse size;

        pool.* = .{
            .io = io,
            .cond = .init,
            .mutex = .init,
            .conns = conns,
            .opts = opts_copy,
            .ssl_ctx = ssl_ctx,
            .missing = 0,
            .allocator = allocator,
            .available = connect_on_init_count,
            .reconnector = Reconnector.init(pool),
            .timeout_ms = opts.timeout_ms,
        };

        var opened_connections: usize = 0;
        errdefer {
            for (0..opened_connections) |i| {
                pool.conns[i].deinit();
            }
        }

        for (0..connect_on_init_count) |i| {
            pool.conns[i] = try newConnection(pool, true);
            opened_connections += 1;
        }

        const lazy_start_count = size - connect_on_init_count;
        pool.missing = lazy_start_count;
        for (0..lazy_start_count) |_| {
            try pool.reconnector.reconnect();
        }

        return pool;
    }

    pub fn deinit(self: *Pool) void {
        self.reconnector.stop();
        const allocator = self.allocator;
        for (self.conns) |conn| {
            conn.deinit();
            allocator.destroy(conn);
        }
        allocator.free(self.conns);

        if (self.opts.connect.host) |host| {
            self.allocator.free(host);
        }

        if (self.opts.auth.database) |database| {
            self.allocator.free(database);
        }
        if (self.opts.auth.password) |password| {
            self.allocator.free(password);
        }
        self.allocator.free(self.opts.auth.username);


        lib.freeSSLContext(self.ssl_ctx);
        self.allocator.destroy(self);
    }

    pub fn acquire(self: *Pool) !*Conn {
        const conns = self.conns;
        const io = self.io;
        const deadline = @as(i64, @intCast(self.timeout_ms)) * std.time.ns_per_ms;
        const start = std.Io.Timestamp.now(io, .awake);

        try self.mutex.lock(io);
        errdefer self.mutex.unlock(io);

        const SelectResult = union(enum) { t: Io.Cancelable!void, c: Io.Cancelable!void };
        var select_buf: [1]SelectResult = undefined;

        while (true) {
            const available = self.available;
            const missing = self.missing;

            if (available == 0) {
                // Check if pool is completely exhausted
                const total_alive = self.conns.len - missing;
                if (total_alive == 0) {
                    return error.PoolExhausted;
                }

                lib.metrics.poolEmpty();

                // Calculate remaining timeout
                const now = std.Io.Timestamp.now(io, .awake);
                const elapsed = start.durationTo(now).toNanoseconds();
                if (elapsed >= deadline) {
                    return error.Timeout;
                }

                const remaining_ns = deadline - elapsed;

                var select: Io.Select(SelectResult) = .init(io, &select_buf);
                defer select.cancelDiscard();
                try select.concurrent(.t, Io.sleep, .{ io, .fromNanoseconds(remaining_ns), .awake });
                try select.concurrent(.c, Io.Condition.wait, .{ &self.cond, io, &self.mutex });

                _ = try select.await();
                continue;
            }

            const index = available - 1;
            const conn = conns[index];
            self.available = index;
            self.mutex.unlock(io);
            return conn;
        }
    }

    pub fn release(self: *Pool, conn: *Conn) void {
        var conn_to_add = conn;
        const io = self.io;

        if (conn.state != .idle) {
            lib.metrics.poolDirty();
            // conn should always be idle when being released. It's possible we can
            // recover from this (e.g. maybe we just need to read until we get a
            // ReadyForQuery), but we wouldn't want to block for too long. For now,
            // we'll just replace the connection.
            conn.deinit();
            self.allocator.destroy(conn);

            conn_to_add = newConnection(self, true) catch |err1| {
                // we failed to create the connection, track it as missing and let
                // the background reconnector try
                self.mutex.lockUncancelable(io);
                self.missing += 1;
                self.mutex.unlock(io);

                self.reconnector.reconnect() catch |err2| {
                    log.err("Re-opening connection failed ({}) and background reconnector failed to start ({})", .{ err1, err2 });
                };
                return;
            };
        }

        var conns = self.conns;
        self.mutex.lockUncancelable(io);
        const available = self.available;
        conns[available] = conn_to_add;
        self.available = available + 1;
        self.mutex.unlock(io);
        self.cond.signal(io);
    }
};

const Reconnector = struct {
    // number of connections that the pool is missing, i.e. how many need to be
    // reconnected
    count: usize,

    // when stop is called, this is set to true
    stopped: bool,

    pool: *Pool,
    mutex: Io.Mutex,

    // the thread, if any, that the monitor is running in
    thread: ?Thread,

    fn init(pool: *Pool) Reconnector {
        return .{
            .pool = pool,
            .count = 0,
            .mutex = .init,
            .stopped = false,
            .thread = null,
        };
    }

    fn run(self: *Reconnector) void {
        const pool = self.pool;
        const io = pool.io;
        const retry_delay = 2 * std.time.ns_per_s;

        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        loop: while (self.count > 0) {
            const stopped = self.stopped;
            self.mutex.unlock(io);
            if (stopped == true) {
                return;
            }

            const conn = newConnection(pool, false) catch {
                std.Io.sleep(io, .fromNanoseconds(retry_delay), .awake) catch {};
                self.mutex.lockUncancelable(io);
                continue :loop;
            };

            // Decrement missing count when successfully recreated
            pool.mutex.lockUncancelable(io);
            std.debug.assert(pool.missing > 0);
            pool.missing -= 1;
            pool.mutex.unlock(io);

            conn.release(); // inserts it into the pool
            self.mutex.lockUncancelable(io);
            self.count -= 1;
        }

        self.thread.?.detach();
        self.thread = null;
    }

    fn stop(self: *Reconnector) void {
        const io = self.pool.io;
        self.mutex.lockUncancelable(io);
        self.stopped = true;
        self.mutex.unlock(io);
        if (self.thread) |*thrd| {
            thrd.join();
        }
    }

    fn reconnect(self: *Reconnector) !void {
        const io = self.pool.io;
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.count += 1;
        if (self.thread == null) {
            self.thread = try Thread.spawn(.{ .stack_size = 1024 * 1024 }, Reconnector.run, .{self});
        }
    }
};

fn newConnection(pool: *Pool, log_failure: bool) !*Conn {
    const opts = &pool.opts;
    const allocator = pool.allocator;
    const io = pool.io;

    const conn = allocator.create(Conn) catch |err| {
        if (log_failure) log.err("connect error: {}", .{err});
        return err;
    };
    errdefer allocator.destroy(conn);

    conn.* = Conn.open(io, allocator, opts.connect) catch |err| {
        if (log_failure) log.err("connect error: {}", .{err});
        return err;
    };
    errdefer conn.deinit();

    conn.auth(opts.auth) catch |err| {
        if (log_failure) {
            if (conn.err) |pg_err| {
                log.err("connect error: {s}", .{pg_err.message});
            } else {
                log.err("connect error: {}", .{err});
            }
        }
        return err;
    };
    conn.pool = pool;
    return conn;
}

const t = lib.testing;
test "Pool" {
    var pool = try Pool.init(t.io, t.allocator, .{
        .size = 2,
        .auth = t.authOpts(.{}),
        .connect_on_init_count = 1,
    });
    defer pool.deinit();

    {
        const c1 = try pool.acquire();
        defer pool.release(c1);
        _ = try c1.exec(
            \\ drop table if exists pool_test;
            \\ create table pool_test (id int not null)
        , .{});
    }

    const t1 = try std.Thread.spawn(.{}, testPool, .{pool});
    const t2 = try std.Thread.spawn(.{}, testPool, .{pool});
    const t3 = try std.Thread.spawn(.{}, testPool, .{pool});

    t1.join();
    t2.join();
    t3.join();

    {
        const c1 = try pool.acquire();
        defer c1.release();

        const affected = try c1.exec("delete from pool_test", .{});
        try t.expectEqual(1500, affected.?);
    }
}

test "Pool: Release" {
    var pool = try Pool.init(t.io, t.allocator, .{
        .size = 2,
        .auth = .{
            .database = "postgres",
            .username = "postgres",
            .password = "postgres",
        },
    });
    defer pool.deinit();

    const c1 = try pool.acquire();
    c1.state = .query;
    pool.release(c1);
}

test "Pool: init owns its connection strings" {
    // Heap-allocate the auth strings and free them right after init to prove the
    // pool kept its own copies and doesn't depend on the caller's `opts`.
    const username = try t.allocator.dupe(u8, "postgres");
    const password = try t.allocator.dupe(u8, "postgres");
    const database = try t.allocator.dupe(u8, "postgres");
    const host = try t.allocator.dupe(u8, "127.0.0.1");

    var pool = try Pool.init(t.io, t.allocator, .{
        .size = 2,
        .auth = .{ .username = username, .password = password, .database = database },
        .connect = .{ .host = host },
    });
    defer pool.deinit();

    t.allocator.free(username);
    t.allocator.free(password);
    t.allocator.free(database);
    t.allocator.free(host);

    try forceReconnect(pool);
}

fn testPool(p: *Pool) void {
    for (0..500) |i| {
        const conn = p.acquire() catch unreachable;
        _ = conn.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
        conn.release();
    }
}

// forces release() to discard the connection and open a fresh one, exercising
// reconnect with the pool's stored auth strings.
fn forceReconnect(pool: *Pool) !void {
    const c1 = try pool.acquire();
    c1.state = .query;
    pool.release(c1);

    const c2 = try pool.acquire();
    defer pool.release(c2);
    _ = try c2.exec("select 1", .{});
}
