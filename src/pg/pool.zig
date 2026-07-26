const std = @import("std");
const openssl = @import("openssl");

const conn = @import("conn.zig");
const metrics = @import("metrics.zig");
const ssl = @import("ssl.zig");

const Conn = conn.Conn;
const Result = @import("result.zig").Result;

const Thread = std.Thread;
const mem = std.mem;
const Io = std.Io;
const testing = std.testing;

pub const Pool = struct {
    io: Io,
    opts: Opts,
    timeout_ms: i32,
    conns: []*Conn,
    available: usize,
    missing: usize,
    allocator: mem.Allocator,
    mutex: Io.Mutex,
    cond: Io.Condition,
    ssl_ctx: ?*openssl.SSL_CTX,
    reconnector: Reconnector,

    pub const Opts = struct {
        size: u16 = 10,
        connect: conn.Opts,
        timeout_ms: i32 = 10 * std.time.ms_per_s,
        connect_on_init_count: ?u16 = null,
    };

    pub const Stats = struct {
        size: usize,
        available: usize,
        missing: usize,
        in_use: usize,
    };

    pub fn init(io: Io, allocator: mem.Allocator, opts: Opts) !*Pool {
        const pool = try allocator.create(Pool);
        const size = opts.size;
        const conns = try allocator.alloc(*Conn, size);

        // Copy every caller-provided string into our arena so the pool owns them
        // outright. Callers (including initUri) don't need to keep `opts`'s strings
        // alive past this call.
        var opts_copy = opts;
        opts_copy.connect.username = try allocator.dupe(u8, opts.connect.username);
        if (opts.connect.password) |v| opts_copy.connect.password = try allocator.dupe(u8, v);
        if (opts.connect.database) |v| opts_copy.connect.database = try allocator.dupe(u8, v);
        if (opts.connect.application_name) |v| opts_copy.connect.application_name = try allocator.dupe(u8, v);
        if (opts.connect.host) |v| opts_copy.connect.host = try allocator.dupe(u8, v);
        // Note: auth.startup_parameters (a StringHashMap) is not deep-copied; it is
        // currently unused, but if it ever gets wired up it must be owned here too.

        var ssl_ctx: ?*openssl.SSL_CTX = null;
            switch (opts_copy.connect.tls) {
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
                    ssl_ctx = try ssl.initializeSSLContext(tls_config);
                },
        }
        errdefer ssl.freeSSLContext(ssl_ctx);
        const connect_on_init_count = opts_copy.connect_on_init_count orelse size;

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
            .timeout_ms = opts_copy.timeout_ms,
        };

        var opened_connections: usize = 0;
        errdefer {
            for (0..opened_connections) |i| {
                pool.conns[i].deinit();
            }
        }

        for (0..connect_on_init_count) |i| {
            pool.conns[i] = try newConnection(pool);
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
        for (self.conns) |connection| {
            connection.deinit();
            allocator.destroy(connection);
        }
        allocator.free(self.conns);

        if (self.opts.connect.host) |host| {
            self.allocator.free(host);
        }

        if (self.opts.connect.database) |database| {
            self.allocator.free(database);
        }
        if (self.opts.connect.password) |password| {
            self.allocator.free(password);
        }
        self.allocator.free(self.opts.connect.username);


        ssl.freeSSLContext(self.ssl_ctx);
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

                metrics.poolEmpty();

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
            const connnection = conns[index];
            self.available = index;
            self.mutex.unlock(io);
            return connnection;
        }
    }

    pub fn release(self: *Pool, connection: *Conn) void {
        var conn_to_add = connection;

        if (connection.state != .idle) {
            metrics.poolDirty();
            // conn should always be idle when being released. It's possible we can
            // recover from this (e.g. maybe we just need to read until we get a
            // ReadyForQuery), but we wouldn't want to block for too long. For now,
            // we'll just replace the connection.
            connection.deinit();
            self.allocator.destroy(connection);

            conn_to_add = newConnection(self) catch |err1| {
                // we failed to create the connection, track it as missing and let
                // the background reconnector try
                self.mutex.lockUncancelable(self.io);
                self.missing += 1;
                self.mutex.unlock(self.io);

                self.reconnector.reconnect() catch |err2| {
                    const err_message = std.fmt.allocPrint(self.allocator, "Re-opening connection failed ({}) and background reconnector failed to start ({})", .{ err1, err2 }) catch "Re-opening connection failed";
                    defer self.allocator.free(err_message);

                    Io.File.stderr().writeStreamingAll(self.io, err_message) catch {};
                };
                return;
            };
        }

        var conns = self.conns;
        self.mutex.lockUncancelable(self.io);
        const available = self.available;
        conns[available] = conn_to_add;
        self.available = available + 1;
        self.mutex.unlock(self.io);
        self.cond.signal(self.io);
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

            const connection = newConnection(pool) catch {
                std.Io.sleep(io, .fromNanoseconds(retry_delay), .awake) catch {};
                self.mutex.lockUncancelable(io);
                continue :loop;
            };

            // Decrement missing count when successfully recreated
            pool.mutex.lockUncancelable(io);
            std.debug.assert(pool.missing > 0);
            pool.missing -= 1;
            pool.mutex.unlock(io);

            connection.release(); // inserts it into the pool
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

fn newConnection(pool: *Pool) !*Conn {
    const opts = &pool.opts;
    const allocator = pool.allocator;
    const io = pool.io;

    const connection = try allocator.create(Conn);
    errdefer allocator.destroy(connection);

    connection.* = try Conn.open(io, allocator, opts.connect);
    errdefer connection.deinit();

    try connection.auth(opts.connect);
    connection.pool = pool;
    return connection;
}

const t = @import("t.zig");
test "Pool" {
    const allocator = testing.allocator;
    const io = testing.io;

    var pool = try Pool.init(io, allocator, .{
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
    const allocator = testing.allocator;
    const io = testing.io;

    var pool = try Pool.init(io, allocator, .{
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
    const allocator = testing.allocator;
    const io = testing.io;

    // Heap-allocate the auth strings and free them right after init to prove the
    // pool kept its own copies and doesn't depend on the caller's `opts`.
    const username = try allocator.dupe(u8, "postgres");
    const password = try allocator.dupe(u8, "postgres");
    const database = try allocator.dupe(u8, "postgres");
    const host = try allocator.dupe(u8, "127.0.0.1");

    var pool = try Pool.init(io, allocator, .{
        .size = 2,
        .auth = .{ .username = username, .password = password, .database = database },
        .connect = .{ .host = host },
    });
    defer pool.deinit();

    allocator.free(username);
    allocator.free(password);
    allocator.free(database);
    allocator.free(host);

    try forceReconnect(pool);
}

fn testPool(p: *Pool) void {
    for (0..500) |i| {
        const connection = p.acquire() catch unreachable;
        _ = connection.exec("insert into pool_test (id) values ($1)", .{i}) catch unreachable;
        connection.release();
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
