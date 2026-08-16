const std = @import("std");

const Io = std.Io;
const mem = std.mem;

const pg = @import("pg");

pub const ConnectionStates = enum {
    streaming,
    probing,
};

pub const ConnectionManager = struct {
    allocator: mem.Allocator,
    io: Io,

    opts: pg.PgConfig,

    mutex: Io.Mutex,
    cond_streaming: Io.Condition,
    cond_probing: Io.Condition,
    state: ConnectionStates,

    pub fn init(allocator: mem.Allocator, io: Io, opts: pg.PgConfig) @This() {
        return .{
            .allocator = allocator,
            .io = io,
            .opts = opts,
            .mutex = .init,
            .cond_streaming = .init,
            .cond_probing = .init,
            .state = .probing,
        };
    }

    fn sleep(self: *@This(), backoff_ms: u64, max_backoff_ms: u64) !u64 {
        try Io.sleep(self.io, Io.Duration{ .nanoseconds = backoff_ms * std.time.ns_per_ms }, .real);
        if (backoff_ms < max_backoff_ms) {
            return backoff_ms * 2;
        }
        return backoff_ms;
    }

    pub fn runProber(self: *@This()) !void {
        var backoff_ms: u64 = 100;
        const max_backoff_ms: u64 = 10_000;

        while (true) {
            try self.mutex.lock(self.io);

            while (self.state == .streaming) {
                try self.cond_probing.wait(self.io, &self.mutex);
            }
            self.mutex.unlock(self.io);

            var conn = pg.Conn.init(self.io, self.allocator, self.opts) catch {
                backoff_ms = try self.sleep(backoff_ms, max_backoff_ms);
                continue;
            };
            errdefer conn.deinit();

            conn.auth() catch {
                backoff_ms = try self.sleep(backoff_ms, max_backoff_ms);
                continue;
            };

            conn.deinit();

            try self.mutex.lock(self.io);
            self.state = .streaming;

            self.cond_streaming.signal(self.io);
            self.mutex.unlock(self.io);

            backoff_ms = 100;
        }
    }

    pub fn reportDisconnectAndWait(self: *@This()) !void {
        std.log.debug("trigger disconnect\n", .{});
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        self.state = .probing;
        self.cond_probing.signal(self.io);

        while (self.state != .streaming) {
            std.log.debug("state: {s}\n", .{@tagName(self.state)});
            try self.cond_streaming.wait(self.io, &self.mutex);
        }
    }
};
