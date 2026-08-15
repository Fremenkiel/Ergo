const std = @import("std");

const Io = std.Io;

const pg = @import("pg");

pub const ConnectionStates = enum {
    streaming,
    probing,
};

pub const ConnectionManager = struct {
    io: Io,

    mutex: Io.Mutex,
    cond_streaming: Io.Condition,
    cond_probing: Io.Condition,
    state: ConnectionStates,

    pub fn init(io: Io) @This() {
        return .{
            .io = io,
            .mutex = .init,
            .cond_streaming = .init,
            .cond_probing = .init,
            .state = .probing,
        };
    }

    pub fn runProber(self: *@This(), host: []const u8, port: u16) !void {
        var backoff_ms: u64 = 100;
        const max_backoff_ms: u64 = 10_000;

        while (true) {
            try self.mutex.lock(self.io);

            while (self.state == .streaming) {
                try self.cond_probing.wait(self.io, &self.mutex);
            }
            self.mutex.unlock(self.io);

            if (pg.stream.attemptConnection(self.io, host, port)) |stream| {
                stream.close(self.io);

                try self.mutex.lock(self.io);
                self.state = .streaming;

                self.cond_streaming.signal(self.io);
                self.mutex.unlock(self.io);

                backoff_ms = 100;
            } else |_| {
                try Io.sleep(self.io, Io.Duration{ .nanoseconds = backoff_ms * std.time.ns_per_ms }, .real);
                if (backoff_ms < max_backoff_ms) {
                    backoff_ms *= 2;
                }
            }
        }
    }

    pub fn reportDisconnectAndWait(self: *@This()) !void {
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);

        self.state = .probing;
        self.cond_probing.signal(self.io);

        while (self.state != .streaming) {
            try self.cond_streaming.wait(self.io, &self.mutex);
        }
    }
};
