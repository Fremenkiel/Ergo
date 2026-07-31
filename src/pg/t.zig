const std = @import("std");

const Io = std.Io;
const mem = std.mem;

const Conn = @import("conn.zig").Conn;
const Opts = @import("conn.zig").Opts;

const test_opts: Opts = .{
    .port = 5432,
    .host = "localhost",
    .username = "postgres",
    .password = "postgres",
    .database = "db",
    .application_name = "Ergo",
    .timeout_ms = 10_000,
    .startup_parameters = undefined,
};

// std.testing.expectEqual won't coerce expected to actual, which is a problem
// when expected is frequently a comptime.
// https://github.com/ziglang/zig/issues/4437
pub fn expectDelta(expected: anytype, actual: anytype, delta: anytype) !void {
    std.testing.expectEqual(true, expected - delta <= actual) catch |err| {
        std.debug.print("{d} !~ {d}", .{ expected, actual });
        return err;
    };
    std.testing.expectEqual(true, expected + delta >= actual) catch |err| {
        std.debug.print("{d} !~ {d}", .{ expected, actual });
        return err;
    };
}

pub fn getRandom(io: Io) std.Random.DefaultPrng {
    var seed: u64 = undefined;
    std.Io.random(io, std.mem.asBytes(&seed));
    return std.Random.DefaultPrng.init(seed);
}

pub const Stream = struct {
    allocator: mem.Allocator,
    closed: bool,
    read_index: usize,
    socket: c_int = 0,
    to_read: std.ArrayList(u8),
    received_array: std.ArrayList(u8),

    pub fn init(allocator: mem.Allocator) *Stream {
        const s = allocator.create(Stream) catch unreachable;
        s.* = .{
            .allocator = allocator,
            .closed = false,
            .read_index = 0,
            .to_read = .empty,
            .received_array = .empty,
        };
        return s;
    }

    pub fn deinit(self: *Stream) void {
        self.to_read.deinit(self.allocator);
        self.received_array.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn reset(self: *Stream) void {
        self.read_index = 0;
        self.to_read.clearRetainingCapacity();
        self.received_array.clearRetainingCapacity();
    }

    pub fn received(self: *Stream) []const u8 {
        return self.received_array.items;
    }

    pub fn add(self: *Stream, value: []const u8) void {
        self.to_read.appendSlice(self.allocator, value) catch unreachable;
    }

    pub fn readWithTimeout(self: *@This(), buf: []u8, timeout_ms: i32) !usize {
        _ = timeout_ms;
        return try self.read(buf);
    }

    pub fn read(self: *Stream, buf: []u8) !usize {
        std.debug.assert(!self.closed);

        const read_index = self.read_index;
        const items = self.to_read.items;

        if (read_index == items.len) {
            return 0;
        }
        if (buf.len == 0) {
            return 0;
        }

        // let's fragment this message
        const left_to_read = items.len - read_index;
        const max_can_read = if (buf.len < left_to_read) buf.len else left_to_read;

        const to_read = max_can_read;
        var data = items[read_index..(read_index + to_read)];
        if (data.len > buf.len) {
            // we have more data than we have space in buf (our target)
            // we'll give it when it can take
            data = data[0..buf.len];
        }
        self.read_index = read_index + data.len;

        @memcpy(buf[0..data.len], data);
        return data.len;
    }

    // store messages that are written to the stream
    pub fn writeAll(self: *Stream, data: []const u8) !void {
        self.received_array.appendSlice(self.allocator, data) catch unreachable;
    }

    pub fn close(self: *Stream) void {
        self.closed = true;
    }
};

pub fn connect(allocator: mem.Allocator, io: Io, opts: Opts) !Conn {
    var c = try Conn.init(io, allocator, opts);

    c.auth() catch |err| {
        if (c.err) |pg| {
            @panic(pg.message);
        }
        @panic(@errorName(err));
    };
    return c;
}

pub fn fail(c: Conn, err: anyerror) !void {
    if (c.err) |pg_err| {
        std.debug.print("PG ERROR: {s}\n", .{pg_err.message});
    }
    return err;
}
