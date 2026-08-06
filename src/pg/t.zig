const std = @import("std");

const Io = std.Io;
const mem = std.mem;

const assert = std.debug.assert;

const conn = @import("conn.zig");

const PgConfig = @import("root.zig").PgConfig;
const Message = @import("reader.zig").Message;
const Reader = @import("reader.zig").Reader;
const State = @import("conn.zig").State;

pub const test_opts: PgConfig = .{
    .port = 5432,
    .host = "localhost",
    .username = "db_rw",
    .password = "12345678",
    .database = "db",
    .application_name = "Ergo",
    .timeout_ms = 10_000,
    .startup_parameters = null,
};

pub fn getRandom(io: Io) std.Random.DefaultPrng {
    var seed: u64 = undefined;
    std.Io.random(io, std.mem.asBytes(&seed));
    return std.Random.DefaultPrng.init(seed);
}

pub const Conn = struct {
    state: State,
    reader: Reader,

    pub fn init(allocator: mem.Allocator) !@This() {
        _ = allocator;
        return .{
            .state = .idle,
            .reader = undefined,
        };
    }

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        _ = self;
        _ = allocator;
    }

    pub fn unexpectedDBMessage(self: *@This()) error{UnexpectedDBMessage} {
        _ = self;
    }
    
    pub fn recoverFromError(self: *@This()) error{Canceled}!void {
        _ = self;
    }

    pub fn read(self: *@This()) !Message {
        _ = self;
    }

    pub fn readyForQuery(self: *@This()) !void {
        _ = self;
    }

    pub fn write(self: *@This(), data: []const u8) !void {
        _ = self;
        _ = data;
    }
};

pub const Stream = struct {
    allocator: mem.Allocator,

    closed: bool,

    buffer: []u8,
    writer: Io.Writer,
    reader: Io.Reader,

    pub fn init(allocator: mem.Allocator, buffer_size: ?usize) !@This() {
        const buffer = try allocator.alloc(u8, buffer_size orelse 512);
        const writer = Io.Writer.fixed(buffer);
        const reader = Io.Reader.fixed(buffer);

        return .{
            .allocator = allocator,
            .closed = false,
            .buffer = buffer,
            .writer = writer,
            .reader = reader,
        };
    }

    pub fn deinit(self: *@This()) void {
        self.allocator.free(self.buffer);
    }

    pub fn received(self: *@This()) []const u8 {
        return self.buffer[0 .. self.writer.end];
    }

    pub fn readWithTimeout(self: *@This(), buf: []u8, timeout_ms: i32) !usize {
        _ = timeout_ms;
        return try self.read(buf);
    }

    pub fn read(self: *@This(), buf: []u8) !usize {
        assert(!self.closed);

        if (buf.len == 0) {
            return 0;
        }

        const left_to_read = self.writer.end - self.reader.seek;
        const max_can_read = if (buf.len < left_to_read) buf.len else left_to_read;

        const data = try self.reader.take(max_can_read);

        @memcpy(buf[0..data.len], data);
        return data.len;
    }

    pub fn writeAll(self: *@This(), str: []const u8) !void {
        try self.writer.writeAll(str);
    }

    pub fn toString(self: *@This()) []const u8 {
        return self.buffer[0 .. self.writer.end];
    }

    pub fn reset(self: *@This()) void {
        self.writer.end = 0;
        self.reader.seek = 0;
    }

    pub fn close(self: *@This()) void {
        self.closed = true;
    }
};


pub fn connect(allocator: mem.Allocator, io: Io, opts: PgConfig) !conn.Conn {
    var c = try conn.Conn.init(io, allocator, opts);

    c.auth() catch |err| {
        if (c.err) |pg| {
            @panic(pg.message);
        }
        @panic(@errorName(err));
    };
    return c;
}
