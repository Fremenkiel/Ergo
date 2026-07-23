const std = @import("std");
const Io = std.Io;
const posix = std.posix;

const lib = @import("lib.zig");
const Buffer = @import("buffer").Buffer;

const Stream = lib.Stream;
const Reader = lib.Reader;

pub const StreamController = struct {
    io: Io,
    stream: Stream,
    cancel_pipe: [2]posix.fd_t,

    pub fn init(io: Io, stream: Stream) !StreamController {

        return .{
            .io = io,
            .stream = stream,
            .cancel_pipe = try Io.Threaded.pipe2(.{ .CLOEXEC = true })
        };
    }

    pub fn deinit(self: *@This()) void {
        if (self.cancel_pipe[0] != -1) Io.Threaded.closeFd(self.cancel_pipe[0]);
        if (self.cancel_pipe[0] != self.cancel_pipe[1]) Io.Threaded.closeFd(self.cancel_pipe[1]);
    }

    pub fn cancel(self: *@This()) !void {
        var buf: [1]u8 = undefined;
        const write_file = std.Io.File{ .handle = self.cancel_pipe[0] };

        var writer = write_file.writer(self.io, &buf);
        var w = &writer.interface;

        try w.writeAll(&[_]u8{1});
    }

    pub fn read(self: *@This(), buffer: []u8) !usize {
        return try self.stream.read(buffer);
    }

    pub fn readWithTimeout(self: *@This(), buffer: []u8, timeout_ms: i32) !usize {
        var fds = [_]std.posix.pollfd{
            .{ .fd = self.stream.stream.socket.handle, .events = std.posix.POLL.IN, .revents = 0 },
            .{ .fd = self.cancel_pipe[0], .events = std.posix.POLL.IN, .revents = 0 },
        };

        const ready_count = try posix.poll(&fds, timeout_ms);

        if (ready_count == 0) {
            return error.Timeout;
        }

        if ((fds[1].revents & posix.POLL.IN) != 0) {
            var dummy: [1]u8 = undefined;
            _ = try posix.read(self.cancel_pipe[0], &dummy);

            return error.Cancelled;
        }

        if ((fds[0].revents & posix.POLL.IN) != 0) {
            return try posix.read(self.stream.stream.socket.handle, buffer);
        }

        return error.UnexpectedPollEvent;
    }
};

test "cancel stream while read" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var stream = try Stream.connect(io, allocator, .{ .port = 5432, .host = "localhost" }, null);
    errdefer stream.close();

    var controller = try StreamController.init(io, stream);
    errdefer controller.deinit();

    const buf = try Buffer.init(allocator, 2048);
    errdefer buf.deinit();

    var reader = try Reader.init(allocator, 4096, controller);
    errdefer reader.deinit();

    _ = try reader.nextWithTimeout(2500);
    std.debug.print("Just testing", .{});
}
