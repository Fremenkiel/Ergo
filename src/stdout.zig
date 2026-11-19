const std = @import("std");
const Io = std.Io;
const File = Io.File;
const fmt = std.fmt;
const Environ = std.process.Environ;

pub const State = enum(u8) {
    ready,
    failed,
    exited,
};

const StdoutError = fmt.ParseIntError;

const WriteStateError = File.Writer.Error;

pub const Stdout = struct {
    io: Io,
    file: File,

    pub fn init(io: Io, env_map: *Environ.Map) StdoutError!?@This() {
        const fd_string = env_map.get("ERGO_READINESS_FD") orelse return null;

        const fd = try fmt.parseInt(std.posix.fd_t, fd_string, 10);

        const file: File = .{
            .handle = fd,
            .flags = .{ .nonblocking = false },
        };

        return .{
            .io = io,
            .file = file,
        };
    }

    pub fn deinit(self: *@This()) void {
        self.file.close(self.io);
    }

    pub fn writeState(
    self: *@This(),
    state: State
    ) WriteStateError!void {
        try self.file.writeStreamingAll(self.io, &.{@backingInt(state)});
    }
};
