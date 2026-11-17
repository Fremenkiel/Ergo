const std = @import("std");
const Io = std.Io;
const File = Io.File;
const fmt = std.fmt;
const Environ = std.process.Environ;

const NofityError = fmt.ParseIntError || File.Writer.Error;

pub fn notify(
io: Io,
env_map: *Environ.Map
) !void {
    const fd_string = env_map.get("ERGO_READINESS_FD") orelse return;

    const fd = try fmt.parseInt(std.posix.fd_t, fd_string, 10);

    const file: File = .{
        .handle = fd,
        .flags = .{ .nonblocking = false },
    };
    defer file.close(io);

    try file.writeStreamingAll(io, &.{0x01});
}
