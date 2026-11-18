const std = @import("std");
const Io = std.Io;
const File = Io.File;
const fmt = std.fmt;
const Environ = std.process.Environ;

const NofityError = fmt.ParseIntError || File.Writer.Error;

pub const State = enum(u8) {
    Ready,
    Failed,
    Exited,
};

pub fn notify(
io: Io,
env_map: *Environ.Map,
state: State
) !void {
    const fd_string = env_map.get("ERGO_READINESS_FD") orelse return;

    const fd = try fmt.parseInt(std.posix.fd_t, fd_string, 10);

    const file: File = .{
        .handle = fd,
        .flags = .{ .nonblocking = false },
    };
    defer file.close(io);

    switch (state) {
        .Ready => try file.writeStreamingAll(io, &.{0x01}),
        .Exited => try file.writeStreamingAll(io, &.{0x02}),
        .Failed => try file.writeStreamingAll(io, &.{0x03}),
    }
}
