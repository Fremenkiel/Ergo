const std = @import("std");

pub fn main() !void {
    var fds = [_]std.posix.pollfd{
        .{
            .fd = 0,
            .events = std.posix.POLL.IN,
            .revents = 0,
        }
    };
    const n = try std.posix.poll(&fds, 0);
    std.debug.print("poll returned {}\n", .{n});
}
