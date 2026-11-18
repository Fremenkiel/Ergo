const std = @import("std");
const Io = std.Io;
const File = Io.File;
const mem = std.mem;
const Allocator = mem.Allocator;
const process = std.process;
const Child = process.Child;
const Environ = process.Environ;
const posix = std.posix;

const SpawnError = ReadinessPipeError || Allocator.Error || process.SpawnError;

const WaitUntilReadyError = error{
NoReadinessChannel,
InvalidReadySignal,
InvalidState,
ReadinessChannelClosed,
ReadinessTimeout,
} || File.ReadStreamingError || Io.Cancelable;

const TerminateError = error{
ChildNotStartedError,
} || posix.KillError || Child.WaitError;

pub const TestProcess = struct {
    io: Io,

    child: Child,
    ready_reader: ?File,

    state: State = .starting,

    pub const State = enum {
        starting,
        ready,
        exited,
        failed,
    };

    pub const Readiness = enum {
        pipe,
        none,
    };

    pub const Options = struct {
        argv: []const []const u8,
        readiness: Readiness = .none,
        cwd: Child.Cwd = .inherit,
    };

    pub const WaitOptions = struct {
        timeout_ms: i32 = 10_000,
    };

    pub fn spawn(allocator: Allocator, io: Io, env: *Environ.Map, options: Options) SpawnError!@This() {
        const pipe = try ReadinessPipe.init();
        errdefer pipe.reader.close(io);

        defer pipe.writer.close(io);

        const fd_handle = try allocator.print("{d}", .{ pipe.writer.handle });
        defer allocator.free(fd_handle);

        try env.put("ERGO_READINESS_FD", fd_handle);

        const child = try process.spawn(io, .{
            .argv = options.argv,
            .cwd = options.cwd,
            .environ_map = env,
            .stdout = .inherit,
            .stderr = .inherit,
        });

        return .{
            .io = io,
            .child = child,
            .ready_reader = pipe.reader,
        };
    }

    pub fn waitUntilReady(self: *@This(), options: WaitOptions) WaitUntilReadyError!void {
        if (self.state == .ready) return;
        if (self.state != .starting) return WaitUntilReadyError.InvalidState;

        const file = self.ready_reader orelse return WaitUntilReadyError.NoReadinessChannel;
        defer {
            file.close(self.io);
            self.ready_reader = null;
        }

        errdefer self.state = .failed;

        const Result = union(enum) {
            ready: File.ReadStreamingError!u8,
            timeout: Io.Cancelable!void,
        };

        var results: [2]Result = undefined;
        var select = Io.Select(Result).init(self.io, &results);

        defer select.cancelDiscard();

        select.async(.ready, readReadyByte, .{ file, self.io });
        select.async(.timeout, Io.sleep, .{ self.io, Io.Duration.fromMilliseconds(options.timeout_ms), .awake });

        const result = try select.await();

        switch (result) {
            .ready => |read_result| {
                const byte = read_result catch |err|
                switch (err) {
                    File.ReadStreamingError.EndOfStream => return WaitUntilReadyError.ReadinessChannelClosed,
                    else => return err,
                };

                if (byte != 0x01) return WaitUntilReadyError.InvalidReadySignal;

                self.state = .ready;
            },
            .timeout => |sleep_result| {
                try sleep_result;
                return WaitUntilReadyError.ReadinessTimeout;
            },
        }

        self.state = .ready;
    }

    fn readReadyByte(
    file: Io.File,
    io: Io,
    ) Io.File.ReadStreamingError!u8 {
        var byte: [1]u8 = undefined;

        while (true) {
            _ = try file.readStreaming(
            io,
            &.{byte[0..]},
            );

            return byte[0];
        }
    }

    pub fn terminate(self: *@This()) TerminateError!Child.Term {
        if (self.child.id) |pid| {
            try posix.kill(pid, std.posix.SIG.TERM);
            return try self.child.wait(self.io);
        }

        return TerminateError.ChildNotStartedError;
    }

    pub fn deinit(self: *@This()) void {
        if (self.ready_reader) |file| {
            file.close(self.io);
            self.ready_reader = null;
        }

        if (self.child.id) |pid| {
            std.posix.kill(pid, std.posix.SIG.TERM) catch @panic("Unable to terminate child process");
        }

        self.state = .exited;
    }
};

const ReadinessPipeError = error{
ProcessFdQuotaExceeded,
SystemFdQuotaExceeded,
PipeCreationFailed,
};

const ReadinessPipe = struct {
    reader: Io.File,
    writer: Io.File,

    pub fn init() ReadinessPipeError!@This() {
        var fds: [2]std.posix.fd_t = undefined;

        const rc = std.c.pipe(&fds);

        if (rc == -1) {
            return switch (std.c.errno(rc)) {
                .MFILE => error.ProcessFdQuotaExceeded,
                .NFILE => error.SystemFdQuotaExceeded,
                else => error.PipeCreationFailed,
            };
        }

        return .{
            .reader = .{
                .handle = fds[0],
                .flags = .{ .nonblocking = false },
            },
            .writer = .{
                .handle = fds[1],
                .flags = .{ .nonblocking = false },
            },
        };
    }

    pub fn deinit(self: *@This(), io: Io) void {
        self.reader.close(io);
        self.writer.close(io);
    }
};
