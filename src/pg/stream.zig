const std = @import("std");
const builtin = @import("builtin");
const openssl = @import("openssl");

const posix = std.posix;
const testing = std.testing;
const Io = std.Io;
const mem = std.mem;

const conn = @import("conn.zig");

const Conn = conn.Conn;
const Reader = @import("reader.zig").Reader;

const printSSLError = @import("ssl.zig").printSSLError;

pub const Stream = struct {
    io: Io,

    stream: Io.net.Stream,
    buffer: []u8,
    writer: *Io.Writer,

    valid: bool,
    ssl: ?*openssl.SSL,

    pub fn init(allocator: mem.Allocator, io: Io, opts: conn.Opts, ctx_: ?*openssl.SSL_CTX) !Stream {
        const is_unix = opts.host.len > 0 and opts.host[0] == '/';

        const io_stream = try blk: {
            if (is_unix) {
                if (comptime Io.net.has_unix_sockets == false or std.posix.AF == void) {
                    return error.UnixPathNotSupported;
                }
                const addr: Io.net.UnixAddress = try .init(opts.host);
                break :blk addr.connect(io);
            }
            const port = opts.port orelse 5432;
            const hostname: Io.net.HostName = try .init(opts.host);
            break :blk hostname.connect(io, port, .{ .mode = .stream });
        };
        errdefer io_stream.close(io);

        if (is_unix == false) {
            try setKeepalive(io_stream.socket.handle, opts);
        }

        const buffer = try allocator.alloc(u8, 1028);
        errdefer allocator.free(buffer);
        
        var writer = io_stream.writer(io, buffer);
        const w = &writer.interface; 

        var stream: Stream = .{
            .ssl = null,
            .valid = true,
            .stream = io_stream,
            .buffer = buffer,
            .writer = w,
            .io = io,
        };

        if (ctx_) |ctx| {
            // PostgreSQL TLS starts off as a plain connection which we upgrade
            try stream.writeStream(&.{ 0, 0, 0, 8, 4, 210, 22, 47 });
            var buf = [1]u8{0};
            _ = try stream.readStream(&buf);
            if (buf[0] != 'S') {
                return error.SSLNotSupportedByServer;
            }

            stream.ssl = openssl.SSL_new(ctx) orelse return error.SSLNewFailed;
            errdefer openssl.SSL_free(stream.ssl);

            if (isHostName(opts.host)) {
                // don't send this for an ip address
                var owned = false;
                const h = opts.hostz orelse blk: {
                    owned = true;
                    break :blk try allocator.dupeZ(u8, opts.host);
                };

                defer if (owned) {
                    allocator.free(h);
                };

                if (openssl.SSL_set_tlsext_host_name(stream.ssl, h.ptr) != 1) {
                    return error.SSLHostNameFailed;
                }
            }
            switch (opts.tls) {
                .verify_full => openssl.SSL_set_verify(stream.ssl, openssl.SSL_VERIFY_PEER, null),
                else => {},
            }

            if (openssl.SSL_set_fd(stream.ssl, io_stream.socket.handle) != 1) {
                return error.SSLSetFdFailed;
            }

            {
                const ret = openssl.SSL_connect(stream.ssl);
                if (ret != 1) {
                    const verification_code = openssl.SSL_get_verify_result(stream.ssl);
                    printSSLError();
                    if (verification_code != openssl.X509_V_OK) {
                        if (!builtin.is_test) {
                            std.log.err("ssl verification error: {s}\n", .{openssl.X509_verify_cert_error_string(verification_code)});
                        }
                        return error.SSLCertificationVerificationError;
                    }
                    return error.SSLConnectFailed;
                }
            }
        }

        return stream;
    }

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        if (self.ssl) |ssl| {
            if (self.valid) {
                _ = openssl.SSL_shutdown(ssl);
                self.valid = false;
            }
            openssl.SSL_free(ssl);
        }
        self.stream.close(self.io);
        allocator.free(self.buffer);
    }

    pub fn shutdown(self: *const @This(), how: ShutdownHow) !void {
        return sockShutdown(self.stream.socket.handle, how);
    }

    fn writeStream(self: *@This(), data: []const u8) !void {
        var writer = self.stream.writer(self.io, self.buffer);
        var w = &writer.interface;

        w.writeAll(data) catch |err| switch (err) {
            error.WriteFailed => return writer.err orelse err,
        };
        w.flush() catch |err| switch (err) {
            error.WriteFailed => return writer.err orelse err,
        };
    }

    pub fn writeAll(self: *@This(), data: []const u8) !void {
        if (self.ssl) |ssl| {
            const result = openssl.SSL_write(ssl, data.ptr, @intCast(data.len));
            if (result <= 0) {
                self.valid = false;
                return error.SSLWriteFailed;
            }
            return;
        }
        return self.writeStream(data);
    }

    fn readStream(self: *@This(), buf: []u8) !usize {
        var vecs: [1][]u8 = .{buf};
        var reader = self.stream.reader(self.io, &.{});
        const r = &reader.interface;
        return r.readVec(&vecs) catch |err| switch (err) {
            error.ReadFailed => return reader.err orelse err,
            else => return err,
        };
    }

    pub fn read(self: *Stream, buf: []u8) !usize {
        if (self.ssl) |ssl| {
            var read_len: usize = undefined;
            const result = openssl.SSL_read_ex(ssl, buf.ptr, @intCast(buf.len), &read_len);
            if (result <= 0) {
                self.valid = false;
                return error.SSLReadFailed;
            }
            return read_len;
        }

        return self.readStream(buf);
    }

    pub fn readWithTimeout(self: *@This(), buffer: []u8, timeout_ms: i32) !usize {
        var fds = [_]std.posix.pollfd{
            .{ .fd = self.stream.socket.handle, .events = std.posix.POLL.IN, .revents = 0 },
        };

        const ready_count = try posix.poll(&fds, timeout_ms);

        if (ready_count == 0) {
            return error.Timeout;
        }

        if ((fds[0].revents & posix.POLL.IN) != 0) {
            return self.read(buffer);
        }

        return error.UnexpectedPollEvent;
    }
};

fn setKeepalive(handle: posix.socket_t, opts: conn.Opts) !void {
    if (opts.keepalive == false) {
        return;
    }
    const on: c_int = 1;
    try setsockopt(handle, posix.SOL.SOCKET, posix.SO.KEEPALIVE, std.mem.asBytes(&on));

    const TCP = posix.TCP;
    const level = posix.IPPROTO.TCP;

    if (opts.keepalive_idle) |idle| {
        const optname: ?u32 = comptime if (@hasDecl(TCP, "KEEPIDLE"))
            TCP.KEEPIDLE
        else if (@hasDecl(TCP, "KEEPALIVE"))
            TCP.KEEPALIVE
        else
            null;
        if (optname) |name| {
            const v: c_int = @intCast(idle);
            setsockopt(handle, level, name, std.mem.asBytes(&v)) catch {};
        }
    }

    if (opts.keepalive_interval) |intvl| {
        if (comptime @hasDecl(TCP, "KEEPINTVL")) {
            const v: c_int = @intCast(intvl);
            setsockopt(handle, level, TCP.KEEPINTVL, std.mem.asBytes(&v)) catch {};
        }
    }

    if (opts.keepalive_count) |cnt| {
        if (comptime @hasDecl(TCP, "KEEPCNT")) {
            const v: c_int = @intCast(cnt);
            setsockopt(handle, level, TCP.KEEPCNT, std.mem.asBytes(&v)) catch {};
        }
    }
}

fn setsockopt(fd: posix.socket_t, level: i32, optname: u32, opt: []const u8) !void {
    return posix.setsockopt(fd, level, optname, opt);
}

const ShutdownHow = enum { recv, send, both };
fn sockShutdown(sock: posix.socket_t, how: ShutdownHow) !void {
    const rc = posix.system.shutdown(sock, switch (how) {
        .recv => posix.system.SHUT.RD,
        .send => posix.system.SHUT.WR,
        .both => posix.system.SHUT.RDWR,
    });
    switch (posix.errno(rc)) {
        .SUCCESS => return,
        .BADF => unreachable,
        .INVAL => unreachable,
        .NOTCONN => return error.SocketNotConnected,
        .NOTSOCK => unreachable,
        .NOBUFS => return error.SystemResources,
        else => return error.Unexpected,
    }
}

// Sends a best-effort Terminate ('X') message, shielded from cancellation so
// teardown can't be interrupted.
pub fn sendTerminate(stream: *Stream, io: Io) void {
    const prev = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(prev);
    stream.writeAll(&.{ 'X', 0, 0, 0, 4 }) catch {};
}

fn isHostName(host: []const u8) bool {
    if (std.mem.findScalar(u8, host, ':') != null) {
        // IPv6
        return false;
    }
    return std.mem.findNone(u8, host, "0123456789.") != null;
}

test "cancel stream while read" {
    const allocator = testing.allocator;
    const io = testing.io;

    var stream = try Stream.init(allocator, io, .{ 
        .port = 5432,
        .host = "localhost",
        .database = "db",
        .application_name = "Ergo test",
        .startup_parameters = .init(allocator) }, null);
    defer allocator.free(stream.buffer);

    const pipes = try Io.Threaded.pipe2(.{ .CLOEXEC = true });
    defer {
        Io.Threaded.closeFd(pipes[0]);
        Io.Threaded.closeFd(pipes[1]);
    }

    stream.stream.socket.handle = pipes[0];

    var buf: [256]u8 = undefined;

    try testing.expectError(error.Timeout, stream.readWithTimeout(&buf, 250));
}
