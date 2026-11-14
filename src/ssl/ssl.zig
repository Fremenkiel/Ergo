const builtin = @import("builtin");

const std = @import("std");
const posix = std.posix;
const mem = std.mem;
const Allocator = mem.Allocator;

const assert = std.debug.assert;

const openssl = @import("openssl");
const SSL_CTX = openssl.SSL_CTX;

pub const TLS = union(enum) {
    off: void,
    require: void,
    verify_full: ?[]const u8,
};

pub const SSLError = error{
SSLNewFailed,
} || Allocator.Error || SetHostNameError || SetFdError || ConnectError || InitializeContextError;

const SetHostNameError = error{
SSLHostNameFailed,
};

const SetFdError = error{
SSLSetFdFailed,
};

const ConnectError = error{
SSLCertificationVerificationError,
SSLConnectFailed,
};

pub const WriteError = error{
SSLWriteFailed,
};

pub const ReadError = error{
SSLReadFailed,
};

const Status = enum(u8) {
    Initialized,
    Connected,
    Invalid
};

pub const SSL = struct {
    ssl: *openssl.SSL,
    ctx: *openssl.SSL_CTX,

    status: Status,

    pub fn init(allocator: Allocator, host: []const u8, tls: TLS, handle: posix.fd_t) SSLError!@This() {
        assert(tls != .off);

        const ctx = try initializeContext(tls);

        const ssl_connection = openssl.SSL_new(ctx) orelse {
            openssl.SSL_CTX_free(ctx);
            return SSLError.SSLNewFailed;
        };

        var ssl_context = SSL{
            .ssl = ssl_connection,
            .ctx = ctx,
            .status = .Initialized
        };
        errdefer ssl_context.deinit();

        if (isHostName(host)) {
            const owned_host = try allocator.dupeSentinel(u8, host, 0);
            defer allocator.free(owned_host);

            if (openssl.SSL_set_tlsext_host_name(ssl_context.ssl, owned_host.ptr) != 1) {
                return SetHostNameError.SSLHostNameFailed;
            }
        }

        ssl_context.setVerify(tls);

        try ssl_context.setFd(handle);
        try ssl_context.connect();

        ssl_context.status = .Connected;

        return ssl_context;
    }

    pub fn deinit(self: *@This()) void {
        if (self.status != .Invalid) {
            _ = openssl.SSL_shutdown(self.ssl);
        }

        openssl.SSL_free(self.ssl);
        openssl.SSL_CTX_free(self.ctx);
    }

    fn setVerify(self: *@This(), tls: TLS) void {
        switch (tls) {
            .verify_full => openssl.SSL_set_verify(self.ssl, openssl.SSL_VERIFY_PEER, null),
            else => {},
        }
    }

    fn setFd(self: *@This(), handle: posix.fd_t) SetFdError!void {
        if (openssl.SSL_set_fd(self.ssl, handle) != 1) {
            return SetFdError.SSLSetFdFailed;
        }
    }

    fn connect(self: *@This()) ConnectError!void {
        const ret = openssl.SSL_connect(self.ssl);
        if (ret != 1) {
            const verification_code = openssl.SSL_get_verify_result(self.ssl);
            printSSLError();
            if (verification_code != openssl.X509_V_OK) {
                if (builtin.is_test == false) {
                    std.log.err("ssl verification error: {s}\n", .{openssl.X509_verify_cert_error_string(verification_code)});
                }
                return ConnectError.SSLCertificationVerificationError;
            }
            return ConnectError.SSLConnectFailed;
        }
    }

    pub fn write(self: *@This(), data: []const u8) WriteError!void {
        const result = openssl.SSL_write(self.ssl, data.ptr, @intCast(data.len));
        if (result <= 0) {
            self.status = .Invalid;
            printSSLError();
            return WriteError.SSLWriteFailed;
        }
    }

    pub fn read(self: *@This(), buf: []u8) ReadError!usize {
        var read_len: usize = undefined;
        const result = openssl.SSL_read_ex(self.ssl, buf.ptr, @intCast(buf.len), &read_len);
        if (result <= 0) {
            return ReadError.SSLReadFailed;
        }
        return read_len;
    }
};

const InitializeContextError = error{
SSLContextNew,
SSLMinVersion,
SSLVerifyPaths,
SSLDefaultVerifyPaths,
};

fn initializeContext(config: TLS) InitializeContextError!*SSL_CTX {
    const ctx = openssl.SSL_CTX_new(openssl.TLS_client_method()) orelse {
        return InitializeContextError.SSLContextNew;
    };
    errdefer openssl.SSL_CTX_free(ctx);

    if (openssl.SSL_CTX_set_min_proto_version(ctx, openssl.TLS1_2_VERSION) != 1) {
        return InitializeContextError.SSLMinVersion;
    }

    _ = openssl.SSL_CTX_set_mode(ctx, openssl.SSL_MODE_AUTO_RETRY);

    switch (config) {
        .off, .require => {},
        .verify_full => |path_to_root| {
            if (path_to_root) |p| {
                var pathz: [std.fs.max_path_bytes + 1]u8 = undefined;
                @memcpy(pathz[0..p.len], p);
                pathz[p.len] = 0;
                if (openssl.SSL_CTX_load_verify_locations(ctx, pathz[0 .. p.len + 1].ptr, null) != 1) {
                    printSSLError();
                    return InitializeContextError.SSLVerifyPaths;
                }
            } else {
                if (openssl.SSL_CTX_set_default_verify_paths(ctx) != 1) {
                    printSSLError();
                    return InitializeContextError.SSLDefaultVerifyPaths;
                }
            }
            openssl.SSL_CTX_set_verify(ctx, openssl.SSL_VERIFY_PEER, null);
        },
    }

    return ctx;
}

fn printSSLError() void {
    if (builtin.is_test == true) return;
    var err: c_ulong = openssl.ERR_get_error();
    while (err != 0) {
        const str = openssl.ERR_error_string(err, 0);

        std.log.err("{s}", .{ str });

        err = openssl.ERR_get_error();
    }
}

fn isHostName(host: []const u8) bool {
    if (std.mem.findScalar(u8, host, ':') != null) {
        // IPv6
        return false;
    }
    return std.mem.findNone(u8, host, "0123456789.") != null;
}
