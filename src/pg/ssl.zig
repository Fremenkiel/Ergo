const std = @import("std");

const conn = @import("conn.zig");
pub const openssl = @import("openssl");

const Conn = conn.Conn;

pub fn initializeSSLContext(config: conn.Opts.TLS) !*openssl.SSL_CTX {
    const ctx = openssl.SSL_CTX_new(openssl.TLS_client_method()) orelse {
        return error.SSLContextNew;
    };
    errdefer openssl.SSL_CTX_free(ctx);

    if (openssl.SSL_CTX_set_min_proto_version(ctx, openssl.TLS1_2_VERSION) != 1) {
        return error.SSLMinVersion;
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
                    return error.SSLVerifyPaths;
                }
            } else {
                if (openssl.SSL_CTX_set_default_verify_paths(ctx) != 1) {
                        printSSLError();
                    return error.SSLDefaultVerifyPaths;
                }
            }
            openssl.SSL_CTX_set_verify(ctx, openssl.SSL_VERIFY_PEER, null);
        },
    }

    return ctx;
}

pub fn freeSSLContext(ctx: ?*openssl.SSL_CTX) void {
    if (ctx) |c| {
        openssl.SSL_CTX_free(c);
    }
}

pub fn printSSLError() void {
    const bio = openssl.BIO_new(openssl.BIO_s_mem());
    defer _ = openssl.BIO_free(bio);
    openssl.ERR_print_errors(bio);
    var buf: [*]u8 = undefined;
    const len: usize = @intCast(openssl.BIO_get_mem_data(bio, &buf));
    if (len > 0) {
        std.debug.print("{s}\n", .{buf[0..len]});
    }
}
