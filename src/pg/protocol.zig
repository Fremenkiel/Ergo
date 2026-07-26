const Buffer = @import("buffer").Buffer;

const conn = @import("conn.zig");


pub const ClientHello = struct {
    pub fn write(buf: *Buffer, opts: conn.Opts) !void {
        // 4 +   4        + 4      + 1 + N         + 1 + 8        + 1 + M         + 1 + 1 = 25 + N + M
        // len + protocol + "user" + 0 + $username + 0 "database" + 0 + $database + 0 + 0 + 0
        var payload_len = 25 + opts.username.len + opts.database.len;
        if (opts.startup_parameters) |p| {
            var it = p.iterator();
            while (it.next()) |kv| {
                // +2 because both key and value are null-terminated
                payload_len += kv.key_ptr.len + kv.value_ptr.len + 2;
            }
        }
        if (opts.application_name) |an| {
            // +2 because both key and value are null-terminated
            payload_len += "application_name".len + an.len + 2;
        }

        try buf.ensureTotalCapacity(payload_len);

        // this nonsense is to skip the buffers bound checking, since we've already
        // ensured the available capacity
        var view = buf.skip(payload_len) catch unreachable;
        view.writeIntBig(u32, @intCast(payload_len));
        view.write(self.protocol);
        view.write(&[_]u8{ 'u', 's', 'e', 'r', 0 });
        view.write(self.username);
        view.writeByte(0);
        view.write(&[_]u8{ 'd', 'a', 't', 'a', 'b', 'a', 's', 'e', 0 });
        view.write(self.database);
        view.writeByte(0);
        if (self.application_name) |an| {
            view.write("application_name");
            view.writeByte(0);
            view.write(an);
            view.writeByte(0);
        }
        if (self.params) |p| {
            var it = p.iterator();
            while (it.next()) |kv| {
                view.write(kv.key_ptr.*);
                view.writeByte(0);
                view.write(kv.value_ptr.*);
                view.writeByte(0);
            }
        }
        view.writeByte(0);
    }
};
