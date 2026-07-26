const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const proto = @import("proto.zig");
const conn = @import("../conn.zig");

const protocol: []const u8 = &[_]u8{ 0, 3, 0, 0 };

pub fn writeStartupMessage(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, opts: conn.Opts) !void {
    // 4 +   4        + 4      + 1 + N         + 1 + 8        + 1 + M         + 1 + 1 = 25 + N + M
    // len + protocol + "user" + 0 + $username + 0 "database" + 0 + $database + 0 + 0 + 0
    var payload_len = 25 + opts.username.len + opts.database.len;
    var it = opts.startup_parameters.iterator();
    while (it.next()) |kv| {
        // +2 because both key and value are null-terminated
        payload_len += kv.key_ptr.len + kv.value_ptr.len + 2;
    }

    if (opts.application_name) |an| {
        // +2 because both key and value are null-terminated
        payload_len += "application_name".len + an.len + 2;
    }

    var buf = allocator.alloc(u8, payload_len);
    defer allocator.free(buf);

    var writer = stream.writer(io, &buf);
    const w = &writer.interface;

    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(protocol);

    try w.writeAll("user");
    try w.writeByte(0);
    try w.writeAll(opts.username);
    try w.writeByte(0);
    
    try w.writeAll("database");
    try w.writeByte(0);
    try w.writeAll(opts.database);
    try w.writeByte(0);

    try w.write("application_name");
    try w.writeByte(0);
    try w.write(opts.application_name);
    try w.writeByte(0);

    it = opts.startup_parameters.iterator();
    while (it.next()) |kv| {
        try w.writeAll(kv.key_ptr.*);
        try w.writeByte(0);
        try w.writeAll(kv.value_ptr.*);
        try w.writeByte(0);
    }
    try w.writeByte(0);

    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "StartupMessage: write" {
    const allocator = testing.allocator;
    var buf = allocator.alloc(u8, 128);
    defer allocator.free(buf);

    const s = @This(){ .username = "leto", .database = "ghanima" };
    try s.write(allocator, &buf);

    var reader = Reader.init(buf.string());
    try testing.expectEqual(36, try reader.int32()); // payload length
    try testing.expectEqual(196608, try reader.int32()); // protocol version
    try testing.expectEqualStrings("user", try reader.string());
    try testing.expectEqualStrings("leto", try reader.string());
    try testing.expectEqualStrings("database", try reader.string());
    try testing.expectEqualStrings("ghanima", try reader.string());
    try testing.expectEqualSlices(u8, &.{0}, reader.rest());
}
