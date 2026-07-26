const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const proto = @import("proto.zig");

const PasswordMessage = @This();

password: []const u8,

pub fn writePasswordMessage(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, password: []const u8) !void {
    // +4 since the payload length includes the length itself
    // +1 for null terminated string
    const payload_len = password.len + 5;

    // +1 for the type field, 'p'
    const total_length = payload_len + 1;
    var buf = try allocator.alloc(u8, total_length);
    defer allocator.free(buf);

    var writer = stream.writer(io, &buf);
    var w = &writer.interface;

    try w.writeByte('p');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(password);
    try w.writeByte(0);

    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "PasswordMessage: write" {
    var buf = try proto.Buffer.init(t.allocator, 128);
    defer buf.deinit();

    const pw = PasswordMessage{ .password = "gh@nim@" };
    try pw.write(&buf);

    var reader = Reader.init(buf.string());
    try t.expectEqual('p', try reader.byte());
    try t.expectEqual(12, try reader.int32()); // payload length
    try t.expectString("gh@nim@", try reader.string());
}
