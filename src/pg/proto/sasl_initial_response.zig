const std = @import("std");

const Io = std.Io;
const testing = std.testing;

const proto = @import("proto.zig");

pub fn writeSASLInitialResponse(io: Io, stream: Io.net.Stream, response: []const u8, mechanism: []const u8) !void {
    // 4 +   M          + 1 + 4             + R
    // len + $mechanism + 0 + $response.len + $response
    const payload_len = 9 + mechanism.len + response.len;

    var buf: [payload_len * 2]u8 = undefined;
    var writer = stream.writer(io, &buf);
    const w = &writer.interface;

    try w.writeByte('p');
    try w.writeInt(u32, @intCast(payload_len));
    try w.writeAll(mechanism);
    try w.writeByte(0);
    try w.writeInt(u32, @intCast(response.len));
    try w.writeAll(response);

    try w.flush();
}

const Reader = proto.Reader;
test "SASLInitialResponse: write" {
    const io = testing.io;

    const s = writeSASLInitialResponse(io, stream, "a sasl response", "SCRAM-SHA-256");

    var reader = Reader.init(buf.string());
    try t.expectEqual('p', try reader.byte());
    try t.expectEqual(37, try reader.int32()); // payload length
    try t.expectString("SCRAM-SHA-256", try reader.string());
    try t.expectEqual(15, try reader.int32()); // length of response
    try t.expectString("a sasl response", reader.rest());
}
