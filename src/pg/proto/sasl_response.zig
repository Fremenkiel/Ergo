const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const proto = @import("proto.zig");

const Reader = proto.Reader;

pub fn writeSASLResponse(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, data: []const u8) !void {
    // 4 +   N
    // len + $data
    const payload_len = 4 + data.len;

    const total_length = payload_len + 1;
    var buf = allocator.alloc(u8, total_length);
    allocator.free(buf);

    var writer = stream.writer(io, &buf);
    const w = &writer.interface;

    try w.writeByte('p');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(data);

    try w.flush();
}

const t = proto.testing;
test "SASLResponse: write" {
    const io = testing.io;

    const s = writeSASLResponse(io, stream, "the response");

    var reader = Reader.init(buf.string());
    try testing.expectEqual('p', try reader.byte());
    try testing.expectEqual(16, try reader.int32()); // payload length
    try testing.expectEqualStrings("the response", reader.rest());
}
