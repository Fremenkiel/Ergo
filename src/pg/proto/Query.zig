const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const proto = @import("proto.zig");

sql: []const u8,

pub fn writeQuery(allocator: mem.Allocator, io: Io, stream: Io.net.Stream, sql: []const u8) !void {
    // 4   + S    + 1
    // len + $sql + 0
    const payload_len = 5 + sql.len;

    // +1 for the type field, 'Q'
    const total_length = payload_len + 1;

    var buf = try allocator.alloc(u8, total_length);
    defer allocator.free(buf);

    var writer = stream.writer(io, &buf);
    var w = &writer.interface;

    try w.writeByte('Q');
    try w.writeInt(u32, @intCast(payload_len), .big);
    try w.writeAll(sql);
    try w.writeByte(0);

    try w.flush();
}

const t = proto.testing;
const Reader = proto.Reader;
test "Query: write" {
    const allocator = testing.allocator;
    const io = testing.io;

    const q = writeQuery(allocator, io, stream, "select 1");

    var reader = Reader.init(buf.string());
    try testing.expectEqual('Q', try reader.byte());
    try testing.expectEqual(13, try reader.int32()); // payload length
    try testing.expectEqualStrings("select 1", try reader.restAsString());
}
