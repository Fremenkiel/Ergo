const std = @import("std");
const lz4 = @import("lz4");

pub fn compress(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const max_dst_size = lz4.LZ4_compressBound(@intCast(data.len));
    const compressed = try allocator.alloc(u8, @as(usize, @intCast(max_dst_size)));
    errdefer allocator.free(compressed);

    const compressed_size = lz4.LZ4_compress_default(
        @ptrCast(data.ptr),
        @ptrCast(compressed.ptr),
        @intCast(data.len),
        @intCast(compressed.len),
    );

    if (compressed_size <= 0) {
        return error.CompressionFailed;
    }

    return allocator.realloc(compressed, @as(usize, @intCast(compressed_size)));
}

