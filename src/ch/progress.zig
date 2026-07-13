const std = @import("std");

pub const Progress = struct {
    rows: u64 = 0,
    bytes: u64 = 0,
    total_rows: u64 = 0,
    written_rows: u64 = 0,
    written_bytes: u64 = 0,
    elapsed_ns: u64 = 0,

    pub fn read(reader: *std.Io.Reader) !Progress {
        return Progress{
            .rows = try reader.takeInt(u64, .little),
            .bytes = try reader.takeInt(u64, .little),
            .total_rows = try reader.takeInt(u64, .little),
            .written_rows = try reader.takeInt(u64, .little),
            .written_bytes = try reader.takeInt(u64, .little),
            .elapsed_ns = try reader.takeInt(u64, .little),
        };
    }

    pub fn merge(self: *Progress, other: Progress) void {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.total_rows = other.total_rows;
        self.written_rows += other.written_rows;
        self.written_bytes += other.written_bytes;
        self.elapsed_ns = other.elapsed_ns;
    }
};
