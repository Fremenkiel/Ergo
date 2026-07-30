const std = @import("std");

const Io = std.Io;
const testing = std.testing;

pub const Error = struct {
    code: []const u8,
    message: []const u8,
    severity: []const u8,

    column: ?[]const u8 = null,
    constraint: ?[]const u8 = null,
    data_type_name: ?[]const u8 = null,
    detail: ?[]const u8 = null,
    file: ?[]const u8 = null,
    hint: ?[]const u8 = null,
    internal_position: ?[]const u8 = null,
    internal_query: ?[]const u8 = null,
    line: ?[]const u8 = null,
    position: ?[]const u8 = null,
    routine: ?[]const u8 = null,
    schema: ?[]const u8 = null,
    severity2: ?[]const u8 = null,
    table: ?[]const u8 = null,
    where: ?[]const u8 = null,
    
    pub fn init(data: []const u8) Error {
        var err = Error{
            .code = "",
            .message = "",
            .severity = "",
        };

        var pos: usize = 0;
        while (pos < data.len) {
            const value_end = std.mem.indexOfScalarPos(u8, data, pos + 1, 0) orelse {
                // TODO: should not happen
                break;
            };

            const value = data[pos + 1 .. value_end];
            switch (data[pos]) {
                'S' => err.severity = value,
                'V' => err.severity2 = value,
                'C' => err.code = value,
                'M' => err.message = value,
                'D' => err.detail = value,
                'H' => err.hint = value,
                'P' => err.position = value,
                'p' => err.internal_position = value,
                'q' => err.internal_query = value,
                'W' => err.where = value,
                's' => err.schema = value,
                't' => err.table = value,
                'c' => err.column = value,
                'd' => err.data_type_name = value,
                'n' => err.constraint = value,
                'F' => err.file = value,
                'L' => err.line = value,
                'R' => err.routine = value,
                else => unreachable,
            }
            pos = value_end + 1;
        }

        return err;
    }

    pub fn isUnique(self: *@This()) bool {
        return std.mem.eql(u8, self.code, "23505");
    }
};

test "Error: parse" {
    const allocator = testing.allocator;

    const buffer = try allocator.alloc(u8, 512);
    defer allocator.free(buffer);
    var writer = Io.Writer.fixed(buffer);

    {
        // only required
        try writer.writeByte('C');
        try writer.writeAll("10391A");
        try writer.writeByte(0);

        try writer.writeByte('M');
        try writer.writeAll("The Message");
        try writer.writeByte(0);

        try writer.writeByte('S');
        try writer.writeAll("FATAL");
        try writer.writeByte(0);

        const err = Error.init(buffer[0 .. writer.end]);
        try testing.expectEqualStrings("10391A", err.code);
        try testing.expectEqualStrings("The Message", err.message);
        try testing.expectEqualStrings("FATAL", err.severity);
    }

    writer.end = 0;

    {
        // all fields
        const fields = [_]u8{ 'S', 'V', 'C', 'M', 'D', 'H', 'P', 'p', 'q', 'W', 's', 't', 'c', 'd', 'n', 'F', 'L', 'R' };
        for (fields) |field| {
            try writer.writeByte(field);
            try writer.writeByte(field);
            try writer.writeAll("-value");
            try writer.writeByte(0);
        }

        const err = Error.init(buffer[0 .. writer.end]);
        try testing.expectEqualStrings("C-value", err.code);
        try testing.expectEqualStrings("M-value", err.message);
        try testing.expectEqualStrings("S-value", err.severity);
        try testing.expectEqualStrings("V-value", err.severity2.?);
        try testing.expectEqualStrings("D-value", err.detail.?);
        try testing.expectEqualStrings("H-value", err.hint.?);
        try testing.expectEqualStrings("P-value", err.position.?);
        try testing.expectEqualStrings("p-value", err.internal_position.?);
        try testing.expectEqualStrings("q-value", err.internal_query.?);
        try testing.expectEqualStrings("W-value", err.where.?);
        try testing.expectEqualStrings("s-value", err.schema.?);
        try testing.expectEqualStrings("t-value", err.table.?);
        try testing.expectEqualStrings("c-value", err.column.?);
        try testing.expectEqualStrings("d-value", err.data_type_name.?);
        try testing.expectEqualStrings("n-value", err.constraint.?);
        try testing.expectEqualStrings("F-value", err.file.?);
        try testing.expectEqualStrings("L-value", err.line.?);
        try testing.expectEqualStrings("R-value", err.routine.?);
    }
}

