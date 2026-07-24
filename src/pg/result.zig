const std = @import("std");
const lib = @import("lib.zig");

const mem = std.mem;

const types = lib.types;
const proto = lib.proto;
const Conn = lib.Conn;

pub const Result = struct {
    number_of_columns: usize,

    // will be empty unless the query was executed with the column_names = true option
    column_names: [][]const u8,

    conn: *Conn,

    // a sliced version of state.oids (so we don't have to keep reslicing it to
    // number_of_columns on each row)
    oids: []i32,

    // a sliced version of state.values (so we don't have to keep reslicing it to
    // number_of_columns on each row)
    values: []State.Value,

    // When true, result.deinit() will call conn.release()
    // Used when the result came directly from the pool.query() helper.
    release_conn: bool,

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        // value.data references the buffer of the reader, this buffer is potentially
        // reused and potentially discarded. There are at least a few very good
        // reasons why the least we can do is blank it out.
        for (self.values) |*value| {
            value.data = &[_]u8{};
        }

        self.conn.reader.endFlow() catch {
            // this can only fail in extreme conditions (OOM) and it will only impact
            // the next query (and if the app is using the pool, the pool will try to
            // recover from this anyways)
            self.conn.state = .fail;
        };

        if (self.release_conn) {
            self.conn.release();
        }

        allocator.destroy(self);
    }

    // Caller should typically call next() until null is returned.
    // But in some cases, that might not be desirable. So they can
    // "drain" to empty the rest of the result.
    // I don't want to do this implictly in deinit because it can fail
    // and returning an error union in deinit is a pain for the caller.
    pub fn drain(self: *@This()) !void {
        var conn = self.conn;
        // Only an in-flight query has anything to drain; reading in any other
        // state (e.g. a poisoned connection) would block.
        if (conn.state != .query) {
            return;
        }

        while (true) {
            const msg = try conn.read();
            switch (msg.type) {
                'C' => {}, // CommandComplete
                'D' => {}, // DataRow
                'Z' => return,
                else => return error.UnexpectedDBMessage,
            }
        }
    }

    pub fn next(self: *@This()) !?Row {
        return self._next(.safe);
    }
    pub fn nextUnsafe(self: *@This()) !?RowUnsafe {
        return self._next(.unsafe);
    }

    fn _next(self: *@This(), comptime fail_mode: lib.FailMode) !(if (fail_mode == .safe) ?Row else ?RowUnsafe) {
        if (self.conn.state != .query) {
            // Possibly weird state. Most likely cause is calling next() multiple times
            // despite null being returned.
            return null;
        }

        const msg = try self.conn.read();
        switch (msg.type) {
            'D' => {
                const data = msg.data;
                // Since our Row API gets data by column #, we need translate the column
                // # to a slice within msg.data. We could do this on the fly within Row,
                // but creating this mapping up front simplifies things and, in normal
                // cases, performs best. "Normal case" here assumes that the client app
                // is going to fetch most/all columns.

                // first column starts at position 2
                var offset: usize = 2;
                const values = self.values;
                for (values) |*value| {
                    const data_start = offset + 4;
                    const length = std.mem.readInt(i32, data[offset..data_start][0..4], .big);
                    if (length == -1) {
                        value.is_null = true;
                        value.data = &[_]u8{};
                        offset = data_start;
                    } else {
                        const data_end = data_start + @as(usize, @intCast(length));
                        value.is_null = false;
                        value.data = data[data_start..data_end];
                        offset = data_end;
                    }
                }

                return .{
                    .values = values,
                    .oids = self.oids,
                    .result = self,
                };
            },
            'C' => {
                try self.conn.readyForQuery();
                return null;
            },
            else => return error.UnexpectedDBMessage,
        }
    }

    pub fn columnIndex(self: *@This(), column_name: []const u8) ?usize {
        for (self.column_names, 0..) |n, i| {
            if (std.mem.eql(u8, n, column_name)) {
                return i;
            }
        }
        return null;
    }

    // For every query, we need to store the type of each column (so we know
    // how to parse the data). Optionally, we might need the name of each column.
    // The connection has a default Result.State for a max # of columns, and we'll use
    // that whenever we can. Otherwise, we'll create this dynamically.
    pub const State = struct {
        // The name for each returned column, we only populate this if we're told
        // to (since it requires us to dupe the data)
        names: ?[][]const u8,

        // This is different than the above. The above are set once per query
        // from the RowDescription response of our Describe message. This is set for
        // each DataRow message we receive. It maps a column position with the encoded
        // value.
        values: []Value,

        // The OID for each returned column
        oids: []i32,

        capacity: usize,
        len: usize,

        pub const Value = struct {
            is_null: bool,
            data: []const u8,
        };

        pub fn init(allocator: mem.Allocator, size: usize) !State {
            const values = try allocator.alloc(Value, size);
            errdefer allocator.free(values);

            const oids = try allocator.alloc(i32, size);
            errdefer allocator.free(oids);

            return .{
                .names = null,
                .values = values,
                .oids = oids,
                .capacity = size,
                .len = 0,
            };
        }

        pub fn deinit(self: *const @This(), allocator: mem.Allocator) void {
            if (self.names) |names| {
                for (0..self.len) |i| {
                    allocator.free(names[i]);
                }
                allocator.free(names);
            }
            allocator.free(self.values);
            allocator.free(self.oids);
        }

        // Populates the State from the RowDescription payload
        // We already read the number_of_columns from data, so we pass it in here
        // We also already know that number_of_columns fits within our arrays
        pub fn from(self: *@This(), allocator: mem.Allocator, number_of_columns: u16, data: []const u8) !void {
            // skip the column count, which we already know as number_of_columns
            var pos: usize = 2;

            if (self.names == null) {
                self.names = try allocator.alloc([]const u8, self.capacity);
                errdefer allocator.free(self.names);
            } else {
                for (0..self.len) |i| {
                    allocator.free(self.names.?[i]);
                }
            }

            self.len = number_of_columns;
            for (0..number_of_columns) |i| {
                const end_pos = std.mem.indexOfScalarPos(u8, data, pos, 0) orelse return error.InvalidDataRow;
                if (data.len < (end_pos + 19)) {
                    return error.InvalidDataRow;
                }
                self.names.?[i] = try allocator.dupe(u8, data[pos..end_pos]);

                // skip the name null terminator (1)
                // skip the table object_id this table belongs to (4)
                // skip the attribute number of this table column (2)
                pos = end_pos + 7;

                {
                    const end = pos + 4;
                    self.oids[i] = std.mem.readInt(i32, data[pos..end][0..4], .big);
                    pos = end;
                }

                // skip date type size (2), type modifier (4) format code (2)
                pos += 8;
            }
        }
    };
};

pub const Row = RowT(.safe);
pub const RowUnsafe = RowT(.unsafe);

pub fn RowT(comptime fail_mode: lib.FailMode) type {
    return struct {
        result: *Result,
        oids: []i32,
        values: []Result.State.Value,

        pub fn get(self: *const @This(), comptime T: type, col: usize) if (fail_mode == .safe) lib.TypeError!T else T {
            const value = self.values[col];
            const TT = switch (@typeInfo(T)) {
                .optional => |opt| {
                    if (value.is_null) {
                        return null;
                    }
                    const val = self.get(opt.child, col);
                    if (comptime fail_mode == .safe) {
                        return try val;
                    }
                    return val;
                },
                .@"struct", .@"union" => blk: {
                    if (@hasDecl(T, "fromPgzRow") == true) {
                        return T.fromPgzRow(value, self.oids[col]) catch |err| {
                            if (comptime fail_mode == .safe) {
                                return err;
                            }
                            std.debug.panic("PostgreSQL value of type {s} could not be read into a " ++ @typeName(T) ++ ".", .{types.oidToString(self.oids[col])});
                        };
                    }
                    break :blk T;
                },
                else => blk: {
                    lib.verifyNotNull(fail_mode, T, value.is_null) catch |err| {
                        if (comptime fail_mode == .unsafe) unreachable;
                        return err;
                    };
                    break :blk T;
                },
            };

            return types.decodeScalar(fail_mode, TT, value.data, self.oids[col]);
        }

        pub fn getCol(self: *@This(), comptime T: type, name: []const u8) if (fail_mode == .safe) lib.TypeError!T else T {
            const col = self.result.columnIndex(name);
            try lib.verifyColumnName(fail_mode, name, col != null);
            return self.get(T, col.?);
        }
    };
}

// pub const QueryRow = QueryRowT(.safe);
// pub const QueryRowUnsafe = QueryRowT(.unsafe);
//
// pub fn QueryRowT(comptime fail_mode: lib.FailMode) type {
//     return struct {
//         row: RowT(fail_mode),
//         result: *Result,
//
//         const Self = @This();
//
//         pub fn get(self: *@This(), comptime T: type, col: usize) if (fail_mode == .safe) lib.TypeError!T else T {
//             return self.row.get(T, col);
//         }
//
//         pub fn deinit(self: *@This()) !void {
//             // this is unfortunate
//             try self.result.drain();
//             self.result.deinit();
//         }
//     };
// }

// pub const Record = RecordT(.safe);
// pub const RecordUnsafe = RecordT(.unsafe);
//
// pub fn RecordT(comptime fail_mode: lib.FailMode) type {
//     return struct {
//         data: []const u8,
//         number_of_columns: usize,
//
//         const Self = @This();
//
//         pub fn next(self: *@This(), comptime T: type) if (fail_mode == .safe) lib.TypeError!T else T {
//             var data = self.data;
//
//             // at least 4 bytes for the type and 4 bytes for the lenght
//             lib.assert(data.len >= 8);
//
//             const oid = std.mem.readInt(i32, data[0..4], .big);
//
//             data = data[4..];
//             const len = std.mem.readInt(i32, data[0..4], .big);
//
//             const TT = switch (@typeInfo(T)) {
//                 .optional => |opt| blk: {
//                     if (len == -1) return null;
//                     break :blk opt.child;
//                 },
//                 else => T,
//             };
//
//             // end of the data for this "column"
//             const end = @as(usize, @intCast(len)) + 4;
//
//             // the rest of the data
//             self.data = data[end..];
//
//             // start at 4 to skip the length which we already read
//             return types.decodeScalar(fail_mode, TT, data[4..end], oid);
//         }
//     };
// }

const t = lib.testing;

test "Result: ints" {
    const allocator = std.testing.allocator;
    var c = try t.connect(.{});
    defer c.deinit();
    const sql = "select $1::smallint, $2::int, $3::bigint";

    {
        // int max
        var result = try c.query(sql, .{ @as(i16, 32767), @as(i32, 2147483647), @as(i64, 9223372036854775807) });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(32767, row.get(i16, 0));
        try t.expectEqual(2147483647, row.get(i32, 1));
        try t.expectEqual(9223372036854775807, row.get(i64, 2));

        try t.expectEqual(32767, row.get(?i16, 0));
        try t.expectEqual(2147483647, row.get(?i32, 1));
        try t.expectEqual(9223372036854775807, row.get(?i64, 2));

        try t.expectEqual(null, result.next());
    }

    {
        // int min
        var result = try c.query(sql, .{ @as(i16, -32768), @as(i32, -2147483648), @as(i64, -9223372036854775808) });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(-32768, row.get(i16, 0));
        try t.expectEqual(-2147483648, row.get(i32, 1));
        try t.expectEqual(-9223372036854775808, row.get(i64, 2));
        try result.drain();
    }

    {
        // int null
        var result = try c.query(sql, .{ null, null, null });
        defer result.deinit(allocator);
        defer result.drain() catch unreachable;
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(null, row.get(?i16, 0));
        try t.expectEqual(null, row.get(?i32, 1));
        try t.expectEqual(null, row.get(?i64, 2));
    }

    {
        // uint within limit
        var result = try c.query(sql, .{ @as(u16, 32767), @as(u32, 2147483647), @as(u64, 9223372036854775807) });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(32767, row.get(i16, 0));
        try t.expectEqual(2147483647, row.get(i32, 1));
        try t.expectEqual(9223372036854775807, row.get(i64, 2));

        try t.expectEqual(32767, row.get(?i16, 0));
        try t.expectEqual(2147483647, row.get(?i32, 1));
        try t.expectEqual(9223372036854775807, row.get(?i64, 2));
        try result.drain();
    }

    {
        // u16 outside of limit
        try t.expectError(error.IntWontFit, c.query(sql, .{ @as(u16, 32768), @as(u32, 0), @as(u64, 0) }));
        // u32 outside of limit
        try t.expectError(error.IntWontFit, c.query(sql, .{ @as(u16, 0), @as(u32, 2147483648), @as(u64, 0) }));
        // u64 outside of limit
        try t.expectError(error.IntWontFit, c.query(sql, .{ @as(u16, 0), @as(u32, 0), @as(u64, 9223372036854775808) }));
    }
}

test "Result: floats" {
    const allocator = std.testing.allocator;
    var c = try t.connect(.{});
    defer c.deinit();
    const sql = "select $1::float4, $2::float8";

    {
        // positive float
        var result = try c.query(sql, .{ @as(f32, 1.23456), @as(f64, 1093.229183) });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(1.23456, row.get(f32, 0));
        try t.expectEqual(1093.229183, row.get(f64, 1));

        try t.expectEqual(1.23456, row.get(?f32, 0));
        try t.expectEqual(1093.229183, row.get(?f64, 1));

        try t.expectEqual(null, result.next());
    }

    {
        // negative float
        var result = try c.query(sql, .{ @as(f32, -392.31), @as(f64, -99991.99992) });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(-392.31, row.get(f32, 0));
        try t.expectEqual(-99991.99992, row.get(f64, 1));
        try t.expectEqual(null, result.next());
    }

    {
        // null float
        var result = try c.query(sql, .{ null, null });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(null, row.get(?f32, 0));
        try t.expectEqual(null, row.get(?f64, 1));
        try t.expectEqual(null, result.next());
    }
}

test "Result: bool" {
    const allocator = std.testing.allocator;
    var c = try t.connect(.{});
    defer c.deinit();
    const sql = "select $1::bool";

    {
        // true
        var result = try c.query(sql, .{true});
        defer result.deinit(allocator);
        defer result.drain() catch unreachable;
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(true, row.get(bool, 0));
        try t.expectEqual(true, row.get(?bool, 0));
        try t.expectEqual(null, result.next());
    }

    {
        // false
        var result = try c.query(sql, .{false});
        defer result.deinit(allocator);
        defer result.drain() catch unreachable;
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(false, row.get(bool, 0));
        try t.expectEqual(false, row.get(?bool, 0));
        try t.expectEqual(null, result.next());
    }

    {
        // null
        var result = try c.query(sql, .{null});
        defer result.deinit(allocator);
        defer result.drain() catch unreachable;
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(null, row.get(?bool, 0));
        try t.expectEqual(null, result.next());
    }
}

test "Result: text and bytea" {
    const allocator = std.testing.allocator;
    var c = try t.connect(.{});
    defer c.deinit();
    const sql = "select $1::text, $2::bytea";

    {
        // empty
        var result = try c.query(sql, .{ "", "" });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectString("", row.get([]u8, 0));
        try t.expectString("", row.get(?[]u8, 0).?);
        try t.expectString("", row.get([]u8, 1));
        try t.expectString("", row.get(?[]u8, 1).?);
        try result.drain();
    }

    {
        // not empty
        var result = try c.query(sql, .{ "it's over 9000!!!", "i will Not fear" });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectString("it's over 9000!!!", row.get([]u8, 0));
        try t.expectString("it's over 9000!!!", row.get(?[]const u8, 0).?);
        try t.expectString("i will Not fear", row.get([]const u8, 1));
        try t.expectString("i will Not fear", row.get(?[]u8, 1).?);
        try result.drain();
    }

    {
        // as an array
        var result = try c.query(sql, .{ [_]u8{ 'a', 'c', 'b' }, [_]u8{ 'z', 'z', '3' } });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectString("acb", row.get([]const u8, 0));
        try t.expectString("acb", row.get(?[]u8, 0).?);
        try t.expectString("zz3", row.get([]const u8, 1));
        try t.expectString("zz3", row.get(?[]u8, 1).?);
        try result.drain();
    }

    {
        // as a slice
        const s1 = try t.allocator.alloc(u8, 4);
        defer t.allocator.free(s1);
        @memcpy(s1, "Leto");

        var result = try c.query(sql, .{ s1, constString() });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectString("Leto", row.get([]u8, 0));
        try t.expectString("Leto", row.get(?[]u8, 0).?);
        try t.expectString("Ghanima", row.get([]u8, 1));
        try t.expectString("Ghanima", row.get(?[]u8, 1).?);
        try result.drain();
    }

    {
        // null
        var result = try c.query(sql, .{ null, null });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(null, row.get(?[]u8, 0));
        try t.expectEqual(null, row.get(?[]u8, 1));
        try result.drain();
    }
}

fn constString() []const u8 {
    return "Ghanima";
}

test "Result: optional" {
    const allocator = std.testing.allocator;
    var c = try t.connect(.{});
    defer c.deinit();
    const sql = "select $1::int, $2::int";

    {
        // int max
        var result = try c.query(sql, .{ @as(?i32, 321), @as(?i32, null) });
        defer result.deinit(allocator);
        const row = (try result.nextUnsafe()).?;
        try t.expectEqual(321, row.get(i32, 0));

        try t.expectEqual(321, row.get(?i32, 0));
        try t.expectEqual(null, row.get(?i32, 1));
        try t.expectEqual(null, result.next());
    }
}

test "Result: UUID" {
    const allocator = std.testing.allocator;
    var c = try t.connect(.{});
    defer c.deinit();
    const sql = "select $1::uuid, $2::uuid";
    var result = try c.query(sql, .{ "fcbebf0f-b996-43b9-9818-672bc689cda8", &[_]u8{ 174, 47, 71, 95, 128, 112, 65, 183, 186, 51, 134, 187, 168, 137, 123, 222 } });
    defer result.deinit(allocator);

    const row = (try result.nextUnsafe()).?;
    try t.expectSlice(u8, &.{ 252, 190, 191, 15, 185, 150, 67, 185, 152, 24, 103, 43, 198, 137, 205, 168 }, row.get([]u8, 0));
    try t.expectSlice(u8, &.{ 174, 47, 71, 95, 128, 112, 65, 183, 186, 51, 134, 187, 168, 137, 123, 222 }, row.get([]u8, 1));
}

test "Result: lsn" {
    const allocator = std.testing.allocator;
    var c = try t.connect(.{});
    defer c.deinit();
    const sql = "select $1::pg_lsn + 1";
    var result = try c.query(sql, .{32788447688});
    defer result.deinit(allocator);

    const row = (try result.nextUnsafe()).?;
    try t.expectEqual(32788447689, row.get(i64, 0));
}

test "Result: safe" {
    const allocator = std.testing.allocator;
    var c = try t.connect(.{});
    defer c.deinit();
    const sql = "select $1::int, $2::int";

    {
        var result = try c.query(sql, .{ @as(?i32, 321), @as(?i32, null) });
        defer result.deinit(allocator);
        const row = (try result.next()).?;
        try t.expectEqual(321, try row.get(i32, 0));
        try t.expectEqual(error.InvalidType, row.get(bool, 0));

        try t.expectEqual(321, try row.get(?i32, 0));
        try t.expectEqual(null, try row.get(?i32, 1));
        try t.expectEqual(null, result.next());
    }
}
