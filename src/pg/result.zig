const std = @import("std");

const mem = std.mem;
const testing = std.testing;

const types = @import("types.zig");

const Conn = @import("conn.zig").Conn;
const PgError = @import("root.zig").PgError;

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

pub const Row = struct {
        result: *Result,
        oids: []i32,
        values: []Result.State.Value,

        pub fn get(self: *const @This(), col: usize) PgError![]const u8 {
            return types.Bytea.decode(self.values[col].data, self.oids[col]);
        }
};

const t = @import("t.zig");
fn constString() []const u8 {
    return "Ghanima";
}

test "Result: text and bytea" {
    const allocator = testing.allocator;
    const io = testing.io;

    var c = try t.connect(allocator, io, t.test_opts);
    defer c.deinit();

    {
        // empty
        const sql = try std.fmt.allocPrint(allocator, "SELECT '{s}'::text, '{s}'::bytea", .{ "", "" });
        defer allocator.free(sql);

        var result = try c.query(sql, .{});
        defer result.deinit(allocator);
        const row = (try result.next()).?;

        try testing.expectEqualStrings("", try row.get(0));
        try testing.expectEqualStrings("", try row.get(1));

        try result.drain();
    }

    {
        // not empty
        const sql = try std.fmt.allocPrint(allocator, "SELECT '{s}'::text, '{s}'::bytea", .{ "its over 9000!!!", "i will Not fear" });
        defer allocator.free(sql);

        var result = try c.query(sql, .{});
        defer result.deinit(allocator);
        const row = (try result.next()).?;

        try testing.expectEqualStrings("its over 9000!!!", try row.get(0));
        try testing.expectEqualStrings("i will Not fear", try row.get(1));

        try result.drain();
    }

    {
        // as an array
        const sql = try std.fmt.allocPrint(allocator, "SELECT '{s}'::text, '{s}'::bytea", .{ [_]u8{ 'a', 'c', 'b' }, [_]u8{ 'z', 'z', '3' } });
        defer allocator.free(sql);

        var result = try c.query(sql, .{});
        defer result.deinit(allocator);
        const row = (try result.next()).?;

        try testing.expectEqualStrings("acb", try row.get(0));
        try testing.expectEqualStrings("zz3", try row.get(1));

        try result.drain();
    }

    {
        // as a slice
        const s1 = try allocator.alloc(u8, 4);
        defer allocator.free(s1);
        @memcpy(s1, "Leto");

        const sql = try std.fmt.allocPrint(allocator, "SELECT '{s}'::text, '{s}'::bytea", .{ s1, constString() });
        defer allocator.free(sql);

        var result = try c.query(sql, .{});
        defer result.deinit(allocator);
        const row = (try result.next()).?;

        try testing.expectEqualStrings("Leto", try row.get(0));
        try testing.expectEqualStrings("Ghanima", try row.get(1));

        try result.drain();
    }

    {
        // null
        const sql = try std.fmt.allocPrint(allocator, "SELECT {any}::text, {any}::bytea", .{ null, null });
        defer allocator.free(sql);

        var result = try c.query(sql, .{});
        defer result.deinit(allocator);
        const row = (try result.next()).?;

        try testing.expectEqualStrings("", try row.get(0));

        try result.drain();
    }
}
