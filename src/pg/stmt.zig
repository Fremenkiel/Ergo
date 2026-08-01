const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const types = @import("types.zig");

const Conn = @import("conn.zig").Conn;
const Message = @import("reader.zig").Message;
const Reader = @import("reader.zig").Reader;
const Result = @import("result.zig").Result;
const State = @import("conn.zig").State;
const Stream = @import("stream.zig").Stream;

pub const Stmt = StmtT(Conn);

fn StmtT(comptime StmtConn: type) type {
    return struct {
        allocator: mem.Allocator,

        opts: Conn.QueryOpts,

        conn: *StmtConn,

        buffer: []u8,
        writer: Io.Writer,

        // Every call to stmt.bind increments this value. Important because the Bind
        // message contains all the parameter meta data first, then the serialized
        // values. So when we bind a parameter, we need to jump around our buf payload
        // based on the param_index * $some_offset.
        param_index: u16,

        // Number of parameters in the query.
        param_count: u16,

        // The type of each parameter, which postgresql tells us after we send it the
        // SQL and ask for a description. `param_oids.len` can be greater than
        // `param_count` because we initially use the conn.param_oids which is
        // globally configured.
        param_oids: []i32,

        // Number of colums in the result
        column_count: u16,

        // Information about the colums in the result, which postgresql tells us after
        // we send it the SQL and ask for a description. The slices in this structure
        // can be larger than `column_count` because we initially conn.result_state
        // which is globally configured.
        result_state: Result.State,

        // Name of the prepared statement. Empty == unnamed, so it won't be cached
        // by the server
        name: []const u8,

        pub fn init(allocator: mem.Allocator, conn: *StmtConn, opts: Conn.QueryOpts) !@This() {
            const buffer = try allocator.alloc(u8, 1028);
            const writer = Io.Writer.fixed(buffer);

            return .{
                .conn = conn,
                .opts = opts,
                .allocator = allocator,
                .buffer = buffer,
                .writer = writer,
                .param_index = 0,
                .param_count = 0,
                .param_oids = conn.param_oids,
                .column_count = 0,
                .result_state = conn.result_state,
                .name = opts.cache_name orelse "",
            };
        }

        pub fn fromDescribe(allocator: mem.Allocator, conn: *StmtConn, describe: *Describe, opts: Conn.QueryOpts) !Stmt {
            const buffer = try allocator.alloc(u8, 1028);
            const writer = Io.Writer.fixed(buffer);

            return .{
                .conn = conn,
                .opts = opts,
                .allocator = allocator,
                .buffer = buffer,
                .writer = writer,
                .param_index = 0,
                .param_count = @intCast(describe.param_oids.len),
                .param_oids = describe.param_oids,
                .column_count = @intCast(describe.result_state.oids.len),
                .result_state = describe.result_state,
                .name = opts.cache_name.?,
            };
        }

        // Should only be called in an error case. In a normal case, where
        // stmt.execute() returns a result, stmt.deinit() must not be called (all
        // ownership is passed to the result).
        pub fn deinit(self: *@This()) void {
            self.allocator.free(self.buffer);
        }

        fn writeAllFromBuffer(self: *@This()) !void {
            try self.conn.write(self.buffer[0 .. self.writer.end]);
            self.writer.end = 0;
        }

        fn writePrepareCommands(self: *@This(), sql: []const u8) !void {
            const bind_payload_len = 8 + sql.len + self.name.len;
            const describe_payload_len = 6 + self.name.len;

            // PARSE
            try self.writer.writeByte('P');
            try self.writer.writeInt(u32, @intCast(bind_payload_len), .big);
            try self.writer.writeAll(self.name);
            try self.writer.writeByte(0);
            try self.writer.writeAll(sql);
            // null terminate sql string, and we'll be specifying 0 parameter types
            try self.writer.writeAll(&.{ 0, 0, 0 });

            // DESCRIBE
            try self.writer.writeByte('D');
            try self.writer.writeInt(u32, @intCast(describe_payload_len), .big);
            try self.writer.writeByte('S'); // Describe a prepared statement
            try self.writer.writeAll(self.name);
            try self.writer.writeByte(0); // null terminate our name

            // SYNC
            try self.writer.writeAll(&.{ 'S', 0, 0, 0, 4 });

            try self.writeAllFromBuffer();
        }

        // (in conn.preparedstatements).
        pub fn prepare(self: *@This(), sql: []const u8) !void {
            var conn = self.conn;
            const opts = &self.opts;

            try conn.reader.startFlow(opts.timeout_ms);

            try self.writePrepareCommands(sql);

            // no longer idle, we're now in a query
            conn.state = .query;

            // First message we expect back is a ParseComplete, which has no data.
        {
                // If Parse fails, then the server won't reply to our other messages
                // (i.e. Describe) and it'l immediately send a ReadyForQuery.
                const msg = conn.read() catch |err| {
                    if (err == error.PG) try conn.recoverFromError();
                    return err;
                };

                if (msg.type != '1') {
                    return conn.unexpectedDBMessage();
                }
            }

            var param_count: u16 = 0;

        {
                // we expect a ParameterDescription message
                const msg = try conn.read();
                if (msg.type != 't') {
                    return conn.unexpectedDBMessage();
                }

                const data = msg.data;
                param_count = std.mem.readInt(u16, data[0..2], .big);
                if (self.name.len > 0) {
                    self.result_state = try Result.State.init(self.allocator, param_count);
                } else {
                    if (conn.param_oids.len < param_count) {
                        self.allocator.free(conn.param_oids);
                        conn.param_oids = try self.allocator.alloc(i32, param_count);
                    }
                    self.param_oids = conn.param_oids;
                }

                var pos: usize = 2;
                for (0..param_count) |i| {
                    const end = pos + 4;
                    self.param_oids[i] = std.mem.readInt(i32, data[pos..end][0..4], .big);
                    pos = end;
                }
                self.param_count = param_count;
            }

        {
                // We now expect an answer to our describe message.
                // This is either going to be a RowDescription, or a NoData. NoData means
                // our statement doesn't return any data. Either way, we're going to use
                // this information when we generate our Bind message, next.
                const msg = try conn.read();
                switch (msg.type) {
                    'n' => {}, // no data, column_count = 0
                    'T' => {
                        const data = msg.data;
                        const column_count = std.mem.readInt(u16, data[0..2], .big);

                        if (self.name.len > 0) {
                            self.result_state = try Result.State.init(self.allocator, column_count);
                        } else {
                            if (conn.result_state.capacity < column_count) {
                                conn.result_state.deinit(self.allocator);
                                conn.result_state = try Result.State.init(self.allocator, column_count);
                            }
                            self.result_state = conn.result_state;
                        }
                        try self.result_state.from(self.allocator, column_count, data);

                        if (self.name.len == 0) {
                            conn.result_state = self.result_state;
                        }
                        self.column_count = column_count;
                    },
                    else => return conn.unexpectedDBMessage(),
                }
            }

            return self.prepareForBind(param_count);
        }

        // We need to call Bind for every value we're binding. Rather than having
        // to check "is this the first call to bind" each time, we make it the caller's
        // responsibility to "prepareForBind" upfront.
        pub fn prepareForBind(self: *@This(), param_count: u16) !void {
            try self.conn.readyForQuery();

            const name = self.name;

            // Bind command = 'B'
            // 4 byte length placeholder - 0, 0, 0, 0
            // portal name (empty string, length 0) - 0
            // prepared statement name  + null terminator
            const n_times = param_count * 2;

            // length of buffer is guaranteed to be 128, so it's safe to use
            // writeAssumeCapacity (4 byte length placeholder, 1 byte empty portal)
            try self.writer.writeAll(&.{ 'B', 0, 0, 0, 0, 0 });

            try self.writer.writeAll(name);
            try self.writer.writeByte(0);

            // number of parameters types we're sending a
            try self.writer.writeInt(u16, param_count, .big);

            // the format (text or binary) of each parameter. We'll default to text
            // for now, and fill this in as we get the data
            var i: u32 = 0;
            while (i < n_times) : (i += 1) {
                try self.writer.writeByte(0);
            }

            // number of parameters we're sending a
            try self.writer.writeInt(u16, param_count, .big);
        }

        pub fn bind(self: *@This(), writer: *Io.Writer, value: anytype) !void {
            const name = self.name;

            const param_index = self.param_index;
            assert(param_index < self.param_count);

            // We tell PostgreSQL the format (text or binary) of each parameter. This
            // information is at the start of the message, always starts at byte 9
            // and each value is 2 bytes.
            const format_offset = 9 + (param_index * 2) + name.len;

            try types.bindValue(@TypeOf(value), self.param_oids[param_index], value, writer, format_offset);
            self.param_index = param_index + 1;
        }

        pub fn execute(self: *@This()) !*Result {
            assert(self.param_index == self.param_count);

            // The last part of the bind message is telling PostgreSQL the format we
            // want to receive the result columns in.
            try types.resultEncoding(self.result_state.oids[0..self.column_count], &self.writer);

            // write the full payload length, which always starts at byte 1 (after
            // the 'B' message type)
            // Reaching directly into buf.buf is bad!
            // -1 because the length doesn't include the 'B'
            mem.writeInt(u32, self.buffer[1 .. 5], @intCast(self.writer.end - 1), .big);

            try self.writer.writeAll(&.{
                'E',
                // message length
                0,
                0,
                0,
                9,
                // unname portal
                0,
                // no row limit
                0,
                0,
                0,
                0,
                // sync
                'S',
                // message length
                0,
                0,
                0,
                4,
            });

            try self.writeAllFromBuffer();

            const msg = self.conn.read() catch |err| {
                if (err == error.PG) try self.conn.recoverFromError();
                return err;
            };
            if (msg.type != '2') {
                // expecting a BindComplete
                return self.conn.unexpectedDBMessage();
            }

            try self.conn.peekForError();

            // our call to readyForQuery above changed the state, but as far as we're
            // concerned, we're still doing the query.
            self.conn.state = .query;

            const opts = &self.opts;
            const state = self.result_state;
            const column_count = self.column_count;

            const result = try self.allocator.create(Result);
            result.* = .{
                .conn = self.conn,
                .oids = state.oids[0..column_count],
                .values = state.values[0..column_count],
                .column_names = if (opts.column_names and state.names != null) state.names.?[0..column_count] else &[_][]const u8{},
                .number_of_columns = column_count,
            };
            return result;
        }

        pub fn endStmt(self: *@This()) void {
            self.conn.reader.endFlow() catch {
                // this can only fail in extreme conditions (OOM) and it will only impact
                // the next query (and if the app is using the pool, the pool will try to
                // recover from this anyways)
                self.conn.state = .fail;
            };
        }


        pub const Describe = struct {
            param_oids: []i32,
            result_state: Result.State,
        };
    };
}

const MockConn = struct {
    state: State,
    reader: Reader,
    param_oids: []i32,
    result_state: Result.State,

    pub fn init(allocator: mem.Allocator) !@This() {
        const result_state = try Result.State.init(allocator, 32);
        errdefer result_state.deinit(allocator);

        const param_oids = try allocator.alloc(i32, 32);
        errdefer param_oids.free(allocator);

        return .{
            .state = .idle,
            .reader = undefined,
            .param_oids = param_oids,
            .result_state = result_state
        };
    }

    pub fn deinit(self: *@This(), allcator: mem.Allocator) void {
        allcator.free(self.param_oids);
        self.result_state.deinit(allcator);
    }

    pub fn peekForError(self: *@This()) !void {
        _ = self;
    }

    pub fn unexpectedDBMessage(self: *@This()) error{UnexpectedDBMessage} {
        _ = self;
    }
    
    pub fn recoverFromError(self: *@This()) error{Canceled}!void {
        _ = self;
    }

    pub fn read(self: *@This()) !Message {
        _ = self;
    }

    pub fn readyForQuery(self: *@This()) !void {
        _ = self;
    }

    pub fn write(self: *@This(), data: []const u8) !void {
        _ = self;
        _ = data;
    }
};

const TestStmt = StmtT(MockConn);

test "writePrepareCommands: ensure correct output" {
    const allocator = testing.allocator;

    var conn = try MockConn.init(allocator);
    defer conn.deinit(allocator);

    var stmt = try TestStmt.init(allocator, &conn, .{ .column_names = true });
    defer stmt.deinit();

    var reader = Io.Reader.fixed(stmt.buffer);

    const sql = (
    \\ SELECT
    \\   a.attname AS column_name,
    \\   COALESCE((SELECT string_agg(c.contype::text, '') FROM pg_constraint c WHERE a.attnum = ANY(c.conkey) AND c.conrelid = a.attrelid), '') AS constraint_types
    \\ FROM pg_attribute a
    \\ WHERE a.attrelid = 'addresses'::regclass AND a.attnum > 0 AND NOT a.attisdropped
    \\ ORDER BY a.attnum;
    );

    try stmt.writePrepareCommands(sql);

    const bind_payload_len = 8 + sql.len;
    const describe_payload_len = 6;

    try testing.expectEqual('P', try reader.takeByte());
    try testing.expectEqual(bind_payload_len, try reader.takeInt(u32, .big));
    try testing.expectEqualSlices(u8, &.{}, (try reader.takeDelimiter(0)).?);
    try testing.expectEqualStrings(sql, try reader.take(sql.len));
    try testing.expectEqualSlices(u8, &.{ 0, 0, 0 }, try reader.take(3));

    // DESCRIBE
    try testing.expectEqual('D', try reader.takeByte());
    try testing.expectEqual(describe_payload_len, try reader.takeInt(u32, .big));
    try testing.expectEqual('S', try reader.takeByte());
    try testing.expectEqualSlices(u8, &.{}, (try reader.takeDelimiter(0)).?);

    // SYNC
    try testing.expectEqualSlices(u8, &.{ 'S', 0, 0, 0, 4 }, try reader.take(5));
}
