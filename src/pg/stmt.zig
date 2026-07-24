const std = @import("std");
const lib = @import("lib.zig");
const Buffer = @import("buffer").Buffer;

const mem = std.mem;

const types = lib.types;
const Conn = lib.Conn;
const Result = lib.Result;

pub const Stmt = struct {
    allocator: mem.Allocator,
    buf: *Buffer,

    opts: Conn.QueryOpts,

    conn: *Conn,

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

    pub fn init(allocator: mem.Allocator, conn: *Conn, opts: Conn.QueryOpts) !Stmt {
        return .{
            .conn = conn,
            .opts = opts,
            .buf = &conn.buf,
            .allocator = allocator,
            .param_index = 0,
            .param_count = 0,
            .param_oids = conn.param_oids,
            .column_count = 0,
            .result_state = conn.result_state,
            .name = opts.cache_name orelse "",
        };
    }

    pub fn fromDescribe(allocator: mem.Allocator, conn: *Conn, describe: *Describe, opts: Conn.QueryOpts) !Stmt {
        return .{
            .conn = conn,
            .opts = opts,
            .allocator = allocator,
            .buf = &conn.buf,
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
        self.allocator.free(self.param_oids);
    }

    // (in conn.preparedstatements).
    pub fn prepare(self: *@This(), sql: []const u8) !void {
        var conn = self.conn;
        const opts = &self.opts;

        try conn.reader.startFlow(opts.timeout_ms);

        var buf = self.buf;
        buf.reset();

        const name = self.name;

        // This function will issue 3 commands: Parse, Describe, Sync
        // We need the response from describe to put together our Bind message.
        // Specifically, describe will tell us the type of the return columns, and
        // in Bind, we tell the server how we want it to encode each column (text
        // or binary) and to do that, we need to know what they are.
        {
            // Build the payload from our 3 commands

            // We can calculate exactly how many bytes our 3 messages are going to be
            // and make sure our buffer is big enough, thus avoiding some unecessary
            // bound checking
            const bind_payload_len = 8 + sql.len + name.len;
            const describe_payload_len = 6 + name.len;
            const sync_payload_len = 4;

            // the +3 for the initial byte message for each of the 3 messages
            const total_length = 3 + bind_payload_len + describe_payload_len + sync_payload_len;

            try buf.ensureTotalCapacity(total_length);
            var view = buf.skip(total_length) catch unreachable;

            // PARSE
            view.writeByte('P');
            view.writeIntBig(u32, @intCast(bind_payload_len));
            view.write(name);
            view.writeByte(0);
            view.write(sql);
            // null terminate sql string, and we'll be specifying 0 parameter types
            view.write(&.{ 0, 0, 0 });

            // DESCRIBE
            view.writeByte('D');
            view.writeIntBig(u32, @intCast(describe_payload_len));
            view.writeByte('S'); // Describe a prepared statement
            view.write(name);
            view.writeByte(0); // null terminate our name

            // SYNC
            view.write(&.{ 'S', 0, 0, 0, 4 });
            try conn.write(buf.string());
        }

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

        var buf = self.buf;
        buf.resetRetainingCapacity();

        const name = self.name;

        // Bind command = 'B'
        // 4 byte length placeholder - 0, 0, 0, 0
        // portal name (empty string, length 0) - 0
        // prepared statement name  + null terminator
        try buf.ensureTotalCapacity(1 + 4 + 1 + name.len + 1 + 2);

        // length of buffer is guaranteed to be 128, so it's safe to use
        // writeAssumeCapacity (4 byte length placeholder, 1 byte empty portal)
        buf.writeAssumeCapacity(&.{ 'B', 0, 0, 0, 0, 0 });

        buf.writeAssumeCapacity(name);
        buf.writeByteAssumeCapacity(0);

        // number of parameters types we're sending a
        try buf.writeIntBig(u16, param_count);

        // the format (text or binary) of each parameter. We'll default to text
        // for now, and fill this in as we get the data
        try buf.writeByteNTimes(0, param_count * 2);

        // number of parameters we're sending a
        try buf.writeIntBig(u16, param_count);
    }

    pub fn bind(self: *@This(), value: anytype) !void {
        const name = self.name;

        const param_index = self.param_index;
        lib.assert(param_index < self.param_count);

        // We tell PostgreSQL the format (text or binary) of each parameter. This
        // information is at the start of the message, always starts at byte 9
        // and each value is 2 bytes.
        const format_offset = 9 + (param_index * 2) + name.len;

        try types.bindValue(@TypeOf(value), self.param_oids[param_index], value, self.buf, format_offset);
        self.param_index = param_index + 1;
    }

    pub fn execute(self: *@This()) !*Result {
        lib.assert(self.param_index == self.param_count);

        // We haven't sent our `bind` message yet. We need to finish it, and then
        // send it, along with our `Execute` and a final `Sync` message.

        const buf = self.buf;
        const conn = self.conn;

        // The last part of the bind message is telling PostgreSQL the format we
        // want to receive the result columns in.
        try lib.types.resultEncoding(self.result_state.oids[0..self.column_count], buf);

        // write the full payload length, which always starts at byte 1 (after
        // the 'B' message type)
        // Reaching directly into buf.buf is bad!
        // -1 because the length doesn't include the 'B'
        std.mem.writeInt(u32, buf.buf[1..5], @intCast(buf.len() - 1), .big);

        try buf.write(&.{
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

        try conn.write(buf.string());

        {
            const msg = conn.read() catch |err| {
                if (err == error.PG) try conn.recoverFromError();
                return err;
            };
            if (msg.type != '2') {
                // expecting a BindComplete
                return conn.unexpectedDBMessage();
            }
        }

        try conn.peekForError();

        // our call to readyForQuery above changed the state, but as far as we're
        // concerned, we're still doing the query.
        conn.state = .query;

        lib.metrics.query();

        const opts = &self.opts;
        const state = self.result_state;
        const column_count = self.column_count;

        const result = try self.allocator.create(Result);
        result.* = .{
            .conn = conn,
            .release_conn = opts.release_conn,
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
