const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const pg = @import("pg");
const types = @import("types.zig");

pub const PgClientError = error{
PostgresReplicationError,
WalConnectionNotInitialized,
DefaultConnectionNotInitialized,
UnableToSendCopyDone,
InvalidReadyForQueryMessage,
InvalidCopyDoneMessage,
TransactionErrorState,
TransactionStateUnknown,
WalConnectionUnableToStop,
};

pub const ReadResponse = struct {
    data: ?types.AuditEntry,
    timestamp: ?i64,
    message: pg.packet.ServerPacket,

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        if (self.data) |*entry| {
            entry.deinit(allocator);
        }
    }
};

pub const ParseResponse = struct {
    data: ?types.AuditEntry,
    last_lsn: ?u64,
    timestamp: ?u64,

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        if (self.data) |*entry| {
            entry.deinit(allocator);
        }
    }
};

pub const TransactionContext = struct {
    xid: u32,
    user_id: []const u8,
    ip_address: []const u8,
    primary_key: []const u8,
    changed_columns: std.StringHashMap(types.ChangedColumns),
};

pub const ColumnDef = struct {
    name: []const u8,
    is_key: bool,
};

pub const TableDef = struct {
    namespace: []const u8,
    name: []const u8,
    // indicates whether the column is pk or not.
    columns: std.ArrayList(ColumnDef),
};

pub const PgClient = struct {
    allocator: mem.Allocator,
    io: Io,

    default_opts: pg.conn.Opts,
    wal_opts: pg.conn.Opts,

    last_lsn: u64,
    last_timestamp: i64,

    context: TransactionContext,
    table_reg: std.hash_map.HashMap(u32, TableDef, std.hash_map.AutoContext(u32), 80),

    default_conn: ?*pg.Conn,
    wal_conn: ?*pg.Conn,

    pub fn init(io: Io, allocator: mem.Allocator, opts: pg.conn.Opts) !PgClient{
        var wal_opts = opts;

        wal_opts.startup_parameters = .init(allocator);
        try wal_opts.startup_parameters.ensureUnusedCapacity(opts.startup_parameters.count() + 1);

        var it = opts.startup_parameters.iterator();
        while (it.next()) |entry| {
            wal_opts.startup_parameters.putAssumeCapacity(entry.key_ptr.*, entry.value_ptr.*);
        }

        wal_opts.startup_parameters.putAssumeCapacity("replication", "database");

        var default_conn = try createConn(allocator, io, opts);
        errdefer default_conn.deinit();

        var wal_conn = try createConn(allocator, io, wal_opts);
        errdefer wal_conn.deinit();

        var changed_columns = std.StringHashMap(types.ChangedColumns).init(allocator);
        try changed_columns.ensureUnusedCapacity(16);

        return .{
            .allocator = allocator,
            .io = io,
            .default_opts = opts,
            .wal_opts = wal_opts,
            .last_lsn = 0,
            .last_timestamp = 0,
            .context = .{
                .xid = 0,
                .user_id = "",
                .ip_address = "",
                .primary_key = "",
                .changed_columns = changed_columns,
            },
            .table_reg = std.AutoHashMap(u32, TableDef).init(allocator),
            .wal_conn = wal_conn,
            .default_conn = default_conn,
        };
    }

    pub fn deinit(self: *@This()) void {
        if (self.wal_conn) |wal_conn| {
            wal_conn.deinit();
            self.allocator.destroy(wal_conn);
        }

        if (self.default_conn) |default_conn| {
            default_conn.deinit();
            self.allocator.destroy(default_conn);
        }

        var it = self.table_reg.valueIterator();
        while (it.next()) |table| {
            for (table.columns.items) |col| {
                self.allocator.free(col.name);
            }
            table.columns.deinit(self.allocator);
            self.allocator.free(table.namespace);
            self.allocator.free(table.name);
        }

        self.table_reg.deinit();
        self.context.changed_columns.deinit();
        if (self.context.user_id.len > 0) self.allocator.free(self.context.user_id);
        if (self.context.ip_address.len > 0) self.allocator.free(self.context.ip_address);
    }

    pub fn cancel(self: *@This()) void {
        if (self.wal_conn) |conn| conn.cancel();
        if (self.default_conn) |conn| conn.cancel();
    }

    fn resetContext(self: *@This()) void {
        self.context.xid = 0;
        if (self.context.user_id.len > 0) self.allocator.free(self.context.user_id);
        self.context.user_id = "";
        if (self.context.ip_address.len > 0) self.allocator.free(self.context.ip_address);
        self.context.ip_address = "";
        self.context.primary_key = "";
        self.context.changed_columns.clearRetainingCapacity();
    }

    pub fn startWALReader(self: *@This(), timeout_ms: i32) !void {
        if (self.default_conn == null) return PgClientError.DefaultConnectionNotInitialized;
        if (self.wal_conn == null) return PgClientError.WalConnectionNotInitialized;

        const query = try std.fmt.allocPrint(self.allocator, "START_REPLICATION SLOT {s} LOGICAL 0/0 (proto_version '1', publication_names 'db_pub', messages 'true');", .{self.wal_opts.wal});

        const msg_len: u32 = @as(u32, @intCast(query.len)) + 4 + 1;
        var len_buf: [4]u8 = undefined;
        mem.writeInt(u32, &len_buf, msg_len, .big);

        if (self.wal_conn) |wal_conn| {
            try wal_conn.write("Q");
            try wal_conn.write(&len_buf);
            try wal_conn.write(query);
            try wal_conn.write(&[_]u8{0});
        } else {
            return PgClientError.WalConnectionNotInitialized;
        }

        try self.wal_conn.?.reader.startFlow(timeout_ms);
    }

    pub fn endWALReader(self: *@This()) !void {
        if (self.default_conn == null) return PgClientError.DefaultConnectionNotInitialized;
        if (self.wal_conn == null) return PgClientError.WalConnectionNotInitialized;

        const query = try std.fmt.allocPrint(self.allocator, "DROP_REPLICATION SLOT {s};", .{self.wal_opts.wal});

        const msg_len: u32 = @as(u32, @intCast(query.len)) + 4 + 1;
        var len_buf: [4]u8 = undefined;
        mem.writeInt(u32, &len_buf, msg_len, .big);

        if (self.wal_conn) |wal_conn| {
            try wal_conn.write("Q");
            try wal_conn.write(&len_buf);
            try wal_conn.write(query);
            try wal_conn.write(&[_]u8{0});
        } else {
            return PgClientError.WalConnectionUnableToStop;
        }

        try self.wal_conn.?.reader.endFlow();
    }

    pub fn readWAL(self: *@This()) !?ReadResponse {
        if (self.wal_conn == null) return PgClientError.WalConnectionNotInitialized;

        const msg = try self.wal_conn.?.reader.next();
        switch (msg.type) {
            'W' => {
                // Server entered COPY BOTH mode,
            },
            'd' => {
                if (msg.data.len == 0) return null;

                var response = ReadResponse{
                    .data = null,
                    .timestamp = null,
                    .message = undefined,
                };

                const data_type = msg.data[0];

                switch (data_type) {
                    'w' => {
                        response.message = pg.packet.ServerPacket.XLogData;

                        assert(msg.data.len >= 25);

                        const start_lsn = mem.readInt(u64, msg.data[1..9][0..8], .big);
                        const server_timestamp = mem.readInt(i64, msg.data[17..25][0..8], .big);

                        self.last_timestamp = server_timestamp;

                        const payload = msg.data[25..];

                        const parse_response = try self.parsePgOutput(payload);

                        if (parse_response.last_lsn) |lsn| {
                            response.timestamp = pgWalToClickHouseMs(parse_response.timestamp.?);
                            if (lsn > self.last_lsn) {
                                self.last_lsn = lsn;
                            }
                        } else {
                            if (start_lsn > self.last_lsn) {
                                self.last_lsn = start_lsn;
                            }
                        }

                        if (parse_response.data) |entry| {
                            response.data = entry;
                        }

                        // Proactively acknowledge this processed WAL chunk
                        try self.wal_conn.?.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);
                    },
                    'k' => {
                        response.message = pg.packet.ServerPacket.Keepalive;

                        assert(msg.data.len >= 18);

                        const current_lsn = mem.readInt(u64, msg.data[1..9][0..8], .big);
                        const server_timestamp = mem.readInt(i64, msg.data[9..17][0..8], .big);
                        const reply_requested = msg.data[17];

                        if (current_lsn > self.last_lsn) {
                            self.last_lsn = current_lsn;
                        }
                        if (server_timestamp > self.last_timestamp) {
                            self.last_timestamp = server_timestamp;
                        }

                        if (reply_requested == 1) {
                            try self.wal_conn.?.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);
                        }
                    },
                    else => {}
                }
                return response;
            },
            'E' => {
                const pg_err = pg.Error.init(msg.data);
                const err_msg = try std.fmt.allocPrint(self.allocator, "Error from server! Code: {s}, Message: {s}\n", .{pg_err.code, pg_err.message});
                try std.Io.File.stderr().writeStreamingAll(self.io, err_msg);
                return PgClientError.PostgresReplicationError;
            },
            'c' => {
                assert(msg.len == 4);

                return .{
                    .message = pg.packet.ServerPacket.CopyDone,
                    .data = null,
                    .timestamp = null,
                };
            },
            'C' => {
                return .{
                    .message = pg.packet.ServerPacket.CommandComplete,
                    .data = null,
                    .timestamp = null,
                };
            },
            'Z' => {
                const transaction_status = msg.data[0..1][0];

                switch (transaction_status) {
                    'I' => {
                        // Idle
                    },
                    'T' => {
                        // In Transaction
                    },
                    'E' => return PgClientError.TransactionErrorState,
                    else => return PgClientError.TransactionStateUnknown,
                }

                return .{
                    .message = pg.packet.ServerPacket.ReadyForQuery,
                    .data = null,
                    .timestamp = null,
                };
            },
            else => {
                // Ignore other messages
            }
        }
        return null;
    }

    pub fn sendCopyDone(self: *@This()) !void {
        var len_buf: [4]u8 = undefined;
        mem.writeInt(i32, &len_buf, 4, .big);

        if (self.wal_conn) |wal_conn| {
            try wal_conn.write("c");
            try wal_conn.write(&len_buf);
        } else {
            return PgClientError.UnableToSendCopyDone;
        }
    }

    pub fn parsePgOutput(self: *@This(), payload: []const u8) !ParseResponse {
        var response = ParseResponse{
            .last_lsn = null,
            .timestamp = null,
            .data = null,
        };

        if (payload.len == 0) return response;

        var reader = Io.Reader.fixed(payload);

        const msg_type = try reader.takeByte();

        switch (msg_type) {
            'B' => {
                // final lsn
                _ = try reader.takeInt(u64, .big);
                const timestamp = try reader.takeInt(u64, .big);
                const xid = try reader.takeInt(u32, .big);
                self.context.xid = xid;

                _ = timestamp;
            },
            'C' => {
                // flags
                _ = try reader.takeByte();
                // lsn of commit
                _ = try reader.takeInt(u64, .big);
                response.last_lsn = try reader.takeInt(u64, .big);
                response.timestamp = try reader.takeInt(u64, .big);

                self.resetContext();

                return response;
            },
            'R' => {
                // Relation: send before any insert or update
                const rel_id = try reader.takeInt(u32, .big);

                const namespace = try self.allocator.dupe(u8, try reader.takeDelimiterExclusive(0));
                _ = try reader.takeByte();

                const rel_name = try self.allocator.dupe(u8, try reader.takeDelimiterExclusive(0));
                _ = try reader.takeByte();

                const repl_ident = try reader.takeByte();
                _ = repl_ident;

                const num_columns = try reader.takeInt(u16, .big);
                const table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{namespace, rel_name});
                defer self.allocator.free(table_name);

                const columns = try self.readSchemaKeys(table_name);

                var i: u16 = 0;
                while (i < num_columns) : (i += 1) {
                    // flags
                    _ = try reader.takeByte();

                    // col name
                    _ = try reader.takeDelimiterExclusive(0);
                    _ = try reader.takeByte();

                    // type_id
                    _ = try reader.takeInt(u32, .big);

                    // typemod
                    _ = try reader.takeInt(u32, .big);
                }

                try self.table_reg.put(rel_id, .{
                    .namespace = namespace,
                    .name = rel_name,
                    .columns = columns,
                });

            },
            'I' => {
                const rel_id = try reader.takeInt(u32, .big);
                const tuple_type = try reader.takeByte();

                if (tuple_type != 'N') {
                    std.debug.print("Error: Received insert with invalid tuple type: {c}\n", .{tuple_type});
                }


                if (self.table_reg.get(rel_id)) |table| {
                    const new_values = try self.parseTupleData(&reader, table);

                        response.data = types.AuditEntry{
                            .event_time = undefined,
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .new_values = new_values,
                            .old_values = .empty,
                            .action = 1,
                            .changed_columns = try self.context.changed_columns.clone(),
                            .transaction_id = self.context.xid,
                            .user_id = if (self.context.user_id.len > 0) try self.allocator.dupe(u8, self.context.user_id) else "",
                            .ip_address = if (self.context.ip_address.len > 0) try self.allocator.dupe(u8, self.context.ip_address) else "",
                            .primary_key = if (self.context.primary_key.len > 0) try self.allocator.dupe(u8, self.context.primary_key) else "",
                        };
                } else {
                    std.debug.print("Error: Received insert for unknown relation ID {d}\n", .{rel_id});
                }
                self.context.changed_columns.clearRetainingCapacity();
            },
            'U' => {
                const rel_id = try reader.takeInt(u32, .big);
                var tuple_type = try reader.takeByte();

                if (self.table_reg.get(rel_id)) |table| {
                    var old_values: std.StringHashMapUnmanaged([]const u8) = .empty;

                    if (tuple_type == 'O' or tuple_type == 'K') {
                        old_values = try self.parseTupleData(&reader, table);

                        tuple_type = try reader.takeByte();
                    }

                    if (tuple_type == 'N') {
                        const new_values = try self.parseTupleData(&reader, table);
                        response.data = types.AuditEntry{
                            .event_time = undefined,
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .new_values = new_values,
                            .old_values = old_values,
                            .action = 2,
                            .changed_columns = try self.context.changed_columns.clone(),
                            .transaction_id = self.context.xid,
                            .user_id = if (self.context.user_id.len > 0) try self.allocator.dupe(u8, self.context.user_id) else "",
                            .ip_address = if (self.context.ip_address.len > 0) try self.allocator.dupe(u8, self.context.ip_address) else "",
                            .primary_key = if (self.context.primary_key.len > 0) try self.allocator.dupe(u8, self.context.primary_key) else "",
                        };
                    } else {
                        std.debug.print("Error: Expected 'N', got '{c}'\n", .{tuple_type});
                    }
                } else {
                    std.debug.print("Error: Received update for unknown relation ID {d}\n", .{rel_id});
                }

                self.context.changed_columns.clearRetainingCapacity();
            },
            'D' => {
                const rel_id = try reader.takeInt(u32, .big);

                const tuple_type = try reader.takeByte();

                if (self.table_reg.get(rel_id)) |table| {
                    if (tuple_type == 'O' or tuple_type == 'K') {
                        const old_values = try self.parseTupleData(&reader, table);
                        response.data = types.AuditEntry{
                            .event_time = undefined,
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .new_values = .empty,
                            .old_values = old_values,
                            .action = 3,
                            .changed_columns = try self.context.changed_columns.clone(),
                            .transaction_id = self.context.xid,
                            .user_id = if (self.context.user_id.len > 0) try self.allocator.dupe(u8, self.context.user_id) else "",
                            .ip_address = if (self.context.ip_address.len > 0) try self.allocator.dupe(u8, self.context.ip_address) else "",
                            .primary_key = if (self.context.primary_key.len > 0) try self.allocator.dupe(u8, self.context.primary_key) else "",
                        };
                    } else {
                        std.debug.print("Error: Expected 'O' or 'K', got '{c}'\n", .{tuple_type});
                    }
                } else {
                    std.debug.print("Error: Received delete for unknown relation ID {d}\n", .{rel_id});
                }

                self.context.changed_columns.clearRetainingCapacity();
            },
            'M' => {
                const flags = try reader.takeByte();
                const lsn = try reader.takeInt(u64, .big);

                const prefix = try reader.takeDelimiterExclusive(0);
                _ = try reader.takeByte();

                const content_len = try reader.takeInt(u32, .big);

                const content = try reader.take(content_len);

                _ = flags;
                _ = lsn;
                if (mem.eql(u8, prefix, "ergo_meta")) {
                    var it = mem.splitAny(u8, content, ",");

                    const user_id_str = it.next() orelse return error.InvalidMapType;
                    const ip_address_str = it.next() orelse return error.InvalidMapType;

                    var user_id_it = mem.splitAny(u8, user_id_str, ":");
                    var ip_address_it = mem.splitAny(u8, ip_address_str, ":");

                    const user_id_key_str = user_id_it.next() orelse return error.InvalidMapType;
                    const ip_address_key_str = ip_address_it.next() orelse return error.InvalidMapType;

                    const user_id_key = mem.trim(u8, user_id_key_str, " ");
                    const ip_address_key = mem.trim(u8, ip_address_key_str, " ");

                    if (!mem.eql(u8, user_id_key, "\"user_id\"")) {
                        return error.InvalidMapType;
                    }

                    if (!mem.eql(u8, ip_address_key, "\"ip\"")) {
                        return error.InvalidMapType;
                    }

                    const user_id_value_str = user_id_it.next() orelse return error.InvalidMapType;
                    const ip_address_value_str = ip_address_it.next() orelse return error.InvalidMapType;

                    const user_id_value = mem.trim(u8, mem.trim(u8, user_id_value_str, " "), "\"");
                    const ip_address_value = mem.trim(u8, mem.trim(u8, ip_address_value_str, " "), "\"");

                    const check_str = try std.fmt.allocPrint(self.allocator, "{s}: \"{s}\", {s}: \"{s}\"", .{user_id_key, user_id_value, ip_address_key, ip_address_value});
                    defer self.allocator.free(check_str);
                    assert(mem.eql(u8, check_str, content));

                    self.context.user_id = try self.allocator.dupe(u8, user_id_value);
                    self.context.ip_address = try self.allocator.dupe(u8, ip_address_value);
                }
            },
            else => {
                std.debug.print("Unknown pgoutput message type: {c}\n", .{msg_type});
            }
        }

        return response;
    }

    fn parseTupleData(self: *@This(), reader: *Io.Reader, table: TableDef) !std.StringHashMapUnmanaged([]const u8) {
        const num_columns = try reader.takeInt(u16, .big);

        if (num_columns > table.columns.items.len) {
            return error.ColumnMismatch;
        }

        var changes = std.StringHashMapUnmanaged([]const u8).empty;
        errdefer {
            var it = changes.valueIterator();
            while (it.next()) |val| {
                self.allocator.free(val.*);
            }
            changes.deinit(self.allocator);
        }

        for (table.columns.items) |col| {
            const col_type = try reader.takeByte();
            const col_name = col.name;
            const is_pk = col.is_key;

            switch (col_type) {
                'n' => {
                    // Null
                },
                'u' => {
                    // Unchanged TOAST
                },
                't' => {
                    const col_len = try reader.takeInt(u32, .big);

                    const val_buf_raw = try reader.take(col_len);
                    const val_buf = try self.allocator.dupe(u8, val_buf_raw);
                    errdefer self.allocator.free(val_buf);

                    try changes.put(self.allocator, col_name, val_buf);
                    const changed_column = try self.context.changed_columns.getOrPut(col_name);

                    if (!changed_column.found_existing) {
                        changed_column.value_ptr.* = .{
                            .value = val_buf,
                            .has_changes = true,
                        };
                    } else if (mem.eql(u8, changed_column.value_ptr.*.value, val_buf)) {
                        changed_column.value_ptr.*.has_changes = false;
                    }

                    if (is_pk) {
                        self.context.primary_key = val_buf;
                    }
                },
                else => return error.UnknownTupleFormat,
            }
        }
        return changes;
    }

    fn readSchemaKeys(self: *@This(), table_name: []const u8) !std.ArrayList(ColumnDef) {
        if (self.default_conn == null) return PgClientError.DefaultConnectionNotInitialized;

        const query = try std.fmt.allocPrint(self.allocator,
        \\ SELECT
        \\   a.attname AS column_name,
        \\   COALESCE((SELECT string_agg(c.contype::text, '') FROM pg_constraint c WHERE a.attnum = ANY(c.conkey) AND c.conrelid = a.attrelid), '') AS constraint_types
        \\ FROM pg_attribute a
        \\ WHERE a.attrelid = '{s}'::regclass AND a.attnum > 0 AND NOT a.attisdropped
        \\ ORDER BY a.attnum;
        , .{ table_name });
        defer self.allocator.free(query);

        var result = try self.default_conn.?.query(query, .{ .column_names = true });
        defer result.deinit(self.allocator);

        var columns = std.ArrayList(ColumnDef).empty;
        const column_name_index = result.columnIndex("column_name").?;
        const constraint_types_index = result.columnIndex("constraint_types").?;
        while (try result.next()) |row| {
            const column_name_raw = try row.get([]const u8, column_name_index);
            const column_name = try self.allocator.dupe(u8, column_name_raw);

            const constraint_types = try row.get([]const u8, constraint_types_index);
            try columns.append(self.allocator, .{ .name = column_name, .is_key = mem.containsAtLeast(u8, constraint_types, 1, "p") });
        }
        return columns;
    }

    pub fn createConn(allocator: mem.Allocator, io:  Io, opts: pg.conn.Opts) !*pg.Conn {
        var conn = try allocator.create(pg.Conn);
        conn.* = pg.Conn.init(io, allocator, opts) catch |err| {
            std.debug.print("Failed to connect: {}\n", .{err});
            return err;
        };
        errdefer allocator.destroy(conn);

        conn.auth() catch |err| {
            if (conn.err) |pg_err| {
                std.debug.print("Failed to auth: {} {s}: {s}\n", .{err, pg_err.code, pg_err.message});
            } else {
                std.debug.print("Failed to auth: {}\n", .{err});
            }
            return err;
        };

        return conn;
    }

    pub fn pgWalToClickHouseMs(pg_wal_us: u64) i64 {
        const seconds_between_epochs: u64 = 946_684_800;
        const us_between_epochs: u64 = seconds_between_epochs * 1_000_000;

        const unix_us: u64 = pg_wal_us + us_between_epochs;

        const unix_ms: u64 = unix_us / 1000;

        return @intCast(unix_ms);
    }
};

fn setupMockClient(allocator: mem.Allocator, io: Io) !PgClient {
    var cols = std.ArrayList(ColumnDef).empty;
    try cols.ensureUnusedCapacity(allocator, 6);
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "id"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "address_line_1"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "address_line_2"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "postal_code"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "city"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "country"), .is_key = true });

    var table_reg = std.AutoHashMap(u32, TableDef).init(allocator);
    try table_reg.put(16390, .{
        .namespace = try allocator.dupe(u8, "public"),
        .name = try allocator.dupe(u8, "addresses"),
        .columns = cols,
    });

    var changed_columns = std.StringHashMap(types.ChangedColumns).init(allocator);
    try changed_columns.ensureUnusedCapacity(6);

    return .{
        .allocator = allocator,
        .io = io,
        .default_opts = undefined,
        .wal_opts = undefined,
        .last_lsn = 0,
        .last_timestamp = 0,
        .context = .{
            .xid = 0,
            .user_id = "",
            .ip_address = "",
            .primary_key = "",
            .changed_columns = changed_columns,
        },
        .table_reg = table_reg,
        .wal_conn = null,
        .default_conn = null,
    };
}

test "parsePgOutput maps BEGIN correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    const commit_hex = "420000000001c160880002f9a2afe34ece00000317";
    const xid: u64 = 791;

    const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
    defer allocator.free(commit_bytes);
    _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);

    var result = try client.parsePgOutput(commit_bytes);
    defer result.deinit(allocator);

    try testing.expectEqual(xid, client.context.xid);
    try testing.expectEqualStrings("", client.context.ip_address);
    try testing.expectEqualStrings("", client.context.user_id);
    try testing.expectEqual(null, result.data);
    try testing.expectEqual(null, result.last_lsn);
    try testing.expectEqual(null, result.timestamp);
}

test "parsePgOutput maps METADATA correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    // PERFORM pg_logical_emit_message(
    //     true, 
    //     'ergo_meta', 
    //     '"user_id": "42", "ip": "192.168.1.50"'
    // );
    const metadata_hex = "4d010000000001c15f186572676f5f6d657461000000002522757365725f6964223a20223432222c20226970223a20223139322e3136382e312e353022";

    const metadata_bytes = try allocator.alloc(u8, metadata_hex.len / 2);
    defer allocator.free(metadata_bytes);
    _ = try std.fmt.hexToBytes(metadata_bytes, metadata_hex);

    var result = try client.parsePgOutput(metadata_bytes);
    defer result.deinit(allocator);

    try testing.expectEqualStrings("42", client.context.user_id);
    try testing.expectEqualStrings("192.168.1.50", client.context.ip_address);

    try testing.expectEqual(null, result.data);
    try testing.expectEqual(null, result.last_lsn);
    try testing.expectEqual(null, result.timestamp);
}

test "parsePgOutput maps RELATION correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var startup_parameters: std.StringHashMap([]const u8) = .init(allocator); 
    defer startup_parameters.deinit();

    var client = PgClient{
        .allocator = allocator,
        .io = io,
        .default_opts = undefined,
        .wal_opts = undefined,
        .last_lsn = 0,
        .last_timestamp = 0,
        .context = .{
            .xid = 0,
            .user_id = "",
            .ip_address = "",
            .primary_key = "",
            .changed_columns = .init(allocator),
        },
        .table_reg = .init(allocator),
        .wal_conn = null,
        .default_conn = try PgClient.createConn(allocator, io, .{
            .port = 5432,
            .host = "localhost",
            .wal = "wal_slot",
            .username = "db_rp",
            .password = "12345678",
            .database = "db",
            .timeout_ms = 500,
            .startup_parameters = startup_parameters,
            .application_name = "Ergo test",
        }),
    };
    defer client.deinit();

    // INSERT INTO addresses (address_line_1, postal_code, city, country) 
    // VALUES ('1 Apple Park Way', '95014', 'Cupertino', 'US');
    const insert_hex = "52000040067075626c696300616464726573736573006600060169640000000014ffffffff01616464726573735f6c696e655f3100000004130000010301616464726573735f6c696e655f3200000004130000010301706f7374616c5f636f6465000000041300000014016369747900000004130000010301636f756e747279000000041300000006";

    const insert_bytes = try allocator.alloc(u8, insert_hex.len / 2);
    defer allocator.free(insert_bytes);
    _ = try std.fmt.hexToBytes(insert_bytes, insert_hex);

    var result = try client.parsePgOutput(insert_bytes);
    defer result.deinit(allocator);

    try testing.expectEqual(1, client.table_reg.count());
}

test "parsePgOutput maps INSERT correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    // INSERT INTO addresses (address_line_1, postal_code, city, country) 
    // VALUES ('1 Apple Park Way', '95014', 'Cupertino', 'US');
    const insert_hex = "49000040064e0006740000000131740000001031204170706c65205061726b205761796e740000000539353031347400000009437570657274696e6f74000000025553";

    const insert_bytes = try allocator.alloc(u8, insert_hex.len / 2);
    defer allocator.free(insert_bytes);
    _ = try std.fmt.hexToBytes(insert_bytes, insert_hex);

    var result = try client.parsePgOutput(insert_bytes);
    defer result.deinit(allocator);

    try testing.expectEqual(1, result.data.?.action);
    try testing.expectEqualStrings("public.addresses", result.data.?.table_name);

    // Changed columns
    try testing.expectEqual(true, result.data.?.changed_columns.get("id").?.has_changes);
    try testing.expectEqualStrings("1", result.data.?.changed_columns.get("id").?.value);

    try testing.expectEqual(true, result.data.?.changed_columns.get("address_line_1").?.has_changes);
    try testing.expectEqualStrings("1 Apple Park Way", result.data.?.changed_columns.get("address_line_1").?.value);

    try testing.expectEqual(null, result.data.?.changed_columns.get("address_line_2"));

    try testing.expectEqual(true, result.data.?.changed_columns.get("postal_code").?.has_changes);
    try testing.expectEqualStrings("95014", result.data.?.changed_columns.get("postal_code").?.value);

    try testing.expectEqual(true, result.data.?.changed_columns.get("city").?.has_changes);
    try testing.expectEqualStrings("Cupertino", result.data.?.changed_columns.get("city").?.value);

    try testing.expectEqual(true, result.data.?.changed_columns.get("country").?.has_changes);
    try testing.expectEqualStrings("US", result.data.?.changed_columns.get("country").?.value);

    // Old values
    try testing.expectEqual(0, result.data.?.old_values.capacity());

    // New values
    try testing.expectEqualStrings("1", result.data.?.new_values.get("id").?);
    try testing.expectEqualStrings("1 Apple Park Way", result.data.?.new_values.get("address_line_1").?);
    try testing.expectEqual(null, result.data.?.new_values.get("address_line_2"));
    try testing.expectEqualStrings("95014", result.data.?.new_values.get("postal_code").?);
    try testing.expectEqualStrings("Cupertino", result.data.?.new_values.get("city").?);
    try testing.expectEqualStrings("US", result.data.?.new_values.get("country").?);

    try testing.expectEqual(0, client.context.xid);
    try testing.expectEqualStrings("", client.context.ip_address);
    try testing.expectEqualStrings("", client.context.user_id);
    try testing.expectEqual(null, result.last_lsn);
    try testing.expectEqual(null, result.timestamp);
}

test "parsePgOutput maps UPDATE correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    // UPDATE addresses SET 
    //  address_line_1 = 'Googleplex',
    //  city = 'Mountain View',
    //  postal_code = '94043'
    // WHERE id = 1;
    const update_hex = "55000040064f0006740000000131740000001031204170706c65205061726b205761796e740000000539353031347400000009437570657274696e6f740000000255534e0006740000000131740000000a476f6f676c65706c65786e74000000053934303433740000000d4d6f756e7461696e205669657774000000025553";

    const update_bytes = try allocator.alloc(u8, update_hex.len / 2);
    defer allocator.free(update_bytes);
    _ = try std.fmt.hexToBytes(update_bytes, update_hex);

    var result = try client.parsePgOutput(update_bytes);
    defer result.deinit(allocator);

    try testing.expectEqual(2, result.data.?.action);
    try testing.expectEqualStrings("public.addresses", result.data.?.table_name);

    // Changed columns
    try testing.expectEqual(false, result.data.?.changed_columns.get("id").?.has_changes);
    try testing.expectEqualStrings("1", result.data.?.changed_columns.get("id").?.value);

    try testing.expectEqual(true, result.data.?.changed_columns.get("address_line_1").?.has_changes);
    try testing.expectEqualStrings("1 Apple Park Way", result.data.?.changed_columns.get("address_line_1").?.value);

    try testing.expectEqual(null, result.data.?.changed_columns.get("address_line_2"));

    try testing.expectEqual(true, result.data.?.changed_columns.get("postal_code").?.has_changes);
    try testing.expectEqualStrings("95014", result.data.?.changed_columns.get("postal_code").?.value);

    try testing.expectEqual(true, result.data.?.changed_columns.get("city").?.has_changes);
    try testing.expectEqualStrings("Cupertino", result.data.?.changed_columns.get("city").?.value);

    try testing.expectEqual(false, result.data.?.changed_columns.get("country").?.has_changes);
    try testing.expectEqualStrings("US", result.data.?.changed_columns.get("country").?.value);

    // Old values
    try testing.expectEqualStrings("1 Apple Park Way", result.data.?.old_values.get("address_line_1").?);
    try testing.expectEqual(null, result.data.?.old_values.get("address_line_2"));
    try testing.expectEqualStrings("95014", result.data.?.old_values.get("postal_code").?);
    try testing.expectEqualStrings("Cupertino", result.data.?.old_values.get("city").?);
    try testing.expectEqualStrings("US", result.data.?.old_values.get("country").?);

    // New values
    try testing.expectEqualStrings("Googleplex", result.data.?.new_values.get("address_line_1").?);
    try testing.expectEqual(null, result.data.?.new_values.get("address_line_2"));
    try testing.expectEqualStrings("94043", result.data.?.new_values.get("postal_code").?);
    try testing.expectEqualStrings("Mountain View", result.data.?.new_values.get("city").?);
    try testing.expectEqualStrings("US", result.data.?.new_values.get("country").?);

    try testing.expectEqual(0, client.context.xid);
    try testing.expectEqualStrings("", client.context.ip_address);
    try testing.expectEqualStrings("", client.context.user_id);
    try testing.expectEqual(null, result.last_lsn);
    try testing.expectEqual(null, result.timestamp);
}

test "parsePgOutput maps DELETE correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    // DELETE FROM addresses WHERE id = 1;
    const delete_hex = "44000040064f0006740000000131740000000a476f6f676c65706c65786e74000000053934303433740000000d4d6f756e7461696e205669657774000000025553";

    const delete_bytes = try allocator.alloc(u8, delete_hex.len / 2);
    defer allocator.free(delete_bytes);
    _ = try std.fmt.hexToBytes(delete_bytes, delete_hex);

    var result = try client.parsePgOutput(delete_bytes);
    defer result.deinit(allocator);

    try testing.expectEqual(3, result.data.?.action);
    try testing.expectEqualStrings("public.addresses", result.data.?.table_name);

    // Changed columns
    try testing.expectEqual(true, result.data.?.changed_columns.get("id").?.has_changes);
    try testing.expectEqualStrings("1", result.data.?.changed_columns.get("id").?.value);

    try testing.expectEqual(true, result.data.?.changed_columns.get("address_line_1").?.has_changes);
    try testing.expectEqualStrings("Googleplex", result.data.?.changed_columns.get("address_line_1").?.value);

    try testing.expectEqual(null, result.data.?.changed_columns.get("address_line_2"));

    try testing.expectEqual(true, result.data.?.changed_columns.get("postal_code").?.has_changes);
    try testing.expectEqualStrings("94043", result.data.?.changed_columns.get("postal_code").?.value);

    try testing.expectEqual(true, result.data.?.changed_columns.get("city").?.has_changes);
    try testing.expectEqualStrings("Mountain View", result.data.?.changed_columns.get("city").?.value);

    try testing.expectEqual(true, result.data.?.changed_columns.get("country").?.has_changes);
    try testing.expectEqualStrings("US", result.data.?.changed_columns.get("country").?.value);

    // Old values
    try testing.expectEqualStrings("1", result.data.?.old_values.get("id").?);
    try testing.expectEqualStrings("Googleplex", result.data.?.old_values.get("address_line_1").?);
    try testing.expectEqual(null, result.data.?.old_values.get("address_line_2"));
    try testing.expectEqualStrings("94043", result.data.?.old_values.get("postal_code").?);
    try testing.expectEqualStrings("Mountain View", result.data.?.old_values.get("city").?);
    try testing.expectEqualStrings("US", result.data.?.old_values.get("country").?);

    // New values
    try testing.expectEqual(0, result.data.?.new_values.capacity());

    try testing.expectEqual(0, client.context.xid);
    try testing.expectEqualStrings("", client.context.ip_address);
    try testing.expectEqualStrings("", client.context.user_id);
    try testing.expectEqual(null, result.last_lsn);
    try testing.expectEqual(null, result.timestamp);
}

test "parsePgOutput maps COMMIT correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    const insert_hex = "43000000000001c160880000000001c160b80002f9a2afe34ece";
    const last_lsn: u64 = 29450424;
    const commit_timestamp: u64 = 837427084349134;

    const insert_bytes = try allocator.alloc(u8, insert_hex.len / 2);
    defer allocator.free(insert_bytes);
    _ = try std.fmt.hexToBytes(insert_bytes, insert_hex);

    var result = try client.parsePgOutput(insert_bytes);
    defer result.deinit(allocator);

    try testing.expectEqual(0, client.context.xid);
    try testing.expectEqualStrings("", client.context.ip_address);
    try testing.expectEqualStrings("", client.context.user_id);
    try testing.expectEqual(null, result.data);
    try testing.expectEqual(last_lsn, result.last_lsn);
    try testing.expectEqual(commit_timestamp, result.timestamp);
}

test "resetContext clears context correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    client.context.ip_address = try allocator.dupe(u8, "192.168.1.50");
    client.context.user_id = try allocator.dupe(u8, "42");
    client.context.xid = 791;

    const commit_hex = "43000000000001c160880000000001c160b80002f9a2afe34ece";

    const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
    defer allocator.free(commit_bytes);
    _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);

    var result = try client.parsePgOutput(commit_bytes);
    result.deinit(allocator);

    try testing.expectEqual(null, result.data);

    try testing.expectEqual(0, client.context.xid);
    try testing.expectEqualStrings("", client.context.ip_address);
    try testing.expectEqualStrings("", client.context.user_id);
}

test "parseTupleData" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    const commit_hex = "0006740000000131740000001031204170706c65205061726b205761796e740000000539353031347400000009437570657274696e6f74000000025553";

    const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
    defer allocator.free(commit_bytes);
    _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);

    var reader = Io.Reader.fixed(commit_bytes);

    var result = try client.parseTupleData(&reader, client.table_reg.get(16390).?);
    defer {
        var it = result.valueIterator();
        while (it.next()) |val| {
            allocator.free(val.*);
        }
        result.deinit(allocator);
    }

    try testing.expectEqualStrings("1", result.get("id").?);
    try testing.expectEqualStrings("1 Apple Park Way", result.get("address_line_1").?);
    try testing.expectEqual(null, result.get("address_line_2"));
    try testing.expectEqualStrings("95014", result.get("postal_code").?);
    try testing.expectEqualStrings("Cupertino", result.get("city").?);
    try testing.expectEqualStrings("US", result.get("country").?);

    try testing.expectEqual(0, client.context.xid);
    try testing.expectEqualStrings("", client.context.ip_address);
    try testing.expectEqualStrings("", client.context.user_id);
}

test "readSchemaKeys" {
    const allocator = testing.allocator;
    const io = testing.io;

    var startup_parameters: std.StringHashMap([]const u8) = .init(allocator); 
    defer startup_parameters.deinit();

    var client = PgClient{
        .allocator = allocator,
        .io = undefined,
        .context = undefined,
        .default_opts = undefined,
        .wal_opts = undefined,
        .last_lsn = undefined,
        .last_timestamp = undefined,
        .default_conn = try PgClient.createConn(allocator, io, .{
            .port = 5432,
            .host = "localhost",
            .wal = "wal_slot",
            .username = "db_rp",
            .password = "12345678",
            .database = "db",
            .timeout_ms = 500,
            .startup_parameters = startup_parameters,
            .application_name = "Ergo test",
        }),
        .table_reg = undefined,
        .wal_conn = null,
    };
    defer client.deinit();

    var columns = try client.readSchemaKeys("users");
    defer {
        for (columns.items) |col| {
            allocator.free(col.name);
        }
        columns.deinit(allocator);
    }

    try testing.expectEqual(6, columns.items.len);

    for (columns.items) |item|  {
        if (mem.eql(u8, "id", item.name)) { try testing.expectEqual(true, item.is_key); }
        else if (mem.eql(u8, "address_id", item.name)) { try testing.expectEqual(false, item.is_key); }
        else if (mem.eql(u8, "age", item.name)) { try testing.expectEqual(false, item.is_key); }
        else if (mem.eql(u8, "email_address", item.name)) { try testing.expectEqual(false, item.is_key); }
        else if (mem.eql(u8, "gender", item.name)) { try testing.expectEqual(false, item.is_key); }
        else if (mem.eql(u8, "name", item.name)) { try testing.expectEqual(false, item.is_key); }
    }
}

test "update from value to null" {
}
