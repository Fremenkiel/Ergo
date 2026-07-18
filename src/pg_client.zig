const std = @import("std");
const assert = std.debug.assert;

const pg = @import("pg");

const types = @import("types.zig");

pub const PgClientError = error{
PostgresReplicationError,
WalConnectionNotInitialized,
ConnectionPoolNotInitialized,
};

pub const ReadResponse = struct {
    entry: ?types.AuditEntry,
    commit_timestamp: ?i64,
    eof: bool = false,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        if (self.entry) |*entry| {
            entry.deinit(allocator);
        }
    }
};

pub const EOFReadResponse: ReadResponse = .{
    .entry = null,
    .commit_timestamp = null,
    .eof = true,
};

pub const ParseResponse = struct {
    entry: ?types.AuditEntry,
    last_lsn: ?u64,
    commit_timestamp: ?u64,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        if (self.entry) |*entry| {
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

pub const Opts = struct {
    auth: pg.Conn.AuthOpts = .{},
    connect: pg.Conn.Opts = .{},
};

pub const PgClient = struct {
    allocator: std.mem.Allocator,
    io: std.Io,

    conn_opts: Opts,

    last_lsn: u64,
    last_timestamp: i64,

    context: TransactionContext,
    table_reg: std.hash_map.HashMap(u32, TableDef, std.hash_map.AutoContext(u32), 80),

    pool: ?*pg.Pool,
    wal_conn: ?*pg.Conn,

    pub fn init(io: std.Io, allocator: std.mem.Allocator, opts: Opts) !PgClient{
        var pool = try createConnPool(allocator, io, opts);
        errdefer pool.deinit();

        var conn = try createWalConn(allocator, io, opts);
        errdefer conn.deinit();

        var changed_columns = std.StringHashMap(types.ChangedColumns).init(allocator);
        try changed_columns.ensureUnusedCapacity(16);

        return .{
            .allocator = allocator,
            .io = io,
            .conn_opts = opts,
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
            .wal_conn = conn,
            .pool = pool,
        };
    }

    pub fn deinit(self: *PgClient) void {
        if (self.wal_conn) |wal_conn| {
            self.endFlow() catch {};
            wal_conn.deinit();
            self.allocator.destroy(wal_conn);
        }

        if (self.pool) |pool| {
            pool.deinit();
            // self.allocator.destroy(pool);
        }

        var it = self.table_reg.valueIterator();
        while (it.next()) |table| {
            table.columns.deinit(self.allocator);
        }

        self.table_reg.deinit();
        self.context.changed_columns.deinit();
        self.allocator.free(self.context.user_id);
        self.allocator.free(self.context.ip_address);
    }

    fn resetContext(self: *PgClient) void {
        self.context.xid = 0;
        self.context.user_id = "";
        self.context.ip_address = "";
        self.context.primary_key = "";
        self.context.changed_columns.clearRetainingCapacity();
    }

    pub fn startWALReader(self: *PgClient) !void {
        const query = "START_REPLICATION SLOT wal_slot LOGICAL 0/0 (proto_version '1', publication_names 'db_pub', messages 'true')";

        const msg_len: u32 = @as(u32, @intCast(query.len)) + 4 + 1;
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, msg_len, .big);

        if (self.wal_conn) |wal_conn| {
            try wal_conn.write("Q");
            try wal_conn.write(&len_buf);
            try wal_conn.write(query);
            try wal_conn.write(&[_]u8{0});
        } else {
            return PgClientError.WalConnectionNotInitialized;
        }

        try self.startFlow();
    }

    pub fn readWAL(self: *PgClient) !ReadResponse {
        if (self.wal_conn == null) return PgClientError.WalConnectionNotInitialized;

        const msg = try self.wal_conn.?._reader.next();

        var response = ReadResponse{
            .entry = null,
            .commit_timestamp = null,
        };

        switch (msg.type) {
            'W' => {
                // Server entered COPY BOTH mode,
            },
            'd' => {
                if (msg.data.len == 0) return response;

                const data_type = msg.data[0];
                if (data_type == 'w') {
                    if (msg.data.len < 25) return response;

                    const start_lsn = std.mem.readInt(u64, msg.data[1..9][0..8], .big);
                    const server_timestamp = std.mem.readInt(i64, msg.data[17..25][0..8], .big);

                    self.last_timestamp = server_timestamp;

                    const payload = msg.data[25..];

                    const parse_response = try self.parsePgOutput(payload);

                    if (parse_response.last_lsn) |lsn| {
                        response.commit_timestamp = pgWalToClickHouseMs(parse_response.commit_timestamp.?);
                        if (lsn > self.last_lsn) {
                            self.last_lsn = lsn;
                        }
                    } else {
                        if (start_lsn > self.last_lsn) {
                            self.last_lsn = start_lsn;
                        }
                    }

                    if (parse_response.entry) |entry| {
                        response.entry = entry;
                    }

                    // Proactively acknowledge this processed WAL chunk
                    try self.wal_conn.?.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);
                } else if (data_type == 'k') {
                    if (msg.data.len < 18) return response;

                    const current_lsn = std.mem.readInt(u64, msg.data[1..9][0..8], .big);
                    const server_timestamp = std.mem.readInt(i64, msg.data[9..17][0..8], .big);
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
                }
            },
            'E' => {
                const err_msg = pg.Error.parse(msg.data);
                std.debug.print("Error from server! Code: {s}, Message: {s}\n", .{err_msg.code, err_msg.message});
                return PgClientError.PostgresReplicationError;
            },
            else => {
                // Ignore other messages
            }
        }
        return response;
    }

    pub fn parsePgOutput(self: *PgClient, payload: []const u8) !ParseResponse {
        var response = ParseResponse{
            .last_lsn = null,
            .commit_timestamp = null,
            .entry = null,
        };

        if (payload.len == 0) return response;

        var reader = std.Io.Reader.fixed(payload);

        const msg_type = try reader.takeByte();

        switch (msg_type) {
            'B' => {
                // final lsn
                _ = try reader.takeInt(u64, .big);
                const commit_timestamp = try reader.takeInt(u64, .big);
                const xid = try reader.takeInt(u32, .big);
                self.context.xid = xid;

                _ = commit_timestamp;
            },
            'C' => {
                // flags
                _ = try reader.takeByte();
                // lsn of commit
                _ = try reader.takeInt(u64, .big);
                response.last_lsn = try reader.takeInt(u64, .big);
                response.commit_timestamp = try reader.takeInt(u64, .big);

                self.resetContext();

                return response;
            },
            'R' => {
                // Relation: send before any insert or update
                const rel_id = try reader.takeInt(u32, .big);

                const namespace = try self.allocator.dupe(u8, try reader.takeDelimiterExclusive(0));
                defer self.allocator.free(namespace);
                _ = try reader.takeByte();

                const rel_name = try self.allocator.dupe(u8, try reader.takeDelimiterExclusive(0));
                defer self.allocator.free(rel_name);
                _ = try reader.takeByte();

                const repl_ident = try reader.takeByte();
                _ = repl_ident;

                const num_columns = try reader.takeInt(u16, .big);
                const table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{namespace, rel_name});
                defer self.allocator.free(table_name);

                const columns = self.readSchemaKeys(table_name) catch |err| switch (err) {
                    PgClientError.ConnectionPoolNotInitialized => blk: {
                        self.pool = try createConnPool(self.allocator, self.io, self.conn_opts);

                        break :blk try self.readSchemaKeys(table_name);
                    },
                    else => return err,
                };

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

                    response.entry = types.AuditEntry{
                        .event_time = undefined,
                        .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                        .new_values = new_values,
                        .old_values = .empty,
                        .action = 1,
                        .changed_columns = try self.context.changed_columns.clone(),
                        .transaction_id = self.context.xid,
                        .user_id = self.context.user_id,
                        .ip_address = self.context.ip_address,
                        .primary_key = self.context.primary_key,
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
                        response.entry = types.AuditEntry{
                            .event_time = undefined,
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .new_values = new_values,
                            .old_values = old_values,
                            .action = 2,
                            .changed_columns = try self.context.changed_columns.clone(),
                            .transaction_id = self.context.xid,
                            .user_id = self.context.user_id,
                            .ip_address = self.context.ip_address,
                            .primary_key = self.context.primary_key,
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
                        response.entry = types.AuditEntry{
                            .event_time = undefined,
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .new_values = .empty,
                            .old_values = old_values,
                            .action = 3,
                            .changed_columns = try self.context.changed_columns.clone(),
                            .transaction_id = self.context.xid,
                            .user_id = self.context.user_id,
                            .ip_address = self.context.ip_address,
                            .primary_key = self.context.primary_key,
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
                if (std.mem.eql(u8, prefix, "ergo_meta")) {
                    var it = std.mem.splitAny(u8, content, ",");

                    const user_id_str = it.next() orelse return error.InvalidMapType;
                    const ip_address_str = it.next() orelse return error.InvalidMapType;

                    var user_id_it = std.mem.splitAny(u8, user_id_str, ":");
                    var ip_address_it = std.mem.splitAny(u8, ip_address_str, ":");

                    const user_id_key_str = user_id_it.next() orelse return error.InvalidMapType;
                    const ip_address_key_str = ip_address_it.next() orelse return error.InvalidMapType;

                    const user_id_key = std.mem.trim(u8, user_id_key_str, " ");
                    const ip_address_key = std.mem.trim(u8, ip_address_key_str, " ");

                    if (!std.mem.eql(u8, user_id_key, "\"user_id\"")) {
                        return error.InvalidMapType;
                    }

                    if (!std.mem.eql(u8, ip_address_key, "\"ip\"")) {
                        return error.InvalidMapType;
                    }

                    const user_id_value_str = user_id_it.next() orelse return error.InvalidMapType;
                    const ip_address_value_str = ip_address_it.next() orelse return error.InvalidMapType;

                    const user_id_value = std.mem.trim(u8, std.mem.trim(u8, user_id_value_str, " "), "\"");
                    const ip_address_value = std.mem.trim(u8, std.mem.trim(u8, ip_address_value_str, " "), "\"");

                    const check_str = try std.fmt.allocPrint(self.allocator, "{s}: \"{s}\", {s}: \"{s}\"", .{user_id_key, user_id_value, ip_address_key, ip_address_value});
                    defer self.allocator.free(check_str);
                    assert(std.mem.eql(u8, check_str, content));

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

    fn parseTupleData(self: *PgClient, reader: *std.Io.Reader, table: TableDef) !std.StringHashMapUnmanaged([]const u8) {
        const num_columns = try reader.takeInt(u16, .big);

        if (num_columns > table.columns.items.len) {
            return error.ColumnMismatch;
        }

        var changes = std.StringHashMapUnmanaged([]const u8).empty;

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

                    const val_buf = try reader.take(col_len);

                    try changes.put(self.allocator, col_name, val_buf);
                    const changed_column = try self.context.changed_columns.getOrPut(col_name);

                    if (!changed_column.found_existing) {
                        changed_column.value_ptr.* = .{
                            .value = val_buf,
                            .has_changes = true,
                        };
                    } else if (std.mem.eql(u8, changed_column.value_ptr.*.value, val_buf)) {
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

    fn readSchemaKeys(self: *PgClient, table_name: []const u8) !std.ArrayList(ColumnDef) {
        if (self.pool == null) return PgClientError.ConnectionPoolNotInitialized;

        var conn = try self.pool.?.acquire();
        defer conn.release();
        var result = try conn.queryOpts(
        \\ SELECT
\\   a.attname AS column_name,
\\   array_agg(c.contype) AS constraint_types
\\ FROM pg_constraint c
\\ JOIN pg_attribute a ON a.attnum = ANY(c.conkey)
\\   AND a.attrelid = c.conrelid
\\ WHERE c.conrelid = $1::regclass
\\ GROUP BY column_name;
        , .{table_name}, .{ .column_names = true });
        defer result.deinit();

        var columns = std.ArrayList(ColumnDef).empty;
        const column_name_index = result.columnIndex("column_name").?;
        const constraint_types_index = result.columnIndex("constraint_types").?;
        while (try result.next()) |row| {
            const column_name = try row.get([]const u8, column_name_index);
            const constraint_types = try row.get([]const u8, constraint_types_index);
            try columns.append(self.allocator, .{ .name = column_name, .is_key = std.mem.containsAtLeast(u8, constraint_types, 1, "p") });
        }
        return columns;
    }

    pub fn pgWalToClickHouseMs(pg_wal_us: u64) i64 {
        const seconds_between_epochs: u64 = 946_684_800;
        const us_between_epochs: u64 = seconds_between_epochs * 1_000_000;

        const unix_us: u64 = pg_wal_us + us_between_epochs;

        const unix_ms: u64 = unix_us / 1000;

        return @intCast(unix_ms);
    }

    pub fn startFlow(self: *PgClient) !void {
        if (self.wal_conn == null) return PgClientError.WalConnectionNotInitialized;

        try self.wal_conn.?._reader.startFlow(null, null);
    }

    pub fn endFlow(self: *PgClient) !void {
        if (self.wal_conn == null) return PgClientError.WalConnectionNotInitialized;

        try self.wal_conn.?._reader.endFlow();
    }

    pub fn createWalConn(allocator: std.mem.Allocator, io:  std.Io, opts: Opts) !*pg.Conn {
        var conn = try allocator.create(pg.Conn);
        conn.* = pg.Conn.open(io, allocator, opts.connect) catch |err| {
            std.debug.print("Failed to connect: {}\n", .{err});
            std.process.exit(1);
        };

        var authOpts = opts.auth;

        if (authOpts.startup_parameters == null) {
            authOpts.startup_parameters = std.StringHashMap([]const u8).init(allocator);
        }
        try authOpts.startup_parameters.?.put("replication", "database");

        conn.auth(authOpts) catch |err| {
            if (conn.err) |pg_err| {
                std.debug.print("Failed to auth: {} {s}: {s}\n", .{err, pg_err.code, pg_err.message});
            } else {
                std.debug.print("Failed to auth: {}\n", .{err});
            }
            std.process.exit(1);
        };

        return conn;
    }

    pub fn createConnPool(allocator: std.mem.Allocator, io: std.Io, opts: Opts) !*pg.Pool {
        return pg.Pool.init(io, allocator, .{ .size = 1, .connect = opts.connect, .auth = opts.auth}) catch |err| {
            std.debug.print("Failed to connect: {}\n", .{err});
            std.process.exit(1);
        };
    }
};

fn setupMockClient(allocator: std.mem.Allocator, io: std.Io) !PgClient {
    var cols = std.ArrayList(ColumnDef).empty;
    try cols.ensureUnusedCapacity(allocator, 6);
    try cols.append(allocator, .{ .name = "id", .is_key = true });
    try cols.append(allocator, .{ .name = "address_line_1", .is_key = true });
    try cols.append(allocator, .{ .name = "address_line_2", .is_key = true });
    try cols.append(allocator, .{ .name = "postal_code", .is_key = true });
    try cols.append(allocator, .{ .name = "city", .is_key = true });
    try cols.append(allocator, .{ .name = "country", .is_key = true });

    var table_reg = std.AutoHashMap(u32, TableDef).init(allocator);
    try table_reg.put(16390, .{
        .namespace = "public",
        .name = "addresses",
        .columns = cols,
    });

    var changed_columns = std.StringHashMap(types.ChangedColumns).init(allocator);
    try changed_columns.ensureUnusedCapacity(6);

    return .{
        .allocator = allocator,
        .io = io,
        .conn_opts = undefined,
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
        .pool = null,
    };
}

test "parsePgOutput maps BEGIN correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    const commit_hex = "420000000001c160880002f9a2afe34ece00000317";
    const xid: u64 = 791;

    const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
    defer allocator.free(commit_bytes);
    _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);

    var result = try client.parsePgOutput(commit_bytes);
    defer result.deinit(allocator);

    try std.testing.expectEqual(xid, client.context.xid);
    try std.testing.expectEqualStrings("", client.context.ip_address);
    try std.testing.expectEqualStrings("", client.context.user_id);
    try std.testing.expectEqual(null, result.entry);
    try std.testing.expectEqual(null, result.last_lsn);
    try std.testing.expectEqual(null, result.commit_timestamp);
}

test "parsePgOutput maps METADATA correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

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

    try std.testing.expectEqualStrings("42", client.context.user_id);
    try std.testing.expectEqualStrings("192.168.1.50", client.context.ip_address);

    try std.testing.expectEqual(null, result.entry);
    try std.testing.expectEqual(null, result.last_lsn);
    try std.testing.expectEqual(null, result.commit_timestamp);
}

test "parsePgOutput maps RELATION correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = PgClient{
        .allocator = allocator,
        .io = io,
        .conn_opts = undefined,
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
        .pool = try PgClient.createConnPool(allocator, io, .{
            .connect = .{  
                .port = 5432,
                .host = "localhost",
            },
            .auth = .{
                .username = "db_rp",
                .password = "12345678",
                .database = "db",
                .timeout = 10_000,
            } 
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

    try std.testing.expectEqual(1, client.table_reg.count());
}

test "parsePgOutput maps INSERT correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

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

    try std.testing.expectEqual(1, result.entry.?.action);
    try std.testing.expectEqualStrings("public.addresses", result.entry.?.table_name);

    // Changed columns
    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("id").?.has_changes);
    try std.testing.expectEqualStrings("1", result.entry.?.changed_columns.get("id").?.value);

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("address_line_1").?.has_changes);
    try std.testing.expectEqualStrings("1 Apple Park Way", result.entry.?.changed_columns.get("address_line_1").?.value);

    try std.testing.expectEqual(null, result.entry.?.changed_columns.get("address_line_2"));

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("postal_code").?.has_changes);
    try std.testing.expectEqualStrings("95014", result.entry.?.changed_columns.get("postal_code").?.value);

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("city").?.has_changes);
    try std.testing.expectEqualStrings("Cupertino", result.entry.?.changed_columns.get("city").?.value);

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("country").?.has_changes);
    try std.testing.expectEqualStrings("US", result.entry.?.changed_columns.get("country").?.value);

    // Old values
    try std.testing.expectEqual(0, result.entry.?.old_values.capacity());

    // New values
    try std.testing.expectEqualStrings("1", result.entry.?.new_values.get("id").?);
    try std.testing.expectEqualStrings("1 Apple Park Way", result.entry.?.new_values.get("address_line_1").?);
    try std.testing.expectEqual(null, result.entry.?.new_values.get("address_line_2"));
    try std.testing.expectEqualStrings("95014", result.entry.?.new_values.get("postal_code").?);
    try std.testing.expectEqualStrings("Cupertino", result.entry.?.new_values.get("city").?);
    try std.testing.expectEqualStrings("US", result.entry.?.new_values.get("country").?);

    try std.testing.expectEqual(0, client.context.xid);
    try std.testing.expectEqualStrings("", client.context.ip_address);
    try std.testing.expectEqualStrings("", client.context.user_id);
    try std.testing.expectEqual(null, result.last_lsn);
    try std.testing.expectEqual(null, result.commit_timestamp);
}

test "parsePgOutput maps UPDATE correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

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

    try std.testing.expectEqual(2, result.entry.?.action);
    try std.testing.expectEqualStrings("public.addresses", result.entry.?.table_name);

    // Changed columns
    try std.testing.expectEqual(false, result.entry.?.changed_columns.get("id").?.has_changes);
    try std.testing.expectEqualStrings("1", result.entry.?.changed_columns.get("id").?.value);

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("address_line_1").?.has_changes);
    try std.testing.expectEqualStrings("1 Apple Park Way", result.entry.?.changed_columns.get("address_line_1").?.value);

    try std.testing.expectEqual(null, result.entry.?.changed_columns.get("address_line_2"));

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("postal_code").?.has_changes);
    try std.testing.expectEqualStrings("95014", result.entry.?.changed_columns.get("postal_code").?.value);

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("city").?.has_changes);
    try std.testing.expectEqualStrings("Cupertino", result.entry.?.changed_columns.get("city").?.value);

    try std.testing.expectEqual(false, result.entry.?.changed_columns.get("country").?.has_changes);
    try std.testing.expectEqualStrings("US", result.entry.?.changed_columns.get("country").?.value);

    // Old values
    try std.testing.expectEqualStrings("1 Apple Park Way", result.entry.?.old_values.get("address_line_1").?);
    try std.testing.expectEqual(null, result.entry.?.old_values.get("address_line_2"));
    try std.testing.expectEqualStrings("95014", result.entry.?.old_values.get("postal_code").?);
    try std.testing.expectEqualStrings("Cupertino", result.entry.?.old_values.get("city").?);
    try std.testing.expectEqualStrings("US", result.entry.?.old_values.get("country").?);

    // New values
    try std.testing.expectEqualStrings("Googleplex", result.entry.?.new_values.get("address_line_1").?);
    try std.testing.expectEqual(null, result.entry.?.new_values.get("address_line_2"));
    try std.testing.expectEqualStrings("94043", result.entry.?.new_values.get("postal_code").?);
    try std.testing.expectEqualStrings("Mountain View", result.entry.?.new_values.get("city").?);
    try std.testing.expectEqualStrings("US", result.entry.?.new_values.get("country").?);

    try std.testing.expectEqual(0, client.context.xid);
    try std.testing.expectEqualStrings("", client.context.ip_address);
    try std.testing.expectEqualStrings("", client.context.user_id);
    try std.testing.expectEqual(null, result.last_lsn);
    try std.testing.expectEqual(null, result.commit_timestamp);
}

test "parsePgOutput maps DELETE correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    // DELETE FROM addresses WHERE id = 1;
    const delete_hex = "44000040064f0006740000000131740000000a476f6f676c65706c65786e74000000053934303433740000000d4d6f756e7461696e205669657774000000025553";

    const delete_bytes = try allocator.alloc(u8, delete_hex.len / 2);
    defer allocator.free(delete_bytes);
    _ = try std.fmt.hexToBytes(delete_bytes, delete_hex);

    var result = try client.parsePgOutput(delete_bytes);
    defer result.deinit(allocator);

    try std.testing.expectEqual(3, result.entry.?.action);
    try std.testing.expectEqualStrings("public.addresses", result.entry.?.table_name);

    // Changed columns
    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("id").?.has_changes);
    try std.testing.expectEqualStrings("1", result.entry.?.changed_columns.get("id").?.value);

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("address_line_1").?.has_changes);
    try std.testing.expectEqualStrings("Googleplex", result.entry.?.changed_columns.get("address_line_1").?.value);

    try std.testing.expectEqual(null, result.entry.?.changed_columns.get("address_line_2"));

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("postal_code").?.has_changes);
    try std.testing.expectEqualStrings("94043", result.entry.?.changed_columns.get("postal_code").?.value);

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("city").?.has_changes);
    try std.testing.expectEqualStrings("Mountain View", result.entry.?.changed_columns.get("city").?.value);

    try std.testing.expectEqual(true, result.entry.?.changed_columns.get("country").?.has_changes);
    try std.testing.expectEqualStrings("US", result.entry.?.changed_columns.get("country").?.value);

    // Old values
    try std.testing.expectEqualStrings("1", result.entry.?.old_values.get("id").?);
    try std.testing.expectEqualStrings("Googleplex", result.entry.?.old_values.get("address_line_1").?);
    try std.testing.expectEqual(null, result.entry.?.old_values.get("address_line_2"));
    try std.testing.expectEqualStrings("94043", result.entry.?.old_values.get("postal_code").?);
    try std.testing.expectEqualStrings("Mountain View", result.entry.?.old_values.get("city").?);
    try std.testing.expectEqualStrings("US", result.entry.?.old_values.get("country").?);

    // New values
    try std.testing.expectEqual(0, result.entry.?.new_values.capacity());

    try std.testing.expectEqual(0, client.context.xid);
    try std.testing.expectEqualStrings("", client.context.ip_address);
    try std.testing.expectEqualStrings("", client.context.user_id);
    try std.testing.expectEqual(null, result.last_lsn);
    try std.testing.expectEqual(null, result.commit_timestamp);
}

test "parsePgOutput maps COMMIT correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

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

    try std.testing.expectEqual(0, client.context.xid);
    try std.testing.expectEqualStrings("", client.context.ip_address);
    try std.testing.expectEqualStrings("", client.context.user_id);
    try std.testing.expectEqual(null, result.entry);
    try std.testing.expectEqual(last_lsn, result.last_lsn);
    try std.testing.expectEqual(commit_timestamp, result.commit_timestamp);
}

test "resetContext clears context correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    client.context.ip_address = "192.168.1.50";
    client.context.user_id = "42";
    client.context.xid = 791;

    const commit_hex = "43000000000001c160880000000001c160b80002f9a2afe34ece";

    const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
    defer allocator.free(commit_bytes);
    _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);

    var result = try client.parsePgOutput(commit_bytes);
    result.deinit(allocator);

    try std.testing.expectEqual(null, result.entry);

    try std.testing.expectEqual(0, client.context.xid);
    try std.testing.expectEqualStrings("", client.context.ip_address);
    try std.testing.expectEqualStrings("", client.context.user_id);
}

test "parseTupleData" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = try setupMockClient(allocator, io);
    defer client.deinit();

    const commit_hex = "0006740000000131740000001031204170706c65205061726b205761796e740000000539353031347400000009437570657274696e6f74000000025553";

    const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
    defer allocator.free(commit_bytes);
    _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);

    var reader = std.Io.Reader.fixed(commit_bytes);

    var result = try client.parseTupleData(&reader, client.table_reg.get(16390).?);
    defer result.deinit(allocator);

    try std.testing.expectEqualStrings("1", result.get("id").?);
    try std.testing.expectEqualStrings("1 Apple Park Way", result.get("address_line_1").?);
    try std.testing.expectEqual(null, result.get("address_line_2"));
    try std.testing.expectEqualStrings("95014", result.get("postal_code").?);
    try std.testing.expectEqualStrings("Cupertino", result.get("city").?);
    try std.testing.expectEqualStrings("US", result.get("country").?);

    try std.testing.expectEqual(0, client.context.xid);
    try std.testing.expectEqualStrings("", client.context.ip_address);
    try std.testing.expectEqualStrings("", client.context.user_id);
}

test "readSchemaKeys" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var client = PgClient{
        .allocator = allocator,
        .io = undefined,
        .context = undefined,
        .conn_opts = undefined,
        .last_lsn = undefined,
        .last_timestamp = undefined,
        .pool = try PgClient.createConnPool(allocator, io, .{
            .connect = .{  
                .port = 5432,
                .host = "localhost",
            },
            .auth = .{
                .username = "db_rp",
                .password = "12345678",
                .database = "db",
                .timeout = 10_000,
            } 
        }),
        .table_reg = undefined,
        .wal_conn = null,
    };
    defer client.deinit();

    var columns = try client.readSchemaKeys("users");
    defer columns.deinit(allocator);

    try std.testing.expectEqual(6, columns.items.len);

    for (columns.items) |item|  {
        if (std.mem.eql(u8, "id", item.name)) { try std.testing.expectEqual(true, item.is_key); }
        else if (std.mem.eql(u8, "address_id", item.name)) { try std.testing.expectEqual(false, item.is_key); }
        else if (std.mem.eql(u8, "age", item.name)) { try std.testing.expectEqual(false, item.is_key); }
        else if (std.mem.eql(u8, "email_address", item.name)) { try std.testing.expectEqual(false, item.is_key); }
        else if (std.mem.eql(u8, "gender", item.name)) { try std.testing.expectEqual(false, item.is_key); }
        else if (std.mem.eql(u8, "name", item.name)) { try std.testing.expectEqual(false, item.is_key); }
    }
}
