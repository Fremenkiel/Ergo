const std = @import("std");
const assert = std.debug.assert;

const pg = @import("pg");
const types = @import("types.zig");

pub const PgClientError = error{
    PostgresReplicationError,
};

pub const ReadResponse = struct {
    entry: ?types.AuditEntry,
    commit_timestamp: ?i64,
};

pub const ParseResponse = struct {
    entry: ?types.AuditEntry,
    commit_lsn: ?u64,
    commit_timestamp: ?u64,
};

pub const TransactionContext = struct {
    xid: u32,
    user_id: []const u8,
    ip_address: []const u8,
    primary_key: []const u8,
    changed_columns: std.StringHashMapUnmanaged(types.ChangedColumns),
};

pub const ColumnDef = struct {
    name: []const u8,
    type_id: u32,
    is_key: bool,
};

pub const TableDef = struct {
    namespace: []const u8,
    name: []const u8,
    columns: []ColumnDef,
};

pub const PgClient = struct {
    last_lsn: u64,
    last_timestamp: i64,

    context: TransactionContext,

    table_reg: std.hash_map.HashMap(u32, TableDef, std.hash_map.AutoContext(u32), 80),
    allocator: std.mem.Allocator,
    pool: *pg.Pool,
    wal_conn: *pg.Conn,

    pub const Opts = struct {
        auth: pg.Conn.AuthOpts = .{},
        connect: pg.Conn.Opts = .{},
    };

    pub fn init(io: std.Io, allocator: std.mem.Allocator, opts: Opts) !PgClient{
        var pool = pg.Pool.init(io, allocator, .{ .size = 1, .connect = opts.connect, .auth = opts.auth}) catch |err| {
            std.debug.print("Failed to connect: {}\n", .{err});
            std.process.exit(1);
        };
        errdefer pool.deinit();

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
        errdefer conn.deinit();

        return .{
            .allocator = allocator,
            .last_lsn = 0,
            .last_timestamp = 0,
            .context = .{
                .xid = 0,
                .user_id = "",
                .ip_address = "",
                .primary_key = "",
                .changed_columns = std.StringHashMapUnmanaged(types.ChangedColumns).empty,
            },
            .table_reg = std.AutoHashMap(u32, TableDef).init(allocator),
            .wal_conn = conn,
            .pool = pool,
        };
    }

    pub fn deinit(self: *PgClient) void {
        self.endFlow() catch {};
        self.wal_conn.deinit();
        self.allocator.destroy(self.wal_conn);

        self.table_reg.deinit();
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

        try self.wal_conn.write("Q");
        try self.wal_conn.write(&len_buf);
        try self.wal_conn.write(query);
        try self.wal_conn.write(&[_]u8{0});

        std.debug.print("Sent START_REPLICATION. Waiting for stream...\n", .{});
        try self.startFlow();
    }

    pub fn readWAL(self: *PgClient) !ReadResponse {
        const msg = try self.wal_conn._reader.next();
        var response = ReadResponse{
            .entry = null,
            .commit_timestamp = null,
        };

        switch (msg.type) {
            'W' => {
                std.debug.print("Server entered COPY BOTH mode.\n", .{});
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

                    const parse_response = try self.parsePgOutput(payload, &self.table_reg);

                    if (parse_response.commit_lsn) |lsn| {
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

                    std.debug.print("Received WAL Data (start_lsn: {x}, last_lsn: {x})\n", .{start_lsn, self.last_lsn});

                    // Proactively acknowledge this processed WAL chunk
                    try self.wal_conn.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);
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

                    std.debug.print("Received Keepalive (LSN: {x}, Reply: {d})\n", .{current_lsn, reply_requested});

                    if (reply_requested == 1) {
                        try self.wal_conn.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);
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

    pub fn parsePgOutput(self: *PgClient, payload: []const u8, table_reg: *std.hash_map.HashMap(u32, TableDef, std.hash_map.AutoContext(u32), 80)) !ParseResponse {
        var response = ParseResponse{
            .commit_lsn = null,
            .commit_timestamp = null,
            .entry = null,
        };

        if (payload.len == 0) return response;

        var reader = std.Io.Reader.fixed(payload);

        const msg_type = try reader.takeByte();

        switch (msg_type) {
            'B' => {
                const final_lsn = try reader.takeInt(u64, .big);
                const commit_timestamp = try reader.takeInt(u64, .big);
                const xid = try reader.takeInt(u32, .big);
                self.context.xid = xid;

                _ = commit_timestamp;
                std.debug.print("BEGIN: xid={d}, lsn={x}\n", .{xid, final_lsn});
            },
            'C' => {
                const flags = try reader.takeByte();
                const commit_lsn = try reader.takeInt(u64, .big);
                response.commit_lsn = try reader.takeInt(u64, .big);
                response.commit_timestamp = try reader.takeInt(u64, .big);

                _ = flags;
                std.debug.print("COMMIT: lsn={x}\n", .{commit_lsn});

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
                var columns = try self.allocator.alloc(ColumnDef, num_columns);

                var keys = std.StringHashMap(void).init(self.allocator);
                defer keys.deinit();
                try self.readSchemaKeys(try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{namespace, rel_name}), &keys);

                var i: u16 = 0;
                while (i < num_columns) : (i += 1) {
                    const flags = try reader.takeByte();

                    const col_name = try self.allocator.dupe(u8, try reader.takeDelimiterExclusive(0));
                    _ = try reader.takeByte();

                    const type_id = try reader.takeInt(u32, .big);

                    const typmod = try reader.takeInt(u32, .big);
                    _ = typmod;

                    columns[i] = .{
                        .name = col_name,
                        .type_id = type_id,
                        .is_key = (keys.get(col_name) != null and flags == 1),
                    };
                }

                try table_reg.put(rel_id, .{
                    .namespace = namespace,
                    .name = rel_name,
                    .columns = columns,
                });

                std.debug.print("RELATION: {s}.{s} (ID: {d}\n", .{
                    namespace, rel_name, rel_id
                });
            },
            'I' => {
                const rel_id = try reader.takeInt(u32, .big);
                const tuple_type = try reader.takeByte();

                if (tuple_type != 'N') {
                    std.debug.print("Error: Received insert with invalid tuple type: {c}\n", .{tuple_type});
                }

                if (table_reg.get(rel_id)) |table| {
                    std.debug.print("INSERT INTO {s}.{s}:\n", .{table.namespace, table.name});
                    const new_values = try self.parseTupleData(&reader, table);

                    response.entry = types.AuditEntry{
                        .event_time = undefined,
                        .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                        .new_values = new_values,
                        .old_values = .empty,
                        .action = 1,
                        .changed_columns = try self.context.changed_columns.clone(self.allocator),
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

                if (table_reg.get(rel_id)) |table| {
                    std.debug.print("UPDATE {s}.{s}:\n", .{table.namespace, table.name});
                    var old_values: std.StringHashMapUnmanaged([]const u8) = .empty;

                    if (tuple_type == 'O' or tuple_type == 'K') {
                        std.debug.print("   -> Has Old/Key Data ({c}):\n", .{tuple_type});
                        old_values = try self.parseTupleData(&reader, table);

                        tuple_type = try reader.takeByte();
                    }

                    if (tuple_type == 'N') {
                        std.debug.print("   -> Has new data (N):\n", .{});
                        const new_values = try self.parseTupleData(&reader, table);
                        response.entry = types.AuditEntry{
                            .event_time = undefined,
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .new_values = new_values,
                            .old_values = old_values,
                            .action = 2,
                            .changed_columns = try self.context.changed_columns.clone(self.allocator),
                            .transaction_id = self.context.xid,
                            .user_id = self.context.user_id,
                            .ip_address = self.context.ip_address,
                            .primary_key = self.context.primary_key,
                        };
                    } else {
                        std.debug.print("   -> Expected 'N', got '{c}'\n", .{tuple_type});
                    }
                } else {
                    std.debug.print("Error: Received update for unknown relation ID {d}\n", .{rel_id});
                }

                self.context.changed_columns.clearRetainingCapacity();
            },
            'D' => {
                const rel_id = try reader.takeInt(u32, .big);

                const tuple_type = try reader.takeByte();

                if (table_reg.get(rel_id)) |table| {
                    std.debug.print("DELETE FROM {s}.{s}:\n", .{table.namespace, table.name});
                    if (tuple_type == 'O' or tuple_type == 'K') {
                        std.debug.print("-> Deleted data ({c}):\n", .{tuple_type});
                        const old_values = try self.parseTupleData(&reader, table);
                        response.entry = types.AuditEntry{
                            .event_time = undefined,
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .new_values = .empty,
                            .old_values = old_values,
                            .action = 3,
                            .changed_columns = try self.context.changed_columns.clone(self.allocator),
                            .transaction_id = self.context.xid,
                            .user_id = self.context.user_id,
                            .ip_address = self.context.ip_address,
                            .primary_key = self.context.primary_key,
                        };
                    } else {
                        std.debug.print("   -> Expected 'O' or 'K', got '{c}'\n", .{tuple_type});
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
                std.debug.print("METADATA prefix received: {s}\n", .{prefix});

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
                    assert(std.mem.eql(u8, check_str, content));

                    self.context.user_id = try self.allocator.dupe(u8, user_id_value);
                    self.context.ip_address = try self.allocator.dupe(u8, ip_address_value);

                    std.debug.print("METADATA RECEIVED: {s}\n", .{content});
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

        if (num_columns > table.columns.len) {
            return error.ColumnMismatch;
        }

        std.debug.print("   Row data ({d} columns):\n", .{num_columns});

        var changes = std.StringHashMapUnmanaged([]const u8).empty;
        var i: u16 = 0;
        while (i < num_columns) : (i += 1) {
            const col_type = try reader.takeByte();
            const col_name = table.columns[i].name;
            const is_pk = table.columns[i].is_key;

            switch (col_type) {
                'n' => {
                    std.debug.print("   {s}: NULL\n", .{col_name});
                },
                'u' => {
                    std.debug.print("   {s}: Unchanged TOAST value\n", .{col_name});
                },
                't' => {
                    const col_len = try reader.takeInt(u32, .big);

                    const val_buf = try reader.take(col_len);

                    try changes.put(self.allocator, col_name, val_buf);
                    const changed_column = try self.context.changed_columns.getOrPut(self.allocator, col_name);
                    
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

                    std.debug.print("   {s}: {s}\n", .{col_name, val_buf});
                },
                else => return error.UnknownTupleFormat,
            }
        }
        return changes;
    }

    fn readSchemaKeys(self: *PgClient, table_name: []const u8, primary_key_map: *std.StringHashMap(void)) !void {
        var conn = try self.pool.acquire();
        defer conn.release();
        var result = try conn.queryOpts(
            \\ SELECT
            \\   a.attname AS column_name,
            \\   c.contype AS constraint_type
            \\ FROM pg_constraint c
            \\ JOIN pg_attribute a ON a.attnum = ANY(c.conkey)
            \\   AND a.attrelid = c.conrelid
            \\ WHERE c.conrelid = $1::regclass
            \\   AND c.contype IN ('p', 'f');
        , .{table_name}, .{ .column_names = true });
        defer result.deinit();

        const column_name_index = result.columnIndex("column_name").?;
        const constraint_type_index = result.columnIndex("constraint_type").?;
        while (try result.next()) |row| {
            const column_name = try row.get([]const u8, column_name_index);
            const constraint_type = try row.get([]const u8, constraint_type_index);
            if (std.mem.eql(u8, constraint_type, "p")) {
                try primary_key_map.put(column_name, {});
            }
        }
    }

    pub fn pgWalToClickHouseMs(pg_wal_us: u64) i64 {
        const seconds_between_epochs: u64 = 946_684_800;
        const us_between_epochs: u64 = seconds_between_epochs * 1_000_000;

        const unix_us: u64 = pg_wal_us + us_between_epochs;

        const unix_ms: u64 = unix_us / 1000;

        return @intCast(unix_ms);
    }

    pub fn startFlow(self: *PgClient) !void {
        try self.wal_conn._reader.startFlow(null, null);
    }

    pub fn endFlow(self: *PgClient) !void {
        try self.wal_conn._reader.endFlow();
    }
};

