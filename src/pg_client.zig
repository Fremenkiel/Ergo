const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const pg = @import("pg");
const t = @import("t.zig");
const types = @import("types");

pub const PgClientError = error{
PostgresReplicationError,
DefaultConnectionNotInitialized,
UnableToSendCopyDone,
InvalidReadyForQueryMessage,
InvalidCopyDoneMessage,
InvalidMessage,
TransactionErrorState,
TransactionStateUnknown,
WalConnectionUnableToStop,
};

// const Action = enum(u8) {
//     Insert,
//     UpdateOld,
//     UpdateNew,
//     Delete,
// };
//
// pub const ReadResponse = struct {
//     data: ?types.AuditEntry,
//     timestamp: ?i64,
//     message: pg.packet.ServerPacket,
//
//     pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
//         if (self.data) |*entry| {
//             entry.deinit(allocator);
//         }
//     }
// };
//
// pub const ParseResponse = struct {
//     data: ?types.AuditEntry,
//     last_lsn: ?u64,
//     timestamp: ?u64,
//
//     pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
//         if (self.data) |*entry| {
//             entry.deinit(allocator);
//         }
//     }
// };
//
// pub const TransactionContext = struct {
//     xid: u32,
//     user_id: []const u8,
//     ip_address: []const u8,
//     primary_key: []const u8,
//
//     pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
//         if (self.user_id.len > 0) allocator.free(self.user_id);
//         if (self.ip_address.len > 0) allocator.free(self.ip_address);
//     }
//
//     fn reset(self: *@This(), allocator: mem.Allocator) void {
//         self.xid = 0;
//
//         if (self.user_id.len > 0) allocator.free(self.user_id);
//         self.user_id = "";
//
//         if (self.ip_address.len > 0) allocator.free(self.ip_address);
//         self.ip_address = "";
//
//         self.primary_key = "";
//     }
// };
//
// pub const ColumnDef = struct {
//     name: []const u8,
//     is_key: bool,
// };
//
// pub const TableDef = struct {
//     namespace: []const u8,
//     name: []const u8,
//     columns: std.ArrayList(ColumnDef),
//
//     pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
//         for (self.columns.items) |*col| { allocator.free(col.name); }
//         self.columns.clearAndFree(allocator);
//         self.columns.deinit(allocator);
//
//         allocator.free(self.namespace);
//         allocator.free(self.name);
//     }
// };

pub const PgClient = struct {
    allocator: mem.Allocator,
    io: Io,

    opts: pg.PgConfig,

    last_lsn: u64,
    last_timestamp: i64,

    transaction: types.Transaction,
    // rows: std.ArrayList(std.ArrayList(types.ColumnChange)) = .empty,

    // context: TransactionContext,
    // table_reg: std.hash_map.HashMap(u32, TableDef, std.hash_map.AutoContext(u32), 80),

    parser: pg.parser.PgOutput,

    conn: ?*pg.Conn,

    pub fn init(allocator: mem.Allocator, io: Io, opts: pg.PgConfig) !PgClient{
        var conn = try createConn(allocator, io, opts);
        errdefer conn.deinit();

        return .{
            .allocator = allocator,
            .io = io,
            .opts = opts,
            .last_lsn = 0,
            .last_timestamp = 0,
            // .context = .{
            //     .xid = 0,
            //     .user_id = "",
            //     .ip_address = "",
            //     .primary_key = "",
            // },
            // .table_reg = std.AutoHashMap(u32, TableDef).init(allocator),
            .transaction = .empty,
            .parser = .init(allocator),
            .conn = conn,
        };
    }

    pub fn deinit(self: *@This()) void {
        if (self.conn) |conn| {
            conn.deinit();
            self.allocator.destroy(conn);
        }
    }

    pub fn cancel(self: *@This()) void {
        if (self.conn) |conn| conn.cancel();
    }

    // fn resetContext(self: *@This()) void {
    //     self.context.reset(self.allocator);
    //
    //     var it = self.table_reg.iterator(); 
    //     while (it.next()) |table_reg| { table_reg.value_ptr.*.deinit(self.allocator); }
    //     self.table_reg.clearRetainingCapacity();
    // }

    pub fn startFlow(self: *@This(), timeout_ms: i32) !void {
        if (self.conn == null) return pg.PgError.WalConnectionNotInitialized;
        try self.conn.?.reader.startWALFlow(self.opts.wal, timeout_ms);
    }

    pub fn endFlow(self: *@This()) !void {
        if (self.conn == null) return pg.PgError.WalConnectionNotInitialized;
        try self.conn.?.reader.endWALFlow(self.opts.wal);
    }

    pub fn readWAL(self: *@This()) !types.Transaction {
        if (self.conn == null) return pg.PgError.WalConnectionNotInitialized;

        var transaction: types.Transaction = .empty;
        defer transaction.deinit(self.allocator);

        while(true) {
            const msg = try self.conn.?.reader.next();

            switch (msg.type) {
                'W' => {
                    // Server entered COPY BOTH mode,
                },
                'd' => {
                    if (msg.data.len == 0) continue;

                    const data_type = msg.data[0];

                    switch (data_type) {
                        'w' => {
                            assert(msg.data.len >= 25);

                            const start_lsn = mem.readInt(u64, msg.data[1..9][0..8], .big);
                            const server_timestamp = mem.readInt(i64, msg.data[17..25][0..8], .big);

                            self.last_timestamp = server_timestamp;

                            const payload = msg.data[25..];

                            const parse_response = try self.parser.decode(payload);
                            
                            // Proactively acknowledge this processed WAL chunk
                            try self.conn.?.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);

                            if (parse_response) |res| {
                                if (res.data) |row| {
                                    try self.transaction.rows.append(self.allocator, row);
                                }

                                if (res.xid) |xid| {
                                    self.transaction.transaction_id = xid;
                                }

                                if (res.user_id) |user_id| {
                                    self.transaction.user_id = user_id;
                                }

                                if (res.ip_address) |ip_address| {
                                    self.transaction.ip_address = ip_address;
                                }

                                if (res.timestamp) |timestamp| {
                                    self.transaction.event_time = timestamp;
                                }

                                if (res.last_lsn) |lsn| {
                                    if (lsn > self.last_lsn) {
                                        self.last_lsn = lsn;

                                        self.parser.clear();

                                        return transaction;
                                    }
                                } else {
                                    if (start_lsn > self.last_lsn) {
                                        self.last_lsn = start_lsn;
                                    }
                                }
                            }
                        },
                        'k' => {
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
                                try self.conn.?.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);
                            }
                        },
                        else => {}
                    }
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
        }
    }

    pub fn sendCopyDone(self: *@This()) !void {
        if (self.conn == null) {
            return PgClientError.UnableToSendCopyDone;
        }

        try pg.protocol.CopyDone.write(&self.conn.?.stream);
    }

    // pub fn parsePgOutput(self: *@This(), payload: []const u8) !ParseResponse {
    //     var response = ParseResponse{
    //         .last_lsn = null,
    //         .timestamp = null,
    //         .data = null,
    //     };
    //
    //     if (payload.len == 0) return response;
    //
    //     var reader = Io.Reader.fixed(payload);
    //
    //     const msg_type = try reader.takeByte();
    //
    //     switch (msg_type) {
    //         'B' => {
    //             // final lsn
    //             _ = try reader.takeInt(u64, .big);
    //             const timestamp = try reader.takeInt(u64, .big);
    //             const xid = try reader.takeInt(u32, .big);
    //             self.context.xid = xid;
    //
    //             _ = timestamp;
    //         },
    //         'C' => {
    //             // flags
    //             _ = try reader.takeByte();
    //             // lsn of commit
    //             _ = try reader.takeInt(u64, .big);
    //             response.last_lsn = try reader.takeInt(u64, .big);
    //             response.timestamp = try reader.takeInt(u64, .big);
    //
    //             self.resetContext();
    //         },
    //         'R' => {
    //             // Relation: send before any insert or update
    //             const rel_id = try reader.takeInt(u32, .big);
    //
    //             const namespace = try reader.takeDelimiter(0);
    //             if (namespace == null) {
    //                 return PgClientError.InvalidMessage;
    //             }
    //
    //             const rel_name = try reader.takeDelimiter(0);
    //             if (rel_name == null) {
    //                 return PgClientError.InvalidMessage;
    //             }
    //
    //             const repl_ident = try reader.takeByte();
    //             _ = repl_ident;
    //
    //             const num_columns = try reader.takeInt(u16, .big);
    //
    //             // const columns = try self.readSchemaKeys(table_name);
    //             var columns = std.ArrayList(ColumnDef).empty;
    //
    //             var i: u16 = 0;
    //             while (i < num_columns) : (i += 1) {
    //                 const flag = try reader.takeByte();
    //
    //                 // col name
    //                 const column_name = try reader.takeDelimiter(0);
    //                 if (column_name == null) {
    //                     return PgClientError.InvalidMessage;
    //                 }
    //
    //                 // type_id
    //                 _ = try reader.takeInt(u32, .big);
    //
    //                 // typemod
    //                 _ = try reader.takeInt(u32, .big);
    //
    //                 try columns.append(self.allocator, .{ .name = try self.allocator.dupe(u8, column_name.?), .is_key = flag == 1 });
    //             }
    //
    //             try self.table_reg.put(rel_id, .{
    //                 .namespace = try self.allocator.dupe(u8, namespace.?),
    //                 .name = try self.allocator.dupe(u8, rel_name.?),
    //                 .columns = columns,
    //             });
    //
    //         },
    //         'I' => {
    //             const rel_id = try reader.takeInt(u32, .big);
    //             const tuple_type = try reader.takeByte();
    //
    //             if (tuple_type != 'N') {
    //                 std.debug.print("Error: Received insert with invalid tuple type: {c}\n", .{tuple_type});
    //             }
    //
    //
    //             if (self.table_reg.get(rel_id)) |table| {
    //                 const columns = try initContextColumns(self.allocator, table);
    //
    //                 response.data = types.AuditEntry{
    //                     .event_time = undefined,
    //                     .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
    //                     .action = 1,
    //                     .columns = try self.parseTupleData(&reader, columns, .Insert),
    //                     .transaction_id = self.context.xid,
    //                     .user_id = if (self.context.user_id.len > 0) try self.allocator.dupe(u8, self.context.user_id) else "",
    //                     .ip_address = if (self.context.ip_address.len > 0) try self.allocator.dupe(u8, self.context.ip_address) else "",
    //                 };
    //             } else {
    //                 std.debug.print("Error: Received insert for unknown relation ID {d}\n", .{rel_id});
    //             }
    //         },
    //         'U' => {
    //             const rel_id = try reader.takeInt(u32, .big);
    //             var tuple_type = try reader.takeByte();
    //
    //             if (self.table_reg.get(rel_id)) |table| {
    //                 var columns = try initContextColumns(self.allocator, table);
    //                 if (tuple_type == 'O' or tuple_type == 'K') {
    //                     columns = try self.parseTupleData(&reader, columns, .UpdateOld);
    //
    //                     tuple_type = try reader.takeByte();
    //                 }
    //
    //                 if (tuple_type == 'N') {
    //                     response.data = types.AuditEntry{
    //                         .event_time = undefined,
    //                         .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
    //                         .action = 2,
    //                         .columns = try self.parseTupleData(&reader, columns, .UpdateNew),
    //                         .transaction_id = self.context.xid,
    //                         .user_id = if (self.context.user_id.len > 0) try self.allocator.dupe(u8, self.context.user_id) else "",
    //                         .ip_address = if (self.context.ip_address.len > 0) try self.allocator.dupe(u8, self.context.ip_address) else "",
    //                     };
    //                 } else {
    //                     std.debug.print("Error: Expected 'N', got '{c}'\n", .{tuple_type});
    //                 }
    //             } else {
    //                 std.debug.print("Error: Received update for unknown relation ID {d}\n", .{rel_id});
    //             }
    //         },
    //         'D' => {
    //             const rel_id = try reader.takeInt(u32, .big);
    //
    //             const tuple_type = try reader.takeByte();
    //
    //             if (self.table_reg.get(rel_id)) |table| {
    //                 const columns = try initContextColumns(self.allocator, table);
    //                 if (tuple_type == 'O' or tuple_type == 'K') {
    //                     response.data = types.AuditEntry{
    //                         .event_time = undefined,
    //                         .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
    //                         .action = 3,
    //                         .columns = try self.parseTupleData(&reader, columns, .Delete),
    //                         .transaction_id = self.context.xid,
    //                         .user_id = if (self.context.user_id.len > 0) try self.allocator.dupe(u8, self.context.user_id) else "",
    //                         .ip_address = if (self.context.ip_address.len > 0) try self.allocator.dupe(u8, self.context.ip_address) else "",
    //                     };
    //                 } else {
    //                     std.debug.print("Error: Expected 'O' or 'K', got '{c}'\n", .{tuple_type});
    //                 }
    //             } else {
    //                 std.debug.print("Error: Received delete for unknown relation ID {d}\n", .{rel_id});
    //             }
    //         },
    //         'M' => {
    //             const flags = try reader.takeByte();
    //             const lsn = try reader.takeInt(u64, .big);
    //
    //             const prefix = try reader.takeDelimiter(0);
    //             if (prefix == null) {
    //                 return PgClientError.InvalidMessage;
    //             }
    //
    //             const content_len = try reader.takeInt(u32, .big);
    //
    //             const content = try reader.take(content_len);
    //
    //             _ = flags;
    //             _ = lsn;
    //             if (mem.eql(u8, prefix.?, "ergo_meta")) {
    //                 var it = mem.splitAny(u8, content, ",");
    //
    //                 const user_id_str = it.next() orelse return error.InvalidMapType;
    //                 const ip_address_str = it.next() orelse return error.InvalidMapType;
    //
    //                 var user_id_it = mem.splitAny(u8, user_id_str, ":");
    //                 var ip_address_it = mem.splitAny(u8, ip_address_str, ":");
    //
    //                 const user_id_key_str = user_id_it.next() orelse return error.InvalidMapType;
    //                 const ip_address_key_str = ip_address_it.next() orelse return error.InvalidMapType;
    //
    //                 const user_id_key = mem.trim(u8, user_id_key_str, " ");
    //                 const ip_address_key = mem.trim(u8, ip_address_key_str, " ");
    //
    //                 if (!mem.eql(u8, user_id_key, "\"user_id\"")) {
    //                     return error.InvalidMapType;
    //                 }
    //
    //                 if (!mem.eql(u8, ip_address_key, "\"ip\"")) {
    //                     return error.InvalidMapType;
    //                 }
    //
    //                 const user_id_value_str = user_id_it.next() orelse return error.InvalidMapType;
    //                 const ip_address_value_str = ip_address_it.next() orelse return error.InvalidMapType;
    //
    //                 const user_id_value = mem.trim(u8, mem.trim(u8, user_id_value_str, " "), "\"");
    //                 const ip_address_value = mem.trim(u8, mem.trim(u8, ip_address_value_str, " "), "\"");
    //
    //                 const check_str = try std.fmt.allocPrint(self.allocator, "{s}: \"{s}\", {s}: \"{s}\"", .{user_id_key, user_id_value, ip_address_key, ip_address_value});
    //                 defer self.allocator.free(check_str);
    //                 assert(mem.eql(u8, check_str, content));
    //
    //                 self.context.user_id = try self.allocator.dupe(u8, user_id_value);
    //                 self.context.ip_address = try self.allocator.dupe(u8, ip_address_value);
    //             }
    //         },
    //         'Y' => {
    //             // XID
    //             _ = try reader.takeInt(i32, .big);
    //
    //             // OID
    //             _ = try reader.takeInt(i32, .big);
    //
    //             // namespace
    //             _ = try reader.takeDelimiter(0);
    //
    //             // data type name
    //             _ = try reader.takeDelimiter(0);
    //         },
    //         else => {
    //             std.debug.print("Unknown pgoutput message type: {c}\n", .{msg_type});
    //         }
    //     }
    //
    //     return response;
    // }
    //
    // fn parseTupleData(self: *@This(), reader: *Io.Reader, columns: std.ArrayList(types.ColumnChange), action: Action) !std.ArrayList(types.ColumnChange) {
    //     const num_columns = try reader.takeInt(u16, .big);
    //
    //     if (num_columns > columns.items.len) {
    //         return error.ColumnMismatch;
    //     }
    //
    //     for (columns.items) |*col| {
    //         const col_type = try reader.takeByte();
    //
    //         switch (col_type) {
    //             'n' => {
    //                 // Null
    //             },
    //             'u' => {
    //                 // Unchanged TOAST
    //             },
    //             't' => {
    //                 const col_len = try reader.takeInt(u32, .big);
    //
    //                 const val_raw = try reader.take(col_len);
    //                 const val = try self.allocator.dupe(u8, val_raw);
    //                 errdefer self.allocator.free(val);
    //
    //                 switch (action) {
    //                     .Insert => {
    //                         col.new_value = val;
    //                         col.has_changes = true;
    //                     },
    //                     .UpdateNew => {
    //                         col.new_value = val;
    //
    //                         if ((col.old_value == null) != (col.new_value == null) or 
    //                             (col.old_value != null and col.new_value != null and !std.mem.eql(u8, col.old_value.?, col.new_value.?))) {
    //                             col.has_changes = true;
    //                         }
    //                     },
    //                     .UpdateOld => {
    //                         col.old_value = val;
    //                     },
    //                     .Delete => {
    //                         col.old_value = val;
    //                         col.has_changes = true;
    //                     },
    //                 }
    //             },
    //             else => return error.UnknownTupleFormat,
    //         }
    //     }
    //
    //     return columns;
    // }

    pub fn createConn(allocator: mem.Allocator, io:  Io, opts: pg.PgConfig) !*pg.Conn {
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

// fn initContextColumns(allocator: mem.Allocator, table_def: TableDef) !std.ArrayList(types.ColumnChange) {
//     var columns: std.ArrayList(types.ColumnChange) = .empty;
//     errdefer {
//         columns.clearAndFree(allocator);
//         columns.deinit(allocator);
//     }
//
//     try columns.ensureUnusedCapacity(allocator, table_def.columns.items.len);
//
//     for (table_def.columns.items) |col| {
//         columns.appendAssumeCapacity(.{
//             .is_key = col.is_key,
//             .column_name = try allocator.dupe(u8, col.name),
//             .old_value = null,
//             .new_value = null,
//             .has_changes = false,
//         });
//     }
//
//     return columns;
// }

fn setupClient(allocator: mem.Allocator, io: Io) !PgClient {
    var cols = std.ArrayList(types.ColumnDef).empty;
    try cols.ensureUnusedCapacity(allocator, 6);
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "id"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "address_line_1"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "address_line_2"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "postal_code"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "city"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "country"), .is_key = true });

    var table_reg = std.AutoHashMap(u32, types.TableDef).init(allocator);
    try table_reg.put(16390, .{
        .namespace = try allocator.dupe(u8, "public"),
        .name = try allocator.dupe(u8, "addresses"),
        .columns = cols,
    });

    return .{
        .allocator = allocator,
        .io = io,
        .opts = .{
            .host = "localhost",
            .port = 5432,
            .database = "db",
            .username = "db_rw",
            .application_name = "Ergo test",
            .startup_parameters = null,
        },
        .last_lsn = 0,
        .last_timestamp = 0,
        .context = .{
            .xid = 0,
            .user_id = "",
            .ip_address = "",
            .primary_key = "",
        },
        .table_reg = table_reg,
        .conn = null,
    };
}

test "resetContext clears context correctly" {
    const allocator = testing.allocator;
    const io = testing.io;

    var client = try setupClient(allocator, io);
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
