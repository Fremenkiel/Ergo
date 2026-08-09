const std = @import("std");
const builtin = @import("builtin");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;
const posix = std.posix;

const assert = std.debug.assert;

const pg = @import("pg");
const types = @import("types");

const application_name = "Ergo WAL logger";

const Options = struct {
    pg_host: []const u8 = "localhost",
    pg_port: u16 = 5432,
    pg_user: []const u8 = "db_rw",
    pg_pass: []const u8 = "12345678",
    pg_db: []const u8 = "db",
    pg_wal: []const u8 = "wal_slot",

    user: []const u8,

    fn init(allocator: mem.Allocator, environ_map: *std.process.Environ.Map) !Options {
        const os_user = environ_map.get("USER");

        assert(os_user != null);

        return .{
            .user = try allocator.dupe(u8, os_user.?)
        };
    }

    fn deinit(self: *@This(), allocator: mem.Allocator) void {
        allocator.free(self.user);
    }
};

const WALLogger = struct {
    last_lsn: u64,
    last_timestamp: i64,

    conn: *pg.Conn,

    pub fn init(conn: *pg.Conn) @This() {
        return .{
            .last_lsn = 0,
            .last_timestamp = 0,
            .conn = conn,
        };
    }

    pub fn startFlow(self: *@This(), timeout_ms: i32) !void {
        try self.conn.reader.startWALFlow(self.conn.opts.wal, timeout_ms);
    }

    pub fn endFlow(self: *@This()) !void {
        try self.conn.reader.endWALFlow(self.conn.opts.wal);
    }

    pub fn readWAL(self: *@This()) !void {
        while(true) {
            const msg = self.conn.reader.next() catch |err| {
                if (err == error.Timeout) {
                    continue;
                }
                return err;
            };

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

                            std.log.info("RECEIVED: msg type {s}, data type {s}, start_lsn: {d}, server_timestamp: {d}", .{[_]u8{msg.type}, [_]u8{data_type}, start_lsn, server_timestamp});

                            const payload = msg.data[25..];

                            try self.decode(payload);

                            // Proactively acknowledge this processed WAL chunk
                            try self.conn.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);
                        },
                        'k' => {
                            assert(msg.data.len >= 18);

                            const current_lsn = mem.readInt(u64, msg.data[1..9][0..8], .big);
                            const server_timestamp = mem.readInt(i64, msg.data[9..17][0..8], .big);
                            const reply_requested = msg.data[17];

                            std.log.info("RECEIVED: msg type {s}, data type {s}, current_lsn: {d}, server_timestamp: {d}, reply_requested: {d}", .{[_]u8{msg.type}, [_]u8{data_type}, current_lsn, server_timestamp, reply_requested});

                            if (reply_requested == 1) {
                                try self.conn.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);
                            }
                        },
                        else => {}
                    }
                },
                'E' => {
                    std.log.info("RECEIVED: msg type {s}", .{[_]u8{msg.type}});
                    const pg_err = pg.Error.init(msg.data);
                    std.log.err("Error from server! Code: {s}, Message: {s}", .{pg_err.code, pg_err.message});
                    return error.InvalidOperation;
                },
                'c' => {
                    std.log.info("RECEIVED: msg type {s}, copy done", .{[_]u8{msg.type}});
                    assert(msg.len == 4);

                    return;
                },
                'C' => {
                    std.log.info("RECEIVED: msg type {s}, command complete", .{[_]u8{msg.type}});

                    return;
                },
                'Z' => {
                    const transaction_status = msg.data[0..1][0];

                    switch (transaction_status) {
                        'I' => {
                            // Idle
                            std.log.info("RECEIVED: msg type {s}, ready for query - idle", .{[_]u8{msg.type}});
                        },
                        'T' => {
                            // In Transaction
                            std.log.info("RECEIVED: msg type {s}, ready for query - in transaction", .{[_]u8{msg.type}});
                        },
                        'E' => {
                            std.log.info("RECEIVED: msg type {s}, ready for query", .{[_]u8{msg.type}});
                            std.log.err("ERROR: transaction error state", .{});
                            return error.TransactionErrorState;
                        },
                        else => return error.TransactionStateUnknown,
                    }

                    return;
                },
                else => {
                    std.log.info("RECEIVED: unhandled messages {s}", .{[_]u8{msg.type}});
                    // Ignore other messages
                }
            }
        }
    }

    pub fn decode(self: *@This(), payload: []const u8) !void {
        if (payload.len == 0) return;

        var reader = Io.Reader.fixed(payload);

        const msg_type = try reader.takeByte();

        switch (msg_type) {
            'B' => {
                const final_lsn = try reader.takeInt(u64, .big);
                const timestamp = try reader.takeInt(u64, .big);
                const xid = try reader.takeInt(u32, .big);

                std.log.info("- RECEIVED: msg type: {s}, final_lsn: {d}, timestamp: {d}, xid: {d}", .{[_]u8{msg_type}, final_lsn, timestamp, xid});

                return;
            },
            'C' => {
                const flag = try reader.takeByte();
                const commit_lsn = try reader.takeInt(u64, .big);
                const last_lsn = try reader.takeInt(u64, .big);
                const timestamp = try reader.takeInt(u64, .big);

                std.log.info(" - RECEIVED: msg type: {s}, flag: {d}, commit_lsn: {d}, last_lsn: {d}, timestamp: {d}", .{[_]u8{msg_type}, flag, commit_lsn, last_lsn, timestamp});

                self.last_lsn = last_lsn;
                self.last_timestamp = @as(i64, @intCast(timestamp));

                return;
            },
            'R' => {
                // Relation: send before any insert or update
                const rel_id = try reader.takeInt(u32, .big);
                const namespace = try reader.takeDelimiter(0);
                const rel_name = try reader.takeDelimiter(0);
                const repl_ident = try reader.takeByte();
                const num_columns = try reader.takeInt(u16, .big);

                std.log.info(" - RECEIVED: msg type: {s}, rel_id: {d}, namespace: {s}, rel_name: {s}, repl_ident: {d}, num_columns: {d}", .{[_]u8{msg_type}, rel_id, namespace.?, rel_name.?, repl_ident, num_columns});

                var i: u16 = 0;
                while (i < num_columns) : (i += 1) {
                    const flag = try reader.takeByte();
                    const column_name = try reader.takeDelimiter(0);
                    const type_id = try reader.takeInt(u32, .big);
                    const typemode = try reader.takeInt(u32, .big);

                    std.log.info(" -- flag: {d}, column_name: {s}, type_id: {d}, typemode: {d}", .{flag, column_name.?, type_id, typemode});
                }

                return;
            },
            'I' => {
                const rel_id = try reader.takeInt(u32, .big);
                const tuple_type = try reader.takeByte();

                std.log.info(" - RECEIVED: msg type: {s}, rel_id: {d}, tuple_type: {d}", .{[_]u8{msg_type}, rel_id, tuple_type});

                try parseTupleData(&reader);

                return;
            },
            'U' => {
                const rel_id = try reader.takeInt(u32, .big);
                var tuple_type = try reader.takeByte();

                std.log.info(" - RECEIVED: msg type: {s}, rel_id: {d}, tuple_type: {s}", .{[_]u8{msg_type}, rel_id, [_]u8{tuple_type}});

                try parseTupleData(&reader);

                tuple_type = try reader.takeByte();

                std.log.info(" - RECEIVED: #2 tuple_type: {s}", .{[_]u8{tuple_type}});

                try parseTupleData(&reader);

                return;
            },
            'D' => {
                const rel_id = try reader.takeInt(u32, .big);

                const tuple_type = try reader.takeByte();

                std.log.info(" - RECEIVED: msg type: {s}, rel_id: {d}, tuple_type: {s}", .{[_]u8{msg_type}, rel_id, [_]u8{tuple_type}});

                try parseTupleData(&reader);

                return;
            },
            'M' => {
                const flags = try reader.takeByte();
                const lsn = try reader.takeInt(u64, .big);

                const prefix = try reader.takeDelimiter(0);

                std.log.info(" - RECEIVED: msg type: {s}, flags: {d}, lsn: {d}, prefix: {s}", .{[_]u8{msg_type}, flags, lsn, prefix.?});

                const content_len = try reader.takeInt(u32, .big);
                const content = try reader.take(content_len);

                if (mem.eql(u8, prefix.?, "ergo_meta")) {
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

                    std.log.info(" -- {s}: {s}, {s}: {s}", .{user_id_key, user_id_value, ip_address_key, ip_address_value});
                }

                return;
            },
            'Y' => {
                const xid = try reader.takeInt(i32, .big);
                const oid = try reader.takeInt(i32, .big);
                const namespace = try reader.takeDelimiter(0);
                const data_type_name = try reader.takeDelimiter(0);

                std.log.info(" - RECEIVED: msg type: {s}, xid: {d}, oid: {d}, namespace: {s}, data_type_name: {s}", .{[_]u8{msg_type}, xid, oid, namespace.?, data_type_name.?});

                return;
            },
            else => {
                std.log.err(" - ERROR: Unknown pgoutput message type: {c}", .{msg_type});
            }
        }

        return;
    }

    fn parseTupleData(reader: *Io.Reader) !void {
        const num_columns = try reader.takeInt(u16, .big);

        var i: u32 = 0;
        while (i < num_columns) : (i += 1) {
            const col_type = try reader.takeByte();

            switch (col_type) {
                'n' => {
                    std.log.info(" -- col_type: {s}", .{[_]u8{col_type}});
                    // Null
                },
                'u' => {
                    std.log.info(" -- col_type: {s}", .{[_]u8{col_type}});
                    // Unchanged TOAST
                },
                't' => {
                    const col_len = try reader.takeInt(u32, .big);

                    const val_raw = try reader.take(col_len);

                    std.log.info(" -- col_type: {s}, data: {s}", .{[_]u8{col_type}, val_raw});
                },
                else => std.log.err(" -- ERROR: unknown tuple type", .{}),
            }
        }

        return;
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator: mem.Allocator = init.arena.allocator();
    const io = init.io;

    var options: Options = try .init(allocator, init.environ_map);
    defer options.deinit(allocator);

    var conn = try allocator.create(pg.Conn);
    conn.* = pg.Conn.init(io, allocator, .{
        .port = options.pg_port,
        .host = options.pg_host,
        .wal = options.pg_wal,
        .username = options.pg_user,
        .password = options.pg_pass,
        .database = options.pg_db,
        .application_name = application_name,
        .timeout_ms = 10_000,
        .startup_parameters = null
    }) catch |err| {
        std.debug.print("Failed to connect: {}", .{err});
        return err;
    };
    errdefer allocator.destroy(conn);

    conn.auth() catch |err| {
        if (conn.err) |pg_err| {
            std.log.err("ERROR: failed to auth: {} {s}: {s}", .{err, pg_err.code, pg_err.message});
        } else {
            std.log.err("ERROR: failed to auth: {}", .{err});
        }
        std.process.exit(1);
        return;
    };

    var logger = WALLogger.init(conn);

    try logger.startFlow(500);
    defer {
        logger.endFlow() catch {};
    }

    // Blocking
    try logger.readWAL();

    std.process.exit(0);
}
