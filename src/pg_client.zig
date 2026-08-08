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
UnableToSendCopyDone,
TransactionErrorState,
TransactionStateUnknown,
};

pub const PgClient = struct {
    allocator: mem.Allocator,
    io: Io,

    opts: pg.PgConfig,

    last_lsn: u64,
    last_timestamp: i64,

    server_status: pg.packet.ServerPacket,

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
            .server_status = .ReadyForQuery,
            .parser = .init(allocator),
            .conn = conn,
        };
    }

    pub fn deinit(self: *@This()) void {
        if (self.conn) |conn| {
            conn.deinit();
            self.allocator.destroy(conn);
        }
        self.parser.deinit();
    }

    pub fn cancel(self: *@This()) void {
        if (self.conn) |conn| conn.cancel();
    }

    pub fn startFlow(self: *@This(), timeout_ms: i32) !void {
        if (self.conn == null) return pg.PgError.WalConnectionNotInitialized;
        try self.conn.?.reader.startWALFlow(self.opts.wal, timeout_ms);
    }

    pub fn endFlow(self: *@This()) !void {
        if (self.conn == null) return pg.PgError.WalConnectionNotInitialized;
        try self.conn.?.reader.endWALFlow(self.opts.wal);
    }

    pub fn readWAL(self: *@This()) !?types.Transaction {
        if (self.conn == null) return pg.PgError.WalConnectionNotInitialized;

        var transaction: ?types.Transaction = null;

        while(true) {
            const msg = self.conn.?.reader.next() catch |err| switch (err) {
                error.Timeout => {
                    if (transaction == null) {
                        return err;
                    }
                    continue; 
                },
                else => return err,
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

                            self.server_status = .XLogData;

                            if (transaction == null) transaction = .empty;

                            const start_lsn = mem.readInt(u64, msg.data[1..9][0..8], .big);
                            const server_timestamp = mem.readInt(i64, msg.data[17..25][0..8], .big);

                            self.last_timestamp = server_timestamp;

                            const payload = msg.data[25..];

                            const parse_response = try self.parser.decode(payload);
                            
                            // Proactively acknowledge this processed WAL chunk
                            try self.conn.?.sendStandbyStatusUpdate(self.last_lsn, self.last_timestamp);

                            if (parse_response) |res| {
                                if (res.data) |row| {
                                    try transaction.?.rows.append(self.allocator, row);
                                }

                                if (res.xid) |xid| {
                                    transaction.?.meta.transaction_id = xid;
                                }

                                if (res.user_id) |user_id| {
                                    transaction.?.meta.user_id = user_id;
                                }

                                if (res.ip_address) |ip_address| {
                                    transaction.?.meta.ip_address = ip_address;
                                }

                                if (res.timestamp) |timestamp| {
                                    transaction.?.meta.event_time = timestamp;
                                }

                                if (res.last_lsn) |lsn| {
                                    if (lsn > self.last_lsn) {
                                        self.last_lsn = lsn;

                                        self.parser.clear();

                                        return transaction.?;
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

                            self.server_status = .Keepalive;

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

                    self.server_status = .CopyDone;

                    return null;
                },
                'C' => {
                    self.server_status = .CommandComplete;

                    return null;
                },
                'Z' => {
                    self.server_status = .ReadyForQuery;

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

                    return null;
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
};
