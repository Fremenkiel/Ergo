const std = @import("std");
const types = @import("types");

const pg = @import("pg");
const pg_client = @import("pg_client");

pub fn WalProcessor(comptime PgClient: type, comptime ChClient: type) type {
    return struct {
        duration: std.Io.Duration = std.Io.Duration.fromSeconds(1),
        last_write_timestamp: std.Io.Timestamp,
        uncommited_changes: bool = false,
        log_array: std.ArrayList([]types.AuditEntry) = .empty,

        allocator: std.mem.Allocator,
        io: std.Io,

        pg_client: *PgClient,
        ch_client: *ChClient,

        pub fn startStreaming(self: *@This()) !void {
            self.pg_client.startWALReader() catch |err| switch (err) {
                pg_client.PgClientError.WalConnectionNotInitialized => {
                    self.pg_client.*.wal_conn = try PgClient.createWalConn(self.allocator, self.io, self.pg_client.*.conn_opts);
                    try self.pg_client.startWALReader();
                },
                else => return err,
            };

            var transaction_array: std.ArrayList(types.AuditEntry) = .empty;

            errdefer transaction_array.deinit(self.allocator);

            while (true) {
                var response = try self.pg_client.readWAL();
                defer response.deinit(self.allocator);

                if (response.eof) {
                    return;
                }

                if (response.entry) |entry| {
                    try transaction_array.append(self.allocator, entry);
                }

                if (response.commit_timestamp != null) {
                    for (transaction_array.items) |*row| {
                        row.event_time = response.commit_timestamp.?;
                    }
                    try self.log_array.append(self.allocator, try self.allocator.dupe(types.AuditEntry, transaction_array.items));

                    self.uncommited_changes = false;
                    transaction_array.clearAndFree(self.allocator);
                } else { 
                    self.uncommited_changes = true;
                }

                std.debug.print("tester.....\n", .{});
                if (self.last_write_timestamp.addDuration(self.duration).toMilliseconds() < std.Io.Clock.real.now(self.io).toMilliseconds() and !self.uncommited_changes and self.log_array.items.len > 0) {
                    try self.ch_client.writeLog(self.io, self.log_array.items);

                    self.log_array.clearRetainingCapacity();
                    self.last_write_timestamp = std.Io.Clock.real.now(self.io);
                }
            }
        }
    };
}

const MockPgClient = struct {
    allocator: std.mem.Allocator,
    responses: []pg_client.ReadResponse,
    read_response_index: ?u8,

    conn_opts: void,
    wal_conn: *bool,

    pub fn init(allocator: std.mem.Allocator, responses: []pg_client.ReadResponse) !MockPgClient {
        const conn = try allocator.create(bool);
        conn.* = true;
        return .{
            .allocator = allocator,
            .responses = responses,
            .read_response_index = null,
            .conn_opts = {},
            .wal_conn = conn,
        };
    }

    pub fn deinit(self: *@This()) void {
        self.allocator.destroy(self.wal_conn);
    }

    pub fn startWALReader(self: *@This()) !void {
        if (!self.wal_conn.*) return pg_client.PgClientError.WalConnectionNotInitialized;
    }

    pub fn readWAL(self: *@This()) !pg_client.ReadResponse {
        if (self.read_response_index == null) {
            self.read_response_index = 0;
        } else {
            self.read_response_index.? += 1;
        }

        if (self.read_response_index.? == self.responses.len) {
            return pg_client.EOFReadResponse;
        }

        return self.responses[self.read_response_index.?];
    }

    pub fn createWalConn(allocator: std.mem.Allocator, io: std.Io, opts: anytype) !*bool {
        _ = io;
        _ = opts;

        const new_conn = try allocator.create(bool);
        new_conn.* = true;
        return new_conn;
    }
};

const MockChClient = struct {
    pub fn init() MockChClient {
        return .{};
    }

    pub fn writeLog(self: *@This(), io: std.Io, entries: [][]types.AuditEntry) !void {
        _ = self;
        _ = io;
        std.debug.print("CH LOGGER ------\n", .{});

        var i: u8 = 0;
        while (i < entries.len) : (i += 1) {
            for (entries[i]) |entry| {
                std.debug.print("Got: {s}\n", .{entry.table_name});
            }
        }
    }
};

test "startStreaming read and parse correctly" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    var changed_columns = std.StringHashMap(types.ChangedColumns).init(allocator);
    defer changed_columns.deinit();
    try changed_columns.ensureUnusedCapacity(1);

    try changed_columns.put("id", .{ .has_changes = true, .value = "1" });

    var res = [_]pg_client.ReadResponse{
        .{ 
            .entry = .{
                .event_time = undefined,
                .table_name = "test",
                .new_values = .empty,
                .old_values = .empty,
                .action = 1,
                .changed_columns = changed_columns,
                .transaction_id = 793,
                .user_id = "42",
                .ip_address = "192.168.1.50",
                .primary_key = "1",
            },
            .commit_timestamp = 10
        },
    };

    var mock_pg_client = try MockPgClient.init(allocator, &res);
    defer mock_pg_client.deinit();

    var mock_ch_client = MockChClient.init();

    var processor = WalProcessor(MockPgClient, MockChClient){ 
        .pg_client = &mock_pg_client, 
        .ch_client = &mock_ch_client,
        .last_write_timestamp = std.Io.Clock.real.now(io).subDuration(
        std.Io.Duration.fromSeconds(2)
    ),
        .allocator = allocator,
        .io = io,
    };

    try processor.startStreaming();
}
