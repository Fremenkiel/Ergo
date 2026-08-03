const std = @import("std");

const Io = std.Io;
const mem = std.mem;

const pg = @import("pg");
const pg_client = @import("pg_client.zig");
const types = @import("types.zig");

pub const AllTypesColumn = enum { 
    id,
    col_int2,
    col_int2_arr,
    col_int4,
    col_int4_arr,
    col_int8,
    col_int8_arr,
    col_float4,
    col_float4_arr,
    col_float8,
    col_float8_arr,
    col_bool,
    col_bool_arr,
    col_text,
    col_text_arr,
    col_bytea,
    col_bytea_arr,
    col_enum,
    col_enum_arr,
    col_uuid,
    col_uuid_arr,
    col_numeric,
    col_numeric_arr,
    col_timestamp,
    col_timestamp_arr,
    col_json,
    col_json_arr,
    col_jsonb,
    col_jsonb_arr,
    col_char,
    col_char_arr,
    col_charn,
    col_charn_arr,
    col_timestamptz,
    col_timestamptz_arr,
    col_cidr,
    col_cidr_arr,
    col_inet,
    col_inet_arr,
    col_macaddr,
    col_macaddr_arr,
    col_macaddr8,
    col_macaddr8_arr,
};

pub const ChClient = struct {
    allocator: mem.Allocator,
    written_logs: std.ArrayList(types.AuditEntry) = .empty,
    log_array: std.ArrayList(types.AuditEntry) = .empty,

    pub fn init(allocator: mem.Allocator) @This() {
        return .{.allocator = allocator};
    }

    pub fn deinit(self: *@This()) void {
        for (self.written_logs.items) |*item| item.deinit(self.allocator);
        self.written_logs.deinit(self.allocator);
        for (self.log_array.items) |*entry| entry.deinit(self.allocator);
        self.log_array.deinit(self.allocator);
    }

    pub fn writeLog(self: *@This(), entries: []types.AuditEntry) !void {
        var copy_slice = try self.allocator.alloc(types.AuditEntry, entries.len);
        for (entries, 0..) |entry, i| {
            copy_slice[i] = entry;

            copy_slice[i].table_name = try self.allocator.dupe(u8, entry.table_name);
            copy_slice[i].user_id = try self.allocator.dupe(u8, entry.user_id);
            copy_slice[i].ip_address = try self.allocator.dupe(u8, entry.ip_address);

            copy_slice[i].changed_columns = try entry.changed_columns.clone();
            copy_slice[i].new_values = try entry.new_values.clone(self.allocator);
            copy_slice[i].old_values = try entry.old_values.clone(self.allocator);
            copy_slice[i].primary_key = try self.allocator.dupe(u8, entry.primary_key);

            var old_it = copy_slice[i].old_values.iterator();
            while (old_it.next()) |kv| {
                kv.value_ptr.* = try self.allocator.dupe(u8, kv.value_ptr.*);
            }

            var new_it = copy_slice[i].new_values.iterator();
            while (new_it.next()) |kv| {
                kv.value_ptr.* = try self.allocator.dupe(u8, kv.value_ptr.*);
            }
        }

        try self.written_logs.appendSlice(self.allocator, copy_slice);
        self.allocator.free(copy_slice);
    }
};

pub const PgClient = struct {
    allocator: mem.Allocator,
    io: Io,

    responses: []pg_client.ReadResponse,
    read_response_index: ?u8,

    default_opts: void,
    wal_opts: void,

    wal_conn: *bool,
    default_conn: *bool,

    pub fn init(allocator: mem.Allocator, io: Io, responses: []pg_client.ReadResponse) !@This() {
        const conn = try allocator.create(bool);
        conn.* = true;
        return .{
            .allocator = allocator,
            .io = io,
            .responses = responses,
            .read_response_index = null,
            .default_opts = {},
            .default_conn = undefined,
            .wal_opts = {},
            .wal_conn = conn,
        };
    }

    pub fn cancel(self: *@This()) void {
        self.wal_conn.* = false;
    }

    pub fn deinit(self: *@This()) void {
        self.allocator.destroy(self.wal_conn);
    }

    pub fn startWALReader(self: *@This(), timeout_ms: i32) !void {
        _ = timeout_ms;
        if (!self.wal_conn.*) return pg_client.PgClientError.WalConnectionNotInitialized;
    }

    pub fn endWALReader(self: *@This()) !void {
        if (!self.wal_conn.*) return pg_client.PgClientError.WalConnectionNotInitialized;
    }

    pub fn sendCopyDone(self: *@This()) !void {
        _ = self;
    }

    pub fn readWAL(self: *@This()) error{Timeout,WouldBlock,Canceled}!?pg_client.ReadResponse {
        if (self.read_response_index == null) {
            self.read_response_index = 0;
        } else {
            if (self.read_response_index.? > self.responses.len) {
                try Io.sleep(self.io, Io.Duration{ .nanoseconds = 500 * std.time.ns_per_ms }, .real);
                return error.Timeout;
            }
            self.read_response_index.? += 1;
        }

        if (self.read_response_index.? == self.responses.len) {
            return .{
                .data = null,
                .timestamp = null,
                .message = pg.packet.ServerPacket.ReadyForQuery,
            };
        }

        return self.responses[self.read_response_index.?];
    }

    pub fn createConn(allocator: mem.Allocator, io: Io, opts: anytype) !*bool {
        _ = io;
        _ = opts;

        const new_conn = try allocator.create(bool);
        new_conn.* = true;
        return new_conn;
    }
};

pub fn genereateDbName(allocator: mem.Allocator, io: Io) ![]const u8 {
    const db_name = try std.fmt.allocPrint(allocator, "test_db_{d}", .{std.Io.Clock.real.now(io).toNanoseconds()});
    errdefer allocator.free(db_name);

    return db_name;
}

pub fn createTestChDb(allocator: mem.Allocator, io: Io, db_name: []const u8) !void {
    // CH
    const ch_query = try std.fmt.allocPrint(allocator, "CREATE DATABASE IF NOT EXISTS {s}", .{db_name});
    defer allocator.free(ch_query);

    var ch_create_argv = [_][]const u8{ 
        "clickhouse-client", 
        "--host", "127.0.0.1",
        "--port", "9000",
        "--user", "default",
        "--password", "clickhouse",
        "--query", ch_query
    };
    const ch_create_result = try std.process.run(allocator, io, .{ 
        .argv = &ch_create_argv,
    });
    defer {
        allocator.free(ch_create_result.stdout);
        allocator.free(ch_create_result.stderr);
    }

    if (ch_create_result.stderr.len > 0) {
        std.debug.print("Error: unable to create new ch db: {s}\n", .{ch_create_result.stderr});
        return error.CreateTestDbFailedError;
    }

    var ch_init_argv = [_][]const u8{ 
        "clickhouse-client", 
        "--host", "127.0.0.1",
        "--port", "9000",
        "--user", "default",
        "--password", "clickhouse",
	"--database", db_name,
	"--queries-file", "./infra/ch/init.sql"
    };
    const ch_init_result = try std.process.run(allocator, io, .{ 
        .argv = &ch_init_argv,
    });
    defer {
        allocator.free(ch_init_result.stdout);
        allocator.free(ch_init_result.stderr);
    }

    if (ch_init_result.stderr.len > 0) {
        std.debug.print("Error: CH create db failed: {s}\n", .{ch_init_result.stderr});
        return error.CreateTestDbFailedInitError;
    }
}

pub fn createTestPgDb(allocator: mem.Allocator, io: Io, db_name: []const u8) !void {
    // PG
    const pg_query = try std.fmt.allocPrint(allocator, "CREATE DATABASE {s}", .{db_name});
    defer allocator.free(pg_query);

    var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "postgres");

    var pg_create_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "postgres",
        "-d", "postgres",
        "-q", "-t", "-A", // Formatting
        "-c", pg_query,
    };
    const pg_create_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_create_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_create_result.stdout);
        allocator.free(pg_create_result.stderr);
    }

    if (pg_create_result.term != .exited or pg_create_result.term.exited != 0 or pg_create_result.stderr.len > 0) {
        std.debug.print("Error: PSQL create failed: {s}\n", .{pg_create_result.stderr});
        return error.PsqlExecutionFailed;
    }

    var pg_init_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "postgres",
        "-d", db_name,
        "-a",
        "-f", "./test_fixtures/create-schema.sql"
    };
    const pg_init_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_init_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_init_result.stdout);
        allocator.free(pg_init_result.stderr);
    }

    if (pg_init_result.term != .exited or pg_init_result.term.exited != 0 or pg_init_result.stderr.len > 0) {
        std.debug.print("Error: PSQL init failed: {s}\n", .{pg_init_result.stderr});
        return error.PsqlExecutionFailed;
    }
}

pub fn createTestDb(allocator: mem.Allocator, io: Io, db_name: []const u8) !void {
    try createTestChDb(allocator, io, db_name);
    try createTestPgDb(allocator, io, db_name);
}

pub fn teardownTestChDb(allocator: mem.Allocator, io: Io, db_name: []const u8) !void {
    // CH
    const ch_query = try std.fmt.allocPrint(allocator, "DROP DATABASE {s}", .{db_name});
    defer allocator.free(ch_query);

    var ch_db_argv = [_][]const u8{ 
        "clickhouse-client", 
        "--host", "127.0.0.1",
        "--port", "9000",
        "--user", "default",
        "--password", "clickhouse",
        "--query", ch_query
    };
    const ch_db_result = try std.process.run(allocator, io, .{ 
        .argv = &ch_db_argv,
    });
    defer {
        allocator.free(ch_db_result.stdout);
        allocator.free(ch_db_result.stderr);
    }

    if (ch_db_result.stderr.len > 0) {
        std.debug.print("Error: CH drop db failed: {s}\n", .{ch_db_result.stderr});
        return error.CreateTestDbFailedError;
    }
}

pub fn teardownTestPgDb(allocator: mem.Allocator, io: Io, db_name: []const u8, wal_name: []const u8) !void {
    // PG
    const pg_rep_query = try std.fmt.allocPrint(allocator, "SELECT pg_drop_replication_slot('{s}');", .{wal_name});
    defer allocator.free(pg_rep_query);
    
    const pg_db_query = try std.fmt.allocPrint(allocator, "DROP DATABASE {s}", .{db_name});
    defer allocator.free(pg_db_query);

    var pg_env = try std.process.Environ.createMap(std.testing.environ, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "postgres");

    var pg_rep_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "postgres",
        "-d", "postgres",
        "-q", "-t", "-A", // Formatting
        "-c", pg_rep_query,
    };
    const pg_rep_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_rep_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_rep_result.stdout);
        allocator.free(pg_rep_result.stderr);
    }

    if (pg_rep_result.term != .exited or pg_rep_result.term.exited != 0 or pg_rep_result.stderr.len > 0) {
        std.debug.print("Error: PSQL drop replication failed: {s}\n", .{pg_rep_result.stderr});
        return error.PsqlExecutionFailed;
    }

    var pg_db_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "postgres",
        "-d", "postgres",
        "-q", "-t", "-A", // Formatting
        "-c", pg_db_query,
    };
    const pg_db_result = try std.process.run(allocator, io, .{ 
        .argv = &pg_db_argv,
        .environ_map = &pg_env, 
    });
    defer {
        allocator.free(pg_db_result.stdout);
        allocator.free(pg_db_result.stderr);
    }

    if (pg_db_result.term != .exited or pg_db_result.term.exited != 0 or pg_db_result.stderr.len > 0) {
        std.debug.print("Error: PSQL drop db failed: {s}\n", .{pg_db_result.stderr});
        return error.PsqlExecutionFailed;
    }
}

pub fn teardownTestDb(allocator: mem.Allocator, io: Io, db_name: []const u8, wal_name: []const u8) !void {
    try teardownTestChDb(allocator, io, db_name);
    try teardownTestPgDb(allocator, io, db_name, wal_name);
}

