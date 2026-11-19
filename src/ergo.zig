const std = @import("std");
const Io = std.Io;
const mem = std.mem;
const Allocator = mem.Allocator;
const Thread = std.Thread;
const posix = std.posix;

const Config = @import("config/root.zig").Config;

const ChClient = @import("ch_client.zig").ChClient;
const PgClient = @import("pg_client.zig").PgClient;
const PgConfig = @import("pg").PgConfig;

const ConnectionManager = @import("connection_manager.zig").ConnectionManager;
const WalStream = @import("wal_stream.zig").WalStream;

const test_hooks = @import("test_hooks.zig");

pub const application_name = "Ergo";

pub const Ergo = struct {
    allocator: Allocator,
    io: Io,

    clickhouse_client: ChClient,
    postgres_client: PgClient,

    streamer: WalStream,

    prober_thread: Thread,

    sigcall_future: ?Io.Future(void),

    shutdown_event: Io.Event = .unset,

    pub fn init(allocator: Allocator, io: Io, config: *Config, os_user: []const u8) !*@This() {
        var ch_client = ChClient.init(allocator, io, .{
            .host = config.clickhouse.host,
            .port = config.clickhouse.port,
            .database = config.clickhouse.database,
            .username = config.clickhouse.user,
            .password = config.clickhouse.password,
            .application_name = application_name,
        }, os_user);

        try ch_client.connect();

        // implement these
        _ = config.replication.batch_size;

        // todo: redo this (Kevin)
        const pg_config: PgConfig = .{
            .host = config.postgres.host,
            .port = config.postgres.port,
            .database = config.postgres.database,
            .username = config.postgres.user,
            .password = config.postgres.password,
            .wal = config.postgres.slot,
            .application_name = application_name,
            .timeout_ms = 10_000,
            .startup_parameters = null
        };

        var pg_client = try PgClient.init(allocator, io, pg_config);

        var conn_manager = ConnectionManager.init(allocator, io, pg_config);

        var barrier: test_hooks.TransactionReceivedBarrier = .{};

        const barrier_prt = if(config.test_pause_at) |test_pause_at| switch (test_pause_at) {
            .transaction_received_before_enqueue => &barrier,
        } else null;

        const streamer = WalStream.init(
        allocator,
        io,
        &ch_client,
        &pg_client,
        &conn_manager,
        config.replication.flush_interval_ms,
        barrier_prt,
        );

        const prober_thread = try Thread.spawn(
        .{}, 
        ConnectionManager.runProber, 
        .{ &conn_manager }
        );

        var mask = posix.sigemptyset();
        posix.sigaddset(&mask, .INT);
        posix.sigaddset(&mask, .TERM);

        posix.sigprocmask(posix.SIG.BLOCK, &mask, null);

        var ergo = try allocator.create(Ergo);

        ergo.* = .{
            .allocator = allocator,
            .io = io,
            .clickhouse_client = ch_client,
            .postgres_client = pg_client,
            .streamer = streamer,
            .prober_thread = prober_thread,
            .sigcall_future = null,
        };

        ergo.sigcall_future = try io.concurrent(waitForSignal, .{ ergo, mask });

        try ergo.streamer.startStreaming();

        return ergo;
    }
    
    pub fn deinit(self: *@This()) void {
        self.streamer.endStreaming() catch |err| std.log.err("Error: An error occurred while ending stream: {s}\n", .{ @errorName(err) });

        self.prober_thread.detach();

        if (self.sigcall_future) |*fut| {
            fut.cancel(self.io);
            self.sigcall_future = null;
        }

        defer self.streamer.deinit();

        defer self.postgres_client.deinit();
        defer self.postgres_client.cancel();

        defer self.clickhouse_client.disconnect();
        defer self.clickhouse_client.deinit();

        self.allocator.destroy(self);
    }

    pub fn waitForSignal(self: *@This(), mask: posix.sigset_t) void {
        var signals = mask;
        var signal: c_int = undefined;

        if (std.c.sigwait(&signals, &signal) == 0) {
            self.shutdown_event.set(self.io);
        }
    }

    pub fn run(self: *@This()) !void {
        try self.streamer.stream(&self.shutdown_event);
    }

};
