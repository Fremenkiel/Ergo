const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const process = std.process;
const debug = std.debug;

const assert = debug.assert;

var mock_is_shutting_down = std.atomic.Value(bool).init(false);

pub fn runQuery(allocator: mem.Allocator, io: Io, db_name: []const u8, flag: *std.atomic.Value(bool), env: std.process.Environ) !void {
    var pg_env = try std.process.Environ.createMap(env, allocator);
    defer pg_env.deinit();
    try pg_env.put("PGPASSWORD", "12345678");

    var pg_argv = [_][]const u8{
        "psql",
        "-h", "127.0.0.1",
        "-p", "5432",
        "-U", "db_rw",
        "-d", db_name,
        "-a",
        "-f", "./test_fixtures/simple-query.sql"
    };

    {
        const pg_result = try std.process.run(allocator, io, .{ 
            .argv = &pg_argv,
            .environ_map = &pg_env, 
        });
        defer {
            allocator.free(pg_result.stdout);
            allocator.free(pg_result.stderr);
        }

        if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
            std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
            return error.PsqlExecutionFailed;
        }
    }

    var docker_kill_argv = [_][]const u8{
        "docker",
        "kill", "db"
    };
    const docker_kill_result = try std.process.run(allocator, io, .{ 
        .argv = &docker_kill_argv,
    });
    defer {
        allocator.free(docker_kill_result.stdout);
        allocator.free(docker_kill_result.stderr);
    }

    if (docker_kill_result.term != .exited or docker_kill_result.term.exited != 0 or docker_kill_result.stderr.len > 0) {
        std.debug.print("Error: Docker failed: {s}\n", .{docker_kill_result.stderr});
        return error.DockerKillFailed;
    }

    try Io.sleep(io, .{ .nanoseconds = 5 * std.time.ns_per_ms }, .real);

    var docker_start_argv = [_][]const u8{
        "docker",
        "start", "db"
    };
    const docker_start_result = try std.process.run(allocator, io, .{ 
        .argv = &docker_start_argv,
    });
    defer {
        allocator.free(docker_start_result.stdout);
        allocator.free(docker_start_result.stderr);
    }

    if (docker_start_result.term != .exited or docker_start_result.term.exited != 0 or docker_start_result.stderr.len > 0) {
        std.debug.print("Error: Docker failed: {s}\n", .{docker_start_result.stderr});
        return error.DockerStartFailed;
    }

    var container_ready_check_argv = [_][]const u8{
        "zsh", "-c",
        "while ! pg_isready -h 127.0.0.1 -U postgres -d postgres > /dev/null 2>&1; do sleep 1; done;",
    };
    const container_ready_check_result = try std.process.run(allocator, io, .{
        .argv = &container_ready_check_argv,
    });
    defer {
        allocator.free(container_ready_check_result.stdout);
        allocator.free(container_ready_check_result.stderr);
    }

    if (container_ready_check_result.term != .exited or container_ready_check_result.term.exited != 0 or container_ready_check_result.stderr.len > 0) {
        std.debug.print("Error: Container ready check failed: {s}\n", .{container_ready_check_result.stderr});
        return error.ContainerReadyCheck;
    }

    std.debug.print("check complete", .{});

    {
        const pg_result = try std.process.run(allocator, io, .{ 
            .argv = &pg_argv,
            .environ_map = &pg_env, 
        });
        defer {
            allocator.free(pg_result.stdout);
            allocator.free(pg_result.stderr);
        }

        if (pg_result.term != .exited or pg_result.term.exited != 0 or pg_result.stderr.len > 0) {
            std.debug.print("Error: PSQL failed: {s}\n", .{pg_result.stderr});
            return error.PsqlExecutionFailed;
        }
    }

    flag.store(true, .seq_cst);
}

pub fn main(init: process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;
    const env = init.minimal.environ;

    try runQuery(allocator, io, "db", &mock_is_shutting_down, env);
}
