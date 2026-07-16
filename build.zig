const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const exe = b.addExecutable(.{
        .name = "ergo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

    // Types
    const types = b.createModule(.{
        .root_source_file = b.path("src/types.zig"),
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("types", types);

    const buffer_dep = b.dependency("buffer", .{
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("buffer", buffer_dep.module("buffer"));
    
    const metrics_dep = b.dependency("metrics", .{
        .target = target,
        .optimize = optimize,
    });
    exe.root_module.addImport("metrics", metrics_dep.module("metrics"));

    // CH Module
    const ch_module = b.createModule(.{
        .root_source_file = b.path("src/ch/ch.zig"),
        .target = target,
        .optimize = optimize,
    });
    ch_module.addCSourceFile(.{
        .file = b.path("deps/lz4/lib/lz4.c"),
        .flags = &[_][]const u8{"-std=c99"},
    });
    ch_module.addIncludePath(b.path("deps/lz4/lib"));
    exe.root_module.addImport("ch", ch_module);

    // CH Client
    const ch_client_module = b.createModule(.{
        .root_source_file = b.path("src/ch_client.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "ch", .module = ch_module },
            .{ .name = "types", .module = types },
        }
    });
    exe.root_module.addImport("ch_client", ch_client_module);

    // Pg Module
    const openssl_lib_name = b.option([]const u8, "openssl_lib_name", "");
    const openssl_lib_path = b.option(std.Build.LazyPath, "openssl_lib_path", "");
    const openssl_include_path = b.option(std.Build.LazyPath, "openssl_include_path", "");
    const openssl = b.option(bool, "openssl", "Enable OpenSSL/TLS support") orelse
        (openssl_lib_name != null or openssl_lib_path != null or openssl_include_path != null);

    const openssl_module = if (openssl) blk: {
        const t = b.addTranslateC(.{
            .root_source_file = b.path("src/pg/openssl.h"),
            .target = target,
            .optimize = optimize,
        });
        if (openssl_include_path) |p| t.addIncludePath(p);
        break :blk t.createModule();
    } else b.createModule(.{ .root_source_file = b.path("src/pg/openssl_stub.zig") });

    const pg_module = b.addModule("pg", .{
        .target = target,
        .optimize = optimize,
        .root_source_file = b.path("src/pg/pg.zig"),
        .imports = &.{
            .{ .name = "buffer", .module = buffer_dep.module("buffer") },
            .{ .name = "metrics", .module = metrics_dep.module("metrics") },
            .{ .name = "openssl", .module = openssl_module },
        },
    });

    if (openssl) {
        if (openssl_lib_path) |p| pg_module.addLibraryPath(p);
        pg_module.linkSystemLibrary("crypto", .{});
        pg_module.linkSystemLibrary(openssl_lib_name orelse "ssl", .{});
        pg_module.link_libc = true;
    }

    var column_names = false;
    const column_names_opt = b.option(bool, "column_names", "");

    if (column_names_opt) |val| {
        column_names = val;
    }

    {
        const options = b.addOptions();
        options.addOption(bool, "openssl", openssl);
        options.addOption(bool, "column_names", column_names);
        pg_module.addOptions("config", options);
    }
    exe.root_module.addImport("pg", pg_module);

    // PG Client
    const pg_client_module = b.createModule(.{
        .root_source_file = b.path("src/pg_client.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "pg", .module = pg_module },
            .{ .name = "types", .module = types },
        }
    });
    exe.root_module.addImport("pg_client", pg_client_module);

    // Wal processor
    const wal_processor_module = b.createModule(.{
        .root_source_file = b.path("src/wal_processor.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "pg", .module = pg_module },
            .{ .name = "types", .module = types },
            .{ .name = "pg_client", .module = pg_client_module },
        }
    });
    exe.root_module.addImport("wal_processor", wal_processor_module);

    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    if (b.args) |args| {
        run_cmd.addArgs(args);
    }

    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
    });
    const run_exe_tests = b.addRunArtifact(exe_tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_exe_tests.step);

    const pg_client_tests = b.addTest(.{
        .root_module = pg_client_module,
    });
    const run_pg_client_tests = b.addRunArtifact(pg_client_tests);
    test_step.dependOn(&run_pg_client_tests.step);

    const wal_processor_tests = b.addTest(.{
        .root_module = wal_processor_module,
    });
    const run_wal_processor_tests = b.addRunArtifact(wal_processor_tests);
    test_step.dependOn(&run_wal_processor_tests.step);

    const bench = b.addExecutable(.{
        .name = "ergo_bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tests/clickhouse_integration.zig"),
            .target = target,
            .optimize = .ReleaseFast,
            .imports = &.{
                .{ .name = "ch", .module = ch_module },
                .{ .name = "ch_module", .module = ch_client_module },
            }
        }),
    });

    const run_bench_tests = b.addRunArtifact(bench);

    const bench_step = b.step("bench", "Run ClickHouse connection benchmarks");
    bench_step.dependOn(&run_bench_tests.step);
    run_cmd.step.dependOn(b.getInstallStep());
}
