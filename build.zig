const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{
        .preferred_optimize_mode = .ReleaseSafe
    });

    const exe = b.addExecutable(.{
        .name = "ergo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });

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

    exe_tests.root_module.addImport("buffer", buffer_dep.module("buffer"));
    exe_tests.root_module.addImport("metrics", metrics_dep.module("metrics"));
    exe_tests.root_module.addImport("ch", ch_module);
    exe_tests.root_module.addImport("pg", pg_module);

    const run_exe_tests = b.addRunArtifact(exe_tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(b.getInstallStep());
    test_step.dependOn(&run_exe_tests.step);

    // CH test
    const ch_tests = b.addTest(.{
        .root_module = ch_module,
    });
    const run_ch_tests = b.addRunArtifact(ch_tests);
    test_step.dependOn(&run_ch_tests.step);

    // PG test
    // const pg_tests = b.addTest(.{
    //     .root_module = pg_module,
    // });
    // const run_pg_tests = b.addRunArtifact(pg_tests);
    // test_step.dependOn(&run_pg_tests.step);
}
