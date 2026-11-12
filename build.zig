const std = @import("std");
const Build = std.Build;

const assert = std.debug.assert;

const Translator = @import("translate_c").Translator;

pub fn build(b: *std.Build) void {
    const test_filter = b.option([]const u8, "test_filter", "Test filter");

    const openssl_lib_name = b.option([]const u8, "openssl_lib_name", "Open SSL lib name | ssl");
    const openssl_lib_path = b.option(Build.LazyPath, "openssl_lib_path", "OpenSSL lib path | /opt/homebrew/opt/openssl@3/lib");
    const openssl_include_path = b.option(Build.LazyPath, "openssl_include_path", "OpenSSL include path | /opt/homebrew/opt/openssl@3/include");

    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{
        .preferred_optimize_mode = .ReleaseSafe
    });

    const translate_c = b.dependency("translate_c", .{});

    // lz4
    const liblz4 = buildLibLz4(b, target, optimize);
    const trans_liblz4: Translator = .init(translate_c, .{
        .c_source_file = b.path("deps/lz4/lib/lz4.h"),
        .target = target,
        .optimize = optimize,
    });
    trans_liblz4.linkLibrary(liblz4);

    // openssl
    const trans_libopenssl: Translator = .init(translate_c, .{
        .c_source_file = b.addWriteFiles().add("c.h", 
            \\ #include <openssl/ssl.h>
            \\ #include <openssl/err.h>
        ),
        .target = target,
        .optimize = optimize,
    });

    if (openssl_include_path) |path| {
        trans_libopenssl.addIncludePath(path);
    }

    if (openssl_lib_path) |path| {
        trans_libopenssl.mod.addLibraryPath(path);
    }

    trans_libopenssl.mod.link_libc = true;
    trans_libopenssl.mod.linkSystemLibrary(openssl_lib_name orelse "ssl", .{});
    trans_libopenssl.mod.linkSystemLibrary("crypto", .{});

    // ssl
    const ssl_module = b.createModule(.{
        .root_source_file = b.path("src/ssl/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "openssl", .module = trans_libopenssl.mod },
        },
    });

    // remove this
    const types_module = b.createModule(.{
        .root_source_file = b.path("src/types.zig"),
        .target = target,
        .optimize = optimize,
    });

    const phantom = b.dependency("zpg_phantom", .{
        .target = target,
        .optimize = optimize,
    });

    // CH Module
    const ch_module = b.createModule(.{
        .root_source_file = b.path("src/ch/root.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "types", .module = types_module },
            .{ .name = "lz4", .module = trans_liblz4.mod },
        },
    });

    // Pg Module
    const pg_module = b.addModule("pg", .{
        .target = target,
        .root_source_file = b.path("src/pg/root.zig"),
        .imports = &.{
            .{ .name = "ssl", .module = ssl_module },
            .{ .name = "types", .module = types_module },
            .{ .name = "phantom", .module = phantom.module("phantom") },
        },
    });


    // WTF is this?
    var column_names = false;
    const column_names_opt = b.option(bool, "column_names", "");

    if (column_names_opt) |val| {
        column_names = val;
    }

    const options = b.addOptions();
    options.addOption(bool, "column_names", column_names);
    pg_module.addOptions("config", options);

    const exe = b.addExecutable(.{
        .name = "ergo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/main.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    exe.root_module.addImport("pg", pg_module);
    exe.root_module.addImport("ch", ch_module);
    exe.root_module.addImport("types", types_module);
    
    b.installArtifact(exe);

    const run_step = b.step("run", "Run the app");
    const run_cmd = b.addRunArtifact(exe);
    run_step.dependOn(&run_cmd.step);
    run_cmd.step.dependOn(b.getInstallStep());

    run_cmd.addPassthruArgs();

    // CH test
    const ch_tests = b.addTest(.{
        .root_module = ch_module,
        .filters = if (test_filter) |filter| &.{filter} else &.{}
    });
    const run_ch_tests = b.addRunArtifact(ch_tests);

    const ch_test_step = b.step("ch_test", "Run ch tests");
    ch_test_step.dependOn(&run_ch_tests.step);

    // PG test
    const pg_tests = b.addTest(.{
        .root_module = pg_module,
        .filters = if (test_filter) |filter| &.{filter} else &.{}
    });
    const run_pg_tests = b.addRunArtifact(pg_tests);

    const pg_test_step = b.step("pg_test", "Run pg tests");
    pg_test_step.dependOn(&run_pg_tests.step);

    // Main test
    const exe_tests = b.addTest(.{
        .root_module = exe.root_module,
        .filters = if (test_filter) |filter| &.{filter} else &.{}
    });

    exe_tests.root_module.addImport("ch", ch_module);
    exe_tests.root_module.addImport("pg", pg_module);

    const run_exe_tests = b.addRunArtifact(exe_tests);
    const test_step = b.step("test", "Run tests");

    test_step.dependOn(b.getInstallStep());
    test_step.dependOn(&run_ch_tests.step);
    test_step.dependOn(&run_pg_tests.step);
    test_step.dependOn(&run_exe_tests.step);

    // Test conn
    const debug_exe = b.addExecutable(.{
        .name = "debug ergo",
        .root_module = b.createModule(.{
            .root_source_file = b.path("debug.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    debug_exe.root_module.addImport("ssl", ssl_module);
    debug_exe.root_module.addImport("pg", pg_module);
    // debug_exe.root_module.addImport("ch", ch_module);
    debug_exe.root_module.addImport("phantom", phantom.module("phantom"));
    // debug_exe.root_module.addImport("types", types_module);

    b.installArtifact(debug_exe);

    const debug_step = b.step("debug", "Run the app in debug mode");
    const debug_cmd = b.addRunArtifact(debug_exe);
    debug_step.dependOn(&debug_cmd.step);
    debug_cmd.step.dependOn(b.getInstallStep());

    debug_cmd.addPassthruArgs();

    // WAL logger util
    const wal_logger = b.addExecutable(.{
        .name = "wal_logger",
        .root_module = b.createModule(.{
            .root_source_file = b.path("tools/wal_logger.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    wal_logger.root_module.addImport("pg", pg_module);
    wal_logger.root_module.addImport("types", types_module);

    const run_wal_logger = b.addRunArtifact(wal_logger);
    
    run_wal_logger.addPassthruArgs();

    const wal_logger_step = b.step("wal-logger", "Run the WAL logger utility");
    wal_logger_step.dependOn(&run_wal_logger.step);
}

fn buildLibLz4(
b: *std.Build,
target: std.Build.ResolvedTarget,
optimize: std.builtin.OptimizeMode,
) *std.Build.Step.Compile {
    const mod = b.createModule(.{
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });

    mod.addCSourceFiles(.{
        .root = b.path("deps/lz4/lib"),
        .files = &.{ "lz4.c" },
        .flags = &[_][]const u8{"-std=c99"},
    });

    const lib = b.addLibrary(.{
        .name = "lz4",
        .root_module = mod,
    });

    lib.installHeader(b.path("deps/lz4/lib/lz4.h"), "lz4/lz4.h");

    return lib;
}
