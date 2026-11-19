const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const File = Io.File;
const Reader = Io.Reader;
const Writer = Io.Writer;
const mem = std.mem;
const Allocator = mem.Allocator;
const fmt = std.fmt;
const posix = std.posix;
const Environ = Environ;

const root = @import("root.zig");
const Config = root.Config;
const TestPauseOptions = root.TestPauseOptions;
const SslMode = root.SslMode;

const Entry = struct {
    path: []const []const u8,
    value: Value,
};

const Value = union(enum) {
    string: ?[]const u8,
    integer: u16,
    boolean: bool
};

pub const LoadTomlError = error{
InvalidTableDefinition,
} || File.OpenError || Reader.DelimiterError || ConvertKeyValueError || ParseTomlError;

pub fn loadToml(
        allocator: Allocator,
        io: Io,
        target: *Config,
        sub_path: []const u8
    ) LoadTomlError!void {
    return parseFile(Config, allocator, io, target, sub_path);
}

const ParseFileError = error{
InvalidTableDefinition,
} || File.OpenError || Reader.DelimiterError || ConvertKeyValueError || ParseTomlError;

fn parseFile(
comptime T: type,
allocator: Allocator,
io: Io,
target: *T,
sub_path: []const u8
    ) ParseFileError!void {
    const cwd = Dir.cwd();
    const file = try cwd.openFile(io, sub_path, .{ .mode = .read_only });
    defer file.close(io);

    var buf: [4096]u8 = undefined;
    var reader = file.reader(io, &buf);
    const r = &reader.interface;

    var entries: std.ArrayList(Entry) = try .initCapacity(allocator, 16);
    defer {
        for (entries.items) |*entry| {
            for (entry.path) |path| allocator.free(path);
            allocator.free(entry.path);
        }

        entries.deinit(allocator);
    }
    errdefer for (entries.items) |*entry| if (entry.value == .string and entry.value.string != null) allocator.free(entry.value.string.?);

    var table: []const u8 = "";
    while (true) {
        const line = try r.takeDelimiter('\n') orelse break;

        const trimmed = mem.trim(u8, line, &.{' '});

        if (trimmed.len == 0) continue;
        switch (trimmed[0]) {
            '#' => continue,
            '[' => {
                const len = trimmed.len;
                if (trimmed[len - 1] != ']') return ParseFileError.InvalidTableDefinition;
                table = trimmed[1..len - 1];
            },
            else => {
                const entry = try convertKeyValue(allocator, table, trimmed);
                try entries.append(allocator, entry);
            }
        }
    }

    for (entries.items) |*entry| {
        try parseToml(allocator, T, target, entry.path, entry.value);
    }
}

const ConvertKeyValueError = error{
InvalidKey,
InvalidValue,
InvalidKeyValuePair,
} || ConvertValueError;

fn convertKeyValue(allocator: Allocator, table: []const u8, line: []const u8) ConvertKeyValueError!Entry {
    var it = mem.splitScalar(u8, line, '=');

    const raw_key = it.next();
    if (raw_key == null) return ConvertKeyValueError.InvalidKey;

    const key = mem.trim(u8, raw_key.?, &.{' '});

    const raw_val = it.next();
    if (raw_val == null) return ConvertKeyValueError.InvalidValue;

    const val = mem.trim(u8, raw_val.?, &.{' '});

    if (it.next() != null) return ConvertKeyValueError.InvalidKeyValuePair;

    const value = try convertValue(allocator, val);

    const path = try allocator.alloc([] const u8, if (mem.indexOfScalar(u8, key, '.') != null) 3 else 2);
    errdefer allocator.free(path);

    var i: usize = 0;
    path[i] = try allocator.dupe(u8, table);
    errdefer for (0..i) |pi| allocator.free(path[pi]);

    var kit = mem.splitScalar(u8, key, '.');
    while (kit.next()) |k| {
        i += 1;
        path[i] = try allocator.dupe(u8, k);
    }

    return Entry{
        .path = path,
        .value = value,
    };
}

const ConvertValueError = error{
EmptyValue,
InvalidType,
InvalidString,
} || fmt.ParseIntError || Allocator.Error;

fn convertValue(allocator: Allocator, value: []const u8) ConvertValueError!Value {
    if (value.len == 0) return ConvertValueError.EmptyValue;

    switch (value[0]) {
        '"' => {
            const vlen = value.len;
            if (value[vlen - 1] != '"') return ConvertValueError.InvalidString;
            const v = value[1..vlen - 1];
            return Value{ .string = if (v.len > 0) try allocator.dupe(u8, v) else null };
        },
        '0'...'9' => {
            return Value{ .integer = try fmt.parseInt(u16, value, 10) };
        },
        else => {
            if (mem.eql(u8, value, "false")) {
                return Value{ .boolean = false };
            }
            if (mem.eql(u8, value, "true")) {
                return Value{ .boolean = true };
            }
        }
    }

    return ConvertValueError.InvalidType;
}

const ParseTomlError = error{
EmptyPath,
InvalidStructFormat,
InvalidUnionFormat,
InvalidKeyValue,
InvalidValueType,
UnknownKey,
} || ConvertTomlError;

fn parseToml(
allocator: Allocator,
comptime T: type,
target: *T,
path: []const []const u8,
value: Value,
    ) ParseTomlError!void {
    if (path.len == 0) return ParseTomlError.EmptyPath;

    const info = @typeInfo(T).@"struct";

    inline for (info.field_names, info.field_types) |name, field| {
        if (mem.eql(u8, name, path[0])) {
            switch (comptime @typeInfo(field)) {
                .@"struct" => {
                    if (path.len == 1) return ParseTomlError.InvalidStructFormat;

                    return parseToml(allocator, field, &@field(target.*, name), path[1..], value);
                },
                .@"union" => |union_info| {
                    const variant_name = switch (path.len) {
                        1 => switch (value) {
                            .string => |ns| if (ns) |s| s else return ParseTomlError.InvalidValueType,
                            else => return ParseTomlError.InvalidValueType,
                        },
                        2 => path[1],
                        else => return ParseTomlError.InvalidUnionFormat,
                    };

                    inline for (
                    union_info.field_names,
                    union_info.field_types,
                    ) |variant, Payload| {
                        if (mem.eql(u8, variant, variant_name)) {
                            switch (Payload) {
                                void => {
                                    if (path.len != 1)
                                    return ParseTomlError.InvalidUnionFormat;

                                    @field(target.*, name) = @unionInit(field, variant, {});

                                    if (path.len == 1) {
                                        allocator.free(value.string.?);
                                    }
                                },
                                else => {
                                    if (path.len != 2)
                                    return ParseTomlError.InvalidUnionFormat;

                                    const base_type = switch (@typeInfo(Payload)) {
                                        .optional => |opt| opt.child,
                                        else => Payload,
                                    };

                                    @field(target.*, name) = @unionInit(
                                    field,
                                    variant,
                                    try convertToml(base_type, value),
                                    );
                                }
                            }
                            return;
                        }
                    }
                },
                else => {
                    if (path.len != 1) return ParseTomlError.InvalidKeyValue;

                    @field(target.*, name) = try convertToml(field, value) orelse return ParseTomlError.InvalidValueType;
                    return;
                }
            }
        }
    }

    return ParseTomlError.UnknownKey;
}

const ConvertTomlError = error{
InvalidValueType,
IntegerOutOfRange,
};

fn convertToml(comptime T: type, value: Value) ConvertTomlError!?T {
    switch (T) {
        []const u8 => return switch (value) {
            .string => |s| s,
            else => error.InvalidValueType,
        },
        bool => 
        return switch (value) {
            .boolean => |b| b,
            else => error.InvalidValueType,
        },
        // not supported in toml config, only for test.
        TestPauseOptions => return null,
        else => switch (@typeInfo(T)) {
            .int => return switch (value) {
                .integer => |n| std.math.cast(T, n) orelse
                error.IntegerOutOfRange,
                else => error.InvalidValueType,
            },
            .optional => |info| return try convertToml(info.child, value),
            else => @compileError("Unsupported type: " ++ @typeName(T)),
        }
    }
}

const TestConfig = struct {
    database: DatabaseConfig,
    app: AppConfig,

    const DatabaseConfig = struct {
        host: []const u8,
        port: u16,
        tls: bool,
        sslmode: SslMode,
    };

    const AppConfig = struct {
        log_level: []const u8,
        timeout: ?u16,
    };
};

const testing = std.testing;

test "parse: ensure correct config" {
    const allocator = testing.allocator;
    const io = testing.io;

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ host = "localhost"
        \\ port = 1234
        \\ tls = true
        \\ sslmode.verify_full = "ca.crt"
        \\ 
        \\ # Configure the app log level.
        \\ [app]
        \\ log_level = "err"
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        );
        defer {
            allocator.free(config.database.host);
            allocator.free(config.database.sslmode.verify_full.?);
            allocator.free(config.app.log_level);
        }

        try testing.expectEqualSlices(u8, "localhost", config.database.host);
        try testing.expectEqual(1234, config.database.port);
        try testing.expectEqual(true, config.database.tls);
        try testing.expectEqualSlices(u8, "ca.crt", config.database.sslmode.verify_full.?);

        try testing.expectEqualSlices(u8, "err", config.app.log_level);
        try testing.expectEqual(null, config.app.timeout);
    }

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ sslmode.verify_full = ""
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        );

        try testing.expect(config.database.sslmode == .verify_full);
        try testing.expectEqual(null, config.database.sslmode.verify_full);
    }

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ sslmode = "off"
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        );

        try testing.expect(config.database.sslmode == .off);
    }

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ sslmode = "require"
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        );

        try testing.expect(config.database.sslmode == .require);
    }
}

test "parse: invalid types" {
    const allocator = testing.allocator;
    const io = testing.io;

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ port = "NOT_VALID"
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try testing.expectEqual(ParseFileError.InvalidValueType, parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        ));
    }

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ host = 1234
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try testing.expectEqual(ParseFileError.InvalidValueType, parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        ));
    }

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ tls = "INVALID"
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try testing.expectEqual(ParseFileError.InvalidValueType, parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        ));
    }

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ tls = INVALID
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try testing.expectEqual(ParseFileError.InvalidType, parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        ));
    }

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try testing.expectEqual(ParseFileError.InvalidTableDefinition, parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        ));
    }

    {
        var tmp_dir = testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        const content = 
        \\ [database]
        \\ secure = true
        ;

        const file = try tmp_dir.dir.createFile(io, "config.toml", .{});
        try file.writeStreamingAll(io, content);
        file.close(io);

        const file_path = try allocator.print(".zig-cache/tmp/{s}/config.toml", .{ tmp_dir.sub_path });
        defer allocator.free(file_path);

        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try testing.expectEqual(ParseFileError.UnknownKey, parseFile(
        TestConfig,
        allocator,
        io,
        config,
        file_path,
        ));
    }
}

test "convert key value: sub key definition" {
    const allocator = testing.allocator;

    const entry = try convertKeyValue(
    allocator,
    "postgres",
    "sslmode.verify_full = \"ca.crt\""
    );
    defer {
        for (entry.path) |path| allocator.free(path);
        allocator.free(entry.path);
        if (entry.value == .string and entry.value.string != null) allocator.free(entry.value.string.?);
    }

    try testing.expectEqual(3, entry.path.len);

    try testing.expectEqualSlices(u8, "postgres", entry.path[0]);
    try testing.expectEqualSlices(u8, "sslmode", entry.path[1]);
    try testing.expectEqualSlices(u8, "verify_full", entry.path[2]);

    try testing.expectEqualSlices(u8, "ca.crt", entry.value.string.?);
}

test "parse toml: union" {
    const allocator = testing.allocator;

    {
        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try parseToml(
        allocator,
        TestConfig,
        config,
        &.{
            "database",
            "sslmode",
        },
        Value{ .string = try allocator.dupe(u8, "off") },
        );

        try testing.expect(config.database.sslmode == .off);
    }

    {
        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try parseToml(
        allocator,
        TestConfig,
        config,
        &.{
            "database",
            "sslmode",
        },
        Value{ .string = try allocator.dupe(u8, "require") },
        );

        try testing.expect(config.database.sslmode == .require);
    }

    {
        const config = try allocator.create(TestConfig);
        defer allocator.destroy(config);

        try parseToml(
        allocator,
        TestConfig,
        config,
        &.{
            "database",
            "sslmode",
            "verify_full"
        },
        Value{ .string = "ca.crt" },
        );

        try testing.expect(config.database.sslmode == .verify_full);
        try testing.expectEqualSlices(u8, "ca.crt", config.database.sslmode.verify_full.?);
    }
}
