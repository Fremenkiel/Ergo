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

const Entry = struct {
    path: []const []const u8,
    value: Value,
};

const Value = union(enum) {
    string: []const u8,
    integer: u16,
    boolean: bool
};

const ParseError = error{
InvalidTableDefinition,
} || File.OpenError || Reader.DelimiterError || ConvertKeyValueError || ParseTomlError;

const ConvertKeyValueError = error{
InvalidKey,
InvalidValue,
InvalidKeyValuePair,
} || ConvertValueError;

const ConvertValueError = error{
EmptyValue,
InvalidType,
InvalidString,
} || fmt.ParseIntError || Allocator.Error;

const ParseTomlError = error{
EmptyPath,
InvalidStructFormat,
InvalidKeyValue,
UnknownKey,
} || ConvertTomlError;

const ConvertTomlError = error{
InvalidValueType,
IntegerOutOfRange,
};

pub fn loadToml(
        allocator: Allocator,
        io: Io,
        target: *Config,
        sub_path: []const u8
    ) ParseError!void {
    return TomlParser.parse(Config, allocator, io, target, sub_path);
}


const TomlParser = struct {
    pub fn parse(
        comptime T: type,
        allocator: Allocator,
        io: Io,
        target: *T,
        sub_path: []const u8
    ) ParseError!void {
        const cwd = Dir.cwd();
        const file = try cwd.openFile(io, sub_path, .{ .mode = .read_only });
        defer file.close(io);

        var buf: [4096]u8 = undefined;
        var reader = file.reader(io, &buf);
        const r = &reader.interface;

        var entries: std.ArrayList(Entry) = try .initCapacity(allocator, 16);
        defer entries.clearAndFree(allocator);

        var table: []const u8 = "";
        while (true) {
            const line = try r.takeDelimiter('\n') orelse break;

            const trimmed = mem.trim(u8, line, &.{' '});

            if (trimmed.len == 0) continue;
            switch (trimmed[0]) {
                '#' => continue,
                '[' => {
                    const len = trimmed.len;
                    if (trimmed[len - 1] != ']') return ParseError.InvalidTableDefinition;
                    table = trimmed[1..len - 1];
                },
                else => {
                    const entry = try convertKeyValue(allocator, table, trimmed);
                    try entries.append(allocator, entry);
                }
            }
        }

        for (entries.items) |*entry| {
            try parseToml(T, target, entry.path, entry.value);

            allocator.free(entry.path[0]);
            allocator.free(entry.path[1]);
            allocator.free(entry.path);
        }
    }

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

        const path = try allocator.alloc([] const u8, 2);
        errdefer allocator.free(path);

        path[0] = try allocator.dupe(u8, table);
        errdefer allocator.free(path[0]);

        path[1] = try allocator.dupe(u8, key);

        return Entry{
            .path = path,
            .value = value,
        };
    }

    fn convertValue(allocator: Allocator, value: []const u8) ConvertValueError!Value {
        if (value.len == 0) return ConvertValueError.EmptyValue;

        switch (value[0]) {
            '"' => {
                const vlen = value.len;
                if (value[vlen - 1] != '"') return ConvertValueError.InvalidString;
                return Value{ .string = try allocator.dupe(u8, value[1..vlen - 1]) };
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

    fn parseToml(
        comptime T: type,
        target: *T,
        path: []const []const u8,
        value: Value,
    ) ParseTomlError!void {
        if (path.len == 0) return ParseTomlError.EmptyPath;

        const info = @typeInfo(T).@"struct";

        inline for (info.field_names, info.field_types) |name, field| {
            if (mem.eql(u8, name, path[0])) {
                if (comptime @typeInfo(field) == .@"struct") {
                    if (path.len == 1) return ParseTomlError.InvalidStructFormat;

                    return parseToml(field, &@field(target.*, name), path[1..], value);
                } else {
                    if (path.len != 1) return ParseTomlError.InvalidKeyValue;

                    @field(target.*, name) = try convertToml(field, value);
                    return;
                }
            }
        }

        return ParseTomlError.UnknownKey;
    }

    fn convertToml(comptime T: type, value: Value) ConvertTomlError!T {
        if (T == []const u8) {
            return switch (value) {
                .string => |s| s,
                else => error.InvalidValueType,
            };
        } else if (T == bool) {
            return switch (value) {
                .boolean => |b| b,
                else => error.InvalidValueType,
            };
        } else if (@typeInfo(T) == .int) {
            return switch (value) {
                .integer => |n| std.math.cast(T, n) orelse
                error.IntegerOutOfRange,
                else => error.InvalidValueType,
            };
        } else {
            @compileError("Unsupported type: " ++ @typeName(T));
        }
    }
};

const TestConfig = struct {
    database: DatabaseConfig,
    app: AppConfig,

    const DatabaseConfig = struct {
        host: []const u8,
        port: u16,
        tls: bool
    };

    const AppConfig = struct {
        log_level: []const u8
    };
};

const testing = std.testing;
test "toml: parse config" {
    const allocator = testing.allocator;
    const io = testing.io;

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const content = 
        \\ [database]
        \\ host = "localhost"
        \\ port = 1234
        \\ tls = true
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

    const config = try TomlParser.parse(
        TestConfig,
        allocator,
        io,
        file_path,
    );
    defer {
        allocator.free(config.database.host);
        allocator.free(config.app.log_level);
        allocator.destroy(config);
    }

    try testing.expectEqualSlices(u8, config.database.host, "localhost");
    try testing.expectEqual(config.database.port, 1234);
    try testing.expectEqual(config.database.tls, true);

    try testing.expectEqualSlices(u8, config.app.log_level, "err");
}
