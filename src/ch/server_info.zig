const std = @import("std");

const protocol = @import("protocol.zig");

pub const ServerInfo = struct {
    name: []const u8,
    major_version: u64,
    minor_version: u64,
    revision: u64,
    timezone: []const u8,
    display_name: []const u8,
    version_patch: u64,

    pub fn read(reader: *std.Io.Reader) !ServerInfo {
        const name_len = try protocol.readVarInt(reader);
        const name = try reader.take(name_len);

        const major_version = try protocol.readVarInt(reader);
        const minor_version = try protocol.readVarInt(reader);
        const revision = try protocol.readVarInt(reader);

        const tz_len = try protocol.readVarInt(reader);
        const tz = try reader.take(tz_len);

        const display_len = try protocol.readVarInt(reader);
        const display = try reader.take(display_len);

        const version_patch = try protocol.readVarInt(reader);

        return ServerInfo{
            .name = name,
            .major_version = major_version,
            .minor_version = minor_version,
            .revision = revision,
            .timezone = tz,
            .display_name = display,
            .version_patch = version_patch,
        };
    }

    pub fn deinit(self: *ServerInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.timezone);
        allocator.free(self.display_name);
    }
};
