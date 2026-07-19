const std = @import("std");

const protocol = @import("protocol.zig");

pub const ServerInfo = struct {
    allocator: std.mem.Allocator,

    name: []const u8,
    major_version: u64,
    minor_version: u64,
    revision: u64,
    timezone: []const u8,
    display_name: []const u8,
    version_patch: u64,

    pub fn read(allocator: std.mem.Allocator, reader: *std.Io.Reader) !ServerInfo {
        const name = try allocator.dupe(u8, try protocol.readString(reader));

        const major_version = try protocol.readVarInt(reader);
        const minor_version = try protocol.readVarInt(reader);
        const revision = try protocol.readVarInt(reader);

        const tz = try allocator.dupe(u8, try protocol.readString(reader));

        const display = try allocator.dupe(u8, try protocol.readString(reader));

        const version_patch = try protocol.readVarInt(reader);

        return ServerInfo{
            .allocator = allocator,
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
