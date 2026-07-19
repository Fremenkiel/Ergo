const std = @import("std");

const protocol = @import("protocol.zig");

pub const ServerPacket = enum(u64) {
    Hello = 0,
    Data = 1,
    Exception = 2,
    Progress = 3,
    Pong = 4,
    EndOfStream = 5,
    ProfileInfo = 6,
    Totals = 7,
    Extremes = 8,
    TablesStatusResponse = 9,
    Log = 10,
    TableColumns = 11,
    PartUUIDs = 12,
    ReadTaskRequest = 13,
    ProfileEvents = 14,
    MergeTreeAllRangesAnnouncement = 15,
    MergeTreeReadTaskRequest = 16,
};

pub const ClientPacket = enum(u64) {
    Hello = 0,
    Query = 1,
    Data = 2,
    Cancel = 3,
    Ping = 4,
    TablesStatusRequest = 5,
    KeepAlive = 6,
    Scalar = 7,
    IgnoredPartUUIDs = 8,
    ReadTaskResponse = 9,
    MergeTreeReadTaskResponse = 10,
    SSHChallengeRequest = 11,
    SSHChallengeResponse = 12,
    QueryPlan = 13,
};

pub fn writeClientPacketHeader(writer: *std.Io.Writer, packet_type: ClientPacket) !void {
    try protocol.writeVarInt(writer, @intFromEnum(packet_type));
}
