pub const ServerPacket = enum(u64) {
    XLogData = 0,
    Keepalive = 1,
    CopyDone = 2,
    CommandComplete = 3,
    ReadyForQuery = 4,
};

pub const ClientPacket = enum(u64) {
    StandbyStatusUpdate = 0,
    HotStandbyFeedback = 1,
};
