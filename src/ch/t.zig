const ChConfig = @import("root.zig").ChConfig;

pub const default_config: ChConfig = .{
    .host = "localhost",
    .port = 9000,
    .username = "default",
    .password = "clickhouse",
    .database = "audit_log",
    .application_name = "Ergo test",
};
