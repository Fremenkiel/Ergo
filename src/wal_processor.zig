const std = @import("std");
const types = @import("types.zig");

pub fn WalProcessor(comptime PgClient: type, comptime ChClient: type) type {
    return struct {
        duration: std.Io.Duration = std.Io.Duration.fromSeconds(1),
        last_write_timestamp: std.Io.Timestamp,
        uncommited_changes: bool = false,
        log_array: std.ArrayList([]types.AuditEntry) = .empty,

        allocator: std.mem.Allocator,
        io: std.Io,

        pg_client: *PgClient,
        ch_client: *ChClient,

        pub fn startStreaming(self: *@This()) !void {
            try self.pg_client.startWALReader();

            var transaction_array: std.ArrayList(types.AuditEntry) = .empty;

            errdefer transaction_array.deinit(self.allocator);

            while (true) {
                const response = try self.pg_client.readWAL();
                if (response.entry) |entry| {
                    try transaction_array.append(self.allocator, entry);
                }

                if (response.commit_timestamp != null) {
                    for (transaction_array.items) |*row| {
                        row.event_time = response.commit_timestamp.?;
                    }
                    try self.log_array.append(self.allocator, try self.allocator.dupe(types.AuditEntry, transaction_array.items));

                    self.uncommited_changes = false;
                    transaction_array.clearAndFree(self.allocator);
                } else { 
                    self.uncommited_changes = true;
                }

                if (self.last_write_timestamp.addDuration(self.duration).toMilliseconds() < std.Io.Clock.real.now(self.io).toMilliseconds() and !self.uncommited_changes and self.log_array.items.len > 0) {
                    try self.ch_client.writeLog(self.io, self.log_array.items);

                    self.log_array.clearRetainingCapacity();
                    self.last_write_timestamp = std.Io.Clock.real.now(self.io);
                }
            }
        }
    };
}
