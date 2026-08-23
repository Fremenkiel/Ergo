const std = @import("std");

const Io = std.Io;
const mem = std.mem;


pub const ColumnDef = struct {
    name: []const u8,
    is_key: bool,
};

pub const TableDef = struct {
    namespace: []const u8,
    name: []const u8,
    columns: std.ArrayList(ColumnDef),

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        for (self.columns.items) |*col| { allocator.free(col.name); }
        self.columns.clearAndFree(allocator);
        self.columns.deinit(allocator);

        allocator.free(self.namespace);
        allocator.free(self.name);
    }
};

const TrackingAllocator = struct {
    parent_allocator: mem.Allocator,

    current_allocated: usize = 0,
    peak_allocated: usize = 0,

    tracking: bool = false,
    tracked_allocated: usize = 0,
    tracked_peak_allocated: usize = 0,
    tracked_total_allocated: usize = 0,

    pub fn allocator(self: *@This()) mem.Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize, 
                .free = free,
                .remap = remap,
            }
        };
    }

    pub fn startTracking(self: *@This()) void {
        self.tracking = true;
    }

    pub fn stopTracking(self: *@This()) void {
        self.tracking = false;
    }

    pub fn resetTracing(self: *@This()) void {
        self.tracked_allocated = 0;
    }

    pub fn increaseAllocation(self: *@This(), len: usize) void {
        self.current_allocated += len;

        if (self.tracking) {
            self.tracked_allocated += len;
            self.tracked_total_allocated += len;
            if (self.tracked_allocated > self.tracked_peak_allocated) {
                self.tracked_peak_allocated = self.tracked_allocated;
            }
        }

        if (self.current_allocated > self.peak_allocated) {
            self.peak_allocated = self.current_allocated;
        }
    }

    pub fn decreaseAllocation(self: *@This(), len: usize) void {
        self.current_allocated -= len;

        if (self.tracking) {
            self.tracked_allocated -= len;
        }
    }

    pub fn alloc(ctx: *anyopaque, len: usize, alignment: mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));

        const result = self.parent_allocator.rawAlloc(len, alignment, ret_addr);

        if (result != null) {
            self.increaseAllocation(len);
        }
        return result;
    }

    pub fn resize(ctx: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(ctx));

        if (self.parent_allocator.rawResize(memory, alignment, new_len, ret_addr)) {
            if (new_len > memory.len) {
                self.increaseAllocation(new_len - memory.len);
            } else {
                self.decreaseAllocation(memory.len - new_len);
            }
            return true;
        }
        return false;
    }

    pub fn free(ctx: *anyopaque, memory: []u8, alignment: mem.Alignment, ret_addr: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));

        self.decreaseAllocation(memory.len);

        self.parent_allocator.rawFree(memory, alignment, ret_addr);
    }

    pub fn remap(ctx: *anyopaque, memory: []u8, alignment: mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));

        return self.parent_allocator.rawRemap(memory, alignment, new_len, ret_addr);
    }
};

pub fn removeAndPut(io: Io) !void {
    var debug_alloc = std.heap.DebugAllocator(.{
        .stack_trace_frames = 40,
    }).init;
    defer {
        if(debug_alloc.deinit() == .leak) {
            std.log.err("memory leaked", .{});
        }
    }

    var tracker = TrackingAllocator{ .parent_allocator = debug_alloc.allocator() };

    const allocator = tracker.allocator();

    tracker.startTracking();

    var table_reg: std.hash_map.HashMap(u32, TableDef, std.hash_map.AutoContext(u32), 80) = .init(allocator);
    defer {
        var it = table_reg.iterator(); 
        while (it.next()) |reg| { reg.value_ptr.*.deinit(allocator); }
        table_reg.clearAndFree();
        table_reg.deinit();
    }

    var columns = std.ArrayList(ColumnDef).empty;
    try columns.ensureUnusedCapacity(allocator, 4);

    columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "id"), . is_key = true });
    columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "address_line_1"), . is_key = false });
    columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "postal_code"), . is_key = false });
    columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "city"), . is_key = false });

    const rel_id = 12345;

    try table_reg.put(rel_id, .{
        .namespace = try allocator.dupe(u8, "public"),
        .name = try allocator.dupe(u8, "address"),
        .columns = columns,
    });

    var start = std.Io.Clock.real.now(io);

    var new_columns = std.ArrayList(ColumnDef).empty;
    try new_columns.ensureUnusedCapacity(allocator, 5);

    new_columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "id"), . is_key = true });
    new_columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "address_line_1"), . is_key = false });
    new_columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "address_line_2"), . is_key = false });
    new_columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "postal_code"), . is_key = false });
    new_columns.appendAssumeCapacity(.{ .name = try allocator.dupe(u8, "city"), . is_key = false });

    var key_value = table_reg.fetchRemove(rel_id);
    if (key_value) |*kv| {
        kv.value.deinit(allocator);
    }

    try table_reg.put(rel_id, .{
        .namespace = try allocator.dupe(u8, "public"),
        .name = try allocator.dupe(u8, "address"),
        .columns = new_columns,
    });

    std.log.info("time elapsed: {d}", .{ start.untilNow(io, .real).toNanoseconds() });
    std.log.info("memory alloc: {d}", .{ tracker.tracked_allocated });
    std.log.info("memory peak alloc: {d}", .{ tracker.tracked_peak_allocated });
    std.log.info("memory total alloc: {d}", .{ tracker.tracked_total_allocated });
}

pub fn main(init: std.process.Init) !void {
    _ = init;
}
