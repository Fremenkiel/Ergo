const std = @import("std");

const Io = std.Io;
const mem = std.mem;
const testing = std.testing;

const assert = std.debug.assert;

const PgError = @import("root.zig").PgError;
const Timestamp = @import("types.zig").Timestamp;
const types = @import("types");

pub const ParseResponse = struct {
    data: ?types.Row,
    last_lsn: ?u64,
    timestamp: ?Io.Timestamp,
    xid: ?u32,
    user_id: ?[]const u8,
    ip_address: ?[]const u8,

    pub const empty: @This() = .{
        .last_lsn = null,
        .timestamp = null,
        .data = null,
        .xid = null,
        .user_id = null,
        .ip_address = null,
    };

    pub fn deinit(self: *@This(), allocator: mem.Allocator) void {
        if (self.user_id) |*user_id| allocator.free(user_id);
        self.user_id = null;

        if (self.ip_address) |*ip_address| allocator.free(ip_address);
        self.ip_address = null;

        if (self.data) |*entry| {
            entry.deinit(allocator);
        }
    }
};

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

const Action = enum(u8) {
    Insert,
    UpdateOld,
    UpdateNew,
    Delete,
};

pub const PgOutput = struct {
    allocator: mem.Allocator,

    table_reg: std.hash_map.HashMap(u32, TableDef, std.hash_map.AutoContext(u32), 80),

    pub fn init(allocator: mem.Allocator) @This() {
        return .{
            .allocator = allocator,
            .table_reg = .init(allocator)
        };
    }

    pub fn deinit(self: *@This()) void {
        var it = self.table_reg.iterator(); 
        while (it.next()) |table_reg| { table_reg.value_ptr.*.deinit(self.allocator); }
        self.table_reg.clearAndFree();
        self.table_reg.deinit();
    }

    pub fn clear(self: *@This()) void {
        var it = self.table_reg.iterator(); 
        while (it.next()) |table_reg| { table_reg.value_ptr.*.deinit(self.allocator); }
        self.table_reg.clearRetainingCapacity();
    }

    pub fn decode(self: *@This(), payload: []const u8) !?ParseResponse {
        var response: ParseResponse = .empty; 

        if (payload.len == 0) return response;

        var reader = Io.Reader.fixed(payload);

        const msg_type = try reader.takeByte();

        switch (msg_type) {
            'B' => {
                // final lsn
                _ = try reader.takeInt(u64, .big);

                // timestamp
                _ = try reader.takeInt(u64, .big);

                response.xid = try reader.takeInt(u32, .big);

                return response;
            },
            'C' => {
                // flags
                _ = try reader.takeByte();
                
                // lsn of commit
                _ = try reader.takeInt(u64, .big);

                response.last_lsn = try reader.takeInt(u64, .big);
                response.timestamp = Timestamp.decode(try reader.takeInt(u64, .big));

                self.clear();

                return response;
            },
            'R' => {
                // Relation: send before any insert or update
                const rel_id = try reader.takeInt(u32, .big);

                const namespace = try reader.takeDelimiter(0);
                if (namespace == null) {
                    return PgError.InvalidMessage;
                }

                const rel_name = try reader.takeDelimiter(0);
                if (rel_name == null) {
                    return PgError.InvalidMessage;
                }

                const repl_ident = try reader.takeByte();
                _ = repl_ident;

                const num_columns = try reader.takeInt(u16, .big);

                // const columns = try self.readSchemaKeys(table_name);
                var columns = std.ArrayList(ColumnDef).empty;

                var i: u16 = 0;
                while (i < num_columns) : (i += 1) {
                    const flag = try reader.takeByte();

                    // col name
                    const column_name = try reader.takeDelimiter(0);
                    if (column_name == null) {
                        return PgError.InvalidMessage;
                    }

                    // type_id
                    _ = try reader.takeInt(u32, .big);

                    // typemod
                    _ = try reader.takeInt(u32, .big);

                    try columns.append(self.allocator, .{ .name = try self.allocator.dupe(u8, column_name.?), .is_key = flag == 1 });
                }

                try self.table_reg.put(rel_id, .{
                    .namespace = try self.allocator.dupe(u8, namespace.?),
                    .name = try self.allocator.dupe(u8, rel_name.?),
                    .columns = columns,
                });

                return response;

            },
            'I' => {
                const rel_id = try reader.takeInt(u32, .big);
                const tuple_type = try reader.takeByte();

                if (tuple_type != 'N') {
                    std.debug.print("Error: Received insert with invalid tuple type: {c}\n", .{tuple_type});
                }


                if (self.table_reg.get(rel_id)) |table| {
                    const columns = try initContextColumns(self.allocator, table);

                    response.data = .{
                        .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                        .action = 1,
                        .columns = try self.parseTupleData(&reader, columns, .Insert),
                    };
                } else {
                    std.debug.print("Error: Received insert for unknown relation ID {d}\n", .{rel_id});
                }

                return response;
            },
            'U' => {
                const rel_id = try reader.takeInt(u32, .big);
                var tuple_type = try reader.takeByte();

                if (self.table_reg.get(rel_id)) |table| {
                    var columns = try initContextColumns(self.allocator, table);
                    if (tuple_type == 'O' or tuple_type == 'K') {
                        columns = try self.parseTupleData(&reader, columns, .UpdateOld);

                        tuple_type = try reader.takeByte();
                    }

                    if (tuple_type == 'N') {
                        response.data = .{
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .action = 2,
                            .columns = try self.parseTupleData(&reader, columns, .UpdateNew),
                        };
                    } else {
                        std.debug.print("Error: Expected 'N', got '{c}'\n", .{tuple_type});
                    }
                } else {
                    std.debug.print("Error: Received update for unknown relation ID {d}\n", .{rel_id});
                }

                return response;
            },
            'D' => {
                const rel_id = try reader.takeInt(u32, .big);

                const tuple_type = try reader.takeByte();

                if (self.table_reg.get(rel_id)) |table| {
                    const columns = try initContextColumns(self.allocator, table);
                    if (tuple_type == 'O' or tuple_type == 'K') {
                        response.data = .{
                            .table_name = try std.fmt.allocPrint(self.allocator, "{s}.{s}", .{table.namespace, table.name}),
                            .action = 3,
                            .columns = try self.parseTupleData(&reader, columns, .Delete),
                        };
                    } else {
                        std.debug.print("Error: Expected 'O' or 'K', got '{c}'\n", .{tuple_type});
                    }
                } else {
                    std.debug.print("Error: Received delete for unknown relation ID {d}\n", .{rel_id});
                }

                return response;
            },
            'M' => {
                const flags = try reader.takeByte();
                const lsn = try reader.takeInt(u64, .big);

                const prefix = try reader.takeDelimiter(0);
                if (prefix == null) {
                    return PgError.InvalidMessage;
                }

                const content_len = try reader.takeInt(u32, .big);

                const content = try reader.take(content_len);

                _ = flags;
                _ = lsn;
                if (mem.eql(u8, prefix.?, "ergo_meta")) {
                    var it = mem.splitAny(u8, content, ",");

                    const user_id_str = it.next() orelse return error.InvalidMapType;
                    const ip_address_str = it.next() orelse return error.InvalidMapType;

                    var user_id_it = mem.splitAny(u8, user_id_str, ":");
                    var ip_address_it = mem.splitAny(u8, ip_address_str, ":");

                    const user_id_key_str = user_id_it.next() orelse return error.InvalidMapType;
                    const ip_address_key_str = ip_address_it.next() orelse return error.InvalidMapType;

                    const user_id_key = mem.trim(u8, user_id_key_str, " ");
                    const ip_address_key = mem.trim(u8, ip_address_key_str, " ");

                    if (!mem.eql(u8, user_id_key, "\"user_id\"")) {
                        return error.InvalidMapType;
                    }

                    if (!mem.eql(u8, ip_address_key, "\"ip\"")) {
                        return error.InvalidMapType;
                    }

                    const user_id_value_str = user_id_it.next() orelse return error.InvalidMapType;
                    const ip_address_value_str = ip_address_it.next() orelse return error.InvalidMapType;

                    const user_id_value = mem.trim(u8, mem.trim(u8, user_id_value_str, " "), "\"");
                    const ip_address_value = mem.trim(u8, mem.trim(u8, ip_address_value_str, " "), "\"");

                    const check_str = try std.fmt.allocPrint(self.allocator, "{s}: \"{s}\", {s}: \"{s}\"", .{user_id_key, user_id_value, ip_address_key, ip_address_value});
                    defer self.allocator.free(check_str);
                    assert(mem.eql(u8, check_str, content));

                    response.user_id = try self.allocator.dupe(u8, user_id_value);
                    response.ip_address = try self.allocator.dupe(u8, ip_address_value);
                }

                return response;
            },
            'Y' => {
                // XID
                _ = try reader.takeInt(i32, .big);

                // OID
                _ = try reader.takeInt(i32, .big);
                
                // namespace
                _ = try reader.takeDelimiter(0);
                
                // data type name
                _ = try reader.takeDelimiter(0);

                return response;
            },
            else => {
                std.debug.print("Unknown pgoutput message type: {c}\n", .{msg_type});
            }
        }

        return null;
    }

    fn parseTupleData(self: *@This(), reader: *Io.Reader, columns: std.ArrayList(types.ColumnChange), action: Action) !std.ArrayList(types.ColumnChange) {
        const num_columns = try reader.takeInt(u16, .big);

        if (num_columns > columns.items.len) {
            return error.ColumnMismatch;
        }

        for (columns.items) |*col| {
            const col_type = try reader.takeByte();

            switch (col_type) {
                'n' => {
                    // Null
                },
                'u' => {
                    // Unchanged TOAST
                },
                't' => {
                    const col_len = try reader.takeInt(u32, .big);

                    const val_raw = try reader.take(col_len);
                    const val = try self.allocator.dupe(u8, val_raw);
                    errdefer self.allocator.free(val);

                    switch (action) {
                        .Insert => {
                            col.new_value = val;
                            col.has_changes = true;
                        },
                        .UpdateNew => {
                            col.new_value = val;

                            if ((col.old_value == null) != (col.new_value == null) or 
                                (col.old_value != null and col.new_value != null and !std.mem.eql(u8, col.old_value.?, col.new_value.?))) {
                                col.has_changes = true;
                            }
                        },
                        .UpdateOld => {
                            col.old_value = val;
                        },
                        .Delete => {
                            col.old_value = val;
                            col.has_changes = true;
                        },
                    }
                },
                else => return error.UnknownTupleFormat,
            }
        }

        return columns;
    }

};

fn initContextColumns(allocator: mem.Allocator, table_def: TableDef) !std.ArrayList(types.ColumnChange) {
    var columns: std.ArrayList(types.ColumnChange) = .empty;
    errdefer {
        columns.clearAndFree(allocator);
        columns.deinit(allocator);
    }

    try columns.ensureUnusedCapacity(allocator, table_def.columns.items.len);

    for (table_def.columns.items) |col| {
        columns.appendAssumeCapacity(.{
            .is_key = col.is_key,
            .column_name = try allocator.dupe(u8, col.name),
            .old_value = null,
            .new_value = null,
            .has_changes = false,
        });
    }

    return columns;
}

fn setupParser(allocator: mem.Allocator) !PgOutput {
    var cols = std.ArrayList(ColumnDef).empty;
    try cols.ensureUnusedCapacity(allocator, 6);
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "id"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "address_line_1"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "address_line_2"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "postal_code"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "city"), .is_key = true });
    try cols.append(allocator, .{ .name = try allocator.dupe(u8, "country"), .is_key = true });

    var table_reg = std.AutoHashMap(u32, TableDef).init(allocator);
    try table_reg.put(16390, .{
        .namespace = try allocator.dupe(u8, "public"),
        .name = try allocator.dupe(u8, "addresses"),
        .columns = cols,
    });

    return .{
        .allocator = allocator,
        .table_reg = table_reg,
    };
}

test "parsePgOutput maps BEGIN correctly" {
    const allocator = testing.allocator;

    const commit_hex = "420000000001c160880002f9a2afe34ece00000317";
    const xid: u32 = 791;

    const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
    defer allocator.free(commit_bytes);
    _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);

    var parser = PgOutput.init(allocator);
    defer parser.deinit();

    var result = try parser.decode(commit_bytes);
    defer {
        if (result) |*res| {
            res.deinit(allocator);
        }
    }

    try testing.expectEqual(true, result != null);
    try testing.expectEqual(xid, result.?.xid);
    try testing.expectEqual(null, result.?.ip_address);
    try testing.expectEqual(null, result.?.user_id);
    try testing.expectEqual(null, result.?.data);
    try testing.expectEqual(null, result.?.last_lsn);
    try testing.expectEqual(null, result.?.timestamp);
}

test "parsePgOutput maps METADATA correctly" {
    const allocator = testing.allocator;

    // PERFORM pg_logical_emit_message(
    //     true, 
    //     'ergo_meta', 
    //     '"user_id": "42", "ip": "192.168.1.50"'
    // );
    const metadata_hex = "4d010000000001c15f186572676f5f6d657461000000002522757365725f6964223a20223432222c20226970223a20223139322e3136382e312e353022";

    const metadata_bytes = try allocator.alloc(u8, metadata_hex.len / 2);
    defer allocator.free(metadata_bytes);
    _ = try std.fmt.hexToBytes(metadata_bytes, metadata_hex);

    var parser = PgOutput.init(allocator);
    defer parser.deinit();

    var result = try parser.decode(metadata_bytes);
    defer {
        if (result) |*res| {
            res.deinit(allocator);
        }
    }

    try testing.expectEqual(true, result != null);

    try testing.expectEqualStrings("42", result.?.user_id.?);
    try testing.expectEqualStrings("192.168.1.50", result.?.ip_address.?);

    try testing.expectEqual(true, result != null);
    try testing.expectEqual(null, result.?.data);
    try testing.expectEqual(null, result.?.last_lsn);
    try testing.expectEqual(null, result.?.timestamp);
}

test "parsePgOutput maps RELATION correctly" {
    const allocator = testing.allocator;

    // INSERT INTO addresses (address_line_1, postal_code, city, country) 
    // VALUES ('1 Apple Park Way', '95014', 'Cupertino', 'US');
    const insert_hex = "52000040067075626c696300616464726573736573006600060169640000000014ffffffff01616464726573735f6c696e655f3100000004130000010301616464726573735f6c696e655f3200000004130000010301706f7374616c5f636f6465000000041300000014016369747900000004130000010301636f756e747279000000041300000006";

    const insert_bytes = try allocator.alloc(u8, insert_hex.len / 2);
    defer allocator.free(insert_bytes);
    _ = try std.fmt.hexToBytes(insert_bytes, insert_hex);

    var parser = PgOutput.init(allocator);
    defer parser.deinit();

    var result = try parser.decode(insert_bytes);
    defer {
        if (result) |*res| {
            res.deinit(allocator);
        }
    }

    try testing.expectEqual(1, parser.table_reg.count());
}

test "parsePgOutput maps INSERT correctly" {
    const allocator = testing.allocator;

    // INSERT INTO addresses (address_line_1, postal_code, city, country) 
    // VALUES ('1 Apple Park Way', '95014', 'Cupertino', 'US');
    const insert_hex = "49000040064e0006740000000131740000001031204170706c65205061726b205761796e740000000539353031347400000009437570657274696e6f74000000025553";

    const insert_bytes = try allocator.alloc(u8, insert_hex.len / 2);
    defer allocator.free(insert_bytes);
    _ = try std.fmt.hexToBytes(insert_bytes, insert_hex);

    var parser = PgOutput.init(allocator);
    defer parser.deinit();

    var result = try parser.decode(insert_bytes);
    defer {
        if (result) |*res| {
            res.deinit(allocator);
        }
    }

    try testing.expectEqual(true, result != null);
    try testing.expectEqual(1, result.?.data.?.action);
    try testing.expectEqualStrings("public.addresses", result.?.data.?.table_name);

    try testing.expectEqual(6, result.?.data.?.columns.items.len);

    const id = result.?.data.?.columns.items[0];
    const address_line_1 = result.?.data.?.columns.items[1];
    const address_line_2 = result.?.data.?.columns.items[2];
    const postal_code = result.?.data.?.columns.items[3];
    const city = result.?.data.?.columns.items[4];
    const country = result.?.data.?.columns.items[5];

    // Column names
    try testing.expectEqualStrings("id", id.column_name);
    try testing.expectEqualStrings("address_line_1", address_line_1.column_name);
    try testing.expectEqualStrings("address_line_2", address_line_2.column_name);
    try testing.expectEqualStrings("postal_code", postal_code.column_name);
    try testing.expectEqualStrings("city", city.column_name);
    try testing.expectEqualStrings("country", country.column_name);

    // Changed columns
    try testing.expectEqual(true, id.has_changes);
    try testing.expectEqual(true, address_line_1.has_changes);
    try testing.expectEqual(false, address_line_2.has_changes);
    try testing.expectEqual(true, postal_code.has_changes);
    try testing.expectEqual(true, city.has_changes);
    try testing.expectEqual(true, country.has_changes);

    // Old values
    try testing.expectEqual(null, id.old_value);
    try testing.expectEqual(null, address_line_1.old_value);
    try testing.expectEqual(null, address_line_2.old_value);
    try testing.expectEqual(null, postal_code.old_value);
    try testing.expectEqual(null, city.old_value);
    try testing.expectEqual(null, country.old_value);

    // New values
    try testing.expectEqualStrings("1", id.new_value.?);
    try testing.expectEqualStrings("1 Apple Park Way", address_line_1.new_value.?);
    try testing.expectEqual(null, address_line_2.new_value);
    try testing.expectEqualStrings("95014", postal_code.new_value.?);
    try testing.expectEqualStrings("Cupertino", city.new_value.?);
    try testing.expectEqualStrings("US", country.new_value.?);

    try testing.expectEqual(null, result.?.xid);
    try testing.expectEqual(null, result.?.ip_address);
    try testing.expectEqual(null, result.?.user_id);
    try testing.expectEqual(null, result.?.last_lsn);
    try testing.expectEqual(null, result.?.timestamp);
}

test "parsePgOutput maps UPDATE correctly" {
    const allocator = testing.allocator;

    // UPDATE addresses SET 
    //  address_line_1 = 'Googleplex',
    //  city = 'Mountain View',
    //  postal_code = '94043'
    // WHERE id = 1;
    const update_hex = "55000040064f0006740000000131740000001031204170706c65205061726b205761796e740000000539353031347400000009437570657274696e6f740000000255534e0006740000000131740000000a476f6f676c65706c65786e74000000053934303433740000000d4d6f756e7461696e205669657774000000025553";

    const update_bytes = try allocator.alloc(u8, update_hex.len / 2);
    defer allocator.free(update_bytes);
    _ = try std.fmt.hexToBytes(update_bytes, update_hex);

    var parser = PgOutput.init(allocator);
    defer parser.deinit();

    var result = try parser.decode(update_bytes);
    defer {
        if (result) |*res| {
            res.deinit(allocator);
        }
    }

    try testing.expectEqual(true, result != null);
    try testing.expectEqual(2, result.?.data.?.action);
    try testing.expectEqualStrings("public.addresses", result.?.data.?.table_name);

    try testing.expectEqual(6, result.?.data.?.columns.items.len);

    const id = result.?.data.?.columns.items[0];
    const address_line_1 = result.?.data.?.columns.items[1];
    const address_line_2 = result.?.data.?.columns.items[2];
    const postal_code = result.?.data.?.columns.items[3];
    const city = result.?.data.?.columns.items[4];
    const country = result.?.data.?.columns.items[5];

    // Column names
    try testing.expectEqualStrings("id", id.column_name);
    try testing.expectEqualStrings("address_line_1", address_line_1.column_name);
    try testing.expectEqualStrings("address_line_2", address_line_2.column_name);
    try testing.expectEqualStrings("postal_code", postal_code.column_name);
    try testing.expectEqualStrings("city", city.column_name);
    try testing.expectEqualStrings("country", country.column_name);

    // Changed columns
    try testing.expectEqual(false, id.has_changes);
    try testing.expectEqual(true, address_line_1.has_changes);
    try testing.expectEqual(false, address_line_2.has_changes);
    try testing.expectEqual(true, postal_code.has_changes);
    try testing.expectEqual(true, city.has_changes);
    try testing.expectEqual(false, country.has_changes);

    // Old values
    try testing.expectEqualStrings("1", id.old_value.?);
    try testing.expectEqualStrings("1 Apple Park Way", address_line_1.old_value.?);
    try testing.expectEqual(null, address_line_2.old_value);
    try testing.expectEqualStrings("95014", postal_code.old_value.?);
    try testing.expectEqualStrings("Cupertino", city.old_value.?);
    try testing.expectEqualStrings("US", country.old_value.?);

    // New values
    try testing.expectEqualStrings("1", id.new_value.?);
    try testing.expectEqualStrings("Googleplex", address_line_1.new_value.?);
    try testing.expectEqual(null, address_line_2.new_value);
    try testing.expectEqualStrings("94043", postal_code.new_value.?);
    try testing.expectEqualStrings("Mountain View", city.new_value.?);
    try testing.expectEqualStrings("US", country.new_value.?);

    try testing.expectEqual(null, result.?.xid);
    try testing.expectEqual(null, result.?.ip_address);
    try testing.expectEqual(null, result.?.user_id);
    try testing.expectEqual(null, result.?.last_lsn);
    try testing.expectEqual(null, result.?.timestamp);
}

test "parsePgOutput maps DELETE correctly" {
    const allocator = testing.allocator;

    // DELETE FROM addresses WHERE id = 1;
    const delete_hex = "44000040064f0006740000000131740000000a476f6f676c65706c65786e74000000053934303433740000000d4d6f756e7461696e205669657774000000025553";

    const delete_bytes = try allocator.alloc(u8, delete_hex.len / 2);
    defer allocator.free(delete_bytes);
    _ = try std.fmt.hexToBytes(delete_bytes, delete_hex);

    var parser = PgOutput.init(allocator);
    defer parser.deinit();

    var result = try parser.decode(delete_bytes);
    defer {
        if (result) |*res| {
            res.deinit(allocator);
        }
    }

    try testing.expectEqual(true, result != null);
    try testing.expectEqual(3, result.?.data.?.action);
    try testing.expectEqualStrings("public.addresses", result.?.data.?.table_name);

    try testing.expectEqual(6, result.?.data.?.columns.items.len);

    const id = result.?.data.?.columns.items[0];
    const address_line_1 = result.?.data.?.columns.items[1];
    const address_line_2 = result.?.data.?.columns.items[2];
    const postal_code = result.?.data.?.columns.items[3];
    const city = result.?.data.?.columns.items[4];
    const country = result.?.data.?.columns.items[5];

    // Column names
    try testing.expectEqualStrings("id", id.column_name);
    try testing.expectEqualStrings("address_line_1", address_line_1.column_name);
    try testing.expectEqualStrings("address_line_2", address_line_2.column_name);
    try testing.expectEqualStrings("postal_code", postal_code.column_name);
    try testing.expectEqualStrings("city", city.column_name);
    try testing.expectEqualStrings("country", country.column_name);

    // Changed columns
    try testing.expectEqual(true, id.has_changes);
    try testing.expectEqual(true, address_line_1.has_changes);
    try testing.expectEqual(false, address_line_2.has_changes);
    try testing.expectEqual(true, postal_code.has_changes);
    try testing.expectEqual(true, city.has_changes);
    try testing.expectEqual(true, country.has_changes);

    // Old values
    try testing.expectEqualStrings("1", id.old_value.?);
    try testing.expectEqualStrings("Googleplex", address_line_1.old_value.?);
    try testing.expectEqual(null, address_line_2.old_value);
    try testing.expectEqualStrings("94043", postal_code.old_value.?);
    try testing.expectEqualStrings("Mountain View", city.old_value.?);
    try testing.expectEqualStrings("US", country.old_value.?);

    // New values
    try testing.expectEqual(null, id.new_value);
    try testing.expectEqual(null, address_line_1.new_value);
    try testing.expectEqual(null, address_line_2.new_value);
    try testing.expectEqual(null, postal_code.new_value);
    try testing.expectEqual(null, city.new_value);
    try testing.expectEqual(null, country.new_value);

    try testing.expectEqual(0, result.?.xid);
    try testing.expectEqual(null, result.?.ip_address);
    try testing.expectEqual(null, result.?.user_id);
    try testing.expectEqual(null, result.?.last_lsn);
    try testing.expectEqual(null, result.?.timestamp);
}

test "parsePgOutput maps COMMIT correctly" {
    const allocator = testing.allocator;

    const insert_hex = "43000000000001c160880000000001c160b80002f9a2afe34ece";
    const last_lsn: u64 = 29450424;
    const commit_timestamp: Io.Timestamp = Io.Timestamp.fromNanoseconds(1784111884349134000);

    const insert_bytes = try allocator.alloc(u8, insert_hex.len / 2);
    defer allocator.free(insert_bytes);
    _ = try std.fmt.hexToBytes(insert_bytes, insert_hex);

    var parser = PgOutput.init(allocator);
    defer parser.deinit();

    var result = try parser.decode(insert_bytes);
    defer {
        if (result) |*res| {
            res.deinit(allocator);
        }
    }

    try testing.expectEqual(true, result != null);
    try testing.expectEqual(null, result.?.xid);
    try testing.expectEqual(null, result.?.ip_address);
    try testing.expectEqual(null, result.?.user_id);
    try testing.expectEqual(null, result.?.data);
    try testing.expectEqual(last_lsn, result.?.last_lsn);
    try testing.expectEqual(commit_timestamp, result.?.timestamp);
}

// test "parseTupleData: correct parsing of input" {
//     const allocator = testing.allocator;
//
//     const commit_hex = "0006740000000131740000001031204170706c65205061726b205761796e740000000539353031347400000009437570657274696e6f74000000025553";
//
//     const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
//     defer allocator.free(commit_bytes);
//     _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);
//
//     var reader = Io.Reader.fixed(commit_bytes);
//
//     const columns = initContextColumns(allocator, parser.table_reg.get(16390).?);
//
//     var parser = PgOutput.init();
//     var result = try parser.parseTupleData(&reader, columns, .Insert);
//     defer {
//         for (result.items) |*col| { col.deinit(allocator); }
//         result.deinit(allocator);
//     }
//
//     try testing.expectEqualStrings("1", result.items[0].new_value.?);
//     try testing.expectEqualStrings("1 Apple Park Way", result.items[1].new_value.?);
//     try testing.expectEqual(null, result.items[2].new_value);
//     try testing.expectEqualStrings("95014", result.items[3].new_value.?);
//     try testing.expectEqualStrings("Cupertino", result.items[4].new_value.?);
//     try testing.expectEqualStrings("US", result.items[5].new_value.?);
//
//     try testing.expectEqual(0, parser.xid);
//     try testing.expectEqualStrings("", parser.ip_address);
//     try testing.expectEqualStrings("", parser.user_id);
// }

test "clear: ensure context correctly" {
    const allocator = testing.allocator;

    var parser = try setupParser(allocator);
    defer parser.deinit();

    const commit_hex = "43000000000001c160880000000001c160b80002f9a2afe34ece";

    const commit_bytes = try allocator.alloc(u8, commit_hex.len / 2);
    defer allocator.free(commit_bytes);
    _ = try std.fmt.hexToBytes(commit_bytes, commit_hex);

    var result = try parser.decode(commit_bytes);

    result.?.ip_address = try allocator.dupe(u8, "192.168.1.50");
    result.?.user_id = try allocator.dupe(u8, "42");
    result.?.xid = 791;

        if (result) |*res| {
            res.deinit(allocator);
        }

    try testing.expectEqual(true, result != null);
    try testing.expectEqual(null, result.?.data);

    try testing.expectEqual(0, result.?.xid);
    try testing.expectEqual(undefined, result.?.ip_address);
    try testing.expectEqual(undefined, result.?.user_id);
}

test "update from value to null" {
}
