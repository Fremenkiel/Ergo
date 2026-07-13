const std = @import("std");

pub const ClickHouseType = enum {
    UInt64,
    String,
    DateTime64,
    Array,
    Enum8,
    Map,
    LowCardinality,
    IPv4,
    
    pub fn fromStr(type_str: []const u8) !ClickHouseType {
        if (std.mem.eql(u8, type_str, "UInt64")) return .UInt64;
        if (std.mem.eql(u8, type_str, "String")) return .String;
        if (std.mem.eql(u8, type_str, "DateTime64")) return .DateTime64;
        if (std.mem.eql(u8, type_str, "IPv4")) return .IPv4;

        if (std.mem.startsWith(u8, type_str, "Array")) return .Array;
        if (std.mem.startsWith(u8, type_str, "Enum8")) return .Enum8;
        if (std.mem.startsWith(u8, type_str, "Map")) return .Map;
        if (std.mem.startsWith(u8, type_str, "LowCardinality")) return .LowCardinality;
        return error.UnsupportedType;
    }

    pub fn isDecimal(self: ClickHouseType) bool {
        return switch (self) {
            .Decimal, .Decimal32, .Decimal64, .Decimal128 => true,
            else => false,
        };
    }

    pub fn isEnum(self: ClickHouseType) bool {
        return switch (self) {
            .Enum8, .Enum16 => true,
            else => false,
        };
    }

    pub fn isComplex(self: ClickHouseType) bool {
        return switch (self) {
            .Array, .Nullable, .Map, .Tuple, .LowCardinality, .Nested => true,
            else => false,
        };
    }
};

pub const TypeInfo = struct {
    base_type: ClickHouseType,
    precision: ?u8 = null,
    enum_values: ?[]const u8 = null,
    key_type: ?* TypeInfo = null,
    value_type: ?* TypeInfo = null,

    pub const EnumValue = struct {
        name: []const u8,
        value: i8,
    };

    pub const NestedField = struct {
        name: []const u8,
        type_info: TypeInfo,
    };

    pub fn parse(allocator: std.mem.Allocator, type_str: []const u8) !TypeInfo {
        var result = TypeInfo{
            .base_type = undefined,
        };

        if (std.mem.startsWith(u8, type_str, "Array(")) {
            const inner_type = type_str[6 .. type_str.len - 1];
            const inner_info = try parse(allocator, inner_type);
            result.base_type = .Array;
            result.key_type = try allocator.create(TypeInfo);
            result.key_type.?.* = inner_info;
            return result;
        }

        if (std.mem.startsWith(u8, type_str, "LowCardinality(")) {
            const inner_type = type_str[15 .. type_str.len - 1];
            const inner_info = try parse(allocator, inner_type);
            result.base_type = .LowCardinality;
            result.key_type = try allocator.create(TypeInfo);
            result.key_type.?.* = inner_info;
            return result;
        }

        if (std.mem.startsWith(u8, type_str, "Enum8(")) {
            const inner_dec = type_str[6 .. type_str.len - 1];
            result.enum_values = inner_dec;
            result.base_type = .Enum8;

            return result;
        }

        if (std.mem.startsWith(u8, type_str, "Map(")) {
            result.base_type = .Map;
            const map_types = type_str[4 .. type_str.len - 1];
            var it = std.mem.splitAny(u8, map_types, ",");
            const key_type_str = it.next() orelse return error.InvalidMapType;
            const value_type_str = it.next() orelse return error.InvalidMapType;
            
            result.key_type = try allocator.create(TypeInfo);
            result.value_type = try allocator.create(TypeInfo);
            result.key_type.?.* = try parse(allocator, std.mem.trim(u8, key_type_str, " "));
            result.value_type.?.* = try parse(allocator, std.mem.trim(u8, value_type_str, " "));
            return result;
        }

        result.base_type = try ClickHouseType.fromStr(type_str);
        return result;
    }

    pub fn deinit(self: *TypeInfo, allocator: std.mem.Allocator) void {
        if (self.map_key_type) |key_type| {
            key_type.deinit(allocator);
            allocator.destroy(key_type);
        }
        if (self.map_value_type) |value_type| {
            value_type.deinit(allocator);
            allocator.destroy(value_type);
        }
        if (self.low_cardinality_type) |lc_type| {
            lc_type.deinit(allocator);
            allocator.destroy(lc_type);
        }
        if (self.nested_types) |nested| {
            for (nested) |*field| {
                allocator.free(field.name);
                field.type_info.deinit(allocator);
            }
            allocator.free(nested);
        }
    }
};
