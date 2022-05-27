const std = @import("std");
const testing = std.testing;

const Sentinel = std.math.maxInt(usize);

pub fn SparseSet(comptime T: type) type {
    const indexedT = struct { id: usize, value: T };
    return struct {
        const Self = @This();

        storage: std.ArrayList(indexedT),
        indexes: std.ArrayList(usize),

        pub fn init(allocator: std.mem.Allocator) Self {
            return Self{
                .storage = std.ArrayList(indexedT).init(allocator),
                .indexes = std.ArrayList(usize).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            self.storage.deinit();
            self.indexes.deinit();
        }

        pub fn set(self: *Self, id: usize, value: T) !void {
            var slot: usize = 0;
            var entry = indexedT{ .id = id, .value = value };
            if (self.storage.items.len == 0) {
                try self.storage.append(entry);
            } else {
                slot = self.locate_slot(id, 0, self.storage.items.len - 1);
                if (self.storage.items.len > id and self.storage.items[slot].id == id) { // Replace value
                    self.storage.items[slot] = .{ .id = id, .value = value };
                } else { // Insert value at slot position
                    try self.storage.insert(slot, .{ .id = id, .value = value });
                }
            }
            if (id >= self.indexes.items.len) {
                try self.indexes.appendNTimes(Sentinel, id + 1 - self.indexes.items.len);
            }
            self.indexes.items[id] = slot;
        }

        pub fn clear(self: *Self, index: usize) !void {
            var storage_idx = self.indexes.items[index];
            _ = self.storage.swapRemove(storage_idx);
            self.indexes.items[index] = Sentinel;
        }

        pub fn get(self: *Self, index: usize) ?T {
            if (self.indexes.items.len > index) {
                var storage_idx = self.indexes.items[index];
                return if (storage_idx != Sentinel) self.storage.items[storage_idx].value else null;
            } else {
                return null;
            }
        }

        pub fn len(self: *Self) usize {
            return self.storage.items.len;
        }

        pub fn max(self: *Self) usize {
            return self.storage.items[self.storage.items.len - 1].id;
        }

        fn locate_slot(self: *Self, value: usize, begin: usize, end: usize) usize {
            if (self.storage.items[begin].id >= value) return begin;
            if (self.storage.items[end].id < value) return end + 1;
            var middle = @floatToInt(usize, (std.math.round(@intToFloat(f64, begin) + @intToFloat(f64, end)) / 2.0));
            if (value <= self.storage.items[middle].id) {
                return self.locate_slot(value, begin + 1, middle);
            } else {
                return self.locate_slot(value, middle + 1, end);
            }
        }
    };
}

test "init and deinit" {
    const U8SparseSet = SparseSet(u8);
    var ss = U8SparseSet.init(std.testing.allocator);
    ss.deinit();
}

test "simple set/get/clear" {
    const U8SparseSet = SparseSet(u8);
    var ss = U8SparseSet.init(std.testing.allocator);
    defer ss.deinit();
    try ss.set(100, 65);
    try std.testing.expectEqual(@as(u8, 65), ss.get(100).?);
    try ss.clear(100);
    var clearedValue = ss.get(100) orelse @as(u8, 99);
    try std.testing.expectEqual(@as(u8, 99), clearedValue);
}

test "test overwrite" {
    const U8SparseSet = SparseSet(u8);
    var ss = U8SparseSet.init(std.testing.allocator);
    defer ss.deinit();
    try ss.set(100, 65);
    try ss.set(100, 75);
    try std.testing.expectEqual(@as(u8, 75), ss.get(100).?);
}

test "storage sorts entries based on index" {
    const U8SparseSet = SparseSet(u8);
    var ss = U8SparseSet.init(std.testing.allocator);
    defer ss.deinit();
    try ss.set(0, 65);
    try ss.set(5, 66);
    try ss.set(3, 67);
    try ss.set(7, 68);
    try ss.set(10, 69);
    try ss.set(8, 70);
    try ss.set(6, 71);
    try ss.set(1, 72);
    try ss.set(9, 73);
    try ss.set(2, 74);
    try ss.set(4, 75);
    try std.testing.expectEqual(@as(u8, 65), ss.get(0).?);
    try std.testing.expectEqual(@as(usize, 0), ss.storage.items[0].id);
    try std.testing.expectEqual(@as(usize, 1), ss.storage.items[1].id);
    try std.testing.expectEqual(@as(usize, 2), ss.storage.items[2].id);
    try std.testing.expectEqual(@as(usize, 3), ss.storage.items[3].id);
    try std.testing.expectEqual(@as(usize, 4), ss.storage.items[4].id);
    try std.testing.expectEqual(@as(usize, 5), ss.storage.items[5].id);
    try std.testing.expectEqual(@as(usize, 6), ss.storage.items[6].id);
    try std.testing.expectEqual(@as(usize, 7), ss.storage.items[7].id);
    try std.testing.expectEqual(@as(usize, 8), ss.storage.items[8].id);
    try std.testing.expectEqual(@as(usize, 9), ss.storage.items[9].id);
    try std.testing.expectEqual(@as(usize, 10), ss.storage.items[10].id);
}
