const std = @import("std");
const sparseset = @import("./sparseset.zig");
const SparseSet = sparseset.SparseSet;
const testing = std.testing;

const Entity = usize;

fn Iterator(comptime T: type) type {
    return struct {
        const Self = @This();
        index: usize = 0,
        source: *SparseSet(T),

        fn next(self: *Self) ?T {
            self.index += 1;
            return self.source.get(self.index - 1);
        }
    };
}

fn ListContainer(comptime Container: type) type {
    const container = @typeInfo(Container);
    var struct_fields: [container.Struct.fields.len]std.builtin.TypeInfo.StructField = undefined;
    inline for (container.Struct.fields) |F, i| {
        var field_type = switch (@typeInfo(F.field_type)) {
            .Int => Entity,
            .Struct => *SparseSet(F.field_type),
            .Optional => |opt| *SparseSet(opt.child),
            else => {
                @compileError("Unexpected type for iterator");
            },
        };
        struct_fields[i] = .{
            .name = F.name,
            .field_type = field_type,
            .is_comptime = false,
            .default_value = null,
            .alignment = if (@sizeOf(field_type) > 0) @alignOf(field_type) else 0,
        };
    }
    return @Type(.{
        .Struct = .{
            .is_tuple = false,
            .layout = .Auto,
            .decls = &.{},
            .fields = &struct_fields,
        },
    });
}

fn StructuredIterator(comptime Container: type) type {
    const container = @typeInfo(Container);
    const Sources = ListContainer(Container);
    return struct {
        const Self = @This();
        index: usize = 0,
        minlen: usize,
        sources: Sources,

        fn next(self: *Self) ?Container {
            while (self.index < self.minlen) : (self.index += 1) {
                const entity = self.nextLargestEntity();
                if (self.entityInAllSources(entity)) {
                    self.index += 1;
                    return self.structuredContainerForEntity(entity);
                }
            } else {
                return null;
            }
        }

        fn nextLargestEntity(self: *Self) Entity {
            var largest_entity: Entity = 0;
            inline for (container.Struct.fields) |F| {
                switch (@typeInfo(F.field_type)) {
                    .Struct => {
                        var entity = @field(self.sources, F.name).storage.items[self.index].id;
                        if (entity > largest_entity) largest_entity = entity;
                    },
                    else => continue,
                }
            }
            return largest_entity;
        }

        fn entityInAllSources(self: *Self, entity: Entity) bool {
            inline for (container.Struct.fields) |F| {
                switch (@typeInfo(F.field_type)) {
                    .Struct => {
                        if (@field(self.sources, F.name).get(entity) == null) return false;
                    },
                    else => continue,
                }
            }
            return true;
        }

        fn structuredContainerForEntity(self: *Self, entity: Entity) Container {
            var cont: Container = undefined;
            inline for (container.Struct.fields) |F| {
                switch (@typeInfo(F.field_type)) {
                    .Int => {
                        @field(cont, F.name) = entity;
                    },
                    .Struct => {
                        @field(cont, F.name) = @field(self.sources, F.name).get(entity).?;
                    },
                    .Optional => {
                        @field(cont, F.name) = @field(self.sources, F.name).get(entity);
                    },
                    else => unreachable,
                }
            }
            return cont;
        }
    };
}

const Entities = struct {
    const Self = @This();
    opaque_map: std.StringHashMap(*anyopaque),
    free_list: std.ArrayList(Entity),
    allocator: std.mem.Allocator,
    max_entity: Entity = 0,

    pub fn init(allocator: std.mem.Allocator) !Self {
        return Self{
            .opaque_map = std.StringHashMap(*anyopaque).init(allocator),
            .free_list = std.ArrayList(Entity).init(allocator),
            .allocator = allocator,
        };
    }

    fn deinit(self: *Self) void {
        var iter = self.opaque_map.keyIterator();
        while (iter.next()) |key| {
            var ptr = self.opaque_map.get(key.*).?;
            // This feels like sneaking behind the back of the type system
            var aligned = @alignCast(@alignOf(*SparseSet(u8)), ptr);
            var list = @ptrCast(*SparseSet(u8), aligned);
            list.deinit();
            self.allocator.destroy(list);
        }
        self.free_list.deinit();
        self.opaque_map.deinit();
    }

    fn new(self: *Self) !Entity {
        if (self.free_list.items.len > 0) {
            return self.free_list.pop();
        } else {
            self.max_entity += 1;
            return self.max_entity - 1;
        }
    }

    fn rm(self: *Self, entity: Entity) !void {
        self.unsetAll(entity);
        var element = try self.free_list.addOne();
        element.* = entity;
    }

    fn set(self: *Self, entity: Entity, value: anytype) !void {
        const T = @TypeOf(value);
        const type_name = @typeName(T);
        var list: *SparseSet(T) = undefined;
        if (self.opaque_map.contains(type_name)) {
            list = try self.getSparseSet(T);
        } else {
            list = try self.createSparseSet(T);
        }
        try list.set(entity, value);
    }

    fn unset(self: *Self, entity: Entity, comptime T: type) !void {
        const type_name = @typeName(T);
        var list: *SparseSet(T) = undefined;
        if (self.opaque_map.contains(type_name)) {
            list = try self.getSparseSet(T);
            try list.clear(entity);
        }
    }

    fn unsetAll(self: *Self, entity: Entity) void {
        var iter = self.opaque_map.keyIterator();
        while (iter.next()) |key| {
            var ptr = self.opaque_map.get(key.*).?;
            // This feels like sneaking behind the back of the type system
            var aligned = @alignCast(@alignOf(*SparseSet(u8)), ptr);
            var list = @ptrCast(*SparseSet(u8), aligned);
            list.clear(entity);
        }
    }

    fn getSparseSet(self: *Self, comptime T: type) !*SparseSet(T) {
        var ptr = self.opaque_map.get(@typeName(T)).?;
        var aligned = @alignCast(@alignOf(*SparseSet(T)), ptr);
        return @ptrCast(*SparseSet(T), aligned);
    }

    fn createSparseSet(self: *Self, comptime T: type) !*SparseSet(T) {
        var ptr = try self.allocator.create(SparseSet(T));
        ptr.* = SparseSet(T).init(self.allocator);
        try self.opaque_map.put(@typeName(T), ptr);
        return self.getSparseSet(T);
    }

    fn iterator(self: *Self, comptime T: type) Iterator(T) {
        const source = try self.getSparseSet(T);
        return Iterator(T){ .source = source };
    }

    fn structured_iterator(self: *Self, comptime T: type) StructuredIterator(T) {
        const Sources = ListContainer(T);
        var minlen: usize = std.math.maxInt(usize);
        var maxlen: usize = 0;
        var sources: Sources = undefined;
        const container = @typeInfo(T);
        inline for (container.Struct.fields) |F| {
            switch (@typeInfo(F.field_type)) {
                .Int => {
                    continue;
                },
                .Struct => {
                    var source = try self.getSparseSet(F.field_type);
                    if (source.len() < minlen) minlen = source.len();
                    @field(sources, F.name) = source;
                },
                .Optional => |opt| {
                    std.debug.assert(@typeInfo(opt.child) == .Struct);
                    var source = try self.getSparseSet(opt.child);
                    if (source.max() > maxlen) maxlen = source.max();
                    @field(sources, F.name) = source;
                },
                else => {},
            }
        }

        return StructuredIterator(T){
            .minlen = if (minlen == std.math.maxInt(usize)) maxlen else minlen,
            .sources = sources,
        };
    }
};

test "basic test" {
    var entities = try Entities.init(std.testing.allocator);
    defer entities.deinit();
    var entity = try entities.new();
    try std.testing.expectEqual(@as(Entity, 0), entity);
}

test "setting and unsetting a component" {
    const Position = struct {
        x: f16 = 0,
        y: f16 = 0,
    };

    var entities = try Entities.init(std.testing.allocator);
    defer entities.deinit();
    var entity = try entities.new();
    try entities.set(entity, Position{ .x = 5.0, .y = 10.0 });
    var source = try entities.getSparseSet(Position);
    std.debug.print("{s}\n", .{source.storage.items});
    try std.testing.expectEqual(@as(usize, 1), source.len());
    try entities.unset(entity, Position);
    try std.testing.expectEqual(@as(usize, 0), source.len());
}

test "simple component iterator" {
    const Position = struct {
        x: f16 = 0,
        y: f16 = 0,
    };

    var entities = try Entities.init(std.testing.allocator);
    defer entities.deinit();
    var entity = try entities.new();
    try entities.set(entity, Position{ .x = 5.0, .y = 10.0 });
    var iterator = entities.iterator(Position);
    var position = iterator.next().?;
    try std.testing.expectEqual(Position{ .x = 5.0, .y = 10.0 }, position);
    std.debug.assert(iterator.next() == null);
}

test "custom component iterator" {
    const Position = struct {
        x: f16 = 0,
        y: f16 = 0,
    };

    const Velocity = struct {
        x: f16 = 0,
        y: f16 = 0,
    };

    var entities = try Entities.init(std.testing.allocator);
    defer entities.deinit();
    var entone = try entities.new();
    try entities.set(entone, Position{ .x = 5.0, .y = 10.0 });
    try entities.set(entone, Velocity{});
    var enttwo = try entities.new();
    try entities.set(enttwo, Position{ .x = 512.0, .y = 700.0 });
    try entities.set(enttwo, Velocity{ .x = 11.0, .y = 50.5 });

    const View = struct {
        position: Position,
        velocity: Velocity,
    };
    var mit = entities.structured_iterator(View);
    var first = mit.next().?;
    try std.testing.expectEqual(first.position, Position{ .x = 5.0, .y = 10.0 });
    try std.testing.expectEqual(first.velocity, Velocity{ .x = 0, .y = 0 });
    var second = mit.next().?;
    try std.testing.expectEqual(second.position, Position{ .x = 512.0, .y = 700.0 });
    try std.testing.expectEqual(second.velocity, Velocity{ .x = 11.0, .y = 50.5 });
    var end = mit.next();
    try std.testing.expectEqual(end, null);
}

test "custom component iterator with entity" {
    const Position = struct {
        x: f16 = 0,
        y: f16 = 0,
    };

    var entities = try Entities.init(std.testing.allocator);
    defer entities.deinit();
    var entone = try entities.new();
    try entities.set(entone, Position{ .x = 5.0, .y = 10.0 });
    var enttwo = try entities.new();
    try entities.set(enttwo, Position{ .x = 512.0, .y = 700.0 });

    const View = struct {
        entity: Entity,
        position: Position,
    };
    var mit = entities.structured_iterator(View);
    var first = mit.next().?;
    try std.testing.expectEqual(first.entity, @as(Entity, 0));
    try std.testing.expectEqual(first.position, Position{ .x = 5.0, .y = 10.0 });
    var second = mit.next().?;
    try std.testing.expectEqual(second.entity, @as(Entity, 1));
    try std.testing.expectEqual(second.position, Position{ .x = 512.0, .y = 700.0 });
    var end = mit.next();
    try std.testing.expectEqual(end, null);
}

test "custom component iterator with optional fields" {
    const Position = struct {
        x: f16 = 0,
        y: f16 = 0,
    };

    const Velocity = struct {
        x: f16,
        y: f16,
    };

    var entities = try Entities.init(std.testing.allocator);
    defer entities.deinit();
    var entone = try entities.new();
    try entities.set(entone, Position{ .x = 5.0, .y = 10.0 });
    try entities.set(entone, Velocity{ .x = 1.0, .y = 1.5 });
    var enttwo = try entities.new();
    try entities.set(enttwo, Position{ .x = 512.0, .y = 700.0 });
    var entthree = try entities.new();
    try entities.set(entthree, Velocity{ .x = 15.0, .y = 20.5 });
    var entfour = try entities.new();
    try entities.set(entfour, Position{ .x = 100.0, .y = 200.0 });

    const View = struct {
        position: Position,
        velocity: ?Velocity,
    };
    var mit = entities.structured_iterator(View);
    var entry = mit.next().?;
    try std.testing.expectEqual(entry.position, Position{ .x = 5.0, .y = 10.0 });
    try std.testing.expectEqual(entry.velocity, Velocity{ .x = 1.0, .y = 1.5 });
    entry = mit.next().?;
    try std.testing.expectEqual(entry.position, Position{ .x = 512.0, .y = 700.0 });
    try std.testing.expectEqual(entry.velocity, null);
    // Third entry ("entthree") should be skipped because it doesn't have a position
    // and position is required.
    entry = mit.next().?;
    try std.testing.expectEqual(entry.position, Position{ .x = 100.0, .y = 200.0 });
    try std.testing.expectEqual(entry.velocity, null);
    var end = mit.next();
    try std.testing.expectEqual(end, null);
}

test "simple iteration speed for 100k entries" {
    const Position = struct {
        x: f32 = 0,
        y: f32 = 0,
    };

    var entities = try Entities.init(std.testing.allocator);
    var prng = std.rand.DefaultPrng.init(0);

    defer entities.deinit();

    var i: usize = 0;
    while (i < 100000) : (i += 1) {
        var e = try entities.new();
        try entities.set(e, Position{ .x = prng.random().float(f32), .y = prng.random().float(f32) });
    }
    const start = std.time.nanoTimestamp();
    var it = entities.iterator(Position);
    var x_tot: f64 = 0.0;
    var y_tot: f64 = 0.0;
    while (it.next()) |p| {
        x_tot += p.x;
        y_tot += p.y;
    }
    const end = std.time.nanoTimestamp() - start;
    std.debug.print("\n X Total: {}\t", .{x_tot});
    std.debug.print("Y Total: {}\n", .{y_tot});
    std.debug.print("\n\nSimple iterator 100,000 in {} ns\n\n", .{end});
    try std.testing.expect(end < 1500000);
}

test "structured iteration speed for 100k entries" {
    const Position = struct {
        x: f32 = 0,
        y: f32 = 0,
    };
    const Velocity = struct {
        x: f32 = 0,
        y: f32 = 0,
    };
    const View = struct {
        position: Position,
        velocity: ?Velocity,
    };

    var entities = try Entities.init(std.testing.allocator);
    var prng = std.rand.DefaultPrng.init(0);

    defer entities.deinit();

    var i: usize = 0;
    while (i < 100000) : (i += 1) {
        var e = try entities.new();
        try entities.set(e, Position{ .x = prng.random().float(f32), .y = prng.random().float(f32) });
        try entities.set(e, Velocity{ .x = prng.random().float(f32), .y = prng.random().float(f32) });
    }
    const start = std.time.nanoTimestamp();
    var it = entities.structured_iterator(View);
    var x_tot: f64 = 0.0;
    var y_tot: f64 = 0.0;
    while (it.next()) |d| {
        x_tot += d.position.x;
        y_tot += d.position.y;
    }
    const end = std.time.nanoTimestamp() - start;
    std.debug.print("\n X Total: {}\t", .{x_tot});
    std.debug.print("Y Total: {}\n", .{y_tot});
    std.debug.print("\n\nStructured iterator 100,000 in {} ns\n\n", .{end});
    try std.testing.expect(end < 1500000);
}
