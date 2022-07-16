const std = @import("std");
const testing = std.testing;

const MessengerError = error{
    UnregisteredEvent,
    DuplicateRegisteration,
};

const Messenger = struct {
    const Self = Messenger;
    allocator: std.mem.Allocator,
    clearingHouse: std.StringHashMap(*anyopaque),

    fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
            .clearingHouse = std.StringHashMap(*anyopaque).init(allocator),
        };
    }

    fn deinit(self: *Self) void {
        var iter = self.clearingHouse.keyIterator();
        while (iter.next()) |key| {
            var ptr = self.clearingHouse.get(key.*).?;
            // This feels like sneaking behind the back of the type system
            var aligned = @alignCast(@alignOf(*std.ArrayList(fn (u8) void)), ptr);
            var list = @ptrCast(*std.ArrayList(fn (u8) void), aligned);
            list.deinit();
            self.allocator.destroy(list);
        }
        self.clearingHouse.deinit();
    }

    fn register(self: *Self, comptime name: []const u8, comptime T: type) !void {
        const key = stringKey(name, T);
        if (self.clearingHouse.get(key)) |_| {
            return MessengerError.DuplicateRegisteration;
        }
        var ptr = try self.allocator.create(std.ArrayList(fn (T) void));
        ptr.* = std.ArrayList(fn (T) void).init(self.allocator);
        try self.clearingHouse.put(key, ptr);
    }

    fn unregister(self: *Self, comptime name: []const u8, comptime T: type) void {
        const key = stringKey(name, T);
        if (self.clearingHouse.get(key)) |ptr| {
            var aligned = @alignCast(@alignOf(*std.ArrayList(fn (T) void)), ptr);
            var listenerList = @ptrCast(*std.ArrayList(fn (T) void), aligned);
            listenerList.deinit();
            self.allocator.destroy(listenerList);
            _ = self.clearingHouse.remove(key);
        }
    }

    fn publish(self: *Self, comptime name: []const u8, value: anytype) !void {
        const key = stringKey(name, @TypeOf(value));
        if (self.clearingHouse.get(key)) |ptr| {
            var aligned = @alignCast(@alignOf(*std.ArrayList(fn (@TypeOf(value)) void)), ptr);
            var listenerList = @ptrCast(*std.ArrayList(fn (@TypeOf(value)) void), aligned);
            for (listenerList.items) |listener| {
                listener(value);
            }
        } else {
            return MessengerError.UnregisteredEvent;
        }
    }

    fn subscribe(self: *Self, comptime name: []const u8, comptime T: type, comptime listener: fn (T) void) !void {
        const key = stringKey(name, T);
        if (self.clearingHouse.get(key)) |ptr| {
            var aligned = @alignCast(@alignOf(*std.ArrayList(fn (T) void)), ptr);
            const listenerList = @ptrCast(*std.ArrayList(fn (T) void), aligned);
            try listenerList.append(listener);
        } else {
            return MessengerError.UnregisteredEvent;
        }
    }

    fn unsubscribe(self: *Self, comptime name: []const u8, comptime T: type, comptime listener: fn (T) void) void {
        const key = stringKey(name, T);
        if (self.clearingHouse.get(key)) |ptr| {
            var aligned = @alignCast(@alignOf(*std.ArrayList(fn (T) void)), ptr);
            const listenerList = @ptrCast(*std.ArrayList(fn (T) void), aligned);
            for (listenerList.items) |l, i| {
                if (l == listener) {
                    _ = listenerList.swapRemove(i);
                    break;
                }
            }
        }
    }

    fn stringKey(comptime name: []const u8, comptime T: type) []const u8 {
        return name ++ "-" ++ @typeName(T);
    }
};

fn testListener(data: usize) void {
    std.debug.print("\nReceived event: {}\n", .{data});
}

fn failListener(data: usize) void {
    std.debug.print("\nError, I received event: {}\n", .{data});
    std.debug.panic("Failed", .{});
}

test "basic functionality" {
    var m = Messenger.init(std.testing.allocator);
    defer m.deinit();
    try m.register("testing", usize);

    try m.subscribe("testing", usize, testListener);

    try m.publish("testing", @as(usize, 1000));
}

test "prevents duplicate registration" {
    var m = Messenger.init(std.testing.allocator);
    defer m.deinit();
    try m.register("testing", usize);
    try std.testing.expectError(MessengerError.DuplicateRegisteration, m.register("testing", usize));
}

test "prevents publishing unregistered event" {
    var m = Messenger.init(std.testing.allocator);
    defer m.deinit();
    try std.testing.expectError(MessengerError.UnregisteredEvent, m.publish("unregistered", @as(u8, 10)));
}

test "prevents listening for unregisted event" {
    var m = Messenger.init(std.testing.allocator);
    defer m.deinit();
    try std.testing.expectError(MessengerError.UnregisteredEvent, m.subscribe("unregistered", usize, testListener));
}

test "unregister throws away listener list" {
    var m = Messenger.init(std.testing.allocator);
    defer m.deinit();
    try m.register("testing", usize);
    try m.subscribe("testing", usize, testListener);
    m.unregister("testing", usize);
    try std.testing.expectError(MessengerError.UnregisteredEvent, m.publish("testing", @as(usize, 10)));
}

test "removing a listener from an event" {
    var m = Messenger.init(std.testing.allocator);
    defer m.deinit();
    try m.register("testing", usize);
    try m.subscribe("testing", usize, failListener);
    try m.subscribe("testing", usize, testListener);
    m.unsubscribe("testing", usize, failListener);
    try m.publish("testing", @as(usize, 1000));
}
