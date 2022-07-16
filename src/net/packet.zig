const std = @import("std");
const os = @import("std").os;
const testing = @import("std").testing;

pub const Packet = struct {
    const Self = @This();
    allocator: std.mem.Allocator,
    payload: []u8,
    address: *os.sockaddr align(4),
    len: usize = 0,
    max_len: usize,

    pub fn init(allocator: std.mem.Allocator, max_len: usize) !Self {
        //var socket_ptr: *os.sockaddr align(4) = try allocator.create(os.sockaddr);
        return Self{
            .allocator = allocator,
            .payload = try allocator.alloc(u8, max_len),
            .address = try allocator.create(os.sockaddr),
            .len = 0,
            .max_len = max_len,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.destroy(self.address);
        self.allocator.free(self.payload);
    }

    pub fn setPayload(self: *Self, data: []u8) !void {
        if (data.len > self.max_len) {
            return error.PayloadTooLarge;
        }
        self.len = data.len;
        std.mem.copy(u8, data, self.payload[0..data.len]);
    }

    pub fn payloadToOwnedSlice(self: *Self) []u8 {
        var list = std.ArrayList(u8).init(self.allocator);
        list.appendSlice(self.payload[0..self.len]);
        return list.toOwnedSlice();
    }

    // TODO: Is this idiomatic zig?
    pub fn getAddress(self: *const Self) !std.net.Address {
        //const addr: *os.sockaddr align(4) = try self.allocator.create(os.sockaddr);
        //std.mem.copy(u8, std.mem.asBytes(addr), std.mem.asBytes(self.address));
        //const addr_copy: os.sockaddr align(4) = addr.*;
        //const a = std.net.Address.initPosix(@ptrCast(*align(4) const os.sockaddr, self.address));
        const a = std.net.Address.initPosix(@alignCast(4, self.address));
        return a;
    }
};

test "init, deinit don't leak" {
    var p = try Packet.init(testing.allocator, 508);
    p.deinit();
}

test "trying to set too large a payload fails" {
    var p = try Packet.init(testing.allocator, 100);
    defer p.deinit();
    var big_payload = [_]u8{'-'} ** 101;
    const payload: []u8 = &big_payload;
    try testing.expectError(error.PayloadTooLarge, p.setPayload(payload));
}
