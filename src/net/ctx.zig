const std = @import("std");
const Queue = std.atomic.Queue;
const Packet = @import("packet.zig").Packet;

pub const ThreadContext = struct {
    const Self = @This();
    // packets sent from client waiting to be sent over the network
    q: std.atomic.Queue(Packet),
    // empty buffers ready to accept client data
    b: std.atomic.Queue(Packet),
    // A mutex to protect the running state
    mutex: std.Thread.Mutex = std.Thread.Mutex{},
    // You should use the helper methods to check the value of _running, for thread safety.
    _running: bool = false,

    pub fn checkRunning(self: *Self) bool {
        self.mutex.lock();
        defer self.mutex.unlock();
        return self._running;
    }

    pub fn setRunning(self: *Self, state: bool) void {
        self.mutex.lock();
        defer self.mutex.unlock();
        self._running = state;
    }
};
