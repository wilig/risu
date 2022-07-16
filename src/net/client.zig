const std = @import("std");
const Queue = std.atomic.Queue;

const ctx = @import("ctx.zig");
const Packet = @import("packet.zig").Packet;

pub fn Client(comptime Transport: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        transport: Transport,
        context: ctx.ThreadContext,
        worker: std.Thread,
        host: []const u8,
        port: u16,

        pub fn init(allocator: std.mem.Allocator, host: []const u8, port: u16) !Self {
            var queue = Queue(Packet).init();

            // pre-alloc 4096 packets that will be re-used to contain any data to
            // be sent.  These packets will do round-trips between the client and
            // the transport layer.
            var buffers = Queue(Packet).init();
            var i: usize = 0;

            // TODO: Revist the number of packets initialized here after we gain
            // some experience.
            while (i < 4096) : (i += 1) {
                var packet_node: *Queue(Packet).Node = try allocator.create(Queue(Packet).Node);
                packet_node.data = try Packet.init(allocator, Transport.max_payload_size);
                buffers.put(packet_node);
            }
            return Self{
                .allocator = allocator,
                .transport = Transport{},
                .context = ctx.ThreadContext{ .q = queue, .b = buffers },
                .worker = undefined,
                .host = host,
                .port = port,
            };
        }

        pub fn deinit(self: *Self) void {
            // Release all memory
            while (!self.context.q.isEmpty()) {
                var node = self.context.q.get().?;
                node.data.deinit();
                self.allocator.destroy(node);
            }
            while (!self.context.b.isEmpty()) {
                var node = self.context.b.get().?;
                node.data.deinit();
                self.allocator.destroy(node);
            }
        }

        pub fn start(self: *Self) !void {
            self.context.setRunning(true);
            self.worker = try std.Thread.spawn(std.Thread.SpawnConfig{}, Transport.startClient, .{ self.host, self.port, &self.context });
        }

        pub fn stop(self: *Self) void {
            if (self.context.checkRunning()) {
                self.context.setRunning(false);
                self.worker.join();
            } else {
                std.log.warn("Stop called on non-running client\n", .{});
            }
        }

        pub fn send(self: *Self, buf: []const u8) !void {
            if (!self.context.checkRunning()) {
                return error.ClientNotStarted;
            }
            const iterations = @floatToInt(usize, std.math.ceil(@intToFloat(f64, buf.len) / @intToFloat(f64, Transport.max_payload_size)));
            var i: usize = 0;
            while (i < iterations) : (i += 1) {
                const mp = Transport.max_payload_size;
                const offset = i * mp;
                const payload_size = if (buf.len > offset + mp) mp else buf.len - offset;
                const send_buf = self.context.b.get().?;
                send_buf.data.len = payload_size;
                std.mem.copy(u8, send_buf.data.payload[0..payload_size], buf[offset .. offset + payload_size]);
                self.context.q.put(send_buf);
            }
        }
    };
}

test "client basic send" {
    const Udp = @import("udp.zig");
    var client = try Client(Udp).init(std.testing.allocator, "127.0.0.1", 9696);
    defer client.deinit();
    try client.start();
    try client.send("hello world");
    client.stop();
}

test "client throws errors if send called before the client is started" {
    const Udp = @import("udp.zig");
    var client = try Client(Udp).init(std.testing.allocator, "127.0.0.1", 9696);
    defer client.deinit();
    try std.testing.expectError(error.ClientNotStarted, client.send("hello world"));
}
