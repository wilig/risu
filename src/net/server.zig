const std = @import("std");
const Queue = std.atomic.Queue;
const log = std.log.scoped(.server);

const ctx = @import("ctx.zig");
const Packet = @import("packet.zig").Packet;

const testing = @import("std").testing;

pub fn Server(comptime Transport: type) type {
    return struct {
        const Self = @This();
        allocator: std.mem.Allocator,
        transport: Transport,
        context: ctx.ThreadContext,
        worker: std.Thread,
        host: []const u8,
        port: u16,

        pub fn init(allocator: std.mem.Allocator, host: []const u8, port: u16) !Self {
            // queue communicating packets to parse
            var queue = Queue(Packet).init();

            // pre-alloc 4096 packets that will be re-used to contain any data to
            // be sent.  These packets will do round-trips between the client and
            // the transport layer.
            var buffers = Queue(Packet).init();
            var i: usize = 0;

            // TODO: This number seems good enough for now, but may want to revisit it.
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
            log.info("Server: starting worker thread.\n", .{});
            self.worker = try std.Thread.spawn(std.Thread.SpawnConfig{}, Transport.startListener, .{ self.host, self.port, &self.context });
        }

        pub fn stop(self: *Self) void {
            if (self.context.checkRunning()) {
                self.context.setRunning(false);
                self.worker.join();
            } else {
                std.log.warn("Stop called on non-running client\n", .{});
            }
        }

        pub fn receiveToOwnedSlice(self: *Self) []Packet {
            var buffer = std.ArrayList(Packet).init(self.allocator);
            defer buffer.deinit();
            while (!self.context.q.isEmpty()) {
                var node = self.context.q.get().?;
                buffer.append(node.data) catch |err| {
                    log.err("Failed to append to slice, data lost, error was {}", .{err});
                };
                self.context.b.put(node);
            }
            return buffer.toOwnedSlice();
        }
    };
}

test "server send and receive" {
    const num_of_packets = @as(usize, 5);
    const Udp = @import("udp.zig");
    const Client = @import("client.zig").Client;
    var client = try Client(Udp).init(std.testing.allocator, "127.0.0.1", 9696);
    defer client.deinit();
    var server = try Server(Udp).init(std.testing.allocator, "127.0.0.1", 9696);
    defer server.deinit();

    try server.start();
    try client.start();

    var i = num_of_packets;
    while (i > 0) : (i -= 1) {
        try client.send("Hello world!!");
    }

    while (!client.context.q.isEmpty()) {
        std.time.sleep(1000 * 1000);
    }
    std.time.sleep(1000 * 1000 * 1000);
    var packets = server.receiveToOwnedSlice();
    defer (std.testing.allocator.free(packets));
    server.stop();
    try std.testing.expectEqual(num_of_packets, packets.len);
    for (packets) |pkt| {
        try std.testing.expectEqual(@as(usize, 13), pkt.len);
        try std.testing.expectEqualStrings("Hello world!!", pkt.payload[0..pkt.len]);
    }
    client.stop();
}
