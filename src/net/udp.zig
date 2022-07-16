const std = @import("std");
const os = std.os;
const log = std.log.scoped(.udp);
const testing = std.testing;

const ctx = @import("ctx.zig");
const Packet = @import("packet.zig").Packet;

pub const max_payload_size = @as(u32, 508);

fn connect(ip4address: []const u8, port: u16) !i32 {
    var addr: std.net.Address = try std.net.Address.parseIp4(ip4address, port);
    log.debug("Connecting to {s} on port {}", .{ ip4address, port });
    const flags = os.SOCK.DGRAM | os.SOCK.CLOEXEC | os.SOCK.NONBLOCK;
    var sockfd: i32 = try os.socket(os.AF.INET, flags, 0);

    try os.connect(sockfd, &addr.any, @sizeOf(os.sockaddr.in));
    return sockfd;
}

fn bind(ip4address: []const u8, port: u16) !i32 {
    var addr: std.net.Address = try std.net.Address.parseIp4(ip4address, port);
    const flags = os.SOCK.DGRAM | os.SOCK.CLOEXEC | os.SOCK.NONBLOCK;
    log.debug("Binding to {s} on port {}", .{ ip4address, port });
    var sockfd: i32 = try os.socket(os.AF.INET, flags, 0);

    //try os.connect(sockfd, &addr.any, @sizeOf(os.sockaddr.in));
    try os.bind(sockfd, &addr.any, @sizeOf(os.sockaddr.in));
    return sockfd;
}

pub fn startListener(addr: []const u8, port: u16, context: *ctx.ThreadContext) void {
    var sockfd: i32 = bind(addr, port) catch |err| {
        log.err("Couldn't bind to port {}, error was {}, giving up.", .{ port, err });
        return;
    };
    var src_addr: os.sockaddr align(4) = undefined;
    var length = @as(u32, @sizeOf(os.sockaddr));

    // reading buffer
    var array: [508]u8 = undefined;
    var buf: []u8 = &array;

    while (context.checkRunning()) {
        std.os.nanosleep(0, 100 * 1000 * 1000);
        const rlen = os.recvfrom(sockfd, buf, 0, &src_addr, &length) catch {
            continue;
        };

        if (rlen == 0) {
            continue;
        }

        if (context.b.isEmpty()) {
            log.warn("Out of recieving buffer space, dropping packet", .{});
            // no more pre-allocated buffers available, this packet will be dropped.
            continue;
        }

        // take a pre-allocated buffer
        var node = context.b.get().?;

        // copy the data
        std.mem.copy(u8, node.data.payload[0..rlen], buf[0..rlen]);
        std.mem.copy(u8, std.mem.asBytes(node.data.address), std.mem.asBytes(&src_addr));
        node.data.len = rlen;

        // send it for processing
        context.q.put(node);
    }
    os.close(sockfd);
    log.info("startListener: thread exiting.\n", .{});
}

pub fn startClient(addr: []const u8, port: u16, context: *ctx.ThreadContext) !void {
    var sockfd: i32 = connect(addr, port) catch |err| {
        log.err("Couldn't connect to port {} on {s}, error was {}, giving up.", .{ port, addr, err });
        return;
    };

    // sending buffer
    var array: [508]u8 = undefined;
    var buf: []u8 = &array;
    while (context.checkRunning()) {
        while (context.q.get()) |node| {
            std.mem.copy(u8, buf[0..node.data.len], node.data.payload[0..node.data.len]);
            _ = os.sendto(sockfd, buf[0..node.data.len], 0, null, 0) catch {
                continue;
            };
            context.b.put(node);
        }
    }
    os.close(sockfd);
    log.info("startClient: thread exiting.\n", .{});
}

test "connectivity" {
    // Setup
    const Queue = std.atomic.Queue;
    var send_queue = Queue(Packet).init();
    var send_buffers = Queue(Packet).init();
    var queue = Queue(Packet).init();
    var buffers = Queue(Packet).init();

    var index: usize = 0;

    while (index < 10) : (index += 1) {
        var pn: *Queue(Packet).Node = try testing.allocator.create(Queue(Packet).Node);
        pn.data = try Packet.init(testing.allocator, 508);
        buffers.put(pn);
        var spn: *Queue(Packet).Node = try testing.allocator.create(Queue(Packet).Node);
        spn.data = try Packet.init(testing.allocator, 508);
        send_buffers.put(spn);
    }
    var ltx = ctx.ThreadContext{ .q = queue, .b = buffers };
    var context = ctx.ThreadContext{ .q = send_queue, .b = send_buffers };
    var thread = try std.Thread.spawn(std.Thread.SpawnConfig{}, startClient, .{ "127.0.0.1", 9696, &context });
    var listener = try std.Thread.spawn(std.Thread.SpawnConfig{}, startListener, .{ "127.0.0.1", 9696, &ltx });

    ltx.setRunning(true);
    context.setRunning(true);

    // Send and receive some messages via localhost
    var x = @as(u32, 10);
    while (x > 0) : (x -= 1) {
        std.os.nanosleep(0, 100 * 1000 * 1000);
        while (!ltx.q.isEmpty()) {
            var node = ltx.q.get().?;
            try std.testing.expectEqualStrings("Hello world", node.data.payload[0..node.data.len]);
            ltx.b.put(node);
        }
        var send_buf = context.b.get().?;
        var buf: [11]u8 = "Hello world".*;
        std.mem.copy(u8, send_buf.data.payload[0..buf.len], buf[0..buf.len]);
        send_buf.data.len = buf.len;
        context.q.put(send_buf);
    }
    // Make sure the receive queue is empty;
    while (!ltx.q.isEmpty()) {
        var node = ltx.q.get().?;
        ltx.b.put(node);
    }

    // Tear it all down and clean up.
    ltx.setRunning(false);
    context.setRunning(false);
    thread.join();
    listener.join();
    while (!context.b.isEmpty()) {
        var node = context.b.get().?;
        node.data.deinit();
        testing.allocator.destroy(node);
    }
    while (!context.q.isEmpty()) {
        var node = context.q.get().?;
        node.data.deinit();
        testing.allocator.destroy(node);
    }
    while (!ltx.b.isEmpty()) {
        var node = ltx.b.get().?;
        node.data.deinit();
        testing.allocator.destroy(node);
    }
    while (!ltx.q.isEmpty()) {
        var node = ltx.q.get().?;
        node.data.deinit();
        testing.allocator.destroy(node);
    }
}
