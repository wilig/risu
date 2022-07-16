const std = @import("std");
const os = std.os;
const io = std.io;
const fs = std.fs;

const FileMagic = "RISU";
const TocMagic = "TOC!";

const TocEntry = struct {
    offset: u64,
    len: u64,
    timestamp: i64,
};

const Package = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    entries: std.StringHashMap(TocEntry),
    file: fs.File,
    toc_offset: u64 = 0,
    reserved: [4]u64 = [4]u64{ 0, 0, 0, 0 },

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Self {
        var file: fs.File = undefined;
        if (fs.openFileAbsolute(path, fs.File.OpenFlags{ .read = true, .write = true })) |f| {
            file = f;
        } else |err| {
            std.debug.print("{}", .{err});
            file = try createEmptyPackage(path);
        }

        return Self{
            .allocator = allocator,
            .file = file,
            .entries = std.StringHashMap(TocEntry).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        self.file.close();
        self.entries.deinit();
    }

    fn offsetToData(self: *Self) u64 {
        return FileMagic.len +
            @sizeOf(@TypeOf(self.toc_offset)) +
            @sizeOf(@TypeOf(self.reserved));
    }

    pub fn add(self: *Self, path: []const u8) !void {
        var f = std.fs.openFileAbsolute(path, std.fs.File.OpenFlags{}) catch |err| {
            std.debug.print("Couldn't open file {s} error was {}\n", .{ path, err });
            return err;
        };
        var stats = f.stat() catch |err| {
            std.debug.print("Couldn't stat file {s} error was {}\n", .{ path, err });
            return err;
        };
        const buffer = self.allocator.alloc(u8, @as(usize, stats.size)) catch |err| {
            std.debug.print("Couldn't allocate enough space to store file data, error was {}\n", .{err});
            return err;
        };
        defer self.allocator.free(buffer);
        var bytes_read = f.readAll(buffer) catch |err| {
            std.debug.print("Failed to read file data into buffer, error was {}\n", .{err});
            return err;
        };
        if (bytes_read != stats.size) {
            std.debug.print("File size mismatch!  Read {} of {} bytes.\n", .{ bytes_read, stats.size });
            return error.ReadMismatch;
        }
        try self.file.seekTo(self.toc_offset);
        self.file.writeAll(buffer) catch |err| {
            std.debug.print("Couldn't write {s} to package, error was {}.\n", .{ path, err });
            return err;
        };
        var toc_entry = TocEntry{ .offset = self.toc_offset, .len = bytes_read, .timestamp = std.time.milliTimestamp() };
        self.entries.put(path, toc_entry) catch |err| {
            std.debug.print("Failed to store file data in HashMap, error was {}\n", .{err});
            return err;
        };
        self.toc_offset += bytes_read;
        try self.writeTableOfContents();
    }

    pub fn getStream(self: *Self, path: []const u8) !io.FixedBufferStream([]u8) {
        if (!self.entries.contains(path)) {
            std.debug.print("{s} not found in package\n", .{path});
            return error.NotFound;
        }
        return io.fixedBufferStream(self.entries.get(path).?);
    }

    fn createEmptyPackage(path: []const u8) !std.fs.File {
        var file = try fs.createFileAbsolute(path, .{ .exclusive = true, .lock = .Exclusive, .truncate = false });
        var buffer: [8]u8 = undefined;
        const reserved_space = [_]u8{'R'} ** 32;
        var empty_offset = FileMagic.len + @sizeOf(u64) + @sizeOf([4]u64);
        try file.seekTo(0);
        _ = try file.write(FileMagic);
        _ = std.fmt.formatIntBuf(buffer[0..8], empty_offset, 10, std.fmt.Case.lower, .{ .width = 8, .fill = '0' });
        _ = try file.write(&buffer);
        _ = try file.write(&reserved_space);
        return file;
    }

    fn writeTableOfContents(self: *Self) !void {
        try self.file.seekTo(FileMagic.len);
        var toc_buffer: [8]u8 = undefined;
        _ = std.fmt.formatIntBuf(toc_buffer[0..8], self.toc_offset, 10, std.fmt.Case.lower, .{ .width = 8, .fill = '0' });
        _ = try self.file.writeAll(&toc_buffer);
        try self.file.seekTo(self.toc_offset);
        _ = try self.file.writeAll(TocMagic);
        _ = try self.file.writeAll("I would write the table of contents here.");
    }
};

test "Adding a file to a package" {
    const package_file = "/home/wilig/Projects/zig/risu/src/my_package.rsu";
    var my_package = try Package.init(std.testing.allocator, package_file);
    try my_package.add("/home/wilig/Projects/zig/risu/src/pkg.zig");
    my_package.deinit();
    try std.os.unlink(package_file);
}

//test "Getting stream from a package" {
//    var my_package = try Package.init(std.testing.allocator);
//    defer my_package.deinit();
//    try my_package.add("/home/wilig/Projects/zig/risu/src/pkg.zig");
//    var stream = try my_package.getStream("/home/wilig/Projects/zig/risu/src/pkg.zig");
//    var reader = stream.reader();
//    std.debug.print("reader: {}", .{reader});
//}
