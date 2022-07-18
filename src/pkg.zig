const std = @import("std");
const os = std.os;
const io = std.io;
const fs = std.fs;
const File = std.fs.File;
const log = std.log.scoped(.pkg);

const FileMagic = "RISU";
const Version = "0.0.1";
const Reserved = [_]u8{ 'U', 'S', 'I', 'R' } ** 16;
const TocMagic = "TOC!";

const TocEntry = struct {
    offset: u64,
    len: u64,
    timestamp: i64,
};

const FileSlice = struct {
    const Self = @This();

    file: *fs.File,
    startOffset: u64,
    currentOffset: u64 = 0,
    len: u64,
    pub const Reader = io.Reader(*FileSlice, fs.File.ReadError, read);
    pub const SeekableStream = io.SeekableStream(
        *FileSlice,
        File.SeekError,
        File.GetSeekPosError,
        seekTo,
        seekBy,
        getPos,
        getEndPos,
    );

    pub fn seekTo(self: *Self, offset: u64) File.SeekError!void {
        if (offset > self.len) {
            return fs.File.SeekError;
        }
        try self.file.seekTo(self.startOffset + offset);
        self.currentOffset = self.startOffset + offset;
    }

    pub fn seekBy(self: *Self, amt: i64) File.SeekError!void {
        if (self.currentPosition + amt > self.len or self.currentPosition + amt < self.startOffset) {
            return error.SeekError;
        }
        try self.file.seekBy(amt);
    }

    pub fn getEndPos(self: *Self) File.GetSeekPosError!u64 {
        return self.len;
    }

    pub fn getPos(self: *Self) File.GetSeekPosError!u64 {
        return self.currentPosition - self.startOffset;
    }

    pub fn read(self: *Self, dest: []u8) File.ReadError!usize {
        self.file.seekTo(self.startOffset + self.currentOffset) catch |err| {
            log.err("Failed to seek to proper offset during constrainted read, error was {}\n", .{err});
            return error.InputOutput;
        };
        const max_read = std.math.min(self.len - self.currentOffset, dest.len);
        const n = try self.file.read(dest[0..max_read]);
        self.currentOffset += n;
        return n;
    }

    pub fn reader(self: *Self) Reader {
        return .{ .context = self };
    }

    pub fn seekableStream(self: *Self) SeekableStream {
        return .{ .context = self };
    }
};

const Package = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    entries: std.StringArrayHashMap(TocEntry),
    file: fs.File,
    toc_offset: u64 = 0,
    reserved: [Reserved.len]u8 = Reserved,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Self {
        var file: fs.File = undefined;
        file = openAndValidateFile(path) catch |err| switch (err) {
            std.fs.File.OpenError.FileNotFound => try createEmptyPackage(path),
            else => {
                log.err("Failed to open package file, error was {}\n", .{err});
                return err;
            },
        };
        var pkg = Self{
            .allocator = allocator,
            .file = file,
            .entries = std.StringArrayHashMap(TocEntry).init(allocator),
        };
        try pkg.loadTableOfContents();
        return pkg;
    }

    pub fn deinit(self: *Self) void {
        self.file.close();
        for (self.entries.keys()) |k| {
            self.allocator.free(k);
        }
        self.entries.deinit();
    }

    fn openFile(path: []const u8, mode: fs.File.OpenMode) !fs.File {
        if (fs.path.isAbsolute(path)) {
            return try fs.openFileAbsolute(path, .{ .mode = mode });
        } else {
            return try fs.cwd().openFile(path, .{ .mode = mode });
        }
    }

    fn openAndValidateFile(path: []const u8) !fs.File {
        var buffer: [FileMagic.len + Version.len]u8 = std.mem.zeroes([FileMagic.len + Version.len]u8);
        const file = try openFile(path, .read_write);
        _ = try file.readAll(&buffer);
        if (!std.mem.eql(u8, buffer[0..FileMagic.len], FileMagic)) {
            log.err("File is not in package format.\n", .{});
            return error.InvalidFormat;
        }
        if (!std.mem.eql(u8, buffer[FileMagic.len..], Version)) {
            log.err("Package version mismatch.  Expected {s}, recieved {s}\n", .{ Version, buffer[FileMagic.len..] });
            return error.VersionMismatch;
        }
        return file;
    }

    fn offsetToData() u64 {
        return FileMagic.len +
            Version.len +
            @sizeOf(u64) +
            Reserved.len;
    }

    fn offsetToTocOffset() u64 {
        return FileMagic.len + Version.len;
    }

    // TODO: Don't read the whole file into memory, stream it to the package.
    pub fn add(self: *Self, path: []const u8) !void {
        if (self.entries.contains(path)) {
            return error.DuplicateEntry;
        }
        var f = openFile(path, .read_only) catch |err| {
            log.err("Couldn't open file {s} error was {}\n", .{ path, err });
            return err;
        };
        const file_size = f.getEndPos() catch |err| {
            log.err("Failed to get file size, error was {}\n", .{err});
            return err;
        };
        const buffer = self.allocator.alloc(u8, @as(usize, file_size)) catch |err| {
            log.err("Couldn't allocate enough space to store file data, error was {}\n", .{err});
            return err;
        };
        defer self.allocator.free(buffer);
        var bytes_read = f.readAll(buffer) catch |err| {
            log.err("Failed to read file data into buffer, error was {}\n", .{err});
            return err;
        };
        if (bytes_read != file_size) {
            log.err("File size mismatch!  Read {} of {} bytes.\n", .{ bytes_read, file_size });
            return error.ReadMismatch;
        }
        try self.file.seekTo(offsetToData());
        self.file.writeAll(buffer) catch |err| {
            log.err("Couldn't write {s} to package, error was {}.\n", .{ path, err });
            return err;
        };
        var toc_entry = TocEntry{ .offset = self.toc_offset, .len = bytes_read, .timestamp = std.time.milliTimestamp() };
        self.putEntry(path, toc_entry) catch |err| {
            log.err("Failed to add entry to package table of contents, error was {}\n", .{err});
            return err;
        };
        self.toc_offset += bytes_read;
        try self.writeTableOfContents();
    }

    pub fn remove(self: *Self, path: []const u8) !void {
        _ = self;
        _ = path;
        // TODO: Remove entry, and rewrite package file.
    }

    pub fn getEntry(self: *Self, path: []const u8) !FileSlice {
        if (!self.entries.contains(path)) {
            log.err("{s} not found in package\n", .{path});
            return error.NotFound;
        }
        const entry = self.entries.get(path).?;
        return FileSlice{ .file = &self.file, .startOffset = entry.offset, .len = entry.len };
    }

    fn putEntry(self: *Self, path: []const u8, entry: TocEntry) !void {
        const toc_key: []u8 = try self.allocator.alloc(u8, path.len);
        std.mem.copy(u8, toc_key, path);
        try self.entries.put(toc_key, entry);
    }

    fn createEmptyPackage(path: []const u8) !std.fs.File {
        var file: fs.File = undefined;
        if (fs.path.isAbsolute(path)) {
            file = try fs.createFileAbsolute(path, .{ .exclusive = true, .lock = .Exclusive, .read = true, .truncate = false });
        } else {
            file = try fs.cwd().createFile(path, .{ .exclusive = true, .lock = .Exclusive, .read = true, .truncate = false });
        }
        const entry_count: u64 = 0;
        const toc_entry_size: u64 = @sizeOf(TocEntry);
        try file.seekTo(0);
        _ = try file.write(FileMagic);
        _ = try file.write(Version);
        _ = try file.write(&std.mem.toBytes(offsetToData()));
        _ = try file.write(&Reserved);
        _ = try file.write(TocMagic);
        _ = try file.write(&std.mem.toBytes(entry_count));
        _ = try file.write(&std.mem.toBytes(toc_entry_size));

        return file;
    }

    fn writeTableOfContents(self: *Self) !void {
        const entries: u64 = self.entries.count();
        const toc_entry_size: u64 = @sizeOf(TocEntry);
        var pesky_strings = try std.ArrayListUnmanaged(u8).initCapacity(self.allocator, 1000);
        try self.file.seekTo(offsetToTocOffset());
        _ = try self.file.writeAll(&std.mem.toBytes(self.toc_offset));
        try self.file.seekTo(self.toc_offset);
        _ = try self.file.writeAll(TocMagic);
        _ = try self.file.write(&std.mem.toBytes(entries));
        _ = try self.file.write(&std.mem.toBytes(toc_entry_size));

        for (self.entries.keys()) |toc_name| {
            const toc_entry = std.mem.toBytes(self.entries.get(toc_name).?);
            _ = try self.file.writeAll(&toc_entry);
            try pesky_strings.appendSlice(self.allocator, toc_name);
            try pesky_strings.append(self.allocator, 0); // Add sentinel value
        }
        const toc_names = pesky_strings.toOwnedSlice(self.allocator);
        defer self.allocator.free(toc_names);
        // Leave of the trailing zero or splitting on load will fail
        _ = try self.file.writeAll(toc_names[0 .. toc_names.len - 1]);
    }

    fn loadTableOfContents(self: *Self) !void {
        var toc_header: [TocMagic.len + @sizeOf(usize) * 2]u8 = std.mem.zeroes([20]u8);
        var tocOffBuf: [@sizeOf(@TypeOf(self.toc_offset))]u8 = std.mem.zeroes([8]u8);
        try self.file.seekTo(offsetToTocOffset());
        _ = try self.file.readAll(&tocOffBuf);
        self.toc_offset = std.mem.bytesToValue(u64, &tocOffBuf);
        try self.file.seekTo(self.toc_offset);
        _ = try self.file.readAll(toc_header[0..]);

        if (!std.mem.eql(u8, TocMagic, toc_header[0..TocMagic.len])) {
            log.err("Table of contents magic not found, corrupt file?\n", .{});
            return error.CorruptFile;
        }

        var num_of_entries = std.mem.bytesToValue(usize, toc_header[TocMagic.len .. TocMagic.len + @sizeOf(usize)]);
        const toc_size = std.mem.bytesToValue(usize, toc_header[TocMagic.len + @sizeOf(usize) ..]);
        if (toc_size != @sizeOf(TocEntry)) {
            log.err("Size mismatch for table of content entries, corrupt file?\n", .{});
            return error.CorruptFile;
        }

        if (num_of_entries > 0) {
            var entries = try self.allocator.alloc(TocEntry, num_of_entries);
            defer self.allocator.free(entries);

            var idx: usize = 0;
            var raw_entry: [@sizeOf(TocEntry)]u8 = undefined;
            while (idx < num_of_entries) : (idx += 1) {
                _ = try self.file.readAll(&raw_entry);
                var toc_entry = @alignCast(@alignOf(TocEntry), std.mem.bytesAsValue(TocEntry, &raw_entry));
                entries[idx] = toc_entry.*;
            }

            // Read the entry key strings and build the TOC hashmap
            const sentinel: []const u8 = &[_]u8{0};
            const current_pos = try self.file.getPos();
            const end_pos = try self.file.getEndPos();
            const key_buffer = try self.allocator.alloc(u8, end_pos - current_pos);
            defer self.allocator.free(key_buffer);
            _ = try self.file.readAll(key_buffer);

            const key_count = std.mem.count(u8, key_buffer, sentinel);
            std.debug.assert(key_count + 1 == entries.len);
            var ki = std.mem.split(u8, key_buffer, sentinel);
            idx = 0;
            while (ki.next()) |key| {
                try self.putEntry(key, entries[idx]);
                idx += 1;
            }
        }
    }
};

test "Adding a file to a package" {
    const package = "../my_package.rsu";
    var my_package = try Package.init(std.testing.allocator, package);
    try my_package.add("pkg.zig");
    my_package.deinit();
    try std.os.unlink(package);
}

test "Getting stream from a package" {
    const package = "reading_package.rsu";
    const source_file = "pkg.zig";
    errdefer std.os.unlink(package) catch unreachable;
    var test_package = try Package.init(std.testing.allocator, package);
    try test_package.add(source_file);
    test_package.deinit(); // Force close and flush

    test_package = try Package.init(std.testing.allocator, package);
    defer test_package.deinit();
    var data = try test_package.getEntry(source_file);
    var packaged_file = try data.reader().readAllAlloc(std.testing.allocator, try data.getEndPos());
    defer std.testing.allocator.free(packaged_file);
    var original_fh = try fs.cwd().openFile(source_file, .{});
    defer original_fh.close();
    var original_file = try original_fh.reader().readAllAlloc(std.testing.allocator, try original_fh.getEndPos());
    defer std.testing.allocator.free(original_file);
    try std.testing.expect(std.mem.eql(u8, original_file, packaged_file));
    try std.os.unlink(package);
}
