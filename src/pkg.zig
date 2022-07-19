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
            log.err("Failed to seek to proper offset during constrainted read, error was {}", .{err});
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

pub const Package = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    entries: std.StringArrayHashMap(TocEntry),
    path: []const u8,
    file: fs.File,
    toc_offset: u64 = 0,
    reserved: [Reserved.len]u8 = Reserved,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !Self {
        var file: File = undefined;
        file = openFile(path, .read_write) catch |err| switch (err) {
            std.fs.File.OpenError.FileNotFound => try createEmptyPackage(path),
            else => {
                log.err("Failed to open package file, error was {}", .{err});
                return err;
            },
        };
        if (!try isValidPackage(file)) {
            return error.InvalidPackage;
        }
        var pkg = Self{
            .allocator = allocator,
            .file = file,
            .path = path,
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

    pub fn count(self: *Self) usize {
        return self.entries.count();
    }

    fn openFile(path: []const u8, mode: fs.File.OpenMode) !fs.File {
        if (fs.path.isAbsolute(path)) {
            return try fs.openFileAbsolute(path, .{ .mode = mode });
        } else {
            return try fs.cwd().openFile(path, .{ .mode = mode });
        }
    }

    fn isValidPackage(file: fs.File) !bool {
        var buffer: [FileMagic.len + Version.len]u8 = std.mem.zeroes([FileMagic.len + Version.len]u8);
        try file.seekTo(0);
        _ = try file.readAll(&buffer);
        if (!std.mem.eql(u8, buffer[0..FileMagic.len], FileMagic)) {
            log.err("File is not in package format.", .{});
            return false;
        }
        if (!std.mem.eql(u8, buffer[FileMagic.len..], Version)) {
            log.err("Package version mismatch.  Expected {s}, recieved {s}", .{ Version, buffer[FileMagic.len..] });
            return false;
        }
        return true;
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

    pub fn add(self: *Self, path: []const u8) !void {
        if (self.entries.contains(path)) {
            return error.DuplicateEntry;
        }
        var f = openFile(path, .read_only) catch |err| {
            log.err("Couldn't open file {s} error was {}", .{ path, err });
            return err;
        };
        const file_size = f.getEndPos() catch |err| {
            log.err("Failed to get file size, error was {}", .{err});
            return err;
        };
        var bytes_copied = fs.File.copyRangeAll(f, 0, self.file, self.toc_offset, try f.getEndPos()) catch |err| {
            log.err("Failed to copy source file into package, error was: {}", .{err});
            return err;
        };
        if (bytes_copied != file_size) {
            log.err("File size mismatch!  Read {} of {} bytes", .{ bytes_copied, file_size });
            return error.ReadMismatch;
        }
        var toc_entry = TocEntry{ .offset = self.toc_offset, .len = bytes_copied, .timestamp = std.time.milliTimestamp() };
        self.putEntry(path, toc_entry) catch |err| {
            log.err("Failed to add entry to package table of contents, error was {}", .{err});
            return err;
        };
        self.toc_offset += bytes_copied;
        try self.writeTableOfContents(self.file);
    }

    pub fn remove(self: *Self, path: []const u8) !void {
        if (!self.entries.contains(path)) {
            log.err("{s} not found in package", .{path});
            return error.NotFound;
        }

        // Create replacement package file with the same name as original.
        const basename = fs.path.basename(self.path);
        const dir_path = fs.path.dirname(self.path).?;
        const dir = try fs.cwd().openDir(dir_path, .{});
        var new_package = try fs.AtomicFile.init(basename, fs.File.default_mode, dir, true);
        defer new_package.deinit();
        var new_file = new_package.file;

        // Entry to remove
        const etr = self.entries.get(path).?;

        // Copy the before chunk
        var bytes_copied = fs.File.copyRangeAll(self.file, 0, new_file, 0, etr.offset) catch |err| {
            log.err("Failed to copy partial package file, error was: {}", .{err});
            return err;
        };
        std.debug.assert(bytes_copied == etr.offset);

        // Copy the after chunk
        bytes_copied = fs.File.copyRangeAll(self.file, etr.offset + etr.len, new_file, etr.offset, self.toc_offset - (etr.offset + etr.len)) catch |err| {
            log.err("Failed to copy partial package file, error was: {}", .{err});
            return err;
        };
        std.debug.assert(bytes_copied == self.toc_offset - (etr.offset + etr.len));

        // Update all the entries offsets for entries following the removed one
        for (self.entries.values()) |*entry| {
            if (entry.offset > etr.offset) {
                entry.offset = entry.offset - etr.len;
            }
        }

        const removed_key = self.entries.getKey(path).?;
        defer self.allocator.free(removed_key);

        if (!self.entries.swapRemove(path)) {
            log.err("Failed to remove {s} from the table of contents", .{path});
            return error.RemovalFailure;
        }

        self.toc_offset -= etr.len;

        self.writeTableOfContents(new_file) catch |err| {
            log.err("Failed to write new table of contents, error was: {}", .{err});
        };
        new_package.finish() catch |err| {
            log.err("Failed to finalize updated package, error was: {}", .{err});
            return err;
        };
    }

    pub fn getEntry(self: *Self, path: []const u8) !FileSlice {
        if (!self.entries.contains(path)) {
            log.err("{s} not found in package", .{path});
            return error.NotFound;
        }
        const entry = self.entries.get(path).?;
        return FileSlice{ .file = &self.file, .startOffset = entry.offset, .len = entry.len };
    }

    fn putEntry(self: *Self, path: []const u8, entry: TocEntry) !void {
        // TODO:  This seems wrong, need to remove this copy operation.
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

    fn writeTableOfContents(self: *Self, file: File) !void {
        const entries: u64 = self.entries.count();
        const toc_entry_size: u64 = @sizeOf(TocEntry);
        var pesky_strings = try std.ArrayListUnmanaged(u8).initCapacity(self.allocator, 1000);
        try file.seekTo(offsetToTocOffset());
        _ = try file.writeAll(&std.mem.toBytes(self.toc_offset));
        try file.seekTo(self.toc_offset);
        _ = try file.writeAll(TocMagic);
        _ = try file.write(&std.mem.toBytes(entries));
        _ = try file.write(&std.mem.toBytes(toc_entry_size));

        for (self.entries.keys()) |toc_name| {
            const toc_entry = std.mem.toBytes(self.entries.get(toc_name).?);
            _ = try file.writeAll(&toc_entry);
            try pesky_strings.appendSlice(self.allocator, toc_name);
            try pesky_strings.append(self.allocator, 0); // Add sentinel value
        }
        const toc_names = pesky_strings.toOwnedSlice(self.allocator);
        defer self.allocator.free(toc_names);
        // Leave of the trailing zero or splitting on load will fail
        _ = try file.writeAll(toc_names[0 .. toc_names.len - 1]);
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
            log.err("Table of contents magic not found, corrupt file?", .{});
            return error.CorruptFile;
        }

        var num_of_entries = std.mem.bytesToValue(usize, toc_header[TocMagic.len .. TocMagic.len + @sizeOf(usize)]);
        const toc_size = std.mem.bytesToValue(usize, toc_header[TocMagic.len + @sizeOf(usize) ..]);
        if (toc_size != @sizeOf(TocEntry)) {
            log.err("Size mismatch for table of content entries, corrupt file?", .{});
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
    const package_file = "../package.rsu";
    defer std.os.unlink(package_file) catch unreachable;
    var package = try Package.init(std.testing.allocator, package_file);
    try package.add("pkg.zig");
    package.deinit();
}

test "Getting stream from a package" {
    const package_file = "/tmp/package.rsu";
    const source_file = "pkg.zig";
    defer std.os.unlink(package_file) catch unreachable;
    var test_package = try Package.init(std.testing.allocator, package_file);
    try test_package.add(source_file);
    test_package.deinit(); // Force close and flush

    test_package = try Package.init(std.testing.allocator, package_file);
    defer test_package.deinit();
    var data = try test_package.getEntry(source_file);
    var packaged_file = try data.reader().readAllAlloc(std.testing.allocator, try data.getEndPos());
    defer std.testing.allocator.free(packaged_file);
    var original_fh = try fs.cwd().openFile(source_file, .{});
    defer original_fh.close();
    var original_file = try original_fh.reader().readAllAlloc(std.testing.allocator, try original_fh.getEndPos());
    defer std.testing.allocator.free(original_file);
    try std.testing.expect(std.mem.eql(u8, original_file, packaged_file));
}

test "Adding multiple files to a package" {
    const package_file = "/tmp/package.rsu";
    const source_files = [_][]const u8{ "pkg.zig", "log.zig", "main.zig" };
    defer std.os.unlink(package_file) catch unreachable;
    var test_package = try Package.init(std.testing.allocator, package_file);
    for (source_files) |src_file| {
        try test_package.add(src_file);
    }
    test_package.deinit(); // Force close and flush

    test_package = try Package.init(std.testing.allocator, package_file);
    defer test_package.deinit();
    try std.testing.expectEqual(@as(usize, 3), test_package.count());
    for (source_files) |src_file| {
        var data = try test_package.getEntry(src_file);
        var packaged_file = try data.reader().readAllAlloc(std.testing.allocator, try data.getEndPos());

        var original_fh = try fs.cwd().openFile(src_file, .{});
        var original_file = try original_fh.reader().readAllAlloc(std.testing.allocator, try original_fh.getEndPos());

        try std.testing.expect(std.mem.eql(u8, original_file, packaged_file));

        original_fh.close();
        std.testing.allocator.free(original_file);
        std.testing.allocator.free(packaged_file);
    }
}

test "Adding multiple files to a package over time" {
    const package_file = "/tmp/package.rsu";
    var source_files = [_][]const u8{ "pkg.zig", "log.zig" };
    defer std.os.unlink(package_file) catch unreachable;
    var test_package = try Package.init(std.testing.allocator, package_file);
    for (source_files) |src_file| {
        try test_package.add(src_file);
    }
    test_package.deinit(); // Force close and flush

    source_files = [_][]const u8{ "main.zig", "net/udp.zig" };
    test_package = try Package.init(std.testing.allocator, package_file);
    for (source_files) |src_file| {
        try test_package.add(src_file);
    }
    test_package.deinit();

    test_package = try Package.init(std.testing.allocator, package_file);
    defer test_package.deinit();
    try std.testing.expectEqual(@as(usize, 4), test_package.count());
}

test "Removing a file from a package" {
    const package_file = "/tmp/package.rsu";
    const source_files = [_][]const u8{ "pkg.zig", "log.zig", "main.zig" };
    defer std.os.unlink(package_file) catch unreachable;
    var test_package = try Package.init(std.testing.allocator, package_file);
    for (source_files) |src_file| {
        try test_package.add(src_file);
    }
    test_package.deinit(); // Force close and flush

    test_package = try Package.init(std.testing.allocator, package_file);
    try test_package.remove(source_files[0]);
    test_package.deinit();

    test_package = try Package.init(std.testing.allocator, package_file);
    defer test_package.deinit();
    try std.testing.expectEqual(@as(usize, 2), test_package.count());
    for (source_files[1..]) |src_file| {
        var data = try test_package.getEntry(src_file);
        var packaged_file = try data.reader().readAllAlloc(std.testing.allocator, try data.getEndPos());

        var original_fh = try fs.cwd().openFile(src_file, .{});
        var original_file = try original_fh.reader().readAllAlloc(std.testing.allocator, try original_fh.getEndPos());

        try std.testing.expect(std.mem.eql(u8, original_file, packaged_file));

        original_fh.close();
        std.testing.allocator.free(original_file);
        std.testing.allocator.free(packaged_file);
    }
}

test "Fails with invalid file" {
    const package_file = "pkg.zig";
    var package = Package.init(std.testing.allocator, package_file);
    try std.testing.expectError(error.InvalidPackage, package);
}

test "Fails with invalid file on unsupported version" {
    const bad_version = "X.X.X";
    const package_file = "../bad-package.rsu";
    var file = try fs.cwd().createFile(package_file, .{});
    _ = try file.write(FileMagic);
    _ = try file.write(bad_version);
    file.close();

    try std.testing.expectError(error.InvalidPackage, Package.init(std.testing.allocator, package_file));
}
