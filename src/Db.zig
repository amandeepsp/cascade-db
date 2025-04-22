const std = @import("std");
const fs_ext = @import("fs_ext.zig");
const mem = std.mem;
const fs = std.fs;
const path = std.fs.path;
const io = std.io;
const assert = std.debug.assert;
const Memtable = @import("Memtable.zig");
const WriteAheadLogger = @import("WriteAheadLogger.zig");
const Event = @import("record.zig").Event;
pub const DBOptions = struct {
    root_dir: []const u8,
    memtable_flush_limit: usize = 100,
};

pub const DBError = error{
    DBAlreadyExists,
};

const Self = @This();

allocator: mem.Allocator,
options: DBOptions,
memtable: Memtable,
wal: WriteAheadLogger,
root_dir: fs.Dir,

pub fn init(allocator: mem.Allocator, options: DBOptions) !Self {
    const root_dir = openOrCreateRootDir(options.root_dir) catch |err| blk: {
        if (err == DBError.DBAlreadyExists) {
            break :blk try fs_ext.openDir(options.root_dir);
        }
        return err;
    };

    const memtable = try Memtable.init(allocator, options.memtable_flush_limit);
    const wal = try WriteAheadLogger.init(allocator, .{ .file_dir = root_dir });

    return Self{
        .allocator = allocator,
        .options = options,
        .memtable = memtable,
        .root_dir = root_dir,
        .wal = wal,
    };
}

fn openOrCreateRootDir(root_dir: []const u8) !fs.Dir {
    fs_ext.access(root_dir, .{ .mode = .read_write }) catch |err| {
        if (err == error.FileNotFound) {
            try fs_ext.makeDir(root_dir);
            return try fs_ext.openDir(root_dir);
        }
        return err;
    };

    return DBError.DBAlreadyExists;
}

pub fn put(self: *Self, key: []const u8, value: []const u8) !void {
    const event = Event{ .write = .{ .key = key, .value = value } };
    const encoded_event = self.allocator.alloc(u8, event.size()) catch unreachable;
    event.serialize(encoded_event) catch unreachable;
    defer self.allocator.free(encoded_event);
    self.wal.write(encoded_event) catch unreachable;

    try self.memtable.insert(key, value);
}

pub fn get(self: *Self, key: []const u8) ![]const u8 {
    return self.memtable.get(key);
}

pub fn remove(self: *Self, key: []const u8) !void {
    const event = Event{ .delete = .{ .key = key } };
    const encoded_event = self.allocator.alloc(u8, event.size()) catch unreachable;
    event.serialize(encoded_event) catch unreachable;
    defer self.allocator.free(encoded_event);
    self.wal.write(encoded_event) catch unreachable;

    try self.memtable.remove(key);
}

pub fn deinit(self: *Self) void {
    self.memtable.deinit();
    self.wal.deinit();
}
