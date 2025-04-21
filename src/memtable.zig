const std = @import("std");
const mem = std.mem;
const SkipList = @import("Skiplist.zig");

const Self = @This();

allocator: mem.Allocator,
skip_list: SkipList,
max_size: usize,

pub fn init(allocator: mem.Allocator, max_size: usize) !Self {
    return Self{
        .allocator = allocator,
        .skip_list = SkipList.init(allocator),
        .max_size = max_size,
    };
}

pub fn insert(self: *Self, key: []const u8, value: []const u8) !void {
    if (self.skip_list.count() >= self.max_size) {
        const frozen_memtable = try self.skip_list.clone();
        self.skip_list = SkipList.init(self.allocator);
        try self.flush_memtable(frozen_memtable);
        return;
    }
    try self.skip_list.insert(key, value);
}

fn flush_memtable(_: *Self, _: SkipList) !void {
    @panic("Not implemented");
}

pub fn get(self: *Self, key: []const u8) ![]const u8 {
    return self.skip_list.find(key);
}

pub fn remove(self: *Self, key: []const u8) !void {
    try self.skip_list.remove(key);
}

pub fn size(self: *Self) usize {
    return self.skip_list.size();
}

pub fn deinit(self: *Self) void {
    self.skip_list.deinit();
}
