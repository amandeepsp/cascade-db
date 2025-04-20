const std = @import("std");
const Allocator = std.mem.Allocator;
const DefaultPrng = std.Random.DefaultPrng;
const Random = std.Random;
const mem = std.mem;
const expect = std.testing.expect;

// A skip list is a probabilistic data structure that allows for O(log n) average case search, insert and delete operations.
// It maintains multiple layers of linked lists, with each higher layer being a "fast lane" that skips over elements,
// allowing for faster traversal than a regular linked list.

// TODO: Implement concurrency control.

const max_levels: usize = 32;
const p_value: f32 = 0.5;

const SkipListErrors = error{
    NotFound,
    AlreadyExists,
};

fn strLessThan(a: []const u8, b: []const u8) bool {
    const order = std.mem.order(u8, a, b);
    return order == .lt;
}

const Self = @This();
const Node = struct {
    key: []const u8,
    value: []const u8,
    forward: []?*Node,

    pub fn initHead(allocator: Allocator) *Node {
        const head_forwards = allocator.alloc(?*Node, max_levels) catch unreachable;
        @memset(head_forwards, null);
        const node = allocator.create(Node) catch unreachable;
        node.key = undefined;
        node.value = undefined;
        node.forward = head_forwards;
        return node;
    }

    pub fn init(allocator: Allocator, key: []const u8, value: []const u8, level: usize) *Node {
        const node = allocator.create(Node) catch unreachable;
        const key_copy = allocator.dupe(u8, key) catch unreachable;
        const value_copy = allocator.dupe(u8, value) catch unreachable;
        node.key = key_copy;
        node.value = value_copy;
        node.forward = allocator.alloc(?*Node, level) catch unreachable;
        @memset(node.forward, null);
        return node;
    }

    pub fn deinit(self: *Node, allocator: Allocator) void {
        allocator.free(self.key);
        allocator.free(self.value);
        allocator.free(self.forward);
        allocator.destroy(self);
    }

    pub fn deinitHead(self: *Node, allocator: Allocator) void {
        allocator.free(self.forward);
        allocator.destroy(self);
    }
};

allocator: Allocator,
random: Random,
head: *Node,
level: usize,
size: usize,

pub fn init(allocator: Allocator) Self {
    const head = Node.initHead(allocator);

    var rand_gen = DefaultPrng.init(@intCast(std.time.timestamp()));

    return Self{
        .head = head,
        .random = rand_gen.random(),
        .allocator = allocator,
        .level = 0,
        .size = 0,
    };
}

pub fn deinit(self: *Self) void {
    var current: ?*Node = self.head.forward[0];
    self.head.deinitHead(self.allocator);

    while (current) |node| {
        const next = node.forward[0];
        node.deinit(self.allocator);
        current = next;
    }
}

pub fn find(self: *Self, key: []const u8) SkipListErrors![]const u8 {
    var current: ?*Node = self.head;
    var i = self.level;
    while (i >= 0) : (i -= 1) {
        while (current.?.forward[i]) |next_node| {
            if (!strLessThan(next_node.key, key)) break;
            current = next_node;
        }
        if (i == 0) break;
    }

    current = current.?.forward[0];
    if (current != null and mem.eql(u8, current.?.key, key)) {
        return current.?.value;
    } else {
        return SkipListErrors.NotFound;
    }
}

pub fn insert(self: *Self, key: []const u8, value: []const u8) SkipListErrors!void {
    var current: ?*Node = self.head;
    var update: [max_levels]?*Node = undefined;
    @memset(update[0..], null);
    var i = self.level;
    while (i >= 0) : (i -= 1) {
        while (current.?.forward[i]) |next_node| {
            if (!strLessThan(next_node.key, key)) break;
            current = next_node;
        }
        update[i] = current;
        if (i == 0) break;
    }

    current = current.?.forward[0];
    if (current != null and mem.eql(u8, current.?.key, key)) {
        return SkipListErrors.AlreadyExists;
    } else {
        const new_level = self.randomLevel();
        if (new_level > self.level) {
            var j = self.level;
            while (j < new_level) : (j += 1) {
                update[j] = self.head;
            }
            self.level = new_level;
        }
        const new_node = Node.init(self.allocator, key, value, new_level);
        var j: usize = 0;
        while (j < new_level) : (j += 1) {
            if (update[j]) |update_node| {
                new_node.forward[j] = update_node.forward[j];
                update_node.forward[j] = new_node;
            }
        }
    }

    self.size += 1;
}

pub fn remove(self: *Self, key: []const u8) SkipListErrors!void {
    var current: ?*Node = self.head;
    var update: [max_levels]?*Node = undefined;
    @memset(update[0..], null);
    var i = self.level;
    while (i >= 0) : (i -= 1) {
        while (current.?.forward[i]) |next_node| {
            if (!strLessThan(next_node.key, key)) break;
            current = next_node;
        }
        update[i] = current;
        if (i == 0) break;
    }

    current = current.?.forward[0];
    if (current != null and mem.eql(u8, current.?.key, key)) {
        for (0..self.level + 1) |j| {
            if (update[i]) |update_node| {
                if (update_node.forward[j] != current) {
                    break;
                }
                update_node.forward[j] = current.?.forward[j];
            }
        }

        current.?.deinit(self.allocator);

        while (self.level >= 0 and self.head.forward[self.level] == null) {
            self.level -= 1;
            if (self.level == 0) break;
        }
    } else {
        return SkipListErrors.NotFound;
    }

    self.size -= 1;
}

fn randomLevel(self: *Self) usize {
    var level: usize = 1;
    while (level < max_levels - 1 and self.random.float(f32) < p_value) {
        level += 1;
    }
    return level;
}

pub fn count(self: *Self) usize {
    return self.size;
}

pub fn clone(self: *Self) !Self {
    var new_skip_list = Self.init(self.allocator);
    // TODO: Use a more efficient clone method. this is O(n log n), can be improved to O(n)
    var current: ?*Node = self.head;
    while (current) |node| {
        new_skip_list.insert(node.key, node.value) catch unreachable;
        current = node.forward[0];
    }

    return new_skip_list;
}

test "SkipList Non-Concurrent Strings" {
    const allocator = std.testing.allocator;
    var skip_list = Self.init(allocator);
    defer skip_list.deinit();

    try skip_list.insert("1", "2");
    try skip_list.insert("2", "3");
    try skip_list.insert("3", "4");

    try expect(std.mem.eql(u8, try skip_list.find("1"), "2"));
    try expect(std.mem.eql(u8, try skip_list.find("2"), "3"));
    try expect(std.mem.eql(u8, try skip_list.find("3"), "4"));

    try expect(skip_list.find("4") == SkipListErrors.NotFound);

    try skip_list.remove("1");
    try skip_list.remove("2");
    try skip_list.remove("3");

    try expect(skip_list.find("1") == SkipListErrors.NotFound);
    try expect(skip_list.find("2") == SkipListErrors.NotFound);
    try expect(skip_list.find("3") == SkipListErrors.NotFound);
}
