const std = @import("std");
const Allocator = std.mem.Allocator;
const DefaultPrng = std.rand.DefaultPrng;
const Random = std.Random;
const metaEql = std.meta.eql;
const expect = std.testing.expect;

// A skip list is a probabilistic data structure that allows for O(log n) average case search, insert and delete operations.
// It maintains multiple layers of linked lists, with each higher layer being a "fast lane" that skips over elements,
// allowing for faster traversal than a regular linked list.

// TODO: Implement concurrency control.

const max_levels: usize = 32;
const p_value: f32 = 0.5;

const SkipListErrors = error{
    NotFound,
};

pub fn SkipList(
    comptime K: type,
    comptime V: type,
    comptime lessThanFn: fn (a: K, b: K) bool,
) type {
    return struct {
        const Self = @This();
        const Node = struct {
            key: K,
            value: V,
            forward: []?*Node,

            pub fn initHead(allocator: *const Allocator) *Node {
                const head_forwards = allocator.alloc(?*Node, max_levels) catch unreachable;
                @memset(head_forwards, null);
                const node = allocator.create(Node) catch unreachable;
                node.key = undefined;
                node.value = undefined;
                node.forward = head_forwards;
                return node;
            }

            pub fn init(allocator: *const Allocator, key: K, value: V, level: usize) *Node {
                const node = allocator.create(Node) catch unreachable;
                node.key = key;
                node.value = value;
                node.forward = allocator.alloc(?*Node, level) catch unreachable;
                @memset(node.forward, null);
                return node;
            }
        };

        allocator: *const Allocator,
        random: Random,
        head: *Node,
        level: usize,

        pub fn init(allocator: *const Allocator) Self {
            const head = Node.initHead(allocator);

            var rand_gen = DefaultPrng.init(@intCast(std.time.timestamp()));

            return Self{
                .head = head,
                .random = rand_gen.random(),
                .allocator = allocator,
                .level = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            var current: ?*Node = self.head;
            while (current) |node| {
                const next = node.forward[0];
                self.allocator.free(node.forward);
                self.allocator.destroy(node);
                current = next;
            }
        }

        pub fn find(self: *Self, key: K) SkipListErrors!V {
            var current: ?*Node = self.head;
            var i = self.level;
            while (i >= 0) : (i -= 1) {
                while (current.?.forward[i]) |next_node| {
                    if (!lessThanFn(next_node.key, key)) break;
                    current = next_node;
                }
                if (i == 0) break;
            }

            current = current.?.forward[0];
            if (current != null and metaEql(current.?.key, key)) {
                return current.?.value;
            } else {
                return SkipListErrors.NotFound;
            }
        }

        pub fn insert(self: *Self, key: K, value: V) void {
            var current: ?*Node = self.head;
            var update: [max_levels]?*Node = undefined;
            @memset(update[0..], null);
            var i = self.level;
            while (i >= 0) : (i -= 1) {
                while (current.?.forward[i]) |next_node| {
                    if (!lessThanFn(next_node.key, key)) break;
                    current = next_node;
                }
                update[i] = current;
                if (i == 0) break;
            }

            current = current.?.forward[0];
            if (current != null and metaEql(current.?.key, key)) {
                current.?.value = value;
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
        }

        pub fn remove(self: *Self, key: K) void {
            var current: ?*Node = self.head;
            var update: [max_levels]?*Node = undefined;
            @memset(update[0..], null);
            var i = self.level;
            while (i >= 0) : (i -= 1) {
                while (current.?.forward[i]) |next_node| {
                    if (!lessThanFn(next_node.key, key)) break;
                    current = next_node;
                }
                update[i] = current;
                if (i == 0) break;
            }

            current = current.?.forward[0];
            if (current != null and metaEql(current.?.key, key)) {
                for (0..self.level + 1) |j| {
                    if (update[i]) |update_node| {
                        if (update_node.forward[j] != current) {
                            break;
                        }
                        update_node.forward[j] = current.?.forward[j];
                    }
                }
                self.allocator.free(current.?.forward);
                self.allocator.destroy(current.?);
                while (self.level >= 0 and self.head.forward[self.level] == null) {
                    self.level -= 1;
                    if (self.level == 0) break;
                }
            } else {
                std.log.warn("Key not found, nothing to delete", .{});
            }
        }

        fn randomLevel(self: *Self) usize {
            var level: usize = 1;
            while (level < max_levels - 1 and self.random.float(f32) < p_value) {
                level += 1;
            }
            return level;
        }
    };
}

fn compareU32(a: u32, b: u32) bool {
    return a < b;
}

test "SkipList Non-Concurrent" {
    const allocator = std.testing.allocator;
    var skip_list = SkipList(u32, u32, compareU32).init(&allocator);
    defer skip_list.deinit();

    skip_list.insert(1, 2);
    skip_list.insert(2, 3);
    skip_list.insert(3, 4);
    skip_list.insert(4, 5);
    skip_list.insert(5, 6);
    skip_list.insert(6, 7);
    skip_list.insert(7, 8);

    try expect(try skip_list.find(1) == 2);
    try expect(try skip_list.find(2) == 3);
    try expect(try skip_list.find(3) == 4);
    try expect(try skip_list.find(4) == 5);
    try expect(try skip_list.find(5) == 6);
    try expect(try skip_list.find(6) == 7);
    try expect(try skip_list.find(7) == 8);

    try expect(skip_list.find(8) == SkipListErrors.NotFound);

    skip_list.remove(1);
    skip_list.remove(2);
    skip_list.remove(3);
    skip_list.remove(4);
    skip_list.remove(5);
    skip_list.remove(6);
    skip_list.remove(7);

    try expect(skip_list.find(1) == SkipListErrors.NotFound);
    try expect(skip_list.find(2) == SkipListErrors.NotFound);
    try expect(skip_list.find(3) == SkipListErrors.NotFound);
    try expect(skip_list.find(4) == SkipListErrors.NotFound);
    try expect(skip_list.find(5) == SkipListErrors.NotFound);
    try expect(skip_list.find(6) == SkipListErrors.NotFound);
    try expect(skip_list.find(7) == SkipListErrors.NotFound);
}

fn compareStrings(a: []const u8, b: []const u8) bool {
    const order = std.mem.order(u8, a, b);
    return order == .lt;
}

test "SkipList Non-Concurrent Strings" {
    const allocator = std.testing.allocator;
    var skip_list = SkipList([]const u8, []const u8, compareStrings).init(&allocator);
    defer skip_list.deinit();

    skip_list.insert("1", "2");
    skip_list.insert("2", "3");
    skip_list.insert("3", "4");

    try expect(std.mem.eql(u8, try skip_list.find("1"), "2"));
    try expect(std.mem.eql(u8, try skip_list.find("2"), "3"));
    try expect(std.mem.eql(u8, try skip_list.find("3"), "4"));

    try expect(skip_list.find("4") == SkipListErrors.NotFound);

    skip_list.remove("1");
    skip_list.remove("2");
    skip_list.remove("3");

    try expect(skip_list.find("1") == SkipListErrors.NotFound);
    try expect(skip_list.find("2") == SkipListErrors.NotFound);
    try expect(skip_list.find("3") == SkipListErrors.NotFound);
}
