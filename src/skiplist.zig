const std = @import("std");
const Allocator = std.mem.Allocator;
const DefaultPrng = std.rand.DefaultPrng;
const Random = std.Random;
const meta_eql = std.meta.eql;
const expect = std.testing.expect;
const max_levels: usize = 32;
const p_value: f32 = 0.5;

const SkipListErrors = error{
    OutOfRange,
};

pub fn SkipList(
    comptime K: type,
    comptime V: type,
    comptime less_than_fn: fn (a: K, b: K) bool,
) type {
    return struct {
        const Self = @This();
        const Node = struct {
            key: K,
            value: V,
            forward: []?*Node,

            pub fn init_head(allocator: *Allocator) *Node {
                const head_forwards = allocator.alloc(?*Node, max_levels) catch unreachable;
                @memset(head_forwards, null);
                const node = allocator.create(Node) catch unreachable;
                node.key = undefined;
                node.value = undefined;
                node.forward = head_forwards;
                return node;
            }

            pub fn init(allocator: *Allocator, key: K, value: V, level: usize) *Node {
                const node = allocator.create(Node) catch unreachable;
                node.key = key;
                node.value = value;
                node.forward = allocator.alloc(?*Node, level) catch unreachable;
                @memset(node.forward, null);
                return node;
            }
        };

        head: *Node,
        allocator: *Allocator,
        level: usize,
        random: Random,

        pub fn init(allocator: *Allocator) Self {
            const head = Node.init_head(allocator);

            var rand_gen = DefaultPrng.init(@intCast(std.time.timestamp()));

            return Self{
                .head = head,
                .allocator = allocator,
                .level = 0,
                .random = rand_gen.random(),
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

        pub fn find(self: *Self, key: K) !V {
            var current: ?*Node = self.head;
            var i = self.level;
            while (i >= 0) : (i -= 1) {
                while (current.?.forward[i]) |next_node| {
                    if (!less_than_fn(next_node.key, key)) break;
                    current = next_node;
                }
                if (i == 0) break;
            }

            current = current.?.forward[0];
            if (current != null and meta_eql(current.?.key, key)) {
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
                    if (!less_than_fn(next_node.key, key)) break;
                    current = next_node;
                }
                update[i] = current;
                if (i == 0) break;
            }

            current = current.?.forward[0];
            if (current != null and meta_eql(current.?.key, key)) {
                current.?.value = value;
            } else {
                const new_level = self.random_level();
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
                    if (!less_than_fn(next_node.key, key)) break;
                    current = next_node;
                }
                update[i] = current;
                if (i == 0) break;
            }

            current = current.?.forward[0];
            if (current != null and meta_eql(current.?.key, key)) {
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

        fn random_level(self: *Self) usize {
            var level: usize = 1;
            while (level < max_levels - 1 and self.random.float(f32) < p_value) {
                level += 1;
            }
            return level;
        }
    };
}

fn compare_u32(a: u32, b: u32) bool {
    return a < b;
}

test "SkipList Non-Parallel" {
    var allocator = std.testing.allocator;
    var skip_list = SkipList(u32, u32, compare_u32).init(&allocator);
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

    skip_list.remove(1);
    skip_list.remove(2);
    skip_list.remove(3);
    skip_list.remove(4);
    skip_list.remove(5);
    skip_list.remove(6);
    skip_list.remove(7);
}
