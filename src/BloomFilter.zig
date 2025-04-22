const std = @import("std");

const Self = @This();
const HashFn = *const fn ([]const u8) u64;

const hash_fns = [_]HashFn{
    std.hash.CityHash64.hash,
    std.hash.Fnv1a_64.hash,
    std.hash.Murmur2_64.hash,
};

allocator: std.mem.Allocator,
size: usize,
buckets: []u64,

pub fn init(allocator: std.mem.Allocator, size: usize) !Self {
    if (size % 64 != 0) {
        return error.InvalidSize;
    }

    const buckets = try allocator.alloc(u64, size / 64);
    @memset(buckets, 0);

    return Self{
        .allocator = allocator,
        .size = size,
        .buckets = buckets,
    };
}

pub fn deinit(self: *Self) void {
    self.allocator.free(self.buckets);
}

pub fn insert(self: *Self, key: []const u8) !void {
    for (hash_fns) |hash_fn| {
        const key_hash: u64 = hash_fn(key);
        const index = key_hash % self.size;
        const bucket_index = index / 64;
        const bit_index: u6 = @intCast(index % 64);
        const mask: u64 = @as(u64, 1) << bit_index;
        self.buckets[bucket_index] |= mask;
    }
}

pub fn maybe_contains(self: *Self, key: []const u8) bool {
    for (hash_fns) |hash_fn| {
        const key_hash: u64 = hash_fn(key);
        const index = key_hash % self.size;
        const bucket_index = index / 64;
        const bit_index: u6 = @intCast(index % 64);
        const mask: u64 = @as(u64, 1) << bit_index;
        if (self.buckets[bucket_index] & mask == 0) {
            return false;
        }
    }
    return true;
}

test "BloomFilter" {
    var bloom_filter = try Self.init(std.testing.allocator, 1 << 6);
    defer bloom_filter.deinit();
    try bloom_filter.insert("hello");
    try bloom_filter.insert("world");
    try std.testing.expect(bloom_filter.maybe_contains("hello"));
    try std.testing.expect(bloom_filter.maybe_contains("world"));
    try std.testing.expect(!bloom_filter.maybe_contains("foo"));
    try std.testing.expect(!bloom_filter.maybe_contains("bar"));
    try std.testing.expect(!bloom_filter.maybe_contains("baz"));
}
