const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    defer {
        const deinit_result = gpa.deinit();
        if (deinit_result != .ok) {
            @panic("Failed to deinitialize allocator.");
        }
    }

    _ = allocator;
}
