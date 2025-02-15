const std = @import("std");
const DB = @import("db.zig");
const repl = @import("repl.zig");
const logging = @import("logging.zig");

pub const std_options = .{
    .log_level = .info,
    .logFn = logging.ansiLogFn,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    defer {
        const deinit_result = gpa.deinit();
        if (deinit_result != .ok) {
            @panic("Failed to deinitialize allocator.");
        }
    }

    var args = std.process.args();
    _ = args.next(); // skip the first arg (the program name)

    const root_dir = args.next() orelse {
        std.log.err("No root directory provided, using current directory.", .{});
        return;
    };

    var db = try DB.init(allocator, .{ .root_dir = root_dir });
    defer db.deinit();

    try repl.repl_loop(&db);
}
