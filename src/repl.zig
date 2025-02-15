const std = @import("std");
const mem = std.mem;
const io = std.io;
const fs = std.fs;
const log = std.log.scoped(.repl);

const DB = @import("db.zig");

pub fn repl_loop(db: *DB) !void {
    const stdin = io.getStdIn();
    const stdout = io.getStdOut();

    const prompt = "cascade> ";
    var line_buffer: [1024]u8 = undefined;

    while (true) {
        stdout.writeAll(prompt) catch unreachable;
        @memset(&line_buffer, 0);
        const line = try stdin.reader().readUntilDelimiterOrEof(&line_buffer, '\n') orelse "";
        const command = mem.trim(u8, line, " \t\r\n");
        if (command.len == 0) {
            continue;
        }

        if (mem.eql(u8, command, "exit")) {
            stdout.writeAll("bye ;)\n") catch unreachable;
            break;
        }

        if (mem.startsWith(u8, command, "get")) {
            const key = mem.trim(u8, command[4..], " \t");
            const value = db.get(key) catch |err| {
                log.err("key not found: {s}, err: {s}, stack: {any}", .{ key, @errorName(err), @errorReturnTrace() });
                continue;
            };
            stdout.writeAll(value) catch unreachable;
            stdout.writeAll("\n") catch unreachable;
            continue;
        }

        if (mem.startsWith(u8, command, "put")) {
            var kv = mem.splitSequence(u8, command[4..], " ");
            var key = kv.next() orelse {
                stdout.writeAll("invalid put command: key is required\n") catch unreachable;
                continue;
            };
            key = mem.trim(u8, key, " \t");
            var value = kv.next() orelse {
                stdout.writeAll("invalid put command: value is required\n") catch unreachable;
                continue;
            };
            value = mem.trim(u8, value, " \t");
            db.put(key, value) catch |err| {
                log.err("already exists key: {s}, value: {s}, err: {s}, stack: {any}", .{ key, value, @errorName(err), @errorReturnTrace() });
                continue;
            };
            continue;
        }

        if (mem.startsWith(u8, command, "delete")) {
            const key = mem.trim(u8, command[7..], " \t");
            db.remove(key) catch |err| {
                log.err("key not found: {s}, err: {s}, stack: {any}", .{ key, @errorName(err), @errorReturnTrace() });
                continue;
            };
            stdout.writeAll("deleted\n") catch unreachable;
            continue;
        }

        stdout.writeAll("invalid command\n") catch unreachable;
    }
}
