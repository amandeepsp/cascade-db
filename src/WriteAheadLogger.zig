const std = @import("std");
const fs = std.fs;
const testing = std.testing;
const atomic = std.atomic;
const log = std.log.scoped(.wal);
const io = std.io;
const Allocator = std.mem.Allocator;
const Record = @import("record.zig").Record;
const Event = @import("record.zig").Event;
const RecordType = @import("record.zig").RecordType;

pub const WalOptions = struct {
    file_dir: fs.Dir = fs.cwd(),
    file_name: []const u8 = "wal.log",
    block_size: usize = 32 * 1024 * @sizeOf(u8), // 32KB
};

const WriteAheadLogger = @This();
// WAL format adapted form https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format
//       +-----+-------------+--+----+----------+------+-- ... ----+
// File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
//       +-----+-------------+--+----+----------+------+-- ... ----+
//       <--- block_size ------>|<-- block_size ------>|

// rn = variable size records
// P = Padding

// A fixed block size has some drawbacks, i.e. wasted space if the records are small, block read/write overhead is
// same for all types of workloads. But it has the advantage of simplicity and I/O performance because of better
// alignment with os and device blocks, and better cache locality.

// TODO: Implement recycling blocks when a write is committed. Need to keep track of the log seq number and only
// recycle blocks that have been flushed to disk.

// TODO: Investigate various i/o techniques, e.g. direct i/o, mmap, etc.

allocator: Allocator,
fd: fs.File,
block_size: usize,

pub fn init(allocator: Allocator, options: WalOptions) !WriteAheadLogger {
    const fd = try options.file_dir.createFile(
        options.file_name,
        .{ .read = true, .truncate = false },
    );

    std.debug.assert(options.block_size > Record.header_size());

    return WriteAheadLogger{
        .allocator = allocator,
        .fd = fd,
        .block_size = options.block_size,
    };
}

pub fn deinit(self: *WriteAheadLogger) void {
    self.fd.close();
}

pub fn flush(self: *WriteAheadLogger) !void {
    try self.fd.sync();
}

pub fn read(self: *WriteAheadLogger, buffer: []u8) ![]Record {
    var decode_offset: usize = 0;
    var records = std.ArrayList(Record).init(self.allocator.*);
    errdefer records.deinit();
    while (decode_offset < self.block_size) {
        const record = Record.decode(buffer[decode_offset..]) catch {
            log.info("Failed to decode record at offset, hit block padding={d}", .{decode_offset});
            break;
        };
        records.append(record) catch unreachable;
        decode_offset += record.size();
    }

    return records.toOwnedSlice();
}

pub fn write(self: *WriteAheadLogger, payload: []const u8) !void {
    const records = self.encode(payload);
    defer self.allocator.free(records);

    for (records) |record| {
        const curr_end_pos: usize = self.fd.getEndPos() catch unreachable;
        const last_block_space_left = self.block_size - (curr_end_pos % self.block_size);

        const buffer = self.allocator.alloc(u8, record.size()) catch unreachable;
        defer self.allocator.free(buffer);
        @memset(buffer, 0);
        try record.encode(buffer);

        const fd_writer = self.fd.writer();

        if (record.size() <= last_block_space_left) {
            // We can fit the record in the last block
            try fd_writer.writeAll(buffer);
        } else {
            // We need to write to a new block
            try fd_writer.writeByteNTimes(0, last_block_space_left);
            try fd_writer.writeAll(buffer);
        }
    }
}

fn encode(self: *WriteAheadLogger, payload: []const u8) []Record {
    const length = payload.len;
    const header_length = Record.header_size();

    if (length + header_length > self.block_size) {
        var offset: usize = 0;
        const num_records = length / (self.block_size - header_length) + 1;
        const record_length = self.block_size - header_length;
        const records = self.allocator.alloc(Record, num_records) catch unreachable;

        while (offset < payload.len) {
            var curr_payload: []const u8 = undefined;
            if (offset + record_length >= payload.len) {
                curr_payload = payload[offset..];
            } else {
                curr_payload = payload[offset..(offset + record_length)];
            }

            var curr_type: RecordType = undefined;
            if (offset == 0) {
                curr_type = .FIRST;
            } else if (offset + record_length >= payload.len) {
                curr_type = .LAST;
            } else {
                curr_type = .MIDDLE;
            }

            const curr_record = Record.init(curr_payload, curr_type);
            records[offset / record_length] = curr_record;
            offset += record_length;
        }

        return records;
    } else {
        const record = Record.init(payload, .FULL);
        const records = self.allocator.alloc(Record, 1) catch unreachable;
        records[0] = record;
        return records;
    }
}

test "WriteAheadLogger write" {
    fs.cwd().deleteFile("test.log") catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };

    const block_size = 32 * @sizeOf(u8);

    const allocator = std.testing.allocator;
    var wal = try WriteAheadLogger.init(allocator, .{
        .file_dir = fs.cwd(),
        .file_name = "test.log",
        .block_size = block_size,
    });
    defer wal.deinit();

    try wal.write("hello, world-1");
    try wal.write("hello, world-2");
    try wal.write("hello, world-3");
    try wal.write("hel0");
    try wal.write("hello, world");
    try wal.write("hello, world-6");
    try wal.write("lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua");

    const stat = try wal.fd.stat();
    try testing.expectEqual(stat.size, 315); // Last block is not padded.
}
