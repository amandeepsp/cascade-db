const std = @import("std");
const Allocator = std.mem.Allocator;
const fs = std.fs;
const log = std.log;
const testing = std.testing;
const hash = std.hash;

// WAL format adapted form https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format

pub const WalOptions = struct {
    file_dir: fs.Dir = fs.cwd(),
    file_name: []const u8 = "wal.log",
    block_size: usize = 32 * 1024 * @sizeOf(u8), // 32KB
};

// Since block_size is fixed, a record could get broken into multiple blocks,
// to reconstruct, we need info where does the record start and end.
const RecordType = enum(u8) {
    FULL = 1,
    FIRST = 2,
    MIDDLE = 3,
    LAST = 4,
};

const Record = struct {
    // +---------+-------------+------------+--- ... ---+
    // |CRC (u32) | Size (u16) | Type (u8) | Payload   |
    // +---------+-------------+------------+--- ... ---+
    // CRC = 32bit hash computed over the payload using CRC
    // Size = Length of the payload data
    // Type = Type of record, FULL, FIRST, MIDDLE, or LAST
    //       The type is used to group a bunch of records together to represent
    //       blocks that are larger than kBlockSize
    // Payload = Byte stream as long as specified by the payload size
    checksum: u32,
    length: u16,
    record_type: RecordType,
    data: []const u8,

    pub fn size(self: *const Record) usize {
        return @sizeOf(u32) + @sizeOf(u16) + @sizeOf(u8) + self.length;
    }

    pub fn header_size() usize {
        return @sizeOf(u32) + @sizeOf(u16) + @sizeOf(u8);
    }

    pub fn init(payload: []const u8, record_type: RecordType) Record {
        const checksum = hash.Crc32.hash(payload);
        return Record{
            .checksum = checksum,
            .length = @intCast(payload.len),
            .record_type = record_type,
            .data = payload,
        };
    }

    pub fn encode(self: *const Record, buffer: []u8) !void {
        var buffer_stream = std.io.fixedBufferStream(buffer);
        var buffer_writer = buffer_stream.writer();

        try buffer_writer.writeInt(u32, self.checksum, .little);
        try buffer_writer.writeInt(u16, @truncate(self.length), .little);
        try buffer_writer.writeInt(u8, @intFromEnum(self.record_type), .little);
        try buffer_writer.writeAll(self.data);
    }

    pub fn decode(buffer: []const u8) !Record {
        const header_len = Record.header_size();
        var buffer_stream = std.io.fixedBufferStream(buffer);
        var buffer_reader = buffer_stream.reader();

        const checksum = try buffer_reader.readInt(u32, .little);
        const length = try buffer_reader.readInt(u16, .little);
        if (length == 0) {
            return error.InvalidRecord;
        }
        const record_type_int: u8 = try buffer_reader.readInt(u8, .little);
        const record_type: RecordType = std.meta.intToEnum(RecordType, record_type_int) catch {
            return error.InvalidRecord;
        };
        const data = buffer[header_len..(header_len + length)];

        return Record{
            .checksum = checksum,
            .length = length,
            .record_type = record_type,
            .data = data,
        };
    }
};

pub const WriteAheadLog = struct {
    //       +-----+-------------+--+----+----------+------+-- ... ----+
    // File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
    //       +-----+-------------+--+----+----------+------+-- ... ----+
    //       <--- block_size ------>|<-- block_size ------>|

    // rn = variable size records
    // P = Padding
    allocator: *const Allocator,
    fd: fs.File,
    block_size: usize,

    pub fn init(allocator: *const Allocator, options: WalOptions) !WriteAheadLog {
        const fd = try options.file_dir.createFile(
            options.file_name,
            .{ .read = true, .truncate = false },
        );

        std.debug.assert(options.block_size > Record.header_size());

        return WriteAheadLog{
            .allocator = allocator,
            .fd = fd,
            .block_size = options.block_size,
        };
    }

    pub fn deinit(self: *WriteAheadLog) void {
        self.fd.close();
    }

    // Reads a block from the file, at current position and decodes it into a list of records
    pub fn readBlock(self: *WriteAheadLog) ![]Record {
        const buffer = self.allocator.alloc(u8, self.block_size) catch unreachable;
        defer self.allocator.free(buffer);
        _ = try self.fd.readAll(buffer);

        var decode_offset: usize = 0;
        var records = std.ArrayList(Record).init(self.allocator.*);
        errdefer records.deinit();
        while (decode_offset < self.block_size) {
            const record = Record.decode(buffer[decode_offset..]) catch {
                log.info("Failed to decode record at offset, hit block padding {d}", .{decode_offset});
                break;
            };
            records.append(record) catch unreachable;
            decode_offset += record.size();
        }

        return records.toOwnedSlice();
    }

    pub fn write(self: *WriteAheadLog, payload: []const u8) !void {
        const records = self.encode(payload);
        defer self.allocator.free(records);

        for (records) |record| {
            const curr_end_pos: usize = self.fd.getEndPos() catch unreachable;
            self.fd.seekFromEnd(0) catch unreachable;
            var last_block_space_left: usize = 0;
            if (curr_end_pos < self.block_size) {
                last_block_space_left = 0; // First block, no last block exists
            } else {
                last_block_space_left = self.lastBlockSpaceLeft();
            }

            const buffer = self.allocator.alloc(u8, record.size()) catch unreachable;
            defer self.allocator.free(buffer);
            @memset(buffer, 0);
            try record.encode(buffer);

            const fd_writer = self.fd.writer();

            if (record.size() <= last_block_space_left) {
                // We can fit the record in the last block
                self.fd.seekBy(-@as(i64, @intCast(last_block_space_left))) catch unreachable;
                try fd_writer.writeAll(buffer);
                try fd_writer.writeByteNTimes(0, last_block_space_left - record.size());
            } else {
                // We need to write to a new block
                try fd_writer.writeAll(buffer);
                try fd_writer.writeByteNTimes(0, self.block_size - record.size());
            }
        }
    }

    fn lastBlockSpaceLeft(self: *WriteAheadLog) usize {
        const seek_back: i64 = -@as(i64, @intCast(self.block_size));
        const curr_pos = self.fd.getPos() catch unreachable;

        self.fd.seekFromEnd(seek_back) catch unreachable;
        const last_block_records = self.readBlock() catch |err| {
            log.err("Error reading last block: {s}\n{?}", .{ @errorName(err), @errorReturnTrace() });
            return 0;
        };
        defer self.allocator.free(last_block_records);
        var last_block_offset: usize = 0;
        for (last_block_records) |record| {
            last_block_offset += record.size();
        }
        self.fd.seekTo(curr_pos) catch unreachable;
        return self.block_size - last_block_offset;
    }

    fn encode(self: *WriteAheadLog, payload: []const u8) []Record {
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
};

test "Record encode/decode" {
    const allocator = std.testing.allocator;
    const record = Record{
        .checksum = 0x12345678,
        .length = 5,
        .record_type = .FULL,
        .data = "hello",
    };

    const buffer = try allocator.alloc(u8, record.size());
    defer allocator.free(buffer);
    try record.encode(buffer);

    const decoded_record = try Record.decode(buffer);
    try testing.expect(std.mem.eql(u8, record.data, decoded_record.data));
    try testing.expectEqual(record.checksum, decoded_record.checksum);
    try testing.expectEqual(record.length, decoded_record.length);
    try testing.expectEqual(record.record_type, decoded_record.record_type);
}

test "WriteAheadLog write" {
    fs.cwd().deleteFile("test.wal") catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };

    const block_size = 32 * @sizeOf(u8);

    const allocator = std.testing.allocator;
    var wal = try WriteAheadLog.init(&allocator, .{
        .file_dir = fs.cwd(),
        .file_name = "test.wal",
        .block_size = block_size,
    });
    defer wal.deinit();

    try wal.write("hello, world-1");
    try wal.write("hello, world-2");
    try wal.write("hello, world-3");
    try wal.write("hel0");
    try wal.write("hello, world");
    try wal.write("hello, world-6");
    try wal.write("hel1");

    const stat = try wal.fd.stat();
    try testing.expectEqual(stat.size, block_size * 5);
}
