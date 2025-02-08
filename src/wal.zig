const std = @import("std");
const Allocator = std.mem.Allocator;
const fs = std.fs;

// WAL format adapted form https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format

pub const WalOptions = struct {
    file_dir: fs.Dir = fs.cwd(),
    file_name: []const u8 = "wal.log",
    max_block_size: usize = 32 * 2 * @sizeOf(u8),
};

// Since max_block_size is fixed, a record could get broken into multiple blocks,
// to reconstruct, we need info where does the record start and end.
const RecordType = enum(u8) {
    FULL = 1,
    FIRST = 2,
    MIDDLE = 3,
    LAST = 4,
};

const Record = struct {
    checksum: u32,
    length: usize,
    type: RecordType,
    data: []const u8,

    pub fn size(self: *const Record) usize {
        return @sizeOf(u32) + @sizeOf(usize) + @sizeOf(RecordType) + self.length;
    }

    pub fn encode(self: *const Record, buffer: []u8) !void {
        var buffer_stream = std.io.fixedBufferStream(buffer);
        var buffer_writer = buffer_stream.writer();

        try buffer_writer.writeInt(u32, self.checksum, .little);
        try buffer_writer.writeInt(usize, self.length, .little);
        try buffer_writer.writeInt(u8, @intFromEnum(self.type), .little);
        try buffer_writer.writeAll(self.data);
    }
};

pub const WriteAheadLog = struct {
    allocator: *const Allocator,
    fd: fs.File,
    max_block_size: usize,

    pub fn init(allocator: *const Allocator, options: WalOptions) !WriteAheadLog {
        const fd = try options.file_dir.createFile(options.file_name, .{ .truncate = false });

        return WriteAheadLog{
            .allocator = allocator,
            .fd = fd,
            .max_block_size = options.max_block_size,
        };
    }

    pub fn deinit(self: *WriteAheadLog) void {
        self.fd.close();
    }

    pub fn write(self: *WriteAheadLog, payload: []const u8) !void {
        const encoded_bytes = self.encode(payload);
        defer self.allocator.free(encoded_bytes);
        try self.fd.seekFromEnd(0);
        try self.fd.writeAll(encoded_bytes);
    }

    fn encode(self: *WriteAheadLog, payload: []const u8) []const u8 {
        const length = payload.len;
        const header_length = @sizeOf(u32) + @sizeOf(u16) + @sizeOf(u8);

        if (length + header_length > self.max_block_size) {
            var offset: usize = 0;
            const num_records = length / (self.max_block_size - header_length) + 1;
            const record_length = self.max_block_size - header_length;

            const buffer = self.allocator.alloc(u8, num_records * self.max_block_size) catch unreachable;
            @memset(buffer, 0);

            while (offset < payload.len) {
                var curr_payload: []const u8 = undefined;
                if (offset + record_length >= payload.len) {
                    curr_payload = payload[offset..];
                } else {
                    curr_payload = payload[offset..(offset + record_length)];
                }

                const curr_checksum = std.hash.Crc32.hash(curr_payload);
                var curr_type: RecordType = undefined;
                if (offset == 0) {
                    curr_type = .FIRST;
                } else if (offset + record_length >= payload.len) {
                    curr_type = .LAST;
                } else {
                    curr_type = .MIDDLE;
                }

                const curr_record = Record{
                    .checksum = curr_checksum,
                    .length = curr_payload.len,
                    .type = curr_type,
                    .data = curr_payload,
                };

                curr_record.encode(buffer[offset..]) catch unreachable;
                offset += record_length;
            }

            return buffer;
        } else {
            const checksum = std.hash.Crc32.hash(payload);
            const record = Record{
                .checksum = checksum,
                .length = payload.len,
                .type = .FULL,
                .data = payload,
            };

            const buffer = self.allocator.alloc(u8, self.max_block_size) catch unreachable;
            @memset(buffer, 0);
            record.encode(buffer) catch unreachable;
            return buffer;
        }
    }
};
