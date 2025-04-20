const std = @import("std");
const hash = std.hash;
const mem = std.mem;
const testing = std.testing;
const Allocator = std.mem.Allocator;

const EventType = enum(u8) {
    write = 1,
    delete = 2,
};

const WriteEvent = struct {
    key: []const u8,
    value: []const u8,
};

const DeleteEvent = struct {
    key: []const u8,
};

pub const Event = union(EventType) {
    write: WriteEvent,
    delete: DeleteEvent,

    // Serializes the event into a buffer, in little endian format.
    // +---------+-------------+------------+--- ... ---+-----------------+------...--------+
    // |event type (u8) | key len (u32) | key (n bytes) | value len (u32) | value (m bytes) |
    // +---------+-------------+------------+--- ... ---+-----------------+------...-------+

    pub fn serialize(self: Event, buffer: []u8) !void {
        var stream = std.io.fixedBufferStream(buffer);
        var writer = stream.writer();
        switch (self) {
            .write => |we| {
                try writer.writeInt(u8, @intFromEnum(EventType.write), .little);
                try writer.writeInt(u32, @intCast(we.key.len), .little);
                try writer.writeAll(we.key);
                try writer.writeInt(u32, @intCast(we.value.len), .little);
                try writer.writeAll(we.value);
            },
            .delete => |de| {
                try writer.writeInt(u8, @intFromEnum(EventType.delete), .little);
                try writer.writeInt(u32, @intCast(de.key.len), .little);
                try writer.writeAll(de.key);
            },
        }
    }

    pub fn size(self: Event) usize {
        switch (self) {
            .write => |we| {
                return @sizeOf(u8) + @sizeOf(u32) + we.key.len + @sizeOf(u32) + we.value.len;
            },
            .delete => |de| {
                return @sizeOf(u8) + @sizeOf(u32) + de.key.len;
            },
        }
    }

    pub fn deserialize(buffer: []const u8) !Event {
        var stream = std.io.fixedBufferStream(buffer);
        var reader = stream.reader();

        const tag_val = try reader.readInt(u8, .little);
        const event_type = std.meta.intToEnum(EventType, tag_val) catch {
            return error.InvalidEvent;
        };
        var offset: usize = @sizeOf(u8);

        // Read key length and data
        const key_len: u32 = try reader.readInt(u32, .little);
        offset += @sizeOf(u32);
        const key = buffer[offset .. offset + key_len];
        offset += @intCast(key_len);

        if (event_type == EventType.write) {
            const value_len: u32 = try reader.readInt(u32, .little);
            offset += @sizeOf(u32);
            const value = buffer[offset .. offset + value_len];
            return Event{ .write = WriteEvent{ .key = key, .value = value } };
        } else {
            return Event{ .delete = DeleteEvent{ .key = key } };
        }
    }
};

// Since block_size is fixed, a record could get broken into multiple blocks,
// to reconstruct, we need info where does the record start and end.
pub const RecordType = enum(u8) {
    FULL = 1,
    FIRST = 2,
    MIDDLE = 3,
    LAST = 4,
};

pub const Record = struct {
    // +---------+-------------+------------+--- ... ---+
    // |CRC (u32) | Size (u16) | Type (u8) | Payload   |
    // +---------+-------------+------------+--- ... ---+
    // CRC = 32bit hash computed over the payload using CRC, of payload and type
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
        const record_type_byte: u8 = @intFromEnum(record_type);
        var hasher = hash.Crc32.init();
        hasher.update(payload);
        hasher.update(&[_]u8{record_type_byte});
        const checksum = hasher.final();

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
    try testing.expect(mem.eql(u8, record.data, decoded_record.data));
    try testing.expectEqual(record.checksum, decoded_record.checksum);
    try testing.expectEqual(record.length, decoded_record.length);
    try testing.expectEqual(record.record_type, decoded_record.record_type);
}
