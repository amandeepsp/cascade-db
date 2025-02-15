const std = @import("std");
const msgpack = @import("msgpack");
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

    pub fn encode(self: Event, allocator: Allocator) ![]u8 {
        var buffer = std.ArrayList(u8).init(allocator);
        const writer = buffer.writer();

        try msgpack.encode(self, writer);
        return buffer.toOwnedSlice();
    }

    pub fn decode(buffer: []const u8, allocator: Allocator) !msgpack.Decoded(Event) {
        var stream = std.io.fixedBufferStream(buffer);
        const reader = stream.reader();

        //TODO: Maybe replace with decodeLeaky with an arena allocator, when multiple events are decoded
        return try msgpack.decode(Event, allocator, reader);
    }
};

test "Event encode/decode" {
    const allocator = std.testing.allocator;
    const write_event = Event{ .write = .{ .key = "hello", .value = "world" } };
    const buffer = try write_event.encode(allocator);
    defer allocator.free(buffer);
    const decoded_event = try Event.decode(buffer, allocator);
    defer decoded_event.deinit();
    try testing.expect(mem.eql(u8, write_event.write.key, decoded_event.value.write.key));
    try testing.expect(mem.eql(u8, write_event.write.value, decoded_event.value.write.value));

    const delete_event = Event{ .delete = .{ .key = "hello" } };
    const delete_event_buffer = try delete_event.encode(allocator);
    defer allocator.free(delete_event_buffer);
    const decoded_delete_event = try Event.decode(delete_event_buffer, allocator);
    defer decoded_delete_event.deinit();
    try testing.expect(mem.eql(u8, delete_event.delete.key, decoded_delete_event.value.delete.key));
}

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
