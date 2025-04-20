const std = @import("std");

const AnsiCodes = enum {
    // ANSI Escape Sequences ref: https://gist.github.com/fnky/458719343aabd01cfb17a3a4f7296797
    reset,
    bold,
    bold_reset,
    red_fg,
    green_fg,
    yellow_fg,
    default_fg,

    //TODO: Codes can be joined in a sinnge esape seq like, ESC[1;31;{...}m
    pub const AnsiCodeTable = [@typeInfo(AnsiCodes).@"enum".fields.len][:0]const u8{
        "\x1b[0m",
        "\x1b[1m",
        "\x1b[22m",
        "\x1b[31m",
        "\x1b[32m",
        "\x1b[33m",
        "\x1b[39m",
    };

    pub fn str(self: AnsiCodes) [:0]const u8 {
        return AnsiCodeTable[@intFromEnum(self)];
    }
};

pub fn ansiLogFn(
    comptime level: std.log.Level,
    comptime scope: @Type(.enum_literal),
    comptime format: []const u8,
    args: anytype,
) void {
    const level_text = comptime level.asText();
    const prefix = if (scope == .default) ": " else "(" ++ @tagName(scope) ++ "): ";
    const color_seq: AnsiCodes = switch (level) {
        .debug => .default_fg,
        .err => .red_fg,
        .warn => .yellow_fg,
        .info => .green_fg,
    };

    const stderr = std.io.getStdErr().writer();
    var buffered_stderr = std.io.bufferedWriter(stderr);
    const buffered_writer = buffered_stderr.writer();

    std.debug.lockStdErr();
    defer std.debug.unlockStdErr();
    nosuspend {
        buffered_writer.print(
            AnsiCodes.bold.str() ++ color_seq.str() ++ level_text ++ prefix ++ AnsiCodes.reset.str() ++
                format ++ "\n",
            args,
        ) catch return;
        buffered_stderr.flush() catch return;
    }
}
