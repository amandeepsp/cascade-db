const std = @import("std");
const fs = std.fs;

pub fn openDir(path: []const u8) !fs.Dir {
    if (fs.path.isAbsolute(path)) {
        return fs.openDirAbsolute(path, .{ .access_sub_paths = true, .iterate = true });
    } else {
        return fs.cwd().openDir(path, .{ .access_sub_paths = true, .iterate = true });
    }
}

pub fn makeDir(path: []const u8) !void {
    if (fs.path.isAbsolute(path)) {
        return fs.makeDirAbsolute(path);
    } else {
        return fs.cwd().makeDir(path);
    }
}

pub fn access(path: []const u8, flags: fs.File.OpenFlags) !void {
    if (fs.path.isAbsolute(path)) {
        return fs.accessAbsolute(path, flags);
    } else {
        return fs.cwd().access(path, flags);
    }
}
