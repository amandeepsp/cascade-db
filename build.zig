const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // dependencies
    const msgpack_dep = b.dependency("msgpack", .{
        .target = target,
        .optimize = optimize,
    });

    const exe = b.addExecutable(.{
        .name = "cascade",
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
    });

    exe.root_module.addImport("msgpack", msgpack_dep.module("msgpack"));

    b.installArtifact(exe);

    const tests = b.addTest(.{
        .root_source_file = b.path("src/tests.zig"),
        .target = target,
        .optimize = optimize,
    });
    
    tests.root_module.addImport("msgpack", msgpack_dep.module("msgpack"));

    const run_exe = b.addRunArtifact(exe);
    const run_step = b.step("run", "Run cascade-db");
    run_step.dependOn(&run_exe.step);

    const run_tests = b.addRunArtifact(tests);
    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_tests.step);
}
