const std = @import("std");
const writer = std.debug.print;
const log = std.log;

const Level = enum(u3) {
    debug = 0,
    info = 1,
    warn = 2,
    err = 3,
    crit = 4,
};
