// const std = @import("std");
// const lib = @import("lib.zig");
// const Buffer = @import("buffer").Buffer;
//
// const proto = lib.proto;
// const Conn = lib.Conn;
// const Reader = lib.Reader;
// const NotificationResponse = lib.proto.NotificationResponse;
//
// const Stream = lib.Stream;
// const Allocator = std.mem.Allocator;
// const Io = std.Io;
//
// const ListenError = union(enum) {
//     err: anyerror,
//     pg: lib.proto.Error,
// };
//
// pub const Listener = struct {
//     err: ?ListenError = null,
//     closed: bool = false,
//
//     stream: Stream,
//
//     // A buffer used for writing to PG. This can grow dynamically as needed.
//     buf: Buffer,
//
//     // Used to read data from PG. Has its own buffer which can grow dynamically
//     reader: Reader,
//
//     // If we get a PG error, we'll return a LIstenError.pg, and we'll own its
//     // memory.
//     err_data: ?[]const u8 = null,
//
//     allocator: Allocator,
//
//     io: Io,
//
//     pub fn open(io: Io, allocator: Allocator, opts: Conn.Opts) !Listener {
//         var stream = try Stream.connect(io, allocator, opts, null);
//         errdefer stream.close();
//
//         const buf = try Buffer.init(allocator, opts.write_buffer orelse 2048);
//         errdefer buf.deinit();
//
//         const reader = try Reader.init(allocator, opts.read_buffer orelse 4096, stream);
//         errdefer reader.deinit();
//
//         return .{
//             .buf = buf,
//             .stream = stream,
//             .reader = reader,
//             .allocator = allocator,
//             .io = io,
//         };
//     }
//
//     pub fn deinit(self: *Listener) void {
//         if (self.err_data) |err_data| {
//             self.allocator.free(err_data);
//         }
//         self.buf.deinit();
//         self.reader.deinit();
//
//         self.stop() catch {};
//         self.stream.close();
//     }
//
//     pub fn stop(self: *Listener) !void {
//         if (@atomicRmw(bool, &self.closed, .Xchg, true, .monotonic) == true) {
//             return;
//         }
//
//         lib.sendTerminate(&self.stream, self.io);
//         return self.stream.shutdown(.both);
//     }
//
//     pub fn auth(self: *Listener, opts: Conn.AuthOpts) !void {
//         if (try lib.auth.auth(self.io, &self.stream, &self.buf, &self.reader, opts)) |raw_pg_err| {
//             return self.setErr(raw_pg_err);
//         }
//
//         while (true) {
//             const msg = try self.read();
//             switch (msg.type) {
//                 'Z' => return,
//                 'K' => {}, // TODO: BackendKeyData
//                 'S' => {}, // TODO: ParameterStatus,
//                 else => return error.UnexpectedDBMessage,
//             }
//         }
//     }
//
//     pub fn next(self: *Listener) ?NotificationResponse {
//         if (@atomicLoad(bool, &self.closed, .acquire) == true) {
//             return null;
//         }
//
//         const msg = self.read() catch |err| {
//             self.err = .{ .err = err };
//             return null;
//         };
//
//         switch (msg.type) {
//             'A' => return NotificationResponse.parse(msg.data) catch |err| {
//                 self.err = .{ .err = err };
//                 return null;
//             },
//             else => {
//                 self.err = .{ .err = error.UnexpectedDBMessage };
//                 return null;
//             },
//         }
//     }
//
//     fn read(self: *Listener) !lib.Message {
//         var reader = &self.reader;
//         while (true) {
//             const msg = try reader.next();
//             switch (msg.type) {
//                 'N' => {}, // TODO: NoticeResponse
//                 'E' => return self.setErr(msg.data),
//                 else => return msg,
//             }
//         }
//     }
//
//     fn setErr(self: *Listener, data: []const u8) error{ PG, OutOfMemory } {
//         const allocator = self.allocator;
//
//         // The proto.Error that we're about to create is going to reference data.
//         // But data is owned by our Reader and its lifetime doesn't necessarily match
//         // what we want here. So we're going to dupe it and make the connection own
//         // the data so it can tie its lifecycle to the error.
//
//         // That means clearing out any previous duped error data we had
//         if (self.err_data) |err_data| {
//             allocator.free(err_data);
//         }
//
//         const owned = try allocator.dupe(u8, data);
//         self.err_data = owned;
//         self.err = .{ .pg = proto.Error.parse(owned) };
//         return error.PG;
//     }
// };
//
// const t = lib.testing;
//
// fn testNotifier() !void {
//     var c = try t.connect(.{});
//     defer c.deinit();
//     _ = c.exec("select pg_notify($1, $2)", .{ "chan_x", "pl-x" }) catch unreachable;
//     _ = c.exec("select pg_notify($1, $2)", .{ "chan-1", "pl-1" }) catch unreachable;
//     _ = c.exec("select pg_notify($1, $2)", .{ "chan_2", "pl-2" }) catch unreachable;
//     _ = c.exec("select pg_notify($1, null)", .{"chan-1"}) catch unreachable;
// }
