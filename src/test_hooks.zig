const std = @import("std");
   const Io = std.Io;

   pub const transaction_received_marker =
       "TEST_HOOK transaction_received_before_enqueue";

   pub const TransactionReceivedBarrier = struct {
       armed: bool = true,

       pub fn reach(
           self: *@This(),
           io: Io,
           shutdown_event: *Io.Event,
       ) !void {
           if (!self.armed) return;
           self.armed = false;

           try Io.File.stdout().writeStreamingAll(
               io,
               transaction_received_marker ++ "\n",
           );

           try shutdown_event.wait(io);
       }
   };
