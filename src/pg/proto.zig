pub const AuthenticationRequest = @import("proto/authentication_request.zig").AuthenticationRequest;
pub const AuthenticationSASLContinue = @import("proto/authentication_sasl_continue.zig");
pub const AuthenticationSASLFinal = @import("proto/authentication_sasl_final.zig");
pub const CommandComplete = @import("proto/command_complete.zig");
pub const Error = @import("proto/error.zig");
pub const PasswordMessage = @import("proto/password_message.zig");
pub const Query = @import("proto/query.zig");
pub const SASLInitialResponse = @import("proto/sasl_initial_response.zig");
pub const SASLResponse = @import("proto/sasl_response.zig");
pub const StartupMessage = @import("proto/startup_message.zig");

test {
    @import("std").testing.refAllDecls(@This());
}
