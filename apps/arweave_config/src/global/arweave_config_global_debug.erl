%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_global_debug).
-behavior(arweave_config_spec).
-compile(export_all).

default() -> {ok, false}.

required() -> {ok, false}.

legacy() -> {ok, [debug]}.

runtime() -> {ok, false}.

short_description() -> {ok, [
	<<"Enable debug mode.">>
]}.

long_description() -> {ok, [
	"When enabled, debug mode will increase the verbosity level",
	"of the application."
]}.

configuration_key() -> {ok, [global,debug]}.

environment() -> {ok, "AR_DEBUG"}.

short_argument() -> {ok, $d}.

long_argument() -> {ok, [debug]}.

elements() -> {ok, 0}.

handle_get(Key) -> {ok, value};

handle_set(Key, Value) ->
	% 1. set otp debug mode
	% 2. in case of success, update the value in the
	%    configuration store.
	{ok, Value}.
