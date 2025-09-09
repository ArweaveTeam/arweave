%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_global_debug).
-behavior(arweave_config_spec).
-export([
	deprecated/0,
	type/0,
	default/0,
	required/0,
	legacy/0,
	runtime/0,
	short_description/0,
	long_description/0,
	configuration_key/0,
	environment/0,
	short_argument/0,
	long_argument/0,
	elements/0,
	check/2,
	handle_get/1,
	handle_set/2
]).

configuration_key() -> {ok, [global,debug]}.

runtime() -> true.

deprecated() -> false.

type() -> {ok, boolean}.

default() -> {ok, false}.

required() -> {ok, false}.

legacy() -> {ok, [debug]}.

short_description() -> {ok, [
	<<"Enable debug mode.">>
]}.

long_description() -> {ok, [
	"When enabled, debug mode will increase the verbosity level",
	"of the application."
]}.


environment() -> {ok, "AR_DEBUG"}.

short_argument() -> {ok, $d}.

long_argument() -> {ok, [debug]}.

elements() -> {ok, 0}.

check(_Key, Value) when is_boolean(Value) -> ok;
check(_, _) -> {error, bad_value}.

handle_get(_Key) ->
	{ok, value}.

handle_set(_Key, Value) ->
	% 1. set otp debug mode
	% 2. in case of success, update the value in the
	%    configuration store.
	{ok, Value}.
