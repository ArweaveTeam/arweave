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

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
long_description() -> {ok, [
	"When enabled, debug mode will increase the verbosity level",
	"of the application."
]}.

%%--------------------------------------------------------------------
%% if returns true, then the environment variable is generated from
%% the configuration key and prefixed with "AR":
%%   AR_GLOBAL_DEBUG
%% it can also be overwritten.
%%--------------------------------------------------------------------
environment() -> {ok, <<"AR_DEBUG">>}.

%%--------------------------------------------------------------------
%% not defined by default. It should return a positive integer, in the
%% printable ASCII range (e.g. a-z, A-Z and 0-9).
%%--------------------------------------------------------------------
short_argument() -> {ok, $d}.

%%--------------------------------------------------------------------
%% should convert the configuration key by default, like that:
%%   [global, debug] will be come --global.debug
%% but it can be overwritten using long_argument parameter.
%%--------------------------------------------------------------------
long_argument() -> {ok, [debug]}.

%%--------------------------------------------------------------------
%% define the numbers of elements to take after the short or long
%% argument. If it's a flag (default), it's set to 0.
%%--------------------------------------------------------------------
elements() -> {ok, 0}.

%%--------------------------------------------------------------------
%% check if the key/value passed are valid (or not).
%%--------------------------------------------------------------------
check(_Key, Value) when is_boolean(Value) -> ok;
check(_, _) -> {error, bad_value}.

%%--------------------------------------------------------------------
%% @doc Returns debug value.
%% @end
%%--------------------------------------------------------------------
handle_get(_Key) ->
	Value = arweave_config_store:get([global, debug]),
	{ok, Value}.

%%--------------------------------------------------------------------
%% @doc Configure the node in debug mode.
%% @end 
%%--------------------------------------------------------------------
handle_set(_Key, Value) ->
	logger:set_module_level(arweave_config, Value),
	logger:set_module_level(arweave, Value),
	arewave_config_store:set([global, debug], Value),
	{ok, Value}.
