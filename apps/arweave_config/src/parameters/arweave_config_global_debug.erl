%%%===================================================================
%%% @doc
%%%
%%% Arweave Debug Mode Parameter.
%%%
%%% ```
%%% # print usage and help about this flag
%%% ./bin/arweave help -d
%%% ./bin/arweave help --global.debug
%%% ./bin/arweave help AR_DEBUG
%%%
%%% # dynamic help
%%% ./bin/arweave config help global.debug
%%%
%%% # configure debug mode from environment variable
%%% export AR_DEBUG=true
%%%
%%% # configure debug mode via short argument
%%% ./bin/arweave -d
%%%
%%% # configure debug mode via long argument
%%% ./bin/arweave --global.debug
%%%
%%% # configure dynamically debug mode during runtime
%%% ./bin/arweave config get global.debug
%%% ./bin/arweave config set global.debug true
%%% ./bin/arweave config set global.debug false
%%%
%%% # configure dynamically debug mode using webui
%%% curl -X POST ${TARGET}/v1/config/global/debug -d'true'
%%% '''
%%%
%%% @end
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
	handle_set/3
]).
-include("arweave_config.hrl").
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
configuration_key() -> [global,debug].

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
runtime() -> true.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
deprecated() -> false.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
type() -> boolean.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
default() -> ?DEFAULT_GLOBAL_DEBUG.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
required() -> false.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
legacy() -> debug.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
short_description() -> 
	[<<"Enable debug mode.">>].

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
long_description() -> [
	"When enabled, debug mode will increase the verbosity level",
	"of the application."
].

%%--------------------------------------------------------------------
%% if returns true, then the environment variable is generated from
%% the configuration key and prefixed with "AR":
%%   AR_GLOBAL_DEBUG
%% it can also be overwritten.
%%--------------------------------------------------------------------
environment() -> <<"AR_DEBUG">>.

%%--------------------------------------------------------------------
%% not defined by default. It should return a positive integer, in the
%% printable ASCII range (e.g. a-z, A-Z and 0-9).
%%--------------------------------------------------------------------
short_argument() -> $d.

%%--------------------------------------------------------------------
%% should convert the configuration key by default, like that:
%%   [global, debug] will be come --global.debug
%% but it can be overwritten using long_argument parameter.
%%--------------------------------------------------------------------
long_argument() -> [global,debug].

%%--------------------------------------------------------------------
%% define the numbers of elements to take after the short or long
%% argument. If it's a flag (default), it's set to 0.
%%--------------------------------------------------------------------
elements() -> 0.

%%--------------------------------------------------------------------
%% check if the key/value passed are valid (or not).
%%--------------------------------------------------------------------
check(Key, Value) when is_list(Value) ->
	check(Key, list_to_binary(Value));
check(_Key, Value) when is_binary(Value) ->
	try erlang:binary_to_existing_atom(Value) of
		true -> ok;
		false -> ok;
		_ -> {error, bad_value}
	catch
		_:_ ->
			{error, bad_value}
	end;
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
handle_set(Param, X, Y) when is_binary(X) ->
	handle_set(Param, binary_to_existing_atom(X), Y);
handle_set(Param, X, Y) when is_binary(Y) ->
	handle_set(Param, X, binary_to_existing_atom(Y));
handle_set(Param, true, _) ->
	?LOG_INFO("enable ~p", [Param]),
	logger:set_module_level(arweave_config, debug),
	logger:set_module_level(arweave, debug),
	{store, true};
handle_set(Param, false, _) ->
	?LOG_INFO("disable ~p", [Param]),
	logger:set_module_level(arweave_config, info),
	logger:set_module_level(arweave, info),
	{store, false}.
