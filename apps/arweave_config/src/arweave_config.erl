%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Configuration Interface.
%%%
%%% `arweave_config' module is an interface to the Arweave
%%% configuration data store where all configuration parameters are
%%% stored and specified.
%%%
%%% WARNING: this module/application is in active development.
%%% @end
%%%===================================================================
-module(arweave_config).
-vsn(1).
-behavior(application).
-behavior(gen_server).
-export([
	get/1,
	get/2,
	get_env/0,
	is_runtime/0,
	runtime/0,
	set/2,
	set_env/1,
	start/0,
	start_link/0,
	stop/0
]).
% application behavior callbacks.
-export([start/2, stop/1]).
% gen_server behavior callbacks
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2]).
-compile({no_auto_import,[get/1]}).
-include("arweave_config.hrl").
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc helper function to started `arweave_config' application.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, term()}.

start() ->
	case application:ensure_all_started(?MODULE, permanent) of
		{ok, Dependencies} ->
			?LOG_DEBUG("arweave_config started dependencies: ~p", Dependencies),
			ok;
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc help function to stop `arweave_config' application.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.

stop() ->
	application:stop(?MODULE).

%%--------------------------------------------------------------------
%% @doc A wrapper for `application:get_env/2'.
%% @deprecated this function is a temporary interface and will be
%%             replaced by `arweave_config:get/1' function.
%% @see application:get_env/2
%% @end
%%--------------------------------------------------------------------
-spec get_env() -> {ok, #config{}}.

get_env() ->
	arweave_config_legacy:get_env().

%%--------------------------------------------------------------------
%% @doc A wrapper for `application:set_env/3'.
%% @deprecated this function is a temporary interface and will be
%%             replaced by `arweave_config:set/2' function.
%% @see application:set_env/3
%% @end
%%--------------------------------------------------------------------
-spec set_env(term()) -> ok.

set_env(Value) ->
	arweave_config_legacy:set_env(Value).

%%--------------------------------------------------------------------
%% @doc Get a value from the configuration.
%%
%% Note: the behavior of this function is not the same depending of
%% the kind of parameter desired. Indeed, to help the transition to
%% the new configuration format, when an `atom' is set as first
%% argument,   `arweave_config'  will   act  as   proxy  to   the  old
%% configuration method (using a record).
%%
%% == Examples ==
%%
%% ```
%% > get(<<"global.debug">>).
%% {ok, false}
%%
%% > get([global, debug]).
%% {ok, false}
%%
%% > get([test]).
%% {error, #{ reason => not_found }}.
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec get(ParameterKey) -> Return when
	ParameterKey :: atom() | string() | binary() | list(),
	Return :: {ok, term()} | {error, term()}.

get(Key) when is_atom(Key) ->
	% TODO: pattern to remove.
	% this pattern is ONLY for legacy purpose, it should be
	% removed after the full migration to the new arweave
	% configuration format.
	?LOG_DEBUG([
		{function, ?FUNCTION_NAME},
		{module, ?MODULE},
		{key, Key}
	]),
	arweave_config_legacy:get(Key);
get(Key) ->
	case arweave_config_parser:key(Key) of
		{ok, Parameter} ->
			case arweave_config_store:get(Parameter) of
				{ok, Value} ->
					{ok, Value};
				_Elsewise ->
					arweave_config_spec:get_default(Parameter)
			end;
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc Get a value from the  configuration, if not defined, a default
%% value can be returned instead.
%%
%% == Examples ==
%%
%% ```
%% > get(<<"global.debug">>, true).
%% false
%%
%% > get([global, debug], true).
%% false
%%
%% > get([test], true).
%% true
%% '''
%% @end
%%--------------------------------------------------------------------
-spec get(ParameterKey, Default) -> Return when
	ParameterKey :: atom() | string() | binary() | list(),
	Default :: term(),
	Return :: term().

get(Key, Default) ->
	try get(Key) of
		{ok, Value} ->
			Value;
		_Else ->
			Default
	catch
		_:_ -> Default
	end.

%%--------------------------------------------------------------------
%% @doc Set a configuration value using a key.
%%
%% == Examples==
%%
%% ```
%% > set(<<"global.debug">>, <<"true">>).
%% {ok, true}
%%
%% > set([global, debug]), true).
%% {ok, true}
%%
%% > set("global.debug", "true").
%% {ok, true}
%%
%% > set("global.debug", 1234).
%% {error, #{ reason => not_boolean }}
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec set(ParameterKey, Value) -> Return when
	ParameterKey :: atom() | string() | iolist() | binary() | list(),
	Value :: term(),
	Return :: {ok, term()} | {error, term()}.

set(Key, Value) when is_atom(Key) ->
	% TODO: pattern to remove.
	% this pattern is ONLY for legacy purpose and should be
	% removed after the migration to the new arweave configuration
	% format.
	?LOG_DEBUG([
		{function, ?FUNCTION_NAME},
		{module, ?MODULE},
		{key, Key},
		{value, Value}
	]),
	arweave_config_legacy:set(Key, Value);
set(Key, Value) ->
	case arweave_config_parser:key(Key) of
		{ok, Parameter} ->
			arweave_config_spec:set(Parameter, Value);
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc Start arweave_config process.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc Switch to runtime mode. No rollback is possible there, this is
%% a one time operation to announce arweave config is ready to deal
%% with dynamic configuration.
%% @end
%%--------------------------------------------------------------------
-spec runtime() -> ok.

runtime() ->
	gen_server:call(?MODULE, runtime, 10_000).

%%--------------------------------------------------------------------
%% @doc Returns if arweave config is in runtime mode or not.
%% @end
%%--------------------------------------------------------------------
-spec is_runtime() -> boolean().

is_runtime() ->
	case ets:lookup(?MODULE, runtime) of
		[{runtime, true}] -> true;
		_Elsewise -> false
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc `gen_server' callback.
%% @end
%%--------------------------------------------------------------------
init(_) ->
	ets:new(?MODULE, [named_table, protected]),
	{ok, ?MODULE}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc `gen_server' callback.
%% @end
%%--------------------------------------------------------------------
terminate(_, _) ->
	?LOG_INFO("arweave_config process stopped").

%%--------------------------------------------------------------------
%% @hidden
%% @doc `gen_server' callback.
%% @end
%%--------------------------------------------------------------------
handle_call(runtime, _From, State) ->
	try
		ets:insert(?MODULE, {runtime, true})
	of
		true -> ok;
		_ -> ok
	catch
		_:_ -> ok
	end,
	{reply, ok, State};
handle_call(_, _, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc `gen_server' callback.
%% @end
%%--------------------------------------------------------------------
handle_cast(_, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc `gen_server' callback.
%% @end
%%--------------------------------------------------------------------
handle_info(_, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc `application' callback.
%% @end
%%--------------------------------------------------------------------
start(_StartType, _StartArgs) ->
	?LOG_INFO("arweave_config application starting"),

	% start application supervisor
	arweave_config_sup:start_link().

%%--------------------------------------------------------------------
%% @hidden
%% @doc `application' callback.
%% @end
%%--------------------------------------------------------------------
stop(_Args) ->
	?LOG_INFO("arweave_config application stopped"),
	ok.

