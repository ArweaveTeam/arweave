%%%===================================================================
%%% @doc temporary interface to arweave legacy configuration.
%%%
%%% This  module is  mainly used  as a  process to  deal with  arweave
%%% legacy configuration. Indeed, the previous implementation was
%%% using a record to store parameters as record's key, unfortunately,
%%% this is not flexible enough to do everything. This process is a
%%% direct interface to `application:set_env(arweave, config, _)'
%%% function and to `#config{}' record.
%%%
%%% The record needs to be converted as proplists, then, it will
%%% introduce a slower answers, but at this time, the configuration is
%%% not dynamic at all, this means this performance issue will only
%%% impact arweave during startup.
%%%
%%% ```
%%%  _________     ______________________     _______________________
%%% |         |   |                      |   |                       |
%%% | arweave |-->| arweave_config:set/2 |-->| arweave_config_legacy |
%%% |_________|   |______________________|   |_______________________|
%%%                                             ||
%%%                                            _||_
%%%                                            \  /
%%%                                           __\/_________________
%%%                                          |                     |
%%%                                          | application:set_env |
%%%                                          |_____________________|
%%%
%%%
%%% '''
%%%
%%% @TODO TO REMOVE when legacy configuration will be dropped.
%%%
%%% == Examples ==
%%%
%%% @end
%%%===================================================================
-module(arweave_config_legacy).
-behavior(gen_server).
-export([start_link/0, stop/0]).
-export([
	router/0,
	route/2,
	keys/0,
	has_key/1,
	get/0,
	get/1,
	set/2,
	reset/0,
	import/0,
	import/1,
	export/0
]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_info/2, handle_cast/2]).
-export([config_to_proplist/1, proplist_to_config/1]).
-compile({no_auto_import,[get/0, get/1]}).
-include_lib("arweave/include/ar_config.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% @doc route legacy parameter to new parameter.
%% @TODO TO REMOVE when legacy configuration will be dropped.
%% @TODO this function should return a map or a proplist from
%%       arweave_config_spec.
%% @end
%%--------------------------------------------------------------------
router() ->
	arweave_config_spec:get_legacy().

%%--------------------------------------------------------------------
%% @doc a simple router.
%% @TODO TO REMOVE when legacy configuration will be dropped.
%% @end
%%--------------------------------------------------------------------
route(LegacyKey, Value) ->
	Router = router(),
	case Router of
		#{ LegacyKey := Parameter } ->
			arweave_config_spec:set(Parameter, Value);
		_ ->
			ok
	end.

%%--------------------------------------------------------------------
%% @doc returns the complete list of all keys.
%% @end
%%--------------------------------------------------------------------
-spec keys() -> [atom()].

keys() ->
	gen_server:call(?MODULE, keys, 1000).

%%--------------------------------------------------------------------
%% @doc check if a key is present in the record.
%% @end
%%--------------------------------------------------------------------
-spec has_key(atom()) -> boolean().

has_key(Key) ->
	gen_server:call(?MODULE, {has_key, Key}, 1000).

%%--------------------------------------------------------------------
%% @doc Returns the whole configuration as record.
%% @end
%%--------------------------------------------------------------------
-spec get() -> Return when
	Return :: undefined | {ok, #config{}}.

get() ->
	try gen_server:call(?MODULE, get, 1000) of
		{ok, Value} -> Value;
		_Elsewise -> undefined
	catch
		_E:_R:_S -> undefined
	end.

%%--------------------------------------------------------------------
%% @doc Returns the value of a key.
%% @end
%%--------------------------------------------------------------------
-spec get(Key) -> Return when
	Key :: atom(),
	Return :: undefined | {ok, term()}.

get(Key) ->
	try gen_server:call(?MODULE, {get, Key}, 1000) of
		{ok, Value} -> Value;
		_Elsewise -> undefined
	catch
		_E:_R:_S -> undefined
	end.

%%--------------------------------------------------------------------
%% @doc Set a value to a key.
%% @end
%%--------------------------------------------------------------------
-spec set(Key, Value) -> Return when
	Key :: atom(),
	Value :: term(),
	Return :: {ok, Value} | error.

set(Key, Value) ->
	set(Key, Value, #{}).

%%--------------------------------------------------------------------
%% @doc Set a value to a key (custom options). When setting `set_env'
%% to `true', the environment application arweave/config is configured
%% with the content of the store from this process.
%%
%% Warning: this part is not protected against race condition, if
%% another process is setting application environment variable with
%% `application:set_env/2' function, the state present in this process
%% will not have the correct information.
%% @end
%%--------------------------------------------------------------------
-spec set(Key, Value, Opts) -> Return when
	Key :: atom(),
	Value :: term(),
	Opts :: #{ set_env => boolean() },
	Return :: {ok, Value} | error.

set(Key, Value, Opts) ->
	try gen_server:call(?MODULE, {set, Key, Value, Opts}, 1000) of
		{ok, NewValue, _OldValue} -> {ok, NewValue};
		_Elsewise -> error
	catch
		_E:_R:_S -> error
	end.

%%--------------------------------------------------------------------
%% @doc import configuration from `application:get(arweave,config)'.
%% @end
%%--------------------------------------------------------------------
-spec import() -> ok.

import() ->
	gen_server:cast(?MODULE, import).

%%--------------------------------------------------------------------
%% @doc import #config{} record and set it as new state.
%% @end
%%--------------------------------------------------------------------
import(Config) ->
	gen_server:cast(?MODULE, {import, Config}).

%%--------------------------------------------------------------------
%% @doc reset the legacy configuration by using the default values.
%% @end
%%--------------------------------------------------------------------
reset() ->
	gen_server:cast(?MODULE, reset).

%%--------------------------------------------------------------------
%% @doc export the current configuration as `#config{}' record.
%% @end
%%--------------------------------------------------------------------
-spec export() -> #config{}.

export() ->
	gen_server:call(?MODULE, export, 1000).

%%--------------------------------------------------------------------
%% @doc start `arweave_config_legacy' process.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc stop `arweave_config_legacy' process.
%% @end
%%--------------------------------------------------------------------
stop() ->
	gen_server:stop(?MODULE).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_) ->
	logger:set_module_level(?MODULE, debug),
	case application:get_env(arweave, config) of
		undefined ->
			Proplist = config_to_proplist(#config{}),
			?LOG_DEBUG([{configuration, Proplist}]),
			application:set_env(arweave, config, #config{}),
			{ok, Proplist};
		{ok, Config} when is_tuple(Config),
			element(1, Config) =:= config ->
				Proplist = config_to_proplist(Config),
				?LOG_DEBUG([{configuration, Proplist}]),
				application:set_env(arweave, config, #config{}),
				{ok, Proplist};
		_ ->
			{stop, configuration_error}
	end.

init_test() ->
	{ok, _Pid} = start_link(),
	?assertEqual(true, has_key(init)),
	?assertEqual(false, get(init)),
	set(init, true),
	?assertEqual(true, get(init)),
	{ok, C1} = application:get_env(arweave, config),
	?assertEqual(true, C1#config.init),
	?assertEqual(C1, get()),
	stop().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
terminate(_, _) ->
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(Msg = {has_key, Key}, From, State) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, proplists:is_defined(Key, State), State};
handle_call(Msg = keys, From, State) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, proplists:get_keys(State), State};
handle_call(Msg = get, From, State) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, {ok, proplist_to_config(State)}, State};
handle_call(Msg = {get, Key}, From, State)
	when is_atom(Key) ->
		?LOG_DEBUG([{message, Msg}, {from, From}]),
		Return = {ok, proplists:get_value(Key, State)},
		{reply, Return, State};
handle_call(Msg = {set, Key, Value, Opts}, From, State)
	when is_atom(Key), is_map(Opts) ->
		?LOG_DEBUG([{message, Msg}, {from, From}]),
		OldValue = proplists:get_value(Key, State),
		Return = {ok, Value, OldValue},
		NewState = lists:keyreplace(Key, 1, State, {Key, Value}),

		% temporary
		NewConfig = proplist_to_config(NewState),
		application:set_env(arweave, config, NewConfig),

		Fun = fun
			(set_env, true) ->
				NewConfig = proplist_to_config(NewState),
				application:set_env(arweave, config, NewConfig);
			(route, true) ->
				route(Key, Value);
			(_, _) ->
				 ignore
		end,
		try maps:map(Fun, Opts)
		catch _:_ -> ok
		end,
		{reply, Return, NewState};
handle_call(Msg = export, From, State) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	Return = proplist_to_config(State),
	{reply, Return, State};
handle_call(Message, From, State) ->
	Error = [
		{from, From},
		{message, Message},
		{from, From},
		{pid, self()}
	],
	?LOG_ERROR(Error),
	{reply, {error, Error}, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(Msg = import, State) ->
	?LOG_DEBUG([{message, Msg}]),
	case import_config() of
		{ok, NewState} ->
			% temporary
			NewConfig = proplist_to_config(NewState),
			application:set_env(arweave, config, NewConfig),
			{noreply, NewState};
		_ ->
			{noreply, State}
	end;
handle_cast(Msg = {import, Config}, State) ->
	case import_config(Config) of
		{ok, NewState} ->
			% temporary
			NewConfig = proplist_to_config(NewState),
			application:set_env(arweave, config, NewConfig),
			{noreply, NewState};
		_ ->
			{noreply, State}
	end;
handle_cast(Msg = reset, State) ->
	?LOG_DEBUG([{message, Msg}]),
	case reset_config() of
		{ok, NewState} ->
			{noreply, NewState};
		_ ->
			{noreply, State}
	end;
handle_cast(Msg, State) ->
	?LOG_ERROR("received: ~p", [Msg]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
	?LOG_ERROR("received: ~p", [Msg]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @doc Converts `#config{}' records to `proplists'.
%% @end
%%--------------------------------------------------------------------
config_to_proplist(Config)
	when is_tuple(Config), element(1, Config) =:= config ->
		Fields = record_info(fields, config),
		Values = erlang:delete_element(1, Config),
       		List = erlang:tuple_to_list(Values),
		lists:zip(Fields, List).

%%--------------------------------------------------------------------
%% @doc Converts a proplists to a `#config{}' record.
%% @end
%%--------------------------------------------------------------------
proplist_to_config(Proplist)
	when is_list(Proplist) ->
		Fields = record_info(fields, config),
		proplist_to_config2(Proplist, Fields, Proplist, 1).

% check the order of the fields, if not in right order, it will fail.
proplist_to_config2([], [], Proplist, _Pos) ->
	proplist_to_config3(Proplist);
proplist_to_config2([I={Key,_}|R1], [Key|R2], Proplist, Pos) ->
	proplist_to_config2(R1, R2, Proplist, Pos+1);
proplist_to_config2([{K1, _V1}|_R1], [K2|_R2], _, Pos) ->
	throw({error, #{
				reason => {badkey, K1, K2},
				position => Pos
			}
	      }
	);
proplist_to_config2(_, _, _, Pos) ->
	throw({error, #{
				reason => badvalue,
				position => Pos
			}
	      }
	).

proplist_to_config3(Proplist) ->
	Values = lists:map(fun({_,V}) -> V end, Proplist),
	Values2 = [config|Values],
	erlang:list_to_tuple(Values2).

% import config from application:get_env/2.
import_config() ->
	case application:get_env(arweave, config) of
		{ok, Config} when is_tuple(Config),
			element(1, Config) =:= config ->
				Proplist = config_to_proplist(Config),
				{ok, Proplist};
		Elsewise ->
			Elsewise
	end.

import_config(Config) when is_record(Config, config) ->
	Proplist = config_to_proplist(Config),
	{ok, Proplist};
import_config(Config) ->
	{error, Config}.

% reset internal configuration using #config{} record.
reset_config() ->
	Proplist = config_to_proplist(#config{}),
	{ok, Proplist}.
