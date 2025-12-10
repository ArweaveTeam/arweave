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
%%% @deprecated This module is a temporary interface.
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
%%% @TODO TO REMOVE when legacy configuration will be dropped.
%%%
%%% == Examples ==
%%%
%%% ```
%%% % get the configuration as #config{} record from
%%% % arweave_config_legacy process state.Similar to
%%% % application:get_env/2
%%% {ok, #config{}} = arweave_config_legacy:get_env().
%%%
%%% % overwrite the configuration present in `arweave_config_legacy'
%%% % process state, similar to application:set_env/3.
%%% arweave_config_legacy:set_env(#config{}).
%%%
%%% % get value's key.
%%% Init = arweave_config_legacy:get(init).
%%%
%%% % set a value's key.
%%% arweave_config_legacy:set(init, false).
%%%
%%% % reset the configuration with the default state (default values
%%% % from `#config{}'.
%%% arweave_config_legacy:reset().
%%% '''
%%%
%%% @end
%%%===================================================================
-module(arweave_config_legacy).
-behavior(gen_server).
-export([start_link/0, stop/0]).
-export([
	keys/0,
	has_key/1,
	get/0,
	get/1,
	set/2,
	reset/0,
	set_env/1,
	get_env/0,
	get_config_value/2
]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_info/2, handle_cast/2]).
-export([config_to_proplist/1, proplist_to_config/1]).
-compile({no_auto_import,[get/0, get/1]}).
-include("arweave_config.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").
-record(?MODULE, {proplist, record}).

%%--------------------------------------------------------------------
%% @doc Returns the complete list of all keys from configuration
%% process state.
%% @end
%%--------------------------------------------------------------------
-spec keys() -> [atom()].

keys() ->
	gen_server:call(?MODULE, keys, 1000).

%%--------------------------------------------------------------------
%% @doc Check if a key is present in the process record state.
%% @end
%%--------------------------------------------------------------------
-spec has_key(atom()) -> boolean().

has_key(Key) when is_atom(Key) ->
	gen_server:call(?MODULE, {has_key, Key}, 1000).

%%--------------------------------------------------------------------
%% @doc Returns process state configuration as `#config{}' record.
%% @end
%%--------------------------------------------------------------------
-spec get() -> Return when
	Return :: undefined | {ok, #config{}}.

get() ->
	try gen_server:call(?MODULE, get, 1000) of
		{ok, Value} -> Value;
		_Elsewise -> undefined
	catch
		_E:R:S -> throw({error, {R, S}})
	end.

%%--------------------------------------------------------------------
%% @doc Returns the value of a key from process state configuration.
%% @end
%%--------------------------------------------------------------------
-spec get(Key) -> Return when
	Key :: atom(),
	Return :: undefined | term().

get(Key) when is_atom(Key) ->
	try gen_server:call(?MODULE, {get, Key}, 1000) of
		{ok, Value} -> Value;
		_Elsewise -> undefined
	catch
		_E:R:S -> throw({error, {R, S}})
	end.

%%--------------------------------------------------------------------
%% @doc Set a value to a key.
%% @end
%%--------------------------------------------------------------------
-spec set(Key, Value) -> Return when
	Key :: atom(),
	Value :: term(),
	Return :: {ok, Value} | {error, term()}.

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
	Return :: {ok, Value} | {error, term()}.

set(Key, Value, Opts) when is_atom(Key), is_map(Opts) ->
	try gen_server:call(?MODULE, {set, Key, Value, Opts}, 1000) of
		{ok, NewValue, _OldValue} -> {ok, NewValue};
		Elsewise -> {error, Elsewise}
	catch
		_E:R:S -> throw({error, {R, S}})
	end.

%%--------------------------------------------------------------------
%% @doc import #config{} record and set it as new state.
%% @end
%%--------------------------------------------------------------------
set_env(Config) when is_record(Config, config) ->
	gen_server:cast(?MODULE, {set_env, Config}).

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
-spec get_env() -> {ok, #config{}}.

get_env() ->
	gen_server:call(?MODULE, get_env, 1000).

%%--------------------------------------------------------------------
%% @doc start `arweave_config_legacy' process.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	?LOG_INFO("start ~p process (~p)", [?MODULE, self()]),
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
	Proplist = config_to_proplist(#config{}),
	?LOG_DEBUG([{configuration, Proplist}]),
	set_environment(Proplist),
	{ok, #?MODULE{
		proplist = Proplist,
		record = #config{}
	      }
	}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
terminate(_, _) ->
	?LOG_INFO("stop ~p process (~p)", [?MODULE, self()]),
	ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(Msg = {has_key, Key}, From, State = #?MODULE{ proplist = P }) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, proplists:is_defined(Key, P), State};
handle_call(Msg = keys, From, State = #?MODULE{ proplist = P }) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, [ K || {K,_} <- P ], State};
handle_call(Msg = get, From, State = #?MODULE{ record = R }) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, {ok, R}, State};
handle_call(Msg = {get, Key}, From, State = #?MODULE{ proplist = P })
	when is_atom(Key) ->
		?LOG_DEBUG([{message, Msg}, {from, From}]),
		Return = {ok, proplists:get_value(Key, P)},
		{reply, Return, State};
handle_call(Msg = {set, Key, Value, Opts}, From, State = #?MODULE{ proplist = P })
	when is_atom(Key), is_map(Opts) ->
		?LOG_DEBUG([{message, Msg}, {from, From}]),
		OldValue = proplists:get_value(Key, P),
		NewP = lists:keyreplace(Key, 1, P, {Key, Value}),
		set_environment(NewP),
		Return = {ok, Value, OldValue},
		NewState = State#?MODULE{
			proplist = NewP,
			record = proplist_to_config(NewP)
		},
		{reply, Return, NewState};
handle_call(Msg = get_env, From, State = #?MODULE{ record = R }) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, {ok, R}, State};
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
handle_cast(Msg = {set_env, Config}, State) ->
	?LOG_DEBUG([{message, Msg}]),
	case import_config(Config) of
		{ok, NewP} ->
			set_environment(NewP),
			NewState = State#?MODULE{
				proplist = NewP,
				record = proplist_to_config(NewP)
			},
			{noreply, NewState};
		_ ->
			{noreply, State}
	end;
handle_cast(Msg = reset, State) ->
	?LOG_DEBUG([{message, Msg}]),
	case reset_config() of
		{ok, NewP} ->
			NewState = State#?MODULE{
				proplist = NewP,
				record = proplist_to_config(NewP)
			},
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
-spec config_to_proplist(Config) -> Return when
	Config :: #config{},
	Return :: [{atom(), term()}].

config_to_proplist(Config)
	when is_record(Config, config) ->
		Fields = record_info(fields, config),
		Values = erlang:delete_element(1, Config),
       		List = erlang:tuple_to_list(Values),
		lists:zip(Fields, List).

config_to_proplist_test() ->
	Config = #config{},
	Keys = record_info(fields, config),
	Proplist = config_to_proplist(Config),
	[
		begin
			{ok, VC} = get_config_value(Key, Config),
			VP = proplists:get_value(Key, Proplist),
			?assertEqual(VC, VP)
		end
	||
		Key <- Keys
	].

%%--------------------------------------------------------------------
%% @doc Converts a proplists to a `#config{}' record.
%% @end
%%--------------------------------------------------------------------
-spec proplist_to_config(Proplist) -> Return when
	Proplist :: [{atom(), term()}],
	Return :: #config{}.

proplist_to_config(Proplist)
	when is_list(Proplist) ->
		Fields = record_info(fields, config),
		proplist_to_config2(Proplist, Fields, Proplist, 1).

proplist_to_config_test() ->
	Config = #config{},
	Proplist = config_to_proplist(Config),
	NewConfig = proplist_to_config(Proplist),
	[
		begin
			{ok, VC} = get_config_value(Key, NewConfig),
			VP = proplists:get_value(Key, Proplist),
			?assertEqual(VC, VP)
		end
	||
		Key <- proplists:get_keys(Proplist)
	].


%%--------------------------------------------------------------------
%% @hidden
%% private: check the order of the fields, if not in right order, it
%% will fail.
%%--------------------------------------------------------------------
proplist_to_config2([], [], Proplist, _Pos) ->
	proplist_to_config3(Proplist);
proplist_to_config2([{Key,_}|R1], [Key|R2], Proplist, Pos) ->
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

%%--------------------------------------------------------------------
%% @hidden
%% private: finally, convert the last values
%%--------------------------------------------------------------------
proplist_to_config3(Proplist) ->
	Values = lists:map(fun({_,V}) -> V end, Proplist),
	Values2 = [config|Values],
	erlang:list_to_tuple(Values2).

%%--------------------------------------------------------------------
%% @hidden
%% private: import a config record as proplist
%%--------------------------------------------------------------------
import_config(Config)
	when is_record(Config, config) ->
		Proplist = config_to_proplist(Config),
		{ok, Proplist};
import_config(Config) ->
	{error, Config}.

%%--------------------------------------------------------------------
%% @hidden
%% private: reset internal configuration using #config{} record.
%%--------------------------------------------------------------------
reset_config() ->
	Proplist = config_to_proplist(#config{}),
	{ok, Proplist}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc wrapper for `application:set_env/3'.
%% @end
%% @TODO: to  remove, arweave_legacy_config will  set the environment
%% for compatibility. If another application/module is still using
%% application:get_env, it will not be impacted and will have an
%% updated configuration.
%%--------------------------------------------------------------------
set_environment(Config) when is_list(Config) ->
	% convert the list to config record
	Record = proplist_to_config(Config),
	set_environment(Record);
set_environment(Config) when is_record(Config, config) ->
	application:set_env(arweave_config, config, Config).

%%--------------------------------------------------------------------
%% @hidden
%% @doc
%% helper function  to extract  config value using  a key,  similar to
%% `proplists:get_value/2'.
%% @end
%%--------------------------------------------------------------------
-spec get_config_value(Key, Config) -> Return when
	Key :: atom(),
	Config :: #config{},
	Return :: {error, undefined} | {ok, term()}.

get_config_value(Key, Config)
	when is_atom(Key), is_record(Config, config) ->
		Keys = record_info(fields, config),
		[_|List] = tuple_to_list(Config),
		Zip = lists:zip(Keys, List),
		case lists:keyfind(Key, 1, Zip) of
			false -> {error, undefined};
			{Key, Value} -> {ok, Value}
		end.
