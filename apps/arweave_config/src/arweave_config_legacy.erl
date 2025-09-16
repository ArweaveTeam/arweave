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
%%% == Examples ==
%%%
%%% @end
%%%===================================================================
-module(arweave_config_legacy).
-behavior(gen_server).
-export([start_link/0, stop/0]).
-export([keys/0, has_key/1, get/0, get/1, set/2]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_info/2, handle_cast/2]).
-export([config_to_proplist/1, proplist_to_config/1]).
-compile({no_auto_import,[get/0, get/1]}).
-include_lib("arweave/include/ar_config.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

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
set(Key, Value) ->
	try gen_server:call(?MODULE, {set, Key, Value}, 1000) of
		{ok, NewValue, _OldValue} -> {ok, NewValue};
		_Elsewise -> error
	catch
		_E:_R:_S -> error
	end.

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
	case application:get_env(arweave, config) of
		undefined ->
			Proplist = config_to_proplist(#config{}),
			{ok, Proplist};
		{ok, Config} when is_tuple(Config),
			element(1, Config) =:= config ->
				Proplist = config_to_proplist(Config),
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
handle_call({has_key, Key}, _From, State) ->
	{reply, proplists:is_defined(Key, State), State};	
handle_call(keys, _From, State) ->
	{reply, proplists:get_keys(State), State};
handle_call(get, _From, State) ->
	{reply, {ok, proplist_to_config(State)}, State};
handle_call({get, Key}, _From, State)
	when is_atom(Key) ->
		Return = {ok, proplists:get_value(Key, State)},
		{reply, Return, State};
handle_call({set, Key, Value}, _From, State)
	when is_atom(Key) ->
		OldValue = proplists:get_value(Key, State),
		Return = {ok, Value, OldValue},
		NewState = lists:keyreplace(Key, 1, State, {Key, Value}),
		NewConfig = proplist_to_config(NewState),
		application:set_env(arweave,config,NewConfig),
		{reply, Return, NewState};
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
handle_cast(_, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(_, State) ->
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
	

