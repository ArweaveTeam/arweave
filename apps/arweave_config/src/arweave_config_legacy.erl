%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_legacy).
-behavior(gen_server).
-export([start_link/0, stop/0]).
-export([keys/0, has_key/1, get/1, set/2]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_info/2, handle_cast/2]).
-export([config_to_proplist/1, proplist_to_config/1]).
-include_lib("arweave/include/ar_config.hrl").
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
-spec keys() -> [atom()].

keys() ->
	gen_server:call(?MODULE, keys, 1000).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
-spec has_key(atom()) -> boolean().

has_key(Key) ->
	gen_server:call(?MODULE, {has_key, Key}, 1000).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
get(Key) ->
	try gen_server:call(?MODULE, {get, Key}, 1000) of
		{ok, Value} -> Value;
		Elsewise -> throw(Elsewise)
	catch
		E:R:S ->
			throw({E, {R,S}})
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
set(Key, Value) ->
	try gen_server:call(?MODULE, {set, Key, Value}, 1000) of
		{ok, NewValue, _OldValue} -> NewValue;
		Elsewise -> throw(Elsewise)
	catch
		E:R:S ->
			throw({E,{R,S}})
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
stop() ->
	gen_server:stop(?MODULE).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(_) ->
	Proplist = config_to_proplist(#config{}),
	Map = maps:from_list(Proplist),
	{ok, Map}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
terminate(_, _) ->
	ok.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_call({has_key, Key}, _From, State) ->
	{reply, erlang:is_map_key(Key, State), State};	
handle_call(keys, _From, State) ->
	{reply, maps:keys(State), State};
handle_call({get, Key}, _From, State)
	when is_atom(Key), is_map_key(Key, State) ->
		Return = {ok, maps:get(Key, State)},
		{reply, Return, State};
handle_call({set, Key, Value}, _From, State)
	when is_atom(Key), is_map_key(Key, State) ->
		OldValue = maps:get(Key, State),
		Return = {ok, Value, OldValue},
		NewState = State#{ Key => Value },
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
%%
%%--------------------------------------------------------------------
handle_cast(_, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_info(_, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
config_to_proplist(Config)
	when is_tuple(Config) ->
		Fields = record_info(fields, config),
		Values = erlang:delete_element(1, Config),
       		List = erlang:tuple_to_list(Values),
		lists:zip(Fields, List).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
proplist_to_config(Proplist)
	when is_list(Proplist) ->
		_Fields = record_info(fields, config),
		Values = lists:map(fun({_,V}) -> V end, Proplist),
		Values2 = [config|Values],
		erlang:list_to_tuple(Values2).
	
