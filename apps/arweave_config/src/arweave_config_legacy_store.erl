%%%===================================================================
%%% @doc temporary interface to arweave legacy configuration.
%%% provides some function to convert records into proplists/maps and
%%% export them as well
%%% @end
%%%===================================================================
-module(arweave_config_legacy_store).
-behavior(gen_server).
-export([start_link/0]).
-export([keys/0, has_key/1, get/1, set/2]).
-export([proplist_to_config/1, config_to_proplist/1]).
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("arweave/include/ar_config.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
-spec keys() -> [atom()].

keys() ->
	gen_server:call(?MODULE, keys, 10_000).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
-spec has_key(atom()) -> boolean().

has_key(Key) ->
	gen_server:call(?MODULE, {has_key, Key}, 10_000).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
-spec get(Key) -> Return when
	Key :: atom(),
	Return :: atom().

get(Key) ->
	case has_key(Key) of
		true ->
			gen_server:call(?MODULE, {get, Key}, 10_000);
		false ->
			throw({undefined, Key})
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
-spec set(Key, Value) -> Return when
	Key ::  atom(),
	Value :: term(),
	Return :: {ok, OldValue} | timeout,
	OldValue :: Value.

set(Key, Value) ->
	case has_key(Key) of
		true ->
			gen_server:call(?MODULE, {set, Key, Value}, 10_000);
		false ->
			throw({undefined, Key})
	end.

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
handle_call({has_key, Key}, _From, State) ->
	{reply, erlang:is_map_key(Key, State), State};	
handle_call(keys, _From, State) ->
	{reply, maps:keys(State), State};
handle_call({get, Key}, _From, State)
	when is_map_key(Key, State) ->
		{reply, maps:get(Key, State), State};
handle_call({set, Key, Value}, _From, State)
	when is_map_key(Key, State) ->
		OldValue = maps:get(Key, State),
		{reply, {ok, OldValue}, State#{ Key => Value }};
handle_call(_, _From, State) ->
	{noreply, State}.

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
	
