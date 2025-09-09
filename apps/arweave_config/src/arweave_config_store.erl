%%%===================================================================
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_store).
-vsn(1).
-compile([export_all]).
-compile({no_auto_import,[get/1]}).
-behavior(gen_server).
-export([
	start_link/0,
	get/1,
	get/2,
	set/2,
	to_map/0,
	from_map/1
]).
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2
]).
-record(key, {id}).
-record(value, {value, meta}).
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% @doc Starts `arweave_config_store' registered process.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc Retrieve a key from configuration store using ETS directly.
%% @see lookup/1
%% @end
%%--------------------------------------------------------------------
-spec get(Key) -> Return when
	Key :: term(),
	Return :: {ok, term()} | {error, undefined}.

get(Key) ->
	case arweave_config_parser:key(Key) of
		{ok, Id} -> lookup(Id);
		Elsewise -> Elsewise
	end.

get1_test() ->
	{ok, Pid} = start_link(),
	?assertEqual(
		{error, undefined},
		get(test)
	),
	gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% @doc Retrieve a value from ETS table, if not defined, return the
%% default value from second argument.
%% @end
%%--------------------------------------------------------------------
-spec get(Key, Default) -> Return when
	Key :: term(),
	Default :: term(),
	Return :: term() | Default.

get(Key, Default) ->
	case get(Key) of
		{ok, Value} -> Value;
		_ -> Default
	end.

get2_test() ->
	{ok, Pid} = start_link(),
	?assertEqual(
		default,
		get(test, default)
	),
	gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% @doc Configure a value using a new key.
%% @todo if the key is already defined, the old value should be
%%       returned.
%% @end
%%--------------------------------------------------------------------
-spec set(Key, Value) -> Return when
	Key :: term(),
	Value :: term(),
	Return :: {ok, New}
		| {ok, New, Old}
		| {error, term()},
	New :: {Id, Value},
	Old :: {Id, Value},
	Id :: term().

set(Key, Value) ->
	case arweave_config_parser:key(Key) of
		{ok, Id} ->
			gen_server:call(?MODULE, {set, Id, Value});
		Elsewise ->
			Elsewise
	end.

set2_test() ->
	{ok, Pid} = start_link(),
	?assertEqual(
		{ok, {[test], data}},
		set(test, data)
	),
	gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% @doc Converts a map into a valid structure ready to be inserted
%% into an ETS table.
%% @todo create the import feature.
%%
%% ```
%% % from:
%% #{ 1 => 2, 2 => #{ 3 => 4 }}.
%%
%% % every keys/values must be valid, and then the final data
%% % structure before insert should look like that:
%% [{1, 2}, {[2,3], 4}].
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec from_map(Map) -> Return when
	Map :: map(),
	Return :: {ok, list()} | {error, term()}.

from_map(Data) when is_map(Data) ->
	todo.

%%--------------------------------------------------------------------
%% @doc Converts the content of the ETS table into a map. It will
%% be easier to export the database in this case.
%% @todo the merger is not finished yet.
%%
%% ```
%% % the final output should look like that
%% #{ key1 => #{ key2 => value } }.
%%
%% % it can easily be converted into json, yaml or toml.
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec to_map() -> Return when
	Return :: map().

to_map() ->
	Parameters = ets:tab2list(?MODULE),
	ListOfMap = to_map(Parameters, []),
	map_merge(ListOfMap).

to_map([], Buffer) -> Buffer;
to_map([{#key{ id = Id }, #value{ value = Value }}|Rest], Buffer) ->
	to_map(Rest, [map_path(Id, Value)|Buffer]).

to_map_test() ->
	{ok, Pid} = start_link(),
	set("test.a.b", 1),
	set(<<"test.a.c">>, 2),
	?assertEqual(
		#{
			test => #{
				a => #{
				       b => 1,
				       c => 2
				}
			}
		},
		to_map()
	),
	gen_server:stop(Pid).


%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
map_path(List, Value) ->
	[H|Rest] = lists:reverse(List),
	map_path2(Rest, #{ H => Value }).

map_path2([], Buffer) -> Buffer;
map_path2([H|T], Buffer) ->
	map_path2(T, #{ H => Buffer }).

map_path2_test() ->
	?assertEqual(
		#{
			1 => #{
				2 => #{
				       3 => data
				}
			}
		},
		map_path([1,2,3], data)
	).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
map_merge(ListOfMap) ->
	lists:foldr(fun(X, A) -> map_merge(X, A) end, #{}, ListOfMap).

map_merge(A, B) when is_map(A), is_map(B) ->
	I = maps:iterator(A),
	map_merge2(I, B).

map_merge2(none, B) -> B;
map_merge2({K, V, I2}, B) when is_map(V), is_map_key(K, B) ->
	BV = maps:get(K, B, #{}),
	Result = map_merge(V, BV),
	map_merge2(I2, B#{ K => Result });
map_merge2({K, V, I2}, B) when is_map_key(K, B) ->
	BV = maps:get(K, B),
	case V =:= BV of
		true ->
			map_merge2(I2, B#{ K => V });
		false ->
			?LOG_WARNING("overwrite value: ~p", [K, V, BV]),
			map_merge2(I2, B#{ K => V })
	end;
map_merge2({K, V, I2}, B) ->
	map_merge2(I2, B#{ K => V });
map_merge2(I = [0|_], B) ->
	I2 = maps:next(I),
	map_merge2(I2, B).

map_merge_test() ->
	?assertEqual(
		#{
			1 => #{
				2 => test,
				3 => data
			},
			t => #{
			       1 => #{
				      test => data
				}
			}
		},
		map_merge([
			#{ 1 => #{ 2 => test } },
			#{ 1 => #{ 3 => data } },
			#{ t => #{ 1 => #{ test => data }}}
		])
	).


%%--------------------------------------------------------------------
%% @hidden
%% @doc a wrapper around ets:lookup/2
%% @end
%%--------------------------------------------------------------------
lookup(Id) ->
	case ets:lookup(?MODULE, #key{ id = Id }) of
		[] ->
			{error, undefined};
		[{#key{ id = Id }, #value{ value = Value}}] ->
			{ok, {Id, Value}}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_Args) ->
	erlang:process_flag(trap_exit, true),
	Ets = ets:new(?MODULE, [named_table, protected]),
	{ok, Ets}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call({set, Id, Value}, _From, State) ->
	K = #key{ id = Id },
	V = #value{ value = Value, meta = #{} },
	case ets:insert(?MODULE, {K, V}) of
		true ->
			{reply, {ok, {Id, Value}}, State};
		false ->
			{reply, {error, {Id, Value}}, State}
	end;
handle_call({delete, Id}, From, State) ->
	case ets:take(?MODULE, #key{ id = Id }) of
		[] ->
			{reply, {error, undefined}, State};
		[{_, #value{ value = Value}}] ->
			{reply, {ok, {Id, Value}}, State}
	end;
handle_call(Msg, From, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(_Msg, State) ->
	{noreply, State}.

