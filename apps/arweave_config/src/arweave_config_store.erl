%%% @doc Arweave configuration data store.
%%%
%%% ETS-backed key/value store fronted by a gen_server. Keys are
%%% option_key lists (e.g. `[global, debug]`); values are arbitrary
%%% terms.
%%%
%%% == Leaf-as-branch encoding ==
%%%
%%% When the same prefix carries both a scalar and a nested value
%%% (e.g. `[global, debug]` set to `true` AND `[global, debug, foo]`
%%% set to `false`), `to_map/0` represents the scalar at the reserved
%%% key `_` so both values can coexist in the rendered map:
%%%
%%% ```
%%% #{ global => #{ debug => #{ '_' => true, foo => false }}}
%%% '''
-module(arweave_config_store).
-behavior(gen_server).
-vsn(1).
-export([
	start_link/0,
	stop/0,
	get/1,
	get/2,
	set/2,
	delete/1,
	items_with_prefix/1,
	log/0,
	to_map/0,
	snapshot/0,
	restore/1
]).
-export([
	init/1,
	handle_call/3,
	handle_cast/2,
	handle_info/2
]).
-compile({no_auto_import,[get/1]}).
-record(key, {id}).
-record(value, {value, meta}).
-include_lib("kernel/include/logger.hrl").

%% @doc Start the `arweave_config_store` registered process.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop the `arweave_config_store` process.
stop() ->
	gen_server:stop(?MODULE).

%% @doc Retrieve a value from the configuration store.
%% @see lookup/1
-spec get(Key) -> Return when
	Key :: term(),
	Return :: {ok, term()} | {error, undefined}.
get(Key) ->
	case arweave_config_parser:key(Key) of
		{ok, Id} ->
			lookup(Id);
		Else ->
			{error, Else}
	end.

%% @doc Retrieve a value, falling back to `Default` when unset.
-spec get(Key, Default) -> Return when
	Key :: term(),
	Default :: term(),
	Return :: term() | Default.
get(Key, Default) ->
	case get(Key) of
		{ok, Value} -> Value;
		_ -> Default
	end.

%% @doc Set a value at `Key`.
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
		Else ->
			Else
	end.

%% @doc Delete the entry at `Key`.
-spec delete(Key) -> Return when
	Key :: term(),
	Return :: {ok, term()} | {error, undefined}.
delete(Key) ->
	case arweave_config_parser:key(Key) of
		{ok, Id} ->
			gen_server:call(?MODULE, {delete, Id});
		Else ->
			Else
	end.

%% @doc Render the store contents as a nested map suitable for JSON
%% or YAML encoding.
-spec to_map() -> Return when
	Return :: map().
to_map() ->
	Options = ets:tab2list(?MODULE),
	ListOfMap = to_map(Options, []),
	arweave_config_leaf_map:merge_nested_maps(ListOfMap).

%% @doc Capture the current store state. The opaque return value can
%% be handed to `restore/1` to put the store back later.
-spec snapshot() -> [tuple()].
snapshot() ->
	ets:tab2list(?MODULE).

%% @doc Restore a captured store state. Drops every current row and
%% re-inserts the snapshot's rows via the gen_server (the ETS table
%% is `protected`, so only the owner can write).
%%
%% Test scaffolding: concurrent setters during a snapshot/restore
%% window are not safe.
-spec restore([tuple()]) -> ok.
restore(Rows) when is_list(Rows) ->
	gen_server:call(?MODULE, {restore, Rows}, 10_000).

%% @doc Dump every stored option to `?LOG_INFO` as a sorted list of
%% `key.path: value` lines for operator inspection.
-spec log() -> ok.
log() ->
	?LOG_INFO("=============== Start Config ==============="),
	Rows = lists:sort(ets:tab2list(?MODULE)),
	lists:foreach(fun log_row/1, Rows),
	?LOG_INFO("=============== End Config   ===============").

log_row({#key{ id = Id }, #value{ value = Value }}) ->
	?LOG_INFO("~ts: ~ts",
		[arweave_config_parser:format_key(Id), format_value(Value)]).

%% Encode unprintable binaries as base64 so logs stay readable.
format_value(V) when is_binary(V) ->
	case io_lib:printable_unicode_list(binary_to_list(V)) of
		true -> V;
		false -> ar_util:encode(V)
	end;
format_value(V) ->
	iolist_to_binary(io_lib:format("~tp", [V])).

to_map([], Buffer) -> Buffer;
to_map([{#key{ id = Id }, #value{ value = Value }}|Rest], Buffer) ->
	to_map(Rest, [map_path(Id, Value)|Buffer]).

map_path(List, Value) ->
	[H|Rest] = lists:reverse(List),
	map_path2(Rest, #{ H => Value }).

map_path2([], Buffer) -> Buffer;
map_path2([H|T], Buffer) ->
	map_path2(T, #{ H => Buffer }).

%% @doc Every `{Key, Value}` in the store whose key starts with
%% `Prefix`.
-spec items_with_prefix(Prefix) -> [{Key, Value}] when
	Prefix :: list(),
	Key :: list(),
	Value :: term().
items_with_prefix(Prefix) when is_list(Prefix) ->
	ets:foldl(
		fun
			({#key{ id = Id }, #value{ value = Value }}, Acc)
					when is_list(Id) ->
				case has_prefix(Id, Prefix) of
					true -> [{Id, Value} | Acc];
					false -> Acc
				end;
			(_Other, Acc) ->
				Acc
		end,
		[],
		?MODULE
	).

has_prefix(_Id, []) -> true;
has_prefix([H | IdRest], [H | PrefixRest]) -> has_prefix(IdRest, PrefixRest);
has_prefix(_, _) -> false.

lookup(Id) ->
	case ets:lookup(?MODULE, #key{ id = Id }) of
		[] ->
			{error, undefined};
		[{#key{ id = Id }, #value{ value = Value}}] ->
			{ok, Value};
		Else ->
			{error, Else}
	end.

init(_Args) ->
	erlang:process_flag(trap_exit, true),
	Ets = ets:new(?MODULE, [named_table, protected]),
	{ok, Ets}.

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
handle_call({restore, Rows}, _From, State) ->
	true = ets:delete_all_objects(?MODULE),
	true = ets:insert(?MODULE, Rows),
	{reply, ok, State};
handle_call(Msg, From, State) ->
	?LOG_ERROR([{message, Msg}, {from, From}, {module, ?MODULE}]),
	{noreply, State}.

handle_cast(Msg, State) ->
	?LOG_ERROR([{message, Msg}, {module, ?MODULE}]),
	{noreply, State}.

handle_info(Msg, State) ->
	?LOG_ERROR([{message, Msg}, {module, ?MODULE}]),
	{noreply, State}.
