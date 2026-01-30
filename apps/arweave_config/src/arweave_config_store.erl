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
%%% @doc Arweave Configuration Data Store Interface.
%%%
%%% This module/process is in charge to store the configuration.
%%% Usually, `arweave_config_spec' is the only process allowed to do
%%% so, but this rule is not enforced at the moment.
%%%
%%% == Features ==
%%%
%%% === Dealing with map root value ===
%%%
%%% While creating new parameter, a problem will probably arise very
%%% soon. What if a leaf is also a branch? Let imagine we want to
%%% create a more flexible way to configure the debugging parameter,
%%% and permit users to configure debug on some part of the code, the
%%% implementation would be something like the code below:
%%%
%%% ```
%%% {[global,debug], true}
%%% {[global,debug,arweave_config], true}
%%% '''
%%%
%%% Unfortunately, this will not work in the current implementation, a
%%% map. When a value is already present and is not a `map()', then it
%%% will  be  set  as  `root'  item.  The  `root'  is  represented  as
%%% underscore character (`_').
%%%
%%% ```
%%% % extracted from arweave_config_store
%%% Proplist = [
%%%   {[global,debug], true},
%%%   {[global,debug,arweave_config, true}
%%% ].
%%%
%%% % converted as map
%%% Maps = [
%%%   #{ global => #{ debug => true }},
%%%   #{ global => #{ debug => #{ arweave_config => true }}}
%%% ]
%%%
%%% % merged as map
%%% MergedMap = #{
%%%   global => #{
%%%     debug => #{
%%%       '_' => true,
%%%       arweave_config => true
%%%     }
%%%   }
%%% }
%%%
%%% % as JSON
%%% {
%%%   "global": {
%%%     "debug": {
%%%       "_": true,
%%%       "arweave_config": true
%%%     }
%%%   }
%%% }
%%%
%%% % as YAML
%%% global:
%%%   debug:
%%%     "_": true
%%%     arweave_config: true
%%% '''
%%%
%%% The key `_' then becomes a reserved key.
%%%
%%% == TODO ==
%%%
%%% === Single Line Format Support ===
%%%
%%% Instead of exporting classic JSON format, an easier one can be
%%% created, where one value is attributed on one line:
%%%
%%% ```
%%% global.debug=true
%%% global.data.directory="."
%%% peers.[127.0.0.1:1984].enabled=true
%%% '''
%%%
%%% The separatator could be `=' or a null char (e.g `\t', ` '). One
%%% huge advantage is no external module will be required to
%%% parse/decode, and the format is pretty close from what we already
%%% have in the database.
%%%
%%% === JSON Support ===
%%%
%%% The key present in the store can have different type, usually
%%% `atom()', `binary()' and/or `integer()'. Encoder like `jiffy' will
%%% not encode `integer()' key to JSON string directly, then, a
%%% conversion step will be required.
%%%
%%% @end
%%%===================================================================
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
	to_map/0,
	from_map/1
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
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% @doc Starts `arweave_config_store' registered process.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc Stops `arweave_config_store' process.
%% @end
%%--------------------------------------------------------------------
stop() ->
	gen_server:stop(?MODULE).

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
		{ok, Id} ->
			lookup(Id);
		Elsewise ->
			{error, Elsewise}
	end.

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

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec delete(Key) -> Return when
	Key :: term(),
	Return :: {ok, term()} | {error, undefined}.

delete(Key) ->
	case arweave_config_parser:key(Key) of
		{ok, Id} ->
			gen_server:call(?MODULE, {delete, Id});
		Elsewise ->
			Elsewise
	end.

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
	arweave_config_serializer:map_merge(ListOfMap).

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
%% @doc a wrapper around ets:lookup/2
%% @end
%%--------------------------------------------------------------------
lookup(Id) ->
	case ets:lookup(?MODULE, #key{ id = Id }) of
		[] ->
			{error, undefined};
		[{#key{ id = Id }, #value{ value = Value}}] ->
			{ok, Value};
		Elsewise ->
			{error, Elsewise}
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
	?LOG_ERROR([{message, Msg}, {from, From}, {module, ?MODULE}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
	?LOG_ERROR([{message, Msg}, {module, ?MODULE}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
	?LOG_ERROR([{message, Msg}, {module, ?MODULE}]),
	{noreply, State}.
