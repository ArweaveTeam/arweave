%%% @doc Convert between nested config maps and per-leaf config maps.
%%%
%%% A leaf map has one entry per full option path:
%%% `#{[PathSegment, ...] => Value}`.
-module(arweave_config_leaf_map).
-compile(warnings_as_errors).
-export([set/3, convert_key/1]).
-export([leaf_map_to_nested/1, leaf_map_to_nested/2]).
-export([merge_nested_maps/1]).
-include_lib("kernel/include/logger.hrl").

%% @doc Insert `Value' at `Path' in the leaf map. Maps and lists-of-
%% maps in `Value' recurse, extending the path; everything else lands
%% as a single leaf at `Path'.
%%
%% Same path with the same value is idempotent. Same path with a
%% different value returns `conflicting_config_key'.
-spec set(Path, Value, LeafMap) -> Return when
	Path :: list(),
	Value :: term(),
	LeafMap :: map(),
	Return :: {ok, map()} | {error, map()}.
set(Path, Map, LeafMap) when is_map(Map) ->
	walk_nested(maps:to_list(Map), Path, LeafMap);
set(Path, List, LeafMap) when is_list(List) ->
	case io_lib:printable_unicode_list(List) of
		true ->
			insert_leaf(Path, List, LeafMap);
		false ->
			case lists:all(fun is_map/1, List) of
				true -> walk_list(List, 1, Path, LeafMap);
				false -> insert_leaf(Path, List, LeafMap)
			end
	end;
set(Path, Value, LeafMap) ->
	insert_leaf(Path, Value, LeafMap).

walk_nested([], _Prefix, LeafMap) ->
	{ok, LeafMap};
walk_nested([{Key, Value} | Rest], Prefix, LeafMap) ->
	case set(Prefix ++ [convert_key(Key)], Value, LeafMap) of
		{ok, NewLeafMap} -> walk_nested(Rest, Prefix, NewLeafMap);
		{error, _} = Err -> Err
	end.

walk_list([], _Index, _Prefix, LeafMap) ->
	{ok, LeafMap};
walk_list([Item | Rest], Index, Prefix, LeafMap) ->
	case set(Prefix ++ [Index], Item, LeafMap) of
		{ok, NewLeafMap} -> walk_list(Rest, Index + 1, Prefix, NewLeafMap);
		{error, _} = Err -> Err
	end.

insert_leaf(Path, Value, LeafMap) ->
	case maps:find(Path, LeafMap) of
		error ->
			{ok, LeafMap#{Path => Value}};
		{ok, Value} ->
			{ok, LeafMap};
		{ok, Existing} ->
			{error, #{
				reason => conflicting_config_key,
				key => Path,
				existing => Existing,
				value => Value
			}}
	end.

%% Convert string/binary keys to existing atoms; pass through
%% anything else unchanged. Delegates the actual atomization to
%% `arweave_config_type:atom/1' — this wrapper just turns its
%% `{error, _}' "not a known atom" case into a pass-through.
convert_key(Key) ->
	case arweave_config_type:atom(Key) of
		{ok, Atom} -> Atom;
		{error, _} -> Key
	end.

%% @doc Convert a per-leaf config map into a nested config map.
%% @see leaf_map_to_nested/2
-spec leaf_map_to_nested(Map) -> Return when
	Map :: #{ [term()] => term() },
	Return :: {ok, #{ term() => term() }}.
leaf_map_to_nested(Map) ->
	leaf_map_to_nested(Map, #{}).

%% @doc Convert a per-leaf config map into a nested config map.
-spec leaf_map_to_nested(Map, Opts) -> Return when
	Map :: #{ [term()] => term() },
	Opts :: map(),
	Return :: {ok, #{ term() => term() }}.
leaf_map_to_nested(Map, _Opts) ->
	Buffer = leaf_map_to_nested_iter(maps:next(maps:iterator(Map)), []),
	{ok, merge_nested_maps(Buffer)}.

leaf_map_to_nested_iter(none, Buffer) ->
	Buffer;
leaf_map_to_nested_iter({K, V, Iterator}, Buffer) when is_list(K) ->
	[K0 | KS] = lists:reverse(K),
	Nested = lists:foldl(fun(Item, Acc) -> #{Item => Acc} end, #{K0 => V}, KS),
	leaf_map_to_nested_iter(maps:next(Iterator), [Nested | Buffer]).

%% @doc Recursively merge a list of nested maps. When two values
%% collide at the same path, the existing value is preserved at the
%% reserved `_` key (see module doc for `arweave_config_store`).
merge_nested_maps(ListOfMap) ->
	lists:foldr(
		fun(X, A) ->
			merge_nested_maps(X, A)
		end,
		#{},
		ListOfMap
	).

merge_nested_maps(A, B) when is_map(A), is_map(B) ->
	I = maps:iterator(A),
	merge_nested_maps_iter(maps:next(I), B);
merge_nested_maps(A, B) when is_map(A) ->
	A#{ '_' => B }.

merge_nested_maps_iter(none, B) ->
	B;
merge_nested_maps_iter({K, V, I2}, B)
	when is_map(V), is_map_key(K, B) ->
	BV = maps:get(K, B, #{}),
	Result = merge_nested_maps(V, BV),
	merge_nested_maps_iter(I2, B#{ K => Result });
merge_nested_maps_iter({K, V, I2}, B)
	when is_map_key(K, B) ->
	BV = maps:get(K, B),
	case V =:= BV of
		true ->
			merge_nested_maps_iter(I2, B#{ K => V });
		false ->
			merge_nested_maps_conflict(I2, B, K, BV, V)
	end;
merge_nested_maps_iter({K, V, I2}, B) ->
	merge_nested_maps_iter(I2, B#{ K => V }).

merge_nested_maps_conflict(I2, B, K, BV, V) when is_map(BV) ->
	case is_map_key('_', BV) of
		true ->
			OV = maps:get('_', BV),
			?LOG_WARNING("value ~p will be overwritten.", [OV]),
			merge_nested_maps_iter(I2, B#{ K => BV#{ '_' => V }});
		false ->
			merge_nested_maps_iter(I2, B#{ K => BV#{ '_' => V }})
	end;
merge_nested_maps_conflict(I2, B, K, BV, V) ->
	?LOG_WARNING("value ~p will be ignored.", [BV]),
	merge_nested_maps_iter(I2, B#{ K => V }).
