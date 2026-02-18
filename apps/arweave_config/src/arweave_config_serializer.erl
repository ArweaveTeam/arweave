%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Configuration File Serializer.
%%%
%%% This module is in charge of serializing a map and convert it to a
%%% specification compatible format.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_serializer).
-compile(warnings_as_errors).
-export([encode/1, encode/2, decode/1, decode/2]).
-export([map_merge/1]).
-export([encode_enter/1, encode_iterate/1, encode_merge/1]).
-export([decode_enter/1, decode_iterate/1, decode_merge/1]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc convert a map to arweave_config specification format.
%% @see encode/2
%% @end
%%--------------------------------------------------------------------
-spec encode(Map) -> Return when
	Map :: map(),
	Return :: map().

encode(Map) ->
	encode(Map, #{}).

%%--------------------------------------------------------------------
%% @doc convert a map to arweave_config specification format.
%% @end
%%--------------------------------------------------------------------
encode(Map, Opts) ->
	encode(Map, Opts, []).

%%--------------------------------------------------------------------
%% @doc convert a map to arweave_config specification format.
%% @end
%%--------------------------------------------------------------------
-spec encode(Map, Opts, Level) -> Return when
	Map :: map(),
	Opts :: map(),
	Level :: list(),
	Return :: map().

encode(Map, Opts, Level) when is_map(Map), is_list(Level) ->
	Iterator = maps:iterator(Map),
	State = #{
		opts => Opts,
		map => Map,
		iterator => Iterator,
		level => Level
	},
	arweave_config_fsm:init(?MODULE, encode_enter, State);
encode(_, _, _) ->
	{error, badarg}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc initialize the buffer and start the iteration.
%% @end
%%--------------------------------------------------------------------
encode_enter(State = #{ iterator := Iterator }) ->
	NewState = State#{
		iterator => maps:next(Iterator),
		buffer => #{}
	},
	{next, encode_iterate, NewState}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc iterate over the iterator and route the data when needed.
%% @end
%%--------------------------------------------------------------------
encode_iterate(#{ iterator := none, buffer := Buffer }) ->
	{ok, Buffer};
encode_iterate(State = #{ iterator := {K, V, Iterator} })
	when is_map(V) ->
		Opts = maps:get(opts, State),
		Level = maps:get(level, State),
		Buffer = maps:get(buffer, State),
		Key = encode_convert_key(K),
		case encode(V, Opts, [Key|Level]) of
			{ok, MapBuffer} ->
				NewState = State#{
					iterator => Iterator,
					buffer => maps:merge(Buffer, MapBuffer)
				},
				{next, encode_iterate, NewState};
			Else ->
				{error, Else}
		end;
encode_iterate(State = #{ iterator := {_K, _V, _Iterator} }) ->
	{next, encode_merge, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc merge key/values into one unique map.
%% @end
%%--------------------------------------------------------------------
encode_merge(State = #{ iterator := {K, V, Iterator} }) ->
	Buffer = maps:get(buffer, State),
	Level = maps:get(level, State),
	Key = encode_convert_key(K),
	ReversedLevel = lists:reverse([Key|Level]),
	NewState = State#{
		iterator => maps:next(Iterator),
		buffer => Buffer#{ ReversedLevel => V }
	},
	{next, encode_iterate, NewState}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc convert the key as existing atoms.
%% @end
%%--------------------------------------------------------------------
encode_convert_key(List) when is_list(List) ->
	try list_to_existing_atom(List)
	catch _:_ -> List
	end;
encode_convert_key(Binary) when is_binary(Binary) ->
	try binary_to_existing_atom(Binary)
	catch _:_ -> Binary
	end;
encode_convert_key(Else) -> Else.


%%--------------------------------------------------------------------
%% @doc
%% @see decode/2
%% @end
%%--------------------------------------------------------------------
-spec decode(Map) -> Return when
	Map :: #{ [term()] => term() },
	Return :: #{ term() => term() }.

decode(Map) ->
	decode(Map, #{}).

%%--------------------------------------------------------------------
%% @doc decode arweave config serialized map. A similar implementation
%% was present in `arweave_config_store', this one is using
%% `arweave_config_fsm'.
%% @end
%%--------------------------------------------------------------------
-spec decode(Map, Opts) -> Return when
	Map :: #{ [term()] => term() },
	Opts :: map(),
	Return :: #{ term() => term() }.

decode(Map, Opts) ->
	Iterator = maps:iterator(Map),
	Level = [],
	State = #{
		opts => Opts,
		map => Map,
		iterator => Iterator,
		level => Level
	},
	arweave_config_fsm:init(?MODULE, decode_enter, State).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc initialize the decoder and the iterator.
%% @end
%%--------------------------------------------------------------------
decode_enter(State = #{ iterator := Iterator }) ->
	NewState = State#{
		iterator => maps:next(Iterator),
		buffer => []
	},
	{next, decode_iterate, NewState}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc loop over the items present in the map.
%% @end
%%--------------------------------------------------------------------
decode_iterate(State = #{ iterator := none }) ->
	{next, decode_merge, State};
decode_iterate(State = #{ iterator := {K, V, Iterator} }) when is_list(K) ->
	Buffer = maps:get(buffer, State),
	[K0|KS] = lists:reverse(K),
	Fold = lists:foldl(fun decode_fold/2, #{ K0 => V }, KS),
	NewState = State#{
		iterator => maps:next(Iterator),
		buffer => [Fold|Buffer]
	},
	{next, decode_iterate, NewState}.

% lambda using in decode_iterate.
decode_fold(Item, Acc) ->
	#{ Item => Acc }.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc merge the final values.
%% @end
%%--------------------------------------------------------------------
decode_merge(#{ buffer := Buffer }) ->
	Result = map_merge(Buffer),
	{ok, Result}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc merge map recursively using arweave config rules.
%% @end
%% @todo this function should use arweave_config_fsm
%%--------------------------------------------------------------------
map_merge(ListOfMap) ->
	lists:foldr(
		fun(X, A) ->
			map_merge(X, A)
		end,
		#{},
		ListOfMap
	).

map_merge(A, B) when is_map(A), is_map(B) ->
	I = maps:iterator(A),
	map_merge2(I, B);
map_merge(A, B) when is_map(A) ->
	A#{ '_' => B }.

map_merge2(none, B) ->
	B;
map_merge2({K, V, I2}, B)
	when is_map(V), is_map_key(K, B) ->
		BV = maps:get(K, B, #{}),
		Result = map_merge(V, BV),
		map_merge2(I2, B#{ K => Result });
map_merge2({K, V, I2}, B)
	when is_map_key(K, B) ->
		BV = maps:get(K, B),
		case V =:= BV of
			true ->
				map_merge2(I2, B#{ K => V });
			false ->
				map_merge3(I2, B, K, BV, V)
		end;
map_merge2({K, V, I2}, B) ->
	map_merge2(I2, B#{ K => V });
map_merge2(I = [0|_], B) ->
	I2 = maps:next(I),
	map_merge2(I2, B).

map_merge3(I2, B, K, BV, V) when is_map(BV) ->
	case is_map_key('_', BV) of
		true ->
			OV = maps:get('_', BV),
			?LOG_WARNING("value ~p will be overwritten.", [OV]),
			map_merge2(I2, B#{ K => BV#{ '_' => V }});
		false ->
			map_merge2(I2, B#{ K => BV#{ '_' => V }})
	end;
map_merge3(I2, B, K, BV, V) ->
	?LOG_WARNING("value ~p will be ignored.", [BV]),
	map_merge2(I2, B#{ K => V }).
