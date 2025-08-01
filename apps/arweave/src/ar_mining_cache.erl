-module(ar_mining_cache).
-include_lib("arweave/include/ar_mining_cache.hrl").
-include_lib("arweave/include/ar.hrl").

-export([
	new/0, new/1, set_limit/2, get_limit/1,
	cache_size/1, actual_cache_size/1, available_size/1, reserved_size/1, reserved_size/2,
	add_session/2, reserve_for_session/3, release_for_session/3, drop_session/2,
	session_exists/2, get_sessions/1, with_cached_value/4
]).

-define(CACHE_SESSIONS_LIMIT, 4).

%%%===================================================================
%%% Public API.
%%%===================================================================

%% @doc Creates a new mining cache with a default limit of 0.
-spec new() ->
	Cache :: #ar_mining_cache{}.
new() -> #ar_mining_cache{}.

%% @doc Creates a new mining cache with a given limit.
-spec new(Limit :: pos_integer()) ->
	Cache :: #ar_mining_cache{}.
new(Limit) -> #ar_mining_cache{mining_cache_limit_bytes = Limit}.

%% @doc Sets the limit for the mining cache.
-spec set_limit(Limit :: pos_integer(), Cache :: #ar_mining_cache{}) ->
	Cache :: #ar_mining_cache{}.
set_limit(Limit, Cache) ->
	Cache#ar_mining_cache{mining_cache_limit_bytes = Limit}.

%% @doc Returns the limit for the mining cache.
-spec get_limit(Cache :: #ar_mining_cache{}) ->
	Limit :: non_neg_integer().
get_limit(Cache) ->
	Cache#ar_mining_cache.mining_cache_limit_bytes.

%% @doc Returns the size of the cached data in bytes.
%% Note, that cache size includes both the cached data and the reserved space for sessions.
-spec cache_size(Cache :: #ar_mining_cache{}) ->
	Size :: non_neg_integer().
cache_size(Cache) ->
	maps:fold(
		fun(_, #ar_mining_cache_session{mining_cache_size_bytes = Size, reserved_mining_cache_bytes = ReservedSize}, Acc) ->
			Acc + Size + ReservedSize
		end,
		0,
		Cache#ar_mining_cache.mining_cache_sessions
	).

%% @doc Returns the size of the cached data in bytes.
%% Note, that cache size includes both the cached data and the reserved space for sessions.
-spec actual_cache_size(Cache :: #ar_mining_cache{}) ->
	Size :: non_neg_integer().
actual_cache_size(Cache) ->
	maps:fold(
		fun(_, #ar_mining_cache_session{mining_cache = MiningCache}, Acc) ->
			Acc + maps:fold(fun(_, CacheValue, Acc0) ->
				Acc0 + cached_value_size(CacheValue)
			end, 0, MiningCache)
		end,
		0,
		Cache#ar_mining_cache.mining_cache_sessions
	).

%% @doc Returns the available size for the mining cache.
%% Note, that this value does not include the reserved space for sessions,
%% as this space is considered already used.
%% @see reserved_size/1,2
%% @see cache_size/1
-spec available_size(Cache :: #ar_mining_cache{}) ->
	Size :: non_neg_integer().
available_size(Cache) ->
	Cache#ar_mining_cache.mining_cache_limit_bytes - cache_size(Cache).

%% @doc Returns the reserved size for a cache.
-spec reserved_size(Cache0 :: #ar_mining_cache{}) ->
	{ok, Size :: non_neg_integer()} | {error, Reason :: term()}.
reserved_size(Cache0) ->
	{ok, lists:sum([
		begin
			{ok, Size} = reserved_size(SessionId, Cache0),
			Size
		end || SessionId <- get_sessions(Cache0)
	])}.

%% @doc Returns the reserved size for a session.
-spec reserved_size(SessionId :: term(), Cache0 :: #ar_mining_cache{}) ->
	{ok, Size :: non_neg_integer()} | {error, Reason :: term()}.
reserved_size(SessionId, Cache0) ->
	case with_mining_cache_session(SessionId, fun(Session) ->
		{ok, Session#ar_mining_cache_session.reserved_mining_cache_bytes, Session}
	end, Cache0) of
		{ok, Size, _Cache1} -> {ok, Size};
		{error, Reason} -> {error, Reason}
	end.

%% @doc Adds a new mining cache session to the cache.
%% If the cache limit is exceeded, the oldest session is dropped.
-spec add_session(SessionId :: term(), Cache0 :: #ar_mining_cache{}) ->
	Cache1 :: #ar_mining_cache{}.
add_session(SessionId, Cache0) ->
	case maps:is_key(SessionId, Cache0#ar_mining_cache.mining_cache_sessions) of
		true -> Cache0;
		false ->
			Cache1 = Cache0#ar_mining_cache{
				mining_cache_sessions = maps:put(SessionId, #ar_mining_cache_session{}, Cache0#ar_mining_cache.mining_cache_sessions),
				mining_cache_sessions_queue = queue:in(SessionId, Cache0#ar_mining_cache.mining_cache_sessions_queue)
			},
			case queue:len(Cache1#ar_mining_cache.mining_cache_sessions_queue) > ?CACHE_SESSIONS_LIMIT of
				true ->
					{{value, LastSessionId}, Queue1} = queue:out(Cache1#ar_mining_cache.mining_cache_sessions_queue),
					Cache2 = drop_session(LastSessionId, Cache1),
					Cache2#ar_mining_cache{mining_cache_sessions_queue = Queue1};
				false ->
					Cache1
			end
	end.

%% @doc Reserves a certain amount of space for a session.
%% Note, that if the session already has a reserved amount of space, it will be
%% added to the existing reserved space.
-spec reserve_for_session(SessionId :: term(), Size :: non_neg_integer(), Cache0 :: #ar_mining_cache{}) ->
	{ok, Cache1 :: #ar_mining_cache{}} | {error, Reason :: term()}.
reserve_for_session(SessionId, Size, Cache0) ->
	case available_size(Cache0) < Size of
		true -> {error, cache_limit_exceeded};
		false ->
			with_mining_cache_session(SessionId, fun(#ar_mining_cache_session{reserved_mining_cache_bytes = ReservedSize} = Session) ->
				{ok, Session#ar_mining_cache_session{reserved_mining_cache_bytes = ReservedSize + Size}}
			end, Cache0)
	end.

%% @doc Releases the reserved space for a session.
%% If the reserved space is less than the released size, the reserved space will be set to 0.
-spec release_for_session(SessionId :: term(), Size :: non_neg_integer(), Cache0 :: #ar_mining_cache{}) ->
	{ok, Cache1 :: #ar_mining_cache{}} | {error, Reason :: term()}.
release_for_session(SessionId, Size, Cache0) ->
	with_mining_cache_session(SessionId, fun(#ar_mining_cache_session{reserved_mining_cache_bytes = ReservedSize} = Session) ->
		{ok, Session#ar_mining_cache_session{reserved_mining_cache_bytes = max(0, ReservedSize - Size)}}
	end, Cache0).

%% @doc Drops a mining cache session from the cache.
-spec drop_session(SessionId :: term(), Cache0 :: #ar_mining_cache{}) ->
	Cache1 :: #ar_mining_cache{}.
drop_session(SessionId, Cache0) ->
	?LOG_DEBUG([{event, drop_session}, {session_id, SessionId}]),
	case maps:take(SessionId, Cache0#ar_mining_cache.mining_cache_sessions) of
		{Session, Sessions} ->
			maybe_search_for_anomalies(SessionId, Session),
			Cache0#ar_mining_cache{
				mining_cache_sessions = Sessions,
				mining_cache_sessions_queue = queue:filter(
					fun(SessionId0) -> SessionId0 =/= SessionId end,
					Cache0#ar_mining_cache.mining_cache_sessions_queue
				)
			};
		_ -> Cache0
	end.

%% @doc Checks if a session exists in the cache.
-spec session_exists(SessionId :: term(), Cache0 :: #ar_mining_cache{}) ->
	Exists :: boolean().
session_exists(SessionId, Cache0) ->
	maps:is_key(SessionId, Cache0#ar_mining_cache.mining_cache_sessions).

%% @doc Returns the list of sessions in the cache.
%% Note, that this list is not sorted by the chronological order.
-spec get_sessions(Cache0 :: #ar_mining_cache{}) ->
	Sessions :: [term()].
get_sessions(Cache0) ->
	queue:to_list(Cache0#ar_mining_cache.mining_cache_sessions_queue).

%% @doc Maps a cached value for a session into a new value.
%%
%% This function will take care of the cache size and reserved space for the session.
%% If the session does not contain a cached value for the given key, it will be generated,
%% e.g. the very first event for the `Key` is a genesis event.
%%
%% The `Fun` must return one of the following:
%% - `{ok, drop}`: drops the cached value
%% - `{ok, {drop, Size}}`: drops the cached value and
%%   additionally releases the reserved space (`Size` bytes)
%% - `{ok, Value1}`: replaces the cached value
%% - `{ok, Value1, Size}`: replaces the cached value and
%%   reserves the reserved space (`Size` bytes)
%% - `{error, Reason}`: returns an error
%%
%% If the returned value equals to the argument passed into the `Fun`, the cache
%% will not be changed. This implies that cache will not store the empty value.
-spec with_cached_value(
	Key :: term(),
	SessionId :: term(),
	Cache0 :: #ar_mining_cache{},
	Fun :: fun(
		(Value :: #ar_mining_cache_value{}) ->
			{ok, drop} |
			{ok, drop, Size :: non_neg_integer()} |
			{ok, Value1 :: #ar_mining_cache_value{}} |
			{ok, Value1 :: #ar_mining_cache_value{}, Size :: non_neg_integer()} |
			{error, Reason :: term()}
	)
) ->
	Result :: {ok, Cache1 :: #ar_mining_cache{}} | {error, Reason :: term()}.
with_cached_value(Key, SessionId, Cache0, Fun) ->
	with_mining_cache_session(SessionId, fun(Session) ->
		Value0 = maps:get(Key, Session#ar_mining_cache_session.mining_cache, #ar_mining_cache_value{}),
		case Fun(Value0) of
			{error, Reason} -> {error, Reason};
			{ok, drop} ->
				{ok, Session#ar_mining_cache_session{
					mining_cache = maps:remove(Key, Session#ar_mining_cache_session.mining_cache),
					mining_cache_size_bytes = max(0, Session#ar_mining_cache_session.mining_cache_size_bytes - cached_value_size(Value0))
				}};
			{ok, drop, ReservationSizeAdjustment} when ReservationSizeAdjustment < 0 ->
				{ok, Session#ar_mining_cache_session{
					mining_cache = maps:remove(Key, Session#ar_mining_cache_session.mining_cache),
					reserved_mining_cache_bytes = max(0, Session#ar_mining_cache_session.reserved_mining_cache_bytes + ReservationSizeAdjustment),
					mining_cache_size_bytes = max(0, Session#ar_mining_cache_session.mining_cache_size_bytes - cached_value_size(Value0))
				}};
			{ok, Value0} -> {ok, Session};
			{ok, Value0, ReservationSizeAdjustment} when ReservationSizeAdjustment < 0 ->
				{ok, Session#ar_mining_cache_session{
					reserved_mining_cache_bytes = max(0, Session#ar_mining_cache_session.reserved_mining_cache_bytes + ReservationSizeAdjustment)
				}};
			{ok, Value1} ->
				SizeDiff = cached_value_size(Value1) - cached_value_size(Value0),
				SessionAvailableSize = available_size(Cache0) + Session#ar_mining_cache_session.reserved_mining_cache_bytes,
				CacheLimit = get_limit(Cache0),
				case SizeDiff > SessionAvailableSize of
					true when CacheLimit =/= 0 -> {error, cache_limit_exceeded};
					_ ->
						{ok, Session#ar_mining_cache_session{
							mining_cache = maps:put(Key, Value1, Session#ar_mining_cache_session.mining_cache),
							reserved_mining_cache_bytes = max(0, Session#ar_mining_cache_session.reserved_mining_cache_bytes - SizeDiff),
							mining_cache_size_bytes = Session#ar_mining_cache_session.mining_cache_size_bytes + SizeDiff
						}}
				end;
			{ok, Value1, ReservationSizeAdjustment} when ReservationSizeAdjustment < 0 ->
				SizeDiff = cached_value_size(Value1) - cached_value_size(Value0),
				SessionAvailableSize = available_size(Cache0) + Session#ar_mining_cache_session.reserved_mining_cache_bytes,
				CacheLimit = get_limit(Cache0),
				case SizeDiff > SessionAvailableSize of
					true when CacheLimit =/= 0 -> {error, cache_limit_exceeded};
					_ ->
						{ok, Session#ar_mining_cache_session{
							mining_cache = maps:put(Key, Value1, Session#ar_mining_cache_session.mining_cache),
							reserved_mining_cache_bytes = max(0, Session#ar_mining_cache_session.reserved_mining_cache_bytes - SizeDiff + ReservationSizeAdjustment),
							mining_cache_size_bytes = Session#ar_mining_cache_session.mining_cache_size_bytes + SizeDiff
						}}
				end;
			Other ->
				?LOG_WARNING([{event, unexpected_return_value_from_with_cached_value}, {value, Other}]),
				{error, unexpected_return_value_from_with_cached_value}
		end
	end, Cache0).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% Returns the size of the cached data in bytes.
cached_value_size(#ar_mining_cache_value{chunk1 = Chunk1, chunk2 = Chunk2}) ->
  MaybeBinarySize = fun
		(undefined) -> 0;
		(Binary) -> byte_size(Binary)
  end,
  MaybeBinarySize(Chunk1) + MaybeBinarySize(Chunk2).

%% Executes the `Fun` function with the chunk cache session as argument.
%% If the session does not exist, it returns an error without executing the `Fun`.
%% The `Fun` function should return either:
%% - a new chunk cache session `{ok, Session}`, which will be used to replace the old one.
%% - a new chunk cache session with return value `{ok, Return, Session}`, which will
%%   be used to replace the old cache session and return a value to the caller.
%% - an error `{error, Reason}` to report back to the caller.
with_mining_cache_session(SessionId, Fun, Cache0) ->
	case maps:is_key(SessionId, Cache0#ar_mining_cache.mining_cache_sessions) of
		true ->
			case Fun(maps:get(SessionId, Cache0#ar_mining_cache.mining_cache_sessions)) of
				{ok, Return, Session1} -> {ok, Return, Cache0#ar_mining_cache{
					mining_cache_sessions = maps:put(SessionId, Session1, Cache0#ar_mining_cache.mining_cache_sessions)
				}};
				{ok, Session1} -> {ok, Cache0#ar_mining_cache{
					mining_cache_sessions = maps:put(SessionId, Session1, Cache0#ar_mining_cache.mining_cache_sessions)
				}};
				{error, Reason} -> {error, Reason}
			end;
		false ->
			{error, session_not_found}
	end.

%% Searches for anomalies in the mining cache session.
%% If the actual cache size is different from the expected cache size,
%% it will log a warning.
%% If the reserved cache size is different from 0, it will log a warning.
%% It will also search for invalid cache values, e.g. missing chunks, or failed
%% invariants.
%%
%% Perhaps it is a good idea to put this under a config flag, disabled by default.
maybe_search_for_anomalies(SessionId, #ar_mining_cache_session{
  mining_cache = MiningCache,
  mining_cache_size_bytes = MiningCacheSize,
  reserved_mining_cache_bytes = ReservedMiningCacheBytes
}) ->
	ActualCacheSize = maybe_search_for_anomalies_cache_values(SessionId, MiningCache),
	case {ActualCacheSize, MiningCacheSize} of
		{0, 0} -> ok;
		{EqualSize, EqualSize} -> ?LOG_WARNING([
			{event, mining_cache_anomaly}, {anomaly, cache_size_non_zero},
			{session_id, SessionId}, {actual_size, ActualCacheSize}, {reported_size, MiningCacheSize}]);
		{_, _} -> ?LOG_WARNING([
			{event, mining_cache_anomaly}, {anomaly, cache_size_mismatch},
			{session_id, SessionId}, {actual_size, ActualCacheSize}, {reported_size, MiningCacheSize}])
	end,
	case ReservedMiningCacheBytes of
		0 -> ok;
		_ -> ?LOG_WARNING([
			{event, mining_cache_anomaly}, {anomaly, reserved_size_non_zero},
			{session_id, SessionId}, {actual_size, ReservedMiningCacheBytes}, {expected_size, 0}])
	end,
	?LOG_DEBUG([{event, mining_cache_anomaly}, {anomaly, mining_cache_anomaly_search_completed}, {session_id, SessionId}]);
maybe_search_for_anomalies(SessionId, _InvalidSession) ->
	?LOG_ERROR([{event, mining_cache_anomaly}, {anomaly, invalid_session_type}, {session_id, SessionId}]),
	ok.

maybe_search_for_anomalies_cache_values(SessionId, MiningCache) when is_map(MiningCache) ->
	OuterAcc0 = {_Anomalies = #{}, _ActualSize = 0},
	{Anomalies, ActualSize} = maps:fold(fun(Key, Value, {Anomalies0, ActualSize0}) ->
		Anomalies1 = lists:foldl(fun(Check, Anomalies) -> Check({Key, Value}, Anomalies) end, Anomalies0, [
			fun maybe_search_for_anomalies_cache_values_chunk1_missing/2,
			fun maybe_search_for_anomalies_cache_values_chunk1_stale/2,
			fun maybe_search_for_anomalies_cache_values_chunk2_missing/2,
			fun maybe_search_for_anomalies_cache_values_chunk2_stale/2,
			fun maybe_search_for_anomalies_cache_values_h1_missing/2,
			fun maybe_search_for_anomalies_cache_values_h2_missing/2,
			fun maybe_search_for_anomalies_cache_values_h1_passes_diff_checks_present/2
		]),
		{Anomalies1, ActualSize0 + cached_value_size(Value)}
	end, OuterAcc0, MiningCache),
	case maps:size(Anomalies) > 0 of
		true -> ?LOG_WARNING([
			{event, mining_cache_anomaly}, {anomaly, cached_values_anomalies},
			{anomalies, Anomalies}, {session_id, SessionId}]);
		false -> ok
	end,
	ActualSize;
maybe_search_for_anomalies_cache_values(SessionId, _InvalidCache) ->
	?LOG_ERROR([{event, mining_cache_anomaly}, {anomaly, invalid_cache_type}, {session_id, SessionId}]),
	0.

maybe_search_for_anomalies_cache_values_chunk1_missing({
	Key,
	#ar_mining_cache_value{chunk1 = undefined, chunk1_missing = false} = Value
}, Anomalies) ->
	maps:update_with(chunk1_missing, fun(V) -> V + 1 end, 1,
		maps:update_with(chunk1_missing_sample, fun(V) -> V end, {Key, Value}, Anomalies));
maybe_search_for_anomalies_cache_values_chunk1_missing({_, _}, Anomalies) ->
	Anomalies.

maybe_search_for_anomalies_cache_values_chunk1_stale({
	Key,
	#ar_mining_cache_value{chunk1 = Chunk1, chunk1_missing = true} = Value
}, Anomalies) when undefined =/= Chunk1 ->
	maps:update_with(chunk1_stale, fun(V) -> V + 1 end, 1,
		maps:update_with(chunk1_stale_sample, fun(V) -> V end, {Key, Value}, Anomalies));
maybe_search_for_anomalies_cache_values_chunk1_stale({_, _}, Anomalies) ->
	Anomalies.

maybe_search_for_anomalies_cache_values_chunk2_missing({
	Key,
	#ar_mining_cache_value{chunk2 = undefined, chunk2_missing = false} = Value
}, Anomalies) ->
	maps:update_with(chunk2_missing, fun(V) -> V + 1 end, 1,
		maps:update_with(chunk2_missing_sample, fun(V) -> V end, {Key, Value}, Anomalies));
maybe_search_for_anomalies_cache_values_chunk2_missing({_, _}, Anomalies) ->
	Anomalies.

maybe_search_for_anomalies_cache_values_chunk2_stale({
	Key,
	#ar_mining_cache_value{chunk2 = Chunk2, chunk2_missing = true} = Value
}, Anomalies) when undefined =/= Chunk2 ->
	maps:update_with(chunk2_stale, fun(V) -> V + 1 end, 1,
		maps:update_with(chunk2_stale_sample, fun(V) -> V end, {Key, Value}, Anomalies));
maybe_search_for_anomalies_cache_values_chunk2_stale({_, _}, Anomalies) ->
	Anomalies.

maybe_search_for_anomalies_cache_values_h1_missing({
	Key,
	#ar_mining_cache_value{h1 = undefined, chunk1 = Chunk1} = Value
}, Anomalies)
when undefined =/= Chunk1 ->
	maps:update_with(h1_missing, fun(V) -> V + 1 end, 1,
		maps:update_with(h1_missing_sample, fun(V) -> V end, {Key, Value}, Anomalies));
maybe_search_for_anomalies_cache_values_h1_missing({_, _}, Anomalies) ->
	Anomalies.

maybe_search_for_anomalies_cache_values_h2_missing({
	Key,
	#ar_mining_cache_value{h2 = undefined, chunk2 = Chunk2} = Value
}, Anomalies)
when undefined =/= Chunk2 ->
	maps:update_with(h2_missing, fun(V) -> V + 1 end, 1,
		maps:update_with(h2_missing_sample, fun(V) -> V end, {Key, Value}, Anomalies));
maybe_search_for_anomalies_cache_values_h2_missing({_, _}, Anomalies) ->
	Anomalies.

maybe_search_for_anomalies_cache_values_h1_passes_diff_checks_present({
	Key,
	#ar_mining_cache_value{h1_passes_diff_checks = true} = Value
}, Anomalies) ->
	maps:update_with(h1_passes_diff_checks_present, fun(V) -> V + 1 end, 1,
		maps:update_with(h1_passes_diff_checks_present_sample, fun(V) -> V end, {Key, Value}, Anomalies));
maybe_search_for_anomalies_cache_values_h1_passes_diff_checks_present({_, _}, Anomalies) ->
	Anomalies.



%%%===================================================================
%%% Tests.
%%%===================================================================

-include_lib("eunit/include/eunit.hrl").

cache_size_test() ->
	Cache = new(),
	?assertEqual(0, cache_size(Cache)).

add_session_test() ->
	Cache0 = new(),
	SessionId0 = session0,
	Cache1 = add_session(SessionId0, Cache0),
	?assert(session_exists(SessionId0, Cache1)),
	?assertEqual(0, cache_size(Cache1)),
	Cache1 = add_session(SessionId0, Cache1),
	?assertEqual([SessionId0], get_sessions(Cache1)).

add_session_limit_test() ->
	Cache0 = new(),
	Cache1 = add_session(session0, Cache0),
	Cache2 = add_session(session1, Cache1),
	Cache3 = add_session(session2, Cache2),
	Cache4 = add_session(session3, Cache3),
	?assertEqual([session0, session1, session2, session3], get_sessions(Cache4)),
	?assertEqual(0, cache_size(Cache4)),
	Cache5 = add_session(session4, Cache4),
	?assertEqual([session1, session2, session3, session4], get_sessions(Cache5)),
	?assertEqual(0, cache_size(Cache5)).

reserve_test() ->
	Cache0 = new(1024),
	SessionId0 = session0,
	ChunkId = chunk0,
	Data = <<"chunk_data">>,
	ReservedSize = 100,
	%% Add session
	Cache1 = add_session(SessionId0, Cache0),
	%% Reserve space
	{ok, Cache2} = reserve_for_session(SessionId0, ReservedSize, Cache1),
	?assertEqual(ReservedSize, cache_size(Cache2)),
	?assertMatch({ok, ReservedSize}, reserved_size(SessionId0, Cache2)),
	%% Add chunk1
	{ok, Cache3} = with_cached_value(ChunkId, SessionId0, Cache2, fun(Value) ->
		{ok, Value#ar_mining_cache_value{chunk1 = Data}}
	end),
	?assertEqual(ReservedSize, cache_size(Cache3)),
	?assertEqual(byte_size(Data), actual_cache_size(Cache3)),
	ExpectedReservedSize = ReservedSize - byte_size(Data),
	?assertMatch({ok, ExpectedReservedSize}, reserved_size(SessionId0, Cache3)),
	%% Reserve more space
	?assertMatch({error, cache_limit_exceeded}, reserve_for_session(SessionId0, 1024 + ReservedSize, Cache3)),
	%% Drop session
	Cache4 = drop_session(SessionId0, Cache3),
	?assertEqual(0, cache_size(Cache4)).

release_test() ->
	Cache0 = new(1024),
	SessionId0 = session0,
	ChunkId = chunk0,
	Data = <<"chunk_data">>,
	ReservedSize = 100,
	%% Add session
	Cache1 = add_session(SessionId0, Cache0),
	%% Reserve space
	{ok, Cache2} = reserve_for_session(SessionId0, ReservedSize, Cache1),
	?assertEqual(ReservedSize, cache_size(Cache2)),
	?assertMatch({ok, ReservedSize}, reserved_size(SessionId0, Cache2)),
	%% Add chunk1
	{ok, Cache3} = with_cached_value(ChunkId, SessionId0, Cache2, fun(Value) ->
		{ok, Value#ar_mining_cache_value{chunk1 = Data}}
	end),
	ExpectedReservedSize = ReservedSize - byte_size(Data),
	?assertMatch({ok, ExpectedReservedSize}, reserved_size(SessionId0, Cache3)),
	?assertEqual(byte_size(Data), actual_cache_size(Cache3)),
	%% Release space
	{ok, Cache4} = release_for_session(SessionId0, 10, Cache3),
	ExpectedReleasedReserveSize = ExpectedReservedSize - 10,
	?assertMatch({ok, ExpectedReleasedReserveSize}, reserved_size(SessionId0, Cache4)),
	?assertEqual(byte_size(Data), actual_cache_size(Cache4)),
	%% Drop session
	Cache5 = drop_session(SessionId0, Cache4),
	?assertEqual(0, cache_size(Cache5)).

with_cached_value_add_chunk_test() ->
	Cache0 = new(1024),
	ChunkId = chunk0,
	Data = <<"chunk_data">>,
	SessionId0 = session0,
	%% Add session
	Cache1 = add_session(SessionId0, Cache0),
	%% Add chunk1
	{ok, Cache2} = with_cached_value(ChunkId, SessionId0, Cache1, fun(Value) ->
		{ok, Value#ar_mining_cache_value{chunk1 = Data}}
	end),
	?assertEqual(byte_size(Data), cache_size(Cache2)),
	%% Add chunk2
	{ok, Cache3} = with_cached_value(ChunkId, SessionId0, Cache2, fun(Value) ->
		{ok, Value#ar_mining_cache_value{chunk2 = Data}}
	end),
	?assertEqual(byte_size(Data) * 2, cache_size(Cache3)).

with_cached_value_add_hash_test() ->
	Cache0 = new(),
	ChunkId = chunk0,
	Hash = <<"hash">>,
	SessionId0 = session0,
	%% Add session
	Cache1 = add_session(SessionId0, Cache0),
	%% Add h1
	{ok, Cache2} = with_cached_value(ChunkId, SessionId0, Cache1, fun(Value) ->
		{ok, Value#ar_mining_cache_value{h1 = Hash}}
	end),
	?assertEqual(0, cache_size(Cache2)),
	%% Add h2
	{ok, Cache3} = with_cached_value(ChunkId, SessionId0, Cache2, fun(Value) ->
		{ok, Value#ar_mining_cache_value{h2 = Hash}}
	end),
	?assertEqual(0, cache_size(Cache3)).

with_cached_value_drop_test() ->
	Cache0 = new(1024),
	ChunkId = chunk0,
	Data = <<"chunk_data">>,
	SessionId0 = session0,
	%% Add session
	Cache1 = add_session(SessionId0, Cache0),
	%% Add chunk1
	{ok, Cache2} = with_cached_value(ChunkId, SessionId0, Cache1, fun(Value) ->
		{ok, Value#ar_mining_cache_value{chunk1 = Data}}
	end),
	?assertEqual(byte_size(Data), cache_size(Cache2)),
	%% Drop
	{ok, Cache3} = with_cached_value(ChunkId, SessionId0, Cache2, fun(_Value) ->
		{ok, drop}
	end),
	?assertEqual(0, cache_size(Cache3)),
	?assertEqual(0, actual_cache_size(Cache3)).

set_limit_test() ->
	Cache0 = new(),
	Data = <<"chunk_data">>,
	SessionId0 = session0,
	%% Add session
	Cache1 = add_session(SessionId0, Cache0),
	%% Add chunk1
	ChunkId0 = chunk0,
	{ok, Cache2} = with_cached_value(ChunkId0, SessionId0, Cache1, fun(Value) ->
		{ok, Value#ar_mining_cache_value{chunk1 = Data}}
	end),
	?assertEqual(byte_size(Data), cache_size(Cache2)),
	%% Set limit
	ChunkId1 = chunk1,
	Cache3 = set_limit(5, Cache2),
	%% Try to add chunk2
	{error, cache_limit_exceeded} = with_cached_value(ChunkId1, SessionId0, Cache3, fun(Value) ->
		{ok, Value#ar_mining_cache_value{chunk1 = Data}}
	end),
	?assertEqual(byte_size(Data), cache_size(Cache3)).

drop_session_test() ->
	Cache0 = new(1024),
	ChunkId = chunk0,
	Data = <<"chunk_data">>,
	SessionId0 = session0,
	%% Add session
	Cache1 = add_session(SessionId0, Cache0),
	%% Add chunk1
	{ok, Cache2} = with_cached_value(ChunkId, SessionId0, Cache1, fun(Value) ->
		{ok, Value#ar_mining_cache_value{chunk1 = Data}}
	end),
	?assertEqual(byte_size(Data), cache_size(Cache2)),
	%% Drop session
	Cache3 = drop_session(SessionId0, Cache2),
	?assertNot(session_exists(SessionId0, Cache3)),
	?assertEqual(0, cache_size(Cache3)).
