-module(ar_chunk_cache).
-include_lib("arweave/include/ar_chunk_cache.hrl").

-export([
  new/0, new/1, set_limit/2, cache_size/1, available_size/1, reserved_size/2,
  add_group/2, reserve/3, drop_group/2, group_exists/2, get_groups/1,
  add_chunk_to_existing_group/4, add_chunk/4, take_chunk/3,
  drop_chunk/3, chunk_exists/3
]).



-define(CACHE_VALUE(Chunk, Meta), {Chunk, Meta}).



%%%===================================================================
%%% Public API.
%%%===================================================================



-spec new() ->
  Cache :: #ar_chunk_cache{}.

new() -> #ar_chunk_cache{chunk_cache_limit_bytes = 0}.



-spec new(Limit :: pos_integer()) ->
  Cache :: #ar_chunk_cache{}.

new(Limit) -> #ar_chunk_cache{chunk_cache_limit_bytes = Limit}.



-spec set_limit(Limit :: pos_integer(), Cache :: #ar_chunk_cache{}) ->
  Cache :: #ar_chunk_cache{}.

set_limit(Limit, Cache) ->
  Cache#ar_chunk_cache{chunk_cache_limit_bytes = Limit}.




-spec cache_size(Cache :: #ar_chunk_cache{}) ->
  Size :: non_neg_integer().

cache_size(Cache) ->
  maps:fold(
    fun(_, #ar_chunk_cache_group{chunk_cache_size_bytes = Size, reserved_chunk_cache_bytes = ReservedSize}, Acc) ->
      Acc + Size + ReservedSize
    end,
    0,
    Cache#ar_chunk_cache.chunk_cache_groups
  ).



-spec available_size(Cache :: #ar_chunk_cache{}) ->
  Size :: non_neg_integer().

available_size(Cache) ->
  Cache#ar_chunk_cache.chunk_cache_limit_bytes - cache_size(Cache).



-spec reserved_size(GroupId :: term(), Cache0 :: #ar_chunk_cache{}) ->
  {ok, Size :: non_neg_integer()} | {error, Reason :: term()}.

reserved_size(GroupId, Cache0) ->
  with_chunk_cache_group(GroupId, fun(#ar_chunk_cache_group{reserved_chunk_cache_bytes = ReservedSize}) -> {ok, ReservedSize} end, Cache0).



-spec add_group(GroupId :: term(), Cache0 :: #ar_chunk_cache{}) ->
  Cache1 :: #ar_chunk_cache{}.
add_group(GroupId, Cache0) ->
  case maps:is_key(GroupId, Cache0#ar_chunk_cache.chunk_cache_groups) of
    true -> Cache0;
    false -> Cache0#ar_chunk_cache{chunk_cache_groups = maps:put(GroupId, #ar_chunk_cache_group{}, Cache0#ar_chunk_cache.chunk_cache_groups)}
  end.



-spec reserve(GroupId :: term(), Size :: non_neg_integer(), Cache0 :: #ar_chunk_cache{}) ->
  {ok, Cache1 :: #ar_chunk_cache{}} | {error, Reason :: term()}.

reserve(GroupId, Size, Cache0) ->
  case available_size(Cache0) < Size of
    true -> {error, cache_limit_exceeded};
    false ->
      map_chunk_cache_group(GroupId, fun(#ar_chunk_cache_group{reserved_chunk_cache_bytes = ReservedSize}) ->
        {ok, #ar_chunk_cache_group{reserved_chunk_cache_bytes = ReservedSize + Size}}
      end, Cache0)
  end.



-spec drop_group(GroupId :: term(), Cache0 :: #ar_chunk_cache{}) ->
  Cache1 :: #ar_chunk_cache{}.

drop_group(GroupId, Cache0) ->
  Cache0#ar_chunk_cache{chunk_cache_groups = maps:remove(GroupId, Cache0#ar_chunk_cache.chunk_cache_groups)}.



-spec group_exists(GroupId :: term(), Cache0 :: #ar_chunk_cache{}) ->
  Exists :: boolean().

group_exists(GroupId, Cache0) ->
  maps:is_key(GroupId, Cache0#ar_chunk_cache.chunk_cache_groups).



-spec get_groups(Cache0 :: #ar_chunk_cache{}) ->
  Groups :: [term()].

get_groups(Cache0) ->
  maps:keys(Cache0#ar_chunk_cache.chunk_cache_groups).



-spec add_chunk_to_existing_group(
  GroupId :: term(),
  ChunkId :: term(),
  Chunk :: binary() | ?CACHE_VALUE(binary(), #{}),
  Cache0 :: #ar_chunk_cache{}
) ->
  {ok, Cache1 :: #ar_chunk_cache{}} | {error, Reason :: term()}.

add_chunk_to_existing_group(GroupId, ChunkId, ?CACHE_VALUE(Chunk, ChunkMeta), Cache0) ->
  case (byte_size(Chunk) + cache_size(Cache0)) >  Cache0#ar_chunk_cache.chunk_cache_limit_bytes of
    true when Cache0#ar_chunk_cache.chunk_cache_limit_bytes =/= 0 -> {error, cache_limit_exceeded};
    _ -> map_chunk_cache_group(GroupId, add_chunk_map_fun(ChunkId, Chunk, ChunkMeta), Cache0)
  end;

add_chunk_to_existing_group(GroupId, ChunkId, Chunk, Cache0) ->
  add_chunk_to_existing_group(GroupId, ChunkId, ?CACHE_VALUE(Chunk, #{}), Cache0).



-spec add_chunk(
  GroupId :: term(),
  ChunkId :: term(),
  Chunk :: binary() | {binary(), #{}},
  Cache0 :: #ar_chunk_cache{}
) ->
  {ok, Cache1 :: #ar_chunk_cache{}} | {error, Reason :: term()}.

add_chunk(GroupId, ChunkId, ?CACHE_VALUE(Chunk, ChunkMeta), Cache0) ->
  case (byte_size(Chunk) + cache_size(Cache0)) >  Cache0#ar_chunk_cache.chunk_cache_limit_bytes of
    true when Cache0#ar_chunk_cache.chunk_cache_limit_bytes =/= 0 -> {error, cache_limit_exceeded};
    _ -> map_chunk_cache_group(GroupId, add_chunk_map_fun(ChunkId, Chunk, ChunkMeta), Cache0, true)
  end;

add_chunk(GroupId, ChunkId, Chunk, Cache0) ->
  add_chunk(GroupId, ChunkId, ?CACHE_VALUE(Chunk, #{}), Cache0).



-spec take_chunk(GroupId :: term(), ChunkId :: term(), Cache0 :: #ar_chunk_cache{}) ->
  {ok, Chunk :: binary(), Cache1 :: #ar_chunk_cache{}} | {error, Reason :: term()}.

take_chunk(GroupId, ChunkId, Cache0) ->
  map_chunk_cache_group(GroupId, fun(#ar_chunk_cache_group{
    chunk_cache = ChunkCache0,
    chunk_cache_size_bytes = ChunkCacheSize0
  } = Group0) ->
    case maps:take(ChunkId, ChunkCache0) of
      {?CACHE_VALUE(Chunk, _Meta) = RetVal, ChunkCache1} -> {ok, RetVal, Group0#ar_chunk_cache_group{
        chunk_cache = ChunkCache1,
        chunk_cache_size_bytes = ChunkCacheSize0 - byte_size(Chunk)
      }};
      error -> {error, chunk_not_found}
    end
  end, Cache0).



-spec drop_chunk(GroupId :: term(), ChunkId :: term(), Cache0 :: #ar_chunk_cache{}) ->
  {ok, Cache1 :: #ar_chunk_cache{}} | {error, Reason :: term()}.

drop_chunk(GroupId, ChunkId, Cache0) ->
  case take_chunk(GroupId, ChunkId, Cache0) of
    {ok, _, Cache1} -> {ok, Cache1};
    {error, chunk_not_found} -> {ok, Cache0};
    {error, Reason} -> {error, Reason}
  end.


-spec chunk_exists(GroupId :: term(), ChunkId :: term(), Cache0 :: #ar_chunk_cache{}) ->
  {ok, boolean()} | {error, Reason :: term()}.

chunk_exists(GroupId, ChunkId, Cache0) ->
  with_chunk_cache_group(GroupId, fun(Group) ->
    {ok, maps:is_key(ChunkId, Group#ar_chunk_cache_group.chunk_cache)}
  end, Cache0).



%%%===================================================================
%%% Private functions.
%%%===================================================================



%% Generates a closure that captures the `ChunkId` and `Chunk` and returns a function
%% that can be used to add a chunk to the chunk cache group.

add_chunk_map_fun(ChunkId, Chunk, ChunkMeta) ->
  fun(#ar_chunk_cache_group{
    chunk_cache = ChunkCache0,
    chunk_cache_size_bytes = ChunkCacheSize0,
    reserved_chunk_cache_bytes = ReservedChunkCacheBytes0
  } = Group0) ->
    case maps:find(ChunkId, ChunkCache0) of
      {ok, ?CACHE_VALUE(Chunk0, _Meta)} ->
        {ok, Group0#ar_chunk_cache_group{
          chunk_cache = maps:put(ChunkId, {Chunk, ChunkMeta}, ChunkCache0),
          chunk_cache_size_bytes = ChunkCacheSize0 + byte_size(Chunk) - byte_size(Chunk0),
          reserved_chunk_cache_bytes = max(0, ReservedChunkCacheBytes0 - byte_size(Chunk) + byte_size(Chunk0))
        }};
      error when ReservedChunkCacheBytes0 > 0 ->
        {ok, Group0#ar_chunk_cache_group{
          chunk_cache = maps:put(ChunkId, {Chunk, ChunkMeta}, ChunkCache0),
          chunk_cache_size_bytes = ChunkCacheSize0 + byte_size(Chunk),
          reserved_chunk_cache_bytes = max(0, ReservedChunkCacheBytes0 - byte_size(Chunk))
        }};
      error ->
        {ok, Group0#ar_chunk_cache_group{
          chunk_cache = maps:put(ChunkId, {Chunk, ChunkMeta}, ChunkCache0),
          chunk_cache_size_bytes = ChunkCacheSize0 + byte_size(Chunk),
          reserved_chunk_cache_bytes = 0
        }}
    end
  end.



%% Executes the `Fun` function with the chunk cache group as argument.
%% If the group does not exist, it returns an error without executing the `Fun`.

with_chunk_cache_group(GroupId, Fun, Cache0) ->
  case maps:is_key(GroupId, Cache0#ar_chunk_cache.chunk_cache_groups) of
    true ->
      Fun(maps:get(GroupId, Cache0#ar_chunk_cache.chunk_cache_groups));
    false ->
      {error, group_not_found}
  end.



%% Executes the `Fun` function with the chunk cache group as argument.
%% If the group does not exist, it returns an error without executing the `Fun`,
%% unless `InsertIfNotFound` is true (false by default).
%% The `Fun` function should return either:
%% - a new chunk cache group `{ok, Group}`, which will be used to replace the old one.
%% - a new chunk cache group with return value `{ok, Return, Group}`, which will
%%   be used to replace the old cache group and return a value to the caller.
%% - an error `{error, Reason}` to report back to the caller.

map_chunk_cache_group(GroupId, Fun, Cache0) ->
  map_chunk_cache_group(GroupId, Fun, Cache0, false).

map_chunk_cache_group(GroupId, Fun, Cache0, InsertIfNotFound) ->
  case maps:find(GroupId, Cache0#ar_chunk_cache.chunk_cache_groups) of
    {ok, Group0} ->
      case Fun(Group0) of
        {ok, Group1} ->
          Cache1 = Cache0#ar_chunk_cache{
            chunk_cache_groups = maps:put(GroupId, Group1, Cache0#ar_chunk_cache.chunk_cache_groups)
          },
          {ok, Cache1};
        {ok, RetVal, Group1} ->
          Cache1 = Cache0#ar_chunk_cache{
            chunk_cache_groups = maps:put(GroupId, Group1, Cache0#ar_chunk_cache.chunk_cache_groups)
          },
          {ok, RetVal, Cache1};
        {error, Reason} -> {error, Reason}
      end;
    error when InsertIfNotFound ->
      case Fun(#ar_chunk_cache_group{}) of
        {ok, Group1} ->
          Cache1 = Cache0#ar_chunk_cache{
            chunk_cache_groups = maps:put(GroupId, Group1, Cache0#ar_chunk_cache.chunk_cache_groups)
          },
          {ok, Cache1};
        {error, Reason} -> {error, Reason}
      end;
    error ->
      {error, group_not_found}
  end.



%%%===================================================================
%%% Tests.
%%%===================================================================



-include_lib("eunit/include/eunit.hrl").



cache_size_test() ->
  Cache = new(1024),
  ?assertEqual(0, cache_size(Cache)).



add_group_test() ->
  Cache0 = new(1024),
  GroupId0 = session0,
  Cache1 = add_group(GroupId0, Cache0),
  ?assert(group_exists(GroupId0, Cache1)),
  ?assertEqual(0, cache_size(Cache1)),
  Cache1 = add_group(GroupId0, Cache1),
  ?assertEqual([GroupId0], get_groups(Cache1)).



reserve_test() ->
  Cache0 = new(1024),
  GroupId0 = session0,
  ChunkId = chunk0,
  Data = <<"chunk_data">>,
  ReservedSize = 100,

  Cache1 = add_group(GroupId0, Cache0),

  {ok, Cache2} = reserve(GroupId0, ReservedSize, Cache1),
  ?assertEqual(ReservedSize, cache_size(Cache2)),
  ?assertMatch({ok, ReservedSize}, reserved_size(GroupId0, Cache2)),

  {ok, Cache3} = add_chunk(GroupId0, ChunkId, Data, Cache2),
  ?assertEqual(ReservedSize, cache_size(Cache3)),
  ExpectedReservedSize = ReservedSize - byte_size(Data),
  ?assertMatch({ok, ExpectedReservedSize}, reserved_size(GroupId0, Cache3)),

  ?assertMatch({error, cache_limit_exceeded}, reserve(GroupId0, 1024 + ReservedSize, Cache3)),

  Cache4 = drop_group(GroupId0, Cache3),
  ?assertEqual(0, cache_size(Cache4)).



add_chunk_to_existing_group_test() ->
  Cache0 = new(1024),
  ChunkId = chunk0,
  Data = <<"chunk_data">>,

  GroupId0 = session0,
  Cache1 = add_group(GroupId0, Cache0),
  {ok, Cache2} = add_chunk_to_existing_group(GroupId0, ChunkId, Data, Cache1),
  ?assertEqual({ok, true}, chunk_exists(GroupId0, ChunkId, Cache2)),
  ?assertEqual(byte_size(Data), cache_size(Cache2)),
  {error, chunk_already_exists} = add_chunk_to_existing_group(GroupId0, ChunkId, Data, Cache2),

  GroupId1 = session1,
  {error, group_not_found} = add_chunk_to_existing_group(GroupId1, ChunkId, Data, Cache2).



add_chunk_test() ->
  Cache0 = new(1024),
  ChunkId = chunk0,
  Data = <<"chunk_data">>,

  GroupId0 = session0,
  {ok, Cache1} = add_chunk(GroupId0, ChunkId, Data, Cache0),
  ?assertEqual({ok, true}, chunk_exists(GroupId0, ChunkId, Cache1)),
  ?assertEqual(byte_size(Data), cache_size(Cache1)),

  GroupId1 = session1,
  {ok, Cache2} = add_chunk(GroupId1, ChunkId, Data, Cache1),
  ?assertEqual({ok, true}, chunk_exists(GroupId1, ChunkId, Cache2)),
  ?assertEqual(byte_size(Data) * 2, cache_size(Cache2)).



add_chunk_meta_test() ->
  Cache0 = new(1024),
  ChunkId = chunk0,
  Data = <<"chunk_data">>,
  Meta = #{foo => bar},

  GroupId0 = session0,
  {ok, Cache1} = add_chunk(GroupId0, ChunkId, {Data, Meta}, Cache0),
  ?assertEqual({ok, true}, chunk_exists(GroupId0, ChunkId, Cache1)),
  ?assertEqual(byte_size(Data), cache_size(Cache1)),

  GroupId1 = session1,
  {ok, Cache2} = add_chunk(GroupId1, ChunkId, {Data, Meta}, Cache1),
  ?assertEqual({ok, true}, chunk_exists(GroupId1, ChunkId, Cache2)),
  ?assertEqual(byte_size(Data) * 2, cache_size(Cache2)).



take_chunk_test() ->
  Cache0 = new(1024),
  GroupId0 = session0,
  ChunkId0 = chunk0,
  Data = <<"chunk_data">>,
  {ok, Cache1} = add_chunk(GroupId0, ChunkId0, Data, Cache0),
  ?assertEqual(byte_size(Data), cache_size(Cache1)),
  {ok, {Data, #{}}, Cache2} = take_chunk(GroupId0, ChunkId0, Cache1),
  ?assertEqual(0, cache_size(Cache2)).



set_limit_test() ->
  Cache0 = new(),
  Data = <<"chunk_data">>,
  GroupId0 = session0,

  ChunkId0 = chunk0,
  {ok, Cache1} = add_chunk(GroupId0, ChunkId0, Data, Cache0),
  ?assertEqual(byte_size(Data), cache_size(Cache1)),

  ChunkId1 = chunk1,
  Cache2 = set_limit(5, Cache1),
  {error, cache_limit_exceeded} = add_chunk(GroupId0, ChunkId1, Data, Cache2),
  ?assertEqual(byte_size(Data), cache_size(Cache2)).


drop_chunk_test() ->
  Cache0 = new(1024),
  ChunkId = chunk0,
  Data = <<"chunk_data">>,

  GroupId0 = session0,
  {ok,  Cache1} = add_chunk(GroupId0, ChunkId, Data, Cache0),
  ?assertEqual(byte_size(Data), cache_size(Cache1)),

  {ok, Cache2} = drop_chunk(GroupId0, ChunkId, Cache1),
  ?assertMatch({ok, false}, chunk_exists(GroupId0, ChunkId, Cache2)),
  ?assertEqual(0, cache_size(Cache2)).



drop_group_test() ->
  Cache0 = new(1024),
  ChunkId = chunk0,
  Data = <<"chunk_data">>,

  GroupId0 = session0,
  {ok, Cache1} = add_chunk(GroupId0, ChunkId, Data, Cache0),
  ?assertEqual({ok, true}, chunk_exists(GroupId0, ChunkId, Cache1)),
  ?assertEqual(byte_size(Data), cache_size(Cache1)),

  Cache2 = drop_group(GroupId0, Cache1),
  ?assertNot(group_exists(GroupId0, Cache2)),
  ?assertEqual(0, cache_size(Cache2)).
