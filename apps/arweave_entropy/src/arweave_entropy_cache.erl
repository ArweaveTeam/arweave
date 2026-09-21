-module(arweave_entropy_cache).

-export([get/1, clean_up_space/2, put/3, put_with_limit/4, total_size/0]).

-ifdef(AR_TEST).
-export([get/2, put/5, clean_up_space/4, get_fetched_key_count/2]).
-endif.

-include_lib("arweave_constants/include/arweave_constants.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Return the stored value, if any, for the given Key.
get(Key) ->
    get(Key, ar_entropy_cache).

%% @doc Evict entries, oldest first, until Size more bytes fit under MaxSize.
%% An entry that was fetched since it was inserted, or since it was last
%% spared, is moved to the back of the eviction order and spared once instead
%% of evicted, so entropies still in use outlive the ones nobody reads.
clean_up_space(Size, MaxSize) ->
    Table = ar_entropy_cache,
    OrderedKeyTable = ar_entropy_cache_ordered_keys,
    clean_up_space(Size, MaxSize, Table, OrderedKeyTable).

%% @doc Evict the oldest entries needed to fit Size bytes, then store Value.
put_with_limit(Key, Value, Size, MaxSize) ->
    clean_up_space(Size, MaxSize),
    put(Key, Value, Size).

%% @doc Store the given Value in the cache. Associate it with the given Size and
%% increase the total cache size accordingly.
put(Key, Value, Size) ->
    Table = ar_entropy_cache,
    OrderedKeyTable = ar_entropy_cache_ordered_keys,
    put(Key, Value, Size, Table, OrderedKeyTable).

%% @doc Return the size of the cache.
total_size() ->
    Table = ar_entropy_cache,
    total_size(Table).

%%%===================================================================
%%% Private functions.
%%%===================================================================

total_size(Table) ->
    ets:lookup_element(Table, total_size, 2, 0).

get(Key, Table) ->
    case ets:lookup(Table, {key, Key}) of
        [] ->
            not_found;
        [{{key, Key}, Value}] ->
            %% Track the number of used keys per entropy to estimate the efficiency
            %% of the cache.
            ets:update_counter(
                Table,
                {fetched_key_count, Key},
                1,
                {{fetched_key_count, Key}, 0}
            ),
            {ok, Value}
    end.

clean_up_space(Size, MaxSize, Table, OrderedKeyTable) ->
    %% Sparing each entry at most once per clean-up bounds the pass, so a
    %% cache whose every entry is in use still frees the space. SecondChances
    %% counts how many entries have been spared so we know when they have
    %% all been spared (at which point we evict regardless).
    SecondChances = ets:info(OrderedKeyTable, size),
    clean_up_space(Size, MaxSize, Table, OrderedKeyTable, SecondChances).

clean_up_space(Size, MaxSize, Table, OrderedKeyTable, SecondChances) ->
    TotalSize = total_size(Table),
    case TotalSize + Size > MaxSize of
        true ->
            case take_eviction_candidate(OrderedKeyTable) of
                none ->
                    ok;
                {Key, ElementSize, LastFetchedCount} ->
                    CurrentFetchedCount = get_fetched_key_count(Table, Key),
                    case
                        CurrentFetchedCount > LastFetchedCount andalso
                            SecondChances > 0
                    of
                        true ->
                            requeue(
                                Key,
                                ElementSize,
                                CurrentFetchedCount,
                                OrderedKeyTable
                            ),
                            clean_up_space(
                                Size, MaxSize, Table, OrderedKeyTable,
                                SecondChances - 1
                            );
                        false ->
                            evict(Key, ElementSize, CurrentFetchedCount, Table),
                            clean_up_space(
                                Size, MaxSize, Table, OrderedKeyTable,
                                SecondChances
                            )
                    end
            end;
        false ->
            arweave_metrics:gauge_set(replica_2_9_entropy_cache, TotalSize + Size),
            ok
    end.

%% @doc Remove and return the oldest entry of the eviction order; concurrent
%% callers each obtain distinct entries because the delete is atomic.
take_eviction_candidate(OrderedKeyTable) ->
    case ets:first(OrderedKeyTable) of
        '$end_of_table' ->
            none;
        {_Timestamp, Key, ElementSize} = OrderKey ->
            case ets:lookup(OrderedKeyTable, OrderKey) of
                [{OrderKey, LastFetchedCount}] ->
                    case ets:select_delete(
                            OrderedKeyTable, [{{OrderKey, '_'}, [], [true]}]) of
                        1 -> {Key, ElementSize, LastFetchedCount};
                        0 -> take_eviction_candidate(OrderedKeyTable)
                    end;
                [] ->
                    take_eviction_candidate(OrderedKeyTable)
            end
    end.

evict(Key, ElementSize, CurrentFetchedCount, Table) ->
    case ets:take(Table, {key, Key}) of
        [] ->
            ok;
        [_] ->
            %% Eviction is the first point at which this entropy's final
            %% reuse count is known.
            arweave_metrics:histogram_observe(
                replica_2_9_entropy_reuse_count, CurrentFetchedCount
            ),
            ets:update_counter(
                Table, total_size, -ElementSize, {total_size, 0}
            ),
            ets:delete(Table, {fetched_key_count, Key}),
            ok
    end.

get_fetched_key_count(Table, Key) ->
    ets:lookup_element(Table, {fetched_key_count, Key}, 2, 0).

put(Key, Value, Size, Table, OrderedKeyTable) ->
    ets:insert(Table, {{key, Key}, Value}),
    requeue(Key, Size, 0, OrderedKeyTable),
    _ = ets:update_counter(Table, total_size, Size, {total_size, 0}),
    ok.

%% @doc Queue Key at the back of the eviction order with its current fetch
%% count, the baseline the next pass compares against to decide whether the
%% entry was fetched since.
requeue(Key, Size, CurrentFetchedCount, OrderedKeyTable) ->
    Timestamp = erlang:unique_integer([monotonic, positive]),
    ets:insert(OrderedKeyTable, {{Timestamp, Key, Size}, CurrentFetchedCount}),
    ok.
