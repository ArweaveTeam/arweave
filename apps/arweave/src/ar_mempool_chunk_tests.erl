-module(ar_mempool_chunk_tests).
-test_category([fast]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Mempool snapshot traversal and its reduction budget.

take_chunk_test_() ->
    {foreach,
        fun save_mempool/0,
        fun restore_mempool/1,
        [fun test_boundaries/0,
         fun test_removals_between_batches/0,
         fun test_snapshot_updates/0,
         {timeout, 30, fun test_linear_scaling/0}]}.

save_mempool() ->
    %% The test launcher owns node_state; preserve its mempool metadata.
    TXs = ets:match_object(node_state, {{tx, '_'}, '_'}),
    PrioritySet = ets:lookup(node_state, tx_priority_set),
    ets:match_delete(node_state, {{tx, '_'}, '_'}),
    {TXs, PrioritySet}.

restore_mempool({TXs, PrioritySet}) ->
    ets:match_delete(node_state, {{tx, '_'}, '_'}),
    ets:delete(node_state, tx_priority_set),
    ets:insert(node_state, TXs ++ PrioritySet).

test_boundaries() ->
    TXs = seed_transactions(3),
    TXIDs = txids(TXs),
    Missing = <<0:256>>,
    ?assertEqual({ok, [], []}, ar_mempool:take_chunk([], 100)),
    ?assertEqual({ok, [], []}, ar_mempool:take_chunk([], 0)),
    ?assertEqual({ok, [], TXIDs}, ar_mempool:take_chunk(TXIDs, 0)),
    ?assertEqual({ok, [], []},
        ar_mempool:take_chunk([Missing, Missing], 100)),
    ?assertEqual({ok, TXs, []}, ar_mempool:take_chunk(TXIDs, 3)),
    ?assertEqual({ok, TXs, []}, ar_mempool:take_chunk(TXIDs, 100)),
    ?assertEqual({ok, TXs, [Missing]},
        ar_mempool:take_chunk(TXIDs ++ [Missing], 3)),
    ?assertEqual({ok, TXs, []},
        ar_mempool:take_chunk([Missing | TXIDs] ++ [Missing], 100)).

test_removals_between_batches() ->
    TXs = seed_transactions(250),
    Snapshot = lists:reverse(ar_mempool:get_all_txids()),
    ?assertEqual(txids(TXs), Snapshot),
    {ok, First, Rest1} = ar_mempool:take_chunk(Snapshot, 100),
    ?assertEqual(lists:sublist(TXs, 100), First),
    ?assertEqual(lists:nthtail(100, Snapshot), Rest1),

    %% Remove entries at the next batch's start, middle and boundary,
    %% as well as entries in the final partial batch.
    Removed = [101, 125, 200, 225, 250],
    [ets:delete(node_state, {tx, <<N:256>>}) || N <- Removed],
    {ok, Second, Rest2} = ar_mempool:take_chunk(Rest1, 100),
    ExpectedSecond = [TX || #tx{reward = N} = TX <- TXs,
        N >= 102, N =< 203, N /= 125, N /= 200],
    ?assertEqual(100, length(Second)),
    ?assertEqual(ExpectedSecond, Second),
    ?assertEqual(lists:nthtail(203, Snapshot), Rest2),

    {ok, Third, Rest3} = ar_mempool:take_chunk(Rest2, 100),
    ExpectedThird = [TX || #tx{reward = N} = TX <- TXs,
        N >= 204, N < 250, N /= 225],
    ?assertEqual(ExpectedThird, Third),
    ?assertEqual([], Rest3),
    ?assertEqual([TX || #tx{reward = N} = TX <- TXs,
        not lists:member(N, Removed)], First ++ Second ++ Third),
    ?assertEqual({ok, [], []}, ar_mempool:take_chunk(Rest3, 100)).

test_snapshot_updates() ->
    [TX1, TX2, TX3] = seed_transactions(3),
    Snapshot = lists:reverse(ar_mempool:get_all_txids()),
    {ok, First, Rest} = ar_mempool:take_chunk(Snapshot, 1),
    ?assertEqual([TX1], First),
    ets:delete(node_state, {tx, TX2#tx.id}),
    Updated = TX2#tx{data = <<"newly fetched data">>},
    ets:insert(node_state,
        {{tx, TX2#tx.id}, {Updated, ready_for_mining, -2}}),
    NewTX = TX3#tx{id = <<4:256>>},
    ets:insert(node_state, {{tx, NewTX#tx.id}, {NewTX, waiting, -4}}),
    ?assertEqual({ok, [Updated, TX3], []},
        ar_mempool:take_chunk(Rest, 100)),
    ?assertEqual(NewTX, ar_mempool:get_tx(NewTX#tx.id)).

test_linear_scaling() ->
    TXs = seed_transactions(8000),
    SmallTXs = lists:sublist(TXs, 2000),
    {SmallResult, SmallReductions} = measure_drain(txids(SmallTXs)),
    {LargeResult, LargeReductions} = measure_drain(txids(TXs)),
    %% Four times the input permits eight times the work, leaving room for
    %% GC while rejecting the sixteenfold growth of quadratic traversal.
    %% Reductions avoid dependence on machine speed or scheduler load.
    ?assert(LargeReductions < 8 * SmallReductions,
        {small_reductions, SmallReductions,
         large_reductions, LargeReductions}),
    ?assertEqual(SmallTXs, SmallResult),
    ?assertEqual(TXs, LargeResult).

%% @doc Populate only the ETS metadata read by snapshot traversal.
seed_transactions(Count) ->
    TXs = [#tx{id = <<N:256>>, format = 2, denomination = 1, reward = N}
        || N <- lists:seq(1, Count)],
    ets:insert(node_state,
        [{{tx, TX#tx.id}, {TX, waiting, -N}}
            || #tx{reward = N} = TX <- TXs]),
    Priorities = [{{ar_tx:utility(TX), -N}, TX#tx.id, waiting}
        || #tx{reward = N} = TX <- TXs],
    ets:insert(node_state, {tx_priority_set, gb_sets:from_list(Priorities)}),
    TXs.

txids(TXs) ->
    [TX#tx.id || TX <- TXs].

measure_drain(TXIDs) ->
    erlang:garbage_collect(),
    {reductions, Before} = process_info(self(), reductions),
    TXs = drain(TXIDs, []),
    {reductions, After} = process_info(self(), reductions),
    {TXs, After - Before}.

drain([], Chunks) ->
    lists:append(lists:reverse(Chunks));
drain(TXIDs, Chunks) ->
    {ok, TXs, Rest} = ar_mempool:take_chunk(TXIDs, 100),
    drain(Rest, [TXs | Chunks]).
