-module(ar_mining_server_tests).


-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(ALIGNED_PARTITION_SIZE, 2_097_152).
-define(WEAVE_SIZE, (3 * ?ALIGNED_PARTITION_SIZE)).

%% RECALL_RANGE_1 and SYNCED_RECALL_RANGE_2 must be different partitions so that different io
%% threads are used. 
%% ?RECALL_RANGE_1 is set so 1 chunk is synced and one is missing.
-define(RECALL_RANGE_1, (3*?PARTITION_SIZE-?DATA_CHUNK_SIZE)).
-define(SYNCED_RECALL_RANGE_2, ?PARTITION_SIZE).
-define(UNSYNCED_RECALL_RANGE_2, 0).


%% ------------------------------------------------------------------------------------------------
%% Fixtures
%% ------------------------------------------------------------------------------------------------
setup_all() ->
    [B0] = ar_weave:init([], ar_test_node:get_difficulty_for_invalid_hash(), ?WEAVE_SIZE),
    RewardAddr = ar_test_node:generate_address(main),
    Config = arweave_config:internal_snapshot(),
    %% We'll use partition 0 for any unsynced ranges.
    StorageModules = [
        {arweave_lib_constants:partition_size(), 2 * arweave_lib_constants:partition_size(),
            {spora_2_6, RewardAddr}},
        {2 * arweave_lib_constants:partition_size(), 3 * arweave_lib_constants:partition_size(),
            {spora_2_6, RewardAddr}}
    ],
    ar_test_node:start(B0, RewardAddr, #{[storage_modules] => StorageModules}),
    Config.

cleanup_all(Config) ->
    ok = arweave_config:internal_restore(Config).

%% @doc Setup the environment so we can control VDF step generation.
setup_pool_client() ->
    start_controlled_vdf_node(#{
        [pool, is_client] => true,
        [pool, server_address] => <<"http://localhost:2002">>,
        [pool, api_key] => <<"pool_secret">>
    }).

cleanup_pool_client(Config) ->
    ok = arweave_config:internal_restore(Config).

setup_pool_job() ->
    {setup_pool_client(), setup_one()}.

cleanup_pool_job({Config, _}) ->
    cleanup_one(ok),
    cleanup_pool_client(Config).

%% @doc Start a solo miner that reads VDF from an unreachable server, so the
%% only VDF output its mining server sees is what the test sends.
setup_vdf_session() ->
    {start_controlled_vdf_node(#{}), setup_one()}.

cleanup_vdf_session({Config, _}) ->
    cleanup_one(ok),
    ok = arweave_config:internal_restore(Config).

start_controlled_vdf_node(ExtraConfig) ->
    [B0] = ar_weave:init([], ar_test_node:get_difficulty_for_invalid_hash(), ?WEAVE_SIZE),
    RewardAddr = ar_test_node:generate_address(main),
    Config = arweave_config:internal_snapshot(),
    %% We'll use partition 0 for any unsynced ranges.
    StorageModules = [
        {arweave_lib_constants:partition_size(), 2 * arweave_lib_constants:partition_size(),
            {spora_2_6, RewardAddr}},
        {2 * arweave_lib_constants:partition_size(), 3 * arweave_lib_constants:partition_size(),
            {spora_2_6, RewardAddr}}
    ],
    ar_test_node:start(B0, RewardAddr,
        maps:merge(#{
            [storage_modules] => StorageModules,
            [peers, vdf_server] => [arweave_lib_util:format_peer(vdf_server())],
            %% The cm validator requires mining.enabled when
            %% pool.is_client is true.
            [mining, enabled] => true
        }, ExtraConfig)),
    Config.

setup_one() ->
    ets:new(mock_counter, [set, public, named_table]),
    ets:new(add_task, [named_table, bag, public]).

cleanup_one(_) ->
    ets:delete(add_task),
    ets:delete(mock_counter).

%% ------------------------------------------------------------------------------------------------
%% Test Registration
%% ------------------------------------------------------------------------------------------------
chunk_cache_size_test_() ->
    {setup, fun setup_all/0, fun cleanup_all/1,
        {foreach, fun setup_one/0, fun cleanup_one/1,
        [
            {timeout, 30, fun test_h2_solution_chunk1_first/0},
            {timeout, 30, fun test_h2_solution_chunk2_first/0},
            {timeout, 30, fun test_h1_solution_h2_synced_chunk1_first/0},
            {timeout, 30, fun test_h1_solution_h2_synced_chunk2_first/0},
            {timeout, 30, fun test_h1_solution_h2_unsynced/0},
            {timeout, 30, fun test_no_solution_then_h2_solution/0},
            {timeout, 30, fun test_no_solution_then_h1_solution_h2_synced/0},
            {timeout, 30, fun test_no_solution_then_h1_solution_h2_unsynced/0}
        ]}
    }.

%% Each test starts a fresh node: the mining server keeps its sessions between
%% tests, and a session left behind by one test would decide what the next one
%% accepts.
pool_job_test_() ->
    {foreach, fun setup_pool_job/0, fun cleanup_pool_job/1,
        [
            ar_test_node:test_with_all_nodes_mocked([mock_add_task(), mock_get_current_sesssion()],
                fun test_pool_job_no_cached_sessions/0, 120),
            ar_test_node:test_with_all_nodes_mocked([mock_add_task(), mock_get_current_sesssion()],
                fun test_pool_job_follows_pool_session/0, 120)
        ]
    }.

vdf_session_test_() ->
    {foreach, fun setup_vdf_session/0, fun cleanup_vdf_session/1,
        [
            ar_test_node:test_with_all_nodes_mocked([mock_add_task()],
                fun test_vdf_session_next_interval/0, 120),
            ar_test_node:test_with_all_nodes_mocked([mock_add_task()],
                fun test_vdf_session_fork_across_reset_then_retarget/0, 120),
            ar_test_node:test_with_all_nodes_mocked([mock_add_task()],
                fun test_vdf_session_returns_to_lower_interval/0, 120),
            ar_test_node:test_with_all_nodes_mocked([mock_add_task()],
                fun test_vdf_session_known_session_keeps_set/0, 120)
        ]
    }.

%% ------------------------------------------------------------------------------------------------
%% chunk_cache_size_test_
%% ------------------------------------------------------------------------------------------------
test_h2_solution_chunk1_first() ->
    do_test_chunk_cache_size_with_mocks(
        [ar_test_node:invalid_solution()],
        [ar_test_node:valid_solution()],
        [?SYNCED_RECALL_RANGE_2],
        [chunk1]
    ).

test_h2_solution_chunk2_first() ->
    do_test_chunk_cache_size_with_mocks(
        [ar_test_node:invalid_solution()],
        [ar_test_node:valid_solution()],
        [?SYNCED_RECALL_RANGE_2],
        [chunk2]
    ).

test_h1_solution_h2_synced_chunk1_first() ->
    do_test_chunk_cache_size_with_mocks(
        [ar_test_node:valid_solution()],
        [ar_test_node:invalid_solution()],
        [?SYNCED_RECALL_RANGE_2],
        [chunk1]
    ).

test_h1_solution_h2_synced_chunk2_first() ->
    do_test_chunk_cache_size_with_mocks(
        [ar_test_node:valid_solution()],
        [ar_test_node:invalid_solution()],
        [?SYNCED_RECALL_RANGE_2],
        [chunk2]
    ).

test_h1_solution_h2_unsynced() ->
    do_test_chunk_cache_size_with_mocks(
        [ar_test_node:valid_solution()],
        [],
        [?UNSYNCED_RECALL_RANGE_2],
        [chunk1]
    ).

test_no_solution_then_h2_solution() ->
    do_test_chunk_cache_size_with_mocks(
        [ar_test_node:invalid_solution()],
        [ar_test_node:invalid_solution(), ar_test_node:invalid_solution(),
            ar_test_node:valid_solution()],
        [?SYNCED_RECALL_RANGE_2],
        [chunk1]
    ).

test_no_solution_then_h1_solution_h2_synced() ->
    do_test_chunk_cache_size_with_mocks(
        [ar_test_node:invalid_solution(), ar_test_node:invalid_solution(),
            ar_test_node:valid_solution()],
        [ar_test_node:invalid_solution()],
        [?SYNCED_RECALL_RANGE_2],
        [chunk1]
    ).

test_no_solution_then_h1_solution_h2_unsynced() ->
    do_test_chunk_cache_size_with_mocks(
        [ar_test_node:invalid_solution(), ar_test_node:invalid_solution(),
            ar_test_node:valid_solution()],
        [],
        [?UNSYNCED_RECALL_RANGE_2],
        [chunk1]
    ).

%% ------------------------------------------------------------------------------------------------
%% pool_job_test_
%% ------------------------------------------------------------------------------------------------

test_pool_job_no_cached_sessions() ->
    SessionKey1 = {<<"session1">>, 1, 1},
    SessionKey2 = {<<"session2">>, 2, 1},
    SessionKey3 = {<<"session3">>, 3, 1},
    Output = crypto:strong_rand_bytes(32),
    PartitionUpperBound = ?WEAVE_SIZE,
    Seed1 = crypto:strong_rand_bytes(32),
    Seed2 = crypto:strong_rand_bytes(32),
    Seed3 = crypto:strong_rand_bytes(32),
    PartialDiff = {1, 1},
    ar_mining_server:add_pool_job(
        SessionKey1, 1, Output, PartitionUpperBound, Seed1, PartialDiff),
    ar_mining_server:add_pool_job(
        SessionKey1, 2, Output, PartitionUpperBound, Seed1, PartialDiff),
    ?assertEqual(sets:from_list([SessionKey1]), ar_mining_server:active_sessions()),
    ?assertEqual([1, 1, 2, 2], lists:sort(mined_steps())),

    ar_mining_server:add_pool_job(
        SessionKey2, 5, Output, PartitionUpperBound, Seed2, PartialDiff),
    ar_mining_server:add_pool_job(
        SessionKey2, 6, Output, PartitionUpperBound, Seed2, PartialDiff),
    ?assertEqual(sets:from_list([SessionKey1, SessionKey2]), ar_mining_server:active_sessions()),
    ?assertEqual([5, 5, 6, 6], lists:sort(mined_steps())),

    ar_mining_server:add_pool_job(
        SessionKey3, 10, Output, PartitionUpperBound, Seed3, PartialDiff),
    ar_mining_server:add_pool_job(
        SessionKey3, 12, Output, PartitionUpperBound, Seed3, PartialDiff),
    ?assertEqual(sets:from_list([SessionKey2, SessionKey3]), ar_mining_server:active_sessions()),
    ?assertEqual([10, 10, 12, 12],lists:sort(mined_steps())),

    %% The pool can move back to an older session, for example after a reorg.
    ar_mining_server:add_pool_job(
        SessionKey1, 4, Output, PartitionUpperBound, Seed1, PartialDiff),
    ?assertEqual(lists:sort([SessionKey1, SessionKey3]), active_session_list()),
    ?assertEqual([4, 4], lists:sort(mined_steps())).

%% @doc Pool jobs are mined for every session the pool moves to: a new seed or
%% VDF difficulty in the same interval, or a lower interval.
test_pool_job_follows_pool_session() ->
    SessionKey1 = {<<"session1">>, 1, 1},
    SessionKey2 = {<<"session2">>, 1, 1},
    SessionKey3 = {<<"session2">>, 1, 2},
    LowerSessionKey = {<<"session0">>, 0, 1},
    Output = crypto:strong_rand_bytes(32),
    PartitionUpperBound = ?WEAVE_SIZE,
    Seed = crypto:strong_rand_bytes(32),
    PartialDiff = {1, 1},
    %% Every step is mined twice, once per stored partition (1 and 2).
    ar_mining_server:add_pool_job(
        SessionKey1, 1, Output, PartitionUpperBound, Seed, PartialDiff),
    ?assertEqual([SessionKey1], active_session_list()),
    ?assertEqual([1, 1], lists:sort(mined_steps())),

    ar_mining_server:add_pool_job(
        SessionKey2, 2, Output, PartitionUpperBound, Seed, PartialDiff),
    ?assertEqual(lists:sort([SessionKey1, SessionKey2]), active_session_list()),
    ?assertEqual([2, 2], lists:sort(mined_steps())),

    ar_mining_server:add_pool_job(
        SessionKey3, 3, Output, PartitionUpperBound, Seed, PartialDiff),
    ?assertEqual(lists:sort([SessionKey2, SessionKey3]), active_session_list()),
    ?assertEqual([3, 3], lists:sort(mined_steps())),

    ar_mining_server:add_pool_job(
        LowerSessionKey, 4, Output, PartitionUpperBound, Seed, PartialDiff),
    ?assertEqual(lists:sort([SessionKey3, LowerSessionKey]), active_session_list()),
    ?assertEqual([4, 4], lists:sort(mined_steps())).

%% ------------------------------------------------------------------------------------------------
%% vdf_session_test_
%% ------------------------------------------------------------------------------------------------

%% Interval numbers, seeds and VDF difficulties in these tests are arbitrary:
%% the mining server only compares them for equality and order. Every step is
%% mined twice, once per stored partition (1 and 2).

test_vdf_session_next_interval() ->
    Session9 = {<<"seed_a">>, 9, 1000},
    Session10 = {<<"seed_b">>, 10, 1000},
    Session11 = {<<"seed_c">>, 11, 1000},
    ok = start_mining(),

    send_vdf_step(Session9, 1),
    ?assertEqual([Session9], active_session_list()),
    ?assertEqual([1, 1], lists:sort(mined_steps())),

    send_vdf_step(Session10, 2),
    ?assertEqual(lists:sort([Session9, Session10]), active_session_list()),
    ?assertEqual([2, 2], lists:sort(mined_steps())),

    send_vdf_step(Session11, 3),
    ?assertEqual(lists:sort([Session10, Session11]), active_session_list()),
    ?assertEqual([3, 3], lists:sort(mined_steps())).

%% @doc Replay the session keys miners saw before the mainnet stall at height
%% 2021040: a fork across the reset gave interval 10 a session seeded by the
%% orphaned branch (Session1) and one seeded by the canonical branch (Session2),
%% then a VDF difficulty retarget gave the canonical seed a new key (Session3).
test_vdf_session_fork_across_reset_then_retarget() ->
    Session0 = {<<"seed_0">>, 9, 1000},
    Session1 = {<<"seed_1">>, 10, 1000},
    Session2 = {<<"seed_2">>, 10, 1000},
    Session3 = {<<"seed_2">>, 10, 999},
    ok = start_mining(),

    send_vdf_step(Session0, 1),
    send_vdf_step(Session1, 2),
    send_vdf_step(Session2, 3),
    ?assertEqual(lists:sort([Session1, Session2]), active_session_list()),
    ?assertEqual([1, 1, 2, 2, 3, 3], lists:sort(mined_steps())),

    send_vdf_step(Session3, 4),
    ?assertEqual(lists:sort([Session2, Session3]), active_session_list()),
    ?assertEqual([4, 4], lists:sort(mined_steps())),

    send_vdf_step(Session3, 5),
    ?assertEqual(lists:sort([Session2, Session3]), active_session_list()),
    ?assertEqual([5, 5], lists:sort(mined_steps())).

%% @doc A reorg onto a block that has not crossed the reset moves the local VDF
%% back to a session with a lower interval than every active one.
test_vdf_session_returns_to_lower_interval() ->
    Session1 = {<<"seed_1">>, 10, 1000},
    Session2 = {<<"seed_2">>, 10, 1000},
    LowerSession = {<<"seed_0">>, 9, 1000},
    ok = start_mining(),

    send_vdf_step(Session1, 1),
    send_vdf_step(Session2, 2),
    ?assertEqual([1, 1, 2, 2], lists:sort(mined_steps())),

    send_vdf_step(LowerSession, 3),
    ?assertEqual(lists:sort([Session2, LowerSession]), active_session_list()),
    ?assertEqual([3, 3], lists:sort(mined_steps())).

%% @doc A late step from the older active session is mined without changing the
%% set, and the next new session replaces that older one, not the newer one.
test_vdf_session_known_session_keeps_set() ->
    Session9 = {<<"seed_a">>, 9, 1000},
    Session10 = {<<"seed_b">>, 10, 1000},
    Session11 = {<<"seed_c">>, 11, 1000},
    ok = start_mining(),

    send_vdf_step(Session9, 1),
    send_vdf_step(Session10, 2),
    ?assertEqual([1, 1, 2, 2], lists:sort(mined_steps())),

    send_vdf_step(Session9, 3),
    ?assertEqual(lists:sort([Session9, Session10]), active_session_list()),
    ?assertEqual([3, 3], lists:sort(mined_steps())),

    send_vdf_step(Session11, 4),
    ?assertEqual(lists:sort([Session10, Session11]), active_session_list()),
    ?assertEqual([4, 4], lists:sort(mined_steps())).


%% ------------------------------------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------------------------------------
do_test_chunk_cache_size_with_mocks(H1s, H2s, RecallRange2s, FirstChunks) ->
    Height = ar_node:get_height() + 1,
    ets:insert(mock_counter, {compute_h1, 0}),
    ets:insert(mock_counter, {compute_h2, 0}),
    ets:insert(mock_counter, {get_recall_range, 0}),
    ets:insert(mock_counter, {get_range, 0}),
    {Setup, Cleanup} = ar_test_node:mock_all_nodes([
        {
            ar_retarget, is_retarget_height,
            fun (_Height) ->
                false
            end
        },
        {
            ar_block, compute_h1,
            fun (_H0, _Nonce, _Chunk) ->
                Count = increment_mock_counter(compute_h1),
                Solution = get_mock_value(Count, H1s),
                {Solution, Solution}
            end
        },
        {
            ar_block, compute_h2,
            fun (_H0, _Nonce, _Chunk) ->
                Count = increment_mock_counter(compute_h2),
                Solution = get_mock_value(Count, H2s),
                {Solution, Solution}
            end
        },
        {
            ar_block, get_recall_range,
            fun (_H0, _PartitionNumber, _PartitionUpperBound) ->
                Count = increment_mock_counter(get_recall_range),
                RecallRange2 = get_mock_value(Count, RecallRange2s),
                {?RECALL_RANGE_1, RecallRange2}
            end
        },
        {
            arweave_storage, get_chunk_range,
            fun (RangeStart, Size, StoreID) ->
                Count = increment_mock_counter(get_range),
                FirstChunk = get_mock_value(Count, FirstChunks),
                case FirstChunk == chunk1 andalso RangeStart /= ?RECALL_RANGE_1 of
                    true ->
                        timer:sleep(100);
                    _ -> ok
                end,
                case FirstChunk == chunk2 andalso RangeStart == ?RECALL_RANGE_1 of
                    true ->
                        timer:sleep(100);
                    _ -> ok
                end,
                meck:passthrough([RangeStart, Size, StoreID])
            end
        }
    ]),
    Functions = Setup(),

    try
        ar_test_node:mine(),
        ?assertMatch({ok, _}, ar_test_await:node_height(main, Height)),
        %% wait until the mining has stopped
        ok = ar_test_await:until(chunk_cache_drained,
            fun() -> get_chunk_cache_size() == 0 end, 10000)
    after
        Cleanup(Functions)
    end.

get_chunk_cache_size() ->
    Pattern = {{chunk_cache_size, '$1'}, '_'}, % '$1' matches any PartitionNumber
    Entries = ets:match(mock_counter, Pattern),
    lists:foldl(
        fun(PartitionNumber, Acc) ->
            case ets:lookup(ar_mining_server, {chunk_cache_size, PartitionNumber}) of
                [] ->
                    Acc;
                [{_, Size}] ->
                    Acc + Size
            end
        end,
        0,
        Entries
    ).

get_mock_value(Index, Values) when Index < length(Values) ->
    lists:nth(Index, Values);
    
get_mock_value(_, Values) ->
    lists:last(Values).

increment_mock_counter(Mock) ->
    ets:update_counter(mock_counter, Mock, {2, 1}),
    [{_, Count}] = ets:lookup(mock_counter, Mock),
    Count.

vdf_server() ->
    {127,0,0,1,2001}.

%% @doc Start mining and wait until the mining server accepts VDF output.
start_mining() ->
    ar_test_node:mine(),
    ar_test_await:until(mining_started,
        fun() -> not ar_mining_server:is_paused() end).

%% @doc Deliver a VDF step to the mining server the way ar_events does, and
%% return once the server has handled it: a call from this process is served
%% only after the message sent before it.
send_vdf_step(SessionKey, StepNumber) ->
    Output = crypto:strong_rand_bytes(32),
    ar_mining_server ! {event, nonce_limiter,
        {computed_output, {SessionKey, StepNumber, Output, ?WEAVE_SIZE}}},
    _ = ar_mining_server:is_paused(),
    ok.

active_session_list() ->
    lists:sort(sets:to_list(ar_mining_server:active_sessions())).

mined_steps() ->
    Steps = lists:reverse(ets:foldl(
        fun({_Worker, _Task, Step}, Acc) -> [Step | Acc] end, [], add_task)),
    ets:delete_all_objects(add_task),
    Steps.

mock_add_task() ->
    {
        ar_mining_worker, add_task, 
        fun(Worker, TaskType, Candidate) -> 
            ets:insert(add_task, {Worker, TaskType, Candidate#mining_candidate.step_number})
        end
    }.

mock_get_current_sesssion() ->
    {
        ar_nonce_limiter, get_current_session, 
        fun() -> 
            {undefined, not_found}
        end
    }.
