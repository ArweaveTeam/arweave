%%% @doc Transient network-sync fetch worker.
%%%
%%% One process per dispatched task, `spawn_monitor`'d by `ar_sync_dispatcher`.
%%% It performs the chunk HTTP request(s) for the task's byte range and hands
%%% each fetched chunk to `ar_data_sync` for storage, then reports the outcome
%%% to `ar_peers` and exits.
%%%
%%% Exit contract (read by the dispatcher's `'DOWN'` handler): `normal` covers
%%% both a fully-processed range and a definitive fetch failure (the range is
%%% simply re-discovered later); only a genuine crash exits abnormally. The
%%% dispatcher updates all scheduling/back-pressure/capacity accounting on
%%%  `'DOWN'` regardless of outcome.
%%%
%%% Chunk-cache and disk-space is checkedby `ar_sync_dispatcher`
%%% before spawn, so this worker does not re-check it.
-module(ar_data_sync_worker).
-test_category([fast]).

-export([run/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

%%%===================================================================
%%% Entry point.
%%%===================================================================

%% @doc Fetch the task's range and rate the peer. `Concurrency`
%% is the peer's in-flight count at dispatch.
run(#sync_task{ start_offset = Start, end_offset = End, peer = Peer } = Task,
    Concurrency) ->
    {ElapsedUs, Result} = timer:tc(fun() -> fetch_range(Task) end),
    ar_peers:rate_fetched_data(
      Peer, chunk, Result, ElapsedUs, End - Start, Concurrency),
    ok.

%%%===================================================================
%%% Internal.
%%%===================================================================

fetch_range(#sync_task{ start_offset = Start, end_offset = End })
  when Start >= End ->
    ok;
fetch_range(#sync_task{ retry_count = 0, peer = Peer,
                        start_offset = Start, end_offset = End }) ->
    ?LOG_INFO([{event, fetch_range_retries_exhausted},
               {peer, arweave_util:format_peer(Peer)},
               {start_offset, Start}, {end_offset, End}]),
    {error, timeout};
fetch_range(#sync_task{ start_offset = Start, end_offset = End, peer = Peer,
                        store_id = TargetStoreID, retry_count = RetryCount } = Task) ->
    Start2 = ar_tx_blacklist:get_next_not_blacklisted_byte(Start + 1),
    Byte = Start2 - 1,
    IsRecorded = ar_sync_record:is_recorded(Byte + 1, ar_data_sync, TargetStoreID),
    case {Byte >= End, IsRecorded} of
        {true, _} ->
            ok;
        {_, {true, _}} ->
            ok;
        _ ->
            Packing = get_target_packing(TargetStoreID),
            case ar_http_iface_client:get_chunk_binary(Peer, Start2, Packing) of
                {ok, #{ chunk := Chunk } = Proof, _Time, _TransferSize} ->
                    %% In case we fetched a packed small chunk we may skip some
                    %% chunks by continuing with Start2 + byte_size(Chunk) — the
                    %% skipped chunks are requested later.
                    Start3 = ar_block:get_chunk_padded_offset(
                               Start2 + byte_size(Chunk)) + 1,
                    ar_data_sync:store_fetched_chunk(
                      TargetStoreID, Peer, Byte, Proof),
                    ar_data_sync:increment_chunk_cache_size(),
                    fetch_range(Task#sync_task{ start_offset = Start3 });
                {error, timeout} ->
                    ?LOG_DEBUG([{event, timeout_fetching_chunk},
                                {peer, arweave_util:format_peer(Peer)},
                                {start_offset, Start2}, {end_offset, End}]),
                    timer:sleep(1000),
                    fetch_range(Task#sync_task{ retry_count = RetryCount - 1 });
                {error, {ok, {{<<"404">>, _}, _, _, _, _}} = Reason} ->
                    {error, Reason};
                {error, Reason} ->
                    ar_http_iface_client:log_failed_request({error, Reason}, [
                                                                              {event, failed_to_fetch_chunk},
                                                                              {peer, arweave_util:format_peer(Peer)},
                                                                              {start_offset, Start2}, {end_offset, End},
                                                                              {reason, io_lib:format("~p", [Reason])}]),
                    {error, Reason}
            end
    end.

%% @doc Read the target packing for this store, gated by the
%% [sync, request_packed_chunks] config (a cheap ETS read).
get_target_packing(StoreID) ->
    case arweave_config:get([sync, request_packed_chunks]) of
        true -> ar_storage_module:get_packing(StoreID);
        false -> any
    end.

%%%===================================================================
%%% Tests.
%%%===================================================================

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

run_test_() ->
    {foreach, fun() -> ok end, fun(_) -> ok end, [
                                                  %% No HTTP fetch:
                                                  fun test_empty_range_skips_fetch/0,
                                                  fun test_blacklist_past_end_skips_fetch/0,
                                                  fun test_already_recorded_skips_fetch/0,
                                                  %% HTTP fetch outcomes:
                                                  fun test_success_stores_and_rates_ok/0,
                                                  fun test_multi_chunk_stores_each/0,
                                                  fun test_404_rates_error_and_does_not_store/0,
                                                  fun test_generic_error_logs_and_rates_error/0,
                                                  fun test_timeout_retries_then_succeeds/0,
                                                  fun test_retries_exhausted_rates_error/0,
                                                  %% Packing selection:
                                                  fun test_packed_request_selects_store_packing/0
                                                 ]}.

test_empty_range_skips_fetch() ->
    run_with_mocks(fun(_, _, _) -> error(should_not_fetch) end, fun() ->
                                                                        ?assertEqual(ok, run(task(100, 100), 5)),
                                                                        ?assertEqual(0, meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
                                                                        ?assertEqual(0, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
                                                                        ?assert(meck:called(ar_peers, rate_fetched_data,
                                                                                            [test_peer(), chunk, ok, '_', 0, 5]))
                                                                end).

test_blacklist_past_end_skips_fetch() ->
    run_with_mocks(fun(_, _, _) -> error(should_not_fetch) end,
                   [{ar_tx_blacklist, get_next_not_blacklisted_byte, fun(_) -> 1000 end}],
                   fun() ->
                           ?assertEqual(ok, run(task(0, 100), 5)),
                           ?assertEqual(0,
                                        meck:num_calls(ar_http_iface_client, get_chunk_binary, '_'))
                   end).

test_already_recorded_skips_fetch() ->
    run_with_mocks(fun(_, _, _) -> error(should_not_fetch) end,
                   [{ar_sync_record, is_recorded, fun(_, _, _) -> {true, unpacked} end}],
                   fun() ->
                           ?assertEqual(ok, run(task(0, 100), 5)),
                           ?assertEqual(0,
                                        meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
                           ?assertEqual(0,
                                        meck:num_calls(ar_data_sync, store_fetched_chunk, '_'))
                   end).

test_success_stores_and_rates_ok() ->
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end, fun() ->
                                                                 ?assertEqual(ok, run(task(0, 100), 5)),
                                                                 ?assertEqual(1, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
                                                                 ?assert(meck:called(ar_peers, rate_fetched_data,
                                                                                     [test_peer(), chunk, ok, '_', 100, 5]))
                                                         end).

test_multi_chunk_stores_each() ->
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end, fun() ->
                                                                 ?assertEqual(ok, run(task(0, 300), 5)),
                                                                 ?assertEqual(3, meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
                                                                 ?assertEqual(3, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
                                                                 ?assert(meck:called(ar_peers, rate_fetched_data,
                                                                                     [test_peer(), chunk, ok, '_', 300, 5]))
                                                         end).

test_404_rates_error_and_does_not_store() ->
    run_with_mocks(
      fun(_, _, _) ->
              {error, {ok, {{<<"404">>, <<>>}, [], <<>>, undefined, undefined}}}
      end,
      fun() ->
              ?assertEqual(ok, run(task(0, 100), 5)),
              ?assertEqual(0,
                           meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
              ?assertEqual(0,
                           meck:num_calls(ar_http_iface_client, log_failed_request, '_')),
              ?assert(meck:called(ar_peers, rate_fetched_data,
                                  ['_', chunk, {error, '_'}, '_', '_', '_']))
      end).

test_generic_error_logs_and_rates_error() ->
    run_with_mocks(fun(_, _, _) -> {error, econnrefused} end, fun() ->
                                                                      ?assertEqual(ok, run(task(0, 100), 5)),
                                                                      ?assertEqual(0, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
                                                                      ?assertEqual(1,
                                                                                   meck:num_calls(ar_http_iface_client, log_failed_request, '_')),
                                                                      ?assert(meck:called(ar_peers, rate_fetched_data,
                                                                                          ['_', chunk, {error, econnrefused}, '_', '_', '_']))
                                                              end).

test_timeout_retries_then_succeeds() ->
    Counter = counters:new(1, []),
    GetChunk = fun(_, _, _) ->
                       case counters:get(Counter, 1) of
                           0 -> counters:add(Counter, 1, 1), {error, timeout};
                           _ -> chunk_reply(100)
                       end
               end,
    run_with_mocks(GetChunk, fun() ->
                                     ?assertEqual(ok, run(task(0, 100), 5)),
                                     ?assertEqual(2, meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
                                     ?assertEqual(1, meck:num_calls(ar_data_sync, store_fetched_chunk, '_')),
                                     ?assert(meck:called(ar_peers, rate_fetched_data,
                                                         [test_peer(), chunk, ok, '_', 100, 5]))
                             end).

test_retries_exhausted_rates_error() ->
    run_with_mocks(fun(_, _, _) -> error(should_not_fetch) end, fun() ->
                                                                        Task = (task(0, 100))#sync_task{ retry_count = 0 },
                                                                        ?assertEqual(ok, run(Task, 5)),
                                                                        ?assertEqual(0,
                                                                                     meck:num_calls(ar_http_iface_client, get_chunk_binary, '_')),
                                                                        ?assert(meck:called(ar_peers, rate_fetched_data,
                                                                                            [test_peer(), chunk, {error, timeout}, '_', 100, 5]))
                                                                end).

test_packed_request_selects_store_packing() ->
    Packing = {replica_2_9, <<"addr">>},
    run_with_mocks(fun(_, _, _) -> chunk_reply(100) end,
                   [{arweave_config, get,
                     fun([sync, request_packed_chunks]) -> true;
                        (K) -> meck:passthrough([K]) end},
                    {ar_storage_module, get_packing, fun(_) -> Packing end}],
                   fun() ->
                           ?assertEqual(ok, run(task(0, 100), 5)),
                           ?assert(meck:called(ar_http_iface_client, get_chunk_binary,
                                               ['_', '_', Packing]))
                   end).

%%%-------------------------------------------------------------------
%%% Test helpers.
%%%-------------------------------------------------------------------

test_peer() -> {1, 2, 3, 4, 1984}.

task(Start, End) ->
    #sync_task{ start_offset = Start, end_offset = End, peer = test_peer(),
                store_id = store1, footprint_key = none }.

chunk_reply(Size) ->
    {ok, #{ chunk => <<0:(Size * 8)>> }, 1, Size}.

run_with_mocks(GetChunkFun, TestFun) ->
    run_with_mocks(GetChunkFun, [], TestFun).

%% Install the default mocks (an isolated, no-op fetch environment), let
%% ExtraMocks override any default by {Module, Function}, run TestFun, unload.
run_with_mocks(GetChunkFun, ExtraMocks, TestFun) ->
    Defaults = [
                {arweave_config, get,
                 fun([sync, request_packed_chunks]) -> false;
                    (K) -> meck:passthrough([K]) end},
                {ar_tx_blacklist, get_next_not_blacklisted_byte, fun(X) -> X end},
                {ar_sync_record, is_recorded, fun(_, _, _) -> false end},
                {ar_block, get_chunk_padded_offset, fun(X) -> X end},
                {ar_http_iface_client, get_chunk_binary, GetChunkFun},
                {ar_http_iface_client, log_failed_request, fun(_, _) -> ok end},
                {ar_data_sync, store_fetched_chunk, fun(_, _, _, _) -> ok end},
                {ar_data_sync, increment_chunk_cache_size, fun() -> ok end},
                {ar_peers, rate_fetched_data, fun(_, _, _, _, _, _) -> ok end}
               ],
    Mocks = merge_mocks(Defaults, ExtraMocks),
    Modules = lists:usort([M || {M, _, _} <- Mocks]),
    [ar_test_util:new_mock(M, [passthrough]) || M <- Modules],
    [ar_test_util:mock_function(M, F, Impl) || {M, F, Impl} <- Mocks],
    try TestFun()
    after [ar_test_util:unmock_module(M) || M <- Modules]
    end.

%% Merge mock specs keyed by {Module, Function}; later specs (ExtraMocks) win.
merge_mocks(Defaults, Extra) ->
    Keyed = lists:foldl(
              fun({M, F, _} = Spec, Acc) -> Acc#{ {M, F} => Spec } end,
              #{}, Defaults ++ Extra),
    maps:values(Keyed).

-endif.
