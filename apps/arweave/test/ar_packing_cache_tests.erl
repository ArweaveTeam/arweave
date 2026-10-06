-module(ar_packing_cache_tests).

-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").

%% @doc Packing preserves cache accounting across full capacity, replies and
%% producer exit.
packing_cache_test_() ->
    {setup, fun setup/0, fun(_) -> ok end, [
        fun finishes_at_capacity/0,
        fun reply_keeps_capacity/0,
        fun reply_helper_releases_on_failure/0,
        fun queued_payload_survives_producer/0
    ]}.

setup() ->
    [B0] = ar_weave:init(),
    ar_test_node:start(B0),
    ok.

%% @doc Packing existing work succeeds even when no new cache reservation is
%% available.
finishes_at_capacity() ->
    with_one_chunk_capacity(fun() ->
        {ok, CacheRef} = ar_chunk_cache:reserve(packing_test),
        ?assertEqual(full, ar_chunk_cache:reserve(other)),
        Ref = make_ref(),
        ok = ar_packing_server:request_repack(
            Ref,
            self(),
            {unpacked, unpacked, <<0>>, 1, <<>>, 1},
            CacheRef
        ),
        receive
            {chunk, {packed, Ref, {unpacked, <<0>>, 1, <<>>, 1}},
                ReplyCacheRef} ->
                ar_chunk_cache:release(ReplyCacheRef)
        after 5000 ->
            ?assert(false, "Packing required another chunk reservation")
        end,
        ?assertEqual(1, ar_chunk_cache:reserved_size()),
        ar_chunk_cache:release(CacheRef)
    end).

%% @doc An unpack reply keeps its reservation until the receiver releases it.
reply_keeps_capacity() ->
    with_one_chunk_capacity(fun() ->
        {ok, CacheRef} = ar_chunk_cache:reserve(packing_test),
        Ref = make_ref(),
        ok = ar_packing_server:request_unpack(
            Ref,
            self(),
            {unpacked, <<0>>, 1, <<>>, 1},
            CacheRef
        ),
        ar_chunk_cache:release(CacheRef),
        receive
            {chunk, {unpacked, Ref, _}, ReplyCacheRef} ->
                ?assertEqual(full, ar_chunk_cache:reserve(other)),
                ar_chunk_cache:release(ReplyCacheRef)
        after 5000 -> ?assert(false, "Missing unpack reply")
        end
    end).

%% @doc A reply remains accounted for during its check and is released if it fails.
reply_helper_releases_on_failure() ->
    with_one_chunk_capacity(fun() ->
        Ref = make_ref(),
        ok = ar_packing_server:request_unpack(
            Ref, self(), {unpacked, <<0>>, 1, <<>>, 1}
        ),
        ?assertError(
            reply_check_failed,
            ar_test_await:with_packing_reply(
                Ref,
                fun(_) ->
                    ?assertEqual(1, ar_chunk_cache:reserved_size()),
                    erlang:error(reply_check_failed)
                end,
                %% Match the other no-conversion packing reply deadlines.
                5000
            )
        ),
        ?assertEqual(0, ar_chunk_cache:reserved_size())
    end).

%% @doc Queued packing work remains accounted for after its producer exits.
queued_payload_survives_producer() ->
    with_one_chunk_capacity(fun() ->
        Parent = self(),
        PackingPID = whereis(ar_packing_server),
        ok = sys:suspend(PackingPID),
        Ref = make_ref(),
        try
            {Producer, Monitor} = spawn_monitor(fun() ->
                {ok, CacheRef} = ar_chunk_cache:reserve(packing_test),
                ok = ar_packing_server:request_unpack(
                    Ref,
                    Parent,
                    {unpacked, <<0>>, 1, <<>>, 1},
                    CacheRef
                )
            end),
            receive
                {'DOWN', Monitor, process, Producer, normal} -> ok
            end,
            ?assertEqual(1, ar_chunk_cache:reserved_size()),
            ?assertEqual(full, ar_chunk_cache:reserve(other))
        after
            ok = sys:resume(PackingPID)
        end,
        receive
            {chunk, {unpacked, Ref, _}, ReplyCacheRef} ->
                ?assertEqual(1, ar_chunk_cache:reserved_size()),
                ar_chunk_cache:release(ReplyCacheRef)
        after 5000 -> ?assert(false, "Queued work was lost with its producer")
        end
    end).

with_one_chunk_capacity(Test) ->
    Limit = ar_chunk_cache:limit(),
    ?assertEqual(0, ar_chunk_cache:reserved_size()),
    %% Capacity for one chunk exposes deadlocks from a second admission.
    ets:insert(ar_chunk_cache, {limit, 1}),
    try
        Test(),
        ?assertEqual(
            ok,
            ar_test_await:until(
                cache_drained,
                fun() -> ar_chunk_cache:reserved_size() =:= 0 end
            )
        )
    after
        ets:insert(ar_chunk_cache, {limit, Limit})
    end.
