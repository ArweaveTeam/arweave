-module(arweave_storage_entropy_storage_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [record_chunk_releases_semaphore_on_exception].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_storage),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

%%====================================================================
%% Test cases
%%====================================================================

record_chunk_releases_semaphore_on_exception(_Config) ->
    Filepath = "test_entropy_semaphore_file",
    case ets:info(ar_entropy_storage) of
        undefined -> ets:new(ar_entropy_storage, [set, public, named_table]);
        _ -> ok
    end,
    ets:delete(ar_entropy_storage, {semaphore, Filepath}),
    meck:new(arweave_constants, [passthrough]),
    meck:expect(arweave_constants, get_chunk_padded_offset, fun(X) -> X end),
    meck:new(arweave_storage, [passthrough]),
    meck:expect(arweave_storage, get_chunk_bucket_start, fun(_) -> 0 end),
    meck:expect(arweave_storage, locate_chunk_on_disk, fun(_, _) ->
        {0, Filepath, 0, 0}
    end),
    meck:expect(arweave_storage, is_recorded, fun
        (_, any_packing, _, _) ->
            throw(boom);
        (Offset, Packing, ID, StoreID) ->
            meck:passthrough([Offset, Packing, ID, StoreID])
    end),
    try
        Threw =
            try
                arweave_storage_entropy_storage:record_chunk(
                    1, <<0>>, "store", #{}, {true, <<"addr">>}
                ),
                false
            catch
                throw:boom -> true
            end,
        ?assert(Threw),
        ?assertEqual([], ets:lookup(ar_entropy_storage, {semaphore, Filepath}))
    after
        meck:unload([arweave_constants, arweave_storage])
    end.
