-module(arweave_storage_chunk_storage_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("common_test/include/ct.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() ->
    [
        chunk_bucket,
        chunk_byte_from_bucket_end,
        well_aligned,
        not_aligned,
        cross_file_aligned,
        cross_file_not_aligned,
        defrag_command,
        put_to_missing_worker,
        put_to_killed_worker
    ].

init_per_suite(Config) -> arweave_storage_ct_util:init_suite(Config).

end_per_suite(Config) -> arweave_storage_ct_util:end_suite(Config).

init_per_testcase(Name, Config) ->
    Config2 = arweave_storage_ct_util:init_case(Name, Config),
    case Name of
        N when N =:= chunk_bucket; N =:= chunk_byte_from_bucket_end ->
            meck:new(arweave_constants, [passthrough]),
            meck:expect(
                arweave_constants, strict_data_split_threshold, fun() ->
                    700_000
                end
            );
        N when
            N =:= well_aligned;
            N =:= not_aligned;
            N =:= cross_file_aligned;
            N =:= cross_file_not_aligned
        ->
            {ok, _} = arweave_storage:activate();
        _ ->
            ok
    end,
    Config2.

end_per_testcase(Name, Config) ->
    case Name of
        N when N =:= chunk_bucket; N =:= chunk_byte_from_bucket_end ->
            meck:unload(arweave_constants);
        _ ->
            ok
    end,
    arweave_storage_ct_util:end_case(Name, Config).

%%====================================================================
%% Test cases
%%====================================================================

chunk_bucket(_Config) ->
    case arweave_constants:strict_data_split_threshold() of
        700_000 ->
            ok;
        _ ->
            throw(unexpected_strict_data_split_threshold)
    end,

    %% get_chunk_bucket_end pads the provided offset
    %% get_chunk_bucket_start does not pad the provided offset

    %% At and before the STRICT_DATA_SPLIT_THRESHOLD, offsets are not padded.
    ?assertEqual(262144, arweave_storage_chunk_storage:get_chunk_bucket_end(0)),
    ?assertEqual(0, arweave_storage_chunk_storage:get_chunk_bucket_start(0)),

    ?assertEqual(262144, arweave_storage_chunk_storage:get_chunk_bucket_end(1)),
    ?assertEqual(0, arweave_storage_chunk_storage:get_chunk_bucket_start(1)),

    ?assertEqual(
        262144,
        arweave_storage_chunk_storage:get_chunk_bucket_end(?DATA_CHUNK_SIZE - 1)
    ),
    ?assertEqual(
        0,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            ?DATA_CHUNK_SIZE - 1
        )
    ),

    ?assertEqual(
        262144,
        arweave_storage_chunk_storage:get_chunk_bucket_end(?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        0,
        arweave_storage_chunk_storage:get_chunk_bucket_start(?DATA_CHUNK_SIZE)
    ),

    ?assertEqual(
        262144,
        arweave_storage_chunk_storage:get_chunk_bucket_end(?DATA_CHUNK_SIZE + 1)
    ),
    ?assertEqual(
        0,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            ?DATA_CHUNK_SIZE + 1
        )
    ),

    ?assertEqual(
        524288,
        arweave_storage_chunk_storage:get_chunk_bucket_end(2 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        262144,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            2 * ?DATA_CHUNK_SIZE
        )
    ),

    ?assertEqual(
        524288,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            2 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        262144,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            2 * ?DATA_CHUNK_SIZE + 1
        )
    ),

    ?assertEqual(
        524288,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            arweave_constants:strict_data_split_threshold() - 1
        )
    ),
    ?assertEqual(
        262144,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            arweave_constants:strict_data_split_threshold() - 1
        )
    ),

    ?assertEqual(
        524288,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            arweave_constants:strict_data_split_threshold()
        )
    ),
    ?assertEqual(
        262144,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            arweave_constants:strict_data_split_threshold()
        )
    ),

    %% After the STRICT_DATA_SPLIT_THRESHOLD, offsets are padded.
    ?assertEqual(
        786432,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            arweave_constants:strict_data_split_threshold() + 1
        )
    ),
    ?assertEqual(
        524288,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            arweave_constants:strict_data_split_threshold() + 1
        )
    ),

    ?assertEqual(
        786432,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            3 * ?DATA_CHUNK_SIZE - 1
        )
    ),
    ?assertEqual(
        524288,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            3 * ?DATA_CHUNK_SIZE - 1
        )
    ),

    ?assertEqual(
        786432,
        arweave_storage_chunk_storage:get_chunk_bucket_end(3 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        524288,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            3 * ?DATA_CHUNK_SIZE
        )
    ),

    ?assertEqual(
        786432,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            3 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        524288,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            3 * ?DATA_CHUNK_SIZE + 1
        )
    ),

    ?assertEqual(
        1048576,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            4 * ?DATA_CHUNK_SIZE - 1
        )
    ),
    ?assertEqual(
        786432,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            4 * ?DATA_CHUNK_SIZE - 1
        )
    ),

    ?assertEqual(
        1048576,
        arweave_storage_chunk_storage:get_chunk_bucket_end(4 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        786432,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            4 * ?DATA_CHUNK_SIZE
        )
    ),

    ?assertEqual(
        1048576,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            4 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        786432,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            4 * ?DATA_CHUNK_SIZE + 1
        )
    ),

    ?assertEqual(
        1310720,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            5 * ?DATA_CHUNK_SIZE - 1
        )
    ),
    ?assertEqual(
        1048576,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            5 * ?DATA_CHUNK_SIZE - 1
        )
    ),

    ?assertEqual(
        1310720,
        arweave_storage_chunk_storage:get_chunk_bucket_end(5 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        1048576,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            5 * ?DATA_CHUNK_SIZE
        )
    ),

    ?assertEqual(
        1310720,
        arweave_storage_chunk_storage:get_chunk_bucket_end(
            5 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        1048576,
        arweave_storage_chunk_storage:get_chunk_bucket_start(
            5 * ?DATA_CHUNK_SIZE + 1
        )
    ).

chunk_byte_from_bucket_end(_Config) ->
    ?assertEqual(
        262143,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(262144)
    ),
    ?assertEqual(
        524287,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(524288)
    ),
    ?assertEqual(
        700000,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(786432)
    ),
    ?assertEqual(
        962144,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(1048576)
    ),
    ?assertEqual(
        1224288,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(1310720)
    ),
    ?assertEqual(
        1486432,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(1572864)
    ),
    ?assertEqual(
        1748576,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(1835008)
    ),
    ?assertEqual(
        2010720,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(2097152)
    ),
    ?assertEqual(
        2272864,
        arweave_storage_chunk_storage:get_chunk_byte_from_bucket_end(2359296)
    ).

well_aligned(_Config) ->
    Packing = arweave_storage_module:get_packing(?DEFAULT_MODULE),
    C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    {ok, unpacked} = arweave_storage:put_chunk(
        2 * ?DATA_CHUNK_SIZE, C1, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, 2 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(2 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(2 * ?DATA_CHUNK_SIZE + 1, ?DEFAULT_MODULE)
    ),
    arweave_storage_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(not_found, 2 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    arweave_storage:put_chunk(?DATA_CHUNK_SIZE, C2, Packing, ?DEFAULT_MODULE),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(not_found, 2 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    arweave_storage:put_chunk(
        2 * ?DATA_CHUNK_SIZE, C1, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, 2 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        [{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
        arweave_storage_chunk_storage:get_range(0, 2 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        [{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
        arweave_storage_chunk_storage:get_range(1, 2 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        [{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
        arweave_storage_chunk_storage:get_range(1, 2 * ?DATA_CHUNK_SIZE - 1)
    ),
    ?assertEqual(
        [{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
        arweave_storage_chunk_storage:get_range(0, 3 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        [{?DATA_CHUNK_SIZE, C2}, {2 * ?DATA_CHUNK_SIZE, C1}],
        arweave_storage_chunk_storage:get_range(0, ?DATA_CHUNK_SIZE + 1)
    ),
    arweave_storage:put_chunk(
        3 * ?DATA_CHUNK_SIZE, C3, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, 2 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C3, 3 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(3 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(3 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(3 * ?DATA_CHUNK_SIZE + 1, ?DEFAULT_MODULE)
    ),
    arweave_storage:put_chunk(
        2 * ?DATA_CHUNK_SIZE, C2, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, 2 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C3, 3 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(3 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    arweave_storage_chunk_storage:delete(?DATA_CHUNK_SIZE),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(not_found, ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        [], arweave_storage_chunk_storage:get_range(0, ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, 2 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C3, 3 * ?DATA_CHUNK_SIZE),
        arweave_storage_ct_util:chunk_samples(3 * ?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        [{2 * ?DATA_CHUNK_SIZE, C2}, {3 * ?DATA_CHUNK_SIZE, C3}],
        arweave_storage_chunk_storage:get_range(0, 4 * ?DATA_CHUNK_SIZE)
    ),
    ?assertEqual(
        [],
        arweave_storage_chunk_storage:get_range(
            7 * ?DATA_CHUNK_SIZE, 13 * ?DATA_CHUNK_SIZE
        )
    ).

not_aligned(_Config) ->
    Packing = arweave_storage_module:get_packing(?DEFAULT_MODULE),
    C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    arweave_storage:put_chunk(
        2 * ?DATA_CHUNK_SIZE + 7, C1, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, 2 * ?DATA_CHUNK_SIZE + 7),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    arweave_storage_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE + 7),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(not_found, 2 * ?DATA_CHUNK_SIZE + 7),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    arweave_storage:put_chunk(
        2 * ?DATA_CHUNK_SIZE + 7, C1, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, 2 * ?DATA_CHUNK_SIZE + 7),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(2 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(?DATA_CHUNK_SIZE + 7 - 1, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found, arweave_storage:get_chunk(?DATA_CHUNK_SIZE, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(?DATA_CHUNK_SIZE - 1, ?DEFAULT_MODULE)
    ),
    ?assertEqual(not_found, arweave_storage:get_chunk(0, ?DEFAULT_MODULE)),
    ?assertEqual(not_found, arweave_storage:get_chunk(1, ?DEFAULT_MODULE)),
    arweave_storage:put_chunk(
        ?DATA_CHUNK_SIZE + 3, C2, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, ?DATA_CHUNK_SIZE + 3),
        arweave_storage_ct_util:chunk_samples(?DATA_CHUNK_SIZE + 3, ?DEFAULT_MODULE)
    ),
    ?assertEqual(not_found, arweave_storage:get_chunk(0, ?DEFAULT_MODULE)),
    ?assertEqual(not_found, arweave_storage:get_chunk(1, ?DEFAULT_MODULE)),
    ?assertEqual(not_found, arweave_storage:get_chunk(2, ?DEFAULT_MODULE)),
    arweave_storage_chunk_storage:delete(2 * ?DATA_CHUNK_SIZE + 7),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, ?DATA_CHUNK_SIZE + 3),
        arweave_storage_ct_util:chunk_samples(?DATA_CHUNK_SIZE + 3, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(not_found, 2 * ?DATA_CHUNK_SIZE + 7),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    arweave_storage:put_chunk(
        3 * ?DATA_CHUNK_SIZE + 7, C3, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C3, 3 * ?DATA_CHUNK_SIZE + 7),
        arweave_storage_ct_util:chunk_samples(3 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    arweave_storage:put_chunk(
        3 * ?DATA_CHUNK_SIZE + 7, C1, Packing, ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, 3 * ?DATA_CHUNK_SIZE + 7),
        arweave_storage_ct_util:chunk_samples(3 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    arweave_storage:put_chunk(
        4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2,
        C2,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, 4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2),
        arweave_storage_ct_util:chunk_samples(
            4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(3 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(3 * ?DATA_CHUNK_SIZE + 8, ?DEFAULT_MODULE)
    ),
    arweave_storage:put_chunk(
        5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1,
        C2,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, 5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1),
        arweave_storage_ct_util:chunk_samples(
            5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(not_found, 2 * ?DATA_CHUNK_SIZE + 7),
        arweave_storage_ct_util:chunk_samples(2 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    arweave_storage_chunk_storage:delete(
        4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            not_found, 4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            4 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, 5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1),
        arweave_storage_ct_util:chunk_samples(
            5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, 3 * ?DATA_CHUNK_SIZE + 7),
        arweave_storage_ct_util:chunk_samples(3 * ?DATA_CHUNK_SIZE + 7, ?DEFAULT_MODULE)
    ),
    ?assertEqual(
        [{3 * ?DATA_CHUNK_SIZE + 7, C1}],
        arweave_storage_chunk_storage:get_range(
            2 * ?DATA_CHUNK_SIZE + 7, 2 * ?DATA_CHUNK_SIZE
        )
    ),
    ?assertEqual(
        [{3 * ?DATA_CHUNK_SIZE + 7, C1}],
        arweave_storage_chunk_storage:get_range(
            2 * ?DATA_CHUNK_SIZE + 6, 2 * ?DATA_CHUNK_SIZE
        )
    ),
    ?assertEqual(
        [
            {3 * ?DATA_CHUNK_SIZE + 7, C1},
            {5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2}
        ],
        %% The end offset of the second chunk is bigger than Start + Size but
        %% it is included because Start + Size is bigger than the start offset
        %% of the bucket where the last chunk is placed.
        arweave_storage_chunk_storage:get_range(
            2 * ?DATA_CHUNK_SIZE + 7, 2 * ?DATA_CHUNK_SIZE + 1
        )
    ),
    ?assertEqual(
        [
            {3 * ?DATA_CHUNK_SIZE + 7, C1},
            {5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2}
        ],
        arweave_storage_chunk_storage:get_range(
            2 * ?DATA_CHUNK_SIZE + 7, 3 * ?DATA_CHUNK_SIZE
        )
    ),
    ?assertEqual(
        [
            {3 * ?DATA_CHUNK_SIZE + 7, C1},
            {5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2}
        ],
        arweave_storage_chunk_storage:get_range(
            2 * ?DATA_CHUNK_SIZE + 7 - 1, 3 * ?DATA_CHUNK_SIZE
        )
    ),
    ?assertEqual(
        [
            {3 * ?DATA_CHUNK_SIZE + 7, C1},
            {5 * ?DATA_CHUNK_SIZE + ?DATA_CHUNK_SIZE div 2 + 1, C2}
        ],
        arweave_storage_chunk_storage:get_range(
            2 * ?DATA_CHUNK_SIZE, 4 * ?DATA_CHUNK_SIZE
        )
    ).

cross_file_aligned(_Config) ->
    Packing = arweave_storage_module:get_packing(?DEFAULT_MODULE),
    C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    arweave_storage:put_chunk(
        arweave_config:get([chunk_storage_file_size]),
        C1,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, arweave_config:get([chunk_storage_file_size])),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]), ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]), ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]) + 1, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(not_found, arweave_storage:get_chunk(0, ?DEFAULT_MODULE)),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]) - ?DATA_CHUNK_SIZE -
                1,
            ?DEFAULT_MODULE
        )
    ),
    arweave_storage:put_chunk(
        arweave_config:get([chunk_storage_file_size]) + ?DATA_CHUNK_SIZE,
        C2,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            C2, arweave_config:get([chunk_storage_file_size]) + ?DATA_CHUNK_SIZE
        ),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]) + ?DATA_CHUNK_SIZE,
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, arweave_config:get([chunk_storage_file_size])),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]), ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        [
            {arweave_config:get([chunk_storage_file_size]), C1},
            {
                arweave_config:get([chunk_storage_file_size]) +
                    ?DATA_CHUNK_SIZE,
                C2
            }
        ],
        arweave_storage_chunk_storage:get_range(
            arweave_config:get([chunk_storage_file_size]) - ?DATA_CHUNK_SIZE,
            2 * ?DATA_CHUNK_SIZE
        )
    ),
    ?assertEqual(
        [
            {arweave_config:get([chunk_storage_file_size]), C1},
            {
                arweave_config:get([chunk_storage_file_size]) +
                    ?DATA_CHUNK_SIZE,
                C2
            }
        ],
        arweave_storage_chunk_storage:get_range(
            arweave_config:get([chunk_storage_file_size]) - 2 * ?DATA_CHUNK_SIZE -
                1,
            4 * ?DATA_CHUNK_SIZE
        )
    ),
    ?assertEqual(not_found, arweave_storage:get_chunk(0, ?DEFAULT_MODULE)),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]) - ?DATA_CHUNK_SIZE -
                1,
            ?DEFAULT_MODULE
        )
    ),
    arweave_storage:delete_chunk(
        arweave_config:get([chunk_storage_file_size]), ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            not_found, arweave_config:get([chunk_storage_file_size])
        ),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]), ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            C2, arweave_config:get([chunk_storage_file_size]) + ?DATA_CHUNK_SIZE
        ),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]) + ?DATA_CHUNK_SIZE,
            ?DEFAULT_MODULE
        )
    ),
    arweave_storage:put_chunk(
        arweave_config:get([chunk_storage_file_size]),
        C2,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C2, arweave_config:get([chunk_storage_file_size])),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]), ?DEFAULT_MODULE
        )
    ).

cross_file_not_aligned(_Config) ->
    Packing = arweave_storage_module:get_packing(?DEFAULT_MODULE),
    C1 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C2 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C3 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C4 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    C5 = crypto:strong_rand_bytes(?DATA_CHUNK_SIZE),
    arweave_storage:put_chunk(
        arweave_config:get([chunk_storage_file_size]) + 1,
        C1,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, arweave_config:get([chunk_storage_file_size]) + 1),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]) + 1, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]) + 1, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]) - ?DATA_CHUNK_SIZE,
            ?DEFAULT_MODULE
        )
    ),
    arweave_storage:put_chunk(
        2 * arweave_config:get([chunk_storage_file_size]) +
            ?DATA_CHUNK_SIZE div 2,
        C2,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            C2,
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]) + 1, ?DEFAULT_MODULE
        )
    ),
    arweave_storage:put_chunk(
        2 * arweave_config:get([chunk_storage_file_size]) -
            ?DATA_CHUNK_SIZE div 2,
        C3,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            C2,
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            C3,
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    arweave_storage:put_chunk(
        2 * arweave_config:get([chunk_storage_file_size]) +
            3 * ?DATA_CHUNK_SIZE div 2,
        C4,
        Packing,
        ?DEFAULT_MODULE
    ),
    arweave_storage:put_chunk(
        2 * arweave_config:get([chunk_storage_file_size]) +
            5 * ?DATA_CHUNK_SIZE div 2,
        C5,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        [
            {
                2 * arweave_config:get([chunk_storage_file_size]) +
                    ?DATA_CHUNK_SIZE div 2,
                C2
            },
            {
                2 * arweave_config:get([chunk_storage_file_size]) +
                    3 * ?DATA_CHUNK_SIZE div 2,
                C4
            }
        ],
        arweave_storage_chunk_storage:get_range(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2,
            ?DATA_CHUNK_SIZE * 2
        )
    ),
    ?assertEqual(
        [
            {
                2 * arweave_config:get([chunk_storage_file_size]) +
                    ?DATA_CHUNK_SIZE div 2,
                C2
            },
            {
                2 * arweave_config:get([chunk_storage_file_size]) +
                    3 * ?DATA_CHUNK_SIZE div 2,
                C4
            },
            {
                2 * arweave_config:get([chunk_storage_file_size]) +
                    5 * ?DATA_CHUNK_SIZE div 2,
                C5
            }
        ],
        arweave_storage_chunk_storage:get_range(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2 + 10,
            ?DATA_CHUNK_SIZE * 2
        )
    ),

    ?assertEqual(
        [
            {
                2 * arweave_config:get([chunk_storage_file_size]) -
                    ?DATA_CHUNK_SIZE div 2,
                C3
            },
            {
                2 * arweave_config:get([chunk_storage_file_size]) +
                    ?DATA_CHUNK_SIZE div 2,
                C2
            }
        ],
        arweave_storage_chunk_storage:get_range(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2 - ?DATA_CHUNK_SIZE,
            ?DATA_CHUNK_SIZE * 2
        )
    ),
    ?assertEqual(
        [
            {
                2 * arweave_config:get([chunk_storage_file_size]) -
                    ?DATA_CHUNK_SIZE div 2,
                C3
            },
            {
                2 * arweave_config:get([chunk_storage_file_size]) +
                    ?DATA_CHUNK_SIZE div 2,
                C2
            },
            {
                2 * arweave_config:get([chunk_storage_file_size]) +
                    3 * ?DATA_CHUNK_SIZE div 2,
                C4
            }
        ],
        arweave_storage_chunk_storage:get_range(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2 - ?DATA_CHUNK_SIZE + 10,
            ?DATA_CHUNK_SIZE * 2
        )
    ),

    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]) + 1, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2 - 1,
            ?DEFAULT_MODULE
        )
    ),
    arweave_storage_chunk_storage:delete(
        2 * arweave_config:get([chunk_storage_file_size]) -
            ?DATA_CHUNK_SIZE div 2
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            not_found,
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            C2,
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(C1, arweave_config:get([chunk_storage_file_size]) + 1),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]) + 1, ?DEFAULT_MODULE
        )
    ),
    arweave_storage_chunk_storage:delete(
        arweave_config:get([chunk_storage_file_size]) + 1
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            not_found, arweave_config:get([chunk_storage_file_size]) + 1
        ),
        arweave_storage_ct_util:chunk_samples(
            arweave_config:get([chunk_storage_file_size]) + 1, ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            not_found,
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            C2,
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    arweave_storage_chunk_storage:delete(
        2 * arweave_config:get([chunk_storage_file_size]) +
            ?DATA_CHUNK_SIZE div 2
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            not_found,
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) +
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    arweave_storage_chunk_storage:delete(
        arweave_config:get([chunk_storage_file_size]) + 1
    ),
    arweave_storage_chunk_storage:delete(
        100 * arweave_config:get([chunk_storage_file_size]) + 1
    ),
    arweave_storage:put_chunk(
        2 * arweave_config:get([chunk_storage_file_size]) -
            ?DATA_CHUNK_SIZE div 2,
        C1,
        Packing,
        ?DEFAULT_MODULE
    ),
    ?assertEqual(
        arweave_storage_ct_util:expected_samples(
            C1,
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2
        ),
        arweave_storage_ct_util:chunk_samples(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ),
    ?assertEqual(
        not_found,
        arweave_storage:get_chunk(
            2 * arweave_config:get([chunk_storage_file_size]) -
                ?DATA_CHUNK_SIZE div 2,
            ?DEFAULT_MODULE
        )
    ).

defrag_command(Config) ->
    RandomID = crypto:strong_rand_bytes(16),
    Filepath = filename:join(
        ?config(priv_dir, Config),
        "test_defrag_" ++ binary_to_list(arweave_util:encode(RandomID))
    ),
    ok = filelib:ensure_dir(Filepath),
    {ok, F} = file:open(Filepath, [binary, write]),
    {O1, C1} = {236, crypto:strong_rand_bytes(262144)},
    {O2, C2} = {262144, crypto:strong_rand_bytes(262144)},
    {O3, C3} = {262143, crypto:strong_rand_bytes(262144)},
    file:pwrite(F, 1, <<"a">>),
    file:pwrite(F, 1000, <<"b">>),
    file:pwrite(F, 1000000, <<"cde">>),
    file:pwrite(F, 10000001, <<O1:24, C1/binary, O2:24, C2/binary>>),
    file:pwrite(F, 30000001, <<O3:24, C3/binary>>),
    file:close(F),
    arweave_storage_chunk_storage:defrag_files([Filepath]),
    {ok, F2} = file:open(Filepath, [binary, read]),
    ?assertEqual({ok, <<0>>}, file:pread(F2, 0, 1)),
    ?assertEqual({ok, <<"a">>}, file:pread(F2, 1, 1)),
    ?assertEqual({ok, <<0>>}, file:pread(F2, 2, 1)),
    ?assertEqual({ok, <<"b">>}, file:pread(F2, 1000, 1)),
    ?assertEqual({ok, <<"c">>}, file:pread(F2, 1000000, 1)),
    ?assertEqual({ok, <<"cde">>}, file:pread(F2, 1000000, 3)),
    ?assertEqual({ok, C1}, file:pread(F2, 10000001 + 3, 262144)),
    ?assertMatch({ok, <<O1:24, _/binary>>}, file:pread(F2, 10000001, 10)),
    ?assertMatch(
        {ok,
            <<O1:24, C1:262144/binary, O2:24, C2:262144/binary,
                0:((262144 + 3) * 2 * 8)>>},
        file:pread(F2, 10000001, (262144 + 3) * 4)
    ),
    ?assertMatch(
        {ok, <<O3:24, C3:262144/binary>>},
        % End of file => +100 is ignored.
        file:pread(F2, 30000001, 262144 + 3 + 100)
    ),
    ok = file:close(F2),
    ok = file:delete(Filepath).

put_to_missing_worker(_Config) ->
    StoreID = "ar_chunk_storage_put_test_missing",
    seed_label_cache(StoreID),
    ?assertEqual(
        {error, noproc},
        arweave_storage:put_chunk(262144, <<>>, unpacked, StoreID)
    ).

put_to_killed_worker(_Config) ->
    StoreID = "ar_chunk_storage_put_test_killed",
    seed_label_cache(StoreID),
    Name = arweave_storage_chunk_storage:name(StoreID),
    TestPID = self(),
    %% A stub worker that accepts the call, signals us, and never replies.
    Stub = spawn(fun() ->
        receive
            {'$gen_call', _From, _Request} ->
                TestPID ! stub_got_call,
                receive
                after infinity -> ok
                end
        end
    end),
    register(Name, Stub),
    spawn_link(fun() ->
        TestPID !
            {put_result,
                arweave_storage:put_chunk(262144, <<>>, unpacked, StoreID)}
    end),
    receive
        stub_got_call -> ok
    after 10000 -> ?assert(false, "stub never received the put call")
    end,
    exit(Stub, kill),
    receive
        {put_result, Result} ->
            ?assertEqual({error, killed}, Result)
    after 10000 ->
        ?assert(false, "put did not return after the worker was killed")
    end.

%%====================================================================
%% Helpers
%%====================================================================

%% @doc Seed arweave_storage's label cache so name/1
%% resolves without a configured storage module.
seed_label_cache(StoreID) ->
    ets:insert(arweave_storage, {{label, StoreID}, StoreID}).
