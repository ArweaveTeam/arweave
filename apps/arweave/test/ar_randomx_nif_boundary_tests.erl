%%% @doc Boundary/validation tests for the RandomX NIF argument checks.
%%%
%%% Every test here pins down one of the argument checks added in the
%%% "more strict boundaries" commit. The rejection tests are cheap: the NIFs
%%% validate all arguments before creating a RandomX VM, so a light-mode state
%%% is enough and no hashing happens.
-module(ar_randomx_nif_boundary_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

%% MAX_CHUNK_SIZE in apps/arweave/c_src/randomx/rx512/ar_rx512_nif.c.
-define(MAX_CHUNK_SIZE, (256 * 1024)).

%% Keep the packing work per call to a minimum - these tests are about argument
%% validation, not about packing output.
-define(ROUNDS, 1).

setup() ->
    {rx512, State512} = ar_mine_randomx:init_light2(rx512, ?RANDOMX_PACKING_KEY, 0, 0),
    {rxsquared, StateRsp} = ar_mine_randomx:init_light2(rxsquared, ?RANDOMX_PACKING_KEY, 0, 0),
    {State512, StateRsp}.

test_register(TestFun, Fixture) ->
    {timeout, 120, {with, Fixture, [TestFun]}}.

randomx_nif_boundary_test_() ->
    {setup, fun setup/0,
        fun (SetupData) ->
            [
                test_register(fun test_init_rejects_zero_workers_in_fast_mode/1, SetupData),
                test_register(fun test_init_allows_zero_workers_in_light_mode/1, SetupData),
                test_register(fun test_decrypt_rejects_out_of_range_out_size/1, SetupData),
                test_register(fun test_decrypt_accepts_out_size_bounds/1, SetupData),
                test_register(fun test_encrypt_rejects_oversized_chunk/1, SetupData),
                test_register(fun test_reencrypt_rejects_out_of_range_chunk_size/1, SetupData),
                test_register(fun test_reencrypt_rejects_oversized_chunk/1, SetupData),
                test_register(fun test_reencrypt_rejects_empty_chunk/1, SetupData),
                test_register(fun test_reencrypt_accepts_chunk_size_bounds/1, SetupData),
                test_register(fun test_fused_entropy_rejects_negative_counts/1, SetupData),
                test_register(fun test_fused_entropy_zero_program_count/1, SetupData)
            ]
        end
    }.

%% -------------------------------------------------------------------------------------------
%% ar_randomx_impl.h: init_nif rejects numWorkers == 0 when building a dataset.
%% -------------------------------------------------------------------------------------------

%% numWorkers is only consulted in fast mode, where it drives the dataset build. Zero workers
%% used to be accepted and left the dataset uninitialised.
test_init_rejects_zero_workers_in_fast_mode(_Fixture) ->
    Key = ?RANDOMX_PACKING_KEY,
    ?assertError(badarg,
        ar_rx512_nif:rx512_init_nif(Key, ?RANDOMX_HASHING_MODE_FAST, 0, 0, 0)),
    ?assertError(badarg,
        ar_rx4096_nif:rx4096_init_nif(Key, ?RANDOMX_HASHING_MODE_FAST, 0, 0, 0)),
    ?assertError(badarg,
        ar_rxsquared_nif:rxsquared_init_nif(Key, ?RANDOMX_HASHING_MODE_FAST, 0, 0, 0)).

%% Light mode never builds a dataset, so it must keep accepting numWorkers == 0 - that is how
%% ar_mine_randomx:init_light2/4 calls it.
test_init_allows_zero_workers_in_light_mode(_Fixture) ->
    Key = ?RANDOMX_PACKING_KEY,
    ?assertMatch({ok, _},
        ar_rx512_nif:rx512_init_nif(Key, ?RANDOMX_HASHING_MODE_LIGHT, 0, 0, 0)),
    ?assertMatch({ok, _},
        ar_rx4096_nif:rx4096_init_nif(Key, ?RANDOMX_HASHING_MODE_LIGHT, 0, 0, 0)),
    ?assertMatch({ok, _},
        ar_rxsquared_nif:rxsquared_init_nif(Key, ?RANDOMX_HASHING_MODE_LIGHT, 0, 0, 0)).

%% -------------------------------------------------------------------------------------------
%% rx512_decrypt_chunk_nif: OutSize is read as an unsigned int and bounded by MAX_CHUNK_SIZE.
%% -------------------------------------------------------------------------------------------

%% The decrypted chunk is written into a MAX_CHUNK_SIZE stack buffer and then truncated to
%% OutSize, so an OutSize outside [0, MAX_CHUNK_SIZE] used to read past the buffer.
test_decrypt_rejects_out_of_range_out_size(_Fixture = {State512, _}) ->
    Chunk = crypto:strong_rand_bytes(?MAX_CHUNK_SIZE),
    Decrypt = fun(OutSize) ->
        ar_rx512_nif:rx512_decrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, Chunk, OutSize, ?ROUNDS, 0, 0, 0)
    end,
    %% Negative: rejected by enif_get_uint.
    ?assertError(badarg, Decrypt(-1)),
    ?assertError(badarg, Decrypt(-?MAX_CHUNK_SIZE)),
    %% Past the buffer: rejected by the explicit upper bound.
    ?assertError(badarg, Decrypt(?MAX_CHUNK_SIZE + 1)),
    %% Wider than an unsigned int: rejected by enif_get_uint.
    ?assertError(badarg, Decrypt(1 bsl 40)).

%% Both ends of the accepted range must still work, otherwise the bound check above would
%% pass just as well with everything rejected.
test_decrypt_accepts_out_size_bounds(_Fixture = {State512, _}) ->
    Chunk = crypto:strong_rand_bytes(?MAX_CHUNK_SIZE),
    Decrypt = fun(OutSize) ->
        ar_rx512_nif:rx512_decrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, Chunk, OutSize, ?ROUNDS, 0, 0, 0)
    end,
    {ok, Full} = Decrypt(?MAX_CHUNK_SIZE),
    ?assertEqual(?MAX_CHUNK_SIZE, byte_size(Full)),
    {ok, Empty} = Decrypt(0),
    ?assertEqual(<<>>, Empty).

%% -------------------------------------------------------------------------------------------
%% rx512 encrypt/reencrypt: the input chunk is bounded by MAX_CHUNK_SIZE.
%% -------------------------------------------------------------------------------------------

test_encrypt_rejects_oversized_chunk(_Fixture = {State512, _}) ->
    Oversized = crypto:strong_rand_bytes(?MAX_CHUNK_SIZE + 64),
    ?assertError(badarg,
        ar_rx512_nif:rx512_encrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, Oversized, ?ROUNDS, 0, 0, 0)).

%% rx512_reencrypt_chunk_nif decrypts into a MAX_CHUNK_SIZE stack buffer using the *input
%% chunk's* size, so an oversized input chunk overflowed that buffer before this bound existed.
test_reencrypt_rejects_oversized_chunk(_Fixture = {State512, _}) ->
    Oversized = crypto:strong_rand_bytes(?MAX_CHUNK_SIZE + 64),
    ?assertError(badarg,
        ar_rx512_nif:rx512_reencrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, ?RANDOMX_PACKING_KEY, Oversized,
            ?MAX_CHUNK_SIZE, ?ROUNDS, ?ROUNDS, 0, 0, 0)).

test_reencrypt_rejects_out_of_range_chunk_size(_Fixture = {State512, _}) ->
    Chunk = crypto:strong_rand_bytes(?MAX_CHUNK_SIZE),
    Reencrypt = fun(ChunkSize) ->
        ar_rx512_nif:rx512_reencrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, ?RANDOMX_PACKING_KEY, Chunk,
            ChunkSize, ?ROUNDS, ?ROUNDS, 0, 0, 0)
    end,
    ?assertError(badarg, Reencrypt(0)),
    ?assertError(badarg, Reencrypt(-1)),
    ?assertError(badarg, Reencrypt(?MAX_CHUNK_SIZE + 1)),
    ?assertError(badarg, Reencrypt(1 bsl 40)).

%% ChunkSize sizes the decrypted output; the reencrypted output is always a full chunk.
%% Only the upper end is accepted - zero and below are rejected by the `<= 0` bound.
test_reencrypt_accepts_chunk_size_bounds(_Fixture = {State512, _}) ->
    Chunk = crypto:strong_rand_bytes(?MAX_CHUNK_SIZE),
    Reencrypt = fun(ChunkSize) ->
        ar_rx512_nif:rx512_reencrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, ?RANDOMX_PACKING_KEY, Chunk,
            ChunkSize, ?ROUNDS, ?ROUNDS, 0, 0, 0)
    end,
    {ok, Reencrypted, Decrypted} = Reencrypt(?MAX_CHUNK_SIZE),
    ?assertEqual(?MAX_CHUNK_SIZE, byte_size(Reencrypted)),
    ?assertEqual(?MAX_CHUNK_SIZE, byte_size(Decrypted)),
    {ok, Reencrypted1, Decrypted1} = Reencrypt(1),
    ?assertEqual(?MAX_CHUNK_SIZE, byte_size(Reencrypted1)),
    ?assertEqual(1, byte_size(Decrypted1)).

%% ar_mine_randomx_tests:test_empty_chunk_fails covers encrypt/decrypt; reencrypt takes
%% the chunk in a different argument position, so it gets its own check.
test_reencrypt_rejects_empty_chunk(_Fixture = {State512, _}) ->
    ?assertError(badarg,
        ar_rx512_nif:rx512_reencrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, ?RANDOMX_PACKING_KEY, <<>>,
            ?MAX_CHUNK_SIZE, ?ROUNDS, ?ROUNDS, 0, 0, 0)).

%% -------------------------------------------------------------------------------------------
%% rsp_fused_entropy_nif: the counts are read as unsigned ints.
%% -------------------------------------------------------------------------------------------

%% Each of these used to be read with enif_get_int and then used as a loop bound or a
%% calloc/new[] element count.
test_fused_entropy_rejects_negative_counts(_Fixture = {_, StateRsp}) ->
    Args = [?SUB_CHUNK_COUNT, ?SUB_CHUNK_SIZE, ?REPLICA_2_9_RANDOMX_LANE_COUNT,
            ?REPLICA_2_9_RANDOMX_DEPTH, ?REPLICA_2_9_RANDOMX_PROGRAM_COUNT],
    Call = fun([SubChunkCount, SubChunkSize, LaneCount, RxDepth, ProgramCount]) ->
        ar_rxsquared_nif:rsp_fused_entropy_nif(
            StateRsp, SubChunkCount, SubChunkSize, LaneCount, RxDepth,
            0, 0, 0, ProgramCount, ?RANDOMX_PACKING_KEY)
    end,
    lists:foreach(
        fun(Index) ->
            Negative = lists:sublist(Args, Index - 1)
                ++ [-1]
                ++ lists:nthtail(Index, Args),
            ?assertError(badarg, Call(Negative))
        end,
        lists:seq(1, length(Args))).

%% _rsp_exec_inplace loops while `chain + 1 < programCount`. Written the other way round
%% (`chain < programCount - 1`) with an unsigned programCount, a program count of 0 wraps to
%% 4294967295 and the call never returns - this test would then fail on the eunit timeout.
test_fused_entropy_zero_program_count(_Fixture = {_, StateRsp}) ->
    LaneCount = 1,
    RxDepth = 1,
    {ok, Entropy} = ar_rxsquared_nif:rsp_fused_entropy_nif(
        StateRsp, ?SUB_CHUNK_COUNT, ?SUB_CHUNK_SIZE, LaneCount, RxDepth,
        0, 0, 0, 0, ?RANDOMX_PACKING_KEY),
    ?assertEqual(LaneCount * ?RANDOMX_SCRATCHPAD_SIZE, byte_size(Entropy)).
