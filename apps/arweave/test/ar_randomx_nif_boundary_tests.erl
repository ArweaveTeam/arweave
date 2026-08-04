%%% @doc Boundary/validation tests for the RandomX NIF argument checks.
%%%
%%% The first group pins down the argument checks added in the "more strict
%%% boundaries" commit. The second group covers the checks added on top of it.
%%% The rejection tests are cheap: the NIFs validate all arguments before
%%% creating a RandomX VM, so a light-mode state is enough and no hashing
%%% happens.
-module(ar_randomx_nif_boundary_tests).
-test_category([fast]).

%% Runs on the throwaway node started by call_on_peer/3, not from this one.
-export([remote_fused_entropy/1]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

%% MAX_CHUNK_SIZE in apps/arweave/c_src/randomx/rx512/ar_rx512_nif.c.
-define(MAX_CHUNK_SIZE, (256 * 1024)).

%% randomx_decrypt_chunk asserts inChunkSize % (2 * FEISTEL_BLOCK_LENGTH) == 0, so a short
%% chunk has to be a multiple of 64 bytes.
-define(SHORT_CHUNK_SIZE, 1024).

%% Keep the packing work per call to a minimum - these tests are about argument
%% validation, not about packing output.
-define(ROUNDS, 1).

-define(PEER_TIMEOUT, 30000).

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
                test_register(fun test_fused_entropy_zero_program_count/1, SetupData),

                %% Checks added on top of that commit.
                test_register(fun test_decrypt_rejects_out_size_above_chunk_size/1, SetupData),
                test_register(fun test_reencrypt_rejects_chunk_size_above_chunk_size/1,
                    SetupData),
                test_register(fun test_init_rejects_unknown_hashing_mode/1, SetupData),
                test_register(fun test_fused_entropy_rejects_zero_lane_count/1, SetupData),
                test_register(fun test_fused_entropy_rejects_zero_depth/1, SetupData),
                test_register(fun test_fused_entropy_lane_count_does_not_abort/1, SetupData)
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

%% ===========================================================================================
%% Checks added on top of the "more strict boundaries" commit.
%% ===========================================================================================

%% decrypt writes inputChunk.size bytes into an uninitialised MAX_CHUNK_SIZE stack buffer but
%% returns outChunkLen of them, so a short chunk with a large OutSize returns C stack: 259830
%% of the 261120 extra bytes were non-zero when measured, covering all 256 byte values.
%% outChunkLen is bounded by MAX_CHUNK_SIZE above, but not by inputChunk.size.
test_decrypt_rejects_out_size_above_chunk_size(_Fixture = {State512, _}) ->
    Chunk = crypto:strong_rand_bytes(?SHORT_CHUNK_SIZE),
    ?assertError(badarg,
        ar_rx512_nif:rx512_decrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, Chunk, ?MAX_CHUNK_SIZE, ?ROUNDS, 0, 0, 0)),
    ?assertError(badarg,
        ar_rx512_nif:rx512_decrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, Chunk, ?SHORT_CHUNK_SIZE + 1, ?ROUNDS, 0, 0, 0)).

%% reencrypt shares that buffer and re-encrypts chunkSize bytes out of it, so it returns the
%% uninitialised tail twice - in the clear as Decrypted, and under the caller's key as
%% Reencrypted.
test_reencrypt_rejects_chunk_size_above_chunk_size(_Fixture = {State512, _}) ->
    Chunk = crypto:strong_rand_bytes(?SHORT_CHUNK_SIZE),
    ?assertError(badarg,
        ar_rx512_nif:rx512_reencrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, ?RANDOMX_PACKING_KEY, Chunk,
            ?MAX_CHUNK_SIZE, ?ROUNDS, ?ROUNDS, 0, 0, 0)),
    ?assertError(badarg,
        ar_rx512_nif:rx512_reencrypt_chunk_nif(
            State512, ?RANDOMX_PACKING_KEY, ?RANDOMX_PACKING_KEY, Chunk,
            ?SHORT_CHUNK_SIZE + 1, ?ROUNDS, ?ROUNDS, 0, 0, 0)).

%% Any mode that is not HASHING_MODE_FAST silently takes the light branch, skipping the
%% numWorkers check above. Only info_nif notices, much later.
test_init_rejects_unknown_hashing_mode(_Fixture) ->
    Key = ?RANDOMX_PACKING_KEY,
    UnknownMode = 42,
    ?assertError(badarg, ar_rx512_nif:rx512_init_nif(Key, UnknownMode, 0, 0, 0)),
    ?assertError(badarg, ar_rx4096_nif:rx4096_init_nif(Key, UnknownMode, 0, 0, 0)),
    ?assertError(badarg, ar_rxsquared_nif:rxsquared_init_nif(Key, UnknownMode, 0, 0, 0)),
    ?assertError(badarg, ar_rx512_nif:rx512_init_nif(Key, -1, 0, 0, 0)).

%% laneCount reaches C as a bare loop bound with no validation; zero lanes yields an empty
%% entropy binary instead of a rejection.
test_fused_entropy_rejects_zero_lane_count(_Fixture = {_, StateRsp}) ->
    ?assertError(badarg,
        ar_rxsquared_nif:rsp_fused_entropy_nif(
            StateRsp, ?SUB_CHUNK_COUNT, ?SUB_CHUNK_SIZE, 0, ?REPLICA_2_9_RANDOMX_DEPTH,
            0, 0, 0, ?REPLICA_2_9_RANDOMX_PROGRAM_COUNT, ?RANDOMX_PACKING_KEY)).

%% rxDepth = 0 skips every RandomX round and returns the initial scratchpads verbatim.
test_fused_entropy_rejects_zero_depth(_Fixture = {_, StateRsp}) ->
    ?assertError(badarg,
        ar_rxsquared_nif:rsp_fused_entropy_nif(
            StateRsp, ?SUB_CHUNK_COUNT, ?SUB_CHUNK_SIZE, ?REPLICA_2_9_RANDOMX_LANE_COUNT, 0,
            0, 0, 0, ?REPLICA_2_9_RANDOMX_PROGRAM_COUNT, ?RANDOMX_PACKING_KEY)).

%% The upper end of the same missing laneCount bound: `unsigned int totalVMs = 2 * laneCount`
%% wraps to 0 at 2^31, leaving an empty vmList that rsp_fused_entropy still indexes up to
%% laneCount. The emulator never gets that far - the outEntropy allocation of
%% scratchpadSize * laneCount (4 PiB) aborts it first, which is why this runs on a peer node.
test_fused_entropy_lane_count_does_not_abort(_Fixture) ->
    ?assertError(badarg, call_on_peer(?MODULE, remote_fused_entropy, [1 bsl 31])).

remote_fused_entropy(LaneCount) ->
    {rxsquared, State} = ar_mine_randomx:init_light2(rxsquared, ?RANDOMX_PACKING_KEY, 0, 0),
    ar_rxsquared_nif:rsp_fused_entropy_nif(
        State, ?SUB_CHUNK_COUNT, ?SUB_CHUNK_SIZE, LaneCount, ?REPLICA_2_9_RANDOMX_DEPTH,
        0, 0, 0, ?REPLICA_2_9_RANDOMX_PROGRAM_COUNT, ?RANDOMX_PACKING_KEY).

%% Runs the call on a throwaway node so that an emulator abort fails this test instead of
%% taking the suite down with it. The node talks over standard_io rather than distribution,
%% so it needs no cookie and no epmd. A remote badarg is re-raised here as badarg; a node
%% that dies or hangs surfaces as peer_died.
call_on_peer(Module, Function, Args) ->
    {ok, Peer, _Node} = peer:start(
        #{connection => standard_io, args => ["-pa" | code:get_path()],
          env => [{"ERL_CRASH_DUMP_SECONDS", "0"}]}),
    try
        peer:call(Peer, Module, Function, Args, ?PEER_TIMEOUT)
    catch
        exit:Reason ->
            error({peer_died, Reason})
    after
        catch peer:stop(Peer)
    end.
