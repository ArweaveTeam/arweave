%%% @doc Boundary/validation tests for the VDF NIF argument checks.
%%%
%%% The first group pins down the argument checks and the size arithmetic
%%% changed in the "more strict boundaries" commit. The second group covers
%%% checks that are still missing; those tests assert the behaviour a fix should
%%% produce and so FAIL today. The mainstream (positive) VDF vectors are covered
%%% by ar_mine_vdf_tests.
-module(ar_vdf_nif_boundary_tests).
-test_category([vdf, fast]).

-include_lib("eunit/include/eunit.hrl").

%% VDF_SHA_HASH_SIZE and SALT_SIZE in apps/arweave/c_src/vdf/vdf.h.
-define(VDF_SHA_HASH_SIZE, 32).
-define(SALT_SIZE, 32).

-define(PEER_TIMEOUT, 30000).

salt() ->
    << 1:(?SALT_SIZE * 8) >>.

seed() ->
    << 2:(?VDF_SHA_HASH_SIZE * 8) >>.

%% -------------------------------------------------------------------------------------------
%% The three counts are read with enif_get_uint.
%% -------------------------------------------------------------------------------------------

%% checkpointCount, skipCheckpointCount and hashingIterations all reach C as loop bounds and
%% as factors of an output binary size. A negative value used to be accepted by enif_get_int.
vdf_rejects_negative_counts_test_() ->
    {timeout, 60, fun test_vdf_rejects_negative_counts/0}.

test_vdf_rejects_negative_counts() ->
    Salt = salt(),
    Seed = seed(),
    Fns = [fun ar_vdf_nif:vdf_sha2_nif/5,
           fun ar_vdf_nif:vdf_sha2_fused_nif/5,
           fun ar_vdf_nif:vdf_sha2_hiopt_nif/5],
    %% {CheckpointCount, SkipCheckpointCount, Iterations} with one argument negative.
    Cases = [{-1, 0, 1}, {0, -1, 1}, {0, 0, -1},
             {1 bsl 40, 0, 1}, {0, 1 bsl 40, 1}, {0, 0, 1 bsl 40}],
    lists:foreach(
        fun(Fn) ->
            lists:foreach(
                fun({CheckpointCount, SkipCount, Iterations}) ->
                    ?assertError(badarg, Fn(Salt, Seed, CheckpointCount, SkipCount, Iterations))
                end,
                Cases)
        end,
        Fns),
    lists:foreach(
        fun({CheckpointCount, SkipCount, Iterations}) ->
            ?assertError(badarg,
                ar_vdf_nif:vdf_parallel_sha_verify_with_reset_nif(
                    Salt, Seed, CheckpointCount, SkipCount, Iterations,
                    <<>>, Seed, Salt, Seed, 1))
        end,
        Cases).

%% -------------------------------------------------------------------------------------------
%% The checkpoint buffer size is computed in size_t, not int.
%% -------------------------------------------------------------------------------------------

%% vdf_parallel_sha_verify_with_reset_nif checks
%%   InCheckpoint.size != (size_t)checkpointCount * VDF_SHA_HASH_SIZE
%% before it allocates anything. Without the size_t cast the product was computed in int, so
%% checkpointCount = 2^27 overflowed to exactly 0 and an empty InCheckpoint passed the check -
%% after which the NIF went on to size the output buffer from the same overflowing arithmetic.
%% With the cast, the mismatch is caught and the call is rejected before any allocation.
vdf_verify_checkpoint_size_does_not_overflow_test_() ->
    {timeout, 60, fun test_vdf_verify_checkpoint_size_does_not_overflow/0}.

test_vdf_verify_checkpoint_size_does_not_overflow() ->
    Salt = salt(),
    Seed = seed(),
    %% 2^27 * 32 == 2^32, i.e. 0 when truncated to a 32 bit int.
    CheckpointCount = 1 bsl 27,
    ?assertError(badarg,
        ar_vdf_nif:vdf_parallel_sha_verify_with_reset_nif(
            Salt, Seed, CheckpointCount, 0, 1, <<>>, Seed, Salt, Seed, 1)),
    %% Same shape, one checkpoint short of the overflow point, still a size mismatch.
    ?assertError(badarg,
        ar_vdf_nif:vdf_parallel_sha_verify_with_reset_nif(
            Salt, Seed, (1 bsl 27) - 1, 0, 1, <<>>, Seed, Salt, Seed, 1)).

%% -------------------------------------------------------------------------------------------
%% Small iteration counts must not wrap.
%% -------------------------------------------------------------------------------------------

%% hashingIterations is unsigned in the C layer. The fused ARM implementation folds 1-2 SHA
%% rounds into its prologue/epilogue and asks the inner loop for `hashingIterations - 1` or
%% `- 2`; unsigned, that wraps to ~4e9 for a small iteration count unless the subtraction
%% saturates. On a wrap this test fails on the eunit timeout rather than on an assertion.
%%
%% Runs on every architecture: only the ARM build takes the saturating path, so this test is
%% the reason the module carries the `vdf` category (the macOS/ARM VDF workflow).
vdf_small_iteration_counts_test_() ->
    {timeout, 60, fun test_vdf_small_iteration_counts/0}.

test_vdf_small_iteration_counts() ->
    Salt = salt(),
    Seed = seed(),
    Fns = [fun ar_vdf_nif:vdf_sha2_nif/5,
           fun ar_vdf_nif:vdf_sha2_fused_nif/5,
           fun ar_vdf_nif:vdf_sha2_hiopt_nif/5],
    lists:foreach(
        fun(Fn) ->
            lists:foreach(
                fun(Iterations) ->
                    lists:foreach(
                        fun({CheckpointCount, SkipCount}) ->
                            {ok, Out, OutCheckpoint} =
                                Fn(Salt, Seed, CheckpointCount, SkipCount, Iterations),
                            ?assertEqual(?VDF_SHA_HASH_SIZE, byte_size(Out)),
                            ?assertEqual(CheckpointCount * ?VDF_SHA_HASH_SIZE,
                                byte_size(OutCheckpoint))
                        end,
                        %% Both branches of the C implementations: no skips, and skips.
                        [{0, 0}, {2, 0}, {0, 3}, {2, 3}])
                end,
                [0, 1, 2])
        end,
        Fns).

%% ===========================================================================================
%% Checks that have not landed yet. Each test asserts the behaviour a fix should produce, so
%% they fail until the check lands.
%% ===========================================================================================

%% The openssl implementation always runs at least 2 SHA rounds per checkpoint (it hashes an
%% unconditional first and last block around its inner loop); the x86 fused implementation
%% runs exactly hashingIterations, returning the seed unhashed at 0. So below 2 iterations the
%% `openssl` and `fused` algorithms disagree on the same machine, in consensus code -
%% iterations_minus/2 fixed that for ARM only. test_vdf_small_iteration_counts above covers
%% these inputs but asserts only the output sizes.
%%
%% Either fix satisfies this test: make the implementations agree, or reject
%% hashingIterations < 2 in all of them (production difficulty is never that low).
vdf_implementations_agree_on_small_iteration_counts_test_() ->
    {timeout, 60, fun test_vdf_implementations_agree_on_small_iteration_counts/0}.

test_vdf_implementations_agree_on_small_iteration_counts() ->
    Salt = salt(),
    Seed = seed(),
    lists:foreach(
        fun(Iterations) ->
            lists:foreach(
                fun({CheckpointCount, SkipCount}) ->
                    Case = {iterations, Iterations, checkpoints, CheckpointCount,
                            skips, SkipCount},
                    Reference = run(fun ar_vdf_nif:vdf_sha2_nif/5, Salt, Seed,
                            CheckpointCount, SkipCount, Iterations),
                    Fused = run(fun ar_vdf_nif:vdf_sha2_fused_nif/5, Salt, Seed,
                            CheckpointCount, SkipCount, Iterations),
                    Hiopt = run(fun ar_vdf_nif:vdf_sha2_hiopt_nif/5, Salt, Seed,
                            CheckpointCount, SkipCount, Iterations),
                    ?assertEqual({Case, Reference}, {Case, Fused}),
                    ?assertEqual({Case, Reference}, {Case, Hiopt})
                end,
                %% Both branches of the C implementations: no skips, and skips.
                [{0, 0}, {2, 0}, {0, 3}, {2, 3}])
        end,
        [0, 1, 2]).

%% Returns the NIF result, or the atom badarg if the arguments were rejected - so that
%% "all implementations reject" compares equal just like "all implementations agree".
run(Fn, Salt, Seed, CheckpointCount, SkipCount, Iterations) ->
    try
        Fn(Salt, Seed, CheckpointCount, SkipCount, Iterations)
    catch
        error:badarg ->
            badarg
    end.

%% The output buffer is sized VDF_SHA_HASH_SIZE * (1 + checkpointCount) *
%% (1 + skipCheckpointCount). checkpointCount is pinned to the caller's InCheckpoint binary by
%% the size check just above the allocation, but skipCheckpointCount is pinned to nothing, so
%% an empty InCheckpoint still buys 32 MiB and ~2M SHA rounds here. Any sane upper bound
%% satisfies this test; ar_vdf:verify/8 passes a small protocol constant.
vdf_verify_bounds_skip_checkpoint_count_test_() ->
    {timeout, 60, fun test_vdf_verify_bounds_skip_checkpoint_count/0}.

test_vdf_verify_bounds_skip_checkpoint_count() ->
    Salt = salt(),
    Seed = seed(),
    SkipCount = (1 bsl 20) - 1,
    ?assertError(badarg,
        ar_vdf_nif:vdf_parallel_sha_verify_with_reset_nif(
            Salt, Seed, 0, SkipCount, 1, <<>>, Seed, Salt, Seed, 1)).

%% The same missing bound at the top of the range. enif_make_new_binary aborts the emulator
%% rather than returning NULL, so asking for 32 * (1 + 1) * 2^32 == 256 GiB of output takes
%% the node down; hence the peer node. The request has to exceed what any host could satisfy,
%% otherwise it is granted and the NIF goes on to hash into it. InCheckpoint has to match
%% checkpointCount * VDF_SHA_HASH_SIZE exactly, or the size check rejects it before the
%% allocation and the test passes for the wrong reason.
vdf_verify_skip_checkpoint_count_does_not_abort_test_() ->
    {timeout, 120, fun test_vdf_verify_skip_checkpoint_count_does_not_abort/0}.

test_vdf_verify_skip_checkpoint_count_does_not_abort() ->
    Salt = salt(),
    Seed = seed(),
    CheckpointCount = 1,
    InCheckpoint = << 3:(CheckpointCount * ?VDF_SHA_HASH_SIZE * 8) >>,
    Args = [Salt, Seed, CheckpointCount, (1 bsl 32) - 1, 1, InCheckpoint, Seed, Salt, Seed, 1],
    ?assertError(badarg,
        call_on_peer(ar_vdf_nif, vdf_parallel_sha_verify_with_reset_nif, Args)).

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
