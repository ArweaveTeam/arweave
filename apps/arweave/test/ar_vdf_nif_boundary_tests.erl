%%% @doc Boundary/validation tests for the VDF NIF argument checks.
%%%
%%% These pin down the argument checks and the size arithmetic changed in the
%%% "more strict boundaries" commit. The mainstream (positive) VDF vectors are
%%% covered by ar_mine_vdf_tests.
-module(ar_vdf_nif_boundary_tests).
-test_category([vdf, fast]).

-include_lib("eunit/include/eunit.hrl").

%% VDF_SHA_HASH_SIZE and SALT_SIZE in apps/arweave/c_src/vdf/vdf.h.
-define(VDF_SHA_HASH_SIZE, 32).
-define(SALT_SIZE, 32).

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
