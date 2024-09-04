-module(ar_mine_vdf_tests).

-include_lib("eunit/include/eunit.hrl").

-define(ENCODED_PREV_STATE, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(ITERATIONS_SHA, 10).
-define(CHECKPOINT_COUNT, 4).
-define(CHECKPOINT_SKIP_COUNT, 9).
-define(ENCODED_SHA_CHECKPOINT, <<"hewn3qrpFlGUPVexAqyqrb72v7dvhAMd26Cwih7R8w9WPNxZmSZSSqGcibtalnKnwHruGcFvEqzSWq2ySAMsRxzC57qaktI6gV6dAcPQ41ZLzpw9i3AJUPCFsShzNbV8EVx29JpHdMZ0VzlViPUrgbfS_EVSWAqiZKhJcJmUcbI">>).
-define(ENCODED_SHA_CHECKPOINT_FULL, <<"hewn3qrpFlGUPVexAqyqrb72v7dvhAMd26Cwih7R8w9WPNxZmSZSSqGcibtalnKnwHruGcFvEqzSWq2ySAMsRxzC57qaktI6gV6dAcPQ41ZLzpw9i3AJUPCFsShzNbV8EVx29JpHdMZ0VzlViPUrgbfS_EVSWAqiZKhJcJmUcbJlfDXdCy38G1r-6tB_OLCVTYcIg7if-g6ejTQERJ2AXg">>).
-define(ENCODED_SHA_CHECKPOINT_SKIP, <<"_Wl7xvFvX_XOHfnaHxtv17b_UCYIx1fOU2SeuM7VV-_5xuzpESPJCM2wh0ry9sjpDqLPjKmI3hkdWm3CZalD5lSdAOJp62vJXdKbTOstTDi__oL1OIPFOnDxawC_TIxLO-YebYbMN6hGk7bz7le65Bciat3ahUEIM_GmriHJVh4">>).
-define(ENCODED_SHA_RES, <<"ZXw13Qst_Bta_urQfziwlU2HCIO4n_oOno00BESdgF4">>).
-define(ENCODED_SHA_RES_SKIP, <<"Sobef2mx_AgxJ4ubzi2FLDYhouKqojyTzUXASCXrSZ0">>).

%%%===================================================================
%%% utils
%%%===================================================================

soft_implementation_vdf_sha(_Salt, PrevState, 0, _CheckpointCount) ->
	PrevState;

soft_implementation_vdf_sha(Salt, PrevState, Iterations, 0) ->
	NextState = crypto:hash(sha256, <<Salt/binary, PrevState/binary>>),
	soft_implementation_vdf_sha(Salt, NextState, Iterations-1, 0);

soft_implementation_vdf_sha(Salt, PrevState, Iterations, CheckpointCount) ->
	NextState = soft_implementation_vdf_sha(Salt, PrevState, Iterations, 0),
	<< SaltValue:256 >> = Salt,
	NextSalt = << (SaltValue+1):256 >>,
	soft_implementation_vdf_sha(NextSalt, NextState, Iterations, CheckpointCount-1).

%%%===================================================================
%%% 
%%% no skip iterations (needed for last 1 sec has more checkpoints)
%%% 
%%%===================================================================

%%%===================================================================
%%% SHA.
%%%===================================================================

vdf_sha_test_() ->
	{timeout, 500, fun test_vdf_sha/0}.

test_vdf_sha() ->
	PrevState = ar_util:decode(?ENCODED_PREV_STATE),
	OutCheckpointSha3 = ar_util:decode(?ENCODED_SHA_CHECKPOINT),
	OutCheckpointSha3Full = ar_util:decode(?ENCODED_SHA_CHECKPOINT_FULL),
	RealSha3 = ar_util:decode(?ENCODED_SHA_RES),
	Salt1 = << (1):256 >>,
	Salt2 = << (2):256 >>,

	{ok, Real1, _OutCheckpointSha} = ar_vdf_nif:vdf_sha2_nif(Salt1, PrevState, 0, 0, ?ITERATIONS_SHA),
	ExpectedHash = soft_implementation_vdf_sha(Salt1, PrevState, ?ITERATIONS_SHA, 0),
	?assertEqual(ExpectedHash, Real1),

	{ok, RealSha2, OutCheckpointSha2} = ar_vdf_nif:vdf_sha2_nif(Salt2, Real1, ?CHECKPOINT_COUNT-1, 0, ?ITERATIONS_SHA),
	{ok, RealSha3, OutCheckpointSha3} = ar_vdf_nif:vdf_sha2_nif(Salt1, PrevState, ?CHECKPOINT_COUNT, 0, ?ITERATIONS_SHA),
	ExpectedSha3 = soft_implementation_vdf_sha(Salt1, PrevState, ?ITERATIONS_SHA, ?CHECKPOINT_COUNT),
	?assertEqual(ExpectedSha3, RealSha2),
	?assertEqual(ExpectedSha3, RealSha3),
	ExpedctedOutCheckpoint3 = << Real1/binary, OutCheckpointSha2/binary >>,
	?assertEqual(ExpedctedOutCheckpoint3, OutCheckpointSha3),
	ExpectedOutCheckpointSha3Full = << OutCheckpointSha3/binary, RealSha3/binary >>,
	?assertEqual(ExpectedOutCheckpointSha3Full, OutCheckpointSha3Full),
	ok = test_vdf_sha_verify_break1(Salt1, PrevState, ?CHECKPOINT_COUNT, 0, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3),
	ok = test_vdf_sha_verify_break2(Salt1, PrevState, ?CHECKPOINT_COUNT, 0, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3),

	ok.

test_vdf_sha_verify_break1(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash) ->
	test_vdf_sha_verify_break1(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, size(OutCheckpoint)-1).

test_vdf_sha_verify_break1(_Salt, _PrevState, _CheckpointCount, _SkipCheckpointCount, _Iterations, _OutCheckpoint, _Hash, 0) ->
	ok;
test_vdf_sha_verify_break1(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, BreakPos) ->
	test_vdf_sha_verify_break1(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, BreakPos-1).

test_vdf_sha_verify_break2(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash) ->
	test_vdf_sha_verify_break2(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, size(Hash)-1).

test_vdf_sha_verify_break2(_Salt, _PrevState, _CheckpointCount, _SkipCheckpointCount, _Iterations, _OutCheckpoint, _Hash, 0) ->
	ok;
test_vdf_sha_verify_break2(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, BreakPos) ->
	test_vdf_sha_verify_break2(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, BreakPos-1).

%%%===================================================================
%%% 
%%% with skip iterations
%%% 
%%%===================================================================

%%%===================================================================
%%% SHA.
%%%===================================================================

vdf_sha_skip_iterations_test_() ->
	{timeout, 500, fun test_vdf_sha_skip_iterations/0}.

test_vdf_sha_skip_iterations() ->
	PrevState = ar_util:decode(?ENCODED_PREV_STATE),
	OutCheckpointSha3 = ar_util:decode(?ENCODED_SHA_CHECKPOINT_SKIP),
	RealSha3 = ar_util:decode(?ENCODED_SHA_RES_SKIP),
	Salt1 = << (1):256 >>,
	SaltJump = << (1+?CHECKPOINT_SKIP_COUNT+1):256 >>,

	{ok, Real1, _OutCheckpointSha} = ar_vdf_nif:vdf_sha2_nif(Salt1, PrevState, 0, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA),
	ExpectedHash = soft_implementation_vdf_sha(Salt1, PrevState, ?ITERATIONS_SHA, ?CHECKPOINT_SKIP_COUNT),
	?assertEqual(ExpectedHash, Real1),

	{ok, RealSha2, OutCheckpointSha2} = ar_vdf_nif:vdf_sha2_nif(SaltJump, Real1, ?CHECKPOINT_COUNT-1, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA),
	{ok, RealSha3, OutCheckpointSha3} = ar_vdf_nif:vdf_sha2_nif(Salt1, PrevState, ?CHECKPOINT_COUNT, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA),
	ExpectedSha3 = soft_implementation_vdf_sha(Salt1, PrevState, ?ITERATIONS_SHA, (1+?CHECKPOINT_COUNT)*(1+?CHECKPOINT_SKIP_COUNT)-1),
	?assertEqual(ExpectedSha3, RealSha2),
	?assertEqual(ExpectedSha3, RealSha3),
	ExpedctedOutCheckpoint3 = << Real1/binary, OutCheckpointSha2/binary >>,
	?assertEqual(ExpedctedOutCheckpoint3, OutCheckpointSha3),
	ok = test_vdf_sha_verify_break1(Salt1, PrevState, ?CHECKPOINT_COUNT, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3),
	ok = test_vdf_sha_verify_break2(Salt1, PrevState, ?CHECKPOINT_COUNT, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3),

	ok.
