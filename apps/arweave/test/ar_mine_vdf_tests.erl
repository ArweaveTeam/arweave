-module(ar_mine_vdf_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

-define(ENCODED_PREV_STATE, <<"f_z7RLug8etm3SrmRf-xPwXEL0ZQ_xHng2A5emRDQBw">>).
-define(ENCODED_KEY, <<"UbkeSd5Det8s6uLyuNJwCDFOZMQFa2zvsdKJ0k694LM">>).
-define(MAX_THREAD_COUNT, 4).
-define(ITERATIONS_SHA, 10).
-define(ITERATIONS_RANDOMX, 8).
-define(CHECKPOINT_COUNT, 4).
-define(CHECKPOINT_SKIP_COUNT, 9).
-define(ENCODED_SHA_CHECKPOINT, <<"hewn3qrpFlGUPVexAqyqrb72v7dvhAMd26Cwih7R8w9WPNxZmSZSSqGcibtalnKnwHruGcFvEqzSWq2ySAMsRxzC57qaktI6gV6dAcPQ41ZLzpw9i3AJUPCFsShzNbV8EVx29JpHdMZ0VzlViPUrgbfS_EVSWAqiZKhJcJmUcbI">>).
-define(ENCODED_SHA_CHECKPOINT_FULL, <<"hewn3qrpFlGUPVexAqyqrb72v7dvhAMd26Cwih7R8w9WPNxZmSZSSqGcibtalnKnwHruGcFvEqzSWq2ySAMsRxzC57qaktI6gV6dAcPQ41ZLzpw9i3AJUPCFsShzNbV8EVx29JpHdMZ0VzlViPUrgbfS_EVSWAqiZKhJcJmUcbJlfDXdCy38G1r-6tB_OLCVTYcIg7if-g6ejTQERJ2AXg">>).
-define(ENCODED_SHA_CHECKPOINT_SKIP, <<"_Wl7xvFvX_XOHfnaHxtv17b_UCYIx1fOU2SeuM7VV-_5xuzpESPJCM2wh0ry9sjpDqLPjKmI3hkdWm3CZalD5lSdAOJp62vJXdKbTOstTDi__oL1OIPFOnDxawC_TIxLO-YebYbMN6hGk7bz7le65Bciat3ahUEIM_GmriHJVh4">>).
-define(ENCODED_SHA_CHECKPOINT_SKIP_FULL, <<"hewn3qrpFlGUPVexAqyqrb72v7dvhAMd26Cwih7R8w9WPNxZmSZSSqGcibtalnKnwHruGcFvEqzSWq2ySAMsRxzC57qaktI6gV6dAcPQ41ZLzpw9i3AJUPCFsShzNbV8EVx29JpHdMZ0VzlViPUrgbfS_EVSWAqiZKhJcJmUcbJlfDXdCy38G1r-6tB_OLCVTYcIg7if-g6ejTQERJ2AXhCdh0sQVoirKm9rwAmASBiukGSRwvyEyQ80ccxTh6vrTd5rQaSJdkZ0ExCsKpzz8SYkCp6t38w9Rqk_lJXHsANI3gp0T2YTmecMiTevV2RWTyNAaORuYEiux_Jfo49k2DvRCqZfrwsqUhLjRZDSEg1QxNsZHuh8nk5mgYOMf8ed_Wl7xvFvX_XOHfnaHxtv17b_UCYIx1fOU2SeuM7VV--Q3x1t_3r3gSM0O00RTrFOKLZemScJ5T5A2HLBm34_ZbEUPWmzyZwawd7EXlZ7GON7qF4YTC0gknZgi9g98MF_sCmN8ElbZrrF3YDG2SdfPgB49LUyivWgPk2iOsWpqupCP0P_TeeTNOR9hDOPlBl5kO2uR6Eelb9zLTF9Kbuyx1cvHUwOvSwN5MuVcYJhogsMyeJD7r8U7ZkLzd9qn3RLZDNqRXY6IE9BdK1KvgexZz-XxGNttWUCYDR4Hec5-6KkDbJE2YJEpGlWVyUmDQWjWIbaNt-1yPKpRNVAKSm1olVY-vb2kMnYeu5K_905VEkJr8kQ1yKwvrzLiJAL0yRwtnHRwn7c_UuTCNHDFoMF5kYhdNMDMZBwEe5EcWUnsdL5xuzpESPJCM2wh0ry9sjpDqLPjKmI3hkdWm3CZalD5oeZAkJnAcA5TCpoGFW4VDZA0WYjl-e8YbtW_XY2KEJGcWysKNGjmhckM9pgv7bHZWk2uQ5WabYWhytLR6daWGBm-on7VqNmMP4rDWkzA9AANL6GiVMl_ZYzP9fHghUhEylkGgw-8EW0n_XzeaOjD-gGAt2f3VNyR2PQ1hEtNnPeFfCoNRGe7b0imkfTX9pEMghXeGGsGRjw_QDdRpj7F6wCbmR_N1GEnKJopBRxyGD_8CpwEE_j5eZQ3_vgv1KbjQ1b7XMOHxk_nVhGnhPvm2KkNkpjGU34YCDDDY19qYDR2gGVLDNuThRBy9yc_1EUFrgDAY_wGnAz96pNePTBq4UpVVgDi5pWu5v3tApwLwFnu83UJHqkI2Q8szA_soTTOVSdAOJp62vJXdKbTOstTDi__oL1OIPFOnDxawC_TIxLgie4E6K6mCnDjEDv6M0uJxj8B7Q4oniIOHTlY0D8PxELeIs3wbuPpIluiSfZqiQJyFyCLQ0TyGYCSaJ0ypxUHluXMoXHxiBLzXyympXp8zKNYoqLSO9xOKhxsiBAeOw6XEMyyfpzjTZoVeWnopklpaMdHY-Yah5DHUpkqCbBn0B-jMgMuaR11QPt0oV5i2E3g5gahhPrpjSuMr35kf20yrmLGc6yJRc0teU7p-f9lQ1ORm6bVpPvgDVVZIgU_CUyaHxTpNElok2zPa56Lfn8-eToK6BDMxh0VsnPD45-dZ6phPKYX9g5IRxybw3_CbXyhhL4Pi4rogACQU6zdfh2hQtPZoLL7E_bxvX3nj3T08alwy9nOr5os8gw0-KlE8yeO-YebYbMN6hGk7bz7le65Bciat3ahUEIM_GmriHJVh4sZCUrrsHuPKh1fZRofy6JnVhQy29ho8yiubwW9uYpo31yz1Y7XPGGhZWJfJx2t2OPoGreKS46Ddo_rCoJB4my_hDk6Pb18gbus7pE-rwpTWsF0ceQFOwpkunGBi5mJ_gADc3kUG9LrBVtjYPo2jAwcf8NCTKLK7loqmql-IE0txTleG6IiMZkqw_1IOqXcsRFjXmJQGK8Xq1h8Obf3WGwgSDGCfXsNqm3zBddNx50WodlEhtjJzrEEjflP1hZE6DHccf2i5x2VRrhFhF-nfhz46Yr7guxjL9KjzMqmKGfAe7_dO881mbJ_9CEDYWb0_WgSiZE6Bd-kM2-zZiJFuGYNAm6d21pt5kfci7vue2fAnHAGGT2EgM-unxdcOfSrl9Kht5_abH8CDEni5vOLYUsNiGi4qqiPJPNRcBIJetJnQ">>).
-define(ENCODED_SHA_RES, <<"ZXw13Qst_Bta_urQfziwlU2HCIO4n_oOno00BESdgF4">>).
-define(ENCODED_SHA_RES_SKIP, <<"Sobef2mx_AgxJ4ubzi2FLDYhouKqojyTzUXASCXrSZ0">>).
-define(ENCODED_RANDOMX_CHECKPOINT, <<"Ueypayc2rt0-xWhOzN1aUQ4qmHKlraq0AdqDdOH4AXhR_cj0UBuMYjD9p8kKCUKrHlMpR_4m68-MnmUNQm_2u1bITSOT7SO7698f_P_Eeq_ulb8b3Vf4R8F2UO0jM2m0CAcxjQT1vTxjouvrwff35d4obYAjxa7M6a0OYXk8rkw">>).
-define(ENCODED_RANDOMX_CHECKPOINT_SKIP, <<"KMm_wpkAjOAlasyDA2BMJLlXYuwj_Oz0pbaCF33Q7ck8t71IZHhYjujMwAHIKD6A-w7BvAbdBbVj80ir8JoItq3wkhWJ68n6xKFVztz_CBifRihjsRKxKVbfNIee5r_MTUobAIVRLN-6Hzl8kHHeHChbi82qkSwxnYVgowP4oWw">>).
-define(ENCODED_RANDOMX_RES, <<"py-9I07gIORjNf031MADn81bF4hNbvd-Ou3723J2XsA">>).
-define(ENCODED_RANDOMX_RES_SKIP, <<"oujqZRG90Rpf_-ftWLIhc7vfJkqaytIOQmHRoAilwL0">>).
-define(ENCODED_SHA_RANDOMX_CHECKPOINT, <<"hewn3qrpFlGUPVexAqyqrb72v7dvhAMd26Cwih7R8w9WPNxZmSZSSqGcibtalnKnwHruGcFvEqzSWq2ySAMsRxzC57qaktI6gV6dAcPQ41ZLzpw9i3AJUPCFsShzNbV8EVx29JpHdMZ0VzlViPUrgbfS_EVSWAqiZKhJcJmUcbJlfDXdCy38G1r-6tB_OLCVTYcIg7if-g6ejTQERJ2AXlHsqWsnNq7dPsVoTszdWlEOKphypa2qtAHag3Th-AF4Uf3I9FAbjGIw_afJCglCqx5TKUf-JuvPjJ5lDUJv9rtWyE0jk-0ju-vfH_z_xHqv7pW_G91X-EfBdlDtIzNptAgHMY0E9b08Y6Lr68H39-XeKG2AI8WuzOmtDmF5PK5Mpy-9I07gIORjNf031MADn81bF4hNbvd-Ou3723J2XsA">>).
-define(ENCODED_SHA_RANDOMX_CHECKPOINT_SKIP, <<"_Wl7xvFvX_XOHfnaHxtv17b_UCYIx1fOU2SeuM7VV-_5xuzpESPJCM2wh0ry9sjpDqLPjKmI3hkdWm3CZalD5lSdAOJp62vJXdKbTOstTDi__oL1OIPFOnDxawC_TIxLO-YebYbMN6hGk7bz7le65Bciat3ahUEIM_GmriHJVh5Kht5_abH8CDEni5vOLYUsNiGi4qqiPJPNRcBIJetJnSjJv8KZAIzgJWrMgwNgTCS5V2LsI_zs9KW2ghd90O3JPLe9SGR4WI7ozMAByCg-gPsOwbwG3QW1Y_NIq_CaCLat8JIVievJ-sShVc7c_wgYn0YoY7ESsSlW3zSHnua_zE1KGwCFUSzfuh85fJBx3hwoW4vNqpEsMZ2FYKMD-KFsoujqZRG90Rpf_-ftWLIhc7vfJkqaytIOQmHRoAilwL0">>).
-define(ENCODED_SHA_RANDOMX_RES, <<"ExLBxISK_1eY_QEPZOVmiinJmFeaOj1fXFmd08ayhiQ">>).
-define(ENCODED_SHA_RANDOMX_RES_SKIP, <<"JJG59KN2wry0eli7OcIQdUoJKkIFXGXdy3hHe3U10yE">>).

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

break_byte(Buf, Pos)->
	Head = binary:part(Buf, 0, Pos),
	Tail = binary:part(Buf, Pos+1, size(Buf)-Pos-1),
	ChangedByte = binary:at(Buf,Pos) bxor 1,
	<<Head/binary, ChangedByte, Tail/binary>>.

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

	{ok, Real1, _OutCheckpointSha} = ar_mine_randomx:vdf_sha2_nif(Salt1, PrevState, 0, 0, ?ITERATIONS_SHA),
	ExpectedHash = soft_implementation_vdf_sha(Salt1, PrevState, ?ITERATIONS_SHA, 0),
	?assertEqual(ExpectedHash, Real1),

	{ok, RealSha2, OutCheckpointSha2} = ar_mine_randomx:vdf_sha2_nif(Salt2, Real1, ?CHECKPOINT_COUNT-1, 0, ?ITERATIONS_SHA),
	{ok, RealSha3, OutCheckpointSha3} = ar_mine_randomx:vdf_sha2_nif(Salt1, PrevState, ?CHECKPOINT_COUNT, 0, ?ITERATIONS_SHA),
	ExpectedSha3 = soft_implementation_vdf_sha(Salt1, PrevState, ?ITERATIONS_SHA, ?CHECKPOINT_COUNT),
	?assertEqual(ExpectedSha3, RealSha2),
	?assertEqual(ExpectedSha3, RealSha3),
	ExpedctedOutCheckpoint3 = << Real1/binary, OutCheckpointSha2/binary >>,
	?assertEqual(ExpedctedOutCheckpoint3, OutCheckpointSha3),
	ExpectedOutCheckpointSha3Full = << OutCheckpointSha3/binary, RealSha3/binary >>,
	?assertEqual(ExpectedOutCheckpointSha3Full, OutCheckpointSha3Full),
	{ok, OutCheckpointSha3Full} = ar_mine_randomx:vdf_parallel_sha_verify_nif(Salt1, PrevState, ?CHECKPOINT_COUNT, 0, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3, ?MAX_THREAD_COUNT),
	ok = test_vdf_sha_verify_break1(Salt1, PrevState, ?CHECKPOINT_COUNT, 0, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3),
	ok = test_vdf_sha_verify_break2(Salt1, PrevState, ?CHECKPOINT_COUNT, 0, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3),

	ok.

test_vdf_sha_verify_break1(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash) ->
	test_vdf_sha_verify_break1(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, size(OutCheckpoint)-1).

test_vdf_sha_verify_break1(_Salt, _PrevState, _CheckpointCount, _SkipCheckpointCount, _Iterations, _OutCheckpoint, _Hash, 0) ->
	ok;
test_vdf_sha_verify_break1(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, BreakPos) ->
	OutCheckpointBroken = break_byte(OutCheckpoint, BreakPos),
	{error, _} = ar_mine_randomx:vdf_parallel_sha_verify_nif(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpointBroken, Hash, ?MAX_THREAD_COUNT),
	test_vdf_sha_verify_break1(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, BreakPos-1).

test_vdf_sha_verify_break2(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash) ->
	test_vdf_sha_verify_break2(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, size(Hash)-1).

test_vdf_sha_verify_break2(_Salt, _PrevState, _CheckpointCount, _SkipCheckpointCount, _Iterations, _OutCheckpoint, _Hash, 0) ->
	ok;
test_vdf_sha_verify_break2(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, Hash, BreakPos) ->
	HashBroken = break_byte(Hash, BreakPos),
	{error, _} = ar_mine_randomx:vdf_parallel_sha_verify_nif(Salt, PrevState, CheckpointCount, SkipCheckpointCount, Iterations, OutCheckpoint, HashBroken, ?MAX_THREAD_COUNT),
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
	OutCheckpointSha3Full = ar_util:decode(?ENCODED_SHA_CHECKPOINT_SKIP_FULL),
	RealSha3 = ar_util:decode(?ENCODED_SHA_RES_SKIP),
	Salt1 = << (1):256 >>,
	SaltJump = << (1+?CHECKPOINT_SKIP_COUNT+1):256 >>,

	{ok, Real1, _OutCheckpointSha} = ar_mine_randomx:vdf_sha2_nif(Salt1, PrevState, 0, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA),
	ExpectedHash = soft_implementation_vdf_sha(Salt1, PrevState, ?ITERATIONS_SHA, ?CHECKPOINT_SKIP_COUNT),
	?assertEqual(ExpectedHash, Real1),

	{ok, RealSha2, OutCheckpointSha2} = ar_mine_randomx:vdf_sha2_nif(SaltJump, Real1, ?CHECKPOINT_COUNT-1, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA),
	{ok, RealSha3, OutCheckpointSha3} = ar_mine_randomx:vdf_sha2_nif(Salt1, PrevState, ?CHECKPOINT_COUNT, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA),
	ExpectedSha3 = soft_implementation_vdf_sha(Salt1, PrevState, ?ITERATIONS_SHA, (1+?CHECKPOINT_COUNT)*(1+?CHECKPOINT_SKIP_COUNT)-1),
	?assertEqual(ExpectedSha3, RealSha2),
	?assertEqual(ExpectedSha3, RealSha3),
	ExpedctedOutCheckpoint3 = << Real1/binary, OutCheckpointSha2/binary >>,
	?assertEqual(ExpedctedOutCheckpoint3, OutCheckpointSha3),
	{ok, OutCheckpointSha3Full} = ar_mine_randomx:vdf_parallel_sha_verify_nif(Salt1, PrevState, ?CHECKPOINT_COUNT, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3, ?MAX_THREAD_COUNT),
	ok = test_vdf_sha_verify_break1(Salt1, PrevState, ?CHECKPOINT_COUNT, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3),
	ok = test_vdf_sha_verify_break2(Salt1, PrevState, ?CHECKPOINT_COUNT, ?CHECKPOINT_SKIP_COUNT, ?ITERATIONS_SHA, OutCheckpointSha3, RealSha3),

	ok.
