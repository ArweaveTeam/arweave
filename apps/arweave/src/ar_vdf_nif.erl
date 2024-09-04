-module(ar_vdf_nif).

-on_load(init_nif/0).

-export([vdf_sha2_nif/5, vdf_parallel_sha_verify_with_reset_nif/10]).

%%%===================================================================
%%% Public interface.
%%%===================================================================
vdf_sha2_nif(_Salt, _PrevState, _CheckpointCount, _skipCheckpointCount, _Iterations) ->
	erlang:nif_error(nif_not_loaded).
vdf_parallel_sha_verify_with_reset_nif(_Salt, _PrevState, _CheckpointCount,
		_skipCheckpointCount, _Iterations, _InCheckpoint, _InRes, _ResetSalt, _ResetSeed,
		_MaxThreadCount) ->
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "vdf_arweave"]), 0).
