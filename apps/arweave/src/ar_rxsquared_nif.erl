-module(ar_rxsquared_nif).

-include_lib("arweave/include/ar.hrl").

-on_load(init_nif/0).

-export([rxsquared_hash_nif/5, rxsquared_info_nif/1, rxsquared_init_nif/5,
		rsp_fused_entropy_nif/10,
		rsp_feistel_encrypt_nif/2,
		rsp_feistel_decrypt_nif/2]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

rxsquared_info_nif(_State) ->
	?LOG_ERROR("rxsquared_info_nif"),
	erlang:nif_error(nif_not_loaded).

rxsquared_init_nif(_Key, _HashingMode, _JIT, _LargePages, _Threads) ->
	?LOG_ERROR("rxsquared_init_nif"),
	erlang:nif_error(nif_not_loaded).

rxsquared_hash_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	?LOG_ERROR("rxsquared_hash_nif"),
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "rxsquared_arweave"]), 0).

%%%===================================================================
%%% Randomx square packing
%%%===================================================================

rsp_fused_entropy_nif(
	_RandomxState,
	_ReplicaEntropySubChunkCount,
	_CompositePackingSubChunkSize,
	_LaneCount,
	_RxDepth,
	_JitEnabled,
	_LargePagesEnabled,
	_HardwareAESEnabled,
	_RandomxProgramCount,
	_Key
) ->
	?LOG_ERROR("rsp_fused_entropy_nif"),
	erlang:nif_error(nif_not_loaded).

rsp_feistel_encrypt_nif(_InMsg, _Key) ->
	?LOG_ERROR("rsp_feistel_encrypt_nif"),
	erlang:nif_error(nif_not_loaded).

rsp_feistel_decrypt_nif(_InMsg, _Key) ->
	?LOG_ERROR("rsp_feistel_decrypt_nif"),
	erlang:nif_error(nif_not_loaded).
