-module(ar_rx4096_nif).

-include_lib("arweave/include/ar.hrl").

-on_load(init_nif/0).

-export([rx4096_hash_nif/5, rx4096_info_nif/1, rx4096_init_nif/5,
		rx4096_encrypt_composite_chunk_nif/9,
		rx4096_decrypt_composite_chunk_nif/10,
		rx4096_decrypt_composite_sub_chunk_nif/10,
		rx4096_reencrypt_composite_chunk_nif/13
]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

rx4096_info_nif(_State) ->
	?LOG_ERROR("rx4096_info_nif"),
	erlang:nif_error(nif_not_loaded).

rx4096_init_nif(_Key, _HashingMode, _JIT, _LargePages, _Threads) ->
	?LOG_ERROR("rx4096_init_nif"),
	erlang:nif_error(nif_not_loaded).

rx4096_hash_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	?LOG_ERROR("rx4096_hash_nif"),
	erlang:nif_error(nif_not_loaded).

rx4096_encrypt_composite_chunk_nif(_State, _Key, _Chunk, _JIT, _LargePages, _HardwareAES,
		_RoundCount, _IterationCount, _SubChunkCount) ->
	?LOG_ERROR("rx4096_encrypt_composite_chunk_nif"),
	erlang:nif_error(nif_not_loaded).

rx4096_decrypt_composite_chunk_nif(_State, _Data, _Chunk, _OutSize,
		_JIT, _LargePages, _HardwareAES, _RoundCount, _IterationCount, _SubChunkCount) ->
	?LOG_ERROR("rx4096_decrypt_composite_chunk_nif"),
	erlang:nif_error(nif_not_loaded).

rx4096_decrypt_composite_sub_chunk_nif(_State, _Data, _Chunk, _OutSize,
		_JIT, _LargePages, _HardwareAES, _RoundCount, _IterationCount, _Offset) ->
	?LOG_ERROR("rx4096_decrypt_composite_sub_chunk_nif"),
	erlang:nif_error(nif_not_loaded).

rx4096_reencrypt_composite_chunk_nif(_State,
		_DecryptKey, _EncryptKey, _Chunk,
		_JIT, _LargePages, _HardwareAES,
		_DecryptRoundCount, _EncryptRoundCount,
		_DecryptIterationCount, _EncryptIterationCount,
		_DecryptSubChunkCount, _EncryptSubChunkCount) ->
	?LOG_ERROR("rx4096_reencrypt_composite_chunk_nif"),
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	?LOG_ERROR("Loading ar_rx4096_nif"),
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "rx4096_arweave"]), 0),
	?LOG_ERROR("Loaded ar_rx4096_nif").
