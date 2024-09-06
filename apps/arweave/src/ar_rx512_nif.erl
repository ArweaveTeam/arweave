-module(ar_rx512_nif).

-include_lib("arweave/include/ar.hrl").

-on_load(init_nif/0).

-export([rx512_hash_nif/5, rx512_info_nif/1, rx512_init_nif/5,
		rx512_encrypt_chunk_nif/7, rx512_decrypt_chunk_nif/8,
		rx512_reencrypt_chunk_nif/10
]).

%%%===================================================================
%%% Public interface.
%%%===================================================================

rx512_info_nif(_State) ->
	?LOG_ERROR("rx512_info_nif"),
	erlang:nif_error(nif_not_loaded).

rx512_init_nif(_Key, _HashingMode, _JIT, _LargePages, _Threads) ->
	?LOG_ERROR("rx512_init_nif"),
	erlang:nif_error(nif_not_loaded).

rx512_hash_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	?LOG_ERROR("rx512_hash_nif"),
	erlang:nif_error(nif_not_loaded).

rx512_encrypt_chunk_nif(_State, _Data, _Chunk, _RoundCount, _JIT, _LargePages, _HardwareAES) ->
	?LOG_ERROR("rx512_encrypt_chunk_nif"),
	erlang:nif_error(nif_not_loaded).

rx512_decrypt_chunk_nif(_State, _Data, _Chunk, _OutSize, _RoundCount, _JIT, _LargePages,
		_HardwareAES) ->
	?LOG_ERROR("rx512_decrypt_chunk_nif"),
	erlang:nif_error(nif_not_loaded).

rx512_reencrypt_chunk_nif(_State, _DecryptKey, _EncryptKey, _Chunk, _ChunkSize,
		_DecryptRoundCount, _EncryptRoundCount, _JIT, _LargePages, _HardwareAES) ->
	?LOG_ERROR("rx512_reencrypt_chunk_nif"),
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "rx512_arweave"]), 0).
