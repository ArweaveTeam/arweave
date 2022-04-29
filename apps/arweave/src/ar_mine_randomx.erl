-module(ar_mine_randomx).

-on_load(init_nif/0).

-export([
	init_fast/2, hash_fast/2,
	init_light/1, hash_light/2,
	release_state/1,
	bulk_hash_fast/13,
	hash_fast_verify/3,
	randomx_encrypt_chunk/3,
	randomx_decrypt_chunk/4,
	hash_fast_long_with_entropy/2,
	hash_light_long_with_entropy/2,
	bulk_hash_fast_long_with_entropy/13
]).

%% These exports are required for the DEBUG mode, where these functions are unused.
%% Also, some of these functions are used in ar_mine_randomx_tests.
-export([
	init_light_nif/3, hash_light_nif/5,
	init_fast_nif/4, hash_fast_nif/5,
	release_state_nif/1, jit/0, large_pages/0, hardware_aes/0,
	bulk_hash_fast_nif/13,
	hash_fast_verify_nif/6,
	randomx_encrypt_chunk_nif/7, randomx_decrypt_chunk_nif/8,
	hash_fast_long_with_entropy_nif/6, hash_light_long_with_entropy_nif/6,
		bulk_hash_fast_long_with_entropy_nif/14
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_mine.hrl").
-include_lib("arweave/include/ar_config.hrl").

-define(RANDOMX_WITH_ENTROPY_ROUNDS, 8).
-define(RANDOMX_PACKING_ROUNDS, 8 * (?PACKING_DIFFICULTY)).

-ifdef(DEBUG).
init_fast(Key, _Threads) ->
	Key.
-else.
init_fast(Key, Threads) ->
	{ok, FastState} = init_fast_nif(Key, jit(), large_pages(), Threads),
	FastState.
-endif.

-ifdef(DEBUG).
hash_fast(FastState, Data) ->
	crypto:hash(sha256, << FastState/binary, Data/binary >>).
-else.
hash_fast(FastState, Data) ->
	{ok, Hash} =
		hash_fast_nif(FastState, Data, jit(), large_pages(), hardware_aes()),
	Hash.
-endif.

-ifdef(DEBUG).
bulk_hash_fast(FastState, Nonce1, Nonce2, BDS, PrevH, SearchSpaceUpperBound, PIDs, ProxyPIDs,
		_HashingIterations, _JIT, _LargePages, _HardwareAES, Ref) ->
	%% A simple mock of the NIF function with hashingIterations == 2.
	H1 = crypto:hash(sha256, << FastState/binary, Nonce1/binary, BDS/binary >>),
	H2 = crypto:hash(sha256, << FastState/binary, Nonce2/binary, BDS/binary >>),
	[PID1 | OtherPIDs] = PIDs,
	{ok, Byte1} = ar_mine:pick_recall_byte(H1, PrevH, SearchSpaceUpperBound),
	[ProxyPID1 | OtherProxyPIDs] = ProxyPIDs,
	PID1 ! {binary:encode_unsigned(Byte1), H1, Nonce1, ProxyPID1, Ref},
	[PID2 | _] = OtherPIDs ++ [PID1],
	{ok, Byte2} = ar_mine:pick_recall_byte(H2, PrevH, SearchSpaceUpperBound),
	[ProxyPID2 | _] = OtherProxyPIDs ++ [ProxyPID1],
	PID2 ! {binary:encode_unsigned(Byte2), H2, Nonce2, ProxyPID2, Ref},
	ok.
-else.
bulk_hash_fast(FastState, Nonce1, Nonce2, BDS, PrevH, SearchSpaceUpperBound, PIDs, ProxyPIDs,
		HashingIterations, JIT, LargePages, HardwareAES, Ref) ->
	bulk_hash_fast_nif(FastState, Nonce1, Nonce2, BDS, PrevH,
			binary:encode_unsigned(SearchSpaceUpperBound, big), PIDs, ProxyPIDs, Ref,
			HashingIterations, JIT, LargePages, HardwareAES).
-endif.

-ifdef(DEBUG).
hash_fast_verify(FastState, Diff, Preimage) ->
	Hash = crypto:hash(sha256, [FastState | Preimage]),
	case binary:decode_unsigned(Hash, big) > Diff of
		true ->
			{true, Hash};
		false ->
			false
	end.
-else.
hash_fast_verify(FastState, Diff, Preimage) ->
	DiffBinary = binary:encode_unsigned(Diff, big),
	hash_fast_verify_nif(FastState, DiffBinary, Preimage, jit(), large_pages(), hardware_aes()).
-endif.

-ifdef(DEBUG).
init_light(Key) ->
	Key.
-else.
init_light(Key) ->
	{ok, LightState} = init_light_nif(Key, jit(), large_pages()),
	LightState.
-endif.

-ifdef(DEBUG).
hash_light(LightState, Data) ->
	hash_fast(LightState, Data).
-else.
hash_light(LightState, Data) ->
	{ok, Hash} =
		hash_light_nif(LightState, Data, jit(), large_pages(), hardware_aes()),
	Hash.
-endif.

-ifdef(DEBUG).
randomx_encrypt_chunk(_State, Key, Chunk) ->
	Options = [{encrypt, true}, {padding, zero}],
	IV = binary:part(Key, {0, 16}),
	crypto:crypto_one_time(aes_256_cbc, Key, IV, Chunk, Options).

randomx_decrypt_chunk(_State, Key, Chunk, _Size) ->
	Options = [{encrypt, false}],
	IV = binary:part(Key, {0, 16}),
	crypto:crypto_one_time(aes_256_cbc, Key, IV, Chunk, Options).
-else.
randomx_encrypt_chunk(RandomxState, Key, Chunk) ->
	{ok, OutChunk} =
		randomx_encrypt_chunk_nif(RandomxState, Key, Chunk, ?RANDOMX_PACKING_ROUNDS, jit(),
				large_pages(), hardware_aes()),
	OutChunk.

randomx_decrypt_chunk(RandomxState, Key, Chunk, Size) ->
	{ok, OutChunk} =
		randomx_decrypt_chunk_nif(RandomxState, Key, Chunk, Size, ?RANDOMX_PACKING_ROUNDS,
				jit(), large_pages(), hardware_aes()),
	OutChunk.
-endif.

-ifdef(DEBUG).
hash_fast_long_with_entropy(FastState, Data) ->
	Hash = crypto:hash(sha256, << FastState/binary, Data/binary >>),
	%% 256 bytes of entropy for tests (in practice we generate 256 KiB of entropy).
	Entropy = iolist_to_binary([Hash || _ <- lists:seq(1, 8)]),
	{Hash, Entropy}.
-else.
hash_fast_long_with_entropy(FastState, Data) ->
	{ok, Hash, Entropy} =
		hash_fast_long_with_entropy_nif(FastState, Data, ?RANDOMX_WITH_ENTROPY_ROUNDS, jit(),
				large_pages(), hardware_aes()),
	{Hash, Entropy}.
-endif.

-ifdef(DEBUG).
hash_light_long_with_entropy(LightState, Data) ->
	hash_fast_long_with_entropy(LightState, Data).
-else.
hash_light_long_with_entropy(LightState, Data) ->
	{ok, Hash, Entropy} =
		hash_light_long_with_entropy_nif(LightState, Data, ?RANDOMX_WITH_ENTROPY_ROUNDS, jit(),
				large_pages(), hardware_aes()),
	{Hash, Entropy}.
-endif.

-ifdef(DEBUG).
bulk_hash_fast_long_with_entropy(FastState, Nonce1, Nonce2, BDS, PrevH, SearchSpaceUpperBound,
		PIDs, ProxyPIDs, _HashingIterations, _JIT, _LargePages, _HardwareAES, Ref) ->
	%% A simple mock of the NIF function with hashingIterations == 2.
	H1 = crypto:hash(sha256, << FastState/binary, Nonce1/binary, BDS/binary >>),
	Entropy1 = iolist_to_binary([H1 || _ <- lists:seq(1, 8)]),
	H2 = crypto:hash(sha256, << FastState/binary, Nonce2/binary, BDS/binary >>),
	Entropy2 = iolist_to_binary([H2 || _ <- lists:seq(1, 8)]),
	[PID1 | OtherPIDs] = PIDs,
	{ok, Byte1} = ar_mine:pick_recall_byte(H1, PrevH, SearchSpaceUpperBound),
	[ProxyPID1 | OtherProxyPIDs] = ProxyPIDs,
	PID1 ! {binary:encode_unsigned(Byte1), H1, Entropy1, Nonce1, ProxyPID1, Ref},
	[PID2 | _] = OtherPIDs ++ [PID1],
	{ok, Byte2} = ar_mine:pick_recall_byte(H2, PrevH, SearchSpaceUpperBound),
	[ProxyPID2 | _] = OtherProxyPIDs ++ [ProxyPID1],
	PID2 ! {binary:encode_unsigned(Byte2), H2, Entropy2, Nonce2, ProxyPID2, Ref},
	ok.
-else.
bulk_hash_fast_long_with_entropy(FastState, Nonce1, Nonce2, BDS, PrevH, SearchSpaceUpperBound,
		PIDs, ProxyPIDs, HashingIterations, JIT, LargePages, HardwareAES, Ref) ->
	bulk_hash_fast_long_with_entropy_nif(FastState, Nonce1, Nonce2, BDS, PrevH,
			binary:encode_unsigned(SearchSpaceUpperBound, big), PIDs, ProxyPIDs, Ref,
			HashingIterations, ?RANDOMX_WITH_ENTROPY_ROUNDS, JIT, LargePages, HardwareAES).
-endif.

-ifdef(DEBUG).
release_state(_State) ->
	ok.
-else.
release_state(RandomxState) ->
	case release_state_nif(RandomxState) of
		ok ->
			?LOG_INFO([{event, released_randomx_state}]),
			ok;
		{error, Reason} ->
			?LOG_WARNING([{event, failed_to_release_randomx_state}, {reason, Reason}]),
			error
	end.
-endif.

%% Internal

jit() ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(randomx_jit, Config#config.disable) of
		true ->
			0;
		_ ->
			1
	end.

large_pages() ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(randomx_large_pages, Config#config.enable) of
		true ->
			1;
		_ ->
			0
	end.

hardware_aes() ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(randomx_hardware_aes, Config#config.disable) of
		true ->
			0;
		_ ->
			1
	end.

init_fast_nif(_Key, _JIT, _LargePages, _Threads) ->
	erlang:nif_error(nif_not_loaded).

init_light_nif(_Key, _JIT, _LargePages) ->
	erlang:nif_error(nif_not_loaded).

hash_fast_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

bulk_hash_fast_nif(
	_State,
	_Nonce1,
	_Nonce2,
	_BDS,
	_PrevH,
	_SearchSpaceUpperBound,
	_PIDs,
	_ProxyPIDs,
	_Ref,
	_Iterations,
	_JIT,
	_LargePages,
	_HardwareAES
) ->
	erlang:nif_error(nif_not_loaded).

hash_fast_verify_nif(_State, _Diff, _Preimage, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

hash_light_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

randomx_encrypt_chunk_nif(_State, _Data, _Chunk, _RoundCount, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

randomx_decrypt_chunk_nif(_State, _Data, _Chunk, _OutSize, _RoundCount, _JIT, _LargePages,
		_HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

hash_fast_long_with_entropy_nif(_State, _Data, _RoundCount, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

hash_light_long_with_entropy_nif(_State, _Data, _RoundCount, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

bulk_hash_fast_long_with_entropy_nif(
	_State,
	_Nonce1,
	_Nonce2,
	_BDS,
	_PrevH,
	_SearchSpaceUpperBound,
	_PIDs,
	_ProxyPIDs,
	_Ref,
	_Iterations,
	_RoundCount,
	_JIT,
	_LargePages,
	_HardwareAES
) ->
	erlang:nif_error(nif_not_loaded).

release_state_nif(_State) ->
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "arweave"]), 0).
