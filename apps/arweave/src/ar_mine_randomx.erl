-module(ar_mine_randomx).

-on_load(init_nif/0).

-export([init_fast/2, hash_fast/2, init_light/1, hash_light/2, release_state/1,
		randomx_encrypt_chunk/4,
		randomx_decrypt_chunk/5,
		randomx_decrypt_sub_chunk/5,
		randomx_reencrypt_chunk/7,
		hash_fast_long_with_entropy/2, hash_light_long_with_entropy/2]).

%% These exports are required for the DEBUG mode, where these functions are unused.
%% Also, some of these functions are used in ar_mine_randomx_tests.
-export([init_light_nif/3, hash_light_nif/5, init_fast_nif/4, hash_fast_nif/5,
		release_state_nif/1, jit/0, large_pages/0, hardware_aes/0, bulk_hash_fast_nif/13,
		hash_fast_verify_nif/6, randomx_encrypt_chunk_nif/7, randomx_decrypt_chunk_nif/8,
		randomx_reencrypt_chunk_nif/10,
		hash_fast_long_with_entropy_nif/6, hash_light_long_with_entropy_nif/6,
		bulk_hash_fast_long_with_entropy_nif/14,
		vdf_sha2_nif/5, vdf_parallel_sha_verify_nif/8,
		vdf_parallel_sha_verify_with_reset_nif/10,

		randomx_encrypt_composite_chunk_nif/9,
		randomx_decrypt_composite_chunk_nif/10,
		randomx_decrypt_composite_sub_chunk_nif/10,
		randomx_reencrypt_legacy_to_composite_chunk_nif/11,
		randomx_reencrypt_composite_to_composite_chunk_nif/13
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_vdf.hrl").

-define(RANDOMX_WITH_ENTROPY_ROUNDS, 8).

%%%===================================================================
%%% Public interface.
%%%===================================================================

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

randomx_decrypt_chunk(Packing, RandomxState, Key, Chunk, ChunkSize) ->
	PackedSize = byte_size(Chunk),
	%% For the spora_2_6 and composite packing schemes we want to confirm
	%% the padding in the unpacked chunk is all zeros.
	%% To do that we pass in the maximum chunk size (?DATA_CHUNK_SIZE) to prevent the NIF
	%% from removing the padding. We can then validate the padding and remove it in
	%% ar_packing_server:unpad_chunk/4.
	Size = case Packing of
		{spora_2_6, _Addr} ->
			?DATA_CHUNK_SIZE;
		{composite, _Addr, _PackingDifficulty} ->
			?DATA_CHUNK_SIZE;
		_ ->
			ChunkSize
	end,
	case randomx_decrypt_chunk2(RandomxState, Key, Chunk, Size, Packing) of
		{error, Error} ->
			{exception, Error};
		{ok, Unpacked} ->
			%% Validating the padding (for spora_2_6 and composite) and then remove it.
			case ar_packing_server:unpad_chunk(Packing, Unpacked, ChunkSize, PackedSize) of
				error ->
					{error, invalid_padding};
				UnpackedChunk ->
					{ok, UnpackedChunk}
			end
	end.

randomx_decrypt_sub_chunk(Packing, RandomxState, Key, Chunk, SubChunkStartOffset) ->
	randomx_decrypt_sub_chunk2(Packing, RandomxState, Key, Chunk, SubChunkStartOffset).

-ifdef(DEBUG).

randomx_encrypt_chunk({composite, _, PackingDifficulty} = Packing, _State, Key, Chunk) ->
	Options = [{encrypt, true}, {padding, zero}],
	IV = binary:part(Key, {0, 16}),
	SubChunks = split_into_sub_chunks(ar_packing_server:pad_chunk(Chunk)),
	{ok, iolist_to_binary(lists:map(
			fun({SubChunkStartOffset, SubChunk}) ->
				Key2 = crypto:hash(sha256, << Key/binary, SubChunkStartOffset:24 >>),
				lists:foldl(
					fun(_, Acc) ->
						crypto:crypto_one_time(aes_256_cbc, Key2, IV, Acc, Options)
					end,
					SubChunk,
					lists:seq(1, PackingDifficulty)
				)
			end,
			SubChunks))};
randomx_encrypt_chunk(Packing, _State, Key, Chunk) ->
	Options = [{encrypt, true}, {padding, zero}],
	IV = binary:part(Key, {0, 16}),
	{ok, crypto:crypto_one_time(aes_256_cbc, Key, IV,
			ar_packing_server:pad_chunk(Chunk), Options)}.

split_into_sub_chunks(Chunk) ->
	split_into_sub_chunks(Chunk, 0).

split_into_sub_chunks(<<>>, _StartOffset) ->
	[];
split_into_sub_chunks(<< SubChunk:8192/binary, Rest/binary >>, StartOffset) ->
	[{StartOffset, SubChunk} | split_into_sub_chunks(Rest, StartOffset + 8192)].

randomx_decrypt_chunk2(_RandomxState, Key, Chunk, _ChunkSize,
		{composite, _, PackingDifficulty} = Packing) ->
	Options = [{encrypt, false}],
	IV = binary:part(Key, {0, 16}),
	SubChunks = split_into_sub_chunks(Chunk),
	{ok, iolist_to_binary(lists:map(
			fun({SubChunkStartOffset, SubChunk}) ->
				Key2 = crypto:hash(sha256, << Key/binary, SubChunkStartOffset:24 >>),
				lists:foldl(
					fun(_, Acc) ->
						crypto:crypto_one_time(aes_256_cbc, Key2, IV, Acc, Options)
					end,
					SubChunk,
					lists:seq(1, PackingDifficulty)
				)
			end,
			SubChunks))};
randomx_decrypt_chunk2(_RandomxState, Key, Chunk, _ChunkSize, Packing) ->
	Options = [{encrypt, false}],
	IV = binary:part(Key, {0, 16}),
	{ok, crypto:crypto_one_time(aes_256_cbc, Key, IV, Chunk, Options)}.

randomx_decrypt_sub_chunk2(Packing, _RandomxState, Key, Chunk, SubChunkStartOffset) ->
	{_, _, Iterations} = Packing,
	Options = [{encrypt, false}],
	Key2 = crypto:hash(sha256, << Key/binary, SubChunkStartOffset:24 >>),
	IV = binary:part(Key, {0, 16}),
	{ok, lists:foldl(fun(_, Acc) ->
			crypto:crypto_one_time(aes_256_cbc, Key2, IV, Acc, Options)
		end, Chunk, lists:seq(1, Iterations))}.

randomx_reencrypt_chunk(SourcePacking, TargetPacking,
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize) ->
	case randomx_decrypt_chunk(SourcePacking, RandomxState, UnpackKey, Chunk, ChunkSize) of
		{ok, UnpackedChunk} ->
			{ok, RepackedChunk} = randomx_encrypt_chunk(TargetPacking, RandomxState, PackKey,
					ar_packing_server:pad_chunk(UnpackedChunk)),
			case {SourcePacking, TargetPacking} of
				{{composite, Addr, _}, {composite, Addr, _}} ->
					%% See the same function defined for the no-DEBUG mode.
					{ok, RepackedChunk, none};
				_ ->
					{ok, RepackedChunk, UnpackedChunk}
			end;
		Error ->
			Error
	end.
-else.
randomx_encrypt_chunk(Packing, RandomxState, Key, Chunk) ->
	randomx_encrypt_chunk2(Packing, RandomxState, Key, Chunk).

randomx_encrypt_chunk2(spora_2_5, RandomxState, Key, Chunk) ->
	case randomx_encrypt_chunk_nif(RandomxState, Key, Chunk, ?RANDOMX_PACKING_ROUNDS, jit(),
				large_pages(), hardware_aes()) of
		{error, Error} ->
			{exception, Error};
		Reply ->
			Reply
	end;
randomx_encrypt_chunk2({spora_2_6, _Addr}, RandomxState, Key, Chunk) ->
	case randomx_encrypt_chunk_nif(RandomxState, Key, Chunk, ?RANDOMX_PACKING_ROUNDS_2_6,
			jit(), large_pages(), hardware_aes()) of
		{error, Error} ->
			{exception, Error};
		Reply ->
			Reply
	end;
randomx_encrypt_chunk2({composite, _Addr, PackingDifficulty}, RandomxState, Key, Chunk) ->
	case randomx_encrypt_composite_chunk_nif(RandomxState, Key, Chunk,
			jit(), large_pages(), hardware_aes(), ?PACKING_DIFFICULTY_ONE_ROUND_COUNT,
			PackingDifficulty, ?PACKING_DIFFICULTY_ONE_SUB_CHUNK_COUNT) of
		{error, Error} ->
			{exception, Error};
		Reply ->
			Reply
	end.

randomx_decrypt_chunk2(RandomxState, Key, Chunk, ChunkSize, spora_2_5) ->
	randomx_decrypt_chunk_nif(RandomxState, Key, Chunk, ChunkSize, ?RANDOMX_PACKING_ROUNDS,
			jit(), large_pages(), hardware_aes());
randomx_decrypt_chunk2(RandomxState, Key, Chunk, ChunkSize, {spora_2_6, _Addr}) ->
	randomx_decrypt_chunk_nif(RandomxState, Key, Chunk, ChunkSize, ?RANDOMX_PACKING_ROUNDS_2_6,
			jit(), large_pages(), hardware_aes());
randomx_decrypt_chunk2(RandomxState, Key, Chunk, ChunkSize,
		{composite, _Addr, PackingDifficulty}) ->
	randomx_decrypt_composite_chunk_nif(RandomxState, Key, Chunk, ChunkSize,
			jit(), large_pages(), hardware_aes(), ?PACKING_DIFFICULTY_ONE_ROUND_COUNT,
			PackingDifficulty, ?PACKING_DIFFICULTY_ONE_SUB_CHUNK_COUNT).

randomx_decrypt_sub_chunk2(Packing, RandomxState, Key, Chunk, SubChunkStartOffset) ->
	{_, _, IterationCount} = Packing,
	RoundCount = ?PACKING_DIFFICULTY_ONE_ROUND_COUNT,
	OutSize = ?PACKING_DIFFICULTY_ONE_SUB_CHUNK_SIZE,
	randomx_decrypt_composite_sub_chunk_nif(RandomxState, Key, Chunk, OutSize,
		jit(), large_pages(), hardware_aes(), RoundCount, IterationCount, SubChunkStartOffset).

randomx_reencrypt_chunk({composite, Addr1, PackingDifficulty1},
		{composite, Addr2, PackingDifficulty2},
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize) ->
	case randomx_reencrypt_composite_to_composite_chunk_nif(RandomxState, UnpackKey,
			PackKey, Chunk, jit(), large_pages(), hardware_aes(),
			?PACKING_DIFFICULTY_ONE_ROUND_COUNT, ?PACKING_DIFFICULTY_ONE_ROUND_COUNT,
			PackingDifficulty1, PackingDifficulty2,
			?PACKING_DIFFICULTY_ONE_SUB_CHUNK_COUNT, ?PACKING_DIFFICULTY_ONE_SUB_CHUNK_COUNT) of
		{error, Error} ->
			{exception, Error};
		{ok, Repacked, RepackInput} ->
			case Addr1 == Addr2 of
				true ->
					%% When the addresses match, we do not have to unpack the chunk - we may
					%% simply pack the missing iterations so RepackInput is not the unpacked
					%% chunk and we return none instead. If the caller needs the unpacked
					%% chunk as well, they need to make an extra call.
					{ok, Repacked, none};
				false ->
					%% RepackInput is the unpacked chunk - return it.
					Unpadded = ar_packing_server:unpad_chunk(RepackInput, ChunkSize,
							?DATA_CHUNK_SIZE),
					{ok, Repacked, Unpadded}
			end;
		Reply ->
			Reply
	end;
randomx_reencrypt_chunk({spora_2_6, _Addr1}, {composite, _Addr2, PackingDifficulty},
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize) ->
	case randomx_reencrypt_legacy_to_composite_chunk_nif(RandomxState, UnpackKey,
			PackKey, Chunk, jit(), large_pages(), hardware_aes(),
			?RANDOMX_PACKING_ROUNDS_2_6, ?PACKING_DIFFICULTY_ONE_ROUND_COUNT,
			PackingDifficulty, ?PACKING_DIFFICULTY_ONE_SUB_CHUNK_COUNT) of
		{error, Error} ->
			{exception, Error};
		{ok, Repacked, RepackInput} ->
			Unpadded = ar_packing_server:unpad_chunk(RepackInput, ChunkSize, ?DATA_CHUNK_SIZE),
			{ok, Repacked, Unpadded}
	end;
randomx_reencrypt_chunk(spora_2_5, {composite, _Addr2, PackingDifficulty},
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize) ->
	case randomx_reencrypt_legacy_to_composite_chunk_nif(RandomxState, UnpackKey,
			PackKey, Chunk, jit(), large_pages(), hardware_aes(),
			?RANDOMX_PACKING_ROUNDS, ?PACKING_DIFFICULTY_ONE_ROUND_COUNT,
			PackingDifficulty, ?PACKING_DIFFICULTY_ONE_SUB_CHUNK_COUNT) of
		{error, Error} ->
			{exception, Error};
		{ok, Repacked, RepackInput} ->
			Unpadded = ar_packing_server:unpad_chunk(RepackInput, ChunkSize, ?DATA_CHUNK_SIZE),
			{ok, Repacked, Unpadded}
	end;
randomx_reencrypt_chunk(SourcePacking, TargetPacking,
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize) ->
	UnpackRounds = packing_rounds(SourcePacking),
	PackRounds = packing_rounds(TargetPacking),
	case randomx_reencrypt_chunk_nif(RandomxState, UnpackKey, PackKey, Chunk, ChunkSize,
				UnpackRounds, PackRounds, jit(), large_pages(), hardware_aes()) of
		{error, Error} ->
			{exception, Error};
		Reply ->
			Reply
	end.

packing_rounds(spora_2_5) ->
	?RANDOMX_PACKING_ROUNDS;
packing_rounds({spora_2_6, _Addr}) ->
	?RANDOMX_PACKING_ROUNDS_2_6.
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

%%%===================================================================
%%% Private functions.
%%%===================================================================

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
	_PartitionUpperBound,
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

randomx_reencrypt_chunk_nif(_State, _DecryptKey, _EncryptKey, _Chunk, _ChunkSize,
		_DecryptRoundCount, _EncryptRoundCount, _JIT, _LargePages, _HardwareAES) ->
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
	_PartitionUpperBound,
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

vdf_sha2_nif(_Salt, _PrevState, _CheckpointCount, _skipCheckpointCount, _Iterations) ->
	erlang:nif_error(nif_not_loaded).
vdf_parallel_sha_verify_nif(_Salt, _PrevState, _CheckpointCount, _skipCheckpointCount,
		_Iterations, _InCheckpoint, _InRes, _MaxThreadCount) ->
	erlang:nif_error(nif_not_loaded).
vdf_parallel_sha_verify_with_reset_nif(_Salt, _PrevState, _CheckpointCount,
		_skipCheckpointCount, _Iterations, _InCheckpoint, _InRes, _ResetSalt, _ResetSeed,
		_MaxThreadCount) ->
	erlang:nif_error(nif_not_loaded).

randomx_encrypt_composite_chunk_nif(_State, _Key, _Chunk, _JIT, _LargePages, _HardwareAES,
		_RoundCount, _IterationCount, _SubChunkCount) ->
	erlang:nif_error(nif_not_loaded).

randomx_decrypt_composite_chunk_nif(_State, _Data, _Chunk, _OutSize,
		_JIT, _LargePages, _HardwareAES, _RoundCount, _IterationCount, _SubChunkCount) ->
	erlang:nif_error(nif_not_loaded).

randomx_decrypt_composite_sub_chunk_nif(_State, _Data, _Chunk, _OutSize,
		_JIT, _LargePages, _HardwareAES, _RoundCount, _IterationCount, _Offset) ->
	erlang:nif_error(nif_not_loaded).

randomx_reencrypt_legacy_to_composite_chunk_nif(_State, _DecryptKey, _EncryptKey,
		_Chunk, _JIT, _LargePages, _HardwareAES,
		_DecryptRoundCount, _EncryptRoundCount, _IterationCount, _SubChunkCount) ->
	erlang:nif_error(nif_not_loaded).

randomx_reencrypt_composite_to_composite_chunk_nif(_State,
		_DecryptKey, _EncryptKey, _Chunk,
		_JIT, _LargePages, _HardwareAES,
		_DecryptRoundCount, _EncryptRoundCount,
		_DecryptIterationCount, _EncryptIterationCount,
		_DecryptSubChunkCount, _EncryptSubChunkCount) ->
	erlang:nif_error(nif_not_loaded).

init_nif() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "arweave"]), 0).
