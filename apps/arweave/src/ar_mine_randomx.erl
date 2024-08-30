-module(ar_mine_randomx).

-on_load(init_nif/0).

-export([init_fast/2, init_light/1, hash/2,
		randomx_encrypt_chunk/4,
		randomx_decrypt_chunk/5,
		randomx_decrypt_sub_chunk/5,
		randomx_reencrypt_chunk/7]).

%% These exports are required for the DEBUG mode, where these functions are unused.
%% Also, some of these functions are used in ar_mine_randomx_tests.
-export([hash_nif/5, randomx_info_nif/1, init_randomx_nif/5,
		jit/0, large_pages/0, hardware_aes/0,
		randomx_encrypt_chunk_nif/7, randomx_decrypt_chunk_nif/8,
		randomx_reencrypt_chunk_nif/10,
		vdf_sha2_nif/5,
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

%%%===================================================================
%%% Public interface.
%%%===================================================================

-ifdef(DEBUG).
init_fast(Key, _Threads) ->
	{debug_state, Key}.
init_light(Key) ->
	{debug_state, Key}.
hash({debug_state, Key}, Data) ->
	crypto:hash(sha256, << Key/binary, Data/binary >>).
-else.
init_fast(Key, Threads) ->
	{ok, FastState} = init_randomx_nif(Key, ?RANDOMX_HASHING_MODE_FAST, jit(), large_pages(), Threads),
	FastState.
init_light(Key) ->
	{ok, LightState} = init_randomx_nif(Key, ?RANDOMX_HASHING_MODE_LIGHT, jit(), large_pages(), 0),
	LightState.
hash(State, Data) ->
	{ok, Hash} =
		hash_nif(State, Data, jit(), large_pages(), hardware_aes()),
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
			jit(), large_pages(), hardware_aes(), ?COMPOSITE_PACKING_ROUND_COUNT,
			PackingDifficulty, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT) of
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
			jit(), large_pages(), hardware_aes(), ?COMPOSITE_PACKING_ROUND_COUNT,
			PackingDifficulty, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT).

randomx_decrypt_sub_chunk2(Packing, RandomxState, Key, Chunk, SubChunkStartOffset) ->
	{_, _, IterationCount} = Packing,
	RoundCount = ?COMPOSITE_PACKING_ROUND_COUNT,
	OutSize = ?COMPOSITE_PACKING_SUB_CHUNK_SIZE,
	randomx_decrypt_composite_sub_chunk_nif(RandomxState, Key, Chunk, OutSize,
		jit(), large_pages(), hardware_aes(), RoundCount, IterationCount, SubChunkStartOffset).

randomx_reencrypt_chunk({composite, Addr1, PackingDifficulty1},
		{composite, Addr2, PackingDifficulty2},
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize) ->
	case randomx_reencrypt_composite_to_composite_chunk_nif(RandomxState, UnpackKey,
			PackKey, Chunk, jit(), large_pages(), hardware_aes(),
			?COMPOSITE_PACKING_ROUND_COUNT, ?COMPOSITE_PACKING_ROUND_COUNT,
			PackingDifficulty1, PackingDifficulty2,
			?COMPOSITE_PACKING_SUB_CHUNK_COUNT, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT) of
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
			?RANDOMX_PACKING_ROUNDS_2_6, ?COMPOSITE_PACKING_ROUND_COUNT,
			PackingDifficulty, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT) of
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
			?RANDOMX_PACKING_ROUNDS, ?COMPOSITE_PACKING_ROUND_COUNT,
			PackingDifficulty, ?COMPOSITE_PACKING_SUB_CHUNK_COUNT) of
		{error, Error} ->
			{exception, Error};
		{ok, Repacked, RepackInput} ->
			Unpadded = ar_packing_server:unpad_chunk(RepackInput, ChunkSize, ?DATA_CHUNK_SIZE),
			{ok, Repacked, Unpadded}
	end;
randomx_reencrypt_chunk(SourcePacking, TargetPacking,
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize) ->
	randomx_reencrypt_chunk2(SourcePacking, TargetPacking,
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize).

%%%===================================================================
%%% Private functions.
%%%===================================================================


%% -------------------------------------------------------------------------------------------
%% Helper functions
%% -------------------------------------------------------------------------------------------
packing_rounds(spora_2_5) ->
	?RANDOMX_PACKING_ROUNDS;
packing_rounds({spora_2_6, _Addr}) ->
	?RANDOMX_PACKING_ROUNDS_2_6.

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

split_into_sub_chunks(Chunk) ->
	split_into_sub_chunks(Chunk, 0).

split_into_sub_chunks(<<>>, _StartOffset) ->
	[];
split_into_sub_chunks(<< SubChunk:8192/binary, Rest/binary >>, StartOffset) ->
	[{StartOffset, SubChunk} | split_into_sub_chunks(Rest, StartOffset + 8192)].

%% -------------------------------------------------------------------------------------------
%% randomx_[encrypt|decrypt|reencrypt]_chunk2
%% DEBUG implementation, used in tests, is called when State is {debug_state, Key}
%% Otherwise, NIF implementation is used
%% We set it up this way so that we can have some tests trigger the NIF implementation
%% -------------------------------------------------------------------------------------------

%% DEBUG implementation
randomx_decrypt_chunk2({debug_state, _}, Key, Chunk, _ChunkSize,
	{composite, _, PackingDifficulty} = _Packing) ->
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
randomx_decrypt_chunk2({debug_state, _}, Key, Chunk, _ChunkSize, _Packing) ->
	Options = [{encrypt, false}],
	IV = binary:part(Key, {0, 16}),
	{ok, crypto:crypto_one_time(aes_256_cbc, Key, IV, Chunk, Options)};
%% Non-DEBUG implementation
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

%% DEBUG implementation
randomx_decrypt_sub_chunk2(Packing, {debug_state, _}, Key, Chunk, SubChunkStartOffset) ->
	{_, _, Iterations} = Packing,
	Options = [{encrypt, false}],
	Key2 = crypto:hash(sha256, << Key/binary, SubChunkStartOffset:24 >>),
	IV = binary:part(Key, {0, 16}),
	{ok, lists:foldl(fun(_, Acc) ->
			crypto:crypto_one_time(aes_256_cbc, Key2, IV, Acc, Options)
		end, Chunk, lists:seq(1, Iterations))};
%% Non-DEBUG implementation
randomx_decrypt_sub_chunk2(Packing, RandomxState, Key, Chunk, SubChunkStartOffset) ->
	{_, _, IterationCount} = Packing,
	RoundCount = ?PACKING_DIFFICULTY_ONE_ROUND_COUNT,
	OutSize = ?PACKING_DIFFICULTY_ONE_SUB_CHUNK_SIZE,
	randomx_decrypt_composite_sub_chunk_nif(RandomxState, Key, Chunk, OutSize,
		jit(), large_pages(), hardware_aes(), RoundCount, IterationCount, SubChunkStartOffset).

%% DEBUG implementation
randomx_encrypt_chunk2({composite, _, PackingDifficulty} = _Packing, {debug_state, _}, Key, Chunk) ->
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
randomx_encrypt_chunk2(_Packing, {debug_state, _}, Key, Chunk) ->
	Options = [{encrypt, true}, {padding, zero}],
	IV = binary:part(Key, {0, 16}),
	{ok, crypto:crypto_one_time(aes_256_cbc, Key, IV,
			ar_packing_server:pad_chunk(Chunk), Options)};
%% Non-DEBUG implementation
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

%% DEBUG implementation
randomx_reencrypt_chunk2(SourcePacking, TargetPacking,
		{debug_state, _} = State, UnpackKey, PackKey, Chunk, ChunkSize) ->
	case randomx_decrypt_chunk(SourcePacking, State, UnpackKey, Chunk, ChunkSize) of
		{ok, UnpackedChunk} ->
			{ok, RepackedChunk} = randomx_encrypt_chunk2(TargetPacking, State, PackKey,
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
	end;
%% Non-DEBUG implementation
randomx_reencrypt_chunk2({composite, Addr1, PackingDifficulty1},
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
randomx_reencrypt_chunk2({spora_2_6, _Addr1}, {composite, _Addr2, PackingDifficulty},
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
randomx_reencrypt_chunk2(spora_2_5, {composite, _Addr2, PackingDifficulty},
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
randomx_reencrypt_chunk2(SourcePacking, TargetPacking,
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

%% -------------------------------------------------------------------------------------------
%% NIF functions
%% -------------------------------------------------------------------------------------------
randomx_info_nif(_State) ->
	erlang:nif_error(nif_not_loaded).

init_randomx_nif(_Key, _HashingMode, _JIT, _LargePages, _Threads) ->
	erlang:nif_error(nif_not_loaded).

hash_nif(_State, _Data, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

randomx_encrypt_chunk_nif(_State, _Data, _Chunk, _RoundCount, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

randomx_decrypt_chunk_nif(_State, _Data, _Chunk, _OutSize, _RoundCount, _JIT, _LargePages,
		_HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

randomx_reencrypt_chunk_nif(_State, _DecryptKey, _EncryptKey, _Chunk, _ChunkSize,
		_DecryptRoundCount, _EncryptRoundCount, _JIT, _LargePages, _HardwareAES) ->
	erlang:nif_error(nif_not_loaded).

vdf_sha2_nif(_Salt, _PrevState, _CheckpointCount, _skipCheckpointCount, _Iterations) ->
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
