-module(ar_mine_randomx).

-on_load(init_nif/0).

-export([init_fast/2, hash_fast/2, init_light/1, hash_light/2, release_state/1,
		randomx_encrypt_chunk/4,
		randomx_decrypt_chunk/5,
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
		vdf_parallel_sha_verify_with_reset_nif/10]).

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

packing_rounds(Packing) ->
	case Packing of
		spora_2_5 ->
			?RANDOMX_PACKING_ROUNDS;
		spora_2_6 ->
			?RANDOMX_PACKING_ROUNDS_2_6
	end.

unpad_chunk(spora_2_5, Unpacked, ChunkSize, _PackedSize) ->
	binary:part(Unpacked, 0, ChunkSize);
unpad_chunk(spora_2_6, Unpacked, ChunkSize, PackedSize) ->
	Padding = binary:part(Unpacked, ChunkSize, PackedSize - ChunkSize),
	case Padding of
		<<>> ->
			Unpacked;
		_ ->
			case is_zero(Padding) of
				false ->
					error;
				true ->
					binary:part(Unpacked, 0, ChunkSize)
			end
	end.

is_zero(<< 0:8, Rest/binary >>) ->
	is_zero(Rest);
is_zero(<<>>) ->
	true;
is_zero(_Rest) ->
	false.

randomx_decrypt_chunk(Packing, RandomxState, Key, Chunk, ChunkSize) ->
	PackedSize = byte_size(Chunk),
	%% For spora_2_6 we want to confirm that the padding in the unpacked chunk is all zeros.
	%% To do that we pass in the maximum chunk size (?DATA_CHUNK_SIZE) to prevent the NIF
	%% from removing the padding. We can then validate the padding and remove it in
	%% unpad_chunk/4.
	Size = case Packing of
		spora_2_6 ->
			?DATA_CHUNK_SIZE;
		_ ->
			ChunkSize
	end,
	case randomx_decrypt_chunk2(RandomxState, Key, Chunk, Size, packing_rounds(Packing)) of
		{error, Error} ->
			{exception, Error};
		{ok, Unpacked} ->
			%% Validating the padding (for spora_2_6) and then remove it.
			case unpad_chunk(Packing, Unpacked, ChunkSize, PackedSize) of
				error ->
					{error, invalid_padding};
				UnpackedChunk ->
					{ok, UnpackedChunk}
			end
	end.

-ifdef(DEBUG).

%% @doc The NIF will add padding to the encrypted chunk, but we have to do it ourselves
%% when running in DEBUG mode.
pad_chunk(Chunk) ->
	pad_chunk(Chunk, byte_size(Chunk)).
pad_chunk(Chunk, ChunkSize) when ChunkSize == (?DATA_CHUNK_SIZE) ->
	Chunk;
pad_chunk(Chunk, ChunkSize) ->
	Zeros =
		case erlang:get(zero_chunk) of
			undefined ->
				ZeroChunk = << <<0>> || _ <- lists:seq(1, ?DATA_CHUNK_SIZE) >>,
				%% Cache the zero chunk in the process memory, constructing
				%% it is expensive.
				erlang:put(zero_chunk, ZeroChunk),
				ZeroChunk;
			ZeroChunk ->
				ZeroChunk
		end,
	PaddingSize = (?DATA_CHUNK_SIZE) - ChunkSize,
	<< Chunk/binary, (binary:part(Zeros, 0, PaddingSize))/binary >>.

randomx_encrypt_chunk(_Packing, _State, Key, Chunk) ->
	Options = [{encrypt, true}, {padding, zero}],
	IV = binary:part(Key, {0, 16}),
	{ok, crypto:crypto_one_time(aes_256_cbc, Key, IV, pad_chunk(Chunk), Options)}.

randomx_decrypt_chunk2(_RandomxState, Key, Chunk, _ChunkSize, _Rounds) ->
	Options = [{encrypt, false}],
	IV = binary:part(Key, {0, 16}),
	{ok, crypto:crypto_one_time(aes_256_cbc, Key, IV, Chunk, Options)}.

randomx_reencrypt_chunk(SourcePacking, TargetPacking,
		RandomxState, UnpackKey, PackKey, Chunk, ChunkSize) ->
	case randomx_decrypt_chunk(SourcePacking, RandomxState, UnpackKey, Chunk, ChunkSize) of
		{ok, UnpackedChunk} ->
			{ok, RepackedChunk} = randomx_encrypt_chunk(TargetPacking, RandomxState, PackKey, pad_chunk(UnpackedChunk)),
			{ok, RepackedChunk, UnpackedChunk};
		Error ->
			Error
	end.
-else.
randomx_encrypt_chunk(Packing, RandomxState, Key, Chunk) ->
	case randomx_encrypt_chunk_nif(RandomxState, Key, Chunk, packing_rounds(Packing), jit(),
				large_pages(), hardware_aes()) of
		{error, Error} ->
			{exception, Error};
		Reply ->
			Reply
	end.

randomx_decrypt_chunk2(RandomxState, Key, Chunk, ChunkSize, Rounds) ->
	randomx_decrypt_chunk_nif(RandomxState, Key, Chunk, ChunkSize, Rounds,
				jit(), large_pages(), hardware_aes()).

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

init_nif() ->
	PrivDir = code:priv_dir(arweave),
	ok = erlang:load_nif(filename:join([PrivDir, "arweave"]), 0).
