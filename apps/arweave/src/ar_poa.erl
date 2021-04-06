%%% @doc This module implements all mechanisms required to validate a proof of access
%%% for a chunk of data received from the network.
%%% @end
-module(ar_poa).

-export([
	validate_pre_fork_2_4/4, validate_pre_fork_2_5/3, validate/3,
	pack/4, unpack/5
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PACKING_ROUNDS, 20).
-define(MIN_MAX_OPTION_DEPTH, 100).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Validate a proof of access.
validate(RecallByte, BI, SPoA) ->
	{TXRoot, BlockBase, BlockTop, _BH} = find_challenge_block(RecallByte, BI),
	validate(BlockBase, RecallByte - BlockBase, TXRoot, BlockTop - BlockBase, SPoA).

%% @doc Validate a proof of access.
validate_pre_fork_2_5(RecallByte, BI, POA) ->
	{TXRoot, BlockBase, BlockTop, _BH} = find_challenge_block(RecallByte, BI),
	validate_pre_fork_2_5(RecallByte - BlockBase, TXRoot, BlockTop - BlockBase, POA).

%% @doc Validate a proof of access.
validate_pre_fork_2_4(_H, 0, _BI, _POA) ->
	%% The weave does not have data yet.
	true;
validate_pre_fork_2_4(_H, _WS, BI, #poa{ option = Option })
		when Option > length(BI) andalso Option > ?MIN_MAX_OPTION_DEPTH ->
	false;
validate_pre_fork_2_4(LastIndepHash, WeaveSize, BI, POA) ->
	RecallByte = calculate_challenge_byte_pre_fork_2_4(LastIndepHash, WeaveSize, POA#poa.option),
	validate_pre_fork_2_5(RecallByte, BI, POA).

%% @doc Pack the chunk for mining. Packing ensures every mined chunk of data is globally
%% unique and cannot be easily inferred during mining from any metadata stored in RAM.
%% @end
pack(unpacked, _AbsoluteEndOffset, _TXRoot, Chunk) ->
	%% Allows to reuse the same interface for unpacking and repacking.
	Chunk;
pack(aes_256_cbc, AbsoluteEndOffset, TXRoot, Chunk) ->
	Options = [{encrypt, true}, {padding, zero}],
	%% The presence of the absolute end offset in the key makes sure
	%% packing of every chunk is unique, even when the same chunk is
	%% present in the same transaction or across multiple transactions
	%% or blocks. The presence of the transaction root in the key
	%% ensures one cannot find data that has certain patterns after
	%% packing.
	Key = crypto:hash(sha256, << AbsoluteEndOffset:256, TXRoot/binary >>),
	%% An initialization vector is used for compliance with the CBC cipher
	%% although it is not needed for our purposes because every chunk is
	%% encrypted with a unique key.
	IV = binary:part(Key, {0, 16}),
	pack(Key, IV, Chunk, Options, ?PACKING_ROUNDS).

pack(_Key, _IV, Chunk, _Options, 0) ->
	Chunk;
pack(Key, IV, Chunk, Options, Round) ->
	Chunk2 = crypto:crypto_one_time(aes_256_cbc, Key, IV, Chunk, Options),
	pack(Key, IV, Chunk2, Options, Round - 1).

%% @doc Unpack the chunk packed for mining.
unpack(unpacked, _AbsoluteEndOffset, _TXRoot, Chunk, _ChunkSize) ->
	%% Allows to reuse the same interface for unpacking and repacking.
	Chunk;
unpack(aes_256_cbc, AbsoluteEndOffset, TXRoot, Chunk, ChunkSize) ->
	Options = [{encrypt, false}],
	Key = crypto:hash(sha256, << AbsoluteEndOffset:256, TXRoot/binary >>),
	IV = binary:part(Key, {0, 16}),
	Unpacked = unpack2(Key, IV, Chunk, Options, ?PACKING_ROUNDS),
	binary:part(Unpacked, {0, ChunkSize}).

unpack2(_Key, _IV, Chunk, _Options, 0) ->
	Chunk;
unpack2(Key, IV, Chunk, Options, Round) ->
	Chunk2 = crypto:crypto_one_time(aes_256_cbc, Key, IV, Chunk, Options),
	unpack2(Key, IV, Chunk2, Options, Round - 1).

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc The base of the block is the weave_size tag of the _previous_ block.
%% Traverse the block index until the challenge block is inside the block's bounds.
%% @end
find_challenge_block(Byte, [{BH, BlockTop, TXRoot}]) when Byte < BlockTop ->
	{TXRoot, 0, BlockTop, BH};
find_challenge_block(Byte, [{BH, BlockTop, TXRoot}, {_, BlockBase, _} | _])
	when (Byte >= BlockBase) andalso (Byte < BlockTop) -> {TXRoot, BlockBase, BlockTop, BH};
find_challenge_block(Byte, [_ | BI]) ->
	find_challenge_block(Byte, BI).

validate(BlockStartOffset, TXOffset, TXRoot, BlockSize, #poa{ chunk = Chunk } = SPoA) ->
	TXPath = SPoA#poa.tx_path,
	case ar_merkle:validate_path(TXRoot, TXOffset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			ChunkOffset = TXOffset - TXStartOffset,
			TXSize = TXEndOffset - TXStartOffset,
			DataPath = SPoA#poa.data_path,
			case ar_merkle:validate_path_strict(DataRoot, ChunkOffset, TXSize, DataPath) of
				false ->
					false;
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					ChunkSize = ChunkEndOffset - ChunkStartOffset,
					AbsoluteEndOffset = BlockStartOffset + TXStartOffset + ChunkEndOffset,
					Unpacked = unpack(aes_256_cbc, AbsoluteEndOffset, TXRoot, Chunk, ChunkSize),
					case byte_size(Unpacked) == ChunkSize of
						false ->
							false;
						true ->
							ChunkID == ar_tx:generate_chunk_id(Unpacked)
					end
			end
	end.

validate_pre_fork_2_5(BlockOffset, TXRoot, BlockEndOffset, POA) ->
	Validation =
		ar_merkle:validate_path(
			TXRoot,
			BlockOffset,
			BlockEndOffset,
			POA#poa.tx_path
		),
	case Validation of
		false -> false;
		{DataRoot, StartOffset, EndOffset} ->
			TXOffset = BlockOffset - StartOffset,
			validate_data_path(DataRoot, TXOffset, EndOffset - StartOffset, POA)
	end.

validate_data_path(DataRoot, TXOffset, EndOffset, POA) ->
	Validation =
		ar_merkle:validate_path(
			DataRoot,
			TXOffset,
			EndOffset,
			POA#poa.data_path
		),
	case Validation of
		false -> false;
		{ChunkID, _, _} ->
			validate_chunk(ChunkID, POA)
	end.

validate_chunk(ChunkID, POA) ->
	ChunkID == ar_tx:generate_chunk_id(POA#poa.chunk).

calculate_challenge_byte_pre_fork_2_4(_, 0, _) -> 0;
calculate_challenge_byte_pre_fork_2_4(LastIndepHash, WeaveSize, Option) ->
	binary:decode_unsigned(multihash(LastIndepHash, Option)) rem WeaveSize.

multihash(X, Remaining) when Remaining =< 0 -> X;
multihash(X, Remaining) ->
	multihash(crypto:hash(?HASH_ALG, X), Remaining - 1).

%%%===================================================================
%%% Tests.
%%%===================================================================

pack_test() ->
	Root = crypto:strong_rand_bytes(32),
	Cases = [
		{<<1>>, 1, Root},
		{<<1>>, 2, Root},
		{<<0>>, 1, crypto:strong_rand_bytes(32)},
		{<<0>>, 2, crypto:strong_rand_bytes(32)},
		{<<0>>, 1234234534535, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(2), 234134234, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(3), 333, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(15), 9999999999999999999999999999, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(16), 16, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(256 * 1024), 100000000000000, crypto:strong_rand_bytes(32)},
		{crypto:strong_rand_bytes(256 * 1024 - 1), 100000000000000, crypto:strong_rand_bytes(32)}
	],
	lists:foreach(
		fun({Chunk, Offset, TXRoot}) ->
			Packed = pack(aes_256_cbc, Offset, TXRoot, Chunk),
			?assertNotEqual(Packed, Chunk),
			?assertEqual(Chunk, unpack(aes_256_cbc, Offset, TXRoot, Packed, byte_size(Chunk)))
		end,
		Cases
	),
	Packed =
		[ar_poa:pack(aes_256_cbc, Offset, TXRoot, Chunk) || {Chunk, Offset, TXRoot} <- Cases],
	?assertEqual(length(Packed), sets:size(sets:from_list(Packed))).
