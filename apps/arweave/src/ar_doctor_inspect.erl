-module(ar_doctor_inspect).

-export([main/1, help/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% main/1 expects either:
%% 1. [Dir, StartStr, EndStr, Address1, Address2, ...] for traditional inspection
%% 2. ["bitmap", DataDir, StorageModule] for generating a bitmap of chunk states
main(Args) ->
	case Args of
		["bitmap", DataDir, StorageModuleConfig] ->
			bitmap(DataDir, StorageModuleConfig),
			true;
		["chunks", Dir, StartStr, EndStr | AddrListStr] when length(AddrListStr) >= 1 ->
			Addresses = [ar_util:decode(AddrStr) || AddrStr <- AddrListStr],
			arweave_config_legacy:import(#config{
				disable = [],
				enable  = [randomx_large_pages]
			}),
			ar_metrics:register(),
			ar_packing_sup:start_link(),
			Start = ar_block:get_chunk_padded_offset(list_to_integer(StartStr)),
			End = ar_block:get_chunk_padded_offset(list_to_integer(EndStr)),
			ar:console("~nInspecting chunks from padded offset ~p to ~p~n", [Start, End]),
			EncodedAddresses = [ar_util:encode(Address) || Address <- Addresses],
			ar:console("~nChecking chunks against unpacked and all addresses: ~p~n",
				[EncodedAddresses]),
			inspect_range(Dir, Start, End, Addresses),
			true;
		_ ->
			false
	end.

help() ->
	ar:console("Usage: inspect chunks <directory> <start_range> <end_range> <address1> [address2 ...]~n"),
	ar:console("       inspect bitmap <data_dir> <storage_module>~n").

%%--------------------------------------------------------------------
%% Inspect Chunks
%%--------------------------------------------------------------------

%% iterate from Padded (chunk end offset) = Start to End (inclusive)
inspect_range(_Dir, Start, End, _Addresses) when Start > End ->
	ok;
inspect_range(Dir, Start, End, Addresses) ->
	inspect_chunk(Dir, Start, Addresses),
	Next = Start + ?DATA_CHUNK_SIZE,
	inspect_range(Dir, Next, End, Addresses).

%% inspect_chunk/2 locates the chunk file and reads the local chunk,
%% then queries the remote chunk and prints their generated ids.
inspect_chunk(Dir, PaddedEndOffset, Addresses) ->
	ar:console("~n~n--- Inspecting padded offset: ~p ---~n", [PaddedEndOffset]),

	ChunkFileStart = ar_chunk_storage:get_chunk_file_start(PaddedEndOffset),
	Filepath = filename:join([Dir, integer_to_binary(ChunkFileStart)]),
	{Position, ChunkOffset} =
		ar_chunk_storage:get_position_and_relative_chunk_offset(
			ChunkFileStart, PaddedEndOffset),

	ar:console("File path: ~p~n", [Filepath]),
	ar:console("Position: ~p~n", [Position]),
	ar:console("Chunk offset: ~p~n", [ChunkOffset]),

	%% Fetch the expected chunk from arweave.net
	{ok, Proof} = fetch_remote_chunk(PaddedEndOffset),
	ExpectedChunk = maps:get(chunk, Proof),
	TXPath = maps:get(tx_path, Proof),
	{ok, TXRoot} = ar_merkle:extract_root(TXPath),
	ChunkSize = byte_size(ExpectedChunk),
	ExpectedChunkID = ar_tx:generate_chunk_id(ExpectedChunk),
	ar:console("~nExpected chunk size: ~p~n", [byte_size(ExpectedChunk)]),
	ar:console("Expected chunk ID: ~p~n", [ar_util:encode(ExpectedChunkID)]),

	%% Read local chunk from disk.
	{RawChunkOffset, RawChunk} = read_local_chunk(Filepath, Position),
	ar:console("~nRaw chunk: ~p~n", [byte_size(RawChunk)]),
	ar:console("Raw chunk offset: ~p~n", [RawChunkOffset]),
	RawChunkID = ar_tx:generate_chunk_id(RawChunk),
	ar:console("Raw chunk ID: ~p~n", [ar_util:encode(RawChunkID)]),

	%% Try unpacking the local chunk a number of different ways to see if any match the
	%% expected chunk ID.
	Result = check_all(
		ExpectedChunkID, RawChunk, PaddedEndOffset, Addresses, TXRoot, ChunkSize),
	print_match(Result).

%% New functions for checking unpacked chunks without printing per test;
%% only the first matching test is reported.

check_unpacked([], _PaddedEndOffset, _TXRoot, _LocalChunk, _ChunkSize, _ExpectedChunkID) ->
	no_match;
check_unpacked(
		[Address | Rest], PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID) ->
	case check_packings_for_address(
			Address, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID) of
		{match, Packing} ->
			{match, Packing};
		no_match ->
			check_unpacked(
				Rest, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID)
	end.

check_packings_for_address(
		Address, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID) ->
	Packings = [
		{replica_2_9, Address},
		{spora_2_6, Address},
		{composite, Address, 1},
		{composite, Address, 2}
	],
	check_packings(Packings, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID).

check_packings([], _PaddedEndOffset, _TXRoot, _LocalChunk, _ChunkSize, _ExpectedChunkID) ->
	no_match;
check_packings(
		[Packing | Rest], PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID) ->
	case check_packing(
			Packing, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID) of
		{match, _} = Match ->
			Match;
		no_match ->
			check_packings(
				Rest, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID)
	end.

check_packing(Packing, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize, ExpectedChunkID) ->
	case ar_packing_server:unpack(Packing, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize) of
		{ok, Unpacked} ->
			UnpackedID = ar_tx:generate_chunk_id(Unpacked),
			if UnpackedID =:= ExpectedChunkID ->
				{match, Packing};
			true ->
				no_match
			end;
		{error, _Reason} ->
			no_match
	end.

%% read_local_chunk/2 opens the file, reads ?OFFSET_SIZE+?DATA_CHUNK_SIZE bytes 
%% starting at Position and closes the file.
read_local_chunk(Filepath, Position) ->
	case file:open(Filepath, [read, binary, raw]) of
		{ok, F} ->
			%% Read header + chunk data.
			Length = ?OFFSET_SIZE + ?DATA_CHUNK_SIZE,
			case file:pread(F, Position, Length) of
				{ok, << ChunkOffset:?OFFSET_BIT_SIZE, Chunk:?DATA_CHUNK_SIZE/binary, Rest/binary >>} ->
					file:close(F),
					{ChunkOffset, Chunk};
				Error ->
					file:close(F),
					ar:console("Error reading file ~s at position ~p: ~p~n", [Filepath, Position, Error]),
					{0, <<>>}
			end;
		{error, Reason} ->
			ar:console("Error opening file ~s: ~p~n", [Filepath, Reason]),
			{0, <<>>}
	end.

%% fetch_remote_chunk/1 uses httpc (in inets application) to query the remote URL.
fetch_remote_chunk(PaddedOffset) ->
	%% Build URL e.g. "http://arweave.net/chunk2/123456" 
	URL = lists:concat(["https://arweave.net/chunk2/", integer_to_list(PaddedOffset)]),
	ar:console("Fetching remote chunk from ~s~n", [URL]),
	%% Ensure inets is started.
	application:ensure_all_started(inets),
	case httpc:request(get, {URL, []}, [{body_format, binary}], []) of
		{ok, {{_, 200, _}, _Headers, Body}} ->
			Bin = list_to_binary(Body),
			ar_serialize:binary_to_poa(Bin);
		{ok, Response} ->
			ar:console("Unexpected response for ~s: ~p~n", [URL, Response]),
			{error, Response};
		{error, Reason} ->
			ar:console("HTTP request error for ~s: ~p~n", [URL, Reason]),
			{error, Reason}
	end.

%% check_all/6 performs the raw, entropy, and unpacking checks sequentially
check_all(ExpectedChunkID, LocalChunk, PaddedEndOffset, Addresses, TXRoot, ChunkSize) ->
	LocalID = ar_tx:generate_chunk_id(LocalChunk),
	case LocalID =:= ExpectedChunkID of
		true ->
			{match, "Raw chunk"};
		false ->
			Entropy = ar_entropy_storage:generate_missing_entropy(
				PaddedEndOffset, hd(Addresses)),
			EntropyID = ar_tx:generate_chunk_id(Entropy),
			case EntropyID =:= ExpectedChunkID of
				true ->
					{match, "Entropy"};
				false ->
					check_unpacked(
						Addresses, PaddedEndOffset, TXRoot, LocalChunk, ChunkSize,
						ExpectedChunkID)
			end
	end.

%% print_match/1 prints the match result.
print_match({match, Type}) when is_list(Type) ->
	ar:console("~nMATCH: ~s~n", [Type]);
print_match({match, Packing}) ->
	ar:console("~nMATCH: ~p~n", [ar_serialize:encode_packing(Packing, true)]);
print_match(no_match) ->
	ar:console("~nNO MATCH~n").

%%--------------------------------------------------------------------
%% Inspect Bitmap
%%--------------------------------------------------------------------

%% @doc Generates a bitmap of the provided storage module. Each pixel is a chunk where
%% the color is determined by the packing format of the chunk. Each row of the bitmap
%% is a replica.2.9 sector (so the bitmap is 1024 rows high).
bitmap(DataDir, StorageModuleConfig) ->
	{ok, StorageModule} = ar_config:parse_storage_module(StorageModuleConfig),
	
	Config = #config{
		data_dir = DataDir,
		storage_modules = [StorageModule]},
	arweave_config_legacy:import(Config),

	StoreID = ar_storage_module:id(StorageModule),
	
	ar_kv_sup:start_link(),
	ar_storage_sup:start_link(),
	ar_sync_record_sup:start_link(),
	ar_data_sync:init_kv(StoreID),
	
	{ModuleStart, ModuleEnd} = ar_storage_module:module_range(StorageModule),

	ChunkPackings = ar_chunk_visualization:get_chunk_packings(
		ModuleStart, ModuleEnd, StoreID, true),
	ar_chunk_visualization:print_chunk_stats(ChunkPackings),
	Bitmap = ar_chunk_visualization:generate_bitmap(ChunkPackings),
	
	Filename = "bitmap_" ++ StoreID ++ ".ppm",
	file:write_file(Filename, ar_chunk_visualization:bitmap_to_binary(Bitmap)),
	ar:console("Bitmap written to ~s~n", [Filename]).
