-module(app_bulk_upload).
-export([upload_file/2, download/2]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Starts a server that accepts possibly huge files, splits them into small chunks, and
%%% submits them to Arweave as separate transactions. Transactions are submitted via
%%% the queue module.

-define(CHUNK_SIZE, (1024 * 1024)).
-define(BLOB_HASH_ALGO, sha256).

%% @doc Starts a queue server, splits the given file into chunks, wraps the chunks as 
%% Arweave transactions, and submits them to the queue. Returns the queue PID.
upload_file(Wallet, Filename) when is_list(Filename) ->
	{ok, Filecontents} = file:read_file(Filename),
	upload_blob(Wallet, Filecontents).

upload_blob(Wallet, Blob) when is_binary(Blob) ->
	upload_blob(whereis(http_entrypoint_node), Wallet, Blob).

upload_blob(Node, Wallet, Blob) ->
	Queue = app_queue:start(Node, Wallet),
	Hash = crypto:hash(?BLOB_HASH_ALGO, Blob),
	upload_chunks(Queue, Hash, Blob),
	Queue.

%% @doc Takes a binary blob and processes it chunk by chunk. Each chunk is converted into
%% a transaction and put into the queue. Chunk size is 1MB.
upload_chunks(Queue, Hash, Blob) ->
	BlobSize = byte_size(Blob),
	HasRemainder = BlobSize rem ?CHUNK_SIZE =/= 0,
	NumberOfChunks = BlobSize div ?CHUNK_SIZE + case HasRemainder of true -> 1; false -> 0 end,
	upload_chunks(Queue, Hash, Blob, NumberOfChunks, 1).

upload_chunks(Queue, Hash, Blob, NumberOfChunks, ChunkPosition) ->
	case byte_size(Blob) =< ?CHUNK_SIZE of
		true ->
			app_queue:add(Queue, chunk_to_tx(Hash, Blob, NumberOfChunks, ChunkPosition));
		false ->
			<< Chunk:?CHUNK_SIZE/binary, Rest/binary >> = Blob,
			app_queue:add(Queue, chunk_to_tx(Hash, Chunk, NumberOfChunks, ChunkPosition)),
			upload_chunks(Queue, Hash, Rest, NumberOfChunks, ChunkPosition + 1)
	end.

%% @doc Converts the given binary chunk into a transaction. A hash of the whole block the chunk
%% is part of is assigned as a tag.
chunk_to_tx(Hash, Chunk, NumberOfChunks, ChunkPosition) ->
	#tx {
		tags =
			[
				{"app_name", "BulkUpload"},
				{"blob_hash", ar_util:encode(Hash)},
				{"number_of_chunks", integer_to_binary(NumberOfChunks)},
				{"chunk_position", integer_to_binary(ChunkPosition)}
			],
		data = Chunk
	}.

%% @doc Searches the local storage for the chunks of the blob with the given hash.
%% If the blob is reconstructed successfuly, writes it to the specified destination.
%% The provided hash has to be a Base 64 encoded SHA 256 hash of the file as a binary string.
download(Hash, Filename) ->
	{ok, Blob} = download(Hash),
	file:write_file(Filename, iolist_to_binary(Blob), [write]).

download(Hash) ->
	TXIDs = app_search:get_entries(<< "blob_hash" >>, Hash),
	Transactions = lists:map(fun(TX) -> ar_storage:read_tx(TX) end, TXIDs),
	case lists:member(unavailable, Transactions) of
		true ->
			{error, invalid_storage_state};
		false ->
			{ok, Blob} = reconstruct_blob(Transactions),
			BlobHash = ar_util:encode(crypto:hash(?BLOB_HASH_ALGO, iolist_to_binary(Blob))),
			case BlobHash == Hash of
				true ->
					{ok, Blob};
				false ->
					{error, invalid_upload}
			end
	end.

reconstruct_blob([]) -> {error, not_found};
reconstruct_blob(Transactions) ->
	Head = hd(Transactions),
	{_, NumberOfChunksBinary} = lists:keyfind(<< "number_of_chunks" >>, 1, Head#tx.tags),
	NumberOfChunks = binary_to_integer(NumberOfChunksBinary),
	case length(Transactions) < NumberOfChunks of
		true ->
			{error, missing_chunks};
		false ->
			Sorted = sort_transactions_by_chunk_position(Transactions),
			SortedUnique = lists:foldr(fun(V, A) -> drop_duplicates(V, A) end, [], Sorted),
			{ok, [TX#tx.data || TX <- SortedUnique]}
	end.

sort_transactions_by_chunk_position(Transactions) ->
	SortFn = fun(First, Second) ->
		{_, FirstPositionBinary} = lists:keyfind(<< "chunk_position" >>, 1, First#tx.tags),
		{_, SecondPositionBinary} = lists:keyfind(<< "chunk_position" >>, 1, Second#tx.tags),
		FirstChunkPosition = binary_to_integer(FirstPositionBinary),
		SecondChunkPosition = binary_to_integer(SecondPositionBinary),
		FirstChunkPosition =< SecondChunkPosition
	end,
	lists:sort(SortFn, Transactions).

drop_duplicates(TX, []) -> [TX];
drop_duplicates(TX, [Head|Rest]) ->
	ChunkPosition = lists:keyfind(<< "chunk_position" >>, 1, TX#tx.tags),
	HeadPosition = lists:keyfind(<< "chunk_position" >>, 1, Head#tx.tags),
	case ChunkPosition == HeadPosition of
		true -> [Head|Rest];
		false -> [TX,Head|Rest]
	end.

upload_test_() ->
	{timeout, 60, fun() ->
		Wallet = {_, Pub} = ar_wallet:new(),
		Bs = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
		Node = ar_node:start([], Bs),
		Blob = iolist_to_binary(generate_blob(?CHUNK_SIZE * 2)),

		SearchServer = app_search:start(),
		ar_node:add_peers(Node, SearchServer),

		upload_blob(Node, Wallet, Blob),
		BHL = mine_blocks(Node, 6),
		Transactions = collect_transactions(BHL, BHL),
		?assertEqual(2, length(Transactions)),
		[First, Second] = Transactions,
		?assertEqual(Blob, << (First#tx.data)/binary, (Second#tx.data)/binary >>),
		ExpectedHash = ar_util:encode(crypto:hash(?BLOB_HASH_ALGO, Blob)),
		?assertEqual(
			[
				{<< "app_name" >>, << "BulkUpload" >>},
				{<< "blob_hash" >>, ExpectedHash},
				{<< "number_of_chunks" >>, << "2" >>},
				{<< "chunk_position" >>, << "1" >>}
			],
			First#tx.tags
		),
		?assertEqual(
			[
				{<< "app_name" >>, << "BulkUpload" >>},
				{<< "blob_hash" >>, ExpectedHash},
				{<< "number_of_chunks" >>, << "2" >>},
				{<< "chunk_position" >>, << "2" >>}
			],
			Second#tx.tags
		),
		{ok, BlobList} = download(ExpectedHash),
		?assertEqual(
			ExpectedHash,
			ar_util:encode(crypto:hash(?BLOB_HASH_ALGO, iolist_to_binary(BlobList)))
		)
	end}.

%% Generates an iolist() of the given size.
%% The first 100 bytes are randomly picked from [1; 100].
%% The rest is filled with 1. It would take too long to randomly pick every byte for a huge blob.
generate_blob(Size) ->
	generate_blob(Size, []).

generate_blob(0, Acc) ->
	Acc;
generate_blob(Size, Acc) when Size =< 100 ->
	generate_blob(Size - 1, [rand:uniform(100)|Acc]);
generate_blob(Size, _) ->
	generate_blob(100, lists:duplicate(Size - 100, 1)).

mine_blocks(_, Number) when Number =< 0 -> none;
mine_blocks(Node, Number) when Number > 0 ->
	mine_blocks(Node, Number, []).
mine_blocks(Node, Total, Mined) ->
	case Total == 0 of
		true ->
			Mined;
		false ->
			ar_node:mine(Node),
			NewMined = wait_for_blocks(Node,  length(Mined) + 1),
			mine_blocks(Node, Total - 1, NewMined)
	end.

wait_for_blocks(Node, ExpectedLength) ->
	BHL = ar_node:get_hash_list(Node),
	case length(BHL) < ExpectedLength of
		true ->
			%% A relatively big interval is used here to give app_queue some time
			%% to post transactions.
			timer:sleep(1000),
			wait_for_blocks(Node, ExpectedLength);
		false ->
			BHL
	end.

collect_transactions(_, []) ->
	[];
collect_transactions(BHL, BHLRest) ->
	[Head | Rest] = BHLRest,
	Block = ar_storage:read_block(Head, BHL),
	collect_transactions(BHL, Rest) ++ lists:map(
		fun(Hash) ->
			ar_storage:read_tx(Hash)
		end,
		Block#block.txs
	).
