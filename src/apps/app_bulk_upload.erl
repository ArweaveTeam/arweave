-module(app_bulk_upload).
-export([upload/2, upload/3, download/2]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Starts a server that accepts possibly huge files, splits them into small chunks, and
%%% submits them to Arweave as separate transactions. Transactions are submitted via
%%% the queue module.

-define(MB, 1048576). % 1024 * 1024
-define(BLOB_HASH_ALGO, sha256).

%% @doc Starts a queue server, splits the given file into chunks, wraps the chunks as 
%% Arweave transactions, and submits them to the queue. Returns the queue PID.
upload(Wallet, Filename) when is_list(Filename) ->
	{ok, Filecontents} = file:read_file(Filename),
	upload(Wallet, Filecontents);
upload(Wallet, Blob) when is_binary(Blob) ->
	upload(whereis(http_entrypoint_node), Wallet, Blob).

upload(Node, Wallet, Blob) ->
	Queue = app_queue:start(Node, Wallet),
	Hash = crypto:hash(?BLOB_HASH_ALGO, Blob),
	upload_blob(Queue, Hash, Blob),
	Queue.

%% @doc Takes a binary blob and processes it chunk by chunk. Each chunk is converted into
%% a transaction and put into the queue. Chunk size is 1MB.
upload_blob(Queue, Hash, Blob) ->
	BlobSize = byte_size(Blob),
	ChunkNumber = BlobSize div ?MB + case BlobSize rem ?MB =/= 0 of true -> 1; false -> 0 end,
	upload_blob(Queue, Hash, Blob, ChunkNumber, 1).

upload_blob(Queue, Hash, Blob, ChunkNumber, ChunkPosition) ->
	case byte_size(Blob) =< ?MB of
		true ->
			app_queue:add(Queue, chunk_to_tx(Hash, Blob, ChunkNumber, ChunkPosition));
		false ->
			<< Chunk:?MB/binary, Rest/binary >> = Blob,
			app_queue:add(Queue, chunk_to_tx(Hash, Chunk, ChunkNumber, ChunkPosition)),
			upload_blob(Queue, Hash, Rest, ChunkNumber, ChunkPosition + 1)
	end.

%% @doc Converts the given binary chunk into a transaction. A hash of the whole block the chunk
%% is part of is assigned as a tag.
chunk_to_tx(Hash, Chunk, ChunkNumber, ChunkPosition) ->
	#tx {
		tags =
			[
				{"app_name", "BulkUpload"},
				{"blob_hash", ar_util:encode(Hash)},
				{"number_of_chunks", integer_to_binary(ChunkNumber)},
				{"chunk_position", integer_to_binary(ChunkPosition)}
			],
		data = Chunk
	}.

%% @doc Searches the local storage for the chunks of the blob with the given hash.
%% If the blob is reconstructed successfuly, writes it to the specified destination.
%% The provided hash has to be a Base 64 encoded SHA 256 hash of the file as a binary string.
download(Hash, Filename) ->
	{ok, Blob} = download(Hash),
	file:write_file(Filename, Blob, [{encoding, unicode}]).

download(Hash) ->
	app_search:get_entries(<< "blob_hash" >>, Hash),
	receive TXIDs ->
		Transactions = lists:map(fun(TX) -> ar_storage:read_tx(TX) end, TXIDs),
		{ok, Blob} = reconstruct_blob(Transactions),
		BlobHash = ar_util:encode(crypto:hash(?BLOB_HASH_ALGO, Blob)),
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
	{_, ChunkNumberBinary} = lists:keyfind(<< "number_of_chunks" >>, 1, Head#tx.tags),
	ChunkNumber = binary_to_integer(ChunkNumberBinary),
	case length(Transactions) < ChunkNumber of
		true ->
			{error, missing_chunks};
		false ->
			SortedTransactions = lists:sort(fun(First, Second) ->
				{_, FirstPositionBinary} = lists:keyfind(<< "chunk_position" >>, 1, First#tx.tags),
				{_, SecondPositionBinary} = lists:keyfind(<< "chunk_position" >>, 1, Second#tx.tags),
				FirstChunkPosition = binary_to_integer(FirstPositionBinary),
				SecondChunkPosition = binary_to_integer(SecondPositionBinary),
				FirstChunkPosition =< SecondChunkPosition
			end,
			Transactions
			),
			{ok, << (TX#tx.data) || TX <- SortedTransactions >>}
	end.


upload_test_() ->
	{timeout, 60, fun() ->
		app_search:deleteDB(),

		Wallet = {_, Pub} = ar_wallet:new(),
		Bs = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
		Node = ar_node:start([], Bs),
		Blob = list_to_binary(lists:duplicate(?MB * 2, 1)),

		SearchServer = app_search:start(),
		ar_node:add_peers(Node, SearchServer),

		upload(Node, Wallet, Blob),
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
		{ok, Blob} = download(ExpectedHash),
		?assertEqual(ExpectedHash, ar_util:encode(crypto:hash(?BLOB_HASH_ALGO, Blob)))
	end}.

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
