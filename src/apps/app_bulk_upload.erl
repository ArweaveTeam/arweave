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
upload_file(Wallet, Filename) ->
	{ok, Filecontents} = file:read_file(Filename),
	upload_blob(Wallet, Filecontents).

upload_blob(Wallet, Blob) when is_binary(Blob) ->
	upload_blob(whereis(http_entrypoint_node), Wallet, Blob).

upload_blob(Node, Wallet, Blob) ->
	Queue = app_queue:start(Node, Wallet),
	upload_chunks(Queue, Blob),
	Queue.

%% @doc Takes a binary blob and processes it chunk by chunk. Each chunk is converted into
%% a transaction and put into the queue. Chunk size is 1MB.
upload_chunks(Queue, Blob) ->
	upload_chunks(Queue, Blob, true).

upload_chunks(Queue, Blob, IsFirstChunk) ->
	case byte_size(Blob) =< ?CHUNK_SIZE of
		true ->
			app_queue:add(Queue, chunk_to_tx(Blob, IsFirstChunk));
		false ->
			<< Chunk:?CHUNK_SIZE/binary, Rest/binary >> = Blob,
			app_queue:add(Queue, chunk_to_tx(Chunk, IsFirstChunk)),
			upload_chunks(Queue, Rest, false)
	end.

%% @doc Converts the given binary chunk into a transaction.
%% The first chunk is tagged as such so that we know where to stop when the blob is downloaded.
chunk_to_tx(Chunk, IsFirstChunk) ->
	#tx {
		tags = chunk_tags(IsFirstChunk),
		data = Chunk
	}.

chunk_tags(false) ->
	[
		{<< "app_name" >>, << "BulkUpload" >>}
	];
chunk_tags(true) ->
	chunk_tags(false) ++ [{<< "first_chunk" >>, << "true" >>}].

%% @doc Searches the local storage for the chunks of the blob identified by the given
%% Base 64 encoded transaction ID. If the blob is reconstructed successfully,
%% writes it to the specified destination.
download(TXID, Filename) ->
	{ok, Blob} = download(TXID),
	file:write_file(Filename, Blob, [write]).

download(TXID) ->
	case download_chunks(TXID, []) of
		Chunks when is_list(Chunks) ->
			{ok, Chunks};
		Err ->
			{error, Err}
	end.

download_chunks(TXID, Chunks) ->
	case ar_storage:read_tx(ar_util:decode(TXID)) of
		unavailable ->
			tx_not_found;
		TX ->
			case lists:keyfind(<< "app_name" >>, 1, TX#tx.tags) of
				false ->
					invalid_tx;
				_ ->
					case lists:keyfind(<< "first_chunk" >>, 1, TX#tx.tags) of
						false ->
							download_chunks(ar_util:encode(TX#tx.last_tx), [TX#tx.data|Chunks]);
						_ ->
							[TX#tx.data|Chunks]
					end
			end
	end.

upload_test_() ->
	{timeout, 60, fun() ->
		Wallet = {_, Pub} = ar_wallet:new(),
		Bs = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
		Node = ar_node:start([], Bs),
		Blob = iolist_to_binary(generate_blob(?CHUNK_SIZE * 2)),

		upload_blob(Node, Wallet, Blob),
		mine_blocks(Node, 6),
		Transactions = collect_transactions(Node, Wallet),
		?assertEqual(
			hash(Blob),
			hash(<< (TX#tx.data) || TX <- Transactions >>)
		),
		[First,Second|_] = Transactions,
		?assertEqual(
			[
				{<< "app_name" >>, << "BulkUpload" >>},
				{<< "first_chunk" >>, << "true" >>}
			],
			First#tx.tags
		),
		?assertEqual(
			[
				{<< "app_name" >>, << "BulkUpload" >>}
			],
			Second#tx.tags
		),
		LastTX = lists:last(Transactions),
		{ok, DownloadedChunks} = download(ar_util:encode(LastTX#tx.id)),
		?assertEqual(hash(Blob), hash(iolist_to_binary(DownloadedChunks)))
	end}.

hash(Blob) ->
	ar_util:encode(crypto:hash(?BLOB_HASH_ALGO, Blob)).

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

collect_transactions(Node, Wallet) ->
	Addr = ar_wallet:to_address(Wallet),
	TXID = ar_node:get_last_tx(Node, Addr),
	TX = ar_storage:read_tx(TXID),
	collect_chunk_transactions(TX, []).

collect_chunk_transactions(TX, Transactions) ->
	case lists:keyfind(<< "first_chunk" >>, 1, TX#tx.tags) of
		false ->
			PreviousTX = ar_storage:read_tx(TX#tx.last_tx),
			collect_chunk_transactions(PreviousTX, [TX|Transactions]);
		_ ->
			[TX|Transactions]
	end.

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
