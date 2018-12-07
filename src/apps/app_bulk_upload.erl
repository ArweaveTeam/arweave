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
	Queue = app_queue:start(Node, Wallet, <<"previous_chunk">>),
	upload_chunks(Queue, Blob),
	Queue.

%% @doc Takes a binary blob and processes it chunk by chunk. Each chunk is converted into
%% a transaction and put into the queue. Chunk size is 1MB.
upload_chunks(Queue, Blob) ->
	case byte_size(Blob) =< ?CHUNK_SIZE of
		true ->
			app_queue:add(Queue, chunk_to_tx(Blob));
		false ->
			<<Chunk:?CHUNK_SIZE/binary, Rest/binary>> = Blob,
			app_queue:add(Queue, chunk_to_tx(Chunk)),
			upload_chunks(Queue, Rest)
	end.

%% @doc Converts the given binary chunk into a transaction.
chunk_to_tx(Chunk) ->
	#tx {
		tags = [
			{<<"app_name">>, <<"BulkUpload">>}
		],
		data = Chunk
	}.

%% @doc Searches the local storage for the chunks of the blob identified by the given
%% Base 64 encoded transaction ID. If the blob is reconstructed successfully,
%% writes it to the specified destination.
download(TXID, Filename) ->
	{ok, Blob} = download(TXID),
	file:write_file(Filename, Blob, [write]).

download(TXID) ->
	download_chunks(TXID, []).

download_chunks(TXID, Chunks) ->
	case ar_storage:read_tx(ar_util:decode(TXID)) of
		unavailable ->
			{error, tx_not_found};
		TX ->
			AppNameTag = lists:keyfind(<<"app_name">>, 1, TX#tx.tags),
			PreviousTXTag = lists:keyfind(<<"previous_chunk">>, 1, TX#tx.tags),
			case {AppNameTag, PreviousTXTag} of
				{{_, <<"BulkUpload">>}, false} ->
					{ok, [TX#tx.data|Chunks]};
				{{_, <<"BulkUpload">>}, {_, PreviousTXID}} ->
					download_chunks(PreviousTXID, [TX#tx.data|Chunks]);
				_ ->
					{error, non_bulk_upload_tx}
			end
	end.

upload_one_chunk_test_() ->
	{
		timeout, 60, fun() ->
			Blob = iolist_to_binary(generate_blob(?CHUNK_SIZE - 1)),
			upload_blob_test_with_blob(Blob),
			upload_blob_test_with_blob(Blob) %% upload the same blob again
		end
	}.

upload_one_even_chunk_test_() ->
	{
		timeout, 60, fun() ->
			Blob = iolist_to_binary(generate_blob(?CHUNK_SIZE)),
			upload_blob_test_with_blob(Blob)
		end
	}.

upload_two_even_chunks_test_() ->
	{
		timeout, 60, fun() ->
			Blob = iolist_to_binary(generate_blob(?CHUNK_SIZE * 2)),
			upload_blob_test_with_blob(Blob)
		end
	}.

upload_three_chunks_test_() ->
	{
		timeout, 60, fun() ->
			Blob = iolist_to_binary(generate_blob(?CHUNK_SIZE * 2 + 1)),
			upload_blob_test_with_blob(Blob)
		end
	}.

upload_blob_test_with_blob(Blob) ->
	Wallet = {_, Pub} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	Node = ar_node:start([], Bs),

	upload_blob(Node, Wallet, Blob),

	BlobSize = byte_size(Blob),
	BlobSizeByChunkSize = BlobSize div ?CHUNK_SIZE,
	Remainder = case BlobSize rem ?CHUNK_SIZE of 0 -> 0; _ -> 1 end,
	ExpectedTXNumber = BlobSizeByChunkSize + Remainder,

	mine_blocks(Node, ExpectedTXNumber * 3),
	Transactions = collect_transactions(Node, Wallet),
	?assertEqual(ExpectedTXNumber, length(Transactions)),
	?assertEqual(hash(Blob), hash(<<(TX#tx.data) || TX <- Transactions>>)),
	assert_transactions(Transactions),
	LastTX = lists:last(Transactions),
	{ok, DownloadedChunks} = download(ar_util:encode(LastTX#tx.id)),
	?assertEqual(hash(Blob), hash(iolist_to_binary(DownloadedChunks))).

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
	case lists:keyfind(<<"previous_chunk">>, 1, TX#tx.tags) of
		false ->
			[TX | Transactions];
		_ ->
			PreviousTX = ar_storage:read_tx(TX#tx.last_tx),
			collect_chunk_transactions(PreviousTX, [TX | Transactions])
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

assert_transactions(Transactions) ->
	[First|Rest] = Transactions,
	?assertEqual(
		[
			{<<"app_name">>, <<"BulkUpload">>}
		],
		First#tx.tags
	),
	assert_transactions(First, Rest).

assert_transactions(_, []) -> ok;
assert_transactions(PreviousTX, [TX|_]) ->
	?assertEqual(
		[
			{<<"app_name">>, <<"BulkUpload">>},
			{<<"previous_chunk">>, ar_util:encode(PreviousTX#tx.id)}
		],
		TX#tx.tags
	).
