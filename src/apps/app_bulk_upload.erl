-module(app_bulk_upload).
-export([upload/2, upload/3]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Starts a server that accepts possibly huge files, splits them into small chunks, and
%%% submits them to Arweave as separate transactions. Transactions are submitted via
%%% the queue module.

-define(MB, 1048576).
-define(BLOCK_HASH_ALGO, sha256).

%% @doc Starts a queue server, splits the given file into chunks, wraps the chunks as 
%% Arweave transactions, and submits them to the queue. Returns the queue PID.
upload(Wallet, Filename) ->
	upload(whereis(http_entrypoint_node), Wallet, Filename).
upload(Node, Wallet, Filename) ->
	Queue = app_queue:start(Node, Wallet),
	{ok, Filecontents} = file:read_file(Filename),
	Hash = crypto:hash(?BLOCK_HASH_ALGO, Filecontents),
	upload_blob(Queue, Hash, Filecontents),
	Queue.

%% @doc Takes a binary blob and processes it chunk by chunk. Each chunk is converted into
%% a transaction and put into the queue. Chunk size is 1MB.
upload_blob(Queue, Hash, Blob) ->
	if byte_size(Blob) =< ?MB ->
		app_queue:add(Queue, chunk_to_tx(Hash, Blob));
	true ->	
		<< Chunk:?MB/binary, Rest/binary >> = Blob,
		app_queue:add(Queue, chunk_to_tx(Hash, Chunk)),
		upload_blob(Queue, Hash, Rest)
	end.

%% @doc Converts the given binary chunk into a transaction. A hash of the whole block the chunk
%% is part of is assigned as a tag.
chunk_to_tx(Hash, Chunk) ->
	#tx {
		tags =
			[
				{"app_name", "BulkUpload"},
				{"blob_hash", Hash}
			],
		data = Chunk
	}.

upload_test_() ->
	{timeout, 60, fun() ->
		Wallet = {_, Pub} = ar_wallet:new(),
		Bs = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
		Node = ar_node:start([], Bs),
		Filename = filename:join(os:getenv("TMPDIR", "tmp-test-dir"), "tmp-bulk-upload-file"),
		Contents = list_to_binary(lists:duplicate(?MB * 2, 1)),
		ok = file:write_file(Filename, Contents, [write]),
		upload(Node, Wallet, Filename),
		receive after 1000 -> ok end,
		lists:foreach(
			fun(_) ->
				ar_node:mine(Node),
				receive after 500 -> ok end
			end,
			lists:seq(1, 6)
		),
		BHL = ar_node:get_hash_list(Node),
		Transactions = collect_transactions(BHL, BHL),
		?assertEqual(2, length(Transactions)),
		[First, Second] = Transactions,
		?assertEqual(Contents, << (First#tx.data)/binary, (Second#tx.data)/binary >>)
	end}.

collect_transactions(_, []) ->
	[];
collect_transactions(BHL, BHLRest) ->
	[Head | Rest] = BHLRest,
	Block = ar_storage:read_block(Head, BHL),
	lists:map(
		fun(Hash) ->
			ar_storage:read_tx(Hash)
		end,
		Block#block.txs
	) ++ collect_transactions(BHL, Rest).
