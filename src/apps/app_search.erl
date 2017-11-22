-module(app_search).
-export([start/0, start/1, find_block/1, find_block/2]).
-export([new_block/2, message/2]).
-export([build_list/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A search node for the Archain network. Given a transaction hash,
%%% returns to the requesting node the independant block hash of the
%%% block containing the transaction.
%%% For examplary purposes only.

%% txlist DETS file  
-define(TXDATA, "data/txdb.dat").
%% @doc For compatibility. Dets database supercedes state.
-record(state, {
	db = [] % Stores the 'database' of links to chirps.
}).


%% @doc Start a search node.
start() -> start([]).
start(Peers) ->
	filelib:ensure_dir(?TXDATA),
	adt_simple:start(?MODULE, #state{}, Peers).

%% @doc Find the block associated with a transaction.
find_block(TXID) ->
	find_block(whereis(http_search_node), TXID).
find_block(PID, Str) when is_list(Str) ->
	find_block(PID, ar_util:decode(Str));
find_block(PID, TXID) ->
	PID ! {get_tx, self(), TXID},
	receive
		{found_block, IndepHash} ->
			IndepHash;
		not_found ->
			not_found
	end.

%% @doc Add the transactions in a newly recieved block to the trasction db
%% S - current state, B - new block
new_block(S, B) ->
	{ok, Ref} = dets:open_file(?TXDATA,[]),
	{ok, Ref} = insert_transactions(Ref, B#block.txs, B#block.indep_hash),
	dets:close(Ref),
	S.

%% @doc Listen for get_transaction requests, send independant block hash
%% Back to requesting process.
%% S - current state, {get_transaction - atom, T - transaction hash,
%% Process_id - id of the process to return the block hash to}
message(S, {get_tx, PID, T}) ->
	{ok, Ref} = dets:open_file(?TXDATA,[]),
	case dets:member(Ref, T) of
		true ->
			[{_, IndepHash}] = dets:lookup(Ref, T),
			PID ! {found_block, IndepHash};
		false ->
			PID ! not_found
	end,
	dets:close(Ref),
	S;
message(S, _) ->
	S.

%% @doc Add to the dets database all transactions from currently held blocks
build_list() ->
	{ok, Filenames} = file:list_dir("blocks"),
	lists:foreach(
		fun(Filename) -> 
			{ok, Binary} = file:read_file("blocks/" ++ Filename),
			Block = ar_serialize:json_struct_to_block(binary_to_list(Binary)),
			new_block(new, Block)
		end,
		Filenames
	).

%% @doc Insert transactions into the dets database
%% Ref - Ref to dets DB, [T - Transaction hash, Txlist - List of transactions],
%% Bhash - Independant block hash
insert_transactions(Ref, [], _) -> 
	{ok, Ref};
insert_transactions(Ref, [T|Txlist], Bhash) ->
	ar:report_console(
		[
			{adding_tx, ar_util:encode(T#tx.id)},
			{tx_size, byte_size(T#tx.data)}
		]
	),
	dets:insert(Ref, {T#tx.id, Bhash}),
	insert_transactions(Ref, Txlist, Bhash).

%% @doc Test that a new tx placed on the network and mined can be searched for
basic_usage_test() ->
	% Spawn a network with two nodes and a chirper server
	ar_storage:clear(),
	SearchServer = start(),
	Peers = ar_network:start(10, 10),
	ar_node:add_peers(hd(Peers), SearchServer),
	% Create the transaction, send it.
	{Priv, Pub} = ar_wallet:new(),
	% Generate the transaction.
	TX = ar_tx:new(<<"Hello">>),
	% Sign the TX with the public and private key.
	SignedTX = ar_tx:sign(TX, Priv, Pub),
	% Add tx to network
	ar_node:add_tx(hd(Peers), SignedTX),
	% Begin mining
	receive after 250 -> ok end,
	ar_node:mine(hd(Peers)),
	receive after 1000 -> ok end,
	{ok,Ref} = dets:open_file(?TXDATA,[]),
	% recieve a "get transaction" message
	message([], {get_tx,self(),TX#tx.id}),
	% check that newly mined block matches the block the most recent transaction was mined in 
	receive
		{found_block, X} ->
			X = hd(ar_node:get_blocks(hd(Peers)))
	end,
	dets:close(Ref).
	
