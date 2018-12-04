-module(app_ipfs).
-export([start/3, stop/1, get_and_send/2,
	get_block_hashes/1, get_txs/1, get_ipfs_hashes/1]).
-export([confirmed_transaction/2, new_block/2]).
-include("../ar.hrl").

-record(state,{
	adt_pid,
	wallet,
	queue,
	block_hashes = [],
	ipfs_hashes = [],
	txs = []
}).

%%% api

start(Peers, Wallet, IPFSHashes) ->
	Queue = app_queue:start(Wallet),
	PidMod = spawn(fun() -> server(#state{queue=Queue, wallet=Wallet}) end),
	register(?MODULE, PidMod),
	PidADT = adt_simple:start(?MODULE, PidMod),
	lists:foreach(fun(Node) -> ar_node:add_peers(Node, [PidADT]) end, Peers),
	PidMod ! {add_adt_pid, PidADT},
	spawn(?MODULE, get_and_send, [PidMod, IPFSHashes]),
	{ok, PidMod}.

stop(Pid) ->
	Pid ! stop,
	unregister(?MODULE).

get_and_send(Pid, IPFSHashes) ->
	Q = send_and_retrieve(Pid, get_queue, queue),
	lists:foreach(fun(Hash) ->
			spawn(fun() -> maybe_get_hash_and_queue(Hash, Q) end)
		end,
		IPFSHashes).

get_block_hashes(Pid) ->
	send_and_retrieve(Pid, get_block_hashes, block_hashes).

get_ipfs_hashes(Pid) ->
	send_and_retrieve(Pid, get_ipfs_hashes, ipfs_hashes).

get_txs(Pid) ->
	send_and_retrieve(Pid, get_txs, txs).

send_and_retrieve(Pid, SendTag, RecvTag) ->
	Pid ! {SendTag, self()},
	receive
		{RecvTag, X} -> X
	end.

%%% adt_simple callbacks
%%% return the new state (i.e. always the server pid)

confirmed_transaction(Pid, TX) ->
	Pid ! {recv_new_tx, TX},
	Pid.

new_block(Pid, Block) ->
	Pid ! {recv_new_block, Block},
	Pid.

%%% local server

server(State=#state{queue=Q, block_hashes=BHs, ipfs_hashes=IHs, txs=TXs}) ->
	receive
		stop ->
			State#state.adt_pid ! stop,
			ok;
		{add_adt_pid, Pid} ->
			server(State#state{adt_pid=Pid});
		{get_block_hashes, From} ->
			From ! {block_hashes, BHs},
			server(State);
		{get_ipfs_hashes, From} ->
			From ! {ipfs_hashes, IHs},
			server(State);
		{get_txs, From} ->
			From ! {txs, TXs},
			server(State);
		{get_queue, From} ->
			From ! {queue, Q},
			server(State);
		{queue_tx, UnsignedTX} ->
			app_queue:add(Q, UnsignedTX),
			server(State);
		{recv_new_block, Block} ->
			BH = Block#block.indep_hash,
			server(State#state{block_hashes=[BH|BHs]});
		{recv_new_tx, TX=#tx{tags=Tags}} ->
			NewTXs = [TX|TXs],
			NewIHs = case first_ipfs_tag(Tags) of
				{value, {<<"IPFS-Add">>, Hash}} ->
					%% version 0.1, no validation
					{ok, Hash2} = ar_ipfs:add_data(TX#tx.data, Hash),
					[Hash2|IHs];
					%% with validation:
					%% case ar_ipfs:add_data(TX#tx.data, Hash) of
					%%	{ok, Hash} -> [Hash|IHs];
					%%	_          -> IHs
					%% end;
				false ->
					IHs
			end,
			server(State#state{txs=NewTXs, ipfs_hashes=NewIHs})
	end.

%%% private functions

first_ipfs_tag(Tags) ->
	lists:search(fun
		({<<"IPFS-Add">>,  _}) -> true;
		({<<"IPFS-Get">>,  _}) -> true;
		(_) -> false
	end,
	Tags).

maybe_get_hash_and_queue(Hash, Queue) ->
	ar:d({get_maybe, Hash}),
	Pins = ar_ipfs:pin_ls(),
	ar:d({pins, Pins}),
	case lists:member(Hash, Pins) of
		true  ->
			ar:d({got_already, Hash});
		false ->
			{ok, Data} = ar_ipfs:cat_data_by_hash(Hash),
			{ok, Hash2} = ar_ipfs:add_data(Data, Hash),
			ar:d({added, Hash, Hash2}),
			UnsignedTX = #tx{tags=[{<<"IPFS-Add">>, Hash}], data=Data},
			app_queue:add(Queue, UnsignedTX)
	end.
