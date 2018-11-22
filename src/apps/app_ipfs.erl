-module(app_ipfs).
-export([start/1, stop/1, get_block_hashes/1, get_txs/1, get_ipfs_hashes/1]).
-export([confirmed_transaction/2, new_block/2]).
-include("../ar.hrl").

-record(state,{
	adt_pid = undefined,
	block_hashes = [],
	ipfs_hashes = [],
	txs = []
}).

%%% api

start(Peers) ->
	PidMod = spawn(fun() -> server(#state{}) end),
	PidADT = adt_simple:start(?MODULE, PidMod),
	lists:foreach(fun(Node) -> ar_node:add_peers(Node, [PidADT]) end, Peers),
	PidMod ! {add_adt_pid, PidADT},
	{ok, PidMod}.

stop(Pid) ->
	Pid ! stop.

get_block_hashes(Pid) ->
	send_and_retreive(Pid, get_block_hashes, block_hashes).

get_ipfs_hashes(Pid) ->
	send_and_retreive(Pid, get_ipfs_hashes, ipfs_hashes).

get_txs(Pid) ->
	send_and_retreive(Pid, get_txs, txs).

send_and_retreive(Pid, SendTag, RecvTag) ->
	Pid ! {SendTag, self()},
	receive
		{RecvTag, Xs} -> Xs
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

server(State=#state{block_hashes=BHs, ipfs_hashes=IHs, txs=TXs}) ->
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
		{recv_new_block, Block} ->
			BH = Block#block.indep_hash,
			server(State#state{block_hashes=[BH|BHs]});
		{recv_new_tx, TX=#tx{tags=Tags}} ->
			NewTXs = [TX|TXs],
			NewIHs = case
						{lists:keyfind(<<"IPFS-Add">>, 1, Tags),
						 lists:keyfind(<<"IPFS-Hash">>, 1, Tags)} of
				{false, false} ->
					IHs;
				{{<<"IPFS-Add">>, Filename},_}  ->
					{ok, Hash} = ar_ipfs:add_data(TX#tx.data, Filename),
					[Hash|IHs];
				{false, {<<"IPFS-Hash">>, Hash}} ->
					{ok, Hash} = ar_ipfs:add_data(TX#tx.data, Hash),
					[Hash|IHs]
			end,
			server(State#state{txs=NewTXs, ipfs_hashes=NewIHs})
	end.
