-module(app_ipfs).
-export([start/1, stop/1, get_block_hashes/1]).
-export([confirmed_transaction/2, new_block/2]).
-include("../ar.hrl").

-record(state,{
	adt_pid = undefined,
	block_hashes = []
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
	Pid ! {get_block_hashes, self()},
	receive
		{block_hashes, BHs} -> BHs
	end.

%%% adt_simple callbacks
%%% return the new state (i.e. always the server pid)

confirmed_transaction(Pid, TX) ->
	Pid.

new_block(Pid, Block) ->
	Pid ! {recv_new_block, Block},
	Pid.

%%% local server

server(State) ->
	receive
		stop ->
			State#state.adt_pid ! stop,
			ok;
		{add_adt_pid, Pid} ->
			server(State#state{adt_pid=Pid});
		{get_block_hashes, From} ->
			From ! {block_hashes, State#state.block_hashes},
			server(State);
		{recv_new_block, Block} ->
			BH = Block#block.indep_hash,
			server(State#state{block_hashes=[BH|State#state.block_hashes]})
	end.
