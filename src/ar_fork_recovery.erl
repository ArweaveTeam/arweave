-module(ar_fork_recovery).
-export([start/4]).
-include("ar.hrl").

%%% An asynchronous process that asks another node on a different fork
%%% for all of the blocks required to 'catch up' with the network,
%%% verifying each in turn. Once the blocks since divergence have been
%%% verified, the process returns the new state to its parent.

%% Defines the server state
-record(state, {
	parent,
	peer,
	target,
	blocks,
	hash_list = []
}).

%% Start the 'catch up' server.
start(Parent, Peer, TargetHeight, BlockList) ->
	spawn(
		fun() ->
			ar:report(
				[
					{started_fork_recovery_proc, self()},
					{target_height, TargetHeight},
					{current_block_height, (hd(BlockList))#block.height},
					{blocklist_length, length(BlockList)}
				]
			),
			server(
				#state {
					parent = Parent,
					peer = Peer,
					target = TargetHeight,
					blocks = BlockList,
					hash_list = []
				}
			)
		end
	).

%% Main server loop.
server(
	#state {
		parent = Parent,
		target = Target,
		blocks = Bs = [#block { height = Target }|_]
	}) ->
	Parent ! {fork_recovered, Bs};
server(S = #state { peer = Peer, blocks = Bs = [B|_] }) ->
	NextB = ar_node:get_block(Peer, B#block.height + 1),
	BHL = B#block.hash_list ++ [B#block.indep_hash],
	case ar_node:validate(
			BHL,
			ar_node:apply_txs(B#block.wallet_list, NextB#block.txs),
			NextB, B, ar_node:find_recall_block(Bs)) of
		false ->
			ok;
		true ->
			server(S#state { blocks = [NextB|Bs] })
	end.
