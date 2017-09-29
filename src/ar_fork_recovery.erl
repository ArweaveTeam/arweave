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
	ar:report_console(
		[
			{fork_recovered, self()},
			{parent, Parent}
		]
	),
	Parent ! {fork_recovered, Bs};
server(S = #state { peer = Peer, blocks = Bs = [B|_], target = Target }) ->
	NextB = ar_node:get_block(Peer, B#block.height + 1),
	BHL = B#block.hash_list ++ [B#block.indep_hash],
	ar:report(
		[
			{got_block, NextB#block.height},
			{bl_height, B#block.height},
			{target, Target},
			{current_bhl, BHL},
			{next_bhl, NextB#block.hash_list},
			{next_hash, NextB#block.indep_hash},
			{current_wl, ar_node:apply_txs(B#block.wallet_list, NextB#block.txs)},
			{next_wl, NextB#block.wallet_list},
			{recall_block, (ar_node:find_recall_block(Bs))#block.hash},
			{ar_mine_validate,
				ar_mine:validate(
					B#block.hash,
					B#block.diff,
					ar_node:generate_data_segment(
						NextB#block.txs,
						ar_node:find_recall_block(Bs)),
					NextB#block.nonce
				)
			},
			{ar_weave_verify_indep,
				ar_weave:verify_indep(ar_node:find_recall_block(Bs), BHL)
			}
		]
	),
	case ar_node:validate(
			BHL,
			ar_node:apply_txs(B#block.wallet_list, NextB#block.txs),
			NextB, B, ar_node:find_recall_block(Bs)) of
		false ->
			ok;
		true ->
			ar:d(success),
			server(S#state { blocks = [NextB|Bs] })
	end.
