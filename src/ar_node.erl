-module(ar_node).
-export([start/0, start/1, start/2, stop/1]).
-export([get_blocks/1, get_block/2, get_balance/2, generate_data_segment/2]).
-export([mine/1, automine/1, truncate/1, add_tx/2, add_peers/2]).
-export([set_loss_probability/2, set_delay/2, set_mining_delay/2, set_xfer_speed/2]).
-export([apply_txs/2, validate/4, validate/5, find_recall_block/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Blockweave maintaining nodes in the ArkChain system.

-record(state, {
	block_list,
	hash_list = [],
	wallet_list = [],
	gossip,
	txs = [],
	miner,
	mining_delay = 0,
	recovery = undefined,
	automine = false
}).

%% Maximum number of blocks to hold at any time.
%% %% NOTE: This value should be greater than ?RETARGET_BLOCKS + 1
%% in order for the TNT test suite to pass.
-define(MAX_BLOCKS, undefined).

%% Ensure this number of the last blocks are not dropped.
-define(KEEP_LAST_BLOCKS, 5).

%% Start a node, optionally with a list of peers.
start() -> start([]).
start(Peers) -> start(Peers, undefined).
start(Peers, BlockList) -> start(Peers, BlockList, 0).
start(Peers, BlockList, MiningDelay) ->
	spawn(
		fun() ->
			server(
				#state {
					gossip = ar_gossip:init(Peers),
					block_list = BlockList,
					wallet_list =
						case BlockList of
							undefined -> [];
							_ -> (find_sync_block(BlockList))#block.wallet_list
						end,
					mining_delay = MiningDelay
				}
			)
		end
	).

%% Stop a node (and its miner)
stop(Node) ->
	Node ! stop,
	ok.

%% Return the entire block list from a node.
get_blocks(Node) ->
	Node ! {get_blocks, self()},
	receive
		{blocks, Node, Bs} -> Bs
	after ?NET_TIMEOUT -> no_response
	end.

%% Return a specific block from a node, if it has it.
get_block(Node, Height) ->
	Node ! {get_block, self(), Height},
	receive
		{block, Node, B} -> B
	after ?NET_TIMEOUT -> no_response
	end.

%% Return the current balance associated with a wallet.
get_balance(Node, PubKey) ->
	Node ! {get_balance, self(), PubKey},
	receive
		{balance, PubKey, B} -> B
	end.

%% Trigger a node to start mining a block.
mine(Node) ->
	Node ! mine.

%% Trigger a node to mine continually.
automine(Node) ->
	Node ! automine.

%% Cause a node to forget all but the latest block.
truncate(Node) ->
	Node ! truncate.

%% Set the likelihood that a message will be dropped in transmission.
set_loss_probability(Node, Prob) ->
	Node ! {set_loss_probability, Prob}.

%% Set the max network latency delay for a node.
set_delay(Node, MaxDelay) ->
	Node ! {set_delay, MaxDelay}.

%% Set the number of milliseconds to wait between hashes.
set_mining_delay(Node, Delay) ->
	Node ! {set_mining_delay, Delay}.

%% Set the number of bytes the node can transfer in a second.
set_xfer_speed(Node, Speed) ->
	Node ! {set_xfer_speed, Speed}.

%% Add a transaction to the current block.
add_tx(GS, TX) when is_record(GS, gs_state) ->
	{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
	NewGS;
add_tx(Node, TX) ->
	Node ! {add_tx, TX},
	ok.

%% Add peer(s) to a node.
add_peers(Node, Peer) when not is_list(Peer) -> add_peers(Node, [Peer]);
add_peers(Node, Peers) ->
	Node ! {add_peers, Peers},
	ok.

%% The main node server loop.
server(
	S = #state {
		gossip = GS,
		block_list = Bs,
		wallet_list = WalletList
	}) ->
	receive
		Msg when is_record(Msg, gs_msg) ->
			% We have received a gossip mesage. Use the library to process it.
			case ar_gossip:recv(GS, Msg) of
				{NewGS, {new_block, Peer, Height, _B, _RecallB}} when Bs == undefined ->
					% We have no functional blockweave. Try to recover to this one.
					join_weave(S, NewGS, Peer, Height);
				{NewGS, {new_block, Peer, NextHeight, NewB, _RecallB}}
						when (NextHeight > (hd(Bs))#block.height + 1) ->
					% If the block is higher than we were expecting, attempt to
					% catch up.
					fork_recover(S, NewGS, Peer, NextHeight, NewB);
				{NewGS, {new_block, Peer, NextHeight, NewB, RecallB}}
						when (hd(Bs))#block.height + 1 == NextHeight ->
					% We are being informed of a newly mined block. Is it legit?
					process_new_block(S, NewGS, NewB, hd(Bs), RecallB, Peer);
				{NewGS, {new_block, _, NextHeight, _, _}}
						when (hd(Bs))#block.height + 1 >= NextHeight ->
					% The distributed block is below our height.
					server(S#state { gossip = NewGS });
				{NewGS, {add_tx, TX}} ->
					add_tx_to_server(S, NewGS, TX);
				{NewGS, ignore} ->
					server(S#state { gossip = NewGS });
				{NewGS, X} ->
					ar:report(
						[
							{node, self()},
							{unhandeled_gossip_msg, X}
						]
					),
					server(S#state { gossip = NewGS })
			end;
		{add_tx, TX} ->
			% We have a new TX. Distribute it to the
			% gossip network.
			{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
			add_tx_to_server(S, NewGS, TX);
		{replace_block_list, NewBL} ->
			% Replace the entire stored block list, regenerating the hash list.
			server(
				S#state {
					block_list = NewBL,
					hash_list = ar_weave:generate_hash_list(NewBL),
					wallet_list = (find_sync_block(NewBL))#block.wallet_list
				}
			);
		{get_blocks, PID} ->
			PID ! {blocks, self(), Bs},
			server(S);
		{get_block, PID, Height} ->
			PID ! {block, self(), find_block(Height, Bs)},
			server(S);
		{get_balance, PID, PubKey} ->
			PID ! {balance, PubKey,
				case lists:keyfind(PubKey, 1, WalletList) of
					{PubKey, Balance} -> Balance;
					false -> 0
				end},
			server(S);
		mine -> server(start_mining(S));
		automine -> server(start_mining(S#state { automine = true }));
		{work_complete, MinedTXs, _Hash, _NewHash, _Diff, Nonce} ->
			% The miner thinks it has found a new block!
			integrate_block_from_miner(S, MinedTXs, Nonce);
		{add_peers, Ps} ->
			server(S#state { gossip = ar_gossip:add_peers(GS, Ps) });
		stop ->
			case S#state.miner of
				undefined -> do_nothing;
				PID -> ar_mine:stop(PID)
			end,
			ok;
		%% TESTING FUNCTIONALITY
		truncate ->
			% Forget all bar the last block.
			server(S#state { block_list = [hd(Bs)] });
		{forget, BlockNum} ->
			server(
				S#state {
					block_list =
						Bs -- [lists:keyfind(BlockNum, #block.height, Bs)]
				}
			);
		{set_loss_probability, Prob} ->
			server(
				S#state {
					gossip = ar_gossip:set_loss_probability(S#state.gossip, Prob)
				}
			);
		{set_delay, MaxDelay} ->
			server(
				S#state {
					gossip = ar_gossip:set_delay(S#state.gossip, MaxDelay)
				}
			);
		{set_xfer_speed, Speed} ->
			server(
				S#state {
					gossip = ar_gossip:set_xfer_speed(S#state.gossip, Speed)
				}
			);
		{set_mining_delay, Delay} ->
			server(
				S#state {
					mining_delay = Delay
				}
			);
		{fork_recovered, NewBs} when Bs == undefined ->
			server(
				S#state {
					block_list = maybe_drop_blocks(NewBs),
					hash_list = ar_weave:generate_hash_list(NewBs),
					wallet_list = (find_sync_block(NewBs))#block.wallet_list
				}
			);
		{fork_recovered, [TopB|_] = NewBs}
 				when TopB#block.height > (hd(Bs))#block.height ->
			server(
				reset_miner(
					S#state {
						block_list = maybe_drop_blocks(NewBs),
						hash_list = ar_weave:generate_hash_list(NewBs),
						wallet_list = (find_sync_block(NewBs))#block.wallet_list
					}
				)
			);
		{fork_recovered, _} -> server(S);
		Msg ->
			ar:report_console([{unknown_msg, Msg}]),
			server(S)
	end.

%%% Abstracted server functionality

%% Catch up to the current height, from 0.
join_weave(S, NewGS, Peer, TargetHeight) ->
	server(
		S#state {
			gossip = NewGS,
			recovery =
				ar_fork_recovery:start(
					self(),
					Peer,
					TargetHeight,
					[]
				)
		}
	).

%% Recovery from a fork.
fork_recover(
	S = #state {
		block_list = Bs,
		hash_list = HashList
	}, Peer, Height, NewB) ->
	server(
		S#state {
			recovery =
				ar_fork_recovery:start(
					self(),
					Peer,
					Height,
					drop_blocks_to(
						divergence_height(
							HashList,
							NewB#block.hash_list
						),
						Bs
					)
				)
		}
	).
fork_recover(S, NewGS, Peer, Height, NewB) ->
	fork_recover(S#state { gossip = NewGS }, Peer, Height, NewB).

%% Validate whether a new lbock is legitimate, then handle it appropriately.
process_new_block(RawS, NewGS, NewB, OldB, RecallB, Peer) ->
	S = RawS#state { gossip = NewGS },
	case validate(NewS = apply_txs(S, NewB#block.txs), NewB, OldB, RecallB) of
		true ->
			% The block is legit. Accept it.
			integrate_new_block(NewS, NewB);
		false ->
			fork_recover(S, Peer, NewB#block.height, NewB)
	end.

%% We have received a new valid block. Update the node state accordingly.
integrate_new_block(
		S = #state {
			txs = TXs,
			block_list = Bs,
			hash_list = HashList
		},
		NewB) ->
	% Filter completed TXs from the pending list.
	NewTXs =
		lists:filter(
			fun(T) ->
				not ar_weave:is_tx_on_block_list([NewB|Bs], T#tx.id)
			end,
			TXs
		),
	% Recurse over the new block.
	server(
		reset_miner(
			S#state {
				block_list = maybe_drop_blocks([NewB|Bs]),
				hash_list = HashList ++ [NewB#block.indep_hash],
				txs = NewTXs
			}
		)
	).

%% Verify a new block found by a miner, integrate it.
integrate_block_from_miner(
		S = #state {
			hash_list = HashList,
			wallet_list = WalletList,
			block_list = Bs,
			txs = TXs,
			gossip = GS
		},
		MinedTXs, Nonce) ->
	% Build the block record, verify it, and gossip it to the other nodes.
	NextBs = ar_weave:add(Bs, HashList, WalletList, MinedTXs, Nonce),
	% Store the transactions that we know about, but were not mined in
	% this block.
	NotMinedTXs = TXs -- MinedTXs,
	NewS = apply_txs(S#state { txs = MinedTXs }, MinedTXs),
	case validate(NewS, hd(NextBs), hd(Bs), find_recall_block(Bs)) of
		false ->
			ar:report([{miner, self()}, incorrect_nonce]),
			server(S);
		true ->
			{NewGS, _} =
				ar_gossip:send(
					GS,
					{
						new_block,
						self(),
						(hd(NextBs))#block.height,
						hd(NextBs),
						find_recall_block(Bs)
					}
				),
			ar:report(
				[
					{node, self()},
					{accepted_block, (hd(NextBs))#block.height},
					{hash, (hd(NextBs))#block.hash},
					{txs, length(MinedTXs)}
				]
			),
			server(
				reset_miner(
					NewS#state {
						gossip = NewGS,
						block_list = maybe_drop_blocks(NextBs),
						hash_list =
							% TODO: Build hashlist in sensible direction
							HashList ++ [(hd(NextBs))#block.indep_hash],
						txs = NotMinedTXs % TXs not included in the block
					}
				)
			)
	end.

%% Drop blocks until length of block list = ?MAX_BLOCKS.
maybe_drop_blocks(Bs) when length(Bs) > ?MAX_BLOCKS ->
	RecallBs = calculate_prior_recall_blocks(?KEEP_LAST_BLOCKS, Bs),
	DropB = ar_util:pick_random(lists:nthtail(?KEEP_LAST_BLOCKS, Bs)),
	case lists:member(DropB, RecallBs) of
		false -> maybe_drop_blocks(Bs -- [DropB]);
		true -> maybe_drop_blocks(Bs)
	end;
	maybe_drop_blocks(Bs) -> Bs.

%% Calculates Recall blocks to be stored.
calculate_prior_recall_blocks(0, _) -> [];
calculate_prior_recall_blocks(N, Bs) ->
	[ar_weave:calculate_recall_block(Bs) |
		calculate_prior_recall_blocks(N-1, tl(Bs))].

%% Update miner and amend server state when encountering a new transaction.
add_tx_to_server(S, NewGS, TX) ->
	NewTXs = [TX|S#state.txs],
	case S#state.miner of
		undefined -> do_nothing;
		PID ->
			ar_mine:change_data(
				PID,
				generate_data_segment(
					NewTXs,
					find_recall_block(S#state.block_list)
				),
				NewTXs
			)
	end,
	server(S#state { txs = NewTXs, gossip = NewGS }).

%% Validate a new block, given a server state, a claimed new block, the last block,
%% and the recall block.
validate(_, _, _, _, unavailable) -> false;
validate(
		HashList,
		WalletList,
		NewB =
			#block {
				hash_list = HashList,
				wallet_list = WalletList,
				txs = TXs,
				nonce = Nonce
			},
		OldB = #block { hash = Hash, diff = Diff },
		RecallB) ->
	ar_mine:validate(Hash, Diff, generate_data_segment(TXs, RecallB), Nonce) =/= false
		and ar_weave:verify_indep(RecallB, HashList)
		and ar_retarget:validate(NewB, OldB);
validate(_HL, WL, NewB = #block { hash_list = undefined }, OldB, RecallB) ->
	validate(undefined, WL, NewB, OldB, RecallB);
validate(HL, _WL, NewB = #block { wallet_list = undefined }, OldB, RecallB) ->
	validate(HL, undefined, NewB, OldB, RecallB);
validate(_HL, _WL, _NewB, _, _) ->
	false.

validate(#state { hash_list = HashList, wallet_list = WalletList }, B, OldB, RecallB) ->
	validate(HashList, WalletList, B, OldB, RecallB).

%% Update the wallet list of a server with a set of new transactions
apply_txs(S, TXs) when is_record(S, state) ->
	S#state { wallet_list = apply_txs(S#state.wallet_list, TXs) };
apply_txs(WalletList, TXs) ->
	%% TODO: Sorting here probably isn't sufficient...
	lists:sort(
		lists:foldl(
			fun(TX, CurrWalletList) ->
				apply_tx(CurrWalletList, TX)
			end,
			WalletList,
			TXs
		)
	).

%% Apply a transaction to a wallet list, updating it.
apply_tx(WalletList, #tx { type = data }) -> WalletList;
apply_tx(
		WalletList,
		#tx { owner = From, target = To, quantity = Qty, type = transfer }) ->
	alter_wallet(alter_wallet(WalletList, From, -Qty), To, Qty).

%% Alter a wallet in a wallet list.
alter_wallet(WalletList, Target, Adjustment) ->
	case lists:keyfind(Target, 1, WalletList) of
		false ->
			io:format("~p: Could not find pub ~p in ~p~n", [self(), Target, WalletList]),
			[{Target, Adjustment}|WalletList];
		{Target, Balance} ->
			io:format(
				"~p: Target: ~p Balance: ~p Adjustment: ~p~n",
				[self(), Target, Balance, Adjustment]
			),
			lists:keyreplace(
				Target,
				1,
				WalletList,
				{Target, Balance + Adjustment}
			)
	end.

%% Search a block list for the next recall block.
find_recall_block([B0]) -> B0;
find_recall_block(Bs) ->
	find_block(ar_weave:calculate_recall_block(hd(Bs)), Bs).

%% Find a block from an ordered block list.
find_block(Height, Bs) ->
	case lists:keyfind(Height, #block.height, Bs) of
		false -> unavailable;
		B -> B
	end.

%% Returns the last block to include both a wallet and hash list.
find_sync_block([]) -> not_found;
find_sync_block([B = #block { hash_list = HashList, wallet_list = WalletList }|_])
		when HashList =/= undefined, WalletList =/= undefined -> B;
find_sync_block([_|Bs]) -> find_sync_block(Bs).

%% Given a recall block and a list of new transactions, generate a data segment to mine on.
generate_data_segment(TXs, RecallB) ->
	<<
		(ar_weave:generate_block_data(TXs))/binary,
		(RecallB#block.nonce)/binary,
		(RecallB#block.hash)/binary,
		(ar_weave:generate_block_data(RecallB#block.txs))/binary
	>>.

%% Find the block height at which the weaves diverged.
divergence_height([], []) -> -1;
divergence_height([], _) -> -1;
divergence_height(_, []) -> -1;
divergence_height([Hash|HL1], [Hash|HL2]) ->
	1 + divergence_height(HL1, HL2);
divergence_height([_Hash1|_HL1], [_Hash2|_HL2]) ->
	-1.

%% Kill the old miner, optionally start a new miner, depending on the automine setting.
reset_miner(S = #state { miner = undefined, automine = false }) -> S;
reset_miner(S = #state { miner = undefined, automine = true }) ->
	start_mining(S);
reset_miner(S = #state { miner = PID, automine = false }) ->
	ar_mine:stop(PID),
	S#state { miner = undefined };
reset_miner(S = #state { miner = PID, automine = true }) ->
	ar_mine:stop(PID),
	start_mining(S#state { miner = undefined }).

%% Force a node to start mining, update state.
start_mining(S = #state { block_list = Bs, txs = TXs }) ->
	case find_recall_block(Bs) of
		unavailable -> S;
		RecallB ->
			ar:report([{node_starting_miner, self()}, {recall_block, RecallB#block.height}]),
			Miner =
				ar_mine:start(
					(hd(Bs))#block.hash,
					(hd(Bs))#block.diff,
					generate_data_segment(TXs, RecallB),
					S#state.mining_delay,
					TXs
				),
			ar:report([{node, self()}, {started_miner, Miner}]),
			S#state { miner = Miner }
	end.

%% Drop blocks in a block list down to a given height.
drop_blocks_to(Height, Bs) ->
	lists:filter(fun(B) -> B#block.height =< Height end, Bs).

%%% Tests

%% Ensure that divergence heights are appropriately calculated.
divergence_height_test() ->
	2 = divergence_height([a, b, c], [a, b, c, d, e, f]),
	1 = divergence_height([a, b], [a, b, c, d, e, f]),
	2 = divergence_height([1,2,3], [1,2,3]),
	2 = divergence_height([1,2,3, a, b, c], [1,2,3]).

%% Ensure that bogus blocks are not accepted onto the network.
add_bogus_block_test() ->
	Node = start(),
	GS0 = ar_gossip:init([Node]),
	B1 = ar_weave:add(ar_weave:init(), [ar_tx:new(<<"HELLO WORLD">>)]),
	Node ! {replace_block_list, B1},
	B2 = ar_weave:add(B1, [ar_tx:new(<<"NEXT BLOCK.">>)]),
	ar_gossip:send(GS0,
		{
			new_block,
			self(),
			(hd(B2))#block.height,
			(hd(B2))#block { hash = <<"INCORRECT">> },
			find_recall_block(B2)
		}),
	receive after 500 -> ok end,
	Node ! {get_blocks, self()},
	receive {blocks, Node, RecvdB} -> B1 = RecvdB end.

%% Ensure that blocks with incorrect nonces are not accepted onto the network.
add_bogus_block_nonce_test() ->
	Node = start(),
	GS0 = ar_gossip:init([Node]),
	B1 = ar_weave:add(ar_weave:init(), [ar_tx:new(<<"HELLO WORLD">>)]),
	Node ! {replace_block_list, B1},
	B2 = ar_weave:add(B1, [ar_tx:new(<<"NEXT BLOCK.">>)]),
	ar_gossip:send(GS0,
		{new_block,
			self(),
			(hd(B2))#block.height,
			(hd(B2))#block { nonce = <<"INCORRECT">> },
			find_recall_block(B2)
		}
	),
	receive after 500 -> ok end,
	Node ! {get_blocks, self()},
	receive {blocks, Node, RecvdB} -> B1 = RecvdB end.


%% Ensure that blocks with bogus hash lists are not accepted by the network.
add_bogus_hash_list_test() ->
	Node = start(),
	GS0 = ar_gossip:init([Node]),
	B1 = ar_weave:add(ar_weave:init(), [ar_tx:new(<<"HELLO WORLD">>)]),
	Node ! {replace_block_list, B1},
	B2 = ar_weave:add(B1, [ar_tx:new(<<"NEXT BLOCK.">>)]),
	ar_gossip:send(GS0,
		{new_block,
			self(),
			(hd(B2))#block.height,
			(hd(B2))#block {
				hash_list = [<<"INCORRECT HASH">>|tl((hd(B2))#block.hash_list)]
			},
			find_recall_block(B2)
		}),
	receive after 500 -> ok end,
	Node ! {get_blocks, self()},
	receive {blocks, Node, RecvdB} -> B1 = RecvdB end.

%% Run a small, non-auto-mining blockweave. Mine blocks.
tiny_blockweave_with_mining_test() ->
	B0 = ar_weave:init(),
	Node1 = start([], B0),
	Node2 = start([Node1], B0),
	add_peers(Node1, Node2),
	mine(Node1),
	receive after 1000 -> ok end,
	B1 = get_blocks(Node2),
	1 = (hd(B1))#block.height.

%% Ensure that the network add data and have it mined into blocks.
tiny_blockweave_with_added_data_test() ->
	TestData = ar_tx:new(<<"TEST DATA">>),
	B0 = ar_weave:init(),
	Node1 = start([], B0),
	Node2 = start([Node1], B0),
	add_peers(Node1, Node2),
	add_tx(Node2, TestData),
	receive after 100 -> ok end,
	mine(Node1),
	receive after 1000 -> ok end,
	B1 = get_blocks(Node2),
	[TestData] = (hd(B1))#block.txs.

%% Test that a slightly larger network is able to receive data and propogate data and blocks.
large_blockweave_with_data_test() ->
	TestData = ar_tx:new(<<"TEST DATA">>),
	B0 = ar_weave:init(),
	Nodes = [ start([], B0) || _ <- lists:seq(1, 500) ],
	[ add_peers(Node, ar_util:pick_random(Nodes, 100)) || Node <- Nodes ],
	add_tx(ar_util:pick_random(Nodes), TestData),
	receive after 2000 -> ok end,
	mine(ar_util:pick_random(Nodes)),
	receive after 2000 -> ok end,
	B1 = get_blocks(ar_util:pick_random(Nodes)),
	[TestData] = (hd(B1))#block.txs.

%% Test that large networks (500 nodes) with only 1% connectivity still function correctly.
large_weakly_connected_blockweave_with_data_test() ->
	TestData = ar_tx:new(<<"TEST DATA">>),
	B0 = ar_weave:init(),
	Nodes = [ start([], B0) || _ <- lists:seq(1, 500) ],
	[ add_peers(Node, ar_util:pick_random(Nodes, 5)) || Node <- Nodes ],
	add_tx(ar_util:pick_random(Nodes), TestData),
	receive after 2000 -> ok end,
	mine(ar_util:pick_random(Nodes)),
	receive after 2000 -> ok end,
	B1 = get_blocks(ar_util:pick_random(Nodes)),
	[TestData] = (hd(B1))#block.txs.

%% Ensure that the network can add multiple peices of data and have it mined into blocks.
medium_blockweave_mine_multiple_data_test() ->
	TestData1 = ar_tx:new(<<"TEST DATA1">>),
	TestData2 = ar_tx:new(<<"TEST DATA2">>),
	B0 = ar_weave:init(),
	Nodes = [ start([], B0) || _ <- lists:seq(1, 50) ],
	[ add_peers(Node, ar_util:pick_random(Nodes, 5)) || Node <- Nodes ],
	add_tx(ar_util:pick_random(Nodes), TestData1),
	add_tx(ar_util:pick_random(Nodes), TestData2),
	receive after 1000 -> ok end,
	mine(ar_util:pick_random(Nodes)),
	receive after 1000 -> ok end,
	B1 = get_blocks(ar_util:pick_random(Nodes)),
	true = lists:member(TestData1, (hd(B1))#block.txs),
	true = lists:member(TestData2, (hd(B1))#block.txs).

%% Ensure that the network can mine multiple blocks correctly.
medium_blockweave_multi_mine_test() ->
	TestData1 = ar_tx:new(<<"TEST DATA1">>),
	TestData2 = ar_tx:new(<<"TEST DATA2">>),
	B0 = ar_weave:init(),
	Nodes = [ start([], B0) || _ <- lists:seq(1, 50) ],
	[ add_peers(Node, ar_util:pick_random(Nodes, 5)) || Node <- Nodes ],
	add_tx(ar_util:pick_random(Nodes), TestData1),
	receive after 1000 -> ok end,
	mine(ar_util:pick_random(Nodes)),
	receive after 1000 -> ok end,
	B1 = get_blocks(ar_util:pick_random(Nodes)),
	add_tx(ar_util:pick_random(Nodes), TestData2),
	receive after 1000 -> ok end,
	mine(ar_util:pick_random(Nodes)),
	receive after 1000 -> ok end,
	B2 = get_blocks(ar_util:pick_random(Nodes)),
	[TestData1] = (hd(B1))#block.txs,
	[TestData2] = (hd(B2))#block.txs.

%% Setup a network, mine a block, cause one node to forget that block.
%% Ensure that the 'truncated' node can still verify and accept new blocks.
tiny_collaborative_blockweave_mining_test() ->
	B0 = ar_weave:init(),
	Node1 = start([], B0),
	Node2 = start([Node1], B0),
	add_peers(Node1, Node2),
	mine(Node1), % Mine B1
	receive after 500 -> ok end,
	mine(Node1), % Mine B2
	receive after 500 -> ok end,
	truncate(Node1),
	mine(Node2), % Mine B3
	receive after 500 -> ok end,
	B3 = get_blocks(Node1),
	3 = (hd(B3))#block.height.

%% Create two new wallets and a blockweave with a wallet balance.
%% Create and verify execution of a signed exchange of value tx.
wallet_transaction_test() ->
	{Priv1, Pub1} = ar_wallet:new(),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, 9000),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	B0 = ar_weave:init([{Pub1, 10000}]),
	Node1 = start([], B0),
	Node2 = start([Node1], B0),
	add_peers(Node1, Node2),
	add_tx(Node1, SignedTX),
	mine(Node1), % Mine B1
	receive after 500 -> ok end,
	1000 = get_balance(Node2, Pub1),
	9000 = get_balance(Node2, Pub2).

%% Wallet0 -> Wallet1 | mine | Wallet1 -> Wallet2 | mine | check
wallet_two_transaction_test() ->
	{Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, 9000),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, 500),
	SignedTX2 = ar_tx:sign(TX2, Priv2, Pub2),
	B0 = ar_weave:init([{Pub1, 10000}]),
	Node1 = start([], B0),
	Node2 = start([Node1], B0),
	add_peers(Node1, Node2),
	add_tx(Node1, SignedTX),
	mine(Node1), % Mine B1
	receive after 500 -> ok end,
	add_tx(Node2, SignedTX2),
	mine(Node2), % Mine B1
	receive after 500 -> ok end,
	1000 = get_balance(Node1, Pub1),
	8500 = get_balance(Node1, Pub2),
	500 = get_balance(Node1, Pub3).
