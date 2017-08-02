-module(ar_node).
-export([start/0, start/1, start/2]).
-export([get_blocks/1, get_balance/2]).
-export([mine/1, truncate/1, add_tx/2, add_peers/2]).
-export([apply_txs/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Blockweave maintaining nodes in the ArkChain system.

-record(state, {
	block_list,
	hash_list = [],
	wallet_list = [],
	gossip,
	txs = [],
	miner
}).

%% Start a node, optionally with a list of peers.
start() -> start([]).
start(Peers) -> start(Peers, undefined).
start(Peers, BlockList) ->
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
						end
				}
			)
		end
	).

%% Return the entire block list from a node.
get_blocks(Node) ->
	Node ! {get_blocks, self()},
	receive
		{blocks, Node, B} -> B
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

%% Cause a node to forget all but the latest block.
truncate(Node) ->
	Node ! truncate.

%% Add a transaction to the current block.
add_tx(Node, TX) ->
	Node ! {add_tx, TX},
	ok.

%% Add peer(s) to a node.
add_peers(Node, Peer) when not is_list(Peer) -> add_peers(Node, [Peer]);
add_peers(Node, Peers) ->
	Node ! {add_peers, Peers},
	ok.

%% The main node server loop.
server(S = #state { gossip = GS, block_list = Bs, hash_list = HashList, wallet_list = WalletList, txs = TXs }) ->
	receive
		Msg when is_record(Msg, gs_msg) ->
			% We have received a gossip mesage. Use the library to process it.
			case ar_gossip:recv(GS, Msg) of
				{NewGS, {new_block, NextHeight, NewB, RecallB}}
						when (hd(Bs))#block.height + 1 == NextHeight ->
					% We are being informed of a newly mined block. Is it legit?
					case validate(NewS = apply_txs(S, NewB#block.txs), NewB, hd(Bs), RecallB) of
						true ->
							% It is legit.
							% Filter completed TXs from the pending list.
							NewBlockList = [NewB|Bs],
							NewTXs =
								lists:filter(
									fun(T) -> not ar_weave:is_tx_on_block_list(NewBlockList, T#tx.id) end,
									TXs
								),
							% Recurse over the new block.
							server(
								NewS#state {
									block_list = NewBlockList,
									hash_list = HashList ++ [NewB#block.indep_hash],
									gossip = NewGS,
									txs = NewTXs
								}
							);
						false ->
							% The new block was a forgery. Discard it.
							io:format("WARN: ~p received invalid block ~p (hash: ~p).~nSHL: ~p~nBHL: ~p~n",
								[self(), NextHeight, NewB#block.hash, HashList, NewB#block.hash_list]),
							server(S#state { gossip = NewGS })
					end;
				{NewGS, {add_tx, TX}} ->
					server(S#state { gossip = NewGS, txs = [TX|TXs] });
				{NewGS, ignore} ->
					server(S#state { gossip = NewGS });
				{NewGS, X} ->
					io:format("WARN: ~p recvd unhandeled gossip message: ~p.~n",
						[self(), X]),
					server(S#state { gossip = NewGS })
			end;
		{add_tx, TX} ->
			{NewGS, _} = ar_gossip:send(GS, {add_tx, TX}),
			NewTXs = [TX|TXs],
			case S#state.miner of
				undefined -> do_nothing;
				PID ->
					ar_mine:change_data(PID, generate_data_segment(TXs, find_recall_block(Bs)))
			end,
			server(S#state { gossip = NewGS, txs = NewTXs });
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
		{get_balance, PID, PubKey} ->
			PID ! {balance, PubKey,
				case lists:keyfind(PubKey, 1, WalletList) of
					{PubKey, Balance} -> Balance;
					false -> 0
				end},
			server(S);
		mine ->
			% Start a miner process and update the state.
			case find_recall_block(Bs) of
				unavailable -> server(S);
				RecallB ->
					Miner =
						ar_mine:start(
							(hd(Bs))#block.hash,
							(hd(Bs))#block.diff,
							generate_data_segment(TXs, RecallB)
						),
					io:format("INFO: Started miner ~p...~n", [Miner]),
					server(S#state { miner = Miner })
			end;
		{work_complete, _Hash, _NewHash, _Diff, Nonce} ->
			% The miner thinks it has found a new block!
			% Build the block record, verify it, and gossip it to the other nodes.
			NextBs = ar_weave:add(Bs, HashList, WalletList, TXs, Nonce),
			case validate(NewS = apply_txs(S, TXs), hd(NextBs), hd(Bs), find_recall_block(Bs)) of
				false ->
					io:format("WARN: Miner sent us an incorrect nonce!~n"),
					server(S);
				true ->
					{NewGS, _} =
						ar_gossip:send(
							GS,
							{
								new_block,
								(hd(NextBs))#block.height,
								hd(NextBs),
								find_recall_block(Bs)
							}
						),
					server(
						NewS#state {
							miner = undefined,
							gossip = NewGS,
							block_list = NextBs,
							hash_list =
								HashList ++ [(hd(NextBs))#block.indep_hash],
							txs = []
						}
					)
			end;
		{add_peers, Ps} ->
			server(S#state { gossip = ar_gossip:add_peers(GS, Ps) });
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
			)
	end.

%% Validate a new block, given a server state, a claimed new block, the last block,
%% and the recall block.
validate(
		_S = #state { hash_list = HashList, wallet_list = WalletList },
		_NewB =
			#block {
				hash_list = HashList,
				wallet_list = WalletList,
				txs = TXs,
				nonce = Nonce
			},
		_OldB = #block { hash = Hash, diff = Diff },
		RecallB) ->
	ar_mine:validate(Hash, Diff, generate_data_segment(TXs, RecallB), Nonce) =/= false
		and ar_weave:verify_indep(RecallB, HashList);
validate(S, NewB = #block { hash_list = undefined }, OldB, RecallB) ->
	validate(S#state { hash_list = undefined }, NewB, OldB, RecallB);
validate(S, NewB = #block { wallet_list = undefined }, OldB, RecallB) ->
	validate(S#state { wallet_list = undefined }, NewB, OldB, RecallB);
validate(_S, _NewB, _, _) ->
	false.

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
			io:format("~p: Target: ~p Balance: ~p Adjustment: ~p~n", [self(), Target, Balance, Adjustment]),
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
	case lists:keyfind(ar_weave:calculate_recall_block(hd(Bs)), #block.height, Bs) of
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

%%% Tests

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
			(hd(B2))#block.height,
			(hd(B2))#block { hash = <<"INCORRECT">> }
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
			(hd(B2))#block.height,
			(hd(B2))#block { nonce = <<"INCORRECT">> }}),
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
			(hd(B2))#block.height,
			(hd(B2))#block {
				hash_list = [<<"INCORRECT HASH">>|tl((hd(B2))#block.hash_list)]
			}}),
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
	[ add_peers(Node, ar_gossip:pick_random_peers(Nodes, 100)) || Node <- Nodes ],
	add_tx(ar_gossip:pick_random(Nodes), TestData),
	receive after 2000 -> ok end,
	mine(ar_gossip:pick_random(Nodes)),
	receive after 2000 -> ok end,
	B1 = get_blocks(ar_gossip:pick_random(Nodes)),
	[TestData] = (hd(B1))#block.txs.

%% Test that large networks (500 nodes) with only 1% connectivity still function correctly.
large_weakly_connected_blockweave_with_data_test() ->
	TestData = ar_tx:new(<<"TEST DATA">>),
	B0 = ar_weave:init(),
	Nodes = [ start([], B0) || _ <- lists:seq(1, 500) ],
	[ add_peers(Node, ar_gossip:pick_random_peers(Nodes, 5)) || Node <- Nodes ],
	add_tx(ar_gossip:pick_random(Nodes), TestData),
	receive after 2000 -> ok end,
	mine(ar_gossip:pick_random(Nodes)),
	receive after 2000 -> ok end,
	B1 = get_blocks(ar_gossip:pick_random(Nodes)),
	[TestData] = (hd(B1))#block.txs.

%% Ensure that the network can add multiple peices of data and have it mined into blocks.
medium_blockweave_mine_multiple_data_test() ->
	TestData1 = ar_tx:new(<<"TEST DATA1">>),
	TestData2 = ar_tx:new(<<"TEST DATA2">>),
	B0 = ar_weave:init(),
	Nodes = [ start([], B0) || _ <- lists:seq(1, 50) ],
	[ add_peers(Node, ar_gossip:pick_random_peers(Nodes, 5)) || Node <- Nodes ],
	add_tx(ar_gossip:pick_random(Nodes), TestData1),
	add_tx(ar_gossip:pick_random(Nodes), TestData2),
	receive after 1000 -> ok end,
	mine(ar_gossip:pick_random(Nodes)),
	receive after 1000 -> ok end,
	B1 = get_blocks(ar_gossip:pick_random(Nodes)),
	true = lists:member(TestData1, (hd(B1))#block.txs),
	true = lists:member(TestData2, (hd(B1))#block.txs).

%% Ensure that the network can mine multiple blocks correctly.
medium_blockweave_multi_mine_test() ->
	TestData1 = ar_tx:new(<<"TEST DATA1">>),
	TestData2 = ar_tx:new(<<"TEST DATA2">>),
	B0 = ar_weave:init(),
	Nodes = [ start([], B0) || _ <- lists:seq(1, 50) ],
	[ add_peers(Node, ar_gossip:pick_random_peers(Nodes, 5)) || Node <- Nodes ],
	add_tx(ar_gossip:pick_random(Nodes), TestData1),
	receive after 1000 -> ok end,
	mine(ar_gossip:pick_random(Nodes)),
	receive after 1000 -> ok end,
	B1 = get_blocks(ar_gossip:pick_random(Nodes)),
	add_tx(ar_gossip:pick_random(Nodes), TestData2),
	receive after 1000 -> ok end,
	mine(ar_gossip:pick_random(Nodes)),
	receive after 1000 -> ok end,
	B2 = get_blocks(ar_gossip:pick_random(Nodes)),
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
