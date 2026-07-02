%%% @doc Higher-level integration tests for the ETS-based ar_account_tree running inside a real
%%% node. These drive the consensus path (apply_block/set_current via mining and fork
%%% recovery) that the impl-to-impl tests do not, and assert the distinctive account-tree
%%% behaviors: historical (non-tip) balance queries reconstructed from the diff DAG, and
%%% correct balances after a reorg replaces the tip.
-module(ar_account_tree_fork_tests).
-test_peers([peer1]).

-include("ar.hrl").

-include_lib("eunit/include/eunit.hrl").

historical_root_queries_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun historical_root_queries/0}.

balances_after_fork_recovery_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun balances_after_fork_recovery/0}.

%% @doc Mine a couple of blocks moving funds around, then query the wallet tree of each past
%% block by its root hash. Non-tip roots are served by overlaying the diff DAG on the current
%% ETS tip, so this pins that path against blocks produced by real apply_block/set_current.
%%
%%   B0                 B1                      B2  <- tip
%%   Pub1=100AR  --->   Pub1->Pub2 (10AR)  ---> Pub1->Pub3 (20AR)
%%
%%   queried at each block's wallet_list root:
%%     B0: Pub1=100, Pub2=0,  Pub3=0
%%     B1: Pub2=10,  Pub3=0
%%     B2: Pub2=10,  Pub3=20   (== tip)
historical_root_queries() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Addr1 = ar_wallet:to_address(Pub1),
	{_, Pub2} = ar_wallet:new(),
	Addr2 = ar_wallet:to_address(Pub2),
	{_, Pub3} = ar_wallet:new(),
	Addr3 = ar_wallet:to_address(Pub3),
	[B0] = ar_weave:init([{Addr1, ?AR(100), <<>>}]),
	_ = ar_test_node:start(B0),
	%% Height 1: Pub1 -> Pub2, 10 AR.
	TX1 = ar_test_node:sign_tx(main, Key1, #{ target => Addr2, quantity => ?AR(10),
			last_tx => ar_test_node:get_tx_anchor(main) }),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	ar_test_node:mine(),
	{ok, _} = ar_test_await:node_height(main, 1),
	%% Height 2: Pub1 -> Pub3, 20 AR.
	TX2 = ar_test_node:sign_tx(main, Key1, #{ target => Addr3, quantity => ?AR(20),
			last_tx => ar_test_node:get_tx_anchor(main) }),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	ar_test_node:mine(),
	{ok, BI} = ar_test_await:node_height(main, 2),
	B1 = block_at(BI, 1),
	B2 = block_at(BI, 2),
	%% Tip (height 2) balances.
	?assertEqual(?AR(10), ar_node:get_balance(Pub2)),
	?assertEqual(?AR(20), ar_node:get_balance(Pub3)),
	%% Genesis root: only Addr1 is funded; the later recipients are absent.
	?assertEqual(?AR(100), ar_account_tree:get_balance(B0#block.wallet_list, Addr1)),
	?assertEqual(0, ar_account_tree:get_balance(B0#block.wallet_list, Addr2)),
	?assertEqual(0, ar_account_tree:get_balance(B0#block.wallet_list, Addr3)),
	%% Height-1 root: Pub2 has been paid, Pub3 has not.
	?assertEqual(?AR(10), ar_account_tree:get_balance(B1#block.wallet_list, Addr2)),
	?assertEqual(0, ar_account_tree:get_balance(B1#block.wallet_list, Addr3)),
	%% Height-2 root equals the tip.
	?assertEqual(?AR(10), ar_account_tree:get_balance(B2#block.wallet_list, Addr2)),
	?assertEqual(?AR(20), ar_account_tree:get_balance(B2#block.wallet_list, Addr3)),
	%% The map form agrees: Pub3 only appears from height 2 onwards.
	Map1 = ar_account_tree:get(B1#block.wallet_list, [Addr2, Addr3]),
	?assert(maps:is_key(Addr2, Map1)),
	?assertNot(maps:is_key(Addr3, Map1)),
	Map2 = ar_account_tree:get(B2#block.wallet_list, [Addr2, Addr3]),
	?assert(maps:is_key(Addr2, Map2)),
	?assert(maps:is_key(Addr3, Map2)).

%% @doc Diverge main and peer1 after a shared prefix, mine a longer chain on peer1, then let
%% main fork-recover onto it. The transaction on the orphaned tip must vanish and the winning
%% chain's transaction must be reflected - on both nodes, at the tip and at the historical
%% root that carried it.
%%
%%                              B2  (main)   Pub1->Pub2 (10AR)   [orphaned]
%%                             /
%%   B0 ------- B1 (shared) ---
%%   Pub1=100AR               \
%%                              B2' ------- B3'  (peer1)         [wins, longer]
%%                              Pub1->Pub3 (20AR)
%%
%%   On reconnect main fork-recovers onto peer1's chain (tip = B3', height 3). Afterwards, on
%%   both nodes: Pub2=0 (orphaned payment gone), Pub3=20 (winning payment in effect). The
%%   recovered B2' root still reflects Pub3=20, Pub2=0.
balances_after_fork_recovery() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Addr1 = ar_wallet:to_address(Pub1),
	{_, Pub2} = ar_wallet:new(),
	Addr2 = ar_wallet:to_address(Pub2),
	{_, Pub3} = ar_wallet:new(),
	Addr3 = ar_wallet:to_address(Pub3),
	[B0] = ar_weave:init([{Addr1, ?AR(100), <<>>}]),
	_ = ar_test_node:start(B0),
	_ = ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	%% Shared prefix at height 1.
	ar_test_node:mine(peer1),
	{ok, _} = ar_test_await:node_height(peer1, 1),
	{ok, _} = ar_test_await:node_height(main, 1),
	ar_test_node:disconnect_from(peer1),
	%% main mines a block paying Pub2; this block will be orphaned.
	OrphanTX = ar_test_node:sign_tx(main, Key1, #{ target => Addr2, quantity => ?AR(10),
			last_tx => ar_test_node:get_tx_anchor(main) }),
	ar_test_node:assert_post_tx_to_peer(main, OrphanTX),
	ar_test_node:mine(),
	{ok, _} = ar_test_await:node_height(main, 2),
	?assertEqual(?AR(10), ar_node:get_balance(Pub2)),
	%% peer1 builds a longer chain paying Pub3 instead.
	WinTX = ar_test_node:sign_tx(Key1, #{ target => Addr3, quantity => ?AR(20),
			last_tx => ar_test_node:get_tx_anchor(peer1) }),
	ar_test_node:assert_post_tx_to_peer(peer1, WinTX),
	ar_test_node:mine(peer1),
	{ok, _} = ar_test_await:node_height(peer1, 2),
	ar_test_node:mine(peer1),
	{ok, _} = ar_test_await:node_height(peer1, 3),
	%% Reconnect; main fork-recovers onto peer1's chain (height 3).
	ar_test_node:connect_to_peer(peer1),
	{ok, BI} = ar_test_await:node_height(main, 3),
	%% After recovery the orphaned payment is gone and the winning one is in effect, on both
	%% nodes.
	?assertEqual(0, ar_node:get_balance(Pub2)),
	?assertEqual(?AR(20), ar_node:get_balance(Pub3)),
	?assertEqual(0, ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub2])),
	?assertEqual(?AR(20), ar_test_node:remote_call(peer1, ar_node, get_balance, [Pub3])),
	?assertEqual(?AR(100), ar_account_tree:get_balance(B0#block.wallet_list, Addr1)),
	%% The recovered height-2 block carried the winning transaction; its root reflects it.
	B2 = block_at(BI, 2),
	?assertEqual(?AR(20), ar_account_tree:get_balance(B2#block.wallet_list, Addr3)),
	?assertEqual(0, ar_account_tree:get_balance(B2#block.wallet_list, Addr2)).

%% @doc Fetch the block at the given height from a tip-first block index.
block_at(BI, Height) ->
	{BH, _, _} = lists:nth(length(BI) - Height, BI),
	ar_test_await:block_stored(BH).
