%%% @doc Integration tests for ar_account_tree running inside a real node. These drive the
%%% consensus path - apply_block and set_current, via mining and fork recovery - which
%%% ar_account_tree_impl_tests does not, and assert the behaviors specific to the account
%%% tree: historical (non-tip) balance queries reconstructed from the diff DAG, and correct
%%% balances after a reorg replaces the tip.
-module(ar_account_tree_fork_tests).
-test_peers([peer1]).

-include("ar.hrl").

-include_lib("eunit/include/eunit.hrl").

historical_root_queries_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun historical_root_queries/0}.

balances_after_fork_recovery_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun balances_after_fork_recovery/0}.

apply_block_failure_leaves_tip_consistent_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun apply_block_failure_leaves_tip_consistent/0}.

boot_from_disk_preserves_tree_test_() ->
	{timeout, ?TEST_NODE_TIMEOUT, fun boot_from_disk_preserves_tree/0}.

%% @doc Mine two blocks moving funds around, then query the account tree of each past block
%% by its root hash. Non-tip roots are served by combining the per-block diffs from the
%% diff DAG with the current ETS tip, so this exercises that path against blocks produced
%% by real apply_block and set_current calls.
%%
%%   B0                 B1                      B2  <- tip
%%   Pub1=100AR  --->   Pub1->Pub2 (10AR)  ---> Pub1->Pub3 (20AR)
%%
%%   queried at each block's wallet_list root:
%%     B0: Pub1=100, Pub2=0,  Pub3=0
%%     B1: Pub2=10,  Pub3=0
%%     B2: Pub2=10,  Pub3=20   (the tip)
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

%% @doc Diverge main and peer1 after a shared block, mine a longer chain on peer1, then let
%% main fork-recover onto it. The payment on the orphaned branch must vanish and the
%% winning chain's payment must be in effect - on both nodes, at the tip and at the
%% historical root that carried it.
%%
%%                              B2  (main)   Pub1->Pub2 (10AR)   [orphaned]
%%                             /
%%   B0 ------- B1 (shared) ---
%%   Pub1=100AR               \
%%                              B2' ------- B3'  (peer1)         [wins, longer]
%%                              Pub1->Pub3 (20AR)
%%
%%   On reconnect main fork-recovers onto peer1's chain, making B3' the tip. Afterwards,
%%   on both nodes: Pub2=0 (orphaned payment gone), Pub3=20 (winning payment in effect).
%%   The recovered B2' root still reflects Pub3=20, Pub2=0.
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

%% @doc A rejected apply_block must not corrupt the tip. Mine two funds-moving blocks, then
%% feed apply_block two invalid candidates off the same parent (the height-1 block). The
%% first has a bumped denomination and is rejected (invalid_denomination) before the ETS
%% tree is touched at all. The second has a tampered wallet_list and is rejected
%% (invalid_wallet_list) only after apply_block has positioned the ETS tree at the parent,
%% applied the candidate diff in place, and hashed the result - so this candidate really
%% writes into the tip tree and relies on snapshot_restore to roll it back.
%%
%% The tampered candidate carries the real height-2 transaction, so the applied and rolled
%% back diff touches several accounts (the sender, the recipient, and the reward address),
%% not just the reward address an empty block would touch.
%%
%% After both rejections the tip size and balances are unchanged, historical and tip roots
%% still resolve, and the tip ETS tree hashes to the same root as a tree rebuilt from
%% scratch. A valid follow-up block then advances the tip, and its tree likewise matches a
%% from-scratch rebuild.
%%
%% The candidate and parent blocks come from the block cache
%% (ar_node:get_block_shadow_from_cache/1), not disk: a cached block keeps the
%% reward_history that ar_account_tree needs on the parent block, whereas a block read back
%% from storage has it stripped. The cache carries transactions as identifiers, so the
%% candidate's txs are restored to the full records ar_node_utils:apply_txs/3 needs.
apply_block_failure_leaves_tip_consistent() ->
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
	B1 = cached_block_at(BI, 1),
	B2 = cached_block_at(BI, 2),
	%% The candidate is the real height-2 block with its transaction restored to the full record.
	Candidate = B2#block{ txs = [TX2] },
	TipRoot = B2#block.wallet_list,
	Size0 = ar_account_tree:get_size(),
	TableBefore = table_dump(),
	%% Rejected before touching the tree: the denomination does not match the parent's.
	BadDenominationB = Candidate#block{ denomination = Candidate#block.denomination + 1 },
	?assertEqual({error, invalid_denomination},
			ar_account_tree:apply_block(BadDenominationB, B1)),
	%% Rejected after the snapshot excursion, which applies the transaction's diff to the tip tree
	%% and then rolls it back: the recomputed root does not match the tampered wallet_list.
	BadRootB = Candidate#block{ wallet_list = crypto:strong_rand_bytes(48) },
	?assertEqual({error, invalid_wallet_list}, ar_account_tree:apply_block(BadRootB, B1)),
	%% The tip tree is byte-identical after both rejections, cached node hashes included.
	?assertEqual(TableBefore, table_dump()),
	%% The tip is untouched by either rejection.
	?assertEqual(Size0, ar_account_tree:get_size()),
	?assertEqual(?AR(10), ar_node:get_balance(Pub2)),
	?assertEqual(?AR(20), ar_node:get_balance(Pub3)),
	?assertEqual(?AR(10), ar_account_tree:get_balance(B1#block.wallet_list, Addr2)),
	?assertEqual(0, ar_account_tree:get_balance(B1#block.wallet_list, Addr3)),
	?assertEqual(?AR(20), ar_account_tree:get_balance(TipRoot, Addr3)),
	assert_tip_matches_from_scratch(TipRoot),
	%% A valid follow-up block still applies and yields a canonical tree.
	TX3 = ar_test_node:sign_tx(main, Key1, #{ target => Addr2, quantity => ?AR(5),
			last_tx => ar_test_node:get_tx_anchor(main) }),
	ar_test_node:assert_post_tx_to_peer(main, TX3),
	ar_test_node:mine(),
	{ok, BI2} = ar_test_await:node_height(main, 3),
	B3 = block_at(BI2, 3),
	?assertEqual(?AR(15), ar_node:get_balance(Pub2)),
	assert_tip_matches_from_scratch(B3#block.wallet_list).

%% @doc The from-state boot path. Mine two blocks, restart the node in place (which keeps
%% the data directory and re-reads it), and assert the account tree comes back intact: the
%% base tree is reloaded from disk into ETS, the consensus window is replayed to rebuild
%% the diff DAG, and set_current restores the tip. Tip size and balances survive, non-tip
%% roots still reconstruct, and the reloaded tip tree hashes to the same root as before.
boot_from_disk_preserves_tree() ->
	Key1 = {_, Pub1} = ar_wallet:new(),
	Addr1 = ar_wallet:to_address(Pub1),
	{_, Pub2} = ar_wallet:new(),
	Addr2 = ar_wallet:to_address(Pub2),
	{_, Pub3} = ar_wallet:new(),
	Addr3 = ar_wallet:to_address(Pub3),
	[B0] = ar_weave:init([{Addr1, ?AR(100), <<>>}]),
	_ = ar_test_node:start(B0),
	TX1 = ar_test_node:sign_tx(main, Key1, #{ target => Addr2, quantity => ?AR(10),
			last_tx => ar_test_node:get_tx_anchor(main) }),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	ar_test_node:mine(),
	{ok, _} = ar_test_await:node_height(main, 1),
	TX2 = ar_test_node:sign_tx(main, Key1, #{ target => Addr3, quantity => ?AR(20),
			last_tx => ar_test_node:get_tx_anchor(main) }),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	ar_test_node:mine(),
	{ok, BI} = ar_test_await:node_height(main, 2),
	B1 = block_at(BI, 1),
	B2 = block_at(BI, 2),
	TipRoot = B2#block.wallet_list,
	Size0 = ar_account_tree:get_size(),
	assert_tip_matches_from_scratch(TipRoot),
	%% Reboot from the persisted state.
	ar_test_node:restart(),
	{ok, _} = ar_test_await:node_height(main, 2),
	%% The tip survives the reboot.
	?assertEqual(Size0, ar_account_tree:get_size()),
	?assertEqual(?AR(10), ar_node:get_balance(Pub2)),
	?assertEqual(?AR(20), ar_node:get_balance(Pub3)),
	%% Non-tip roots still reconstruct from the DAG rebuilt during boot.
	?assertEqual(?AR(100), ar_account_tree:get_balance(B0#block.wallet_list, Addr1)),
	?assertEqual(?AR(10), ar_account_tree:get_balance(B1#block.wallet_list, Addr2)),
	?assertEqual(0, ar_account_tree:get_balance(B1#block.wallet_list, Addr3)),
	%% The tip tree reloaded into ETS hashes to the canonical root.
	assert_tip_matches_from_scratch(TipRoot).

%% @doc Fetch the block at the given height from a tip-first block index.
block_at(BI, Height) ->
	{BH, _, _} = lists:nth(length(BI) - Height, BI),
	ar_test_await:block_stored(BH).

%% @doc Fetch the block at the given height from the in-memory block cache, which - unlike the
%% on-disk copy - still carries the reward_history that apply_block/2 needs on the parent block.
cached_block_at(BI, Height) ->
	{BH, _, _} = lists:nth(length(BI) - Height, BI),
	ar_node:get_block_shadow_from_cache(BH).

%% @doc Read all accounts of the tree with the given root via the chunk API, rebuild a
%% map-based tree from them, and assert it hashes to the same root.
assert_tip_matches_from_scratch(Root) ->
	Accounts = collect_accounts(Root),
	Tree = lists:foldl(fun({Key, Value}, Acc) -> ar_patricia_tree:insert(Key, Value, Acc) end,
			ar_patricia_tree:new(), Accounts),
	{FromScratch, _, _} = ar_patricia_tree:compute_hash(Tree, ar_block:wallet_list_hash_fun()),
	?assertEqual(Root, FromScratch).

collect_accounts(Root) ->
	{ok, {Cursor, Chunk}} = ar_account_tree:get_wallet_list_chunk(Root, first),
	collect_accounts(Root, Cursor, Chunk).

collect_accounts(_Root, last, Acc) ->
	Acc;
collect_accounts(Root, Cursor, Acc) ->
	{ok, {NextCursor, Chunk}} = ar_account_tree:get_wallet_list_chunk(Root, Cursor),
	collect_accounts(Root, NextCursor, Chunk ++ Acc).

%% @doc Return the whole tip ETS table (cached node hashes included) as a sorted list, for
%% comparing the table before and after an operation.
table_dump() ->
	lists:sort(ets:tab2list(ar_patricia_tree)).
