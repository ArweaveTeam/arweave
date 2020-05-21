-module(ar_downloader_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

cleanup_test_() ->
	{timeout, 240, fun test_cleanup/0}.

test_cleanup() ->
	AR = ?AR(2000),
	Blocks = [begin
		Key1 = {_, Pub1} = ar_wallet:new(),
		Key2 = {_, Pub2} = ar_wallet:new(),
		Key3 = {_, Pub3} = ar_wallet:new(),
		TX1 = ar_test_node:sign_tx(Key1, #{ data => ar_util:encode(crypto:strong_rand_bytes(N)) }),
		TX2 = ar_test_node:sign_tx(Key2, #{ data => ar_util:encode(crypto:strong_rand_bytes(N)) }),
		TX3 = ar_test_node:sign_tx(Key3, #{ data => ar_util:encode(crypto:strong_rand_bytes(N)) }),
		WalletLis = [{ar_wallet:to_address(Pub1), AR, TX1#tx.last_tx}, {ar_wallet:to_address(Pub2), AR, TX2#tx.last_tx}, {ar_wallet:to_address(Pub3), AR, TX3#tx.last_tx}],
		[B0] = ar_weave:init(WalletLis),
		{_, RewardAddr} = ar_wallet:new(),
		{Master, Block} = ar_test_node:start_without_clear(B0#block{txs = [TX1, TX2, TX3]}, ar_wallet:to_address(RewardAddr)),
		ar_test_node:assert_post_tx_to_master(Master, TX1),
		ar_test_node:assert_post_tx_to_master(Master, TX2),
		ar_test_node:assert_post_tx_to_master(Master, TX3),
		ar_node:mine(Master),
		Block
	end || N <- lists:seq(1, 10)],
	Block = lists:last(Blocks),
	?assertEqual(Block#block.indep_hash, (ar_storage:read_block(Block#block.indep_hash))#block.indep_hash),
	?assertEqual({ok, Block#block.wallet_list}, ar_storage:read_wallet_list(Block#block.wallet_list_hash)),
	WalletListHash = ar_storage:wallet_list_filepath(Block#block.wallet_list_hash),
	TXs = (ar_storage:read_block(Block#block.indep_hash))#block.txs,
	lists:foreach(fun(ID) ->
		?assertEqual(true, is_list(ar_storage:lookup_tx_filename(ID)))
	end, TXs),
	US = ar_meta_db:get(used_space),
	DS = ar_meta_db:get(disk_space),
	_ = ar_meta_db:put(used_space, 0),
	_ = ar_meta_db:put(disk_space, 0),
	timer:sleep(100),
	_ = ar_meta_db:put(used_space, US),
	_ = ar_meta_db:put(disk_space, DS),
	lists:foreach(fun(ID) ->
		?assertEqual(unavailable, ar_storage:lookup_tx_filename(ID))
	end, TXs),
	?assertEqual(unavailable, ar_storage:read_block(Block#block.indep_hash)),
	?assertEqual({error, {failed_reading_file, WalletListHash, enoent}}, ar_storage:read_wallet_list(Block#block.wallet_list_hash)).
