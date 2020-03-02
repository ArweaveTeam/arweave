-module(ar_downloader_tests).

-include("src/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [assert_post_tx_to_slave/2]).
-import(ar_test_node, [assert_wait_until_receives_txs/2, wait_until_height/2]).
-import(ar_test_node, [sign_tx/2, sign_v1_tx/2, setup/0]).
-import(ar_test_node, [slave_mine/1, slave_call/3]).
-import(ar_test_node, [test_with_mocked_functions/2]).

passive_block_download_with_format_2_txs_test() ->
	test_with_mocked_functions(
		[
			{ar_fork, height_2_0, fun() -> 0 end}
		],
		fun passive_block_download_with_format_2_txs/0
	).

passive_block_download_with_format_2_txs() ->
	% ensure passive mode is default
	false = ar_meta_db:get(active_download),
	false = slave_call(ar_meta_db, get, [active_download]),

	{MasterNode, SlaveNode, Wallet} = setup(),
	timer:sleep(2000),
	TX1Params = #{format => 2, data => <<"TX-1-DATA">>, reward => ?AR(1)},
	TX2Params = #{format => 2, data => <<"TX-2-DATA">>, reward => ?AR(1)},
	SignedTX1 = #tx{id = TXID1} = sign_tx(Wallet, TX1Params),
	SignedTX2 = #tx{id = TXID2} = sign_tx(Wallet, TX2Params),
	SignedTX1NoData = ar_tx:strip_data(SignedTX1),
	SignedTX2NoData = ar_tx:strip_data(SignedTX2),
	[assert_post_tx_to_slave(SlaveNode, TX) || TX <- SignedTX1, SignedTX2],
	assert_wait_until_receives_txs(MasterNode, [SignedTX1NoData, SignedTX2NoData]),
	slave_mine(SlaveNode),
	BI = wait_until_height(MasterNode, 2),

	%% Format-2 TX on master
	#block{txs = TXS0} = ar_storage:read_block(hd(BI), BI),
	?assertEqual(lists:member(TXID1, TXS0), true),
	?assertEqual(lists:member(TXID2, TXS0), true),
	?assertEqual(ar_storage:read_tx(TXID1), SignedTX1NoData),
	?assertEqual(ar_storage:read_tx(TXID2), SignedTX2NoData),

	%% Format-1 TX on slave
	#block{txs = TXS1} = slave_call(ar_storage, read_block, [hd(BI), BI]),
	?assertEqual(lists:member(TXID1, TXS1), true),
	?assertEqual(lists:member(TXID2, TXS1), true),
	?assertEqual(slave_call(ar_storage, read_tx, [TXID1]), SignedTX1),
	?assertEqual(slave_call(ar_storage, read_tx, [TXID2]), SignedTX2),

	%% Start downloader on master
    ar_downloader:start(
        [{127, 0, 0, 1, slave_call(ar_meta_db, get, [port])}], BI),
    ?assertEqual(is_process_alive(whereis(ar_downloader)), true),

	%% Format-2 TXs replace with Format-1 TXs on master after download
	#block{txs = TXS} = ar_storage:read_block(hd(BI), BI),
	?assertEqual(lists:member(TXID1, TXS), true),
	?assertEqual(lists:member(TXID2, TXS), false),
	?assertEqual(ar_storage:read_tx(TXID1), SignedTX1),
	?assertEqual(ar_storage:read_tx(TXID2), SignedTX2).

active_block_download_with_format_2_txs_test() ->
	test_with_mocked_functions(
		[
			{ar_fork, height_2_0, fun() -> 0 end}
		],
		fun active_block_download_with_format_2_txs/0
	).

active_block_download_with_format_2_txs() ->
	% configure active download mode
	true = ar_meta_db:put(active_download, true),
	true = slave_call(ar_meta_db, put, [active_download, true]),

	{MasterNode, SlaveNode, Wallet} = setup(),
	timer:sleep(2000),
	TX1Params = #{format => 2, data => <<"TX-1-DATA">>, reward => ?AR(1)},
	TX2Params = #{format => 2, data => <<"TX-2-DATA">>, reward => ?AR(1)},
	SignedTX1 = #tx{id = TXID1} = sign_tx(Wallet, TX1Params),
	SignedTX2 = #tx{id = TXID2} = sign_tx(Wallet, TX2Params),
	SignedTX1NoData = ar_tx:strip_data(SignedTX1),
	SignedTX2NoData = ar_tx:strip_data(SignedTX2),
	[assert_post_tx_to_slave(SlaveNode, TX) || TX <- SignedTX1, SignedTX2],
	assert_wait_until_receives_txs(MasterNode, [SignedTX1NoData, SignedTX2NoData]),
	slave_mine(SlaveNode),
	BI = wait_until_height(MasterNode, 2),

	%% Format-2 TX on master
	#block{txs = TXS0} = ar_storage:read_block(hd(BI), BI),
	?assertEqual(lists:member(TXID1, TXS0), true),
	?assertEqual(lists:member(TXID2, TXS0), true),
	?assertEqual(ar_storage:read_tx(TXID1), SignedTX1NoData),
	?assertEqual(ar_storage:read_tx(TXID2), SignedTX2NoData),

	%% Format-1 TX on slave
	#block{txs = TXS1} = slave_call(ar_storage, read_block, [hd(BI), BI]),
	?assertEqual(lists:member(TXID1, TXS1), true),
	?assertEqual(lists:member(TXID2, TXS1), true),
	?assertEqual(slave_call(ar_storage, read_tx, [TXID1]), SignedTX1),
	?assertEqual(slave_call(ar_storage, read_tx, [TXID2]), SignedTX2),

	%% Active downloader mode, peer node downloading full blocks on completion

	%% Format-2 TXs replace with Format-1 TXs on master after download
	#block{txs = TXS} = ar_storage:read_block(hd(BI), BI),
	?assertEqual(lists:member(TXID1, TXS), true),
	?assertEqual(lists:member(TXID2, TXS), false),
	?assertEqual(ar_storage:read_tx(TXID1), SignedTX1),
	?assertEqual(ar_storage:read_tx(TXID2), SignedTX2).
