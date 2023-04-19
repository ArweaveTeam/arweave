-module(ar_mempool_tests).

-include_lib("arweave/include/ar.hrl").

-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_start/1, connect_to_slave/0]).

start_node() ->
	%% Starting a node is slow so we'll run it once for the whole test module
	Key = ar_wallet:new(),
	OtherKey = ar_wallet:new(),
	LastTXID = crypto:strong_rand_bytes(32), 
	[B0] = ar_weave:init([
		wallet(Key, 1000, LastTXID),
		wallet(OtherKey, 800, crypto:strong_rand_bytes(32))
	]),
	start(B0),
	slave_start(B0),
	connect_to_slave(),
	ets:insert(node_state, [{wallet_list, B0#block.wallet_list}]),
	{Key, LastTXID, OtherKey, B0}.

reset_node_state() ->
	ar_mempool:reset(),
	ets:delete_all_objects(ar_tx_emitter_recently_emitted),
	ets:match_delete(node_state, {{tx, '_'}, '_'}),
	ets:match_delete(node_state, {{tx_prefixes, '_'}, '_'}).

add_tx_test_() ->
	{setup, fun start_node/0,
		fun (GenesisData) ->
			{foreach, fun reset_node_state/0,
				[
					{with, GenesisData, [fun test_mempool_sorting/1]},
					{with, GenesisData, [fun test_drop_low_priority_txs_header/1]},
					{with, GenesisData, [fun test_drop_low_priority_txs_data/1]},
					{with, GenesisData, [fun test_drop_low_priority_txs_data_and_header/1]},
					{with, GenesisData, [fun test_clashing_last_tx/1]},
					{with, GenesisData, [fun test_overspent_tx/1]},
					{with, GenesisData, [fun test_mixed_deposit_spend_tx_old_address/1]},
					{with, GenesisData, [fun test_mixed_deposit_spend_tx_new_address/1]},
					{with, GenesisData, [fun test_clash_and_overspend_tx/1]},
					{with, GenesisData, [fun test_clash_and_low_priority_tx/1]}
				]
			}
		end
	}.

%% @doc Test that mempool transactions are correctly sorted in priority order
test_mempool_sorting({{_, {_, Owner}}, _LastTXID, _OtherKey, _B0}) ->
	%% Transactions are named with their expected, prioritized order.
	%% The sorting is assumed to consider, in order:
	%% 1. Reward (higher reward is higher priority)
	%% 2. Timestamp (lower timestamp is higher priority)
	%% 3. Format 1 transactions with a lot of data are deprioritized
	%% 
	%% Only the above criteria are expected to impact sort order
	TX9 = tx(1, Owner, 15, crypto:strong_rand_bytes(200)),
	ar_mempool:add_tx(TX9, waiting),
	TX8 = tx(1, Owner, 20, crypto:strong_rand_bytes(200)),
	ar_mempool:add_tx(TX8, waiting),
	TX5 = tx(2, Owner, 1, <<>>),
	ar_mempool:add_tx(TX5, waiting),
	TX6 = tx(2, Owner, 1, <<"abc">>),
	ar_mempool:add_tx(TX6, waiting),
	TX7 = tx(1, Owner, 1, <<>>),
	ar_mempool:add_tx(TX7, waiting),
	TX1 = tx(1, Owner, 10, <<>>),
	ar_mempool:add_tx(TX1, waiting),
	TX2 = tx(1, Owner, 10, <<"abcdef">>),
	ar_mempool:add_tx(TX2, waiting),
	TX3 = tx(2, Owner, 10, <<>>),
	ar_mempool:add_tx(TX3, waiting),
	TX4 = tx(1, Owner, 10, <<>>),
	ar_mempool:add_tx(TX4, waiting),

	%% {HeaderSize, DataSize}
	%% HeaderSize: TX_SIZE_BASE per transaction plus the data size of all
	%% format 1 transactions.
	%% DataSize: the total data size of all format 2 transactions
	ExpectedMempoolSize = {(9* ?TX_SIZE_BASE) + 200 + 200 + 6, 3},
	ExpectedTXIDs = [
		TX1#tx.id,
		TX2#tx.id,
		TX3#tx.id,
		TX4#tx.id,
		TX5#tx.id,
		TX6#tx.id,
		TX7#tx.id,
		TX8#tx.id,
		TX9#tx.id
		],

	assertMempoolTXIDs(ExpectedTXIDs, "Sorted mempool transactions"),
	assertMempoolSize(ExpectedMempoolSize).
	
%% @doc Test dropping transactions when the mempool max header size is exceeded
test_drop_low_priority_txs_header({{_, {_, Owner}}, _LastTXID, _OtherKey, _B0}) ->
	%% Add 9x Format 1 transactions each with a data size equal to
	%% 1/10 the MEMPOOL_HEADER_SIZE_LIMIT. This puts us close to exceeding the
	%% header size limit
	NumTransactions = 9,
	DataSize = ?MEMPOOL_HEADER_SIZE_LIMIT div (NumTransactions + 1),
	{ExpectedTXIDs, HighestReward, LowestReward} =
		add_transactions(NumTransactions, 1, Owner, DataSize),
	ExpectedMempoolSize = {NumTransactions * (?TX_SIZE_BASE + DataSize),0},

	assertMempoolTXIDs(ExpectedTXIDs, "Mempool is below the header size limit"),
	assertMempoolSize(ExpectedMempoolSize),

	%% Add multiple low priority transactions to push us over the header size
	%% limit. All of these new transactions should be dropped
	ar_mempool:add_tx(
		tx(1, Owner, LowestReward-2, crypto:strong_rand_bytes(500)), waiting),
	ar_mempool:add_tx(
		tx(1, Owner, LowestReward-2, crypto:strong_rand_bytes(500)), waiting),
	ar_mempool:add_tx(
		tx(1, Owner, LowestReward-2, crypto:strong_rand_bytes(500)), waiting),
	ar_mempool:add_tx(
		tx(1, Owner, LowestReward-1, crypto:strong_rand_bytes(DataSize)), waiting),
	assertMempoolTXIDs(
		ExpectedTXIDs, "Multiple low priority TX pushed Mempool over the header size limit"),
	assertMempoolSize(ExpectedMempoolSize),

	%% Add a high priority transaction to push us over the header size limit.
	%% This newest transaction should *not* be dropped, instead a lower priority
	%% transaction should be dropped
	TXHigh = tx(1, Owner, HighestReward+1, crypto:strong_rand_bytes(DataSize)),
	ar_mempool:add_tx(TXHigh, waiting),
	ExpectedTXIDs2 = [TXHigh#tx.id | lists:delete(lists:last(ExpectedTXIDs), ExpectedTXIDs)],
	assertMempoolTXIDs(
		ExpectedTXIDs2, "High priority TX pushed Mempool over the header size limit"),
	assertMempoolSize(ExpectedMempoolSize).

%% @doc Test dropping transactions when the mempool max data size is exceeded
test_drop_low_priority_txs_data({{_, {_, Owner}}, _LastTXID, _OtherKey, _B0}) ->
	%% Add 9x Format 2 transactions each with a data size slightly larger than
	%% 1/10 the MEMPOOL_DATA_SIZE_LIMIT. This puts us close to exceeding the
	%% data size limit
	NumTransactions = 9,
	DataSize = (?MEMPOOL_DATA_SIZE_LIMIT div (NumTransactions+1)) + 1,
	{ExpectedTXIDs, HighestReward, LowestReward} =
		add_transactions(NumTransactions, 2, Owner, DataSize),
	ExpectedMempoolSize = {NumTransactions * ?TX_SIZE_BASE, NumTransactions * DataSize},

	assertMempoolTXIDs(ExpectedTXIDs, "Mempool is below the data size limit"),
	assertMempoolSize(ExpectedMempoolSize),

	%% Add multiple low priority transactions to push us over the data size
	%% limit. All of these new transactions should be dropped
	ar_mempool:add_tx(
		tx(2, Owner, LowestReward-2, crypto:strong_rand_bytes(500)), waiting),
	ar_mempool:add_tx(
		tx(2, Owner, LowestReward-2, crypto:strong_rand_bytes(500)), waiting),
	ar_mempool:add_tx(
		tx(2, Owner, LowestReward-2, crypto:strong_rand_bytes(500)), waiting),
	ar_mempool:add_tx(
		tx(2, Owner, LowestReward-1, crypto:strong_rand_bytes(DataSize)), waiting),
	assertMempoolTXIDs(
		ExpectedTXIDs, "Low priority TX pushed Mempool over the data size limit"),
	assertMempoolSize(ExpectedMempoolSize),

	%% Add a high priority transaction to push us over the data size limit.
	%% This newest transaction should *not* be dropped, instead a lower priority
	%% transaction should be dropped
	TXHigh = tx(2, Owner, HighestReward+1, crypto:strong_rand_bytes(DataSize)),
	ar_mempool:add_tx(TXHigh, waiting),
	ExpectedTXIDs2 = [TXHigh#tx.id | lists:delete(lists:last(ExpectedTXIDs), ExpectedTXIDs)],
	assertMempoolTXIDs(
		ExpectedTXIDs2, "High priority TX pushed Mempool over the data size limit"),
	assertMempoolSize(ExpectedMempoolSize).

%% @doc Test dropping transactions when both the mempool data size and header
%% size are exceeded
test_drop_low_priority_txs_data_and_header({{_, {_, Owner}}, _LastTXID, _OtherKey, _B0}) ->
	NumTransactions = 9,
	Format1DataSize = ?MEMPOOL_HEADER_SIZE_LIMIT div (NumTransactions+1),
	{Format1ExpectedTXIDs, _Format1HighestReward, Format1LowestReward} =
		add_transactions(NumTransactions, 1, Owner, Format1DataSize),
	Format2DataSize = (?MEMPOOL_DATA_SIZE_LIMIT div (NumTransactions+1)) + 1,
	{Format2ExpectedTXIDs, _Format2HighestReward, Format2LowestReward} =
		add_transactions(NumTransactions, 2, Owner, Format2DataSize),

	ExpectedTXIDs = Format2ExpectedTXIDs ++ Format1ExpectedTXIDs,
	{ExpectedHeaderSize, ExpectedDataSize} = {
		(2 * NumTransactions * ?TX_SIZE_BASE) + (NumTransactions * Format1DataSize),
		NumTransactions * Format2DataSize},

	assertMempoolTXIDs(ExpectedTXIDs, "Mempool is below both the data and header size limits"),
	assertMempoolSize({ExpectedHeaderSize, ExpectedDataSize}),

	RemainingHeaderSpace = ?MEMPOOL_HEADER_SIZE_LIMIT - ExpectedHeaderSize,
	ar_mempool:add_tx(
		tx(
			1, 
			Owner, 
			Format1LowestReward-2,
			crypto:strong_rand_bytes(RemainingHeaderSpace - ?TX_SIZE_BASE)
		), waiting),
	%% This transaction will cause both the header and data size limits to be
	%% exceeded simultaneously
	ar_mempool:add_tx(
		tx(
			2,
			Owner,
			Format2LowestReward-1,
			crypto:strong_rand_bytes(Format2DataSize)
		), waiting),

	%% Last two transactions should be dropped
	assertMempoolTXIDs(
		ExpectedTXIDs, "TX pushed the Mempool over both the header and data size limits").

%% @doc Test that only 1 TX with a given last_tx can exist in the mempool.
test_clashing_last_tx({{_, {_, Owner}}, LastTXID, _OtherKey, B0}) ->
	BaseID = crypto:strong_rand_bytes(31),
	
	%% Add some extra, non-clashing, transactions to test that only clashing
	%% transactions are dropped
	NumTransactions = 2,
	Format1DataSize = 200,
	{Format1ExpectedTXIDs, Format1HighestReward, _} =
		add_transactions(NumTransactions, 1, Owner, Format1DataSize),
	Format2DataSize = 50,
	{Format2ExpectedTXIDs, Format2HighestReward, _} =
		add_transactions(NumTransactions, 2, Owner, Format2DataSize),

	ExpectedTXIDs = Format2ExpectedTXIDs ++ Format1ExpectedTXIDs,
	Test0 = "Test 0: Transactions with empty last_tx can never clash",
	assertMempoolTXIDs(ExpectedTXIDs, Test0),

	Test1 = "Test 1: Lower reward TX is dropped",
	TX1 = tx(2, Owner, Format2HighestReward+2, <<>>, <<"c", BaseID/binary>>, LastTXID),
	TX2 = tx(2, Owner, Format2HighestReward+1, <<>>, <<"d", BaseID/binary>>, LastTXID),
	ar_mempool:add_tx(TX1, waiting),
	ar_mempool:add_tx(TX2, waiting),
	assertMempoolTXIDs([TX1#tx.id | ExpectedTXIDs], Test1),

	Test2 = "Test 2: Higher reward TX replace existing TX with lower reward",
	TX3 = tx(2, Owner, Format2HighestReward+3, <<>>, <<"e", BaseID/binary>>,  LastTXID),
	ar_mempool:add_tx(TX3, waiting),
	assertMempoolTXIDs([TX3#tx.id | ExpectedTXIDs], Test2),

	Test3 = "Test 3: Higher TXID alphanumeric order replaces lower",
	TX4 = tx(2, Owner, Format2HighestReward+3, <<>>, <<"f", BaseID/binary>>, LastTXID),
	ar_mempool:add_tx(TX4, waiting),
	assertMempoolTXIDs([TX4#tx.id | ExpectedTXIDs], Test3),

	Test4 = "Test 4: Lower TXID alphanumeric order is dropped",
	TX5 = tx(2, Owner, Format2HighestReward+3, <<>>, <<"b", BaseID/binary>>, LastTXID),
	ar_mempool:add_tx(TX5, waiting),
	assertMempoolTXIDs([TX4#tx.id | ExpectedTXIDs], Test4),

	Test5 = "Test 5: Deprioritized format 1 TX is dropped",
	TX6 = tx(
			1,
			Owner,
			Format1HighestReward+3,
			crypto:strong_rand_bytes(200), <<"g", BaseID/binary>>,
			LastTXID
		),
	ar_mempool:add_tx(TX6, waiting),
	assertMempoolTXIDs([TX4#tx.id | ExpectedTXIDs], Test5),

	Test6 = "Test 6: High priority format 1 replaces a low priority format 2",
	TX7 = tx(1, Owner, Format1HighestReward+3, <<>>, <<"h", BaseID/binary>>, LastTXID),
	ar_mempool:add_tx(TX7, waiting),
	assertMempoolTXIDs([TX7#tx.id | ExpectedTXIDs], Test6),

	Test7 = "Test 7: TX last_tx set to block hash can not clash",
	TX8 = tx(
			2,
			Owner,
			Format2HighestReward+4,
			<<>>,
			<<"i", BaseID/binary>>,
			B0#block.indep_hash
		),
	TX9 = tx(
			2,
			Owner,
			Format2HighestReward+5,
			<<>>,
			<<"j", BaseID/binary>>,
			B0#block.indep_hash
		),
	ar_mempool:add_tx(TX8, waiting),
	ar_mempool:add_tx(TX9, waiting),
	ExpectedTXIDs2 = [TX9#tx.id | [TX8#tx.id | [TX7#tx.id | ExpectedTXIDs]]],
	assertMempoolTXIDs(ExpectedTXIDs2, Test7),	

	Test8 = "Test 8: TX last_tx set to <<>> can not clash",
	TX10 = tx(2, Owner, Format2HighestReward+6, <<>>, <<"k", BaseID/binary>>, <<>>),
	TX11 = tx(2, Owner, Format2HighestReward+7, <<>>, <<"l", BaseID/binary>>, <<>>),
	ar_mempool:add_tx(TX10, waiting),
	ar_mempool:add_tx(TX11, waiting),
	assertMempoolTXIDs([TX11#tx.id | [TX10#tx.id | ExpectedTXIDs2]], Test8).

%% @doc Test that TXs that would overspend an account are dropped from the
%% mempool.
test_overspent_tx({{_, {_, Owner}}, _LastTXID, _OtherKey, _B0}) ->
	BaseID = crypto:strong_rand_bytes(31),

	Test1 = "Test 1: Lower reward TX is dropped",
	TX1 = tx(2, Owner, 3, <<>>, <<"c", BaseID/binary>>, <<>>, 400),
	TX2 = tx(2, Owner, 2, <<>>, <<"d", BaseID/binary>>, <<>>, 400),
	TX3 = tx(2, Owner, 1, <<>>, <<"e", BaseID/binary>>, <<>>, 400),
	ar_mempool:add_tx(TX1, waiting),
	ar_mempool:add_tx(TX2, waiting),
	ar_mempool:add_tx(TX3, waiting),
	assertMempoolTXIDs([TX1#tx.id, TX2#tx.id], Test1),

	Test2 = "Test 2: Higher reward TX replace existing TX with lower reward",
	TX4 = tx(2, Owner, 4, <<>>, <<"f", BaseID/binary>>, <<>>, 400),
	ar_mempool:add_tx(TX4, waiting),
	assertMempoolTXIDs([TX4#tx.id, TX1#tx.id], Test2),

	Test3 = "Test 3: Higher TXID alphanumeric order replaces lower",
	TX5 = tx(2, Owner, 3, <<>>, <<"g", BaseID/binary>>, <<>>, 400),
	ar_mempool:add_tx(TX5, waiting),
	assertMempoolTXIDs([TX4#tx.id, TX5#tx.id], Test3),

	Test4 = "Test 4: Lower TXID alphanumeric order is dropped",
	TX6 = tx(2, Owner, 3, <<>>, <<"b", BaseID/binary>>, <<>>, 400),
	ar_mempool:add_tx(TX6, waiting),
	assertMempoolTXIDs([TX4#tx.id, TX5#tx.id], Test4),

	Test5 = "Test 5: Deprioritized format 1 TX is dropped",
	TX7 = tx(1, Owner, 3, crypto:strong_rand_bytes(200), <<"h", BaseID/binary>>, <<>>, 400),
	ar_mempool:add_tx(TX7, waiting),
	assertMempoolTXIDs([TX4#tx.id, TX5#tx.id], Test5),

	Test6 = "Test 6: High priority format 1 replaces a low priority format 2",
	TX8 = tx(1, Owner, 3, <<>>, <<"i", BaseID/binary>>, <<>>, 400),
	ar_mempool:add_tx(TX8, waiting),
	assertMempoolTXIDs([TX4#tx.id, TX8#tx.id], Test6),

	Test7 = "Test 7: 0 quantity TX can still overspend",
	TX9 = tx(2, Owner, 400, <<>>, <<"j", BaseID/binary>>, <<>>, 0),
	ar_mempool:add_tx(TX9, waiting),
	assertMempoolTXIDs([TX9#tx.id, TX4#tx.id], Test7).

%% @doc Test that unconfirmed deposit TXs are ignored when determining whether an
%% account is overspent. In this case the deposit comes from an address which 
%% has an on-chain balance.
test_mixed_deposit_spend_tx_old_address({
		{_, Pub} = {_, {_, Owner}},
		_LastTXID,
		{_, {_, OtherOwner}},
		_B0}) ->
	BaseID = crypto:strong_rand_bytes(31),
	Origin = ar_wallet:to_address(Pub),

	Test1 = "Test 1: Unconfirmed deposits from old addresses are not considered for overspend",
	TX2 = tx(2, OtherOwner, 9, <<>>, <<"d", BaseID/binary>>, <<>>, 500, Origin),
	TX3 = tx(2, Owner, 8, <<>>, <<"c", BaseID/binary>>, <<>>, 600),
	TX4 = tx(2, Owner, 7, <<>>, <<"b", BaseID/binary>>, <<>>, 600),
	ar_mempool:add_tx(TX2, waiting),
	ar_mempool:add_tx(TX3, waiting),
	ar_mempool:add_tx(TX4, waiting),
	assertMempoolTXIDs([TX2#tx.id, TX3#tx.id], Test1).

%% @doc Test that unconfirmed deposit TXs are ignored when determining whether an
%% account is overspent. In this case the deposit comes from an address which
%% has not made it on-chain yet (deposit and spend are both in the mempool).
test_mixed_deposit_spend_tx_new_address({
		{_, Pub} = {_, {_, Owner}}, _LastTXID, _OtherKey, _B0}) ->
	BaseID = crypto:strong_rand_bytes(31),
	Origin = ar_wallet:to_address(Pub),

	{_, NewPub} = {_, {_, NewOwner}} = ar_wallet:new(),
	NewAddr = ar_wallet:to_address(NewPub),

	Test1 = "Test 1: Unconfirmed deposits from new addresses are not considered for overspend",
	TX1 = tx(2, Owner, 10, <<>>, <<"e", BaseID/binary>>, <<>>, 400, NewAddr),
	TX2 = tx(2, NewOwner, 9, <<>>, <<"d", BaseID/binary>>, <<>>, 400, Origin),
	TX3 = tx(2, Owner, 8, <<>>, <<"c", BaseID/binary>>, <<>>, 400),
	TX4 = tx(2, Owner, 7, <<>>, <<"b", BaseID/binary>>, <<>>, 400),
	ar_mempool:add_tx(TX1, waiting),
	ar_mempool:add_tx(TX2, waiting),
	ar_mempool:add_tx(TX3, waiting),
	ar_mempool:add_tx(TX4, waiting),
	assertMempoolTXIDs([TX1#tx.id, TX3#tx.id], Test1).

%% @doc Test a TX that has a last_tx clash and overspends an account is
%% handled correctly.
test_clash_and_overspend_tx({{_, {_, Owner}}, LastTXID, _OtherKey, _B0}) ->
	BaseID = crypto:strong_rand_bytes(31),

	Test1 = "Test 1: Clashing TXs are dropped before overspend is calculated",
	TX1 = tx(2, Owner, 3, <<>>, <<"d", BaseID/binary>>, <<>>, 400),
	TX2 = tx(2, Owner, 2, <<>>, <<"c", BaseID/binary>>, LastTXID, 400),
	TX3 = tx(2, Owner, 1, <<>>, <<"b", BaseID/binary>>, LastTXID, 400),
	TX4 = tx(2, Owner, 4, <<>>, <<"e", BaseID/binary>>, LastTXID, 400),
	ar_mempool:add_tx(TX1, waiting),
	ar_mempool:add_tx(TX2, waiting),
	ar_mempool:add_tx(TX3, waiting),
	ar_mempool:add_tx(TX4, waiting),
	assertMempoolTXIDs([TX4#tx.id, TX1#tx.id], Test1).

%% @doc Test that the right TXs are dropped when the mempool max data size is reached due to
%% clashing TXs. Only the clashing TXs should be dropped.
test_clash_and_low_priority_tx({{_, {_, Owner}}, LastTXID, _OtherKey, _B0}) ->
	%% Add 9x Format 2 transactions each with a data size slightly larger than
	%% 1/10 the MEMPOOL_DATA_SIZE_LIMIT. This puts us close to exceeding the
	%% data size limit
	NumTransactions = 9,
	DataSize = (?MEMPOOL_DATA_SIZE_LIMIT div (NumTransactions+1)) + 1,
	{ExpectedTXIDs, HighestReward, LowestReward} =
		add_transactions(NumTransactions, 2, Owner, DataSize),

	TX1 = tx(2, Owner, HighestReward+1, <<>>, crypto:strong_rand_bytes(32), LastTXID),
	ar_mempool:add_tx(TX1, waiting),

	ExpectedMempoolSize = {(NumTransactions+1) * ?TX_SIZE_BASE, NumTransactions * DataSize},

	assertMempoolTXIDs([TX1#tx.id] ++ ExpectedTXIDs, "Mempool is below the data size limit"),
	assertMempoolSize(ExpectedMempoolSize),

	ClashTX = tx(
		2, Owner, LowestReward-1, crypto:strong_rand_bytes(DataSize),
		crypto:strong_rand_bytes(32), LastTXID),
	ar_mempool:add_tx(ClashTX, waiting),

	assertMempoolTXIDs([TX1#tx.id] ++ ExpectedTXIDs, "Clashing TX dropped"),
	assertMempoolSize(ExpectedMempoolSize).

add_transactions(NumTransactions, Format, Owner, DataSize) ->
	HighestReward = NumTransactions+2,
	LowestReward = 3,
	TXs = [
		tx(Format, Owner, Reward, crypto:strong_rand_bytes(DataSize))
			|| Reward <- lists:seq(HighestReward, LowestReward, -1)
	],
	lists:foreach(
		fun(TX) ->
			ar_mempool:add_tx(TX, waiting)
		end,
		TXs),
	ExpectedTXIDs = lists:map(
		fun(#tx{id = TXID}) ->
			TXID
		end,
		TXs),
	{ExpectedTXIDs, HighestReward, LowestReward}.

wallet({_, Pub}, Balance, LastTXID) ->
	{ar_wallet:to_address(Pub), Balance, LastTXID}.

tx(Format, Owner, Reward, Data) ->
	tx(Format, Owner, Reward, Data, crypto:strong_rand_bytes(32), <<>>).

tx(Format, Owner, Reward, Data, TXID, Anchor) ->
	tx(Format, Owner, Reward, Data, TXID, Anchor, 0).

tx(Format, Owner, Reward, Data, TXID, Anchor, Quantity) ->
	tx(Format, Owner, Reward, Data, TXID, Anchor, Quantity, <<>>).

tx(Format, Owner, Reward, Data, TXID, Anchor, Quantity, Target) ->
	#tx{
		id = TXID,
		format = Format,
		reward = Reward,
		data = Data,
		data_size = byte_size(Data),
		owner = Owner,
		target = Target,
		last_tx = Anchor,
		quantity = Quantity
	}.

assertMempoolSize(ExpectedMempoolSize) ->
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	?assertEqual(ExpectedMempoolSize, MempoolSize).

assertMempoolTXIDs(ExpectedTXIDs, Title) ->
	%% Unordered list of all TXIDs in the mempool
	TXIDs = lists:map(
		fun([TXID]) ->
			TXID
		end,
		ets:match(node_state, {{tx, '$1'}, '_'})
	),

	%% Ordered list of expected TXIDs paired with their expected status 
	ExpectedTXIDsStatuses = lists:map(
		fun(ExpectedTXID) ->
			{ExpectedTXID, waiting}
		end,
		ExpectedTXIDs
	),

	%% gb_sets:to_list returns elements in ascending order of utility
	%% (lowest reward, latest TX first), so we need to reverse the list to get
	%% the true priority order (highest reward, oldest TX first)
	MempoolInPriorityOrder = lists:reverse(gb_sets:to_list(ar_mempool:get_priority_set())),
	%% Ordered list of actual TXIDs paired with their actual status
	ActualTXIDsStatuses = lists:map(
		fun({_, ActualTXID, ActualStatus}) ->
			{ActualTXID, ActualStatus}
		end,
		MempoolInPriorityOrder
	),

	%% If we're adding and removing TX to/from the last_tx_map and origin_tx_map
	%% correctly, this list of TXIDs should match the total set of TXIDs in the
	%% mempool
	LastTXMapTXIDs = lists:foldl(
		fun({_Priority, TXID}, Acc) ->
			[TXID | Acc]
		end,
		[],
		lists:flatten(
			lists:foldl(
				fun(Set, Acc) ->
					[gb_sets:to_list(Set) | Acc]
				end,
				[],
				maps:values(ar_mempool:get_last_tx_map())
			)
		)
	),

	OriginTXMapTXIDs = lists:foldl(
		fun({_Priority, TXID}, Acc) ->
			[TXID | Acc]
		end,
		[],
		lists:flatten(
			lists:foldl(
				fun(Set, Acc) ->
					[gb_sets:to_list(Set) | Acc]
				end,
				[],
				maps:values(ar_mempool:get_origin_tx_map())
			)
		)
	),

	%% Only this first test will assert the ordering of TXIDs in the mempools
	?assertEqual(ExpectedTXIDsStatuses, ActualTXIDsStatuses, Title),
	%% These remaining tests only assert that the unordered set of TXIDs is correct
	?assertEqual(lists:sort(ExpectedTXIDs), lists:sort(TXIDs), Title),
	?assertEqual(lists:sort(ExpectedTXIDs), lists:sort(LastTXMapTXIDs), Title),
	?assertEqual(lists:sort(ExpectedTXIDs), lists:sort(OriginTXMapTXIDs), Title).