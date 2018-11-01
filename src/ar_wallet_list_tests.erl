-module(ar_wallet_list_tests).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Ensure that a set of txs can be checked for serialization, those that
%% don't serialize disregarded.
filter_out_of_order_txs_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		RawTX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
		TX = RawTX#tx {owner = Pub1},
		SignedTX = ar_tx:sign(TX, Priv1, Pub1),
		RawTX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), SignedTX#tx.id),
		TX2 = RawTX2#tx {owner = Pub1},
		SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
		WalletList =
			[
				{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
				{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
				{ar_wallet:to_address(Pub3), ?AR(3000), <<>>}
			],
		% TX1 applied, TX2 applied
		{_, [SignedTX2, SignedTX]} =
			ar_wallet_list:filter_out_of_order_txs(
				WalletList,
				[SignedTX, SignedTX2]
			),
		% TX2 disregarded, TX1 applied
		{_, [SignedTX]} =
			ar_wallet_list:filter_out_of_order_txs(
				WalletList,
				[SignedTX2, SignedTX]
			)
	end}.

%% @doc Ensure that a large set of txs can be checked for serialization,
%% those that don't serialize disregarded.
filter_out_of_order_txs_large_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
		SignedTX = ar_tx:sign(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), SignedTX#tx.id),
		SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
		TX3 = ar_tx:new(Pub3, ?AR(1), ?AR(50), SignedTX2#tx.id),
		SignedTX3 = ar_tx:sign(TX3, Priv1, Pub1),
		WalletList =
			[
				{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
				{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
				{ar_wallet:to_address(Pub3), ?AR(3000), <<>>}
			],
		% TX1 applied, TX2 applied, TX3 applied
		{_, [SignedTX3, SignedTX2, SignedTX]} =
			ar_wallet_list:filter_out_of_order_txs(
				WalletList,
				[SignedTX, SignedTX2, SignedTX3]
			),
		% TX2 disregarded, TX3 disregarded, TX1 applied
		{_, [SignedTX]} =
			ar_wallet_list:filter_out_of_order_txs(
				WalletList,
				[SignedTX2, SignedTX3, SignedTX]
			),
		% TX1 applied, TX3 disregarded, TX2 applied.
		{_, [SignedTX2, SignedTX]} =
			ar_wallet_list:filter_out_of_order_txs(
				WalletList,
				[SignedTX, SignedTX3, SignedTX2]
			)
	end}.

%% @doc Ensure that a set of txs can be serialized in the best possible order.
filter_all_out_of_order_txs_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
		SignedTX = ar_tx:sign(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), SignedTX#tx.id),
		SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
		WalletList =
			[
				{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
				{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
				{ar_wallet:to_address(Pub3), ?AR(3000), <<>>}
			],
		% TX1 applied, TX2 applied
		[SignedTX, SignedTX2] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX, SignedTX2]
			),
		% TX2 applied, TX1 applied
		[SignedTX, SignedTX2] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX2, SignedTX]
			)
	end}.

%% @doc Ensure that a large set of txs can be serialized in the best
%% possible order.
filter_all_out_of_order_txs_large_test_() ->
	{timeout, 60, fun() ->
		ar_storage:clear(),
		{Priv1, Pub1} = ar_wallet:new(),
		{Priv2, Pub2} = ar_wallet:new(),
		{_Priv3, Pub3} = ar_wallet:new(),
		TX = ar_tx:new(Pub2, ?AR(1), ?AR(500), <<>>),
		SignedTX = ar_tx:sign(TX, Priv1, Pub1),
		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(400), SignedTX#tx.id),
		SignedTX2 = ar_tx:sign(TX2, Priv1, Pub1),
		TX3 = ar_tx:new(Pub3, ?AR(1), ?AR(50), SignedTX2#tx.id),
		SignedTX3 = ar_tx:sign(TX3, Priv1, Pub1),
		TX4 = ar_tx:new(Pub1, ?AR(1), ?AR(25), <<>>),
		SignedTX4 = ar_tx:sign(TX4, Priv2, Pub2),
		WalletList =
			[
				{ar_wallet:to_address(Pub1), ?AR(1000), <<>>},
				{ar_wallet:to_address(Pub2), ?AR(2000), <<>>},
				{ar_wallet:to_address(Pub3), ?AR(3000), <<>>}
			],
		% TX1 applied, TX2 applied, TX3 applied
		[SignedTX, SignedTX2, SignedTX3] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX, SignedTX2, SignedTX3]
			),
		% TX1 applied, TX3 applied, TX2 applied
		[SignedTX, SignedTX2, SignedTX3] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX, SignedTX3, SignedTX2]
			),
		% TX2 applied, TX1 applied, TX3 applied
		[SignedTX, SignedTX2, SignedTX3] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX2, SignedTX, SignedTX3]
			),
		% TX2 applied, TX3 applied, TX1 applied
		[SignedTX, SignedTX2, SignedTX3] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX2, SignedTX3, SignedTX]
			),
		% TX3 applied, TX1 applied, TX2 applied
		[SignedTX, SignedTX2, SignedTX3] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX3, SignedTX, SignedTX2]
			),
		% TX3 applied, TX2 applied, TX1 applied
		[SignedTX, SignedTX2, SignedTX3] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX3, SignedTX2, SignedTX]
			),
		% TX1 applied, TX1 duplicate, TX1 duplicate, TX2 applied, TX4 applied
		% TX1 duplicate, TX3 applied
		% NB: Consider moving into separate test.
		[SignedTX, SignedTX2, SignedTX4, SignedTX3] =
			ar_wallet_list:filter_all_out_of_order_txs(
				WalletList,
				[SignedTX, SignedTX, SignedTX, SignedTX2, SignedTX4, SignedTX, SignedTX3]
			)
	end}.

