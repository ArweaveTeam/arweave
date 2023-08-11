-module(ar_node_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [
		start/1, start/2, slave_mine/0, wait_until_height/1, slave_start/1, slave_start/2,
		connect_to_slave/0, wait_until_receives_txs/1, assert_post_tx_to_master/1,
		assert_slave_wait_until_receives_txs/1, assert_slave_wait_until_height/1,
		slave_call/3, disconnect_from_slave/0, sign_v1_tx/3, sign_tx/2, wait_until_joined/0,
		read_block_when_stored/1, slave_peer/0, post_tx_to_master/2]).

%cannot_spend_accounts_of_other_type_test_() ->
%	test_on_fork(height_2_6, 10, fun test_cannot_spend_accounts_of_other_type/0).
%
%test_cannot_spend_accounts_of_other_type() ->
%	RSA = ar_wallet:new(),
%	RSA2 = ar_wallet:new(),
%	EDDSA = ar_wallet:new({?EDDSA_SIGN_ALG, ed25519}),
%	ECDSA = ar_wallet:new({?ECDSA_SIGN_ALG, secp256k1}),
%	[B0] = ar_weave:init([{ar_wallet:to_address(element(2, RSA)), ?AR(1000), <<>>},
%			{ar_wallet:to_address(element(2, EDDSA)), ?AR(1000), <<>>},
%			{ar_wallet:to_address(element(2, ECDSA)), ?AR(1000), <<>>},
%			{ar_wallet:to_address(element(2, RSA2)), ?AR(1000), <<>>}]),
%	start(B0),
%	slave_start(B0),
%	connect_to_slave(),
%	InvalidTXsBeforeFork = [
%		{["invalid_target_length"],
%				sign_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%						target => ar_wallet:to_address(element(2, EDDSA)) })},
%		{["invalid_target_length"],
%				sign_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%						target => ar_wallet:to_address(element(2, ECDSA)) })},
%		{["tx_malleable", "invalid_target_length"],
%				sign_v1_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%						target => ar_wallet:to_address(element(2, EDDSA)) })},
%		{["tx_malleable", "invalid_target_length"],
%				sign_v1_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%						target => ar_wallet:to_address(element(2, ECDSA)) })},
%		{["tx_signature_not_valid"], sign_tx(ECDSA, #{ last_tx => get_tx_anchor(master) })},
%		{["tx_signature_not_valid"], sign_v1_tx(ECDSA, #{ last_tx => get_tx_anchor(master) })},
%		{["tx_signature_not_valid"], sign_tx(EDDSA, #{ last_tx => get_tx_anchor(master) })},
%		{["tx_signature_not_valid"], sign_v1_tx(EDDSA, #{ last_tx => get_tx_anchor(master) })}
%	],
%	ValidTXsBeforeFork = [
%		sign_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%				target => ar_wallet:to_address(element(2, RSA2)) }),
%		sign_v1_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%				target => ar_wallet:to_address(element(2, RSA2)) })
%	],
%	lists:foreach(
%		fun({ExpectedErrors, TX}) ->
%			?assertMatch({ok, {{<<"400">>, _}, _,
%					<<"Transaction verification failed.">>, _, _}},
%					post_tx_to_master(TX)),
%			?assertEqual({ok, ExpectedErrors}, ar_tx_db:get_error_codes(TX#tx.id))
%		end,
%		InvalidTXsBeforeFork
%	),
%	lists:foreach(
%		fun(TX) ->
%			?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}}, post_tx_to_master(TX))
%		end,
%		ValidTXsBeforeFork
%	),
%	lists:foreach(fun(Height) -> ar_test_node:mine(), [_ | _] = wait_until_height(Height) end,
%			lists:seq(1, 10)),
%	InvalidTXsAfterFork = [
%		{["tx_malleable"],
%				sign_v1_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%						target => ar_wallet:to_address(element(2, EDDSA)) })},
%		{["tx_malleable"],
%				sign_v1_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%						target => ar_wallet:to_address(element(2, ECDSA)) })},
%		{["tx_signature_not_valid"], sign_v1_tx(ECDSA, #{ last_tx => get_tx_anchor(master) })},
%		{["tx_signature_not_valid"], sign_v1_tx(EDDSA, #{ last_tx => get_tx_anchor(master) })}
%	],
%	ValidTXsAfterFork = [
%		sign_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%				target => ar_wallet:to_address(element(2, EDDSA)) }),
%		sign_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%				target => ar_wallet:to_address(element(2, ECDSA)) }),
%		sign_tx(ECDSA, #{ last_tx => get_tx_anchor(master) }),
%		sign_tx(EDDSA, #{ last_tx => get_tx_anchor(master) }),
%		sign_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%				target => ar_wallet:to_address(element(2, RSA2)) }),
%		sign_v1_tx(RSA, #{ last_tx => get_tx_anchor(master), quantity => 1,
%				target => ar_wallet:to_address(element(2, RSA2)) })
%	],
%	lists:foreach(
%		fun({ErrorCodes, TX}) ->
%			?assertMatch({ok, {{<<"400">>, _}, _,
%					<<"Transaction verification failed.">>, _, _}},
%					post_tx_to_master(TX)),
%			?assertEqual({ok, ErrorCodes}, ar_tx_db:get_error_codes(TX#tx.id))
%		end,
%		InvalidTXsAfterFork
%	),
%	lists:foreach(
%		fun(TX) ->
%			?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}}, post_tx_to_master(TX))
%		end,
%		ValidTXsAfterFork
%	).
%
%multi_account_test_() ->
%	lists:map(
%		fun(Case) ->
%			test_with_mocked_functions([
%					{ar_pricing, get_miner_reward_and_endowment_pool,
%						fun({Pool, TXs, _, _, _, _, _}) ->
%								%% Give 3 Winston as a base reward, give 1 Winston for every
%								%% transaction.
%								{3 + lists:sum([1 || _ <- TXs]), Pool + 1} end},
%					%% Pay 1 Winston for every transfer.
%					{ar_pricing, get_tx_fee, fun(_, _, _, _) -> 1 end},
%					%% Pay 1 extra Winston when transferring to a new address.
%					{ar_pricing, usd_to_ar, fun(_, _, _) -> 1 end},
%					{ar_fork, height_2_6, fun() -> 0 end}],
%				fun() -> test_multi_account(Case) end)
%		end,
%		[
%			{"RSA address mining.", gb_sets:from_list([{1, {master, {1, rsa}, []}}]),
%				[{{2, eddsa}, 0}],
%				#{ 1 => [{{2, eddsa}, 0}, {{1, rsa}, 3}] }},
%			{"RSA, ECDSA, EDDSA mining.", gb_sets:from_list([{1, {master, {1, eddsa}, []}},
%								{2, {master, {2, rsa}, []}},
%								{3, {slave, {3, ecdsa}, []}},
%								{4, {master, {3, ecdsa}, []}},
%								{5, {slave, {1, eddsa}, []}},
%								{6, {master, {2, rsa}, []}},
%								{7, {master, {3, ecdsa}, []}},
%								{8, {master, {3, ecdsa}, []}},
%								{9, {master, {3, ecdsa}, []}},
%								{10, {master, {3, ecdsa}, []}},
%								{11, {master, {1, eddsa}, []}},
%								{12, {slave, {2, rsa}, []}}]),
%				[],
%				#{ 1 => [{{1, eddsa}, 3}, {{2, rsa}, 0}, {{3, ecdsa}, 0}],
%					2 => [{{1, eddsa}, 3}, {{2, rsa}, 3}, {{3, ecdsa}, 0}],
%					3 => [{{1, eddsa}, 3}, {{2, rsa}, 3}, {{3, ecdsa}, 3}],
%					4 => [{{1, eddsa}, 3}, {{2, rsa}, 3}, {{3, ecdsa}, 6}],
%					5 => [{{1, eddsa}, 6}, {{2, rsa}, 3}, {{3, ecdsa}, 6}],
%					6 => [{{1, eddsa}, 6}, {{2, rsa}, 6}, {{3, ecdsa}, 6}],
%					7 => [{{1, eddsa}, 6}, {{2, rsa}, 6}, {{3, ecdsa}, 9}],
%					8 => [{{1, eddsa}, 6}, {{2, rsa}, 6}, {{3, ecdsa}, 12}],
%					9 => [{{1, eddsa}, 6}, {{2, rsa}, 6}, {{3, ecdsa}, 15}],
%					10 => [{{1, eddsa}, 6}, {{2, rsa}, 6}, {{3, ecdsa}, 18}],
%					11 => [{{1, eddsa}, 9}, {{2, rsa}, 6}, {{3, ecdsa}, 18}],
%					12 => [{{1, eddsa}, 9}, {{2, rsa}, 9}, {{3, ecdsa}, 18}] }},
%			{"RSA, ECDSA, EDDSA mining and transfers.",
%					gb_sets:from_list([{1, {master, {1, rsa}, []}},
%								{2, {master, {1, rsa}, [
%										%% RSA -> new ECDSA.
%										{{1, rsa}, {{2, ecdsa}, 1, <<>>}}]}},
%								{3, {slave, {1, rsa}, [
%										%% RSA -> existing ECDSA.
%										{{1, rsa}, {{2, ecdsa}, 1, <<>>}},
%										{{1, rsa}, {{2, ecdsa}, 1, <<>>}}]}},
%								{4, {master, {1, rsa}, [
%										%% RSA -> new EDDSA.
%										{{1, rsa}, {{4, eddsa}, 1, <<>>}}]}},
%								{5, {slave, {4, eddsa}, [
%										%% RSA -> new RSA.
%										{{1, rsa}, {{3, rsa}, 2, <<>>}}]}},
%								{6, {master, {1, rsa}, [
%										%% RSA -> existing RSA.
%										{{3, rsa}, {{1, rsa}, 1, <<>>}},
%										%% RSA -> existing EDDSA.
%										{{1, rsa}, {{4, eddsa}, 1, <<>>}}]}},
%								{7, {master, {2, ecdsa}, []}},
%								{8, {master, {4, eddsa}, []}},
%								{9, {master, {5, ecdsa}, []}},
%								{10, {master, {6, eddsa}, [
%										%% EDDSA -> new ECDSA.
%										{{4, eddsa}, {{7, ecdsa}, 1, <<>>}},
%										%% EDDSA -> existing ECDSA.
%										{{4, eddsa}, {{2, ecdsa}, 2, <<>>}}]}},
%								{11, {master, {4, eddsa}, [
%										%% EDDSA -> new RSA.
%										{{6, eddsa}, {{8, rsa}, 1, <<>>}},
%										%% EDDSA -> existing RSA.
%										{{6, eddsa}, {{1, rsa}, 1, <<>>}},
%										%% EDDSA -> new EDDSA.
%										{{13, eddsa}, {{9, eddsa}, 1, <<>>}},
%										%% EDDSA -> existing EDDSA.
%										{{4, eddsa}, {{6, eddsa}, 1, <<>>}},
%										%% ECDSA -> new ECDSA.
%										{{2, ecdsa}, {{10, ecdsa}, 1, <<>>}},
%										%% ECDSA -> existing ECDSA.
%										{{2, ecdsa}, {{5, ecdsa}, 2, <<>>}},
%										%% ECDSA -> new RSA.
%										{{5, ecdsa}, {{11, rsa}, 1, <<>>}},
%										%% ECDSA -> existing RSA.
%										{{2, ecdsa}, {{1, rsa}, 1, <<>>}}]}},
%								{12, {slave, {5, ecdsa}, [
%										%% ECDSA -> existing EDDSA.
%										{{5, ecdsa}, {{4, eddsa}, 1, <<>>}},
%										%% RSA transfer in the same block.
%										{{1, rsa}, {{4, eddsa}, 1, <<>>}},
%										%% RSA upload.
%										{{1, rsa}, {no_address, 0, <<>>}},
%										%% ECDSA upload.
%										{{10, ecdsa}, {no_address, 0, <<>>}},
%										%% EDDSA upload.
%										{{4, eddsa}, {no_address, 0, <<>>}},
%										%% RSA upload.
%										{{1, rsa}, {no_address, 0, <<>>}},
%										%% ECDSA upload.
%										{{7, ecdsa}, {no_address, 0, <<>>}},
%										%% EDDSA upload.
%										{{6, eddsa}, {no_address, 0, <<>>}}]}},
%								{13, {master, {11, rsa}, [
%										%% ECDSA -> new EDDSA.
%										{{5, ecdsa}, {{12, eddsa}, 3, <<>>}}]}}]),
%				[{{13, eddsa}, 10}],
%				#{ 1 => [{{1, rsa}, 3}, {{2, ecdsa}, 0}, {{3, rsa}, 0}, {{4, eddsa}, 0},
%						{{5, ecdsa}, 0}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					%% 3 (current) + 3 (mined) + 1 (tx fee) - 1 (tx fee) - 1 (wallet fee)
%					%% - 1 (transferred).
%					2 => [{{1, rsa}, 4}, {{2, ecdsa}, 1}, {{3, rsa}, 0}, {{4, eddsa}, 0},
%						{{5, ecdsa}, 0}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					3 => [{{1, rsa}, 5}, {{2, ecdsa}, 3}, {{3, rsa}, 0}, {{4, eddsa}, 0},
%						{{5, ecdsa}, 0}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					4 => [{{1, rsa}, 6}, {{2, ecdsa}, 3}, {{3, rsa}, 0}, {{4, eddsa}, 1},
%						{{5, ecdsa}, 0}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					5 => [{{1, rsa}, 2}, {{2, ecdsa}, 3}, {{3, rsa}, 2}, {{4, eddsa}, 5},
%						{{5, ecdsa}, 0}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					6 => [{{1, rsa}, 6}, {{2, ecdsa}, 3}, {{3, rsa}, 0}, {{4, eddsa}, 6},
%						{{5, ecdsa}, 0}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					7 => [{{1, rsa}, 6}, {{2, ecdsa}, 6}, {{3, rsa}, 0}, {{4, eddsa}, 6},
%						{{5, ecdsa}, 0}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					8 => [{{1, rsa}, 6}, {{2, ecdsa}, 6}, {{3, rsa}, 0}, {{4, eddsa}, 9},
%						{{5, ecdsa}, 0}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					9 => [{{1, rsa}, 6}, {{2, ecdsa}, 6}, {{3, rsa}, 0}, {{4, eddsa}, 9},
%						{{5, ecdsa}, 3}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					10 => [{{1, rsa}, 6}, {{2, ecdsa}, 8}, {{3, rsa}, 0}, {{4, eddsa}, 3},
%						{{5, ecdsa}, 3}, {{6, eddsa}, 5}, {{7, ecdsa}, 1}, {{8, rsa}, 0},
%						{{9, eddsa}, 0}, {{10, ecdsa}, 0}, {{11, rsa}, 0}, {{12, eddsa}, 0},
%						{{13, eddsa}, 10}],
%					11 => [{{1, rsa}, 8}, {{2, ecdsa}, 0}, {{3, rsa}, 0}, {{4, eddsa}, 12},
%						{{5, ecdsa}, 2}, {{6, eddsa}, 1}, {{7, ecdsa}, 1}, {{8, rsa}, 1},
%						{{9, eddsa}, 1}, {{10, ecdsa}, 1}, {{11, rsa}, 1}, {{12, eddsa}, 0},
%						{{13, eddsa}, 7}],
%					12 => [{{1, rsa}, 4}, {{2, ecdsa}, 0}, {{3, rsa}, 0}, {{4, eddsa}, 13},
%						{{5, ecdsa}, 11}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 1},
%						{{9, eddsa}, 1}, {{10, ecdsa}, 0}, {{11, rsa}, 1}, {{12, eddsa}, 0},
%						{{13, eddsa}, 7}],
%					13 => [{{1, rsa}, 4}, {{2, ecdsa}, 0}, {{3, rsa}, 0}, {{4, eddsa}, 13},
%						{{5, ecdsa}, 6}, {{6, eddsa}, 0}, {{7, ecdsa}, 0}, {{8, rsa}, 1},
%						{{9, eddsa}, 1}, {{10, ecdsa}, 0}, {{11, rsa}, 5}, {{12, eddsa}, 3},
%						{{13, eddsa}, 7}] }}
%		]
%	).
%
%test_multi_account(Args) ->
%	%% ScenarioTemplate:
%	%%		a sorted map Height => {Node, MiningAddressTemplate, TransfersTemplate}.
%	%% AddressTemplate: {Number, Type}.
%	%% Type: rsa | ecdsa | eddsa.
%	%% TransfersTemplate: a map SenderAddressTemplate => {RecipientAddressTemplate, Amount,
%	%%		Payload}.
%	%% GenesisWalletsTemplate: [{AddressTemplate, Balance}].
%	%% ExpectedAccountsTemplate: a map Height => [{AddressTemplate, Balance}].
%	{Title, ScenarioTemplate, GenesisWalletsTemplate, ExpectedAccountsTemplate} = Args,
%	?debugMsg(Title),
%	%case Title of "RSA address mining." ->
%	%% Scenario: a sorted map Height => {Node, MiningAddress, Transfers}.
%	%% Transfers: a map SenderAddress => {RecipientAddress, Amount, Payload}.
%	%% Wallets: a map Address => Wallet.
%	%% GenesisWallets: [{Wallet, Balance}].
%	%% ExpectedAccounts: a map Height => [{Address, Balance}].
%	{Scenario, Wallets, GenesisWallets,
%			ExpectedAccounts} = generate_wallets(ScenarioTemplate,
%					GenesisWalletsTemplate, ExpectedAccountsTemplate),
%	[B0] = ar_weave:init(GenesisWallets, ?DEFAULT_DIFF, ?AR(1)),
%	{Master, _} = start(B0),
%	{Slave, _} = slave_start(B0),
%	connect_to_slave(),
%	Iterator = gb_sets:iterator(Scenario),
%	test_multi_account(gb_sets:next(Iterator), Title, Wallets, ExpectedAccounts,
%			Master, Slave).
%
%generate_wallets(ScenarioTemplate, GenesisWalletsTemplate, ExpectedAccountsTemplate) ->
%	{Scenario, NumberedWallets} =
%		gb_sets:fold(
%			fun({Height, {Node, MiningAddressTemplate, TransfersTemplate}},
%					{Acc, WalletsAcc}) ->
%				{MiningAddressNumber, _} = MiningAddressTemplate,
%				WalletsAcc2 = generate_wallet(MiningAddressTemplate, WalletsAcc),
%				{Transfers, WalletsAcc3} =
%					lists:foldl(
%						fun({SenderTemplate, {RecipientTemplate, Amount, Payload}},
%								{Acc1, Acc2}) ->
%							{SenderNumber, _} = SenderTemplate,
%							Acc22 = generate_wallet(SenderTemplate, Acc2),
%							SenderAddress = ar_wallet:to_address(element(2,
%									maps:get(SenderNumber, Acc22))),
%							{Acc23, RecipientAddress} =
%								case RecipientTemplate of
%									no_address ->
%										{Acc22, <<>>};
%									_ ->
%										W = generate_wallet(RecipientTemplate, Acc22),
%										{RecipientNumber, _} = RecipientTemplate,
%										{W, ar_wallet:to_address(element(2, maps:get(
%												RecipientNumber, W)))}
%								end,
%							{[{SenderAddress, {RecipientAddress, Amount, Payload}} | Acc1],
%									Acc23}
%						end,
%						{[], WalletsAcc2},
%						TransfersTemplate
%					),
%				MiningAddress = ar_wallet:to_address(element(2,
%						maps:get(MiningAddressNumber, WalletsAcc2))),
%				{gb_sets:add_element({Height, {Node, MiningAddress, Transfers}}, Acc),
%						WalletsAcc3}
%			end,
%			{gb_sets:new(), #{}},
%			ScenarioTemplate
%		),
%	{GenesisWallets, NumberedWallets2} =
%		lists:foldl(
%			fun({Template, Balance}, {Acc1, Acc2}) ->
%				Acc22 = generate_wallet(Template, Acc2),
%				{Number, _} = Template,
%				{[{ar_wallet:to_address(element(2, maps:get(Number, Acc22))), Balance,
%						<<>>} | Acc1], Acc22}
%			end,
%			{[], NumberedWallets},
%			GenesisWalletsTemplate
%		),
%	{ExpectedAccounts, NumberedWallets3} =
%		maps:fold(
%			fun(Height, BalancesTemplate, {Acc1, Acc2}) ->
%				{Balances, Acc22} =
%					lists:foldl(
%						fun({{Number, _} = Template, Balance}, {AccL1, AccL2}) ->
%							AccL22 = generate_wallet(Template, AccL2),
%							{[{ar_wallet:to_address(element(2, maps:get(Number, AccL22))),
%									Balance} | AccL1], AccL22}
%						end,
%						{[], Acc2},
%						BalancesTemplate
%					),
%				{maps:put(Height, Balances, Acc1), Acc22}
%			end,
%			{#{}, NumberedWallets2},
%			ExpectedAccountsTemplate
%		),
%	Wallets =
%		maps:fold(
%			fun(Number, {_, Pub} = Wallet, Acc) ->
%				?debugFmt("Generated a wallet with address ~s for number ~B.",
%						[ar_util:encode(ar_wallet:to_address(Pub)), Number]),
%				maps:put(ar_wallet:to_address(Pub), Wallet, Acc)
%			end,
%			#{},
%			NumberedWallets3
%		),
%	{Scenario, Wallets, GenesisWallets, ExpectedAccounts}.
%
%generate_wallet({Number, Type}, Wallets) ->
%	KeyType =
%		case Type of
%			rsa ->
%				{?RSA_SIGN_ALG, 65537};
%			ecdsa ->
%				{?ECDSA_SIGN_ALG, secp256k1};
%			eddsa ->
%				{?EDDSA_SIGN_ALG, ed25519}
%		end,
%	Wallet = maps:get(Number, Wallets, ar_wallet:new(KeyType)),
%	maps:put(Number, Wallet, Wallets).
%
%test_multi_account(none, _, _, _, _, _) ->
%	ok;
%test_multi_account({{Height, {Node, MiningAddress, Transfers}}, Iterator}, Title, Wallets,
%		ExpectedAccounts, Master, Slave) ->
%	Miner =
%		case Node of
%			master ->
%				ar_node_worker:set_reward_addr(MiningAddress),
%				{master, Master};
%			slave ->
%				slave_call(ar_node_worker, set_reward_addr, [MiningAddress]),
%				{slave, Slave}
%		end,
%	Validator = case Node of master -> {slave, Slave}; slave -> {master, Master} end,
%	TXs =
%		lists:map(
%			fun({Sender, {Recipient, Amount, Payload}}) ->
%				Wallet = maps:get(Sender, Wallets),
%				sign_tx(Wallet, #{ last_tx => get_tx_anchor(master),
%						quantity => Amount, data => Payload, target => Recipient })
%			end,
%			Transfers
%		),
%	post_and_mine(#{ miner => Miner, await_on => Validator }, TXs),
%	[{H, _, _} | _] = wait_until_height(Height),
%	lists:foreach(
%		fun({Addr, Balance}) ->
%			?assertEqual(Balance, get_balance_by_address(slave, Addr),
%					Title ++ "Address: " ++ binary_to_list(ar_util:encode(Addr))
%					++ " Height: " ++ integer_to_list(Height)),
%			?assertEqual(Balance, get_balance_by_address(master, Addr),
%					Title ++ "Address: " ++ binary_to_list(ar_util:encode(Addr))
%					++ " Height: " ++ integer_to_list(Height))
%		end,
%		maps:get(Height, ExpectedAccounts)
%	),
%	B = read_block_when_stored(H),
%	?assertEqual(lists:sort([TXID || #tx{ id = TXID } <- TXs]), lists:sort(B#block.txs)),
%	test_multi_account(gb_sets:next(Iterator), Title, Wallets, ExpectedAccounts, Master,
%			Slave).

ar_node_interface_test_() ->
	{timeout, 30, fun test_ar_node_interface/0}.

test_ar_node_interface() ->
	[B0] = ar_weave:init(),
	{_Node1, _} = start(B0),
	?assertEqual(0, ar_node:get_height()),
	?assertEqual(B0#block.indep_hash, ar_node:get_current_block_hash()),
	ar_test_node:mine(),
	B0H = B0#block.indep_hash,
	[{H, _, _}, {B0H, _, _}] = wait_until_height(1),
	?assertEqual(1, ar_node:get_height()),
	?assertEqual(H, ar_node:get_current_block_hash()).

mining_reward_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_mining_reward/0, 120).

test_mining_reward() ->
	{_Priv1, Pub1} = ar_wallet:new_keyfile(),
	[B0] = ar_weave:init(),
	{_Node1, _} = start(B0, MiningAddr = ar_wallet:to_address(Pub1)),
	ar_test_node:mine(),
	wait_until_height(1),
	B1 = ar_node:get_current_block(),
	[{MiningAddr, _, Reward, 1}, _] = B1#block.reward_history,
	?assertEqual(0, ar_node:get_balance(Pub1)),
	lists:foreach(
		fun(Height) ->
			ar_test_node:mine(),
			wait_until_height(Height + 1)
		end,
		lists:seq(1, ?REWARD_HISTORY_BLOCKS)
	),
	?assertEqual(Reward, ar_node:get_balance(Pub1)).

%% @doc Check that other nodes accept a new block and associated mining reward.
multi_node_mining_reward_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_multi_node_mining_reward/0, 120).

test_multi_node_mining_reward() ->
	{_Priv1, Pub1} = slave_call(ar_wallet, new_keyfile, []),
	[B0] = ar_weave:init(),
	start(B0),
	slave_start(B0, MiningAddr = ar_wallet:to_address(Pub1)),
	connect_to_slave(),
	slave_mine(),
	wait_until_height(1),
	B1 = ar_node:get_current_block(),
	[{MiningAddr, _, Reward, 1}, _] = B1#block.reward_history,
	?assertEqual(0, ar_node:get_balance(Pub1)),
	lists:foreach(
		fun(Height) ->
			ar_test_node:mine(),
			wait_until_height(Height + 1)
		end,
		lists:seq(1, ?REWARD_HISTORY_BLOCKS)
	),
	?assertEqual(Reward, ar_node:get_balance(Pub1)).

%% @doc Ensure that TX replay attack mitigation works.
replay_attack_test_() ->
	{timeout, 120, fun() ->
		Key1 = {_Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		{_Node1, _} = start(B0),
		slave_start(B0),
		connect_to_slave(),
		SignedTX = sign_v1_tx(master, Key1, #{ target => ar_wallet:to_address(Pub2),
				quantity => ?AR(1000), reward => ?AR(1), last_tx => <<>> }),
		assert_post_tx_to_master(SignedTX),
		ar_test_node:mine(),
		assert_slave_wait_until_height(1),
		?assertEqual(?AR(8999), slave_call(ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(1000), slave_call(ar_node, get_balance, [Pub2])),
		ar_events:send(tx, {ready_for_mining, SignedTX}),
		wait_until_receives_txs([SignedTX]),
		ar_test_node:mine(),
		assert_slave_wait_until_height(2),
		?assertEqual(?AR(8999), slave_call(ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(1000), slave_call(ar_node, get_balance, [Pub2]))
	end}.

%% @doc Create two new wallets and a blockweave with a wallet balance.
%% Create and verify execution of a signed exchange of value tx.
wallet_transaction_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_wallet_transaction/0, 120).

test_wallet_transaction() ->
	TestWalletTransaction = fun(KeyType) ->
		fun() ->
			{Priv1, Pub1} = ar_wallet:new_keyfile(KeyType),
			{_Priv2, Pub2} = ar_wallet:new(),
			TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
			SignedTX = ar_tx:sign(TX#tx{ format = 2 }, Priv1, Pub1),
			[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
			{_Node1, _} = start(B0,
					ar_wallet:to_address(ar_wallet:new_keyfile({eddsa, ed25519}))),
			slave_start(B0),
			connect_to_slave(),
			assert_post_tx_to_master(SignedTX),
			ar_test_node:mine(),
			wait_until_height(1),
			assert_slave_wait_until_height(1),
			?assertEqual(?AR(999), slave_call(ar_node, get_balance, [Pub1])),
			?assertEqual(?AR(9000), slave_call(ar_node, get_balance, [Pub2]))
		end
	end,
	[
		{"PS256_65537", timeout, 60, TestWalletTransaction({?RSA_SIGN_ALG, 65537})},
		{"ES256K", timeout, 60, TestWalletTransaction({?ECDSA_SIGN_ALG, secp256k1})},
		{"Ed25519", timeout, 60, TestWalletTransaction({?EDDSA_SIGN_ALG, ed25519})}
	].

%% @doc Wallet0 -> Wallet1 | mine | Wallet1 -> Wallet2 | mine | check
%wallet_two_transaction_test_() ->
%	test_on_fork(height_2_6, 0, fun() ->
%		{Priv1, Pub1} = ar_wallet:new({?RSA_SIGN_ALG, 65537}),
%		{Priv2, Pub2} = ar_wallet:new({?ECDSA_SIGN_ALG, secp256k1}),
%		{_Priv3, Pub3} = ar_wallet:new({?EDDSA_SIGN_ALG, ed25519}),
%		TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
%		SignedTX = ar_tx:sign(TX#tx{ format = 2 }, Priv1, Pub1),
%		TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
%		SignedTX2 = ar_tx:sign(TX2#tx{ format = 2 }, Priv2, Pub2),
%		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}], 8),
%		start(B0),
%		slave_start(B0),
%		connect_to_slave(),
%		assert_post_tx_to_master(SignedTX),
%		ar_test_node:mine(),
%		assert_slave_wait_until_height(1),
%		assert_post_tx_to_slave(SignedTX2),
%		slave_mine(),
%		wait_until_height(2),
%		?AR(999) = ar_node:get_balance(Pub1),
%		?AR(8499) = ar_node:get_balance(Pub2),
%		?AR(500) = ar_node:get_balance(Pub3)
%	end).

%% @doc Ensure that TX Id threading functions correctly (in the positive case).
tx_threading_test_() ->
	{timeout, 120, fun() ->
		Key1 = {_Priv1, Pub1} = ar_wallet:new(),
		{_Priv2, Pub2} = ar_wallet:new(),
		[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
		{_Node1, _} = start(B0),
		slave_start(B0),
		connect_to_slave(),
		SignedTX = sign_v1_tx(master, Key1, #{ target => ar_wallet:to_address(Pub2),
				quantity => ?AR(1000), reward => ?AR(1), last_tx => <<>> }),
		SignedTX2 = sign_v1_tx(master, Key1, #{ target => ar_wallet:to_address(Pub2),
				quantity => ?AR(1000), reward => ?AR(1), last_tx => SignedTX#tx.id }),
		assert_post_tx_to_master(SignedTX),
		ar_test_node:mine(),
		wait_until_height(1),
		assert_post_tx_to_master(SignedTX2),
		ar_test_node:mine(),
		assert_slave_wait_until_height(2),
		?assertEqual(?AR(7998), slave_call(ar_node, get_balance, [Pub1])),
		?assertEqual(?AR(2000), slave_call(ar_node, get_balance, [Pub2]))
	end}.

persisted_mempool_test_() ->
	%% Make the propagation delay noticeable so that the submitted transactions do not
	%% become ready for mining before the node is restarted and we assert that waiting
	%% transactions found in the persisted mempool are (re-)submitted to peers.
	ar_test_node:test_with_mocked_functions([{ar_node_worker, calculate_delay,
			fun(_Size) -> 5000 end}], fun test_persisted_mempool/0).

test_persisted_mempool() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	start(B0),
	slave_start(B0),
	disconnect_from_slave(),
	SignedTX = sign_tx(Wallet, #{ last_tx => ar_test_node:get_tx_anchor(master) }),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = post_tx_to_master(SignedTX, false),
	Mempool = ar_mempool:get_map(),
	true = ar_util:do_until(
		fun() ->
			maps:is_key(SignedTX#tx.id, Mempool)
		end,
		100,
		1000
	),
	ok = application:stop(arweave),
	ok = ar:stop_dependencies(),
	%% Rejoin the network.
	%% Expect the pending transactions to be picked up and distributed.
	{ok, Config} = application:get_env(arweave, config),
	ok = application:set_env(arweave, config, Config#config{
		start_from_block_index = false,
		peers = [slave_peer()]
	}),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	wait_until_joined(),
	assert_slave_wait_until_receives_txs([SignedTX]),
	ar_test_node:mine(),
	[{H, _, _} | _] = assert_slave_wait_until_height(1),
	B = read_block_when_stored(H),
	?assertEqual([SignedTX#tx.id], B#block.txs).
