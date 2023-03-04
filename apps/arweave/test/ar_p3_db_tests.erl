-module(ar_p3_db_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_p3_tests, [raw_request/2, raw_request/3]).

ar_p3_db_test_() ->
	[
		{timeout, 30, fun test_account/0},
	 	{timeout, 30, fun test_account_errors/0},
	 	{timeout, 30, fun test_deposit/0},
		{timeout, 30, fun test_double_deposit/0},
	 	{timeout, 30, fun test_concurrent_deposits/0},
	 	{timeout, 30, fun test_deposit_errors/0},
		{timeout, 30, fun test_charge/0},
		{timeout, 30, fun test_double_charge/0},
		{timeout, 30, fun test_concurrent_charges/0},
	 	{timeout, 30, fun test_charge_errors/0}
	].

test_account() ->
	TestStart = erlang:system_time(microsecond),
	Wallet1 = {_, PubKey1} = ar_wallet:new(),
	Wallet2 = {_, PubKey2} = ar_wallet:new(),
	Address1 = ar_wallet:to_address(Wallet1),
	Address2 = ar_wallet:to_address(Wallet2),
	{ok, Account1} = ar_p3_db:get_or_create_account(
			Address1,
			PubKey1,
			?ARWEAVE_AR),
	?assertEqual(Address1, Account1#p3_account.address),
	?assertEqual(PubKey1, Account1#p3_account.public_key),
	?assertEqual(?ARWEAVE_AR, Account1#p3_account.asset),
	?assertEqual(0, Account1#p3_account.balance),
	?assertEqual(0, Account1#p3_account.count),
	?assert(Account1#p3_account.timestamp >= TestStart),
	{ok, Account2} = ar_p3_db:get_or_create_account(
			Address2,
			PubKey2,
			?ARWEAVE_AR),
	?assertEqual(Address2, Account2#p3_account.address),
	?assertEqual(PubKey2, Account2#p3_account.public_key),
	?assertEqual(?ARWEAVE_AR, Account2#p3_account.asset),
	?assertEqual(0, Account2#p3_account.balance),
	?assertEqual(0, Account2#p3_account.count),
	?assert(Account2#p3_account.timestamp >= TestStart),
	?assertEqual(
		{ok, Account1},
		ar_p3_db:get_account(Account1#p3_account.address)),
	?assertEqual(
		{ok, Account2},
		ar_p3_db:get_account(Account2#p3_account.address)).

test_account_errors() ->
	Wallet = {_, PubKey} = ar_wallet:new(),
	{ok, Account} = ar_p3_db:get_or_create_account(
		ar_wallet:to_address(Wallet),
		PubKey,
		?ARWEAVE_AR
	),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:get_or_create_account(
			binary_to_list(ar_util:encode(Account#p3_account.address)),
			Account#p3_account.public_key,
			Account#p3_account.asset)),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:get_or_create_account(
			<<>>,
			Account#p3_account.public_key,
			Account#p3_account.asset)),
	?assertEqual(
		{error, unsupported_asset},
		ar_p3_db:get_or_create_account(
			Account#p3_account.address,
			Account#p3_account.public_key,
			<<"bitcoin/BTC">>)),
	?assertEqual(
		{error, not_found},
		ar_p3_db:get_account(<<"does_not_exist">>)),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:get_account(binary_to_list(ar_util:encode(Account#p3_account.address)))),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:get_account(<<>>)).

%% @doc Post a few deposits to 2 accounts and confirm that the right transactions are created
%% and that the account data is updated correctly. Then try to recreate one of the accounts
%% and confirm that no data is destroyed.
test_deposit() ->
	TestStart = erlang:system_time(microsecond),
	Wallet1 = {_, PubKey1} = ar_wallet:new(),
	Wallet2 = {_, PubKey2} = ar_wallet:new(),
	Address1 = ar_wallet:to_address(Wallet1),
	Address2 = ar_wallet:to_address(Wallet2),
	{ok, _} = ar_p3_db:get_or_create_account(
			Address1,
			PubKey1,
			?ARWEAVE_AR),
	{ok, _} = ar_p3_db:get_or_create_account(
			Address2,
			PubKey2,
			?ARWEAVE_AR),

	%% Post Tx1 to Account1
	TXID1 = crypto:strong_rand_bytes(32),
	{ok, {TXID1, Deposit1}} = ar_p3_db:post_deposit(Address1, 10, TXID1),
	?assertEqual(Address1, Deposit1#p3_transaction.account),
	?assertEqual(10, Deposit1#p3_transaction.amount),
	?assertEqual(TXID1, Deposit1#p3_transaction.txid),
	?assertEqual(undefined, Deposit1#p3_transaction.request),
	?assert(Deposit1#p3_transaction.timestamp > TestStart),

	?assertEqual({ok, Deposit1}, ar_p3_db:get_transaction(Address1, TXID1)),

	{ok, Account1A} = ar_p3_db:get_account(Address1),
	?assertEqual(1, Account1A#p3_account.count),

	{ok, Balance1} = ar_p3_db:get_balance(Address1),
	?assertEqual(10, Balance1),

	%% Post Tx2 to Account1
	TXID2 = crypto:strong_rand_bytes(32),
	{ok, {TXID2, Deposit2}} = ar_p3_db:post_deposit(Address1, 5, TXID2),
	?assertEqual(Address1, Deposit2#p3_transaction.account),
	?assertEqual(5, Deposit2#p3_transaction.amount),
	?assertEqual(TXID2, Deposit2#p3_transaction.txid),
	?assertEqual(undefined, Deposit2#p3_transaction.request),
	?assert(Deposit2#p3_transaction.timestamp > Deposit1#p3_transaction.timestamp),

	?assertEqual({ok, Deposit2}, ar_p3_db:get_transaction(Address1, TXID2)),

	%% Post Tx3 to Account2
	TXID3 = crypto:strong_rand_bytes(32),
	{ok, {TXID3, Deposit3}} = ar_p3_db:post_deposit(Address2, 7, TXID3),
	?assertEqual(Address2, Deposit3#p3_transaction.account),
	?assertEqual(7, Deposit3#p3_transaction.amount),
	?assertEqual(TXID3, Deposit3#p3_transaction.txid),
	?assertEqual(undefined, Deposit3#p3_transaction.request),
	?assert(Deposit3#p3_transaction.timestamp > Deposit2#p3_transaction.timestamp),

	?assertEqual({ok, Deposit3}, ar_p3_db:get_transaction(Address2, TXID3)),

	{ok, Account1B} = ar_p3_db:get_account(Address1),
	?assertEqual(2, Account1B#p3_account.count),

	{ok, Account2A} = ar_p3_db:get_account(Address2),
	?assertEqual(1, Account2A#p3_account.count),

	{ok, Balance2} = ar_p3_db:get_balance(Address1),
	?assertEqual(15, Balance2),

	{ok, Balance3} = ar_p3_db:get_balance(Address2),
	?assertEqual(7, Balance3),

	%% Confirm that we can't overwrite an account that already exists
	{ok, Account1C} = ar_p3_db:get_account(Address1),
	?assertEqual(
		{ok, Account1C},
		ar_p3_db:get_or_create_account(
			Address1,
			PubKey1,
			?ARWEAVE_AR)), 
	?assertEqual(
		{error, account_mismatch},
		ar_p3_db:get_or_create_account(
			Address1,
			PubKey2,
			?ARWEAVE_AR)), 
	?assertEqual(Address1, Account1C#p3_account.address),
	?assertEqual(PubKey1, Account1C#p3_account.public_key),
	?assertEqual(?ARWEAVE_AR, Account1C#p3_account.asset),
	?assertEqual(15, Account1C#p3_account.balance),
	?assertEqual(2, Account1C#p3_account.count),
	?assert(Account1C#p3_account.timestamp >= TestStart),

	?assertEqual({ok, Deposit1}, ar_p3_db:get_transaction(Address1, TXID1)),
	?assertEqual({ok, Deposit2}, ar_p3_db:get_transaction(Address1, TXID2)),
	?assertEqual({ok, Deposit3}, ar_p3_db:get_transaction(Address2, TXID3)).

test_double_deposit() ->
	TestStart = erlang:system_time(microsecond),
	Wallet1 = {_, PubKey1} = ar_wallet:new(),
	Address1 = ar_wallet:to_address(Wallet1),
	{ok, _} = ar_p3_db:get_or_create_account(
			Address1,
			PubKey1,
			?ARWEAVE_AR),

	%% Post Tx1 to Account1
	TXID1 = crypto:strong_rand_bytes(32),
	{ok, {TXID1, Deposit1}} = ar_p3_db:post_deposit(Address1, 10, TXID1),

	?assertEqual(
		{ok, {TXID1, Deposit1}},
		ar_p3_db:post_deposit(Address1, 10, TXID1),
		"Posting the same transaction twice should just return the first one"),

	?assertEqual(Address1, Deposit1#p3_transaction.account),
	?assertEqual(10, Deposit1#p3_transaction.amount),
	?assertEqual(TXID1, Deposit1#p3_transaction.txid),
	?assertEqual(undefined, Deposit1#p3_transaction.request),
	?assert(Deposit1#p3_transaction.timestamp > TestStart),

	?assertEqual({ok, Deposit1}, ar_p3_db:get_transaction(Address1, TXID1)),

	{ok, Account1A} = ar_p3_db:get_account(Address1),
	?assertEqual(1, Account1A#p3_account.count),

	{ok, Balance1} = ar_p3_db:get_balance(Address1),
	?assertEqual(10, Balance1).

	

test_deposit_errors() ->
	Wallet = {_, PubKey} = ar_wallet:new(),
	Address3 = ar_wallet:to_address(Wallet),
	{ok, _} = ar_p3_db:get_or_create_account(
		Address3,
		PubKey,
		?ARWEAVE_AR
	),
	?assertEqual(
		{error, not_found},
		ar_p3_db:post_deposit(
			<<"does_not_exist">>,
			5,
			crypto:strong_rand_bytes(32))),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:post_deposit(
			binary_to_list(ar_util:encode(Address3)),
			5,
			crypto:strong_rand_bytes(32))),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:post_deposit(
			<<>>,
			5,
			crypto:strong_rand_bytes(32))),
	?assertEqual(
		{error, invalid_amount},
		ar_p3_db:post_deposit(
			Address3,
			-5,
			crypto:strong_rand_bytes(32))),
	?assertEqual(
		{error, invalid_amount},
		ar_p3_db:post_deposit(
			Address3,
			5.5,
			crypto:strong_rand_bytes(32))),
	?assertEqual(
		{ok, 0},
		ar_p3_db:get_balance(<<"does_not_exist">>)),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:get_balance(binary_to_list(ar_util:encode(Address3)))),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:get_balance(<<>>)),
	?assertEqual(
		{error, not_found},
		ar_p3_db:get_transaction(<<"does_not_exist">>, 1)),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:get_transaction(binary_to_list(ar_util:encode(Address3)), 1)),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:get_transaction(<<>>, 1)),
	?assertEqual(
		{error, not_found},
		ar_p3_db:get_transaction(Address3, 1)).

test_charge() ->
	TestStart = erlang:system_time(microsecond),
	Wallet1 = {_, PubKey1} = ar_wallet:new(),
	Address1 = ar_wallet:to_address(Wallet1),
	{ok, _} = ar_p3_db:get_or_create_account(
			Address1,
			PubKey1,
			?ARWEAVE_AR),

	Request = raw_request(<<"GET">>, <<"/time">>),

	{ok, {1, Charge1}} = ar_p3_db:post_charge(
		Address1,
		20,
		-20,
		Request),
	?assertEqual(Address1, Charge1#p3_transaction.account),
	?assertEqual(-20, Charge1#p3_transaction.amount),
	?assertEqual(undefined, Charge1#p3_transaction.txid),
	?assertEqual(<<"GET /time">>, Charge1#p3_transaction.request),
	?assert(Charge1#p3_transaction.timestamp > TestStart),
	?assertEqual({ok, Charge1}, ar_p3_db:get_transaction(Address1, 1)),

	?assertEqual(-20, element(2, ar_p3_db:get_balance(Address1))),

	DepositTXID = crypto:strong_rand_bytes(32),
	{ok, {DepositTXID, Deposit1}} = ar_p3_db:post_deposit(Address1, 10, DepositTXID),
	?assertEqual({ok, Deposit1}, ar_p3_db:get_transaction(Address1, DepositTXID)),

	?assertEqual(-10, element(2, ar_p3_db:get_balance(Address1))),

	?assertEqual(
		{error, insufficient_funds},
		ar_p3_db:post_charge(
			Address1,
			20,
			-20,
			Request)),

	{ok, {3, Charge2}} = ar_p3_db:post_charge(
		Address1,
		5,
		-20,
		Request),
	?assertEqual(Address1, Charge2#p3_transaction.account),
	?assertEqual(-5, Charge2#p3_transaction.amount),
	?assertEqual(undefined, Charge2#p3_transaction.txid),
	?assertEqual(<<"GET /time">>, Charge2#p3_transaction.request),
	?assert(Charge2#p3_transaction.timestamp > TestStart),
	?assertEqual({ok, Charge2}, ar_p3_db:get_transaction(Address1, 3)),

	?assertEqual(-15, element(2, ar_p3_db:get_balance(Address1))).

test_double_charge() ->
	TestStart = erlang:system_time(microsecond),
	Wallet1 = {_, PubKey1} = ar_wallet:new(),
	Address1 = ar_wallet:to_address(Wallet1),
	{ok, _} = ar_p3_db:get_or_create_account(
			Address1,
			PubKey1,
			?ARWEAVE_AR),

	Request = raw_request(<<"GET">>, <<"/time">>),

	{ok, {1, Charge1}} = ar_p3_db:post_charge(
		Address1,
		10,
		-20,
		Request),
	?assertEqual(Address1, Charge1#p3_transaction.account),
	?assertEqual(-10, Charge1#p3_transaction.amount),
	?assertEqual(undefined, Charge1#p3_transaction.txid),
	?assertEqual(<<"GET /time">>, Charge1#p3_transaction.request),
	?assert(Charge1#p3_transaction.timestamp > TestStart),
	?assertEqual({ok, Charge1}, ar_p3_db:get_transaction(Address1, 1)),

	?assertEqual(-10, element(2, ar_p3_db:get_balance(Address1))),

	{ok, {2, Charge2}} = ar_p3_db:post_charge(
		Address1,
		10,
		-20,
		Request),
	?assertEqual(Address1, Charge2#p3_transaction.account),
	?assertEqual(-10, Charge2#p3_transaction.amount),
	?assertEqual(undefined, Charge2#p3_transaction.txid),
	?assertEqual(<<"GET /time">>, Charge2#p3_transaction.request),
	?assert(Charge2#p3_transaction.timestamp > Charge1#p3_transaction.timestamp),
	?assertEqual({ok, Charge2}, ar_p3_db:get_transaction(Address1, 2)),

	?assertEqual(-20, element(2, ar_p3_db:get_balance(Address1))).

test_charge_errors() ->
	Wallet = {_, PubKey} = ar_wallet:new(),
	Address3 = ar_wallet:to_address(Wallet),
	{ok, _} = ar_p3_db:get_or_create_account(
		Address3,
		PubKey,
		?ARWEAVE_AR
	),
	Request = raw_request(<<"GET">>, <<"/time">>),
	?assertEqual(
		{error, not_found},
		ar_p3_db:post_charge(
			<<"does_not_exist">>,
			5,
			-10,
			Request)),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:post_charge(
			binary_to_list(ar_util:encode(Address3)),
			5,
			-10,
			Request)),
	?assertEqual(
		{error, invalid_address},
		ar_p3_db:post_charge(
			<<>>,
			5,
			-10,
			Request)),
	?assertEqual(
		{error, invalid_amount},
		ar_p3_db:post_charge(
			Address3,
			-5,
			-10,
			Request)),
	?assertEqual(
		{error, invalid_amount},
		ar_p3_db:post_charge(
			Address3,
			5.5,
			-10,
			Request)),
	?assertEqual(
		{error, invalid_request},
		ar_p3_db:post_charge(
			Address3,
			5,
			-10,
			#{ method => <<"GET">> })),
	?assertEqual(
		{error, invalid_request},
		ar_p3_db:post_charge(
			Address3,
			5,
			-10,
			#{ method => "GET", path => <<"/time">> })),
	?assertEqual(
		{error, invalid_minimum},
		ar_p3_db:post_charge(
			Address3,
			5,
			0.5,
			Request)),
	?assertEqual(
		{error, insufficient_funds},
		ar_p3_db:post_charge(
			Address3,
			5,
			0,
			Request)).

%% @doc ar_p3_db relies on the fact that a gen_server will process incoming messages
%% synchronously and sequentially. Because of this we can spam a bunch of deposits and be
%% confident that there are no race conditions. To validate this you can change the line
%% ar_p3_db:post_deposit(Address, 1, crypto:strong_rand_bytes(32))
%% to
%% ar_p3_db:handle_call({post_deposit, Address, 1, crypto:strong_rand_bytes(32)}, [], [])
%% This will bypass the gen_server message queue and run the code in parallel - causing
%% the test to fail.
test_concurrent_deposits() ->
	Wallet1 = {_, PubKey1} = ar_wallet:new(),
	Address = ar_wallet:to_address(Wallet1),
	{ok, _} = ar_p3_db:get_or_create_account(
			Address,
			PubKey1,
			?ARWEAVE_AR),
	NumThreads = 100,
	ar_util:pmap(
		fun(_) ->
			ar_p3_db:post_deposit(Address, 1, crypto:strong_rand_bytes(32))
			%% Uncomment to test without gen_server message queue - test will fail:
			%% ar_p3_db:handle_call(
			%% 	{post_deposit, Address, 1, crypto:strong_rand_bytes(32)}, [], [])
		end,
		lists:duplicate(NumThreads,Address)),
	{ok, Account} = ar_p3_db:get_account(Address),
	?assertEqual(NumThreads, Account#p3_account.balance),
	?assertEqual(NumThreads, Account#p3_account.count).

test_concurrent_charges() ->
	Wallet1 = {_, PubKey1} = ar_wallet:new(),
	Address = ar_wallet:to_address(Wallet1),
	{ok, _} = ar_p3_db:get_or_create_account(
			Address,
			PubKey1,
			?ARWEAVE_AR),
	Request = raw_request(<<"GET">>, <<"/time">>),
	ar_p3_db:post_deposit(Address, 10, crypto:strong_rand_bytes(32)),
	NumThreads = 100,
	ar_util:pmap(
		fun(_) ->
			ar_p3_db:post_charge(Address, 1, 0, Request)
			%% Uncomment to test without gen_server message queue - test will fail:
			%% ar_p3_db:handle_call(
			%% 	{post_charge, Address, 1, 0, Request}, [], [])
		end,
		lists:duplicate(NumThreads,Address)),
	{ok, Account} = ar_p3_db:get_account(Address),
	?assertEqual(0, Account#p3_account.balance),
	?assertEqual(11, Account#p3_account.count).

% XXX TODO: test creating an account without a public key to verify that "use before deposit"
% feature
