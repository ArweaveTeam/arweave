-module(ar_p3_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([raw_request/2, raw_request/3, http_request/1]).

-import(ar_test_node, [
	stop/0, slave_start/1, slave_start/3,
	 
	assert_wait_until_height/2,
	 wait_until_height/1, read_block_when_stored/2]).
-import(ar_p3_config_tests, [
	sample_p3_config/0, sample_p3_config/1, sample_p3_config/3, sample_p3_config/4,
	empty_p3_config/0]).

ar_p3_test_() ->
	[
		{timeout, 30, mocked_test_timeout()},
		{timeout, 30, fun test_not_found/0},
		{timeout, 30, fun test_bad_headers/0},
		{timeout, 30, fun test_valid_request/0},
		{timeout, 30, fun test_zero_rate/0},
		{timeout, 30, fun test_checksum_request/0},
		{timeout, 30, fun test_bad_config/0},
		{timeout, 30, fun test_balance_endpoint/0},
		{timeout, 30, fun test_reverse_charge/0},
		{timeout, 120, fun e2e_deposit_before_charge/0},
		{timeout, 120, fun e2e_charge_before_deposit/0},
		{timeout, 600, fun e2e_restart_p3_service/0},
		{timeout, 600, fun e2e_concurrent_requests/0}
	].

test_not_found() ->
	Config = empty_p3_config(),
	?assertEqual(
		{reply, {true, not_p3_service}, Config},
		ar_p3:handle_call({allow_request, raw_request(<<"GET">>, <<"/price/1000">>)}, [], Config)),
	?assertEqual(
		{reply, {true, not_p3_service}, Config},
		ar_p3:handle_call({allow_request, raw_request(<<"GET">>, <<"/info">>)}, [],Config)),
	?assertEqual(
		{reply, {true, not_p3_service}, Config},
		ar_p3:handle_call({allow_request, 
			raw_request(<<"GET">>, <<"/invalid_endpoint">>)}, [], Config)).

test_valid_request() ->
	Wallet = {PrivKey, PubKey} = ar_wallet:new(),
	Address = ar_wallet:to_address(Wallet),
	EncodedAddress = ar_util:encode(Address),
	{ok, _Account} = ar_p3_db:get_or_create_account(
		Address,
		PubKey,
		?ARWEAVE_AR
	),
	{ok, _} = ar_p3_db:post_deposit(
		Address,
		10000,
		crypto:strong_rand_bytes(32)
	),
	Config = sample_p3_config(),
	{_, {_, Transaction1}, _} = Result1 = ar_p3:handle_call({allow_request, 
					signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
						#{
							?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
							?P3_ADDRESS_HEADER => EncodedAddress,
							?P3_MOD_SEQ_HEADER => integer_to_binary(1)
						})}, [], Config),
	?assertMatch(
		{reply, {true, _}, Config},
		Result1,
		"Valid 'modSeq' header"),
	?assertEqual(<<"GET /price/1000">>, Transaction1#p3_transaction.description),
	
	{_, {_, Transaction2}, _} = Result2 = ar_p3:handle_call({allow_request, 
					signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
						#{
							?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
							?P3_ADDRESS_HEADER => EncodedAddress
						})}, [], Config),
	?assertMatch(
		{reply, {true, _}, Config},
		Result2,
		"Missing 'modSeq' header"),
	?assertEqual(<<"GET /price/1000">>, Transaction2#p3_transaction.description).

test_zero_rate() ->
	Wallet = {PrivKey, PubKey} = ar_wallet:new(),
	Address = ar_wallet:to_address(Wallet),
	EncodedAddress = ar_util:encode(Address),
	{ok, _Account} = ar_p3_db:get_or_create_account(
		Address,
		PubKey,
		?ARWEAVE_AR
	),
	ZeroRateConfig = sample_p3_config(crypto:strong_rand_bytes(32), 0, 2, 0),
	{_, {_, Transaction1}, _} = Result1 = ar_p3:handle_call({allow_request, 
					signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
						#{
							?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
							?P3_ADDRESS_HEADER => EncodedAddress,
							?P3_MOD_SEQ_HEADER => integer_to_binary(1)
						})}, [], ZeroRateConfig),
	?assertMatch(
		{reply, {true, _}, ZeroRateConfig},
		Result1,
		"Signed request should succeed"),
	?assertEqual(<<"GET /price/1000">>, Transaction1#p3_transaction.description),
	?assertEqual(
		{reply, {false, invalid_header}, ZeroRateConfig},
		ar_p3:handle_call({allow_request, 
			raw_request(<<"GET">>, <<"/price/1000">>)}, [], ZeroRateConfig),
		"Unsigned request should fail").


test_checksum_request() ->
	Wallet = {PrivKey, PubKey} = ar_wallet:new(),
	Address = ar_wallet:to_address(Wallet),
	Checksum = << (erlang:crc32(Address)):32 >>,
	EncodedAddress = ar_util:encode(Address),
	EncodedChecksum = ar_util:encode(Checksum),
	ValidAddress = << EncodedAddress/binary, ":", EncodedChecksum/binary>>,
	InvalidAddress = << EncodedAddress/binary, ":BAD_CHECKSUM">>,
	{ok, _Account} = ar_p3_db:get_or_create_account(
		Address,
		PubKey,
		?ARWEAVE_AR
	),
	Config = sample_p3_config(),
	?assertEqual(
		{reply, {false, insufficient_funds}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => ValidAddress
				})}, [], Config),
		"Valid checksum"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => InvalidAddress
				})}, [], Config),
		"Invalid checksum").

test_bad_headers() ->
	Wallet = {PrivKey, PubKey} = ar_wallet:new(),
	Address = ar_wallet:to_address(Wallet),
	EncodedAddress = ar_util:encode(Address),
	{ok, _Account} = ar_p3_db:get_or_create_account(
		Address,
		PubKey,
		?ARWEAVE_AR
	),
	Wallet2 = {PrivKey2, PubKey2} = ar_wallet:new(),
	Address2 = ar_wallet:to_address(Wallet2),
	EncodedAddress2 = ar_util:encode(Address2),
	{ok, _Account2} = ar_p3_db:get_or_create_account(
		Address2,
		PubKey2,
		?ARWEAVE_AR
	),
	Config = sample_p3_config(),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_MOD_SEQ_HEADER => <<>>
				})}, [], Config),
		"Empty 'modSeq' header"),
	?assertEqual(
		{reply, {false, stale_mod_seq}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_MOD_SEQ_HEADER => integer_to_binary(2)
				})}, [], Config),
		"Bad 'modSeq' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Missing 'endpoint' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<>>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Empty 'endpoint' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/chunk/{offset}">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Bad 'endpoint' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => "/price/{bytes}",
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Bad 'endpoint' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>
				})}, [], Config),
		"Missing 'address' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => <<>>
				})}, [], Config),
		"Empty 'address' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => Address
				})}, [], Config),
		"Decoded 'address' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress2
				})}, [], Config),
		"Wrong 'address' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_PRICE_HEADER => <<"bitcoin/BTC">>
				})}, [], Config),
		"Mismatch 'price' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			raw_request(<<"GET">>, <<"/price/1000">>,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Missing 'signature' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			raw_request(<<"GET">>, <<"/price/1000">>,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_SIGNATURE_HEADER => <<>>
				})}, [], Config),
		"Empty 'signature' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			raw_request(<<"GET">>, <<"/price/1000">>,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_SIGNATURE_HEADER => <<"def">>
				})}, [], Config),
		"Bad 'signature' header"),
	ValidRequest =
		signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
			#{
				?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
				?P3_ADDRESS_HEADER => EncodedAddress
			}),
	ValidHeaders = maps:get(headers, ValidRequest),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({allow_request, 
			ValidRequest#{
				headers => ValidHeaders#{
					?P3_SIGNATURE_HEADER => ar_util:decode(maps:get(?P3_SIGNATURE_HEADER, ValidHeaders))
				}
			}}, [],
			Config),
		"Decoded 'signature' header").

test_bad_config() ->
	Wallet = {PrivKey, PubKey} = ar_wallet:new(),
	Address = ar_wallet:to_address(Wallet),
	EncodedAddress = ar_util:encode(Address),
	{ok, _Account} = ar_p3_db:get_or_create_account(
		Address,
		PubKey,
		?ARWEAVE_AR
	),
	Config = sample_p3_config(),

	NoPaymentsConfig = Config#p3_config{ payments = #{} },
	?assertEqual(
		{reply, {false, invalid_header}, NoPaymentsConfig},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], NoPaymentsConfig),
		"Empty payments config"),

	MismatchedPaymentsConfig = Config#p3_config{ payments = #{
			<<"bitcoin/BTC">> => #p3_payment{
				address = crypto:strong_rand_bytes(32),
				minimum_balance = 0,
				confirmations = 2
			}
		} },
	?assertEqual(
		{reply, {false, invalid_header}, MismatchedPaymentsConfig},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], MismatchedPaymentsConfig),
		"Mismatched payments config"),

	NoRateConfig = Config#p3_config{ services = #{
			<<"/price/{bytes}">> => #p3_service{
				endpoint = <<"/price/{bytes}">>,
				mod_seq = 1,
				rate_type = <<"request">>,
				rates = #{ }
			} } },
	?assertEqual(
		{reply, {false, invalid_header}, NoRateConfig},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], NoRateConfig),
		"Empty rate config"),

	MismatchRateConfig = Config#p3_config{ services = #{
			<<"/price/{bytes}">> => #p3_service{
				endpoint = <<"/price/{bytes}">>,
				mod_seq = 1,
				rate_type = <<"request">>,
				rates = #{ <<"bitcoin/BTC">> => 1000 }
			} } },
	?assertEqual(
		{reply, {false, invalid_header}, MismatchRateConfig},
		ar_p3:handle_call({allow_request, 
			signed_request(<<"GET">>, <<"/price/1000">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], MismatchRateConfig),
		"Mismatched rate config").

test_balance_endpoint() ->
	Wallet = {PrivKey, PubKey} = ar_wallet:new(),
	Address = ar_wallet:to_address(Wallet),
	Checksum = << (erlang:crc32(Address)):32 >>,
	EncodedAddress = ar_util:encode(Address),
	{ok, _Account} = ar_p3_db:get_or_create_account(
		Address,
		PubKey,
		?ARWEAVE_AR
	),

	BadAddress = crypto:strong_rand_bytes(8),
	BadChecksum = crypto:strong_rand_bytes(6),
	?assertEqual(
		{<<"400">>, <<"Invalid address.">>},
		get_balance(BadAddress, BadChecksum, <<"arweave">>, <<"AR">>)),

	?assertEqual(
		{<<"200">>, <<"0">>},
		get_balance(crypto:strong_rand_bytes(32), <<"arweave">>, <<"AR">>)),

	TXID = crypto:strong_rand_bytes(32),
	{ok, _} = ar_p3_db:post_deposit(Address, 10, TXID),
	?assertEqual(
		{<<"200">>, <<"10">>},
		get_balance(Address, <<"arweave">>, <<"AR">>)),

	?assertEqual(
		{<<"200">>, <<"10">>},
		get_balance(Address, Checksum, <<"arweave">>, <<"AR">>)),

	?assertEqual(
		{<<"200">>, <<"0">>},
		get_balance(Address, <<"bitcoin">>, <<"BTC">>)).

test_reverse_charge() ->
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

	Request = raw_request(<<"GET">>, <<"/price/1000">>),
	{ok, Charge1} = ar_p3_db:post_charge(
		Address1,
		20,
		-20,
		Request),
	?assertEqual({ok, -20}, ar_p3_db:get_balance(Address1)),
	ar_p3:reverse_charge(Charge1),
	?assertEqual({ok, 0}, ar_p3_db:get_balance(Address1)),

	{ok, Charge2} = ar_p3_db:post_charge(
		Address1,
		0,
		0,
		Request),
	?assertEqual({ok, 0}, ar_p3_db:get_balance(Address1)),
	ar_p3:reverse_charge(Charge2),
	?assertEqual({ok, 0}, ar_p3_db:get_balance(Address1)).

mocked_test_timeout() ->
	ar_test_node:test_with_mocked_functions([{ar_p3_config, get_service_config, fun(_, _) -> timer:sleep(10000) end}],
		fun test_timeout/0).

test_timeout() ->
	?assertEqual({error, timeout}, ar_p3:allow_request(raw_request(<<"GET">>, <<"/price/1000">>))).

e2e_deposit_before_charge() ->
	Wallet1 = {Priv1, Pub1} = ar_wallet:new(),
	Wallet2 = {Priv2, Pub2} = ar_wallet:new(),
	{_, Pub3} = ar_wallet:new(),
	{_, Pub4} = ar_wallet:new(),
	RewardAddress = ar_wallet:to_address(ar_wallet:new_keyfile()),
	Sender1Address = ar_wallet:to_address(Pub1),
	EncodedSender1Address = ar_util:encode(Sender1Address),
	Sender2Address = ar_wallet:to_address(Pub2),
	EncodedSender2Address = ar_util:encode(Sender2Address),
	DepositAddress = ar_wallet:to_address(Pub3),
	OtherAddress = ar_wallet:to_address(Pub4),
	[B0] = ar_weave:init([
		{Sender1Address, ?AR(10000), <<>>},
		{Sender2Address, ?AR(10000), <<>>},
		{DepositAddress, ?AR(10000), <<>>}
	]),
	{ok, BaseConfig} = application:get_env(arweave, config),
	Config = BaseConfig#config{ p3 = sample_p3_config(DepositAddress, -100, 3) },
	ar_test_node:start(B0, RewardAddress, Config),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),
	TX1 = ar_test_node:sign_tx(Wallet1, #{ target => DepositAddress, quantity => 700, data => <<"hello">> }),
	TX2 = ar_test_node:sign_tx(Wallet1, #{ target => DepositAddress, quantity => 1200 }),
	TX3 = ar_test_node:sign_tx(Wallet2, #{ target => DepositAddress, quantity => 1000 }),
	TX4 = ar_test_node:sign_tx(Wallet1, #{ target => OtherAddress, quantity => 500 }),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	ar_test_node:assert_post_tx_to_peer(main, TX3),
	ar_test_node:assert_post_tx_to_peer(main, TX4),

	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender1Address)),
	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender2Address)),

	ar_test_node:mine(),
	wait_until_height(1),

	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender1Address)),
	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender2Address)),

	ar_test_node:mine(),
	wait_until_height(2),

	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender1Address)),
	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender2Address)),

	ar_test_node:mine(),
	wait_until_height(3),

	timer:sleep(1000),

	?assertEqual({<<"200">>, <<"1900">>}, get_balance(Sender1Address)),
	?assertEqual({<<"200">>, <<"1000">>}, get_balance(Sender2Address)),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			raw_request(<<"GET">>, <<"/info">>)
		),
		"Requesting unguarded endpoint with unsigned request"
	),

	?assertMatch(
		{ok, {{<<"400">>, _}, _, _, _, _}},
		http_request(
			raw_request(<<"GET">>, <<"/tx/%%%!%%">>)
		),
		"Requesting unguarded endpoint with client error"
	),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/info">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/info">>,
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting unguarded endpoint with signed request"
	),

	?assertMatch(
		{ok, {{<<"400">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/info">>,
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting P3 endpoint bad header"
	),

	?assertMatch(
		{ok, {{<<"428">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_MOD_SEQ_HEADER => integer_to_binary(2),
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting P3 endpoint stale modSeq"
	),

	?assertEqual(
		{<<"200">>, <<"1900">>}, get_balance(Sender1Address),
		"No balance change expected"),
	?assertEqual(
		{<<"200">>, <<"1000">>}, get_balance(Sender2Address),
		"No balance change expected"),

	?assertMatch(
		{ok, {{<<"400">>, <<"Bad Request">>}, _, 
			<<"{\"error\":\"size_must_be_an_integer\"}">>, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/abc">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_MOD_SEQ_HEADER => integer_to_binary(1),
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting P3 endpoint with client error"
	),

	?assertEqual(
		{<<"200">>, <<"1900">>}, get_balance(Sender1Address),
		"No balance change expected"),
	?assertEqual(
		{<<"200">>, <<"1000">>}, get_balance(Sender2Address),
		"No balance change expected"),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_MOD_SEQ_HEADER => integer_to_binary(1),
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting P3 endpoint with sufficient balance"
	),

	?assertEqual(
		{<<"200">>, <<"900">>}, get_balance(Sender1Address),
		"Balance change expected"),
	?assertEqual(
		{<<"200">>, <<"1000">>}, get_balance(Sender2Address),
		"No balance change expected"),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting P3 endpoint with sufficient balance"
	),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv2,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedSender2Address
				}
			)
		),
		"Requesting P3 endpoint with sufficient balance"
	),

	?assertEqual(
		{<<"200">>, <<"-100">>}, get_balance(Sender1Address),
		"Balance change expected"),
	?assertEqual(
		{<<"200">>, <<"0">>}, get_balance(Sender2Address),
		"Balance change expected"),

	?assertMatch(
		{ok, {{<<"402">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting P3 endpoint with insufficient balance"
	),

	?assertMatch(
		{ok, {{<<"402">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv2,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedSender2Address
				}
			)
		),
		"Requesting P3 endpoint with insufficient balance"
	),

	?assertEqual(
		{<<"200">>, <<"-100">>}, get_balance(Sender1Address),
		"No balance change expected"),
	?assertEqual(
		{<<"200">>, <<"0">>}, get_balance(Sender2Address),
		"No balance change expected"),
	ok = application:set_env(arweave, config, BaseConfig).

e2e_charge_before_deposit() ->
	Wallet1 = {Priv1, Pub1} = ar_wallet:new(),
	Wallet2 = {Priv2, Pub2} = ar_wallet:new(),
	{_, Pub3} = ar_wallet:new(),
	{_, Pub4} = ar_wallet:new(),
	RewardAddress = ar_wallet:to_address(ar_wallet:new_keyfile()),
	Address1 = ar_wallet:to_address(Pub1),
	EncodedAddress1 = ar_util:encode(Address1),
	Address2 = ar_wallet:to_address(Pub2),
	DepositAddress = ar_wallet:to_address(Pub3),
	[B0] = ar_weave:init([
		{Address1, ?AR(10000), <<>>}
	]),
	{ok, BaseConfig} = application:get_env(arweave, config),
	Config = BaseConfig#config{ p3 = sample_p3_config(DepositAddress, -2000, 2) },
	ar_test_node:start(B0, RewardAddress, Config),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),

	?assertMatch(
		{ok, {{<<"400">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress1
				}
			)
		),
		"Address has no transactions, so account can't be created"
	),

	?assertEqual({<<"200">>, <<"0">>}, get_balance(Address1)),

	TX1 = ar_test_node:sign_tx(Wallet1, #{ target => Address2, quantity => 10 }),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	
	ar_test_node:mine(),
	wait_until_height(1),

	ar_test_node:mine(),
	wait_until_height(2),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress1
				}
			)
		),
		"Requesting P3 endpoint before deposit"
	),

	?assertEqual({<<"200">>, <<"-1000">>}, get_balance(Address1)),

	TX2 = ar_test_node:sign_tx(Wallet1, #{ target => DepositAddress, quantity => 1200 }),
	ar_test_node:assert_post_tx_to_peer(main, TX2),
	
	ar_test_node:mine(),
	wait_until_height(3),

	ar_test_node:mine(),
	wait_until_height(4),

	timer:sleep(1000),

	?assertEqual({<<"200">>, <<"200">>}, get_balance(Address1)),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
					?P3_ADDRESS_HEADER => EncodedAddress1
				}
			)
		),
		"Requesting P3 endpoint after deposit"
	),

	?assertEqual({<<"200">>, <<"-800">>}, get_balance(Address1)),
	ok = application:set_env(arweave, config, BaseConfig).

%% @doc Test that nodes correctly scan old blocks that came in while they were offline.
e2e_restart_p3_service() ->
	Wallet1 = {_, Pub1} = ar_wallet:new(),
	{_, Pub3} = ar_wallet:new(),
	RewardAddress = ar_wallet:to_address(ar_wallet:new_keyfile()),
	Sender1Address = ar_wallet:to_address(Pub1),
	DepositAddress = ar_wallet:to_address(Pub3),
	[B0] = ar_weave:init([
		{Sender1Address, ?AR(10000), <<>>},
		{DepositAddress, ?AR(10000), <<>>}
	]),
	{ok, BaseConfig} = application:get_env(arweave, config),
	Config = BaseConfig#config{ p3 = sample_p3_config(DepositAddress, -100, 1) },
	ar_test_node:start(B0, RewardAddress, Config),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:join_on(#{ node => main, join_on => peer1 }),
	ar_test_node:disconnect_from(peer1),

	%% This deposit will be too old and will not be scanned when the main node comes back up.
	TX1 = ar_test_node:sign_tx(Wallet1, #{ target => DepositAddress, reward => ?AR(1), quantity => 100 }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX1),

	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 1),

	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 2),

	TX2 = ar_test_node:sign_tx(Wallet1, #{ target => DepositAddress, reward => ?AR(5), quantity => 500 }),
	ar_test_node:assert_post_tx_to_peer(peer1, TX2),
	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 3),

	%% Stop the main node. The peer1 will continue to mine. When the main comes back up
	%% it should correctly scan all the blocks missed since disconnectin
	%% (up to ?MAX_BLOCK_SCAN blocks)
	stop(),

	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 4),

	ar_test_node:rejoin_on(#{ node => main, join_on => peer1 }),
	?assertEqual(0, ar_p3_db:get_scan_height(),
		"Node hasn't seen any blocks yet: scan height 0"),

	%% Nodes only scan for P3 depostics when a new block is received, so the balance will be 0
	%% until the next block comes in.
	wait_until_height(4),
	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender1Address)),
	?assertEqual(0, ar_p3_db:get_scan_height(),
		"Node has seen blocks, but hasn't received a new_tip event yet: scan height 0"),

	ar_test_node:mine(peer1),
	assert_wait_until_height(peer1, 5),
	wait_until_height(5),
	%% allow time for the new_tip event to be processed
	timer:sleep(1000),
	?assertEqual(5, ar_p3_db:get_scan_height(),
		"Node has received a new_tip event: scan height 5"),

	%% We should have scanned the second deposit, and omitted the first deposit since it
	%% occurred before ?MAX_BLOCK_SCAN blocks in the past.
	?assertEqual({<<"200">>, <<"500">>}, get_balance(Sender1Address)),

	ar_test_node:disconnect_from(peer1),
	stop(),
	ar_test_node:rejoin_on(#{ node => main, join_on => peer1 }),
	?assertEqual(5, ar_p3_db:get_scan_height(),
		"Restarting node should not have reset scan height db: scan height 5"),
	
	ok = application:set_env(arweave, config, BaseConfig).

%% @doc Test that a bunch of concurrent requests don't overspend the P3 account and that they
%% are gated before they are processed (i.e. if the account does not have sufficient balance,
%% the request is not processed at all) 
e2e_concurrent_requests() ->
	Wallet1 = {Priv1, Pub1} = ar_wallet:new(),
	{_, Pub3} = ar_wallet:new(),
	RewardAddress = ar_wallet:to_address(ar_wallet:new_keyfile()),
	Address1 = ar_wallet:to_address(Pub1),
	EncodedAddress1 = ar_util:encode(Address1),
	DepositAddress = ar_wallet:to_address(Pub3),
	[B0] = ar_weave:init([
		{Address1, ?AR(10000), <<>>},
		{DepositAddress, ?AR(10000), <<>>}
	]),
	{ok, BaseConfig} = application:get_env(arweave, config),
	Config = BaseConfig#config{ p3 = sample_p3_config(DepositAddress, 0, 1, 100) },
	ar_test_node:start(B0, RewardAddress, Config),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:connect_to_peer(peer1),

	%% Post a 100 winston deposit and wait for it to be picked up.
	TX1 = ar_test_node:sign_tx(Wallet1, #{ target => DepositAddress, quantity => 100 }),
	ar_test_node:assert_post_tx_to_peer(main, TX1),
	
	ar_test_node:mine(),
	wait_until_height(1),

	timer:sleep(1000),

	?assertEqual({<<"200">>, <<"100">>}, get_balance(Address1)),

	%% Post 100 concurrent valid requests which all contain a client error. All of them
	%% should be reversed.
	NumThreads = 100,
	ar_util:pmap(
		fun(_) ->
			http_request(
				signed_request(<<"GET">>, <<"/price/abc">>, Priv1,
					#{
						?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
						?P3_ADDRESS_HEADER => EncodedAddress1
					}
				)
			)
		end,
		lists:duplicate(NumThreads,Address1)
	),

	?assertEqual({<<"200">>, <<"100">>}, get_balance(Address1)),

	{ok, AccountClientError} = ar_p3_db:get_account(Address1),
	%% Due to variation in when requests are processed, some requests will be blocked before
	%% being processed, others will be blocked after being processed. We do know that an
	%% even nmber of transactions should have been added - for each request that is processed
	%% there should be a charge and a reversal - and that at least 2 should have been added
	%% (beyond the earlier deposit trnsaction).
	Count = AccountClientError#p3_account.count,
	?assert(Count >= 3),
	?assertEqual(0, (Count-1) rem 2),

	%% Post 100 concurrent valid requests. Only 1 of them should succeed before the P3 balance
	%% is exhausted. The remaining 99 sould all fail *and* none of them should
	%% generate a reversal. This is because they should all be blocked before being processed.
	ar_util:pmap(
		fun(_) ->
			http_request(
				signed_request(<<"GET">>, <<"/price/1000">>, Priv1,
					#{
						?P3_ENDPOINT_HEADER => <<"/price/{bytes}">>,
						?P3_ADDRESS_HEADER => EncodedAddress1
					}
				)
			)
		end,
		lists:duplicate(NumThreads,Address1)
	),

	?assertEqual({<<"200">>, <<"0">>}, get_balance(Address1)),

	{ok, AccountValid} = ar_p3_db:get_account(Address1),
	?assertEqual(Count+1, AccountValid#p3_account.count),

	ok = application:set_env(arweave, config, BaseConfig).

%% ------------------------------------------------------------------
%% Private helper functions
%% ------------------------------------------------------------------

build_message(Headers) ->
	Endpoint = maps:get(?P3_ENDPOINT_HEADER, Headers, <<>>),
	Address = maps:get(?P3_ADDRESS_HEADER, Headers, <<>>),
	ModSeq = maps:get(?P3_MOD_SEQ_HEADER, Headers, <<>>),
	Price = maps:get(?P3_PRICE_HEADER, Headers, <<>>),
	Anchor = maps:get(?P3_ANCHOR_HEADER, Headers, <<>>),
	Timeout = maps:get(?P3_TIMEOUT_HEADER, Headers, <<>>),
	Message = concat([
		Endpoint,
		Address,
		ModSeq,
		Price,
		Anchor,
		Timeout
	]),
	list_to_binary(Message).

%% @doc from: https://gist.github.com/grantwinney/1a6620865d333ec227be8865b83285a2
concat(Elements) ->
    NonBinaryElements = [case Element of _ when is_binary(Element) -> binary_to_list(Element); _ -> Element end || Element <- Elements],
    lists:concat(NonBinaryElements).

get_balance(Address) ->
	get_balance(Address, <<"arweave">>, <<"AR">>).

get_balance(Address, Checksum, Network, Token) ->
	EncodedAddress = list_to_binary([ar_util:encode(Address), ":", ar_util:encode(Checksum)]),
	get_balance2(EncodedAddress, Network, Token).

get_balance(Address, Network, Token) ->
	EncodedAddress = ar_util:encode(Address),
	get_balance2(EncodedAddress, Network, Token).

get_balance2(EncodedAddress, Network, Token) ->
	Path = <<"/balance/", EncodedAddress/binary, "/", Network/binary, "/", Token/binary>>,
	{ok,{{Status, _}, _, Balance, _, _}} = http_request(
		raw_request(<<"GET">>, Path)
	),
	{Status, Balance}.

signed_request(Method, Path, PrivKey, Headers) 
		when is_map(Headers) ->
	Message = build_message(Headers),
	EncodedSignature = ar_util:encode(ar_wallet:sign(PrivKey, Message)),
	raw_request(Method, Path, Headers#{
		?P3_SIGNATURE_HEADER => EncodedSignature
	}).

raw_request(Method, Path) ->
	raw_request(Method, Path, #{}).
raw_request(Method, Path, Headers)
		when is_bitstring(Method) and is_bitstring(Path) and is_map(Headers) ->
	#{ 
		method => Method,
		path => Path,
		headers => Headers
	}.

http_request(#{method := M, path := P, headers := H}) ->
	Peer = ar_test_node:peer_ip(main),
	{_, _, _, _, Port} = Peer,
	Method = case M of
		<<"GET">> -> get;
		<<"POST">> -> post
	end,
	Path = binary_to_list(P),
	% Headers = maps:to_list(H#{<<"X-P2p-Port">> => integer_to_binary(Port)}),
	Headers = maps:to_list(H),
	ar_http:req(#{
		method => Method,
		peer => Peer,
		path => Path,
		headers => Headers
	}).

%%% XXX TO TEST:
%%%  - rescanning on ndoe restart
%%%  - what if the deposit address changes?
%%%  - same deposit twice (maybe use txid as db key)