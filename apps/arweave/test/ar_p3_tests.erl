-module(ar_p3_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([raw_request/2, raw_request/3, http_request/1]).

-import(ar_test_node, [
	start/1, start/3, slave_start/1, slave_start/3, master_peer/0, slave_peer/0, connect_to_slave/0,
	sign_tx/2, assert_post_tx_to_master/1, wait_until_height/1]).
-import(ar_p3_config_tests, [
	sample_p3_config/0, sample_p3_config/1, sample_p3_config/3, sample_p3_config/4,
	empty_p3_config/0]).

ar_p3_test_() ->
	[
		{timeout, 30, fun test_not_found/0},
		{timeout, 30, fun test_bad_headers/0},
		{timeout, 30, fun test_valid_request/0},
		{timeout, 30, fun test_zero_rate/0},
		{timeout, 30, fun test_checksum_request/0},
		{timeout, 30, fun test_bad_config/0},
		{timeout, 30, fun test_balance_endpoint/0},
		{timeout, 120, fun e2e_deposit_before_charge/0}
	].

test_not_found() ->
	Config = empty_p3_config(),
	?assertEqual(
		{reply, {true, ok}, Config},
		ar_p3:handle_call({request, raw_request(<<"GET">>, <<"/time">>)}, [], Config)),
	?assertEqual(
		{reply, {true, ok}, Config},
		ar_p3:handle_call({request, raw_request(<<"GET">>, <<"/price/1000">>)}, [],Config)),
	?assertEqual(
		{reply, {true, ok}, Config},
		ar_p3:handle_call({request, 
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
	Config = sample_p3_config(),
	?assertEqual(
		{reply, {false, insufficient_funds}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_MOD_SEQ_HEADER => integer_to_binary(1)
				})}, [], Config),
		"Valid 'modSeq' header"),
	?assertEqual(
		{reply, {false, insufficient_funds}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Missing 'modSeq' header").

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
	?assertEqual(
		{reply, {true, ok}, ZeroRateConfig},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_MOD_SEQ_HEADER => integer_to_binary(1)
				})}, [], ZeroRateConfig),
		"Signed request should succeed"),
	?assertEqual(
		{reply, {false, invalid_header}, ZeroRateConfig},
		ar_p3:handle_call({request, 
			raw_request(<<"GET">>, <<"/time">>)}, [], ZeroRateConfig),
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
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => ValidAddress
				})}, [], Config),
		"Valid checksum"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
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
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_MOD_SEQ_HEADER => <<>>
				})}, [], Config),
		"Empty 'modSeq' header"),
	?assertEqual(
		{reply, {false, stale_mod_seq}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_MOD_SEQ_HEADER => integer_to_binary(2)
				})}, [], Config),
		"Bad 'modSeq' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Missing 'endpoint' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<>>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Empty 'endpoint' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/chunk/{offset}">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Bad 'endpoint' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => "/time",
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Bad 'endpoint' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>
				})}, [], Config),
		"Missing 'address' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => <<>>
				})}, [], Config),
		"Empty 'address' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => Address
				})}, [], Config),
		"Decoded 'address' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress2
				})}, [], Config),
		"Wrong 'address' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_PRICE_HEADER => <<"bitcoin/BTC">>
				})}, [], Config),
		"Mismatch 'price' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			raw_request(<<"GET">>, <<"/time">>,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], Config),
		"Missing 'signature' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			raw_request(<<"GET">>, <<"/time">>,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_SIGNATURE_HEADER => <<>>
				})}, [], Config),
		"Empty 'signature' header"),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
			raw_request(<<"GET">>, <<"/time">>,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress,
					?P3_SIGNATURE_HEADER => <<"def">>
				})}, [], Config),
		"Bad 'signature' header"),
	ValidRequest =
		signed_request(<<"GET">>, <<"/time">>, PrivKey,
			#{
				?P3_ENDPOINT_HEADER => <<"/time">>,
				?P3_ADDRESS_HEADER => EncodedAddress
			}),
	ValidHeaders = maps:get(headers, ValidRequest),
	?assertEqual(
		{reply, {false, invalid_header}, Config},
		ar_p3:handle_call({request, 
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
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
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
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], MismatchedPaymentsConfig),
		"Mismatched payments config"),

	NoRateConfig = Config#p3_config{ services = #{
			<<"/time">> => #p3_service{
				endpoint = <<"/time">>,
				mod_seq = 1,
				rate_type = <<"request">>,
				rates = #{ }
			} } },
	?assertEqual(
		{reply, {false, invalid_header}, NoRateConfig},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedAddress
				})}, [], NoRateConfig),
		"Empty rate config"),

	MismatchRateConfig = Config#p3_config{ services = #{
			<<"/time">> => #p3_service{
				endpoint = <<"/time">>,
				mod_seq = 1,
				rate_type = <<"request">>,
				rates = #{ <<"bitcoin/BTC">> => 1000 }
			} } },
	?assertEqual(
		{reply, {false, invalid_header}, MismatchRateConfig},
		ar_p3:handle_call({request, 
			signed_request(<<"GET">>, <<"/time">>, PrivKey,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
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

	{ok, {1, _}} = ar_p3_db:post_deposit(Address, 10, crypto:strong_rand_bytes(32)),
	?assertEqual(
		{<<"200">>, <<"10">>},
		get_balance(Address, <<"arweave">>, <<"AR">>)),

	?assertEqual(
		{<<"200">>, <<"10">>},
		get_balance(Address, Checksum, <<"arweave">>, <<"AR">>)),

	?assertEqual(
		{<<"200">>, <<"0">>},
		get_balance(Address, <<"bitcoin">>, <<"BTC">>)).

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
	start(B0, RewardAddress, Config),
	slave_start(B0),
	connect_to_slave(),
	TX1 = sign_tx(Wallet1, #{ target => DepositAddress, quantity => 700, data => <<"hello">> }),
	TX2 = sign_tx(Wallet1, #{ target => DepositAddress, quantity => 1200 }),
	TX3 = sign_tx(Wallet2, #{ target => DepositAddress, quantity => 1000 }),
	TX4 = sign_tx(Wallet1, #{ target => OtherAddress, quantity => 500 }),
	assert_post_tx_to_master(TX1),
	assert_post_tx_to_master(TX2),
	assert_post_tx_to_master(TX3),
	assert_post_tx_to_master(TX4),

	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender1Address)),
	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender2Address)),

	ar_node:mine(),
	wait_until_height(1),

	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender1Address)),
	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender2Address)),

	ar_node:mine(),
	wait_until_height(2),

	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender1Address)),
	?assertEqual({<<"200">>, <<"0">>}, get_balance(Sender2Address)),

	ar_node:mine(),
	wait_until_height(3),

	timer:sleep(5000),

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
			signed_request(<<"GET">>, <<"/time">>, Priv1,
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
			signed_request(<<"GET">>, <<"/time">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
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
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/time">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
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
			signed_request(<<"GET">>, <<"/time">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting P3 endpoint with sufficient balance"
	),

	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/time">>, Priv2,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
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
			signed_request(<<"GET">>, <<"/time">>, Priv1,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
					?P3_ADDRESS_HEADER => EncodedSender1Address
				}
			)
		),
		"Requesting P3 endpoint with insufficient balance"
	),

	?assertMatch(
		{ok, {{<<"402">>, _}, _, _, _, _}},
		http_request(
			signed_request(<<"GET">>, <<"/time">>, Priv2,
				#{
					?P3_ENDPOINT_HEADER => <<"/time">>,
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
		"No balance change expected").

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
	Peer = master_peer(),
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