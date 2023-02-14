-module(ar_http_iface_tests).

-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/0, start/1, start/2, slave_stop/0, slave_start/0, slave_start/1,
		slave_start/2, connect_to_slave/0, get_tx_anchor/0, disconnect_from_slave/0,
		wait_until_height/1, master_peer/0, wait_until_receives_txs/1, sign_tx/2, sign_tx/3,
		post_tx_json_to_master/1, assert_slave_wait_until_receives_txs/1,
		slave_wait_until_height/1, read_block_when_stored/1, read_block_when_stored/2,
		master_peer/0, slave_peer/0, slave_mine/0, assert_slave_wait_until_height/1,
		slave_call/3, assert_post_tx_to_master/1, assert_post_tx_to_slave/1,
		test_with_mocked_functions/2]).

start_node() ->
	%% Starting a node is slow so we'll run it once for the whole test module
	Wallet1 = {_, Pub1} = ar_wallet:new(),
	Wallet2 = {_, Pub2} = ar_wallet:new(),
	%% This wallet is never spent from or deposited to, so the balance is predictable
	StaticWallet = {_, Pub3} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(10000), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(10000), <<>>},
		{ar_wallet:to_address(Pub3), ?AR(10), <<"TEST_ID">>}
	], 0), %% Set difficulty to 0 to speed up tests
	start(B0),
	slave_start(B0),
	connect_to_slave(),
	{B0, Wallet1, Wallet2, StaticWallet}.

reset_node() ->
	ar_blacklist_middleware:reset(),
	slave_call(ar_blacklist_middleware, reset, []),
	case ar_peers:get_peers() of
		[] ->
			connect_to_slave();
		_ ->
			ok
	end.

setup_all_post_2_6() ->
	{Setup, Cleanup} = ar_test_node:mock_functions([
		{ar_fork, height_2_6, fun() -> 0 end}
		]),
	Functions = Setup(),
	start_node(),
	{Cleanup, Functions}.

cleanup_all_post_2_6({Cleanup, Functions}) ->
	Cleanup(Functions).

setup_one_post_2_6() ->
	reset_node(),
	disconnect_from_slave(),
	slave_call(ar_mine, stop, [miner_2_6]),
	RemoteHeight = slave_height(),
	BTip0 = ar_node:get_current_block(),
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	Key = element(1, slave_call(ar_wallet, load_key, [Config#config.mining_addr])),
	slave_mine(),
	BI = ar_test_node:assert_slave_wait_until_height(RemoteHeight + 1),
	BTip1 = slave_call(ar_storage, read_block, [hd(BI)]),
	{Key, BTip0, BTip1}.

setup_all_batch() ->
	%% Never retarget the difficulty - this ensures the tests are always
	%% run against difficulty 0. Because of this we also have to hardcode
	%% the TX fee, otherwise it can jump pretty high.
	{Setup, Cleanup} = ar_test_node:mock_functions([
		{ar_retarget, is_retarget_height, fun(_Height) -> false end},
		{ar_retarget, is_retarget_block, fun(_Block) -> false end},
		{ar_tx, get_tx_fee, fun(_Args) -> ?AR(1) end}
		]),
	Functions = Setup(),
	GenesisData = start_node(),
	{GenesisData, {Cleanup, Functions}}.

cleanup_all_batch({_GenesisData, {Cleanup, Functions}}) ->
	Cleanup(Functions).

test_register(TestFun, Fixture) ->
	{timeout, 60, {with, Fixture, [TestFun]}}.

instantiator(TestFun) ->
	fun (Fixture) -> {timeout, 60, {with, Fixture, [TestFun]}} end.

%% -------------------------------------------------------------------
%% The spammer tests must run first. All the other tests will call
%% ar_blacklist_middleware:reset() periodically and this will restart
%% the 30-second throttle counter using timer:apply_after(30000, ...).
%% However since most tests are less than 30 seconds, we end up with
%% several pending timer:apply_after that can hit, and reset the
%% throttle counter at any moment. This has caused these spammer tests
%% to fail randomly in the past depending on whether or not the
%% throttle counter was reset before the test finished.
%% -------------------------------------------------------------------

%% @doc Test that nodes sending too many requests are temporarily blocked: (a) GET.
node_blacklisting_get_spammer_test_() ->
	{timeout, 10, fun test_node_blacklisting_get_spammer/0}.

%% @doc Test that nodes sending too many requests are temporarily blocked: (b) POST.
node_blacklisting_post_spammer_test_() ->
	{timeout, 10, fun test_node_blacklisting_post_spammer/0}.

post_2_6_test_() ->
	{setup, fun setup_all_post_2_6/0, fun cleanup_all_post_2_6/1,
		{foreach, fun setup_one_post_2_6/0, [
			instantiator(fun test_reject_block_invalid_miner_reward/1),
			instantiator(fun test_reject_block_invalid_denomination/1),
			instantiator(fun test_reject_block_invalid_kryder_plus_rate_multiplier/1),
			instantiator(fun test_reject_block_invalid_kryder_plus_rate_multiplier_latch/1),
			instantiator(fun test_reject_block_invalid_endowment_pool/1),
			instantiator(fun test_reject_block_invalid_debt_supply/1),
			instantiator(fun test_reject_block_invalid_wallet_list/1),
			instantiator(fun test_add_external_block_with_invalid_timestamp/1)
		]}
	}.

batch_test_() ->
	{setup, fun setup_all_batch/0, fun cleanup_all_batch/1,
		fun ({GenesisData, _MockData}) ->
			{foreach, fun reset_node/0, [
				%% ---------------------------------------------------------
				%% The following tests must be run at a block height of 0.
				%% ---------------------------------------------------------					
				test_register(fun test_get_current_block/1, GenesisData),
				test_register(fun test_get_height/1, GenesisData),
				%% ---------------------------------------------------------
				%% The following tests are read-only and will not modify
				%% state. They assume that the blockchain state
				%% is fixed (and set by start_node and test_get_height). 
				%% ---------------------------------------------------------
				test_register(fun test_get_wallet_list_in_chunks/1, GenesisData),
				test_register(fun test_get_info/1, GenesisData),
				test_register(fun test_get_last_tx_single/1, GenesisData),
				test_register(fun test_get_block_by_hash/1, GenesisData),
				test_register(fun test_get_block_by_height/1, GenesisData),
				test_register(fun test_get_non_existent_block/1, GenesisData),
				%% ---------------------------------------------------------
				%% The following tests are *not* read-only and may modify
				%% state. They can *not* assume a fixed blockchain state. 
				%% ---------------------------------------------------------
				test_register(fun test_addresses_with_checksum/1, GenesisData),
				test_register(fun test_single_regossip/1, GenesisData),
				test_register(fun test_get_balance/1, GenesisData),
				test_register(fun test_get_format_2_tx/1, GenesisData),
				test_register(fun test_get_format_1_tx/1, GenesisData),
				test_register(fun test_add_external_tx_with_tags/1, GenesisData),
				test_register(fun test_find_external_tx/1, GenesisData),
				test_register(fun test_add_tx_and_get_last/1, GenesisData),
				test_register(fun test_get_subfields_of_tx/1, GenesisData),
				test_register(fun test_get_pending_tx/1, GenesisData),
				test_register(fun test_get_tx_body/1, GenesisData),
				test_register(fun test_get_tx_status/1, GenesisData),
				test_register(fun test_post_unsigned_tx/1, GenesisData),
				test_register(fun test_get_error_of_data_limit/1, GenesisData),
				test_register(fun test_send_missing_tx_with_the_block/1, GenesisData),
				test_register(
					fun test_fallback_to_block_endpoint_if_cannot_send_tx/1, GenesisData),
				test_register(fun test_get_recent_hash_list_diff/1, GenesisData)
			]}
		end
	}.

test_addresses_with_checksum({_, Wallet1, {_, Pub2}, _}) ->
	RemoteHeight = slave_height(),
	Address19 = crypto:strong_rand_bytes(19),
	Address65 = crypto:strong_rand_bytes(65),
	Address20 = crypto:strong_rand_bytes(20),
	Address32 = ar_wallet:to_address(Pub2),
	TX = sign_tx(Wallet1, #{ last_tx => get_tx_anchor() }),
	{JSON} = ar_serialize:tx_to_json_struct(TX),
	JSON2 = proplists:delete(<<"target">>, JSON),
	TX2 = sign_tx(Wallet1, #{ last_tx => get_tx_anchor(), target => Address32 }),
	{JSON3} = ar_serialize:tx_to_json_struct(TX2),
	InvalidPayloads = [
		[{<<"target">>, <<":">>} | JSON2],
		[{<<"target">>, << <<":">>/binary, (ar_util:encode(<< 0:32 >>))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address19))/binary, <<":">>/binary,
				(ar_util:encode(<< (erlang:crc32(Address19)):32 >> ))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address65))/binary, <<":">>/binary,
				(ar_util:encode(<< (erlang:crc32(Address65)):32 >>))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address32))/binary, <<":">>/binary,
				(ar_util:encode(<< 0:32 >>))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address20))/binary, <<":">>/binary,
				(ar_util:encode(<< 1:32 >>))/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address32))/binary, <<":">>/binary,
				(ar_util:encode(<< (erlang:crc32(Address32)):32 >>))/binary,
				<<":">>/binary >>} | JSON2],
		[{<<"target">>, << (ar_util:encode(Address32))/binary, <<":">>/binary >>} | JSON3]
	],
	lists:foreach(
		fun(Struct) ->
			Payload = ar_serialize:jsonify({Struct}),
			?assertMatch({ok, {{<<"400">>, _}, _, <<"Invalid JSON.">>, _, _}},
					post_tx_json_to_master(Payload))
		end,
		InvalidPayloads
	),
	ValidPayloads = [
		[{<<"target">>, << (ar_util:encode(Address32))/binary, <<":">>/binary,
				(ar_util:encode(<< (erlang:crc32(Address32)):32 >>))/binary >>} | JSON3],
		JSON
	],
	lists:foreach(
		fun(Struct) ->
			Payload = ar_serialize:jsonify({Struct}),
			?assertMatch({ok, {{<<"200">>, _}, _, <<"OK">>, _, _}},
					post_tx_json_to_master(Payload))
		end,
		ValidPayloads
	),
	assert_slave_wait_until_receives_txs([TX, TX2]),
	ar_node:mine(),
	[{H, _, _} | _] = slave_wait_until_height(RemoteHeight + 1),
	B = read_block_when_stored(H),
	ChecksumAddr = << (ar_util:encode(Address32))/binary, <<":">>/binary,
			(ar_util:encode(<< (erlang:crc32(Address32)):32 >>))/binary >>,
	?assertEqual(2, length(B#block.txs)),
	Balance = get_balance(ar_util:encode(Address32)),
	?assertEqual(Balance, get_balance(ChecksumAddr)),
	LastTX = get_last_tx(ar_util:encode(Address32)),
	?assertEqual(LastTX, get_last_tx(ChecksumAddr)),
	Price = get_price(ar_util:encode(Address32)),
	?assertEqual(Price, get_price(ChecksumAddr)),
	ServeTXTarget = maps:get(<<"target">>, jiffy:decode(get_tx(TX2#tx.id), [return_maps])),
	?assertEqual(ar_util:encode(TX2#tx.target), ServeTXTarget).

get_balance(EncodedAddr) ->
	Peer = master_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet/" ++ binary_to_list(EncodedAddr) ++ "/balance",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	binary_to_integer(Reply).

get_last_tx(EncodedAddr) ->
	Peer = master_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/wallet/" ++ binary_to_list(EncodedAddr) ++ "/last_tx",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	Reply.

get_price(EncodedAddr) ->
	Peer = master_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/price/0/" ++ binary_to_list(EncodedAddr),
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	binary_to_integer(Reply).

get_tx(ID) ->
	Peer = master_peer(),
	{_, _, _, _, Port} = Peer,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/tx/" ++ binary_to_list(ar_util:encode(ID)),
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	Reply.

%% @doc Ensure that server info can be retreived via the HTTP interface.
test_get_info(_) ->
	?assertEqual(<<?NETWORK_NAME>>, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, name)),
	?assertEqual({<<"release">>, ?RELEASE_NUMBER},
			ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, release)),
	?assertEqual(
		?CLIENT_VERSION,
		ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, version)),
	?assertEqual(1, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, peers)),
	ar_util:do_until(
		fun() ->
			1 == ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, blocks)
		end,
		100,
		2000
	),
	?assertEqual(1, ar_http_iface_client:get_info({127, 0, 0, 1, 1984}, height)).

%% @doc Ensure that transactions are only accepted once.
test_single_regossip(_) ->
	disconnect_from_slave(),
	TX = ar_tx:new(),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_http_iface_client:send_tx_json({127, 0, 0, 1, 1984}, TX#tx.id,
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)))
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1983}, TX#tx.id,
				ar_serialize:tx_to_binary(TX))
	),
	?assertMatch(
		{ok, {{<<"208">>, _}, _, _, _, _}},
		ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1983}, TX#tx.id,
				ar_serialize:tx_to_binary(TX))
	),
	?assertMatch(
		{ok, {{<<"208">>, _}, _, _, _, _}},
		ar_http_iface_client:send_tx_json({127, 0, 0, 1, 1983}, TX#tx.id,
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)))
	).

test_node_blacklisting_get_spammer() ->
	{ok, Config} = application:get_env(arweave, config),
	{RequestFun, ErrorResponse} = get_fun_msg_pair(get_info),
	node_blacklisting_test_frame(
		RequestFun,
		ErrorResponse,
		Config#config.requests_per_minute_limit div 2 + 1,
		1
	).

test_node_blacklisting_post_spammer() ->
	{ok, Config} = application:get_env(arweave, config),
	{RequestFun, ErrorResponse} = get_fun_msg_pair(send_tx_binary),
	NErrors = 11,
	NRequests = Config#config.requests_per_minute_limit div 2 + NErrors,
	node_blacklisting_test_frame(RequestFun, ErrorResponse, NRequests, NErrors).

%% @doc Given a label, return a fun and a message.
-spec get_fun_msg_pair(atom()) -> {fun(), any()}.
get_fun_msg_pair(get_info) ->
	{ fun(_) ->
			ar_http_iface_client:get_info({127, 0, 0, 1, 1984})
		end
	, info_unavailable};
get_fun_msg_pair(send_tx_binary) ->
	{ fun(_) ->
			InvalidTX = (ar_tx:new())#tx{ owner = <<"key">>, signature = <<"invalid">> },
			case ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1984},
					InvalidTX#tx.id, ar_serialize:tx_to_binary(InvalidTX)) of
				{ok,
					{{<<"429">>, <<"Too Many Requests">>}, _,
						<<"Too Many Requests">>, _, _}} ->
					too_many_requests;
				{ok, _} ->
					ok;
				{error, Error} ->
					?debugFmt("Unexpected response: ~p.~n", [Error]),
					?assert(false)
			end
		end
	, too_many_requests}.

%% @doc Frame to test spamming an endpoint.
%% TODO: Perform the requests in parallel. Just changing the lists:map/2 call
%% to an ar_util:pmap/2 call fails the tests currently.
-spec node_blacklisting_test_frame(fun(), any(), non_neg_integer(), non_neg_integer()) -> ok.
node_blacklisting_test_frame(RequestFun, ErrorResponse, NRequests, ExpectedErrors) ->
	slave_stop(),
	ar_blacklist_middleware:reset(),
	ar_rate_limiter:off(),
	Responses = lists:map(RequestFun, lists:seq(1, NRequests)),
	?assertEqual(length(Responses), NRequests),
	ar_blacklist_middleware:reset(),
	ByResponseType = count_by_response_type(ErrorResponse, Responses),
	Expected = #{
		error_responses => ExpectedErrors,
		ok_responses => NRequests - ExpectedErrors
	},
	?assertEqual(Expected, ByResponseType),
	ar_rate_limiter:on().

%% @doc Count the number of successful and error responses.
count_by_response_type(ErrorResponse, Responses) ->
	count_by(
		fun
			(Response) when Response == ErrorResponse -> error_responses;
			(_) -> ok_responses
		end,
		Responses
	).

%% @doc Count the occurances in the list based on the predicate.
count_by(Pred, List) ->
	maps:map(fun (_, Value) -> length(Value) end, group(Pred, List)).

%% @doc Group the list based on the key generated by Grouper.
group(Grouper, Values) ->
	group(Grouper, Values, maps:new()).

group(_, [], Acc) ->
	Acc;
group(Grouper, [Item | List], Acc) ->
	Key = Grouper(Item),
	Updater = fun (Old) -> [Item | Old] end,
	NewAcc = maps:update_with(Key, Updater, [Item], Acc),
	group(Grouper, List, NewAcc).

%% @doc Check that balances can be retreived over the network.
test_get_balance({B0, _, _, {_, Pub1}}) ->
	LocalHeight = ar_node:get_height(),
	Addr = binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet/" ++ Addr ++ "/balance"
		}),
	?assertEqual(?AR(10), binary_to_integer(Body)),
	RootHash = binary_to_list(ar_util:encode(B0#block.wallet_list)),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ RootHash ++ "/" ++ Addr ++ "/balance"
		}),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ RootHash ++ "/" ++ Addr ++ "/balance"
		}).

test_get_wallet_list_in_chunks({B0, {_, Pub1}, {_, Pub2}, {_, StaticPub}}) ->
	Addr1 = ar_wallet:to_address(Pub1),
	Addr2 = ar_wallet:to_address(Pub2),
	StaticAddr = ar_wallet:to_address(StaticPub),
	NonExistentRootHash = binary_to_list(ar_util:encode(crypto:strong_rand_bytes(32))),
	{ok, {{<<"404">>, _}, _, <<"Root hash not found.">>, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ NonExistentRootHash
		}),

	[TX] = B0#block.txs,
	GenesisAddr = ar_wallet:to_address(TX#tx.owner, {?RSA_SIGN_ALG, 65537}),
	TXID = TX#tx.id,
	ExpectedWallets = lists:sort([
			{Addr1, {?AR(10000), <<>>}},
			{Addr2, {?AR(10000), <<>>}},
			{StaticAddr, {?AR(10), <<"TEST_ID">>}},
			{GenesisAddr, {0, TXID}}]),
	{ExpectedWallets1, ExpectedWallets2} = lists:split(2, ExpectedWallets),
	RootHash = binary_to_list(ar_util:encode(B0#block.wallet_list)),
	{ok, {{<<"200">>, _}, _, Body1, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ RootHash
		}),
	Cursor = maps:get(next_cursor, binary_to_term(Body1)),
	?assertEqual(#{
			next_cursor => Cursor,
			wallets => lists:reverse(ExpectedWallets1)
		}, binary_to_term(Body1)),

	{ok, {{<<"200">>, _}, _, Body2, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet_list/" ++ RootHash ++ "/" ++ ar_util:encode(Cursor)
		}),
	?assertEqual(#{
			next_cursor => last, 
			wallets => lists:reverse(ExpectedWallets2)
		}, binary_to_term(Body2)).

%% @doc Test that heights are returned correctly.
test_get_height(_) ->
	0 = ar_http_iface_client:get_height({127, 0, 0, 1, 1984}),
	ar_node:mine(),
	wait_until_height(1),
	1 = ar_http_iface_client:get_height({127, 0, 0, 1, 1984}).

%% @doc Test that last tx associated with a wallet can be fetched.
test_get_last_tx_single({_, _, _, {_, StaticPub}}) ->
	Addr = binary_to_list(ar_util:encode(ar_wallet:to_address(StaticPub))),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet/" ++ Addr ++ "/last_tx"
		}),
	?assertEqual(<<"TEST_ID">>, ar_util:decode(Body)).

%% @doc Check that we can qickly get the local time from the peer.
get_time_test() ->
	Now = os:system_time(second),
	{ok, {Min, Max}} = ar_http_iface_client:get_time({127, 0, 0, 1, 1984}, 10 * 1000),
	?assert(Min < Now),
	?assert(Now < Max).

%% @doc Ensure that blocks can be received via a hash.
test_get_block_by_hash({B0, _, _, _}) ->
	{_Peer, B1, _Time, _Size} = ar_http_iface_client:get_block_shadow(B0#block.indep_hash,
			master_peer(), binary),
	TXIDs = [TX#tx.id || TX <- B0#block.txs],
	?assertEqual(B0#block{ size_tagged_txs = unset, account_tree = undefined, txs = TXIDs,
			reward_history = [] }, B1).

%% @doc Ensure that blocks can be received via a height.
test_get_block_by_height({B0, _, _, _}) ->
	{_Peer, B1, _Time, _Size} = ar_http_iface_client:get_block_shadow(0, master_peer(),
			binary),
	TXIDs = [TX#tx.id || TX <- B0#block.txs],
	?assertEqual(B0#block{ size_tagged_txs = unset, account_tree = undefined, txs = TXIDs,
			reward_history = [] }, B1).

test_get_current_block({B0, _, _, _}) ->
	Peer = master_peer(),
	{ok, BI} = ar_http_iface_client:get_block_index(Peer, 0, 100),
	{_Peer, B1, _Time, _Size} = ar_http_iface_client:get_block_shadow(hd(BI), Peer, binary),
	TXIDs = [TX#tx.id || TX <- B0#block.txs],
	?assertEqual(B0#block{ size_tagged_txs = unset, txs = TXIDs, reward_history = [],
			account_tree = undefined }, B1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(), path => "/block/current" }),
	{JSONStruct} = jiffy:decode(Body),
	?assertEqual(ar_util:encode(B0#block.indep_hash),
			proplists:get_value(<<"indep_hash">>, JSONStruct)).

%% @doc Test that the various different methods of GETing a block all perform
%% correctly if the block cannot be found.
test_get_non_existent_block(_) ->
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{method => get, peer => {127, 0, 0, 1, 1984},
				path => "/block/height/100"}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{method => get, peer => {127, 0, 0, 1, 1984},
				path => "/block2/height/100"}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{method => get, peer => {127, 0, 0, 1, 1984},
				path => "/block/hash/abcd"}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{method => get, peer => {127, 0, 0, 1, 1984},
				path => "/block2/hash/abcd"}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/block/height/101/wallet_list"
		}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/block/hash/abcd/wallet_list"
		}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/block/height/101/hash_list"
		}),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/block/hash/abcd/hash_list"
		}).

%% @doc A test for retrieving format=2 transactions from HTTP API.
test_get_format_2_tx(_) ->
	LocalHeight = ar_node:get_height(),
	DataRoot = (ar_tx:generate_chunk_tree(#tx{ data = <<"DATA">> }))#tx.data_root,
	ValidTX = #tx{ id = TXID } = (ar_tx:new(<<"DATA">>))#tx{
			format = 2,
			data_root = DataRoot },
	InvalidDataRootTX = #tx{ id = InvalidTXID } = (ar_tx:new(<<"DATA">>))#tx{ format = 2 },
	EmptyTX = #tx{ id = EmptyTXID } = (ar_tx:new())#tx{ format = 2 },
	EncodedTXID = binary_to_list(ar_util:encode(TXID)),
	EncodedInvalidTXID = binary_to_list(ar_util:encode(InvalidTXID)),
	EncodedEmptyTXID = binary_to_list(ar_util:encode(EmptyTXID)),
	ar_http_iface_client:send_tx_json({127, 0, 0, 1, 1984}, ValidTX#tx.id,
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(ValidTX))),
	{ok, {{<<"400">>, _}, _, <<"The attached data is split in an unknown way.">>, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx",
			body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(InvalidDataRootTX))
		}),
	ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1984},
			InvalidDataRootTX#tx.id,
			ar_serialize:tx_to_binary(InvalidDataRootTX#tx{ data = <<>> })),
	ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1984}, EmptyTX#tx.id,
			ar_serialize:tx_to_binary(EmptyTX)),
	wait_until_receives_txs([ValidTX, EmptyTX, InvalidDataRootTX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	%% Ensure format=2 transactions can be retrieved over the HTTP
	%% interface with no populated data, while retaining info on all other fields.
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ EncodedTXID
		}),
	?assertEqual(ValidTX#tx{ 
			data = <<>>,
			data_size = 4
		}, ar_serialize:json_struct_to_tx(Body)),
	%% Ensure data can be fetched for format=2 transactions via /tx/[ID]/data.
	{ok, Data} = wait_until_syncs_tx_data(TXID),
	?assertEqual(ar_util:encode(<<"DATA">>), Data),
	{ok, {{<<"200">>, _}, _, InvalidData, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ EncodedInvalidTXID ++ "/data"
		}),
	?assertEqual(<<>>, InvalidData),
	%% Ensure /tx/[ID]/data works for format=2 transactions when the data is empty.
	{ok, {{<<"200">>, _}, _, <<>>, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ EncodedEmptyTXID ++ "/data"
		}),
	%% Ensure data can be fetched for format=2 transactions via /tx/[ID]/data.html.
	{ok, {{<<"200">>, _}, Headers, HTMLData, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ EncodedTXID ++ "/data.html"
		}),
	?assertEqual(<<"DATA">>, HTMLData),
	?assertEqual(
		[{<<"content-type">>, <<"text/html">>}],
		proplists:lookup_all(<<"content-type">>, Headers)
	).

test_get_format_1_tx(_) ->
	LocalHeight = ar_node:get_height(),
	TX = #tx{ id = TXID } = ar_tx:new(<<"DATA">>),
	EncodedTXID = binary_to_list(ar_util:encode(TXID)),
	ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1984}, TX#tx.id,
			ar_serialize:tx_to_binary(TX)),
	wait_until_receives_txs([TX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, Body} =
		ar_util:do_until(
			fun() ->
				case ar_http:req(#{
					method => get,
					peer => {127, 0, 0, 1, 1984},
					path => "/tx/" ++ EncodedTXID
				}) of
					{ok, {{<<"404">>, _}, _, _, _, _}} ->
						false;
					{ok, {{<<"200">>, _}, _, Payload, _, _}} ->
						{ok, Payload}
				end
			end,
			100,
			2000
		),
	?assertEqual(TX, ar_serialize:json_struct_to_tx(Body)).

%% @doc Test adding transactions to a block.
test_add_external_tx_with_tags(_) ->
	LocalHeight = ar_node:get_height(),
	TX = ar_tx:new(<<"DATA">>),
	TaggedTX =
		TX#tx {
			tags =
				[
					{<<"TEST_TAG1">>, <<"TEST_VAL1">>},
					{<<"TEST_TAG2">>, <<"TEST_VAL2">>}
				]
		},
	ar_http_iface_client:send_tx_json({127, 0, 0, 1, 1984}, TaggedTX#tx.id,
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TaggedTX))),
	wait_until_receives_txs([TaggedTX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	[B1Hash | _] = ar_node:get_blocks(),
	B1 = read_block_when_stored(B1Hash, true),
	TXID = TaggedTX#tx.id,
	?assertEqual([TXID], [TX2#tx.id || TX2 <- B1#block.txs]),
	?assertEqual(TaggedTX, ar_storage:read_tx(hd(B1#block.txs))).

%% @doc Test getting transactions
test_find_external_tx(_) ->
	LocalHeight = ar_node:get_height(),
	TX = ar_tx:new(<<"DATA">>),
	ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1984}, TX#tx.id,
			ar_serialize:tx_to_binary(TX)),
	wait_until_receives_txs([TX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, FoundTXID} =
		ar_util:do_until(
			fun() ->
				case ar_http_iface_client:get_tx([master_peer()], TX#tx.id, maps:new()) of
					not_found ->
						false;
					TX ->
						{ok, TX#tx.id}
				end
			end,
			100,
			5000
		),
	?assertEqual(FoundTXID, TX#tx.id).

rejects_invalid_blocks_pre_fork_2_6_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> infinity end}],
		fun test_rejects_invalid_blocks_pre_fork_2_6/0).

test_rejects_invalid_blocks_pre_fork_2_6() ->
	[B0] = ar_weave:init([], ar_retarget:switch_to_linear_diff(10)),
	start(B0),
	{_Slave, _} = slave_start(B0),
	disconnect_from_slave(),
	slave_mine(),
	Peer = {127, 0, 0, 1, 1984},
	BI = ar_test_node:assert_slave_wait_until_height(1),
	B1 = slave_call(ar_storage, read_block, [hd(BI)]),
	%% Try to post an invalid block.
	InvalidH = crypto:strong_rand_bytes(48),
	ok = ar_events:subscribe(block),
	post_block(B1#block{ indep_hash = InvalidH }, invalid_hash),
	%% Verify the IP address of self is NOT banned in ar_blacklist_middleware.
	InvalidH2 = crypto:strong_rand_bytes(48),
	post_block(B1#block{ indep_hash = InvalidH2 }, invalid_hash),
	%% The valid block with the ID from the failed attempt can still go through.
	post_block(B1, valid),
	%% Try to post the same block again.
	Peer = ar_test_node:master_peer(),
	?assertMatch({ok, {{<<"208">>, _}, _, _, _, _}}, send_new_block(Peer, B1)),
	%% Correct hash, but invalid PoW.
	B2 = B1#block{ reward_addr = crypto:strong_rand_bytes(32) },
	InvalidH3 = ar_block:indep_hash(B2),
	timer:sleep(100 * 2), % ?THROTTLE_BY_IP_INTERVAL_MS * 2
	post_block(B2#block{ indep_hash = InvalidH3 }, invalid_pow),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset().

rejects_invalid_blocks_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_rejects_invalid_blocks/0).

test_rejects_invalid_blocks() ->
	[B0] = ar_weave:init([], ar_retarget:switch_to_linear_diff(2)),
	start(B0),
	{_Slave, _} = slave_start(B0),
	disconnect_from_slave(),
	slave_mine(),
	BI = ar_test_node:assert_slave_wait_until_height(1),
	B1 = slave_call(ar_storage, read_block, [hd(BI)]),
	%% Try to post an invalid block.
	InvalidH = crypto:strong_rand_bytes(48),
	ok = ar_events:subscribe(block),
	post_block(B1#block{ indep_hash = InvalidH }, invalid_hash),
	%% Verify the IP address of self is NOT banned in ar_blacklist_middleware.
	InvalidH2 = crypto:strong_rand_bytes(48),
	post_block(B1#block{ indep_hash = InvalidH2 }, invalid_hash),
	%% The valid block with the ID from the failed attempt can still go through.
	post_block(B1, valid),
	%% Try to post the same block again.
	Peer = ar_test_node:master_peer(),
	?assertMatch({ok, {{<<"208">>, _}, _, _, _, _}}, send_new_block(Peer, B1)),
	%% Correct hash, but invalid signature.
	B2Preimage = B1#block{ signature = <<>> },
	B2 = B2Preimage#block{ indep_hash = ar_block:indep_hash(B2Preimage) },
	post_block(B2, invalid_signature),
	%% Nonce limiter output too far in the future.
	Info1 = B1#block.nonce_limiter_info,
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	Key = element(1, slave_call(ar_wallet, load_key, [Config#config.mining_addr])),
	B3 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = Info1#nonce_limiter_info{
				global_step_number = 100000 } }, B0, Key),
	post_block(B3, invalid_nonce_limiter_global_step_number),
	%% Nonce limiter output lower than that of the previous block.
	B4 = sign_block(B1#block{ previous_block = B1#block.indep_hash,
			previous_cumulative_diff = B1#block.cumulative_diff,
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			height = B1#block.height + 1,
			nonce_limiter_info = Info1#nonce_limiter_info{ global_step_number = 1 } },
			B1, Key),
	post_block(B4, invalid_nonce_limiter_global_step_number),
	B1SolutionH = B1#block.hash,
	B1SolutionNum = binary:decode_unsigned(B1SolutionH),
	B5 = sign_block(B1#block{ previous_block = B1#block.indep_hash,
			previous_cumulative_diff = B1#block.cumulative_diff,
			height = B1#block.height + 1,
			hash = binary:encode_unsigned(B1SolutionNum - 1) }, B1, Key),
	post_block(B5, invalid_nonce_limiter_global_step_number),
	%% Correct hash, but invalid PoW.
	InvalidKey = ar_wallet:new(),
	InvalidAddr = ar_wallet:to_address(InvalidKey),
	B6 = sign_block(B1#block{ reward_addr = InvalidAddr,
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			reward_key = element(2, InvalidKey) }, B0, element(1, InvalidKey)),
	timer:sleep(100 * 2), % ?THROTTLE_BY_IP_INTERVAL_MS * 2
	post_block(B6, [invalid_hash_preimage, invalid_pow]),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B7 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			%% Also, here it changes the block hash (the previous one would be ignored),
			%% because the poa field does not explicitly go in there (the motivation is to have
			%% a "quick pow" step which is quick to validate and somewhat expensive to
			%% forge).
			hash = crypto:strong_rand_bytes(32),
			poa = (B1#block.poa)#poa{ chunk = <<"a">> } }, B0, Key),
	post_block(B7, invalid_pow),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B8 = sign_block(B1#block{ last_retarget = 100000 }, B0, Key),
	post_block(B8, invalid_last_retarget),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B9 = sign_block(B1#block{ diff = 100000 }, B0, Key),
	post_block(B9, invalid_difficulty),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B10 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce = 100 }, B0, Key),
	post_block(B10, invalid_nonce),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B11_1 = sign_block(B1#block{ partition_number = 1 }, B0, Key),
	%% We might get invalid_hash_preimage occasionally, because the partition number
	%% changes H0 which changes the solution hash which may happen to be lower than
	%% the difficulty.
	post_block(B11_1, [invalid_resigned_solution_hash, invalid_hash_preimage]),
	B11 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			partition_number = 1 }, B0, Key),
	post_block(B11, [invalid_partition_number, invalid_hash_preimage]),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B12 = sign_block(B1#block{
			nonce_limiter_info = (B1#block.nonce_limiter_info)#nonce_limiter_info{
					last_step_checkpoints = [crypto:strong_rand_bytes(32)] } }, B0, Key),
	%% Reset the node to the genesis block.
	start(B0),
	ok = ar_events:subscribe(block),
	post_block(B12, invalid_nonce_limiter),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B13 = sign_block(B1#block{ poa = (B1#block.poa)#poa{ data_path = <<>> } }, B0, Key),
	post_block(B13, invalid_poa),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B14 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = (B1#block.nonce_limiter_info)#nonce_limiter_info{
					next_seed = crypto:strong_rand_bytes(48) } }, B0, Key),
	post_block(B14, invalid_nonce_limiter_seed_data),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B15 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = (B1#block.nonce_limiter_info)#nonce_limiter_info{
					partition_upper_bound = 10000000 } }, B0, Key),
	post_block(B15, invalid_nonce_limiter_seed_data),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset(),
	B16 = sign_block(B1#block{
			%% Change the solution hash so that the validator does not go down
			%% the comparing the resigned solution with the cached solution path.
			hash = crypto:strong_rand_bytes(32),
			nonce_limiter_info = (B1#block.nonce_limiter_info)#nonce_limiter_info{
					next_partition_upper_bound = 10000000 } }, B0, Key),
	post_block(B16, invalid_nonce_limiter_seed_data),
	?assertMatch({ok, {{<<"403">>, _}, _,
			<<"IP address blocked due to previous request.">>, _, _}},
			send_new_block(Peer, B1#block{ indep_hash = crypto:strong_rand_bytes(48) })),
	ar_blacklist_middleware:reset().

test_reject_block_invalid_denomination({Key, BTip0, BTip1}) ->
	ok = ar_events:subscribe(block),
	B = sign_block(BTip1#block{ denomination = 0 }, BTip0, Key),
	post_block(B, invalid_denomination).

rejects_blocks_with_invalid_double_signing_proof_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun test_reject_block_invalid_double_signing_proof/0).

test_reject_block_invalid_double_signing_proof() ->
	Key0 = ar_wallet:new(),
	Addr0 = ar_wallet:to_address(Key0),
	[B0] = ar_weave:init([{Addr0, ?AR(1000), <<>>}], ar_retarget:switch_to_linear_diff(2)),
	start(B0),
	slave_start(B0),
	disconnect_from_slave(),
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	ok = ar_events:subscribe(block),
	{Key, _} = FullKey = slave_call(ar_wallet, load_key, [Config#config.mining_addr]),
	TX0 = sign_tx(Key0, #{ target => ar_wallet:to_address(Key), quantity => ?AR(10) }),
	assert_post_tx_to_slave(TX0),
	assert_post_tx_to_master(TX0),
	slave_mine(),
	BI = ar_test_node:assert_slave_wait_until_height(1),
	B1 = slave_call(ar_storage, read_block, [hd(BI)]),
	Random512 = crypto:strong_rand_bytes(512),
	Random64 = crypto:strong_rand_bytes(64),
	InvalidProof = {Random512, Random512, 2, 1, Random64, Random512, 3, 2, Random64},
	B2 = sign_block(B1#block{ double_signing_proof = InvalidProof }, B0, Key),
	post_block(B2, invalid_double_signing_proof_same_signature),
	Random512_2 = crypto:strong_rand_bytes(512),
	InvalidProof_2 = {Random512, Random512, 2, 1, Random64, Random512_2, 3, 2, Random64},
	B2_2 = sign_block(B1#block{ double_signing_proof = InvalidProof_2 }, B0, Key),
	post_block(B2_2, invalid_double_signing_proof_cdiff),
	CDiff = B1#block.cumulative_diff,
	PrevCDiff = B0#block.cumulative_diff,
	SignedH = ar_block:generate_signed_hash(B1),
	Preimage1 = << (B0#block.hash)/binary, SignedH/binary >>,
	Preimage2 = << (B0#block.hash)/binary, (crypto:strong_rand_bytes(32))/binary >>,
	SignaturePreimage = << (ar_serialize:encode_int(CDiff, 16))/binary,
					(ar_serialize:encode_int(PrevCDiff, 16))/binary, Preimage2/binary >>,
	Signature2 = ar_wallet:sign(Key, SignaturePreimage),
	%% We cannot ban ourselves.
	InvalidProof2 = {element(3, Key), B1#block.signature, CDiff, PrevCDiff, Preimage1,
			Signature2, CDiff, PrevCDiff, Preimage2},
	B3 = sign_block(B1#block{ double_signing_proof = InvalidProof2 }, B0, Key),
	post_block(B3, invalid_double_signing_proof_same_address),
	slave_mine(),
	BI2 = ar_test_node:assert_slave_wait_until_height(2),
	{ok, MasterConfig} = application:get_env(arweave, config),
	Key2 = element(1, ar_wallet:load_key(MasterConfig#config.mining_addr)),
	Preimage3 = << (B0#block.hash)/binary, (crypto:strong_rand_bytes(32))/binary >>,
	Preimage4 = << (B0#block.hash)/binary, (crypto:strong_rand_bytes(32))/binary >>,
	SignaturePreimage3 = << (ar_serialize:encode_int(CDiff, 16))/binary,
					(ar_serialize:encode_int(PrevCDiff, 16))/binary, Preimage3/binary >>,
	SignaturePreimage4 = << (ar_serialize:encode_int(CDiff, 16))/binary,
					(ar_serialize:encode_int(PrevCDiff, 16))/binary, Preimage4/binary >>,
	Signature3 = ar_wallet:sign(Key, SignaturePreimage3),
	Signature4 = ar_wallet:sign(Key, SignaturePreimage4),
	%% The account address is not in the reward history.
	InvalidProof3 = {element(3, Key2), Signature3, CDiff, PrevCDiff, Preimage3,
			Signature4, CDiff, PrevCDiff, Preimage4},
	B5 = sign_block(B1#block{ double_signing_proof = InvalidProof3 }, B0, Key),
	post_block(B5, invalid_double_signing_proof_not_in_reward_history),
	connect_to_slave(),
	wait_until_height(2),
	B6 = slave_call(ar_storage, read_block, [hd(BI2)]),
	B7 = sign_block(B6, B1, Key),
	post_block(B7, valid),
	ar_node:mine(),
	BI3 = assert_slave_wait_until_height(3),
	B8 = slave_call(ar_storage, read_block, [hd(BI3)]),
	?assertNotEqual(undefined, B8#block.double_signing_proof),
	RewardAddr = B8#block.reward_addr,
	BannedAddr = ar_wallet:to_address(Key),
	Accounts = ar_wallets:get(B8#block.wallet_list, [BannedAddr, RewardAddr]),
	?assertMatch(#{ BannedAddr := {_, _, 1, false}, RewardAddr := {_, _} }, Accounts),
	%% The banned address may still use their accounts for transfers/uploads.
	Key3 = ar_wallet:new(),
	Target = ar_wallet:to_address(Key3),
	TX1 = sign_tx(FullKey, #{ last_tx => <<>>, quantity => 1, target => Target }),
	TX2 = sign_tx(FullKey, #{ last_tx => get_tx_anchor(), data => <<"a">> }),
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, [TX1, TX2]),
	ar_node:mine(),
	BI4 = assert_slave_wait_until_height(4),
	B9 = slave_call(ar_storage, read_block, [hd(BI4)]),
	Accounts2 = ar_wallets:get(B9#block.wallet_list, [BannedAddr, Target]),
	TXID = TX2#tx.id,
	?assertEqual(2, length(B9#block.txs)),
	?assertMatch(#{ Target := {1, <<>>}, BannedAddr := {_, TXID, 1, false} }, Accounts2).

test_reject_block_invalid_kryder_plus_rate_multiplier({Key, BTip0, BTip1}) ->
	ok = ar_events:subscribe(block),
	B = sign_block(BTip1#block{ kryder_plus_rate_multiplier = 0 }, BTip0, Key),
	post_block(B, invalid_kryder_plus_rate_multiplier).

test_reject_block_invalid_kryder_plus_rate_multiplier_latch({Key, BTip0, BTip1}) ->
	ok = ar_events:subscribe(block),
	B = sign_block(BTip1#block{ kryder_plus_rate_multiplier_latch = 2 }, BTip0, Key),
	post_block(B, invalid_kryder_plus_rate_multiplier_latch).

test_reject_block_invalid_endowment_pool({Key, BTip0, BTip1}) ->
	ok = ar_events:subscribe(block),
	B = sign_block(BTip1#block{ reward_pool = 2 }, BTip0, Key),
	post_block(B, invalid_reward_pool).

test_reject_block_invalid_debt_supply({Key, BTip0, BTip1}) ->
	ok = ar_events:subscribe(block),
	B = sign_block(BTip1#block{ debt_supply = 100000000 }, BTip0, Key),
	post_block(B, invalid_debt_supply).

test_reject_block_invalid_miner_reward({Key, BTip0, _}) ->
	ok = ar_events:subscribe(block),
	BTip1 = slave_call(ar_node, get_current_block, []),
	BTip2 = sign_block(BTip1#block{ reward = 0 }, BTip0, Key),
	post_block(BTip2, invalid_reward_history_hash),
	HashRate = ar_difficulty:get_hash_rate(BTip2#block.diff),
	RewardHistory = tl(BTip2#block.reward_history),
	Addr = BTip2#block.reward_addr,
	BTip3 = sign_block(BTip2#block{
			reward_history_hash = ar_block:reward_history_hash([{Addr, HashRate, 0, 1}
					| RewardHistory]) }, BTip0, Key),
	post_block(BTip3, invalid_miner_reward).

test_reject_block_invalid_wallet_list({Key, BTip0, BTip1}) ->
	ok = ar_events:subscribe(block),
	B = sign_block(BTip1#block{ wallet_list = crypto:strong_rand_bytes(32) }, BTip0, Key),
	post_block(B, invalid_wallet_list).

post_block(B, ExpectedResult) when not is_list(ExpectedResult) ->
	post_block(B, [ExpectedResult], ar_test_node:master_peer());
post_block(B, ExpectedResults) ->
	post_block(B, ExpectedResults, ar_test_node:master_peer()).

post_block(B, ExpectedResults, Peer) ->
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B)),
	await_post_block(B, ExpectedResults, Peer).

await_post_block(B, ExpectedResults) ->
	await_post_block(B, ExpectedResults, ar_test_node:master_peer()).

await_post_block(#block{ indep_hash = H } = B, ExpectedResults, Peer) ->
	PostGossipFailureCodes = [invalid_denomination,
			invalid_double_signing_proof_same_signature, invalid_double_signing_proof_cdiff,
			invalid_double_signing_proof_same_address,
			invalid_double_signing_proof_not_in_reward_history,
			invalid_double_signing_proof_already_banned,
			invalid_double_signing_proof_invalid_signature,
			mining_address_banned, invalid_account_anchors, invalid_reward_pool,
			invalid_miner_reward, invalid_debt_supply, invalid_reward_history_hash,
			invalid_kryder_plus_rate_multiplier_latch, invalid_kryder_plus_rate_multiplier,
			invalid_wallet_list],
	receive
		{event, block, {rejected, Reason, H, Peer2}} ->
			case lists:member(Reason, PostGossipFailureCodes) of
				true ->
					?assertEqual(no_peer, Peer2);
				false ->
					?assertEqual(Peer, Peer2)
			end,
			case lists:member(Reason, ExpectedResults) of
				true ->
					ok;
				_ ->
					?assert(false, iolist_to_binary(io_lib:format("Unexpected "
							"validation failure: ~p. Expected: ~p.",
							[Reason, ExpectedResults])))
			end;
		{event, block, {new, #block{ indep_hash = H }, #{ source := {peer, Peer} }}} ->
			case ExpectedResults of
				[valid] ->
					ok;
				_ ->
					case lists:any(fun(FailureCode) -> not lists:member(FailureCode,
							PostGossipFailureCodes) end, ExpectedResults) of
						true ->
							?assert(false, iolist_to_binary(io_lib:format("Unexpected "
									"validation success. Expected: ~p.", [ExpectedResults])));
						false ->
							await_post_block(B, ExpectedResults)
					end
			end
	after 5000 ->
			?assert(false, iolist_to_binary(io_lib:format("Timed out. Expected: ~p.",
					[ExpectedResults])))
	end.

sign_block(#block{ cumulative_diff = CDiff } = B, PrevB, Key) ->
	SignedH = ar_block:generate_signed_hash(B),
	PrevCDiff = PrevB#block.cumulative_diff,
	Signature = ar_wallet:sign(Key, << (ar_serialize:encode_int(CDiff, 16))/binary,
		(ar_serialize:encode_int(PrevCDiff, 16))/binary,
		(B#block.previous_solution_hash)/binary, SignedH/binary >>),
	H = ar_block:indep_hash2(SignedH, Signature),
	B#block{ indep_hash = H, signature = Signature }.

add_external_block_with_invalid_timestamp_pre_fork_2_6_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> infinity end}],
		fun test_add_external_block_with_invalid_timestamp_pre_fork_2_6/0).

test_add_external_block_with_invalid_timestamp_pre_fork_2_6() ->
	ar_blacklist_middleware:reset(),
	[B0] = ar_weave:init(),
	start(B0),
	{_Slave, _} = slave_start(B0),
	disconnect_from_slave(),
	slave_mine(),
	BI = assert_slave_wait_until_height(1),
	Peer = {127, 0, 0, 1, 1984},
	B1 = (slave_call(ar_storage, read_block, [hd(BI)]))#block{
			hash_list = [B0#block.indep_hash]
		},
	%% Expect the timestamp too far from the future to be rejected.
	FutureTimestampTolerance = ?JOIN_CLOCK_TOLERANCE * 2 + ?CLOCK_DRIFT_MAX,
	TooFarFutureTimestamp = os:system_time(second) + FutureTimestampTolerance + 3,
	B2 = update_block_timestamp_pre_2_6(B1, TooFarFutureTimestamp),
	ok = ar_events:subscribe(block),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B2)),
	H = B2#block.indep_hash,
	receive
		{event, block, {rejected, invalid_timestamp, H, Peer}} ->
			ok
		after 500 ->
			?assert(false, "Did not receive the rejected block event (invalid_timestamp).")
	end,
	%% Expect the timestamp from the future within the tolerance interval to be accepted.
	OkFutureTimestamp = os:system_time(second) + FutureTimestampTolerance - 3,
	B3 = update_block_timestamp_pre_2_6(B1, OkFutureTimestamp),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B3)),
	%% Expect the timestamp far from the past to be rejected.
	PastTimestampTolerance = lists:sum([?JOIN_CLOCK_TOLERANCE * 2, ?CLOCK_DRIFT_MAX]),
	TooFarPastTimestamp = B0#block.timestamp - PastTimestampTolerance - 3,
	B4 = update_block_timestamp_pre_2_6(B1, TooFarPastTimestamp),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B4)),
	H2 = B4#block.indep_hash,
	receive
		{event, block, {rejected, invalid_timestamp, H2, Peer}} ->
			ok
		after 500 ->
			?assert(false, "Did not receive the rejected block event (invalid_timestamp).")
	end,
	%% Expect the block with a timestamp from the past within the tolerance interval
	%% to be accepted.
	OkPastTimestamp = B0#block.timestamp - PastTimestampTolerance + 3,
	B5 = update_block_timestamp_pre_2_6(B1, OkPastTimestamp),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B5)).

test_add_external_block_with_invalid_timestamp({Key, BTip0, BTip1}) ->
	Peer = {127, 0, 0, 1, 1984},
	B0 = BTip0,
	B1 = BTip1#block{
			hash_list = [B0#block.indep_hash]
		},
	%% Expect the timestamp too far from the future to be rejected.
	FutureTimestampTolerance = ?JOIN_CLOCK_TOLERANCE * 2 + ?CLOCK_DRIFT_MAX,
	TooFarFutureTimestamp = os:system_time(second) + FutureTimestampTolerance + 3,
	B2 = update_block_timestamp(B1, B0, TooFarFutureTimestamp, Key),
	ok = ar_events:subscribe(block),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B2)),
	H = B2#block.indep_hash,
	receive
		{event, block, {rejected, invalid_timestamp, H, Peer}} ->
			ok
		after 500 ->
			?assert(false, "Did not receive the rejected block event (invalid_timestamp)")
	end,
	%% Expect the timestamp from the future within the tolerance interval to be accepted.
	OkFutureTimestamp = os:system_time(second) + FutureTimestampTolerance - 3,
	B3 = update_block_timestamp(B1, B0, OkFutureTimestamp, Key),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B3)),
	%% Expect the timestamp too far behind the previous timestamp to be rejected.
	PastTimestampTolerance = lists:sum([?JOIN_CLOCK_TOLERANCE * 2, ?CLOCK_DRIFT_MAX]),
	TooFarPastTimestamp = B0#block.timestamp - PastTimestampTolerance - 1,
	B4 = update_block_timestamp(B1, B0, TooFarPastTimestamp, Key),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B4)),
	H2 = B4#block.indep_hash,
	receive
		{event, block, {rejected, invalid_timestamp, H2, Peer}} ->
			ok
		after 500 ->
			?assert(false, "Did not receive the rejected block event "
					"(invalid_timestamp).")
	end,
	OkPastTimestamp = B0#block.timestamp - PastTimestampTolerance + 1,
	B5 = update_block_timestamp(B1, B0, OkPastTimestamp, Key),
	?assertMatch({ok, {{<<"200">>, _}, _, _, _, _}}, send_new_block(Peer, B5)).

update_block_timestamp_pre_2_6(B, Timestamp) ->
	#block{
		height = Height,
		nonce = Nonce,
		previous_block = PrevH,
		poa = #poa{ chunk = Chunk }
	} = B,
	B2 = B#block{ timestamp = Timestamp },
	BDS = ar_block:generate_block_data_segment(B2),
	{H0, Entropy} = ar_mine:spora_h0_with_entropy(BDS, Nonce, Height),
	B3 = B2#block{ hash = element(1, ar_mine:spora_solution_hash_with_entropy(PrevH, Timestamp,
			H0, Chunk, Entropy, Height)) },
	B3#block{ indep_hash = ar_block:indep_hash(B3) }.

update_block_timestamp(B, PrevB, Timestamp, Key) ->
	sign_block(B#block{ timestamp = Timestamp }, PrevB, Key).

%% @doc Post a tx to the network and ensure that last_tx call returns the ID of last tx.
test_add_tx_and_get_last({_B0, Wallet1, Wallet2, _StaticWallet}) ->
	LocalHeight = ar_node:get_height(),
	disconnect_from_slave(),
	{_Priv1, Pub1} = Wallet1,
	{_Priv2, Pub2} = Wallet2,
	SignedTX = sign_tx(Wallet1, #{
		target => ar_wallet:to_address(Pub2),
		quantity => ?AR(2),
		reward => ?AR(1)}),
	ID = SignedTX#tx.id,
	ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1984}, SignedTX#tx.id,
			ar_serialize:tx_to_binary(SignedTX)),
	wait_until_receives_txs([SignedTX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet/"
					++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1)))
					++ "/last_tx"
		}),
	?assertEqual(ID, ar_util:decode(Body)).

%% @doc Post a tx to the network and ensure that its subfields can be gathered
test_get_subfields_of_tx(_) ->
	LocalHeight = ar_node:get_height(),
	TX = ar_tx:new(<<"DATA">>),
	ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1984}, TX#tx.id,
			ar_serialize:tx_to_binary(TX)),
	wait_until_receives_txs([TX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, Body} = wait_until_syncs_tx_data(TX#tx.id),
	Orig = TX#tx.data,
	?assertEqual(Orig, ar_util:decode(Body)).

%% @doc Correctly check the status of pending is returned for a pending transaction
test_get_pending_tx(_) ->
	TX = ar_tx:new(<<"DATA1">>),
	ar_http_iface_client:send_tx_json({127, 0, 0, 1, 1984}, TX#tx.id,
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))),
	wait_until_receives_txs([TX]),
	{ok, {{<<"202">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id))
		}),
	?assertEqual(<<"Pending">>, Body).

%% @doc Mine a transaction into a block and retrieve it's binary body via HTTP.
test_get_tx_body(_) ->
	disconnect_from_slave(),
	LocalHeight = ar_node:get_height(),
	TX = ar_tx:new(<<"TEST DATA">>),
	assert_post_tx_to_master(TX),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, Data} = wait_until_syncs_tx_data(TX#tx.id),
	?assertEqual(<<"TEST DATA">>, ar_util:decode(Data)).

test_get_tx_status(_) ->
	LocalHeight = ar_node:get_height(),
	RemoteHeight = slave_height(),
	disconnect_from_slave(),
	TX = (ar_tx:new())#tx{ tags = [{<<"TestName">>, <<"TestVal">>}] },
	assert_post_tx_to_master(TX),
	FetchStatus = fun() ->
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/status"
		})
	end,
	?assertMatch({ok, {{<<"202">>, _}, _, <<"Pending">>, _, _}}, FetchStatus()),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} = FetchStatus(),
	{Res} = ar_serialize:dejsonify(Body),
	BI = ar_node:get_block_index(),
	?assertEqual(
		#{
			<<"block_height">> => length(BI) - 1,
			<<"block_indep_hash">> => ar_util:encode(element(1, hd(BI))),
			<<"number_of_confirmations">> => 1
		},
		maps:from_list(Res)
	),
	ar_node:mine(),
	wait_until_height(LocalHeight + 2),
	ar_util:do_until(
		fun() ->
			{ok, {{<<"200">>, _}, _, Body2, _, _}} = FetchStatus(),
			{Res2} = ar_serialize:dejsonify(Body2),
			#{
				<<"block_height">> => length(BI) - 1,
				<<"block_indep_hash">> => ar_util:encode(element(1, hd(BI))),
				<<"number_of_confirmations">> => 2
			} == maps:from_list(Res2)
		end,
		200,
		5000
	),
	%% Create a fork which returns the TX to mempool.
	slave_mine(),
	assert_slave_wait_until_height(RemoteHeight + 1),
	slave_mine(),
	assert_slave_wait_until_height(RemoteHeight + 2),
	connect_to_slave(),
	slave_mine(),
	wait_until_height(LocalHeight + 3),
	?assertMatch({ok, {{<<"202">>, _}, _, _, _, _}}, FetchStatus()).

test_post_unsigned_tx({_B0, Wallet1, _Wallet2, _StaticWallet}) ->
	LocalHeight = ar_node:get_height(),
	{_, Pub} = Wallet = Wallet1,
	%% Generate a wallet and receive a wallet access code.
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet"
		}),
	{ok, Config} = application:get_env(arweave, config),
	application:set_env(arweave, config,
			Config#config{ internal_api_secret = <<"correct_secret">> }),
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet",
			headers => [{<<"X-Internal-Api-Secret">>, <<"incorrect_secret">>}]
		}),
	{ok, {{<<"200">>, <<"OK">>}, _, CreateWalletBody, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/wallet",
			headers => [{<<"X-Internal-Api-Secret">>, <<"correct_secret">>}]
		}),
	application:set_env(arweave, config, Config#config{ internal_api_secret = not_set }),
	{CreateWalletRes} = ar_serialize:dejsonify(CreateWalletBody),
	[WalletAccessCode] = proplists:get_all_values(<<"wallet_access_code">>, CreateWalletRes),
	[Address] = proplists:get_all_values(<<"wallet_address">>, CreateWalletRes),
	%% Top up the new wallet.
	TopUpTX = sign_tx(Wallet, #{
		owner => Pub,
		target => ar_util:decode(Address),
		quantity => ?AR(100),
		reward => ?AR(1)
		}),
	{ok, {{<<"200">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx",
			body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TopUpTX))
		}),
	wait_until_receives_txs([TopUpTX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	%% Send an unsigned transaction to be signed with the generated key.
	TX = (ar_tx:new())#tx{reward = ?AR(1), last_tx = TopUpTX#tx.id},
	UnsignedTXProps = [
		{<<"last_tx">>, <<>>},
		{<<"target">>, TX#tx.target},
		{<<"quantity">>, integer_to_binary(TX#tx.quantity)},
		{<<"data">>, TX#tx.data},
		{<<"reward">>, integer_to_binary(TX#tx.reward)},
		{<<"denomination">>, integer_to_binary(TopUpTX#tx.denomination)},
		{<<"wallet_access_code">>, WalletAccessCode}
	],
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/unsigned_tx",
			body => ar_serialize:jsonify({UnsignedTXProps})
		}),
	application:set_env(arweave, config,
			Config#config{ internal_api_secret = <<"correct_secret">> }),
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/unsigned_tx",
			headers => [{<<"X-Internal-Api-Secret">>, <<"incorrect_secret">>}],
			body => ar_serialize:jsonify({UnsignedTXProps})
		}),
	{ok, {{<<"200">>, <<"OK">>}, _, Body, _, _}} =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, 1984},
			path => "/unsigned_tx",
			headers => [{<<"X-Internal-Api-Secret">>, <<"correct_secret">>}],
			body => ar_serialize:jsonify({UnsignedTXProps})
		}),
	application:set_env(arweave, config, Config#config{ internal_api_secret = not_set }),
	{Res} = ar_serialize:dejsonify(Body),
	TXID = proplists:get_value(<<"id">>, Res),
	timer:sleep(200),
	ar_node:mine(),
	wait_until_height(LocalHeight + 2),
	{ok, {_, _, GetTXBody, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ binary_to_list(TXID) ++ "/status"
		}),
	{GetTXRes} = ar_serialize:dejsonify(GetTXBody),
	?assertMatch(
		#{
			<<"number_of_confirmations">> := 1
		},
		maps:from_list(GetTXRes)
	).

%% @doc Ensure the HTTP client stops fetching data from an endpoint when its data size
%% limit is exceeded.
test_get_error_of_data_limit(_) ->
	LocalHeight = ar_node:get_height(),
	Limit = 1460,
	TX = ar_tx:new(<< <<0>> || _ <- lists:seq(1, Limit * 2) >>),
	ar_http_iface_client:send_tx_binary({127, 0, 0, 1, 1984}, TX#tx.id,
			ar_serialize:tx_to_binary(TX)),
	wait_until_receives_txs([TX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, _} = wait_until_syncs_tx_data(TX#tx.id),
	Resp =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, 1984},
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/data",
			limit => Limit
		}),
	?assertEqual({error, too_much_data}, Resp).

send_block2_pre_fork_2_6_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> infinity end}],
		fun() -> test_send_block2(fork_2_5) end).

send_block2_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun() -> test_send_block2(fork_2_6) end).

test_send_block2(Fork) ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(100), <<>>}]),
	MasterWallet = ar_wallet:new_keyfile(),
	MasterAddress = ar_wallet:to_address(MasterWallet),
	SlaveWallet = slave_call(ar_wallet, new_keyfile, []),
	SlaveAddress = ar_wallet:to_address(SlaveWallet),
	start(B0, MasterAddress),
	slave_start(B0, SlaveAddress),
	disconnect_from_slave(),
	TXs = [sign_tx(Wallet, #{ last_tx => get_tx_anchor() }) || _ <- lists:seq(1, 10)],
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs),
	ar_node:mine(),
	[{H, _, _}, _] = wait_until_height(1),
	B = ar_storage:read_block(H),
	TXs2 = sort_txs_by_block_order(TXs, B),
	EverySecondTX = element(2, lists:foldl(fun(TX, {N, Acc}) when N rem 2 /= 0 ->
			{N + 1, [TX | Acc]}; (_TX, {N, Acc}) -> {N + 1, Acc} end, {0, []}, TXs2)),
	lists:foreach(fun(TX) -> assert_post_tx_to_slave(TX) end, EverySecondTX),
	Announcement = #block_announcement{ indep_hash = B#block.indep_hash,
			previous_block = B0#block.indep_hash,
			tx_prefixes = [binary:part(TX#tx.id, 0, 8) || TX <- TXs2] },
	{ok, {{<<"200">>, _}, _, Body, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(Announcement) }),
	Response = ar_serialize:binary_to_block_announcement_response(Body),
	?assertEqual({ok, #block_announcement_response{ missing_chunk = true,
			missing_tx_indices = [0, 2, 4, 6, 8] }}, Response),
	Announcement2 = Announcement#block_announcement{ recall_byte = 0 },
	{ok, {{<<"200">>, _}, _, Body2, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(Announcement2) }),
	Response2 = ar_serialize:binary_to_block_announcement_response(Body2),
	?assertEqual({ok, #block_announcement_response{ missing_chunk = false,
			missing_tx_indices = [0, 2, 4, 6, 8] }}, Response2),
	Announcement3 = Announcement#block_announcement{ recall_byte = 100000000000000 },
	{ok, {{<<"200">>, _}, _, Body, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(Announcement3) }),
	{ok, {{<<"418">>, _}, _, Body3, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block2",
			body => ar_serialize:block_to_binary(B) }),
	?assertEqual(iolist_to_binary(lists:foldl(fun(#tx{ id = TXID }, Acc) -> [TXID | Acc] end,
			[], TXs2 -- EverySecondTX)), Body3),
	B2 = B#block{ txs = [lists:nth(1, TXs2) | tl(B#block.txs)] },
	{ok, {{<<"418">>, _}, _, Body4, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block2",
			body => ar_serialize:block_to_binary(B2) }),
	?assertEqual(iolist_to_binary(lists:foldl(fun(#tx{ id = TXID }, Acc) -> [TXID | Acc] end,
			[], (TXs2 -- EverySecondTX) -- [lists:nth(1, TXs2)])), Body4),
	TXs3 = [sign_tx(master, Wallet, #{ last_tx => get_tx_anchor(),
			data => crypto:strong_rand_bytes(10 * 1024) }) || _ <- lists:seq(1, 10)],
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs3),
	ar_node:mine(),
	[{H2, _, _}, _, _] = wait_until_height(2),
	{ok, {{<<"412">>, _}, _, <<>>, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(#block_announcement{
					indep_hash = H2, previous_block = B#block.indep_hash }) }),
	BTXs = ar_storage:read_tx(B#block.txs),
	B3 = B#block{ txs = BTXs },
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block2",
			body => ar_serialize:block_to_binary(B3) }),
	{ok, {{<<"200">>, _}, _, SerializedB, _, _}} = ar_http:req(#{ method => get,
			peer => master_peer(), path => "/block2/height/1" }),
	?assertEqual({ok, B}, ar_serialize:binary_to_block(SerializedB)),
	Map = element(2, lists:foldl(fun(TX, {N, M}) -> {N + 1, maps:put(TX#tx.id, N, M)} end,
			{0, #{}}, TXs2)),
	{ok, {{<<"200">>, _}, _, Serialized2B, _, _}} = ar_http:req(#{ method => get,
			peer => master_peer(), path => "/block2/height/1",
			body => << 1:1, 0:(8 * 125 - 1) >> }),
	?assertEqual({ok, B#block{ txs = [case maps:get(TX#tx.id, Map) == 0 of true -> TX;
			_ -> TX#tx.id end || TX <- BTXs] }}, ar_serialize:binary_to_block(Serialized2B)),
	{ok, {{<<"200">>, _}, _, Serialized2B, _, _}} = ar_http:req(#{ method => get,
			peer => master_peer(), path => "/block2/height/1",
			body => << 1:1, 0:7 >> }),
	{ok, {{<<"200">>, _}, _, Serialized3B, _, _}} = ar_http:req(#{ method => get,
			peer => master_peer(), path => "/block2/height/1",
			body => << 0:1, 1:1, 0:1, 1:1, 0:4 >> }),
	?assertEqual({ok, B#block{ txs = [case lists:member(maps:get(TX#tx.id, Map), [1, 3]) of
			true -> TX; _ -> TX#tx.id end || TX <- BTXs] }},
					ar_serialize:binary_to_block(Serialized3B)),
	B4 = read_block_when_stored(H2, true),
	timer:sleep(500),
	{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block2",
			body => ar_serialize:block_to_binary(B4) }),
	connect_to_slave(),
	lists:foreach(
		fun(Height) ->
			ar_node:mine(),
			assert_slave_wait_until_height(Height)
		end,
		lists:seq(3, 3 + ?SEARCH_SPACE_UPPER_BOUND_DEPTH)
	),
	B5 = ar_storage:read_block(ar_node:get_current_block_hash()),
	{ok, {{<<"208">>, _}, _, _, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(#block_announcement{
					indep_hash = B5#block.indep_hash,
					previous_block = B5#block.previous_block }) }),
	disconnect_from_slave(),
	ar_node:mine(),
	[_ | _] = wait_until_height(3 + ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 1),
	B6 = ar_storage:read_block(ar_node:get_current_block_hash()),
	{ok, {{<<"200">>, _}, _, Body5, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(#block_announcement{
					indep_hash = B6#block.indep_hash,
					previous_block = B6#block.previous_block,
					recall_byte = 0 }) }),
	?assertEqual({ok, #block_announcement_response{ missing_chunk = false,
			missing_tx_indices = [] }},
			ar_serialize:binary_to_block_announcement_response(Body5)),
	{ok, {{<<"200">>, _}, _, Body6, _, _}} = ar_http:req(#{ method => post,
			peer => slave_peer(), path => "/block_announcement",
			body => ar_serialize:block_announcement_to_binary(#block_announcement{
					indep_hash = B6#block.indep_hash,
					previous_block = B6#block.previous_block,
					recall_byte = 1024 }) }),
	?assertEqual({ok, #block_announcement_response{ missing_chunk = false,
			missing_tx_indices = [] }},
			ar_serialize:binary_to_block_announcement_response(Body6)),
	case Fork of
		fork_2_5 ->
			{H0, _Entropy} = ar_mine:spora_h0_with_entropy(
					ar_block:generate_block_data_segment(B6), B6#block.nonce, B6#block.height),
			{_H, PartitionUpperBound} = ar_node:get_recent_partition_upper_bound_by_prev_h(
					B6#block.previous_block),
			{ok, RecallByte} = ar_mine:pick_recall_byte(H0, B6#block.previous_block,
					PartitionUpperBound),
			{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} = ar_http:req(#{ method => post,
				peer => slave_peer(), path => "/block2",
				headers => [{<<"arweave-recall-byte">>, integer_to_binary(RecallByte)}],
				body => ar_serialize:block_to_binary(B6#block{ recall_byte = RecallByte,
						poa = #poa{} }) }),
			assert_slave_wait_until_height(3 + ?SEARCH_SPACE_UPPER_BOUND_DEPTH + 1);
		_ ->
			ok
	end.

sort_txs_by_block_order(TXs, B) ->
	TXByID = lists:foldl(fun(TX, Acc) -> maps:put(tx_id(TX), TX, Acc) end, #{}, TXs),
	lists:foldr(fun(TX, Acc) -> [maps:get(tx_id(TX), TXByID) | Acc] end, [], B#block.txs).

tx_id(#tx{ id = ID }) ->
	ID;
tx_id(ID) ->
	ID.

test_send_missing_tx_with_the_block({_B0, Wallet1, _Wallet2, _StaticWallet}) ->
	LocalHeight = ar_node:get_height(),
	RemoteHeight = slave_height(),
	disconnect_from_slave(),
	TXs = [sign_tx(Wallet1, #{ last_tx => get_tx_anchor() }) || _ <- lists:seq(1, 10)],
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs),
	EverySecondTX = element(2, lists:foldl(fun(TX, {N, Acc}) when N rem 2 /= 0 ->
			{N + 1, [TX | Acc]}; (_TX, {N, Acc}) -> {N + 1, Acc} end, {0, []}, TXs)),
	lists:foreach(fun(TX) -> assert_post_tx_to_slave(TX) end, EverySecondTX),
	ar_node:mine(),
	BI = wait_until_height(LocalHeight + 1),
	B = ar_storage:read_block(hd(BI)),
	B2 = B#block{ txs = ar_storage:read_tx(B#block.txs) },
	connect_to_slave(),
	ar_bridge ! {event, block, {new, B2, #{ recall_byte => undefined }}},
	assert_slave_wait_until_height(RemoteHeight + 1).

test_fallback_to_block_endpoint_if_cannot_send_tx({_B0, Wallet1, _Wallet2, _StaticWallet}) ->
	LocalHeight = ar_node:get_height(),
	RemoteHeight = slave_height(),
	disconnect_from_slave(),
	TXs = [sign_tx(Wallet1, #{ last_tx => get_tx_anchor() }) || _ <- lists:seq(1, 10)],
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs),
	EverySecondTX = element(2, lists:foldl(fun(TX, {N, Acc}) when N rem 2 /= 0 ->
			{N + 1, [TX | Acc]}; (_TX, {N, Acc}) -> {N + 1, Acc} end, {0, []}, TXs)),
	lists:foreach(fun(TX) -> assert_post_tx_to_slave(TX) end, EverySecondTX),
	ar_node:mine(),
	BI = wait_until_height(LocalHeight + 1),
	B = ar_storage:read_block(hd(BI)),
	connect_to_slave(),
	ar_bridge ! {event, block, {new, B, #{ recall_byte => undefined }}},
	assert_slave_wait_until_height(RemoteHeight + 1).

resigned_solution_test_() ->
	test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
		fun() -> test_resigned_solution() end).

test_resigned_solution() ->
	[B0] = ar_weave:init(),
	start(B0),
	slave_start(B0),
	connect_to_slave(),
	slave_mine(),
	wait_until_height(1),
	disconnect_from_slave(),
	slave_mine(),
	B = ar_node:get_current_block(),
	{ok, Config} = slave_call(application, get_env, [arweave, config]),
	Key = element(1, slave_call(ar_wallet, load_key, [Config#config.mining_addr])),
	ok = ar_events:subscribe(block),
	B2 = sign_block(B#block{ tags = [<<"tag1">>] }, B0, Key),
	post_block(B2, [valid]),
	B3 = sign_block(B#block{ tags = [<<"tag2">>] }, B0, Key),
	post_block(B3, [valid]),
	assert_slave_wait_until_height(2),
	B4 = slave_call(ar_node, get_current_block, []),
	?assertEqual(B#block.indep_hash, B4#block.previous_block),
	B2H = B2#block.indep_hash,
	?assertNotEqual(B2#block.indep_hash, B4#block.previous_block),
	PrevStepNumber = (B#block.nonce_limiter_info)#nonce_limiter_info.global_step_number,
	PrevInterval = PrevStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY,
	Info4 = B4#block.nonce_limiter_info,
	StepNumber = Info4#nonce_limiter_info.global_step_number,
	Interval = StepNumber div ?NONCE_LIMITER_RESET_FREQUENCY,
	B5 =
		case Interval == PrevInterval of
			true ->
				sign_block(B4#block{
						hash_list_merkle = ar_block:compute_hash_list_merkle(B2),
						previous_block = B2H }, B2, Key);
			false ->
				sign_block(B4#block{ previous_block = B2H,
						hash_list_merkle = ar_block:compute_hash_list_merkle(B2),
						nonce_limiter_info = Info4#nonce_limiter_info{ next_seed = B2H } },
						B2, Key)
		end,
	B5H = B5#block.indep_hash,
	post_block(B5, [valid]),
	[{B5H, _, _}, {B2H, _, _}, _] = wait_until_height(2),
	ar_node:mine(),
	[{B6H, _, _}, _, _, _] = wait_until_height(3),
	connect_to_slave(),
	[{B6H, _, _}, {B5H, _, _}, {B2H, _, _}, _] = assert_slave_wait_until_height(3).

test_get_recent_hash_list_diff({_B0, Wallet1, _Wallet2, _StaticWallet}) ->
	LocalHeight = ar_node:get_height(),
	BTip = ar_node:get_current_block(),
	disconnect_from_slave(),
	{ok, {{<<"404">>, _}, _, <<>>, _, _}} = ar_http:req(#{ method => get,
		peer => master_peer(), path => "/recent_hash_list_diff",
		headers => [], body => <<>> }),
	{ok, {{<<"400">>, _}, _, <<>>, _, _}} = ar_http:req(#{ method => get,
		peer => master_peer(), path => "/recent_hash_list_diff",
		headers => [], body => crypto:strong_rand_bytes(47) }),
	{ok, {{<<"404">>, _}, _, <<>>, _, _}} = ar_http:req(#{ method => get,
		peer => master_peer(), path => "/recent_hash_list_diff",
		headers => [], body => crypto:strong_rand_bytes(48) }),
	B0H = BTip#block.indep_hash,
	{ok, {{<<"200">>, _}, _, B0H, _, _}} = ar_http:req(#{ method => get,
		peer => master_peer(), path => "/recent_hash_list_diff",
		headers => [], body => B0H }),
	ar_node:mine(),
	BI1 = wait_until_height(LocalHeight + 1),
	{B1H, _, _} = hd(BI1),
	{ok, {{<<"200">>, _}, _, << B0H:48/binary, B1H:48/binary, 0:16 >> , _, _}}
			= ar_http:req(#{ method => get, peer => master_peer(),
			path => "/recent_hash_list_diff", headers => [], body => B0H }),
	TXs = [sign_tx(master, Wallet1, #{ last_tx => get_tx_anchor() }) || _ <- lists:seq(1, 3)],
	lists:foreach(fun(TX) -> assert_post_tx_to_master(TX) end, TXs),
	ar_node:mine(),
	BI2 = wait_until_height(LocalHeight + 2),
	{B2H, _, _} = hd(BI2),
	[TXID1, TXID2, TXID3] = [TX#tx.id || TX <- (ar_node:get_current_block())#block.txs],
	{ok, {{<<"200">>, _}, _, << B0H:48/binary, B1H:48/binary, 0:16, B2H:48/binary,
			3:16, TXID1:32/binary, TXID2:32/binary, TXID3/binary >> , _, _}}
			= ar_http:req(#{ method => get, peer => master_peer(),
			path => "/recent_hash_list_diff", headers => [], body => B0H }),
	{ok, {{<<"200">>, _}, _, << B0H:48/binary, B1H:48/binary, 0:16, B2H:48/binary,
			3:16, TXID1:32/binary, TXID2:32/binary, TXID3/binary >> , _, _}}
			= ar_http:req(#{ method => get, peer => master_peer(),
			path => "/recent_hash_list_diff", headers => [],
			body => << B0H/binary, (crypto:strong_rand_bytes(48))/binary >>}),
	{ok, {{<<"200">>, _}, _, << B1H:48/binary, B2H:48/binary,
			3:16, TXID1:32/binary, TXID2:32/binary, TXID3/binary >> , _, _}}
			= ar_http:req(#{ method => get, peer => master_peer(),
			path => "/recent_hash_list_diff", headers => [],
			body => << B0H/binary, B1H/binary, (crypto:strong_rand_bytes(48))/binary >>}).

send_new_block(Peer, B) ->
	ar_http_iface_client:send_block_binary(Peer, B#block.indep_hash,
			ar_serialize:block_to_binary(B)).

wait_until_syncs_tx_data(TXID) ->
	ar_util:do_until(
		fun() ->
			case ar_http:req(#{
				method => get,
				peer => {127, 0, 0, 1, 1984},
				path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data"
			}) of
				{ok, {{<<"404">>, _}, _, _, _, _}} ->
					false;
				{ok, {{<<"200">>, _}, _, <<>>, _, _}} ->
					false;
				{ok, {{<<"200">>, _}, _, Payload, _, _}} ->
					{ok, Payload}
			end
		end,
		100,
		2000
	).

slave_height() ->
	slave_call(ar_node, get_height, []).
