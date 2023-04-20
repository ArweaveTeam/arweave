-module(ar_http_iface_tests).

-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/1, slave_stop/0, slave_start/1,
		connect_to_slave/0, get_tx_anchor/0, disconnect_from_slave/0,
		wait_until_height/1, wait_until_receives_txs/1, sign_tx/2, sign_tx/3,
		post_tx_json_to_master/1, assert_slave_wait_until_receives_txs/1,
		slave_wait_until_height/1, read_block_when_stored/1, read_block_when_stored/2,
		master_peer/0, slave_peer/0, slave_mine/0, assert_slave_wait_until_height/1,
		slave_call/3, assert_post_tx_to_master/1, assert_post_tx_to_slave/1]).

-define(WALLET1, "arweave_keyfile_AjC9B-cdaZL8AxXN_Qcan3bK1L5nuoDGB0wZbA1aQp0.json").
-define(WALLET2, "arweave_keyfile_lIIRG3Q0doj1DfHR6NWDqAk6iptIeLy2U6SGIPDNmTU.json").
-define(WALLET3, "arweave_keyfile_tyRTXlv9ZRO1U01V4jywy9Ep5drTFrDoSzH6rqtRcmE.json").

start_node() ->
	%% Starting a node is slow so we'll run it once for the whole test module
	Dir = filename:dirname(?FILE),
	WalletsFixtureDir = filename:join(Dir, "../test/fixtures/wallets"),
	Wallet1 = {_, Pub1} = ar_wallet:load_keyfile(filename:join(WalletsFixtureDir, ?WALLET1)),
	Wallet2 = {_, Pub2} = ar_wallet:load_keyfile(filename:join(WalletsFixtureDir, ?WALLET2)),
	%% This wallet is never spent from or deposited to, so the balance is predictable
	StaticWallet = {_, Pub3} = ar_wallet:load_keyfile(filename:join(WalletsFixtureDir, ?WALLET3)),
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
	connect_to_slave().

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
	{timeout, 300, {with, Fixture, [TestFun]}}.

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

% %% @doc Test that nodes sending too many requests are temporarily blocked: (a) GET.
% node_blacklisting_get_spammer_test_() ->
% 	{timeout, 10, fun test_node_blacklisting_get_spammer/0}.

% %% @doc Test that nodes sending too many requests are temporarily blocked: (b) POST.
% node_blacklisting_post_spammer_test_() ->
% 	{timeout, 10, fun test_node_blacklisting_post_spammer/0}.

% %% @doc Check that we can qickly get the local time from the peer.
% get_time_test() ->
% 	Now = os:system_time(second),
% 	{ok, {Min, Max}} = ar_http_iface_client:get_time(master_peer(), 10 * 1000),
% 	?assert(Min < Now),
% 	?assert(Now < Max).

batch_test_() ->
	{setup, fun setup_all_batch/0, fun cleanup_all_batch/1,
		fun ({GenesisData, _MockData}) ->
			{foreach, fun reset_node/0, [
				test_register(fun test_post_tx/1, GenesisData)
				% %% ---------------------------------------------------------
				% %% The following tests must be run at a block height of 0.
				% %% ---------------------------------------------------------
				% test_register(fun test_get_current_block/1, GenesisData),
				% test_register(fun test_get_height/1, GenesisData),
				% %% ---------------------------------------------------------
				% %% The following tests are read-only and will not modify
				% %% state. They assume that the blockchain state
				% %% is fixed (and set by start_node and test_get_height). 
				% %% ---------------------------------------------------------
				% test_register(fun test_get_wallet_list_in_chunks/1, GenesisData),
				% test_register(fun test_get_info/1, GenesisData),
				% test_register(fun test_get_last_tx_single/1, GenesisData),
				% test_register(fun test_get_block_by_hash/1, GenesisData),
				% test_register(fun test_get_block_by_height/1, GenesisData),
				% test_register(fun test_get_non_existent_block/1, GenesisData),
				% %% ---------------------------------------------------------
				% %% The following tests are *not* read-only and may modify
				% %% state. They can *not* assume a fixed blockchain state. 
				% %% ---------------------------------------------------------
				% test_register(fun test_addresses_with_checksum/1, GenesisData),
				% test_register(fun test_single_regossip/1, GenesisData),
				% test_register(fun test_get_balance/1, GenesisData),
				% test_register(fun test_get_format_2_tx/1, GenesisData),
				% test_register(fun test_get_format_1_tx/1, GenesisData),
				% test_register(fun test_add_external_tx_with_tags/1, GenesisData),
				% test_register(fun test_find_external_tx/1, GenesisData),
				% test_register(fun test_add_tx_and_get_last/1, GenesisData),
				% test_register(fun test_get_subfields_of_tx/1, GenesisData),
				% test_register(fun test_get_pending_tx/1, GenesisData),
				% test_register(fun test_get_tx_body/1, GenesisData),
				% test_register(fun test_get_tx_status/1, GenesisData),
				% test_register(fun test_post_unsigned_tx/1, GenesisData),
				% test_register(fun test_get_error_of_data_limit/1, GenesisData),
				% test_register(fun test_send_missing_tx_with_the_block/1, GenesisData),
				% test_register(
				% 	fun test_fallback_to_block_endpoint_if_cannot_send_tx/1, GenesisData),
				% test_register(fun test_get_recent_hash_list_diff/1, GenesisData)
			]}
		end
	}.

test_post_tx({B0, Wallet1, Wallet2, _StaticWallet}) ->
	disconnect_from_slave(),
	% write_tx_data(Wallet1, Wallet2, NumTXs, 2, 0, true).
	% TXs = read_tx_data(NumTXs, 2, 0, true),
	StartGenerate = erlang:timestamp(),
	?LOG_ERROR("Generating..."),
	TXs = generate_tx_data(Wallet1, Wallet2, 100, 1, 0, true),
	NumTXs = length(TXs),
	?LOG_ERROR("Num TXs: ~p", [NumTXs]),
	GenerateTime = timer:now_diff(erlang:timestamp(), StartGenerate) / 1000000,
	?LOG_ERROR("Generate time: ~p", [GenerateTime]),
	?LOG_ERROR("STARTING"),
	Start = erlang:timestamp(),
	ar_util:pmap(fun({TX, SerializedTX}) -> 
			ar_http_iface_client:send_tx_binary(
				master_peer(), TX#tx.id,
				SerializedTX)
		end, TXs),
	PreWaitTime = timer:now_diff(erlang:timestamp(), Start) / 1000000,
	?debugFmt("Pre-wait time: ~p", [PreWaitTime]),
	wait_until_mempool(NumTXs),
	End = erlang:timestamp(),
	?LOG_ERROR("DONE"),
	ElapsedTime = timer:now_diff(End, Start) / 1000000,
	?debugFmt("Total time: ~p", [ElapsedTime]),
	ar_bench_timer:print_timing_data().

generate_tx_data(Wallet1, {_Priv2, Pub2} =  _Wallet2, NumTXs, Format, Quantity, HasData) ->
	Data = case HasData of
		true -> crypto:strong_rand_bytes(100 * 1024);
		false -> <<>>
	end,
	TXData = #{
		format => Format,
		target => ar_wallet:to_address(Pub2),
		quantity => Quantity,
		reward => ?AR(1),
		data => Data },
	NumThreads = erlang:system_info(dirty_cpu_schedulers_online),
	NumTXsPerThread = NumTXs div NumThreads,
	?LOG_ERROR("Num TXs per thread: ~p", [NumTXsPerThread]),
	TXs = ar_util:pmap(fun(NumThreadTXs) ->
			lists:foldl(fun(_, Acc) ->
					TX = sign_tx(Wallet1, TXData),
					[{TX, ar_serialize:tx_to_binary(TX)} | Acc]
				end,
				[],
				lists:seq(1, NumThreadTXs))
		end, lists:duplicate(NumThreads,NumTXsPerThread)),
	lists:flatten(TXs).

write_tx_data(Wallet1, {_Priv2, Pub2} =  _Wallet2, NumTXs, Format, Quantity, HasData) ->
	Data = case HasData of
		true -> crypto:strong_rand_bytes(10 * 1024);
		false -> <<>>
	end,
	TXData = #{
		format => Format,
		target => ar_wallet:to_address(Pub2),
		quantity => Quantity,
		reward => ?AR(1),
		data => Data },
	TXs = lists:foldl(fun(_, Acc) ->
					TX = sign_tx(Wallet1, TXData),
					[ar_serialize:tx_to_json_struct(TX) | Acc]
				end,
				[],
				lists:seq(1, NumTXs)),
	
	Filename = io_lib:format("tx_n~p_f~p_q~p_d~p.json", [NumTXs, Format, Quantity, HasData]),
	{ok, File} = file:open(Filename, [write]),
	file:write(File, ar_serialize:jsonify(TXs)),
    file:close(File).

read_tx_data(NumTXs, Format, Quantity, HasData) ->
	Filename = io_lib:format("tx_n~p_f~p_q~p_d~p.json", [NumTXs, Format, Quantity, HasData]),
	case file:read_file(Filename) of
		{ok, FileData} -> 
			JSONTXs = ar_serialize:dejsonify(FileData),
			lists:map(fun(JSONTX) -> 
					TX = ar_serialize:json_struct_to_tx(JSONTX),
					{TX, ar_serialize:tx_to_binary(TX)}
				end, JSONTXs);
		_ -> 
			{error, file_not_found}
	end.

wait_until_mempool(Size) ->
	{ok, Mempool} = ar_http_iface_client:get_mempool(master_peer()),
	case length(Mempool) >= Size of
		true -> ok;
		_ -> 
			timer:sleep(10),
			wait_until_mempool(Size)
	end.



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
	?assertEqual(<<?NETWORK_NAME>>, ar_http_iface_client:get_info(master_peer(), name)),
	?assertEqual({<<"release">>, ?RELEASE_NUMBER},
			ar_http_iface_client:get_info(master_peer(), release)),
	?assertEqual(
		?CLIENT_VERSION,
		ar_http_iface_client:get_info(master_peer(), version)),
	?assertEqual(1, ar_http_iface_client:get_info(master_peer(), peers)),
	ar_util:do_until(
		fun() ->
			1 == ar_http_iface_client:get_info(master_peer(), blocks)
		end,
		100,
		2000
	),
	?assertEqual(1, ar_http_iface_client:get_info(master_peer(), height)).

%% @doc Ensure that transactions are only accepted once.
test_single_regossip(_) ->
	disconnect_from_slave(),
	TX = ar_tx:new(),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		ar_http_iface_client:send_tx_json(master_peer(), TX#tx.id,
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)))
	),
	?assertMatch(
		{ok, {{<<"200">>, _}, _, _, _, _}},
		slave_call(ar_http_iface_client, send_tx_binary, [slave_peer(), TX#tx.id,
				ar_serialize:tx_to_binary(TX)])
	),
	?assertMatch(
		{ok, {{<<"208">>, _}, _, _, _, _}},
		slave_call(ar_http_iface_client, send_tx_binary, [slave_peer(), TX#tx.id,
				ar_serialize:tx_to_binary(TX)])
	),
	?assertMatch(
		{ok, {{<<"208">>, _}, _, _, _, _}},
		slave_call(ar_http_iface_client, send_tx_json, [slave_peer(), TX#tx.id,
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))])
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
			ar_http_iface_client:get_info(master_peer())
		end
	, info_unavailable};
get_fun_msg_pair(send_tx_binary) ->
	{ fun(_) ->
			InvalidTX = (ar_tx:new())#tx{ owner = <<"key">>, signature = <<"invalid">> },
			case ar_http_iface_client:send_tx_binary(master_peer(),
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
			peer => master_peer(),
			path => "/wallet/" ++ Addr ++ "/balance"
		}),
	?assertEqual(?AR(10), binary_to_integer(Body)),
	RootHash = binary_to_list(ar_util:encode(B0#block.wallet_list)),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/wallet_list/" ++ RootHash ++ "/" ++ Addr ++ "/balance"
		}),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
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
			peer => master_peer(),
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
			peer => master_peer(),
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
			peer => master_peer(),
			path => "/wallet_list/" ++ RootHash ++ "/" ++ ar_util:encode(Cursor)
		}),
	?assertEqual(#{
			next_cursor => last, 
			wallets => lists:reverse(ExpectedWallets2)
		}, binary_to_term(Body2)).

%% @doc Test that heights are returned correctly.
test_get_height(_) ->
	0 = ar_http_iface_client:get_height(master_peer()),
	ar_node:mine(),
	wait_until_height(1),
	1 = ar_http_iface_client:get_height(master_peer()).

%% @doc Test that last tx associated with a wallet can be fetched.
test_get_last_tx_single({_, _, _, {_, StaticPub}}) ->
	Addr = binary_to_list(ar_util:encode(ar_wallet:to_address(StaticPub))),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/wallet/" ++ Addr ++ "/last_tx"
		}),
	?assertEqual(<<"TEST_ID">>, ar_util:decode(Body)).

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
		ar_http:req(#{ method => get, peer => master_peer(), path => "/block/height/100" }),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(), path => "/block2/height/100" }),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(), path => "/block/hash/abcd" }),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(), path => "/block2/hash/abcd" }),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(),
				path => "/block/height/101/wallet_list" }),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(),
				path => "/block/hash/abcd/wallet_list" }),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(),
				path => "/block/height/101/hash_list" }),
	{ok, {{<<"404">>, _}, _, _, _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(),
				path => "/block/hash/abcd/hash_list" }).

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
	ar_http_iface_client:send_tx_json(master_peer(), ValidTX#tx.id,
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(ValidTX))),
	{ok, {{<<"400">>, _}, _, <<"The attached data is split in an unknown way.">>, _, _}} =
		ar_http:req(#{
			method => post,
			peer => master_peer(),
			path => "/tx",
			body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(InvalidDataRootTX))
		}),
	ar_http_iface_client:send_tx_binary(master_peer(),
			InvalidDataRootTX#tx.id,
			ar_serialize:tx_to_binary(InvalidDataRootTX#tx{ data = <<>> })),
	ar_http_iface_client:send_tx_binary(master_peer(), EmptyTX#tx.id,
			ar_serialize:tx_to_binary(EmptyTX)),
	wait_until_receives_txs([ValidTX, EmptyTX, InvalidDataRootTX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	%% Ensure format=2 transactions can be retrieved over the HTTP
	%% interface with no populated data, while retaining info on all other fields.
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
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
			peer => master_peer(),
			path => "/tx/" ++ EncodedInvalidTXID ++ "/data"
		}),
	?assertEqual(<<>>, InvalidData),
	%% Ensure /tx/[ID]/data works for format=2 transactions when the data is empty.
	{ok, {{<<"200">>, _}, _, <<>>, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/tx/" ++ EncodedEmptyTXID ++ "/data"
		}),
	%% Ensure data can be fetched for format=2 transactions via /tx/[ID]/data.html.
	{ok, {{<<"200">>, _}, Headers, HTMLData, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
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
	ar_http_iface_client:send_tx_binary(master_peer(), TX#tx.id,
			ar_serialize:tx_to_binary(TX)),
	wait_until_receives_txs([TX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, Body} =
		ar_util:do_until(
			fun() ->
				case ar_http:req(#{
					method => get,
					peer => master_peer(),
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
	ar_http_iface_client:send_tx_json(master_peer(), TaggedTX#tx.id,
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
	ar_http_iface_client:send_tx_binary(master_peer(), TX#tx.id,
			ar_serialize:tx_to_binary(TX)),
	wait_until_receives_txs([TX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, FoundTXID} =
		ar_util:do_until(
			fun() ->
				case ar_http_iface_client:get_tx([master_peer()], TX#tx.id) of
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
	ar_http_iface_client:send_tx_binary(master_peer(), SignedTX#tx.id,
			ar_serialize:tx_to_binary(SignedTX)),
	wait_until_receives_txs([SignedTX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/wallet/"
					++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1)))
					++ "/last_tx"
		}),
	?assertEqual(ID, ar_util:decode(Body)).

%% @doc Post a tx to the network and ensure that its subfields can be gathered
test_get_subfields_of_tx(_) ->
	LocalHeight = ar_node:get_height(),
	TX = ar_tx:new(<<"DATA">>),
	ar_http_iface_client:send_tx_binary(master_peer(), TX#tx.id,
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
	ar_http_iface_client:send_tx_json(master_peer(), TX#tx.id,
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))),
	wait_until_receives_txs([TX]),
	{ok, {{<<"202">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
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
	connect_to_slave(),
	Height = ar_node:get_height(),
	assert_slave_wait_until_height(Height),
	disconnect_from_slave(),
	TX = (ar_tx:new())#tx{ tags = [{<<"TestName">>, <<"TestVal">>}] },
	assert_post_tx_to_master(TX),
	FetchStatus = fun() ->
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/status"
		})
	end,
	?assertMatch({ok, {{<<"202">>, _}, _, <<"Pending">>, _, _}}, FetchStatus()),
	ar_node:mine(),
	wait_until_height(Height + 1),
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
	wait_until_height(Height + 2),
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
	assert_slave_wait_until_height(Height + 1),
	slave_mine(),
	assert_slave_wait_until_height(Height + 2),
	connect_to_slave(),
	slave_mine(),
	wait_until_height(Height + 3),
	?assertMatch({ok, {{<<"202">>, _}, _, _, _, _}}, FetchStatus()).

test_post_unsigned_tx({_B0, Wallet1, _Wallet2, _StaticWallet}) ->
	LocalHeight = ar_node:get_height(),
	{_, Pub} = Wallet = Wallet1,
	%% Generate a wallet and receive a wallet access code.
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => master_peer(),
			path => "/wallet"
		}),
	{ok, Config} = application:get_env(arweave, config),
	application:set_env(arweave, config,
			Config#config{ internal_api_secret = <<"correct_secret">> }),
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => master_peer(),
			path => "/wallet",
			headers => [{<<"X-Internal-Api-Secret">>, <<"incorrect_secret">>}]
		}),
	{ok, {{<<"200">>, <<"OK">>}, _, CreateWalletBody, _, _}} =
		ar_http:req(#{
			method => post,
			peer => master_peer(),
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
			peer => master_peer(),
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
			peer => master_peer(),
			path => "/unsigned_tx",
			body => ar_serialize:jsonify({UnsignedTXProps})
		}),
	application:set_env(arweave, config,
			Config#config{ internal_api_secret = <<"correct_secret">> }),
	{ok, {{<<"421">>, _}, _, _, _, _}} =
		ar_http:req(#{
			method => post,
			peer => master_peer(),
			path => "/unsigned_tx",
			headers => [{<<"X-Internal-Api-Secret">>, <<"incorrect_secret">>}],
			body => ar_serialize:jsonify({UnsignedTXProps})
		}),
	{ok, {{<<"200">>, <<"OK">>}, _, Body, _, _}} =
		ar_http:req(#{
			method => post,
			peer => master_peer(),
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
			peer => master_peer(),
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
	ar_http_iface_client:send_tx_binary(master_peer(), TX#tx.id,
			ar_serialize:tx_to_binary(TX)),
	wait_until_receives_txs([TX]),
	ar_node:mine(),
	wait_until_height(LocalHeight + 1),
	{ok, _} = wait_until_syncs_tx_data(TX#tx.id),
	Resp =
		ar_http:req(#{
			method => get,
			peer => master_peer(),
			path => "/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/data",
			limit => Limit
		}),
	?assertEqual({error, too_much_data}, Resp).

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
	{ok, {{<<"200">>, _}, _, << B0H:48/binary, B1H:48/binary, 0:16 >> , _, _}} =
		ar_http:req(#{ method => get, peer => master_peer(),
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

wait_until_syncs_tx_data(TXID) ->
	ar_util:do_until(
		fun() ->
			case ar_http:req(#{
				method => get,
				peer => master_peer(),
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
