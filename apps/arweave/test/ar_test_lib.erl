-module(ar_test_lib).

-export([
	start_test_application/0, start_test_application/1,
	stop_test_application/0,

	start_peering/1, start_peering/2, stop_peering/1, stop_peering/2,

	mine/0, mine/1,
	get_tx_anchor/0, get_tx_anchor/1,
	post_tx/1, post_txs/1, post_txs_and_mine/1, post_txs_and_mine/2,
	get_chunk/1,
	post_chunks/1, post_chunks/2,

	sign_tx/2, sign_tx/3,
	sign_tx_v1/2, sign_tx_v1/3,

	wait_for_block/1, wait_for_block/2,
	wait_for_txs/1, wait_for_txs/2,
	wait_for_chunks/1, wait_for_chunks/2,

	read_block_when_stored/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

start_test_application() ->
	start_test_application(unclaimed).

start_test_application(RewardAddress) ->
	{ok, Config} = application:get_env(arweave, config),
	Disable = case os:type() of
		{unix, darwin} -> [randomx_jit];
		_ -> []
	end,
	ok = application:set_env(arweave, config, Config#config{
		mining_addr = RewardAddress,
		disable = Disable
	}),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	ok.

stop_test_application() ->
	{ok, Config} = application:get_env(arweave, config),
	ok = application:stop(arweave),
	%% Do not stop dependencies.
	os:cmd("rm -r " ++ Config#config.data_dir ++ "/*").

start_peering(Node, Peer) ->
	ct_rpc_call_strict(Node, ar_test_lib, start_peering, [Peer]).

start_peering(Peer) ->
	%% Connect the nodes by making two HTTP calls.
	%%
	%% After a request to a peer, the peer is recorded in ar_meta_db but
	%% not in the remote peer list. So we need to remove it from ar_meta_db
	%% otherwise it's not added to the remote peer list when it makes a request
	%% to us in turn.
	{ok, Config} = application:get_env(arweave, config),
	Port = Config#config.port,
	ct_rpc_call_strict(Peer, ar_meta_db, reset_peer, [{127, 0, 0, 1, Port}]),
	PeerPort = ct_rpc_call_strict(Peer, ar_meta_db, get, [port]),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, PeerPort},
			path => "/info",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	ar_meta_db:reset_peer({127, 0, 0, 1, PeerPort}),
	{ok, {{<<"200">>, <<"OK">>}, _, _, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, Port},
			path => "/info",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(PeerPort)}]
		}).

post_txs_and_mine(Node, TXs) when is_list(TXs) ->
	ct_rpc_call_strict(Node, ar_test_lib, post_txs_and_mine, [TXs]).

post_txs_and_mine(TXs) when is_list(TXs) ->
	Height = ar_node:get_height(),
	post_txs(TXs),
	ar_node:mine(),
	Height.

post_txs(TXs) when is_list(TXs) ->
	lists:foreach(fun(TX) -> ok = post_tx(TX) end, TXs),
	wait_for_txs(TXs).

post_tx(TX) ->
	{ok, Config} = application:get_env(arweave, config),
	Port = Config#config.port,
	Reply =
		ar_http:req(#{
			method => post,
			peer => {127, 0, 0, 1, Port},
			path => "/tx",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}],
			body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
		}),
	case Reply of
		{ok, {{<<"200">>, _}, _, <<"OK">>, _, _}} ->
			ok;
		E ->
			ct:log(
				"Failed to post transaction. Error DB entries: ~p - ~p ~n",
				[ar_tx_db:get_error_codes(TX#tx.id), E]
			),
			noop
	end.

wait_for_block(Node, Height) ->
	ct_rpc_call_strict(Node, ar_test_lib, wait_for_block, [Height]).

wait_for_block(Height) ->
	{ok, [{H, _, _} | _BI]} = ar_util:do_until(
		fun() ->
			case ar_node:get_blocks() of
				BI when length(BI) - 1 == Height ->
					{ok, BI};
				_ ->
					false
			end
		end,
		100,
		60 * 1000
	),
	BShadow = read_block_when_stored(H),
	BShadow#block{ txs = ar_storage:read_tx(BShadow#block.txs) }.

wait_for_txs(Node, TXs) ->
	ct_rpc_call_strict(Node, ar_test_lib, wait_for_txs, [TXs]).

wait_for_txs(TXs) ->
	ok = ar_util:do_until(
		fun() ->
			MinedTXIDs = [TX#tx.id || TX <- ar_node:get_ready_for_mining_txs()],
			case lists:all(fun(TX) -> lists:member(TX#tx.id, MinedTXIDs) end, TXs) of
				true ->
					ok;
				_ ->
					false
			end
		end,
		100,
		60 * 1000
	).

wait_for_chunks(Node, Proofs) ->
	ct_rpc_call_strict(Node, ar_test_lib, wait_for_chunks, [Proofs]).

wait_for_chunks([]) ->
	ok;
wait_for_chunks([{EndOffset, Proof} | Proofs]) ->
	true = ar_util:do_until(
		fun() ->
			case get_chunk(EndOffset) of
				{ok, {{<<"200">>, _}, _, EncodedProof, _, _}} ->
					FetchedProof = ar_serialize:json_map_to_chunk_proof(
						jiffy:decode(EncodedProof, [return_maps])
					),
					ExpectedProof = #{
						chunk => ar_util:decode(maps:get(chunk, Proof)),
						tx_path => ar_util:decode(maps:get(tx_path, Proof)),
						data_path => ar_util:decode(maps:get(data_path, Proof))
					},
					compare_proofs(FetchedProof, ExpectedProof);
				_ ->
					false
			end
		end,
		5 * 1000,
		120 * 1000
	),
	wait_for_chunks(Proofs).

get_chunk(Offset) ->
	{ok, Config} = application:get_env(arweave, config),
	Port = Config#config.port,
	ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, Port},
		path => "/chunk/" ++ integer_to_list(Offset)
	}).

post_chunk(Proof) ->
	{ok, Config} = application:get_env(arweave, config),
	Port = Config#config.port,
	ar_http:req(#{
		method => post,
		peer => {127, 0, 0, 1, Port},
		path => "/chunk",
		body => ar_serialize:jsonify(Proof)
	}).

post_chunks(Node, Proofs) ->
	ct_rpc_call_strict(Node, ar_test_lib, post_chunks, [Proofs]).

post_chunks([]) ->
	ok;
post_chunks([{_, Proof} | Proofs]) ->
	case post_chunk(Proof) of
		{ok, {{<<"200">>, _}, _, _, _, _}} ->
			post_chunks(Proofs);
		E ->
			{error, E}
	end.

read_block_when_stored(H) ->
	MaybeB = ar_util:do_until(
		fun() ->
			case ar_storage:read_block(H) of
				unavailable ->
					unavailable;
				B ->
					{ok, B}
			end
		end,
		100,
		5000
	),
	case MaybeB of
		{ok, B} ->
			B;
		_ ->
			MaybeB
	end.

stop_peering(Node, Peer) ->
	ct_rpc_call_strict(Node, ar_test_lib, stop_peering, [Peer]).

stop_peering(Peer) ->
	%% Disconnects this node from a peer so that they do not share blocks
	%% and transactions unless they were bound by ar_node:add_peers/2.
	%% The peers are added in ar_meta_db so that they do not start adding each other
	%% to their peer lists after disconnect.
	%% Also, all HTTP requests made in this module are made with the
	%% x-p2p-port HTTP header corresponding to the listening port of
	%% the receiving node so that freshly started nodes do not start peering
	%% unless connect_to_slave/0 is called.
	{ok, Config} = application:get_env(arweave, config),
	Port = Config#config.port,
	PeerPort = ct_rpc_call_strict(Peer, ar_meta_db, get, [port]),
	true = ar_meta_db:put({peer, {127, 0, 0, 1, PeerPort}}, #performance{}),
	true =
		ct_rpc_call_strict(
			Peer,
			ar_meta_db,
			put,
			[{peer, {127, 0, 0, 1, Port}}, #performance{}]
		),
	ar_bridge:set_remote_peers([]),
	ct_rpc_call_strict(Peer, ar_bridge, set_remote_peers, [[]]),
	application:set_env(arweave, config, Config#config{peers = []}),
	{ok, SlaveConfig} = ct_rpc_call_strict(Peer, application, get_env, [arweave, config]),
	ok =
		ct_rpc_call_strict(
			Peer,
			application,
			set_env,
			[arweave, config, SlaveConfig#config{ peers = [] }]
		).

mine(Node) ->
	ct_rpc_call_strict(Node, ar_test_lib, mine, []).

mine() ->
	Height = ar_node:get_height(),
	ar_node:mine(),
	Height.

get_tx_anchor(Node) ->
	ct_rpc_call_strict(Node, ar_test_lib, get_tx_anchor, []).

get_tx_anchor() ->
	{ok, Config} = application:get_env(arweave, config),
	Port = Config#config.port,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} =
		ar_http:req(#{
			method => get,
			peer => {127, 0, 0, 1, Port},
			path => "/tx_anchor",
			headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
		}),
	ar_util:decode(Reply).

sign_tx(Node, Wallet, TXParams) ->
	ct_rpc_call_strict(Node, ar_test_lib, sign_tx, [Wallet, TXParams]).

sign_tx(Wallet, TXParams) ->
	Data = maps:get(data, TXParams, <<>>),
	DataRoot = maps:get(data_root, TXParams, <<>>),
	TXParams1 = case {Data, DataRoot} of
		{<<>>, _} ->
			TXParams;
		{Data, <<>>} ->
			TX = ar_tx:generate_chunk_tree(#tx{ data = Data }),
			TXParams#{ data_root => TX#tx.data_root };
		_ ->
			TXParams
	end,
	sign_tx2(Wallet, TXParams1#{ format => 2 }, fun ar_tx:sign/2).

sign_tx_v1(Node, Wallet, TXParams) ->
	ct_rpc_call_strict(Node, ar_test_lib, sign_tx_v1, [Wallet, TXParams]).

sign_tx_v1(Wallet, TXParams) ->
	sign_tx2(Wallet, TXParams, fun ar_tx:sign_v1/2).

sign_tx2(Wallet, TXParams, SignFunction) ->
	{_, {_, Owner}} = Wallet,
	Data = maps:get(data, TXParams, <<>>),
	DataSize = maps:get(data_size, TXParams, byte_size(Data)),
	Reward = case maps:get(reward, TXParams, none) of
		none ->
			get_tx_price(DataSize);
		AssignedReward ->
			AssignedReward
	end,
	SignFunction(
		(ar_tx:new())#tx {
			owner = Owner,
			reward = Reward,
			data = Data,
			target = maps:get(target, TXParams, <<>>),
			quantity = maps:get(quantity, TXParams, 0),
			tags = maps:get(tags, TXParams, []),
			last_tx = maps:get(last_tx, TXParams, <<>>),
			data_size = DataSize,
			data_root = maps:get(data_root, TXParams, <<>>),
			format = maps:get(format, TXParams, 1)
		},
		Wallet
	).

get_tx_price(DataSize) ->
	{ok, Config} = application:get_env(arweave, config),
	Port = Config#config.port,
	{ok, {{<<"200">>, _}, _, Reply, _, _}} = ar_http:req(#{
		method => get,
		peer => {127, 0, 0, 1, Port},
		path => "/price/" ++ integer_to_binary(DataSize),
		headers => [{<<"X-P2p-Port">>, integer_to_binary(Port)}]
	}),
	binary_to_integer(Reply).

%%%===================================================================
%%% Private functions.
%%%===================================================================

compare_proofs(
	#{ chunk := C, data_path := D, tx_path := T },
	#{ chunk := C, data_path := D, tx_path := T }
) ->
	true;
compare_proofs(_, _) ->
	false.

%% @doc Run ct_rpc:call/4 and throw on {badrpc, Reason}.
ct_rpc_call_strict(Node, Mod, Fun, Args) ->
	case ct_rpc:call(Node, Mod, Fun, Args) of
		{badrpc, _Reason} = Error ->
			throw(Error);
		Reply ->
			Reply
	end.
