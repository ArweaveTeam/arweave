-module(ar_test_lib).

-export([
	start_test_application/0, start_test_application/1,
	stop_test_application/0,

	join/1, leave/1,

	get_tx_anchor/0,
	post_tx/1, post_txs/1, post_txs_mine/1,
	get_chunk/1,
	post_chunk/1, post_chunks/1,

	sign_tx/1,sign_tx/2,
	sign_tx_v1/1, sign_tx_v1/2,

	wait_block/1,
	wait_txs/1,
	wait_chunks/1,

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
		max_poa_option_depth = 20,
		disable = Disable
	}),
	{ok, _} = application:ensure_all_started(arweave, permanent),
	ok.

stop_test_application() ->
	{ok, Config} = application:get_env(arweave, config),
	ok = application:stop(arweave),
	% do not stop dependencies.
	os:cmd("rm -r " ++ Config#config.data_dir ++ "/*").

join(Peer) ->
	%% Connect the nodes by making two HTTP calls.
	%%
	%% After a request to a peer, the peer is recorded in ar_meta_db but
	%% not in the remote peer list. So we need to remove it from ar_meta_db
	%% otherwise it's not added to the remote peer list when it makes a request
	%% to us in turn.
	{ok, Config} = application:get_env(arweave, config),
	Port = Config#config.port,
	ct_rpc:call(Peer, ar_meta_db, reset_peer, [{127, 0, 0, 1, Port}]),
	PeerPort = ct_rpc:call(Peer, ar_meta_db, get, [port]),
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

post_txs_mine(TXs) when is_list(TXs) ->
	Height = ar_node:get_height(),
	ok = post_txs(TXs),
	ar_node:mine(),
	Height.

post_txs(TXs) when is_list(TXs) ->
	lists:foreach(fun(TX) -> ok = post_tx(TX) end, TXs),
	wait_txs(TXs).

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
		_ ->
			?LOG_INFO(
				"Failed to post transaction. Error DB entries: ~p~n",
				[ar_tx_db:get_error_codes(TX#tx.id)]
			),
			noop
	end.

wait_block(Height) ->
	{ok, _BI} = ar_util:do_until(
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
	H = ar_node:get_current_block_hash(),
	BShadow = read_block_when_stored(H),
	BShadow#block{ txs = ar_storage:read_tx(BShadow#block.txs) }.

wait_txs(TXs) ->
	ar_util:do_until(
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

wait_chunks([]) ->
	ok;
wait_chunks([{EndOffset, Proof} | Proofs]) ->
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
	wait_chunks(Proofs).

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
	J = jiffy:encode(Proof),
	ar_http:req(#{
		method => post,
		peer => {127, 0, 0, 1, Port},
		path => "/chunk",
		body => J
	}).

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

leave(Peer) ->
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
	PeerPort = ct_rpc:call(Peer, ar_meta_db, get, [port]),
	ar_meta_db:put({peer, {127, 0, 0, 1, PeerPort}}, #performance{}),
	ct_rpc:call(Peer, ar_meta_db, put, [{peer, {127, 0, 0, 1, Port}}]),
	ct_rpc:call(Peer, ar_bridge, set_remote_peers, [[]]),
	ar_bridge:set_remote_peers([]),
	ar_node:set_trusted_peers([]),
	ct_rpc:call(Peer, ar_node, set_trusted_peers, [[]]).

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


sign_tx(Wallet) ->
	sign_tx(Wallet, #{ format => 2 }, fun ar_tx:sign/2).
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
	sign_tx(Wallet, TXParams1#{ format => 2 }, fun ar_tx:sign/2).

sign_tx_v1(Wallet) ->
	sign_tx_v1(Wallet, #{}).
sign_tx_v1(Wallet, TXParams) ->
	sign_tx(Wallet, TXParams, fun ar_tx:sign_v1/2).

sign_tx(Wallet, TXParams, SignFunction) ->
	{_, Pub} = Wallet,
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
			owner = Pub,
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
%%
%% private functions
%%


compare_proofs(
	#{ chunk := C, data_path := D, tx_path := T },
	#{ chunk := C, data_path := D, tx_path := T }
) ->
	true;
compare_proofs(_, _) ->
	false.
