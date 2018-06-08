-module(ar_http_iface).
-export([start/0, start/1, start/2, start/3, start/4, start/5, handle/2, handle_event/3]).
-export([send_new_block/3, send_new_block/4, send_new_block/6, send_new_tx/2, get_block/2, get_tx/2, get_full_block/2, get_block_subfield/3, add_peer/1]).
-export([get_encrypted_block/2, get_encrypted_full_block/2]).
-export([get_info/1, get_info/2, get_peers/1, get_pending_txs/1, has_tx/2]).
-export([get_current_block/1]).
-export([reregister/1, reregister/2]).
-export([get_txs_by_send_recv_test_slow/0, get_full_block_by_hash_test_slow/0]).
-include("ar.hrl").
-include("../lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Exposes access to an internal Archain network to external nodes.

%% @doc Start the archain HTTP API and Returns a process ID.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) ->
	spawn(
		fun() ->
			{ok, PID} =
				elli:start_link(
					[
						{callback, ?MODULE},
						{body_timeout, ?NET_TIMEOUT},
						{header_timeout, ?CONNECT_TIMEOUT},
						{max_body_size, ?MAX_BODY_SIZE},
						{request_timeout, ?NET_TIMEOUT},
						{accept_timeout, ?CONNECT_TIMEOUT},
						{port, Port}
					]
				),
			receive stop -> elli:stop(PID) end
		end
	).
start(Port, Node) ->
	start(Port, Node, undefined).
start(Port, Node, SearchNode) ->
	start(Port, Node, SearchNode, undefined).
start(Port, Node, SearchNode, ServiceNode) ->
	start(Port, Node, SearchNode, ServiceNode, undefined).
start(Port, Node, SearchNode, ServiceNode, BridgeNode) ->
	reregister(http_entrypoint_node, Node),
	reregister(http_search_node, SearchNode),
	reregister(http_service_node, ServiceNode),
	reregister(http_bridge_node, BridgeNode),
	start(Port).

%%% Server side functions.

%% @doc Main function to handle a request to the nodes HTTP server.
%%
%% TODO: Add list detailing all available endpoints
%%
%% NB: Blocks and transactions are transmitted between HTTP nodes in JSON format.

handle(Req, _Args) ->
	update_performance_list(Req),
	handle(Req#req.method, elli_request:path(Req), Req).

%% @doc Return network information from a given node.
%% GET request to endpoint /info
%% GET request to endpoint /
handle('GET', [], _Req) ->
	return_info();
handle('GET', [<<"info">>], _Req) ->
	return_info();

%% @doc Return all transactions from node that are waiting to be mined into a block.
%% GET request to endpoint /tx/pending
handle('GET', [<<"tx">>, <<"pending">>], _Req) ->
	{200, [],
			ar_serialize:jsonify(
				%% Should encode
				lists:map(
					fun ar_util:encode/1,
					ar_node:get_pending_txs(whereis(http_entrypoint_node))
				)
			)
	};

%% @doc Return a transaction specified via the the transaction id (hash)
%% GET request to endpoint /tx/{hash}
handle('GET', [<<"tx">>, Hash], _Req) ->
	TX = ar_storage:read_tx(ar_util:decode(Hash)),
	case TX of
		unavailable ->
			case lists:member(ar_util:decode(Hash), ar_node:get_pending_txs(whereis(http_entrypoint_node))) of
				true ->
					{202, [], <<"Pending">>};
				false ->
					case ar_tx_db:get(ar_util:decode(Hash)) of
						not_found -> {404, [], <<"Not Found.">>};
						Err -> {410, [], list_to_binary(Err)}
					end
			end;
		T -> return_tx(T)
	end;

%% @doc Return the transaction IDs of all txs where the tags in post match the given set of key value pairs.
%% POST request to endpoint /arql with body of request being a logical expression valid in ar_parser.
%%
%% Example logical expression.
%% 	{
%% 		op: 	{ and | or | equals }
%% 		expr1: 	{ string | logical expression }
%% 		expr2: 	{ string | logical expression }
%% 	}
%%
handle('POST', [<<"arql">>], Req) ->
	QueryJson = elli_request:body(Req),
	Query = ar_serialize:json_struct_to_query(
		binary_to_list(QueryJson)
	),
	TXs = ar_util:unique(ar_parser:eval(Query)),
	case TXs of
		[] -> {200, [], []};
		Set ->
			{
				200,
				[],
				ar_serialize:jsonify(
					ar_serialize:hash_list_to_json_struct(Set)
				)
			}
	end;

%% @doc Return the data field of the transaction specified via the transaction ID (hash) served as HTML.
%% GET request to endpoint /tx/{hash}/data.html
handle('GET', [<<"tx">>, Hash, <<"data.html">>], _Req) ->
	TX = ar_storage:read_tx(ar_util:decode(Hash)),
	case TX of
		unavailable ->
			case lists:member(
					ar_util:decode(Hash),
					ar_node:get_pending_txs(whereis(http_entrypoint_node))
				) of
				true ->
					{202, [], <<"Pending">>};
				false ->
					{ok, File} = file:read_file("data/not_found.html"),
					{404, [], File}
			end;
		T -> {200, [], T#tx.data}
	end;

%% @doc Share a new block to a peer.
%% POST request to endpoint /block with the body of the request being a JSON encoded block as specified in ar_serialize.
handle('POST', [<<"block">>], Req) ->
	BlockJSON = elli_request:body(Req),
	{Struct} = ar_serialize:dejsonify(BlockJSON),
	{<<"recall_block">>, JSONRecallB} = lists:keyfind(<<"recall_block">>, 1, Struct),
	{<<"new_block">>, JSONB} = lists:keyfind(<<"new_block">>, 1, Struct),
	{<<"recall_size">>, RecallSize} = lists:keyfind(<<"recall_size">>, 1, Struct),
	{<<"port">>, Port} = lists:keyfind(<<"port">>, 1, Struct),
	{<<"key">>, KeyEnc} = lists:keyfind(<<"key">>, 1, Struct),
	{<<"nonce">>, NonceEnc} = lists:keyfind(<<"nonce">>, 1, Struct),
	Key = ar_util:decode(KeyEnc),
	Nonce = ar_util:decode(NonceEnc),
	BShadow = ar_serialize:json_struct_to_block(JSONB),
	case ar_block:verify_timestamp(os:system_time(seconds), BShadow) of
		true ->
			OrigPeer =
				ar_util:parse_peer(
					bitstring_to_list(elli_request:peer(Req))
					++ ":"
					++ integer_to_list(Port)
					),
			TXs = lists:foldr(
				fun(T, Acc) ->
					%state contains it
					case [TX || TX <- ar_node:get_all_known_txs(whereis(http_entrypoint_node)), TX#tx.id == T] of
						[] ->
							case ar_storage:read_tx(T) of
								unavailable ->
									Acc;
								TX -> [TX|Acc]
							end;
						[TX|_] -> [TX|Acc]
					end
				end,
				[],
				BShadow#block.txs
			),
			{FinderPool, _} = ar_node:calculate_reward_pool(
				ar_node:get_reward_pool(whereis(http_entrypoint_node)),
				TXs,
				BShadow#block.reward_addr,
				ar_node:calculate_proportion(
					RecallSize,
					BShadow#block.weave_size,
					BShadow#block.height
				)
				),
			HashList =
				case {BShadow#block.hash_list, ar_node:get_hash_list(whereis(http_entrypoint_node))} of
					{[], []} -> [];
					{[], N} -> N;
					{S, []} -> S;
					{S, N} ->
						S ++
						case lists:dropwhile(
								fun(X) ->
									case S of
										[] -> false;
										_ -> not (X == lists:last(S))
										end
								end,
								N
							)
						of
							[] -> S ++ N;
							List -> tl(List)
						end
				end,
			%ar:d({hashlist, HashList}),
			WalletList =
				ar_node:apply_mining_reward(
					ar_node:apply_txs(ar_node:get_wallet_list(whereis(http_entrypoint_node)), TXs),
					BShadow#block.reward_addr,
					FinderPool,
					BShadow#block.height
				),
			B = BShadow#block { wallet_list = WalletList, hash_list = HashList },
			RecallHash = ar_util:decode(JSONRecallB),
			RecallB =
				case ar_storage:read_block(RecallHash) of
					unavailable ->
						case ar_storage:read_encrypted_block(RecallHash) of
							unavailable ->
								FullBlock = ar_http_iface:get_full_block(OrigPeer, RecallHash),					
								case ?IS_BLOCK(FullBlock)  of
									true ->
										Recall = FullBlock#block {txs = [ T#tx.id || T <- FullBlock#block.txs] },
										ar_storage:write_tx(FullBlock#block.txs),
										ar_storage:write_block(Recall),
										Recall;
									false -> unavailable
								end;
							EncryptedRecall ->
								FBlock = ar_block:decrypt_full_block(B, EncryptedRecall, Key, Nonce),
								case FBlock of
									unavailable -> unavailable;
									FullBlock ->
										Recall = FullBlock#block {txs = [ T#tx.id || T <- FullBlock#block.txs] },
										ar_storage:write_tx(FullBlock#block.txs),
										ar_storage:write_block(Recall),
										Recall
								end
						end;
					Recall -> Recall
				end,
				%ar_bridge:ignore_id(whereis(http_bridge_node), {B#block.indep_hash, OrigPeer}),
				%ar:report_console([{recvd_block, B#block.height}, {port, Port}]),
			ar_bridge:add_block(
				whereis(http_bridge_node),
				OrigPeer,
				B,
				RecallB,
				Key,
				Nonce
			),
			{200, [], <<"OK">>};
		false ->
			{404, [], <<"Invalid Block">>}
	end;

%% @doc Share a new transaction with a peer.
%% POST request to endpoint /tx with the body of the request being a JSON encoded tx as specified in ar_serialize.
handle('POST', [<<"tx">>], Req) ->
	TXJSON = elli_request:body(Req),
	TX = ar_serialize:json_struct_to_tx(TXJSON),
	case ar_node:get_current_diff(whereis(http_entrypoint_node)) of
		unavailable -> {503, [], <<"Transaction verification failed.">>};
		Diff ->
			FloatingWalletList = ar_node:get_wallet_list(whereis(http_entrypoint_node)),
			% OrigPeer =
			% 	ar_util:parse_peer(
			% 		bitstring_to_list(elli_request:peer(Req))
			% 		++ ":"
			% 		++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
			% 		),
			case ar_tx:verify(TX, Diff, FloatingWalletList) of
				false ->
					%ar:d({rejected_tx , ar_util:encode(TX#tx.id)}),
					{400, [], <<"Transaction verification failed.">>};
				true ->
					%ar:d({accepted_tx , ar_util:encode(TX#tx.id)}),
					ar_bridge:add_tx(whereis(http_bridge_node), TX),%, OrigPeer),
					{200, [], <<"OK">>}
			end
	end;
		
%% @doc Return the list of peers held by the node.
%% GET request to endpoint /peers
handle('GET', [<<"peers">>], Req) ->
	{200, [],
		ar_serialize:jsonify(
			[
				list_to_binary(ar_util:format_peer(P))
			||
				P <- ar_bridge:get_remote_peers(whereis(http_bridge_node)),
				P /= ar_util:parse_peer(elli_request:peer(Req))
			]
		)
	};

%% @doc Return the estimated reward cost of transactions with a data body size of 'bytes'.
%% GET request to endpoint /price/{bytes}
%% TODO: Change so current block does not need to be pulled to calculate cost
handle('GET', [<<"price">>, SizeInBytes], _Req) ->
	{200, [],
		integer_to_binary(
			ar_tx:calculate_min_tx_cost(
				list_to_integer(
					binary_to_list(SizeInBytes)
				),
				ar_node:get_current_diff(whereis(http_entrypoint_node))
			)
		)
	};

%% @doc Return the current hash list held by the node.
%% GET request to endpoint /hash_list
handle('GET', [<<"hash_list">>], _Req) ->
	Node = whereis(http_entrypoint_node),
    HashList = ar_node:get_hash_list(Node),
    {200, [],
		ar_serialize:jsonify(
		ar_serialize:hash_list_to_json_struct(HashList)
		)

    };

%% @doc Return the current wallet list held by the node.
%% GET request to endpoint /wallet_list
handle('GET', [<<"wallet_list">>], _Req) ->
    Node = whereis(http_entrypoint_node),
    WalletList = ar_node:get_wallet_list(Node),
    {200, [],
		ar_serialize:jsonify(
		ar_serialize:wallet_list_to_json_struct(WalletList)
        )
    };

%% @doc Share your nodes IP with another peer.
%% POST request to endpoint /peers with the body of the request being your
%% nodes network information JSON encoded as specified in ar_serialize.
% NOTE: Consider returning remaining timeout on a failed request
handle('POST', [<<"peers">>], Req) ->
	BlockJSON = elli_request:body(Req),
	case ar_serialize:dejsonify(BlockJSON) of
		{Struct} ->
			{<<"network">>, NetworkName} = lists:keyfind(<<"network">>, 1, Struct),
			case(NetworkName == <<?NETWORK_NAME>>) of
				false ->
					{400, [], <<"Wrong network.">>};
				true ->
					Peer = elli_request:peer(Req),
					case ar_meta_db:get({peer, ar_util:parse_peer(Peer)}) of
						not_found ->
							ar_bridge:add_remote_peer(whereis(http_bridge_node), ar_util:parse_peer(Peer));
						X -> X
					end,
					{200, [], []}
			end;
		_ -> {400, [], "Wrong network"}
	end;
handle('POST', [<<"peers">>, <<"port">>, RawPort], Req) ->
	BlockJSON = elli_request:body(Req),
	{Struct} = ar_serialize:dejsonify(BlockJSON),
	case lists:keyfind(<<"network">>, 1, Struct) of
		{<<"network">>, NetworkName} ->
			case(NetworkName == <<?NETWORK_NAME>>) of
				false ->
					{400, [], <<"Wrong network.">>};
				true ->
					Peer = elli_request:peer(Req),
					Port = list_to_integer(binary_to_list(RawPort)),
					ar_bridge:add_remote_peer(
						whereis(http_bridge_node),
						ar_util:parse_peer({Peer, Port})
						),
					{200, [], []}
			end;
		_ -> {400, [], "Wrong network"}
	end;

%% @doc Return the balance of the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/balance
handle('GET', [<<"wallet">>, Addr, <<"balance">>], _Req) ->
	{200, [],
		integer_to_binary(
			ar_node:get_balance(
				whereis(http_entrypoint_node),
				ar_util:decode(Addr)
			)
		)
	};

%% @doc Return the last transaction ID (hash) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/last_tx
handle('GET', [<<"wallet">>, Addr, <<"last_tx">>], _Req) ->
	{200, [],
		ar_util:encode(
			ar_node:get_last_tx(
				whereis(http_entrypoint_node),
				ar_util:decode(Addr)
			)
		)
	};

%% @doc Return the encrypted blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/encrypted
handle('GET', [<<"block">>, <<"hash">>, Hash, <<"encrypted">>], _Req) ->
	%ar:d({resp_block_hash, Hash}),
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	case ar_key_db:get(ar_util:decode(Hash)) of
		[{Key, Nonce}] ->
			return_encrypted_block(
				ar_node:get_block(
					whereis(http_entrypoint_node),
					ar_util:decode(Hash)
				),
				Key,
				Nonce
			);
		not_found ->
			ar:d(not_found_block),
			return_encrypted_block(unavailable)
	end;

%% @doc Return the full encrypted block corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/all/encrypted
handle('GET', [<<"block">>, <<"hash">>, Hash, <<"all">>, <<"encrypted">>], _Req) ->
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	case ar_key_db:get(ar_util:decode(Hash)) of
		[{Key, Nonce}] ->
			return_encrypted_full_block(
				ar_node:get_full_block(
					whereis(http_entrypoint_node),
					ar_util:decode(Hash)
				),
				Key,
				Nonce
			);
		not_found ->
			ar:d(not_found_block),
			return_encrypted_full_block(unavailable)
	end;

%% @doc Return the blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}
handle('GET', [<<"block">>, <<"hash">>, Hash], _Req) ->
	%CurrentBlock = ar_node:get_current_block(whereis(http_entrypoint_node)),
	case ar_node:get_hash_list(whereis(http_entrypoint_node)) of
		[Head|HashList] ->
			case
				(ar_util:decode(Hash) == ar_util:get_recall_hash((length(HashList)), Head, HashList)) and
				((length(HashList) + 1) > 10)
			of
				true ->
					return_block(unavailable);
				false ->
					case lists:member(
							ar_util:decode(Hash),
							[Head|HashList]
						) of
						true ->
							return_block(
								ar_node:get_block(whereis(http_entrypoint_node),
									ar_util:decode(Hash))
							);
						false -> return_block(unavailable)
					end
			end;
		_ ->
			return_block(
				ar_node:get_block(whereis(http_entrypoint_node),
					ar_util:decode(Hash))
			)
	end;
%% @doc Return the full block corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/all
handle('GET', [<<"block">>, <<"hash">>, Hash, <<"all">>], _Req) ->
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	case ar_node:get_hash_list(whereis(http_entrypoint_node)) of
		[Head|HashList] ->
			case
				(ar_util:decode(Hash) == ar_util:get_recall_hash((length(HashList)), Head, HashList)) and
				((length(HashList) + 1) > 10)
			of
				true ->
					return_block(unavailable);
				false ->
					case lists:member(
							ar_util:decode(Hash),
							[Head|HashList]
						) of
						true ->
							FullBlock =
								ar_node:get_full_block(
									whereis(http_entrypoint_node),
									ar_util:decode(Hash)
								),
							return_full_block(FullBlock);
						false -> return_block(unavailable)
					end
			end;
		_ ->
			FullBlock =
				ar_node:get_full_block(
					whereis(http_entrypoint_node),
					ar_util:decode(Hash)
				),
			return_full_block(FullBlock)
	end;

%% @doc Return the block at the given height.
%% GET request to endpoint /block/height/{height}
%% TODO: Add handle for negative block numbers
handle('GET', [<<"block">>, <<"height">>, Height], _Req) ->
	case ar_node:get_hash_list(whereis(http_entrypoint_node)) of
		[Head|HashList] ->
			case (list_to_integer(binary_to_list(Height))+1) > length([Head|HashList]) of
				false ->
					case
						((Hash = lists:nth( list_to_integer(binary_to_list(Height))+1,lists:reverse([Head|HashList]))) ==
						ar_util:get_recall_hash((length(HashList)), Head, HashList)) and
						((length(HashList) + 1) > 10)
					of
						true -> return_block(unavailable);
						false ->
							case lists:member(
									Hash,
									[Head|HashList]
								) of
								true ->
									return_block(
										ar_node:get_block(whereis(http_entrypoint_node),
											Hash)
									);
								false -> return_block(unavailable)
							end
					end;
				true -> return_block(unavailable)
			end;
		_ ->
			return_block(
				ar_node:get_block(whereis(http_entrypoint_node),
					0)
			)
	end;

%% @doc Return the current block.
%% GET request to endpoint /current_block
%% GET request to endpoint /block/current
handle('GET', [<<"block">>, <<"current">>], _Req) ->
	return_block(ar_node:get_current_block(whereis(http_entrypoint_node)));
handle('GET', [<<"current_block">>], _Req) ->
	return_block(ar_node:get_current_block(whereis(http_entrypoint_node)));

%% @doc Return a list of known services. 
%% GET request to endpoint /services
handle('GET', [<<"services">>], _Req) ->
	{200, [],
		ar_serialize:jsonify(
			{
				[
					{
						[
							{"name", Name},
							{"host", ar_util:format_peer(Host)},
							{"expires", Expires}
						]
					}
				||
					#service {
						name = Name,
						host = Host,
						expires = Expires
					} <- ar_services:get(whereis(http_service_node))
				]
			}
		)
	};

%% @doc Return a given field of the transaction specified by the transaction ID (hash).
%% GET request to endpoint /tx/{hash}/{field} 
%%  
%% {field} := { id | last_tx | owner | tags | target | quantity | data | signature | reward }
%% 
handle('GET', [<<"tx">>, Hash, Field], _Req) ->
	TX = ar_storage:read_tx(ar_util:decode(Hash)),
	case TX of
		unavailable ->
			case lists:member(ar_util:decode(Hash), ar_node:get_pending_txs(whereis(http_entrypoint_node))) of
				true ->
					{202, [], <<"Pending">>};
				false ->
					{404, [], <<"Not Found.">>}
			end;
		T ->
			{TXJSON} = ar_serialize:tx_to_json_struct(T),
			{_, Res} = lists:keyfind(list_to_existing_atom(binary_to_list(Field)), 1, TXJSON),
			{200, [], Res}
	end;

%% @doc Return a given field of the blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/{field}
%%
%% {field} := { nonce | previous_block | timestamp | last_retarget | diff | height | hash | indep_hash
%% 				txs | hash_list | wallet_list | reward_addr | tags | reward_pool }
%%
handle('GET', [<<"block">>, <<"hash">>, Hash, Field], _Req) ->
	Block = ar_storage:read_block(ar_util:decode(Hash)),
	case Block of
		unavailable ->
				{404, [], <<"Not Found.">>};
		B ->
			{BLOCKJSON} = ar_serialize:block_to_json_struct(B),
			{_, Res} = lists:keyfind(list_to_existing_atom(binary_to_list(Field)), 1, BLOCKJSON),
			Result = block_field_to_string(Field, Res),
			{200, [], Result}
	end;

%% @doc Return a given field for the the blockshadow corresponding to the block height, 'height'.
%% GET request to endpoint /block/hash/{height}/{field}
%%
%% {field} := { nonce | previous_block | timestamp | last_retarget | diff | height | hash | indep_hash
%% 				txs | hash_list | wallet_list | reward_addr | tags | reward_pool }
%%
handle('GET', [<<"block">>, <<"height">>, Height, Field], _Req) ->
	Block = ar_node:get_block(whereis(http_entrypoint_node),
			list_to_integer(binary_to_list(Height))),
	case Block of
		unavailable ->
				{404, [], <<"Not Found.">>};
		B ->
			{BLOCKJSON} = ar_serialize:block_to_json_struct(B),
			{_, Res} = lists:keyfind(list_to_existing_atom(binary_to_list(Field)), 1, BLOCKJSON),
			Result = block_field_to_string(Field, Res),
			{200, [], Result}
	end;

%% @doc Share the location of a given service with a peer.
%% POST request to endpoint /services where the body of the request is a JSON encoded serivce as
%% specified in ar_serialize.
handle('POST', [<<"services">>], Req) ->
	BodyBin = elli_request:body(Req),
	{ServicesJSON} = ar_serialize:jsonify(BodyBin),
	ar_services:add(
		whereis(http_services_node),
		lists:map(
			fun({Vals}) ->
				{<<"name">>, Name} = lists:keyfind(<<"name">>, 1, Vals),
				{<<"host">>, Host} = lists:keyfind(<<"host">>, 1, Vals),
				{<<"expires">>, Expiry} = lists:keyfind(<<"expires">>, 1, Vals),
				#service { name = Name, host = Host, expires = Expiry }
			end,
			ServicesJSON
		)
	),
	{200, [], "OK"};

%% @doc Catch case for requests made to unknown endpoints. 
%% Returns error code 500 - Request type not found.
handle(_, _, _) ->
	{500, [], <<"Request type not found.">>}.
%% @doc Handles all other elli metadata events.
handle_event(Type, Data, Args)
		when (Type == request_throw)
		or (Type == request_error)
		or (Type == request_exit) ->
	ar:report([{elli_event, Type}, {data, Data}, {args, Args}]);
handle_event(_Type, _Data, _Args) -> ok.
	%ar:report_console([{elli_event, Type}, {data, Data}, {args, Args}]).

%% @doc Return a block in JSON via HTTP or 404 if can't be found.
return_block(unavailable) -> {404, [], <<"Block not found.">>};
return_block(not_found) -> {404, [], <<"Block not found.">>};
return_block(B) ->
	{200, [],
		ar_serialize:jsonify(
			ar_serialize:block_to_json_struct(B)
		)
	}.
return_encrypted_block(unavailable) -> {404, [], <<"Block not found.">>}.
return_encrypted_block(unavailable, _, _) -> {404, [], <<"Block not found.">>};
return_encrypted_block(B, Key, Nonce) ->
	{200, [],
		ar_util:encode(ar_block:encrypt_block(B, Key, Nonce))
	}.

%% @doc Return a full block in JSON via HTTP or 404 if can't be found.
return_full_block(unavailable) -> {404, [], <<"Block not found.">>};
return_full_block(B) ->
	{200, [],
		ar_serialize:jsonify(
			ar_serialize:full_block_to_json_struct(B)
		)
	}.
return_encrypted_full_block(unavailable) -> {404, [], <<"Block not found.">>}.
return_encrypted_full_block(unavailable, _, _) -> {404, [], <<"Block not found.">>};
return_encrypted_full_block(B, Key, Nonce) ->
	{200, [],
		ar_util:encode(ar_block:encrypt_full_block(B, Key, Nonce))
	}.

%% @doc Return a tx in JSON via HTTP or 404 if can't be found.
return_tx(unavailable) -> {404, [], <<"TX not found.">>};
return_tx(T) ->
	{200, [],
		ar_serialize:jsonify(
			ar_serialize:tx_to_json_struct(T)
		)
	}.

%% @doc Generate and return an informative JSON object regarding
%% the state of the node.
return_info() ->
	{200, [],
		ar_serialize:jsonify(
			{
				[
					{network, list_to_binary(?NETWORK_NAME)},
					{version, ?CLIENT_VERSION},
					{height,
						case ar_node:get_hash_list(whereis(http_entrypoint_node)) of
							[] -> 0;
							Hashes -> (length(Hashes) - 1)
						end},
					{blocks, ar_storage:blocks_on_disk()},
					{peers, length(ar_bridge:get_remote_peers(whereis(http_bridge_node)))}
				]
			}
		)
	}.

%%% Client functions

%% @doc Send a new transaction to an Archain HTTP node.
send_new_tx(Host, TX) ->
	if 
		(byte_size(TX#tx.data) < 50000) ->
			ar_httpc:request(
				<<"POST">>,
				"http://" ++ ar_util:format_peer(Host),
				"/tx",
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
			);
		true ->
			case has_tx(Host, TX#tx.id) of
				false ->
					ar_httpc:request(
						<<"POST">>,
						"http://" ++ ar_util:format_peer(Host),
						"/tx",
						ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
					);
				true -> not_sent
			end
	end.


%% @doc Check whether a peer has a given transaction
has_tx(Host, ID) ->
	case 
		ar_httpc:request(
				<<"GET">>,
				"http://" ++ ar_util:format_peer(Host),
				"/tx/" ++ binary_to_list(ar_util:encode(ID)) ++ "/id",
				[]
		)
	of
		{ok, {{<<"200">>, _}, _, _, _, _}} -> true;
		{ok, {{<<"202">>, _}, _, _, _, _}} -> true;
		_ -> false
	end.


%% @doc Distribute a newly found block to remote nodes.
send_new_block(IP, NewB, RecallB) ->
	send_new_block(IP, ?DEFAULT_HTTP_IFACE_PORT, NewB, RecallB).
send_new_block(Host, Port, NewB, RecallB) ->
	%ar:report_console([{sending_new_block, NewB#block.height}, {stack, erlang:get_stacktrace()}]),
	NewBShadow = NewB#block { wallet_list= [], hash_list = lists:sublist(NewB#block.hash_list,1,?STORE_BLOCKS_BEHIND_CURRENT)},
	RecallBHash =
		case ?IS_BLOCK(RecallB) of
			true ->  RecallB#block.indep_hash;
			false -> <<>>
		end,
	case ar_key_db:get(RecallBHash) of
		[{Key, Nonce}] ->
			ar_httpc:request(
				<<"POST">>,
				"http://" ++ ar_util:format_peer(Host),
				"/block",
				ar_serialize:jsonify(
					{
						[
							{<<"new_block">>, ar_serialize:block_to_json_struct(NewBShadow)},
							{<<"recall_block">>, ar_util:encode(RecallBHash)},
							{<<"recall_size">>, RecallB#block.block_size},
							{<<"port">>, Port},
							{<<"key">>, ar_util:encode(Key)},
							{<<"nonce">>, ar_util:encode(Nonce)}
						]
					}
				)

			);
		%TODO: 
		_ ->
			ar_httpc:request(
			<<"POST">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block",
			ar_serialize:jsonify(
				{
					[
						{<<"new_block">>, ar_serialize:block_to_json_struct(NewBShadow)},
						{<<"recall_block">>, ar_util:encode(RecallBHash)},
						{<<"recall_size">>, RecallB#block.block_size},
						{<<"port">>, Port},
						{<<"key">>, <<>>},
						{<<"nonce">>, <<>>}
					]
				}
			)

		)
	end.
send_new_block(Host, Port, NewB, RecallB, Key, Nonce) ->
	NewBShadow = NewB#block { wallet_list= [], hash_list = lists:sublist(NewB#block.hash_list,1,?STORE_BLOCKS_BEHIND_CURRENT)},
	RecallBHash =
		case ?IS_BLOCK(RecallB) of
			true ->  RecallB#block.indep_hash;
			false -> <<>>
		end,
		ar_httpc:request(
			<<"POST">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block",
			ar_serialize:jsonify(
				{
					[
						{<<"new_block">>, ar_serialize:block_to_json_struct(NewBShadow)},
						{<<"recall_block">>, ar_util:encode(RecallBHash)},
						{<<"recall_size">>, RecallB#block.block_size},
						{<<"port">>, Port},
						{<<"key">>, ar_util:encode(Key)},
						{<<"nonce">>, ar_util:encode(Nonce)}
					]
				}
			)

		).

%% @doc Add peer (self) to a remote host.
add_peer(Host) ->
	ar_httpc:request(
		<<"POST">>,
		"http://" ++ ar_util:format_peer(Host),
		"/peers",
		ar_serialize:jsonify(
			{
				[
					{network, list_to_binary(?NETWORK_NAME)}
				]
			}
		)
	).

%% @doc Get a peers current, top block.
get_current_block(Host) ->
	handle_block_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/current_block",
			[],
			10000
		)
	).

%% @doc Get the minimum cost that a remote peer would charge for
%% A transaction of data size Size
get_tx_reward(Host, Size) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/price/" ++ integer_to_list(Size),
			[]
		),
	list_to_integer(binary_to_list(Body)).

%% @doc Retreive a block by height or hash from a remote peer.
get_block(Host, Height) when is_integer(Height) ->
	%ar:report_console([{req_getting_block_by_height, Height}]),
	%ar:d([getting_new_block, {host, Host}, {height, Height}]),
	handle_block_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block/height/" ++ integer_to_list(Height),
			[]
	 	)
	);
get_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_block_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)),
			[]
	 	)
	).

%% @doc Get a block in encrypted format from a remote peer.
get_encrypted_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_encrypted_block_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/encrypted",
			[]
		)
	).

%% @doc Get a specified subfield from the block with the given hash
%% or height from a remote peer
get_block_subfield(Host, Hash, Subfield) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host,[] Host}, {hash, Hash}]),
	handle_block_field_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/" ++ Subfield,
			[]
		)
	);
get_block_subfield(Host, Height, Subfield) when is_integer(Height) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_block_field_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block/height/" ++integer_to_list(Height) ++ "/" ++ Subfield,
			[]
	 	)
	).

%% @doc Retreive a full block (full transactions included in body)
%% by hash from a remote peer.
get_full_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_full_block_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/all",
			[]
		)
	).

%% @doc Retreive a full block (full transactions included in body)
%% by hash from a remote peer in an encrypted form
get_encrypted_full_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_encrypted_full_block_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/all/encrypted",
			[]
		)
	).

%% @doc Retreive a tx by hash from a remote peer
get_tx(Host, Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_new_block, {host, Host}, {hash, Hash}]),
	handle_tx_response(
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Host),
			"/tx/" ++ binary_to_list(ar_util:encode(Hash)),
			[]
	 	)
	).

%% @doc Retreive all valid transactions held that have not yet been mined into
%% a block from a remote peer.
get_pending_txs(Peer) ->
	try
		begin
			{ok, {{200, _}, _, Body, _, _}} =
				ar_httpc:request(
					<<"GET">>,
					"http://" ++ ar_util:format_peer(Peer),
					"/tx/pending",
					[]
				),
			PendingTxs = ar_serialize:dejsonify(Body),
			[list_to_binary(P) || P <- PendingTxs]
		end
	catch _:_ -> []
	end.

%% @doc Retreive information from a peer. Optionally, filter the resulting
%% keyval list for required information.
get_info(Peer, Type) ->
	case get_info(Peer) of
		info_unavailable -> info_unavailable;
		Info ->
			{Type, X} = lists:keyfind(Type, 1, Info),
			X
	end.
get_info(Peer) ->
	case 
		ar_httpc:request(
			<<"GET">>,
			"http://" ++ ar_util:format_peer(Peer),
			"/info",
			[],
			3000
		)
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} -> process_get_info(Body);
		_ -> info_unavailable
	end.

%% @doc Return a list of parsed peer IPs for a remote server.
get_peers(Peer) ->
	try
		begin
			{ok, {{<<"200">>, _}, _, Body, _, _}} =
				ar_httpc:request(
				<<"GET">>,	
				"http://" ++ ar_util:format_peer(Peer),
				"/peers",
				[]
				),
			PeerArray = ar_serialize:dejsonify(Body),
			lists:map(fun ar_util:parse_peer/1, PeerArray)
		end
	catch _:_ -> []
	end.

%% @doc Produce a key value list based on a /info response.
process_get_info(Body) ->
	{Struct} = ar_serialize:dejsonify(Body),
	{_, NetworkName} = lists:keyfind(<<"network">>, 1, Struct),
	{_, ClientVersion} = lists:keyfind(<<"version">>, 1, Struct),
	{_, Height} = lists:keyfind(<<"height">>, 1, Struct),
	{_, Blocks} = lists:keyfind(<<"blocks">>, 1, Struct),
	{_, Peers} = lists:keyfind(<<"peers">>, 1, Struct),
	[
		{name, NetworkName},
		{version, ClientVersion},
		{height, Height},
		{blocks, Blocks},
		{peers, Peers}
	].

%% @doc Process the response of an /block call.
handle_block_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_serialize:json_struct_to_block(Body);
handle_block_response({error, _}) -> unavailable;
handle_block_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_block_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> unavailable.

%% @doc Process the response of a /block/.../all call.
handle_full_block_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_serialize:json_struct_to_full_block(Body);
handle_full_block_response({error, _}) -> unavailable;
handle_full_block_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_full_block_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> unavailable.

%% @doc Process the response of a /block/.../encrypted call.
handle_encrypted_block_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_util:decode(Body);
handle_encrypted_block_response({error, _}) -> unavailable;
handle_encrypted_block_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_encrypted_block_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> unavailable.

%% @doc Process the response of a /block/.../all/encrypted call.
handle_encrypted_full_block_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_util:decode(Body);
handle_encrypted_full_block_response({error, _}) -> unavailable;
handle_encrypted_full_block_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_encrypted_full_block_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> unavailable.

%% @doc Process the response of a /block/[{Height}|{Hash}]/{Subfield} call.
handle_block_field_response({"timestamp", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	list_to_integer(binary_to_list(Body));
handle_block_field_response({"last_retarget", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	list_to_integer(binary_to_list(Body));
handle_block_field_response({"diff", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	list_to_integer(binary_to_list(Body));
handle_block_field_response({"height", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	list_to_integer(binary_to_list(Body));
handle_block_field_response({"txs", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	ar_serialize:json_struct_to_tx(Body);
handle_block_field_response({"hash_list", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	ar_serialize:json_struct_to_hash_list(Body);
handle_block_field_response({"wallet_list", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	ar_serialize:json_struct_to_wallet_list(Body);
handle_block_field_response({_Subfield, {ok, {{<<"200">>, _}, _, Body, _, _}}}) -> Body;
handle_block_field_response({error, _}) -> unavailable;
handle_block_field_response({ok, {{<<"404">>, _}, _, _}}) -> not_found;
handle_block_field_response({ok, {{<<"500">>, _}, _, _}}) -> unavailable.

%% @doc Process the response of a /tx call.
handle_tx_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_serialize:json_struct_to_tx(Body);
handle_tx_response({ok, {{<<"202">>, _}, _, _, _, _}}) -> pending;
handle_tx_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_tx_response({ok, {{<<"410">>, _}, _, _, _, _}}) -> gone;
handle_tx_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> not_found.

%% @doc Helper function : registers a new node as the entrypoint.
reregister(Node) ->
	reregister(http_entrypoint_node, Node).
reregister(_, undefined) -> not_registering;
reregister(Name, Node) ->
	case erlang:whereis(Name) of
		undefined -> do_nothing;
		_ -> erlang:unregister(Name)
	end,
	erlang:register(Name, Node).

%% @doc If a post request, update the peer performance DB
%% For a get request, ar_httpc is doing this for us
update_performance_list(Req) ->
	case Req#req.method of
		'POST' ->
			MicroSecs = ar_util:time_difference(
				os:timestamp(),
				elli_request:upload_start_timestamp(Req)
				),
			Peer = ar_util:parse_peer(elli_request:peer(Req)),
			Bytes = byte_size(elli_request:body(Req)),
			store_data_time(Peer, Bytes, MicroSecs),
			case ar_meta_db:get({peer, ar_util:parse_peer(elli_request:peer(Req))}) of
				not_found -> ar_bridge:add_remote_peer(whereis(http_bridge_node), ar_util:parse_peer(elli_request:peer(Req)));
				X -> X
			end,
			ok;
		'GET' -> 
			case ar_meta_db:get({peer, ar_util:parse_peer(elli_request:peer(Req))}) of
				not_found -> ar_bridge:add_remote_peer(whereis(http_bridge_node), ar_util:parse_peer(elli_request:peer(Req)));
				X -> X
			end,
			ok;
		_ -> ok
	end.


%% @doc Store the request data on the peer performance DB
store_data_time(Peer, Bytes, MicroSecs) ->
	P =
		case ar_meta_db:get({peer, Peer}) of
			not_found -> #performance{};
			X -> X
		end,
	ar_meta_db:put({peer, Peer},
		P#performance {
			transfers = P#performance.transfers + 1,
			time = P#performance.time + MicroSecs,
			bytes = P#performance.bytes + Bytes,
			timeout = os:system_time(seconds)
		}
	).

%% @doc Convert a blocks field with the given label into a string
block_field_to_string(<<"nonce">>, Res) -> Res;
block_field_to_string(<<"previous_block">>, Res) -> Res;
block_field_to_string(<<"timestamp">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"last_retarget">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"diff">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"height">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"hash">>, Res) -> Res;
block_field_to_string(<<"indep_hash">>, Res) -> Res;
block_field_to_string(<<"txs">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"hash_list">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"wallet_list">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"reward_addr">>, Res) -> Res.

%%% Tests

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_info_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	reregister(http_bridge_node, BridgeNode),
	<<?NETWORK_NAME>> = get_info({127,0,0,1,1984}, name),
	?CLIENT_VERSION = get_info({127,0,0,1,1984}, version),
	1 = get_info({127,0,0,1,1984}, peers),
	1 = get_info({127,0,0,1,1984}, blocks),
	0 = get_info({127,0,0,1,1984}, height).

%% @doc Ensure transaction reward can be retrieved via http iface.
get_tx_reward_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	% Hand calculated result for 1000 bytes.
	ExpectedPrice = ar:d(ar_tx:calculate_min_tx_cost(1000, B0#block.diff)),
	ExpectedPrice = ar:d(get_tx_reward({127,0,0,1,1984}, 1000)).

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_unjoined_info_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([]),
	reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	reregister(http_bridge_node, BridgeNode),
	<<?NETWORK_NAME>> = get_info({127,0,0,1,1984}, name),
	?CLIENT_VERSION = get_info({127,0,0,1,1984}, version),
	0 = get_info({127,0,0,1,1984}, peers),
	0 = get_info({127,0,0,1,1984}, blocks),
	0 = get_info({127,0,0,1,1984}, height).

%% @doc Check that balances can be retreived over the network.
get_balance_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<>>}]),
	Node1 = ar_node:start([], Bs),
	reregister(Node1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://127.0.0.1:1984",
			"/wallet/"++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/balance",
			[]
		),
	10000 = list_to_integer(binary_to_list(Body)).

%% @doc Test that wallets issued in the pre-sale can be viewed.
get_presale_balance_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<>>}]),
	Node1 = ar_node:start([], Bs),
	reregister(Node1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://127.0.0.1:1984",
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/balance",
			[]
		),
	10000 = list_to_integer(binary_to_list(Body)).

%% @doc Test that last tx associated with a wallet can be fetched.
get_last_tx_single_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<"TEST_ID">>}]),
	Node1 = ar_node:start([], Bs),
	reregister(Node1),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://127.0.0.1:1984",
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx",
			[]
		),
	<<"TEST_ID">> = ar_util:decode(Body).

%% @doc Ensure that blocks can be received via a hash.
get_block_by_hash_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	receive after 200 -> ok end,
	B0 = get_block({127, 0, 0, 1}, B0#block.indep_hash).

% get_recall_block_by_hash_test() ->
% 	ar_storage:clear(),
%     [B0] = ar_weave:init([]),
%     ar_storage:write_block(B0),
%     [B1|_] = ar_weave:add([B0], []),
% 	ar_storage:write_block(B1),
% 	Node1 = ar_node:start([], [B1, B0]),
% 	reregister(Node1),
% 	receive after 200 -> ok end,
% 	not_found = get_block({127, 0, 0, 1}, B0#block.indep_hash).

%% @doc Ensure that full blocks can be received via a hash.
get_full_block_by_hash_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
	SignedTX2 = ar_tx:sign(TX2, Priv2, Pub2),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	receive after 200 -> ok end,
	send_new_tx({127, 0, 0, 1}, SignedTX),
	receive after 200 -> ok end,
	send_new_tx({127, 0, 0, 1}, SignedTX2),
	receive after 200 -> ok end,
	ar_node:mine(Node),
	receive after 200 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	B2 = get_block({127, 0, 0, 1}, B1),
	B3 = get_full_block({127, 0, 0, 1}, B1),
	B3 = B2#block {txs = [SignedTX, SignedTX2]}.

%% @doc Ensure that blocks can be received via a height.
get_block_by_height_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	B0 = get_block({127, 0, 0, 1}, 0).

get_current_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	B0 = get_current_block({127, 0, 0, 1}).

%% @doc Test adding transactions to a block.
add_external_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	send_new_tx({127, 0, 0, 1}, TX = ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	TXID = TX#tx.id,
	[TXID] = (ar_storage:read_block(B1))#block.txs.

%% @doc Test getting transactions
find_external_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	reregister(http_search_node, SearchNode),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	send_new_tx({127, 0, 0, 1}, TX = ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	FoundTXID = (get_tx({127, 0, 0, 1},TX#tx.id))#tx.id,
	FoundTXID = TX#tx.id.

fail_external_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	reregister(http_search_node, SearchNode),
	send_new_tx({127, 0, 0, 1}, ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	BadTX = ar_tx:new(<<"BADDATA">>),
	not_found = get_tx({127, 0, 0, 1}, BadTX#tx.id).

%% @doc Ensure that blocks can be added to a network from outside
%% a single node.
add_external_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	Bridge = ar_bridge:start([], Node1),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node1, Bridge),
	Node2 = ar_node:start([], [B0]),
	ar_node:mine(Node2),
	receive after 1000 -> ok end,
	[B1|_] = ar_node:get_blocks(Node2),
	reregister(Node1),
	send_new_block({127, 0, 0, 1}, ?DEFAULT_HTTP_IFACE_PORT, ar_storage:read_block(B1), B0),
	receive after 500 -> ok end,
	[B1, XB0] = ar_node:get_blocks(Node1),
	B0 = ar_storage:read_block(XB0).

%% @doc Post a tx to the network and ensure that last_tx call returns the ID of last tx.
add_tx_and_get_last_test() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	{_Priv2, Pub2} = ar_wallet:new(),
	TX = ar_tx:new(ar_wallet:to_address(Pub2), ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	ID = SignedTX#tx.id,
	send_new_tx({127, 0, 0, 1}, SignedTX),
	receive after 500 -> ok end,
	ar_node:mine(Node),
	receive after 500 -> ok end,
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://127.0.0.1:1984",
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx",
			[]
		),
	ID = ar_util:decode(Body).

%% @doc Post a tx to the network and ensure that its subfields can be gathered
get_subfields_of_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	reregister(http_search_node, SearchNode),
	send_new_tx({127, 0, 0, 1}, TX = ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://127.0.0.1:1984",
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/data",
			[]
		),
	Orig = TX#tx.data,
	Orig = ar_util:decode(Body).

%% @doc Correctly check the status of pending is returned for a pending transaction
get_pending_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	reregister(http_search_node, SearchNode),
	send_new_tx({127, 0, 0, 1}, TX = ar_tx:new(<<"DATA1">>)),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	{ok, {{<<"202">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://127.0.0.1:1984",
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)),
			[]
		),
	Body == "Pending".

%% @doc Correctly check the status of pending is returned for a pending transaction
get_pending_subfield_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	reregister(http_search_node, SearchNode),
	send_new_tx({127, 0, 0, 1}, TX = ar_tx:new(<<"DATA1">>)),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	{ok, {{<<"202">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://127.0.0.1:1984",
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/data",
			[]
		),
	Body == "Pending".

%% @doc Find all pending transactions in the network
%% TODO: Fix test to send txs from different wallets
get_multiple_pending_txs_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	SearchNode = app_search:start(Node),
	ar_node:add_peers(Node, SearchNode),
	reregister(http_search_node, SearchNode),
	send_new_tx({127, 0, 0, 1}, TX1 = ar_tx:new(<<"DATA1">>)),
	receive after 1000 -> ok end,
	send_new_tx({127, 0, 0, 1}, TX2 = ar_tx:new(<<"DATA2">>)),
	receive after 1000 -> ok end,
	%write a get_tx function like get_block
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			"http://127.0.0.1:1984",
			"/tx/" ++ "pending",
			[]
		),
	PendingTXs = ar_serialize:dejsonify(Body),
	[TX1#tx.id, TX2#tx.id] == [P || P <- PendingTXs].

get_tx_by_tag_test() ->
	% Spawn a network with two nodes and a chirper server
	ar_storage:clear(),
	SearchServer = app_search:start(),
	Peers = ar_network:start(10, 10),
	ar_node:add_peers(hd(Peers), SearchServer),
	% Generate the transaction.
	TX = (ar_tx:new())#tx {tags = [{<<"TestName">>, <<"TestVal">>}]},
	% Add tx to network
	ar_node:add_tx(hd(Peers), TX),
	% Begin mining
	receive after 250 -> ok end,
	ar_node:mine(hd(Peers)),
	receive after 1000 -> ok end,
	QueryJSON = ar_serialize:jsonify(
		ar_serialize:query_to_json_struct(
			{'equals', <<"TestName">>, <<"TestVal">>}
			)
		),
	{ok, {_, _, Body, _, _}} = 
		ar_httpc:request(
			<<"POST">>,
			"http://127.0.0.1:1984",
			"/arql",		
			QueryJSON		
		),
	TXs = ar_serialize:dejsonify(Body),
	true =
		lists:member(
			TX#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		).

get_txs_by_send_recv_test_slow() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	{_Priv3, Pub3} = ar_wallet:new(),
	TX = ar_tx:new(Pub2, ?AR(1), ?AR(9000), <<>>),
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	TX2 = ar_tx:new(Pub3, ?AR(1), ?AR(500), <<>>),
	SignedTX2 = ar_tx:sign(TX2, Priv2, Pub2),
	B0 = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}], 8),
	Node1 = ar_node:start([], B0),
	Node2 = ar_node:start([Node1], B0),
	ar_node:add_peers(Node1, Node2),
	ar_node:add_tx(Node1, SignedTX),
	ar_storage:write_tx([SignedTX]),
	receive after 300 -> ok end,
	ar_node:mine(Node1), % Mine B1
	receive after 1000 -> ok end,
    ar_node:add_tx(Node2, SignedTX2),
    ar_storage:write_tx([SignedTX2]),
	receive after 1000 -> ok end,
	ar_node:mine(Node2), % Mine B2
    receive after 1000 -> ok end,
	QueryJSON = ar_serialize:jsonify(
		ar_serialize:query_to_json_struct(
				{'or', {'equals', "to", TX#tx.target}, {'equals', "from", TX#tx.target}}
			)
		),
	{ok, {_, _, Res, _, _}} = 
		ar_httpc:request(
			<<"POST">>,
			"http://127.0.0.1:1984",
			"/arql",
			QueryJSON
		),
	TXs = ar_serialize:dejsonify(Res),
	true =
		lists:member(
			SignedTX#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		),
	true =
		lists:member(
			SignedTX2#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		).

% get_encrypted_block_test() ->
% 	ar_storage:clear(),
% 	[B0] = ar_weave:init([]),
% 	Node1 = ar_node:start([], [B0]),
% 	reregister(Node1),
% 	receive after 200 -> ok end,
% 	Enc0 = get_encrypted_block({127, 0, 0, 1}, B0#block.indep_hash),
% 	ar_storage:write_encrypted_block(B0#block.indep_hash, Enc0),
% 	ar_cleanup:remove_invalid_blocks([]),
% 	send_new_block(
% 		{127,0,0,1},
% 		B0,
% 		B0
% 	),
% 	receive after 500 -> ok end,
% 	B0 = ar_node:get_current_block(whereis(http_entrypoint_node)),
% 	ar_node:mine(Node1).

% get_encrypted_full_block_test() ->
% 	ar_storage:clear(),
%     B0 = ar_weave:init([]),
%     ar_storage:write_block(B0),
% 	TX = ar_tx:new(<<"DATA1">>),
% 	TX1 = ar_tx:new(<<"DATA2">>),
% 	ar_storage:write_tx([TX, TX1]),
% 	Node = ar_node:start([], B0),
% 	reregister(Node),
% 	ar_node:mine(Node),
% 	receive after 500 -> ok end,
% 	[B1|_] = ar_node:get_blocks(Node),
% 	Enc0 = get_encrypted_full_block({127, 0, 0, 1}, (hd(B0))#block.indep_hash),
% 	ar_storage:write_encrypted_block((hd(B0))#block.indep_hash, Enc0),
% 	ar_cleanup:remove_invalid_blocks([B1]),
% 	send_new_block(
% 		{127,0,0,1},
% 		hd(B0),
% 		hd(B0)
% 	),
% 	receive after 1000 -> ok end,
% 	ar_node:mine(Node).
	% ar_node:add_peers(Node, Bridge),
	% receive after 200 -> ok end,
	% send_new_tx({127, 0, 0, 1}, TX = ar_tx:new(<<"DATA1">>)),
	% receive after 200 -> ok end,
	% send_new_tx({127, 0, 0, 1}, TX1 = ar_tx:new(<<"DATA2">>)),
	% receive after 200 -> ok end,
	% ar_node:mine(Node),
	% receive after 200 -> ok end,
	% [B1|_] = ar_node:get_blocks(Node),
	% B2 = get_block({127, 0, 0, 1}, B1),
	% ar:d(get_encrypted_full_block({127, 0, 0, 1}, B2#block.indep_hash)),
	% B2 = get_block({127, 0, 0, 1}, B1),
	% B3 = get_full_block({127, 0, 0, 1}, B1),
	% B3 = B2#block {txs = [TX, TX1]},
