-module(ar_http_iface).
-export([start/0, start/1, start/2, start/3, start/4, start/5, handle/2, handle_event/3]).
-export([send_new_block/3, send_new_block/4, send_new_tx/2, get_block/2, get_tx/2, get_full_block/2, get_block_subfield/3, add_peer/1]).
-export([get_encrypted_block/2, get_encrypted_full_block/2]).
-export([get_info/1, get_info/2, get_peers/1, get_pending_txs/1]).
-export([get_current_block/1]).
-export([reregister/1, reregister/2]).
-include("ar.hrl").
-include("../lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Exposes access to an internal Archain network to external nodes.

%% The maximum size of a single POST body.
-define(MAX_BODY_SIZE, 1024 * 1024 * 1024 * 512).

%% @doc Start the archain HTTP API and Returns a process ID.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) ->
	spawn(
		fun() ->
			{ok, PID} =
				elli:start_link(
					[
						{callback, ?MODULE},
						{max_body_size, ?MAX_BODY_SIZE},
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
		list_to_binary(
			ar_serialize:jsonify(
				{array,
					%% Should encode
						lists:map(
							fun ar_util:encode/1,
							ar_node:get_pending_txs(whereis(http_entrypoint_node))
						)
				}
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
					{404, [], <<"Not Found.">>}
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
				list_to_binary(
					ar_serialize:jsonify(
						ar_serialize:hash_list_to_json_struct(Set)
					)
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
	{ok, {struct, Struct}} = json2:decode_string(binary_to_list(BlockJSON)),
	{"recall_block", JSONRecallB} = lists:keyfind("recall_block", 1, Struct),
	{"new_block", JSONB} = lists:keyfind("new_block", 1, Struct),
	{"port", Port} = lists:keyfind("port", 1, Struct),
	{"key", KeyEnc} = lists:keyfind("key", 1, Struct),
	Key = ar_util:decode(KeyEnc),
	BShadow = ar_serialize:json_struct_to_block(JSONB),
	OrigPeer =
		ar_util:parse_peer(
			bitstring_to_list(elli_request:peer(Req))
			++ ":"
			++ integer_to_list(Port)
			),
	B = BShadow#block {
		wallet_list = ar_node:apply_txs(
			ar_node:get_wallet_list(whereis(http_entrypoint_node)),
			ar_storage:read_tx(BShadow#block.txs)
		),
		hash_list = ar_node:get_hash_list(whereis(http_entrypoint_node))
		},
	RecallHash = ar_util:decode(JSONRecallB),
	RecallB =
		case ar_storage:read_block(RecallHash) of
			unavailable ->
				case ar_storage:read_encrypted_block(RecallHash) of
					unavailable ->
						unavailable;
					EncryptedRecall -> 
						FullBlock = ar_block:decrypt_full_block(B, EncryptedRecall, Key),
						Recall = FullBlock#block {txs = [ T#tx.id || T <- FullBlock#block.txs] },
						ar_storage:write_block(Recall),
						ar_storage:write_tx(FullBlock#block.txs),
						Recall
				end;
			Recall -> Recall
		end,
		%ar_bridge:ignore_id(whereis(http_bridge_node), {B#block.indep_hash, OrigPeer}),
		%ar:report_console([{recvd_block, B#block.height}, {port, Port}]),
	ar_bridge:add_block(
		whereis(http_bridge_node),
		OrigPeer,
		B,
		RecallB
	),
	{200, [], <<"OK">>};

%% @doc Share a new transaction with a peer.
%% POST request to endpoint /tx with the body of the request being a JSON encoded tx as specified in ar_serialize.
handle('POST', [<<"tx">>], Req) ->
	TXJSON = elli_request:body(Req),
	TX = ar_serialize:json_struct_to_tx(binary_to_list(TXJSON)),
	Diff = ar_node:get_current_diff(whereis(http_entrypoint_node)),
	FloatingWalletList = ar_node:get_wallet_list(whereis(http_entrypoint_node)),
	case ar_tx:verify(TX, Diff, FloatingWalletList) of
		false ->
			ar:d({rejected_tx , ar_util:encode(TX#tx.id)}),
			{400, [], <<"Transaction verification failed.">>};
		true ->
			ar:d({accepted_tx , ar_util:encode(TX#tx.id)}),
			ar_bridge:add_tx(whereis(http_bridge_node), TX),
			{200, [], <<"OK">>}
	end;
 
%% @doc Return the list of peers held by the node.
%% GET request to endpoint /peers
handle('GET', [<<"peers">>], Req) ->
	{200, [],
		list_to_binary(
			ar_serialize:jsonify(
				{array,
					[
						ar_util:format_peer(P)
					||
						P <- ar_bridge:get_remote_peers(whereis(http_bridge_node)),
						P /= ar_util:parse_peer(elli_request:peer(Req))
					]
				}
			)
		)
	};

%% @doc Return the estimated reward cost of transactions with a data body size of 'bytes'.
%% GET request to endpoint /price/{bytes}
handle('GET', [<<"price">>, SizeInBytes], _Req) ->
	Node = whereis(http_entrypoint_node),
	B = ar_node:get_current_block(Node),
	{200, [],
		integer_to_list(
			ar_tx:calculate_min_tx_cost(
				list_to_integer(
					binary_to_list(SizeInBytes)
				),
				B#block.diff
			)
		)
	};

%% @doc Return the current hash list held by the node.
%% GET request to endpoint /hash_list
handle('GET', [<<"hash_list">>], _Req) ->
	Node = whereis(http_entrypoint_node),
    HashList = ar_node:get_hash_list(Node),
    {200, [],
        list_to_binary(
            ar_serialize:jsonify(
                ar_serialize:hash_list_to_json_struct(HashList)
            )
        )
    };

%% @doc Return the current wallet list held by the node.
%% GET request to endpoint /wallet_list
handle('GET', [<<"wallet_list">>], _Req) ->
    Node = whereis(http_entrypoint_node),
    WalletList = ar_node:get_wallet_list(Node),
    {200, [],
        list_to_binary(
            ar_serialize:jsonify(
                ar_serialize:wallet_list_to_json_struct(WalletList)
            )
        )
    };

%% @doc Share your nodes IP with another peer.
%% POST request to endpoint /peers with the body of the request being your
%% nodes network information JSON encoded as specified in ar_serialize.
% NOTE: Consider returning remaining timeout on a failed request
handle('POST', [<<"peers">>], Req) ->
	BlockJSON = elli_request:body(Req),
	case json2:decode_string(binary_to_list(BlockJSON)) of
		{ok, {struct, Struct}} ->
			{"network", NetworkName} = lists:keyfind("network", 1, Struct),
			case(NetworkName == ?NETWORK_NAME) of
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
	{ok, {struct, Struct}} = json2:decode_string(binary_to_list(BlockJSON)),
	case lists:keyfind("network", 1, Struct) of
		{"network", NetworkName} ->
			case(NetworkName == ?NETWORK_NAME) of
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
		list_to_binary(
			integer_to_list(
				ar_node:get_balance(
					whereis(http_entrypoint_node),
					ar_util:decode(Addr)
				)
			)
		)
	};

%% @doc Return the last transaction ID (hash) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/last_tx
handle('GET', [<<"wallet">>, Addr, <<"last_tx">>], _Req) ->
	{200, [],
		list_to_binary(
			ar_util:encode(
				ar_node:get_last_tx(
					whereis(http_entrypoint_node),
					ar_util:decode(Addr)
				)
			)
		)
	};

%% @doc Return the encrypted blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/encrypted
handle('GET', [<<"block">>, <<"hash">>, Hash, <<"encrypted">>], _Req) ->
	%ar:d({resp_block_hash, Hash}),
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	CurrentBlock = ar_node:get_current_block(whereis(http_entrypoint_node)),
	case CurrentBlock of
		unavailable -> return_encrypted_block(unavailable);
		_ ->
			case lists:member(
					ar_util:decode(Hash),
					[CurrentBlock#block.indep_hash|CurrentBlock#block.hash_list]
				) of
				true ->
					return_encrypted_block(
						ar_node:get_block(
							whereis(http_entrypoint_node),
							ar_util:decode(Hash)
							),
						CurrentBlock
					);
				false -> return_encrypted_block(unavailable)
			end
	end;

%% @doc Return the full encrypted block corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/all/encrypted
handle('GET', [<<"block">>, <<"hash">>, Hash, <<"all">>, <<"encrypted">>], _Req) ->
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	CurrentBlock = ar_node:get_current_block(whereis(http_entrypoint_node)),
	case CurrentBlock of
		unavailable -> return_encrypted_full_block(unavailable);
		_ ->
			case lists:member(
					ar_util:decode(Hash),
					[CurrentBlock#block.indep_hash|CurrentBlock#block.hash_list]
				) of
				true ->
					return_encrypted_full_block(
						ar_node:get_full_block(
							whereis(http_entrypoint_node),
							ar_util:decode(Hash)
							),
						CurrentBlock
					);
				false -> return_encrypted_full_block(unavailable)
			end
	end;

%% @doc Return the blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}
handle('GET', [<<"block">>, <<"hash">>, Hash], _Req) ->
	CurrentBlock = ar_node:get_current_block(whereis(http_entrypoint_node)),
	case CurrentBlock of
		unavailable -> return_block(unavailable);
		_ ->
			case ((ar_util:decode(Hash) == 
					ar_node:find_recall_hash(CurrentBlock, CurrentBlock#block.hash_list)) and (CurrentBlock#block.height > 10))
			of
				true -> return_block(unavailable);
				false ->
					case lists:member(
							ar_util:decode(Hash),
							[CurrentBlock#block.indep_hash|CurrentBlock#block.hash_list]
						) of
						true ->
							return_block(
								ar_node:get_block(whereis(http_entrypoint_node),
									ar_util:decode(Hash))
							);
						false -> return_block(unavailable)
					end
			end
	end;

%% @doc Return the full block corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/all
handle('GET', [<<"block">>, <<"hash">>, Hash, <<"all">>], _Req) ->
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	CurrentBlock = ar_node:get_current_block(whereis(http_entrypoint_node)),
	case CurrentBlock of
		unavailable -> return_block(unavailable);
		_ ->
			case ((ar_util:decode(Hash) == 
				ar_node:find_recall_hash(CurrentBlock, CurrentBlock#block.hash_list)) and (CurrentBlock#block.height > 10))
			of
				true -> return_block(unavailable);
				false ->
					case lists:member(
							ar_util:decode(Hash),
							[CurrentBlock#block.indep_hash|CurrentBlock#block.hash_list]
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
			end
	end;

%% @doc Return the block at the given height.
%% GET request to endpoint /block/height/{height}
handle('GET', [<<"block">>, <<"height">>, Height], _Req) ->
	CurrentBlock = ar_node:get_current_block(whereis(http_entrypoint_node)),
	Block = ar_node:get_block(
		whereis(http_entrypoint_node),
		list_to_integer(binary_to_list(Height))
		),
	case (?IS_BLOCK(Block) and ?IS_BLOCK(CurrentBlock)) of
		true ->
			case (Block#block.hash == 
				ar_node:find_recall_hash(CurrentBlock, CurrentBlock#block.hash_list) )
			of
				true -> return_block(unavailable);
				false -> return_block(Block)
			end;
		false -> return_block(unavailable)
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
			{array,
				[
					{struct,
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
			{struct, TXJSON} = ar_serialize:tx_to_json_struct(T),
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
			{struct, BLOCKJSON} = ar_serialize:block_to_json_struct(B),
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
			{struct, BLOCKJSON} = ar_serialize:block_to_json_struct(B),
			{_, Res} = lists:keyfind(list_to_existing_atom(binary_to_list(Field)), 1, BLOCKJSON),
			Result = block_field_to_string(Field, Res),
			{200, [], Result}
	end;

%% @doc Share the location of a given service with a peer.
%% POST request to endpoint /services where the body of the request is a JSON encoded serivce as
%% specified in ar_serialize.
handle('POST', [<<"services">>], Req) ->
	BodyBin = elli_request:body(Req),
	{ok, {struct, ServicesJSON}} = json2:decode_string(binary_to_list(BodyBin)),
	ar_services:add(
		whereis(http_services_node),
		lists:map(
			fun({struct, Vals}) ->
				{"name", Name} = lists:keyfind("name", 1, Vals),
				{"host", Host} = lists:keyfind("host", 1, Vals),
				{"expires", Expiry} = lists:keyfind("expires", 1, Vals),
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
return_block(B) ->
	{200, [],
		list_to_binary(
			ar_serialize:jsonify(
				ar_serialize:block_to_json_struct(B)
			)
		)
	}.
return_encrypted_block(unavailable) -> {404, [], <<"Block not found.">>}.
return_encrypted_block(_, unavailable) -> {404, [], <<"Block not found.">>};
return_encrypted_block(unavailable, _) -> {404, [], <<"Block not found.">>};
return_encrypted_block(R, B) ->
	{Key, EncryptedR} = ar_block:encrypt_block(R, B),
	{200, [],
		ar_util:encode(EncryptedR)
	}.

%% @doc Return a full block in JSON via HTTP or 404 if can't be found.
return_full_block(unavailable) -> {404, [], <<"Block not found.">>};
return_full_block(B) ->
	{200, [],
		list_to_binary(
			ar_serialize:jsonify(
				ar_serialize:full_block_to_json_struct(B)
			)
		)
	}.
return_encrypted_full_block(unavailable) -> {404, [], <<"Block not found.">>}.
return_encrypted_full_block(_, unavailable) -> {404, [], <<"Block not found.">>};
return_encrypted_full_block(unavailable, _) -> {404, [], <<"Block not found.">>};
return_encrypted_full_block(R, B) ->
	{Key, EncryptedR} = ar_block:encrypt_full_block(R, B),
	{200, [],
		ar_util:encode(EncryptedR)
	}.

%% @doc Return a tx in JSON via HTTP or 404 if can't be found.
return_tx(unavailable) -> {404, [], <<"TX not found.">>};
return_tx(T) ->
	{200, [],
		list_to_binary(
			ar_serialize:jsonify(
				ar_serialize:tx_to_json_struct(T)
			)
		)
	}.

%% @doc Generate and return an informative JSON object regarding
%% the state of the node.
return_info() ->
	{200, [],
		list_to_binary(
			ar_serialize:jsonify(
				{struct,
					[
						{network, ?NETWORK_NAME},
						{version, ?CLIENT_VERSION},
						{height,
							case ar_node:get_blocks(whereis(http_entrypoint_node)) of
								[H|_] ->
									case ar_storage:read_block(H) of
										unavailable -> 0;
										B -> B#block.height
									end;
								_ -> 0
							end},
						{blocks, ar_storage:blocks_on_disk()},
						{peers, length(ar_bridge:get_remote_peers(whereis(http_bridge_node)))}
					]
				}
			)
		)
	}.

%%% Client functions

%% @doc Send a new transaction to an Archain HTTP node.
send_new_tx(Host, TX) ->
	ar_httpc:request(
		post,
		{
			"http://" ++ ar_util:format_peer(Host) ++ "/tx",
			[],
			"application/x-www-form-urlencoded",
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
		},
		[{timeout, ?NET_TIMEOUT}],
		[]
	).

%% @doc Distribute a newly found block to remote nodes.
send_new_block(IP, NewB, RecallB) ->
	send_new_block(IP, ?DEFAULT_HTTP_IFACE_PORT, NewB, RecallB).
send_new_block(Host, Port, NewB, RecallB) ->
	%ar:report_console([{sending_new_block, NewB#block.height}, {stack, erlang:get_stacktrace()}]),
	NewBShadow = NewB#block { wallet_list= [], hash_list = []},
	RecallBHash =
		case ?IS_BLOCK(RecallB) of
			true ->  RecallB#block.indep_hash;
			false -> <<>>
		end,
	ar_httpc:request(
		post,
		{
			"http://" ++ ar_util:format_peer(Host) ++ "/block",
			[],
			"application/x-www-form-urlencoded",
			lists:flatten(
				ar_serialize:jsonify(
					{struct,
						[
							{new_block,
								ar_serialize:block_to_json_struct(NewBShadow)},
							{recall_block,
								ar_util:encode(RecallBHash)},
							{port, Port},
							{key, ar_util:encode(ar_block:generate_block_key(RecallB, NewB))}
						]
					}
				)
			)
		}, [{timeout, ?NET_TIMEOUT}], []
	).



%% @doc Add peer (self) to host.
add_peer(Host) ->
	ar_httpc:request(
		post,
		{
			"http://"
				++ ar_util:format_peer(Host)
				++ "/peers",
			[],
			"application/x-www-form-urlencoded",
			lists:flatten(
				ar_serialize:jsonify(
					{struct,
						[
							{network, ?NETWORK_NAME}
						]
					}
				)
			)
		}, [{timeout, ?NET_TIMEOUT}], []
	).

%% @doc Get the current, top block.
get_current_block(Host) ->
	handle_block_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/current_block/",
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
		)
	).

%% @doc Calculate transaction reward.
get_tx_reward(Node, Size) ->
	{ok, {{_, 200, _}, _, Body}} =
		ar_httpc:request(
			get,
			{
					"http://"
					++ ar_util:format_peer(Node)
					++ "/price/"
					++ integer_to_list(Size),
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
	 	),
	list_to_integer(Body).

%% @doc Retreive a block by height or hash from a node.
get_block(Host, Height) when is_integer(Height) ->
	%ar:report_console([{req_getting_block_by_height, Height}]),
	%ar:d([getting_new_block, {host, Host}, {height, Height}]),
	handle_block_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/block/height/"
					++ integer_to_list(Height),
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
	 	)
	);
get_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_block_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/block/hash/"
					++ ar_util:encode(Hash),
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
	 	)
	).
get_encrypted_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_encrypted_block_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/block/hash/"
					++ ar_util:encode(Hash)
					++ "/encrypted",
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
	 	)
	).
get_block_subfield(Host, Hash, Subfield) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host,[] Host}, {hash, Hash}]),
	handle_block_field_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/block/hash/"
					++ ar_util:encode(Hash)
					++ "/"
					++ Subfield,
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
	 	)
	);

get_block_subfield(Host, Height, Subfield) when is_integer(Height) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_block_field_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/block/height/"
					++ integer_to_list(Height)
					++ "/"
					++ Subfield,
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
	 	)
	).

%% @doc Retreive a full block by hash from a node.
get_full_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_full_block_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/block/hash/"
					++ ar_util:encode(Hash)
					++ "/all/",
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
		)
	).

get_encrypted_full_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_encrypted_full_block_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/block/hash/"
					++ ar_util:encode(Hash)
					++ "/all"
					++ "/encrypted",
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
		)
	).

%% @doc Retreive a tx by hash from a node.
get_tx(Host, Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_new_block, {host, Host}, {hash, Hash}]),
	handle_tx_response(
		ar_httpc:request(
			get,
			{
				"http://"
					++ ar_util:format_peer(Host)
					++ "/tx/"
					++ ar_util:encode(Hash),
				[]
			},
			[{timeout, ?NET_TIMEOUT}], []
	 	)
	).

%% @doc Retreive all pending txs from a node.
get_pending_txs(Peer) ->
	try
		begin
			{ok, {{_, 200, _}, _, Body}} =
				ar_httpc:request("http://" ++ ar_util:format_peer(Peer) ++ "/tx/pending"),
			{ok, {array, PendingTxs}} = json2:decode_string(Body),
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
	case ar_httpc:request("http://" ++ ar_util:format_peer(Peer) ++ "/info") of
		{ok, {{_, 200, _}, _, Body}} -> process_get_info(Body);
		_ -> info_unavailable
	end.

%% @doc Return a list of parsed peer IPs for a remote server.
get_peers(Peer) ->
	try
		begin
			{ok, {{_, 200, _}, _, Body}} =
				ar_httpc:request("http://" ++ ar_util:format_peer(Peer) ++ "/peers"),
			{ok, {array, PeerArray}} = json2:decode_string(Body),
			lists:map(fun ar_util:parse_peer/1, PeerArray)
		end
	catch _:_ -> []
	end.

%% @doc Produce a key value list based on a /info response.
process_get_info(Body) ->
	{ok, {struct, Struct}} = json2:decode_string(Body),
	{_, NetworkName} = lists:keyfind("network", 1, Struct),
	{_, ClientVersion} = lists:keyfind("version", 1, Struct),
	{_, Height} = lists:keyfind("height", 1, Struct),
	{_, Blocks} = lists:keyfind("blocks", 1, Struct),
	{_, Peers} = lists:keyfind("peers", 1, Struct),
	[
		{name, NetworkName},
		{version, ClientVersion},
		{height, Height},
		{blocks, Blocks},
		{peers, Peers}
	].

%% @doc Process the response of an /block call.
handle_block_response({ok, {{_, 200, _}, _, Body}}) ->
	ar_serialize:json_struct_to_block(Body);
handle_block_response({error, _}) -> unavailable;
handle_block_response({ok, {{_, 404, _}, _, _}}) -> not_found;
handle_block_response({ok, {{_, 500, _}, _, _}}) -> unavailable.

%% @doc todo
handle_full_block_response({ok, {{_, 200, _}, _, Body}}) ->
	ar_serialize:json_struct_to_full_block(Body);
handle_full_block_response({error, _}) -> unavailable;
handle_full_block_response({ok, {{_, 404, _}, _, _}}) -> not_found;
handle_full_block_response({ok, {{_, 500, _}, _, _}}) -> unavailable.

handle_encrypted_block_response({ok, {{_, 200, _}, _, Body}}) ->
	ar_util:decode(Body);
handle_encrypted_block_response({error, _}) -> unavailable;
handle_encrypted_block_response({ok, {{_, 404, _}, _, _}}) -> not_found;
handle_encrypted_block_response({ok, {{_, 500, _}, _, _}}) -> unavailable.

handle_encrypted_full_block_response({ok, {{_, 200, _}, _, Body}}) ->
	ar_util:decode(Body);
handle_encrypted_full_block_response({error, _}) -> unavailable;
handle_encrypted_full_block_response({ok, {{_, 404, _}, _, _}}) -> not_found;
handle_encrypted_full_block_response({ok, {{_, 500, _}, _, _}}) -> unavailable.

%% @doc Process the response of a /block/[{Height}|{Hash}]/{Subfield} call.
handle_block_field_response({"timestamp", {ok, {{_, 200, _}, _, Body}}}) ->
	list_to_integer(Body);
handle_block_field_response({"last_retarget", {ok, {{_, 200, _}, _, Body}}}) ->
	list_to_integer(Body);
handle_block_field_response({"diff", {ok, {{_, 200, _}, _, Body}}}) ->
	list_to_integer(Body);
handle_block_field_response({"height", {ok, {{_, 200, _}, _, Body}}}) ->
	list_to_integer(Body);
handle_block_field_response({"txs", {ok, {{_, 200, _}, _, Body}}}) ->
	ar_serialize:json_struct_to_tx(Body);
handle_block_field_response({"hash_list", {ok, {{_, 200, _}, _, Body}}}) ->
	ar_serialize:json_struct_to_hash_list(Body);
handle_block_field_response({"wallet_list", {ok, {{_, 200, _}, _, Body}}}) ->
	ar_serialize:json_struct_to_wallet_list(Body);
handle_block_field_response({_Subfield, {ok, {{_, 200, _}, _, Body}}}) -> Body;
handle_block_field_response({error, _}) -> unavailable;
handle_block_field_response({ok, {{_, 404, _}, _, _}}) -> not_found;
handle_block_field_response({ok, {{_, 500, _}, _, _}}) -> unavailable.

%% @doc Process the response of a /tx call.
handle_tx_response({ok, {{_, 200, _}, _, Body}}) ->
	ar_serialize:json_struct_to_tx(Body);
handle_tx_response({ok, {{_, 404, _}, _, _}}) -> not_found;
handle_tx_response({ok, {{_, 500, _}, _, _}}) -> not_found.

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
			bytes = P#performance.bytes + Bytes
		}
	).

%% @doc Calculate the delay before a transaction should be released to the network
%% Currently based on 40s for 95% of peers in the bitcoin network to recieve a
%% 1mb block

%%% Tests

%% @doc Tests add peer functionality
% add_peers_test() ->
% 	ar_storage:clear(),
% 	Bridge = ar_bridge:start([], []),
% 	reregister(http_bridge_node, Bridge),
% 	ar:d(add_peer({127,0,0,1,1984})),
% 	receive after 500 -> ok end,
% 	ar:d({peers, ar_bridge:get_remote_peers(Bridge)}),
% 	true = lists:member({127,0,0,1,1984}, ar_bridge:get_remote_peers(Bridge)).

block_field_to_string(<<"nonce">>, Res) -> Res;
block_field_to_string(<<"previous_block">>, Res) -> Res;
block_field_to_string(<<"timestamp">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"last_retarget">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"diff">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"height">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"hash">>, Res) -> Res;
block_field_to_string(<<"indep_hash">>, Res) -> Res;
block_field_to_string(<<"txs">>, {array, Res}) -> list_to_binary(ar_serialize:jsonify({array, Res}));
block_field_to_string(<<"hash_list">>, {array, Res}) -> list_to_binary(ar_serialize:jsonify({array, Res}));
block_field_to_string(<<"wallet_list">>, {array, Res}) -> list_to_binary(ar_serialize:jsonify({array, Res}));
block_field_to_string(<<"reward_addr">>, Res) -> Res.

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_info_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	reregister(http_bridge_node, BridgeNode),
	?NETWORK_NAME = get_info({127,0,0,1,1984}, name),
	?CLIENT_VERSION = get_info({127,0,0,1,1984}, version),
	0 = get_info({127,0,0,1,1984}, peers),
	1 = get_info({127,0,0,1,1984}, blocks),
	0 = get_info({127,0,0,1,1984}, height).

%% @doc Ensure transaction reward can be retrieved via http iface.
get_tx_reward_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	% Hand calculated result for 1000 bytes.
	ExpectedPrice = ar_tx:calculate_min_tx_cost(1000, B0#block.diff),
	ExpectedPrice = get_tx_reward({127,0,0,1,1984}, 1000).

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_unjoined_info_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([]),
	reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	reregister(http_bridge_node, BridgeNode),
	?NETWORK_NAME = get_info({127,0,0,1,1984}, name),
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
	{ok, {{_, 200, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/wallet/"
		 		++ ar_util:encode(ar_wallet:to_address(Pub1))
				++ "/balance"),
	10000 = list_to_integer(Body).

%% @doc Test that wallets issued in the pre-sale can be viewed.
get_presale_balance_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<>>}]),
	Node1 = ar_node:start([], Bs),
	reregister(Node1),
	{ok, {{_, 200, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/wallet/"
		 		++ ar_util:encode_base64_safe(base64:encode_to_string(ar_wallet:to_address(Pub1)))
				++ "/balance"),
	10000 = list_to_integer(Body).

%% @doc Test that last tx associated with a wallet can be fetched.
get_last_tx_single_test() ->
	ar_storage:clear(),
	{_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), 10000, <<"TEST_ID">>}]),
	Node1 = ar_node:start([], Bs),
	reregister(Node1),
	{ok, {{_, 200, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/wallet/"
		 		++ ar_util:encode(ar_wallet:to_address(Pub1))
				++ "/last_tx"),
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
get_full_block_by_hash_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	receive after 200 -> ok end,
	send_new_tx({127, 0, 0, 1}, TX = ar_tx:new(<<"DATA1">>)),
	receive after 200 -> ok end,
	send_new_tx({127, 0, 0, 1}, TX1 = ar_tx:new(<<"DATA2">>)),
	receive after 200 -> ok end,
	ar_node:mine(Node),
	receive after 200 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	B2 = get_block({127, 0, 0, 1}, B1),
	B3 = get_full_block({127, 0, 0, 1}, B1),
	B3 = B2#block {txs = [TX, TX1]}.

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
	{ok, {{_, 200, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/wallet/"
		 		++ ar_util:encode(ar_wallet:to_address(Pub1))
				++ "/last_tx"),
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
	{ok, {{_, 200, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/tx/"
		 		++ ar_util:encode(TX#tx.id)
				++ "/data"),
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
	{ok, {{_, 202, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/tx/"
				++ ar_util:encode(TX#tx.id)),
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
	{ok, {{_, 202, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/tx/"
				++ ar_util:encode(TX#tx.id)
				++ "/data"),
	Body == "Pending".

%% @doc Find all pending transactions in the network
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
	{ok, {{_, 200, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/tx/"
				++ "pending"),
	{ok, {array, PendingTxs}} = json2:decode_string(Body),
	[TX1#tx.id, TX2#tx.id] == [list_to_binary(P) || P <- PendingTxs].

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
	%Query = ar_serialize:json_struct_to_query(QueryJSON),
	{ok, {_, _, Stuff}} = ar_httpc:request(
		post,
		{
			"http://127.0.0.1:1984" ++ "/arql",
			[],
			"application/x-www-form-urlencoded",
			lists:flatten(
				QueryJSON
			)
		}, [{timeout, ?NET_TIMEOUT}], []
	),
	{ok, {array, TXs}} = ar_serialize:dejsonify(Stuff),
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
	{ok, {_, _, Res}} = ar_httpc:request(
		post,
		{
			"http://127.0.0.1:1984" ++ "/arql",
			[],
			"application/x-www-form-urlencoded",
			lists:flatten(
				QueryJSON
			)
		}, [{timeout, ?NET_TIMEOUT}], []
	),
	{ok, {array, TXs}} = ar_serialize:dejsonify(Res),
	true =
		lists:member(
			TX#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		),
	true =
		lists:member(
			TX2#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		).

get_encrypted_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	receive after 200 -> ok end,
	Enc0 = get_encrypted_block({127, 0, 0, 1}, B0#block.indep_hash),
	ar_storage:write_encrypted_block(B0#block.indep_hash, Enc0),
	ar_cleanup:remove_invalid_blocks([]),
	send_new_block(
		{127,0,0,1},
		B0,
		B0
	),
	B0 = ar_node:get_current_block(whereis(http_entrypoint_node)),
	ar_node:mine(Node1).

get_encrypted_full_block_test() ->
	ar_storage:clear(),
    B0 = ar_weave:init([]),
    ar_storage:write_block(B0),
	TX = ar_tx:new(<<"DATA1">>),
	TX1 = ar_tx:new(<<"DATA2">>),
	ar_storage:write_tx([TX, TX1]),
	Node = ar_node:start([], B0),
	reregister(Node),
	ar_node:mine(Node),
	receive after 500 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	Enc0 = get_encrypted_full_block({127, 0, 0, 1}, (hd(B0))#block.indep_hash),
	ar_storage:write_encrypted_block((hd(B0))#block.indep_hash, Enc0),
	ar_cleanup:remove_invalid_blocks([B1]),
	send_new_block(
		{127,0,0,1},
		hd(B0),
		hd(B0)
	),
	receive after 1000 -> ok end,
	ar_node:mine(Node).
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