-module(ar_http_iface_server).
-export([start/0, start/1, start/2, start/3, start/4, start/5, handle/2, handle_event/3]).
-export([reregister/1, reregister/2]).
-include("ar.hrl").
-include_lib("lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Handles requests from other AR nodes on the network. 

%% @doc Start the Arweave HTTP API and returns a process ID.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) ->
	spawn(
		fun() ->
			Config = [{mods, [{ar_blacklist, []},{ar_metrics, []},{?MODULE, []}]}],
			{ok, PID} =
				elli:start_link(
					[
						{callback, elli_middleware}, {callback_args, Config},
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
%% Available endpoints (more information can be found at the respective functions)
%% GET /info
%% GET /tx/pending
%% GET /tx/{id}
%% GET /tx/{id}/data.html
%% GET /tx/{id}/{field}
%% POST /tx
%% GET /block/hash/{indep_hash}
%% GET /block/hash/{indep_hash}/{field}
%% GET /block/hash/{indep_hash}/encrypted
%% GET /block/hash/{indep_hash}/all
%% GET /block/hash/{indep_hash}/all/encrypted
%% GET /block/hash/{indep_hash}/all
%% GET /block/height/{height}
%% GET /block/height/{height}/{field}
%% GET /block/current
%% GET /current_block
%% POST /block
%% GET /services
%% POST /services
%% GET /peers
%% POST /peers
%% POST /peers/port/{port}
%% GET /hash_list
%% GET /wallet_list
%% GET /wallet/{address}/balance
%% GET /wallet/{address}/last_tx
%% GET /price/{bytes}
%% GET /price/{bytes}/{addr}
%%
%% NB: Blocks and transactions are transmitted between HTTP nodes in a JSON encoded format.
%% To find the specifics regarding this look at ar_serialize module.

handle(Req, _Args) ->
	% inform ar_bridge about new peer, performance rec will be 
	% updated from ar_metrics (this is leftover from update_performance_list)
	case ar_meta_db:get({peer, ar_util:parse_peer(elli_request:peer(Req))}) of
		not_found ->
			ar_bridge:add_remote_peer(
						whereis(http_bridge_node), 
						ar_util:parse_peer(elli_request:peer(Req))
			);
		X -> X
	end,
	handle(Req#req.method, elli_request:path(Req), Req).

%% @doc Return network information from a given node.
%% GET request to endpoint /info
handle('GET', [], _Req) ->
	return_info();
handle('GET', [<<"info">>], _Req) ->
	return_info();

%% @doc Return all transactions from node that are waiting to be mined into a block.
%% GET request to endpoint /tx/pending
handle('GET', [<<"tx">>, <<"pending">>], _Req) ->
	{200, [],
	ar_serialize:jsonify(
			%% Should encode  %% iau: ???
			lists:map(
				fun ar_util:encode/1,
				ar_node:get_pending_txs(whereis(http_entrypoint_node))
			)
		)
	};

%% @doc Return a transaction specified via the the transaction id (hash)
%% GET request to endpoint /tx/{hash}
handle('GET', [<<"tx">>, Hash], _Req) ->
	ID = ar_util:decode(Hash),
	F = ar_storage:lookup_tx_filename(ID),
	case F of
		unavailable ->
			case lists:member(
					ID,
					ar_node:get_pending_txs(whereis(http_entrypoint_node))) 
				of
					true ->
						{202, [], <<"Pending">>};
					false ->
						case ar_tx_db:get(ID) of
							not_found ->
								{404, [], <<"Not Found.">>};
							Err ->
								{410, [], list_to_binary(Err)}
						end
			end;
		Filename -> {ok, [], {file, Filename}}
	end;

%% @doc Return the transaction IDs of all txs where the tags in post match
%% the given set of key value pairs.
%% POST request to endpoint /arql with body of request being a logical expression
%% valid in ar_parser.
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

%% @doc Return the data field of the transaction specified via the transaction
%% ID (hash) served as HTML.
%% GET request to endpoint /tx/{hash}/data.html
handle('GET', [<<"tx">>, Hash, <<"data.html">>], _Req) ->
	ID = ar_util:decode(Hash),
	F = ar_storage:lookup_tx_filename(ID),
	case F of
		unavailable ->
			{404, [], {file, "data/not_found.html"}};
		Filename ->
			T = ar_storage:do_read_tx(Filename),
			{200, [], T#tx.data}
	end;

%% @doc Share a new block to a peer.
%% POST request to endpoint /block with the body of the request being a JSON
%% encoded block as specified in ar_serialize.
handle('POST', [<<"block">>], Req) ->
	BlockJSON = elli_request:body(Req),
	{Struct} = ar_serialize:dejsonify(BlockJSON),
	JSONB = val_for_key(<<"new_block">>, Struct),
	BShadow = ar_serialize:json_struct_to_block(JSONB),
	case verify_all_the_things(BShadow, [id_ignored, timestamp, difficulty]) of
		{error, Reply} ->
			Reply;
		ok ->
			Port = val_for_key(<<"port">>, Struct),
			OrigPeer = make_peer_address(elli_request:peer(Req), Port),
			regossip_block_if_pow_valid(BShadow, Struct, OrigPeer)
	end;

%% @doc Share a new transaction with a peer.
%% POST request to endpoint /tx with the body of the request being a JSON encoded tx as specified in ar_serialize.
handle('POST', [<<"tx">>], Req) ->
	TXJSON = elli_request:body(Req),
	TX = ar_serialize:json_struct_to_tx(TXJSON),
	% Check whether the TX is already ignored, ignore it if it is not
	% (and then pass to processing steps).
	case ar_bridge:is_id_ignored(TX#tx.id) of
		undefined -> {429, <<"Too Many Requests">>};
		true -> {208, <<"Transaction already processed.">>};
		false ->
			ar_bridge:ignore_id(TX#tx.id),
			case ar_node:get_current_diff(whereis(http_entrypoint_node)) of
				unavailable -> {503, [], <<"Transaction verification failed.">>};
				Diff ->
					% Validate that the waiting TXs in the pool for a wallet do not exceed the balance.
					FloatingWalletList = ar_node:get_wallet_list(whereis(http_entrypoint_node)),
					WaitingTXs = ar_node:get_all_known_txs(whereis(http_entrypoint_node)),
					OwnerAddr = ar_wallet:to_address(TX#tx.owner),
					WinstonInQueue =
						lists:sum(
							[
								T#tx.quantity + T#tx.reward
							||
								T <- WaitingTXs, OwnerAddr == ar_wallet:to_address(T#tx.owner)
							]
						),
					case [ Balance || {Addr, Balance, _} <- FloatingWalletList, OwnerAddr == Addr ] of
						[B|_] when ((WinstonInQueue + TX#tx.reward + TX#tx.quantity) > B) ->
							ar:report(
								[
									{offered_txs_for_wallet_exceed_balance, ar_util:encode(OwnerAddr)},
									{balance, B},
									{in_queue, WinstonInQueue},
									{cost, TX#tx.reward + TX#tx.quantity}
								]
							),
							{400, [], <<"Waiting TXs exceed balance for wallet.">>};
						_ ->
							% Finally, validate the veracity of the TX.
							case ar_tx:verify(TX, Diff, FloatingWalletList) of
								false ->
									%ar:d({rejected_tx , ar_util:encode(TX#tx.id)}),
									{400, [], <<"Transaction verification failed.">>};
								true ->
									%ar:d({accepted_tx , ar_util:encode(TX#tx.id)}),
									ar_bridge:add_tx(whereis(http_bridge_node), TX),%, OrigPeer),
									{200, [], <<"OK">>}
							end
					end
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

%% @doc Return the estimated reward cost of transactions with a data body size of 'bytes'.
%% GET request to endpoint /price/{bytes}/{address}
%% TODO: Change so current block does not need to be pulled to calculate cost
handle('GET', [<<"price">>, SizeInBytes, Addr], _Req) ->
	{200, [],
		integer_to_binary(
			ar_tx:calculate_min_tx_cost(
				list_to_integer(
					binary_to_list(SizeInBytes)
				),
				ar_node:get_current_diff(whereis(http_entrypoint_node)),
				ar_node:get_wallet_list(whereis(http_entrypoint_node)),
				ar_util:decode(Addr)
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
			case (NetworkName == <<?NETWORK_NAME>>) of
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
			case (NetworkName == <<?NETWORK_NAME>>) of
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

%% iau: GET /block/hash/<#>/encrypted is entirely commented out

%% @doc Return the encrypted blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/encrypted
%handle('GET', [<<"block">>, <<"hash">>, Hash, <<"encrypted">>], _Req) ->
	%ar:d({resp_block_hash, Hash}),
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	%case ar_key_db:get(ar_util:decode(Hash)) of
	% 	[{Key, Nonce}] ->
	% 		return_encrypted_block(
	% 			ar_node:get_block(
	% 				whereis(http_entrypoint_node),
	% 				ar_util:decode(Hash)
	% 			),
	% 			Key,
	% 			Nonce
	% 		);
	% 	not_found ->
	% 		ar:d(not_found_block),
	% 		return_encrypted_block(unavailable)
	% end;

%% @doc Return the blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}
handle('GET', [<<"block">>, <<"hash">>, Hash], _Req) ->
	case ar_storage:lookup_block_filename(ar_util:decode(Hash)) of
		unavailable ->
			{404, [], <<"Block not found.">>};
		Filename  ->
			{ok, [], {file, Filename}}
	end;

%% @doc Return the block at the given height.
%% GET request to endpoint /block/height/{height}
%% TODO: Add handle for negative block numbers
handle('GET', [<<"block">>, <<"height">>, Height], _Req) ->
	F = ar_storage:lookup_block_filename(list_to_integer(binary_to_list(Height))),
	case F of
		unavailable ->
			{404, [], <<"Block not found.">>};
		Filename  ->
			{ok, [], {file, Filename}}
	end;

%% @doc Return a given field of the blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/{field}
%%
%% {field} := { nonce | previous_block | timestamp | last_retarget | diff | height | hash | indep_hash
%% 				txs | hash_list | wallet_list | reward_addr | tags | reward_pool }
%%
handle('GET', [<<"block">>, <<"hash">>, Hash, Field], _Req) ->
	case ar_meta_db:get(subfield_queries) of
		true ->
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
		_ ->
			{421, [], <<"Subfield block querying is disabled on this node.">>}
	end;

%% @doc Return a given field for the the blockshadow corresponding to the block height, 'height'.
%% GET request to endpoint /block/hash/{height}/{field}
%%
%% {field} := { nonce | previous_block | timestamp | last_retarget | diff | height | hash | indep_hash
%% 				txs | hash_list | wallet_list | reward_addr | tags | reward_pool }
%%
handle('GET', [<<"block">>, <<"height">>, Height, Field], _Req) ->
	case ar_meta_db:get(subfield_queries) of
		true ->
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
		_ -> 
			{421, [], <<"Subfield block querying is disabled on this node.">>}
	end;

%% @doc Return the current block.
%% GET request to endpoint /current_block
%% GET request to endpoint /block/current
handle('GET', [<<"block">>, <<"current">>], _Req) ->
	case length(ar_node:get_hash_list(whereis(http_entrypoint_node))) of
		0 -> {404, [], <<"Block not found.">>};
		Height ->
			F = ar_storage:lookup_block_filename(Height - 1),
			case F of
				unavailable -> {404, [], <<"Block not found.">>};
				Filename  ->
					{ok, [], {file, Filename}}
			end
	end;

%% DEPRECATED (12/07/2018)
handle('GET', [<<"current_block">>], _Req) ->
	case length(ar_node:get_hash_list(whereis(http_entrypoint_node))) of
		0 -> {404, [], <<"Block not found.">>};
		Height ->
			F = ar_storage:lookup_block_filename(Height - 1),
			case F of
				unavailable -> {404, [], <<"Block not found.">>};
				Filename  ->
					{ok, [], {file, Filename}}
			end
	end;

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
	Id=ar_util:decode(Hash),
	F=ar_storage:lookup_tx_filename(Id),
	case F of
		unavailable ->
			case lists:member(Id, ar_node:get_pending_txs(whereis(http_entrypoint_node))) of
				true ->
					{202, [], <<"Pending">>};
				false ->
					{404, [], <<"Not Found.">>}
			end;
		Filename ->
			{ok, JSONBlock} = file:read_file(Filename),
			{TXJSON} = ar_serialize:dejsonify(JSONBlock),
			{_, Res} = lists:keyfind(Field, 1, TXJSON),
			{200, [], Res}
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
%% Returns error code 400 - Request type not found.
handle(_, _, _) ->
	{400, [], <<"Request type not found.">>}.

%% @doc Handles all other elli metadata events.
handle_event(elli_startup, Args, Config) -> ok;
handle_event(Type, Args, Config)
		when (Type == request_throw)
		or (Type == request_error)
		or (Type == request_exit) ->
	ar:report([{elli_event, Type}, {args, Args}, {config, Config}]),
	%ar:report_console([{elli_event, Type}, {args, Args}, {config, Config}]),
	ok;
%% Uncomment to show unhandeled message types.
handle_event(Type, Args, Config) ->
	%ar:report_console([{elli_event, Type}, {args, Args}, {config, Config}]),
	ok.

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
	HL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	{200, [],
		ar_serialize:jsonify(
			{
				[
					{network, list_to_binary(?NETWORK_NAME)},
					{version, ?CLIENT_VERSION},
					{release, ?RELEASE_NUMBER},
					{height,
						case HL of
							[] -> 0;
							Hashes -> (length(Hashes) - 1)
						end
					},
					{current, case HL of [] -> <<"not_joined">>; [C|_] -> ar_util:encode(C) end},
					{blocks, ar_storage:blocks_on_disk()},
					{peers, length(ar_bridge:get_remote_peers(whereis(http_bridge_node)))},
					{queue_length,
						element(
							2,
							erlang:process_info(whereis(http_entrypoint_node), message_queue_len)
						)
					}
				]
			}
		)
	}.

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

%%% private functions

%% @doc Convert a blocks field with the given label into a string.
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

%% @doc Convenience function for lists:keyfind(Key, 1, List).
%% returns Value not {Key, Value}.
val_for_key(K, L) ->
	{K, V} = lists:keyfind(K, 1, L),
	V.

%% @doc Converts peer string and port number to parsed address.
make_peer_address(Peer, Port) ->
	ar_util:parse_peer(bitstring_to_list(Peer) ++ ":" ++ integer_to_list(Port)).

%% @doc Validates the difficulty of an incoming block.
new_block_difficulty_ok(B) ->
	B#block.diff =:= ar_node:get_current_diff(whereis(http_entrypoint_node)).

%% @doc Forwards the block to this node's peers.
%% This is the processing content of POST /block.
regossip_block_if_pow_valid(BShadow, Struct, OrigPeer) ->
	JSONRecallB = val_for_key(<<"recall_block">>, Struct),
	RecallSize = val_for_key(<<"recall_size">>, Struct),
	KeyEnc = val_for_key(<<"key">>, Struct),
	NonceEnc = val_for_key(<<"nonce">>, Struct),
	Key = ar_util:decode(KeyEnc),
	Nonce = ar_util:decode(NonceEnc),
	B = ar_block:generate_block_from_shadow(BShadow,RecallSize),
	RecallHash = ar_util:decode(JSONRecallB),
	RecallB = ar_block:get_recall_block(OrigPeer, RecallHash, B, Key, Nonce),
	case verify_one_thing(B, {work, Nonce, RecallB}) of
		{error, Message} -> Message;
		ok ->
			ar:report([{
						sending_external_block_to_bridge,
						ar_util:encode(BShadow#block.indep_hash)
			}]),
			ar_bridge:ignore_id(BShadow#block.indep_hash),
			ar_bridge:add_block(whereis(http_bridge_node), OrigPeer, B,	RecallB, Key, Nonce),
			{200, [], <<"OK">>}
	end.

verify_all_the_things(_, []) ->
	ok;
verify_all_the_things(X, [H|T]) ->
	case verify_one_thing(X, H) of
		{error, Reply} -> {error, Reply};
		ok             -> verify_all_the_things(X, T)
	end.

verify_one_thing(B, {work, Nonce, RecallB}) ->
	Difficulty = B#block.diff,
	RewardAddr = B#block.reward_addr,
	Tags = B#block.tags,
	Time = B#block.timestamp,
	TXs = B#block.txs,
	DataSegment = ar_block:generate_block_data_segment(B, RecallB, TXs, 
													   RewardAddr, Time, Tags),
	case ar_mine:validate(DataSegment, Nonce, Difficulty) of
		false -> {error, {404, [], <<"Invalid Block Work">>}};
		_     -> ok
	end;
verify_one_thing(BShadow, id_ignored) ->
	case ar_bridge:is_id_ignored(BShadow#block.indep_hash) of
		undefined -> {error, {429, <<"Too Many Requests">>}};
		true      -> {error, {208, <<"Block already processed.">>}};
		false     -> ok
	end;
verify_one_thing(BShadow, timestamp) ->
	case ar_block:verify_timestamp(BShadow) of
		false -> {error, {404, [], <<"Invalid Block">>}};
		true  -> ok
	end;
verify_one_thing(BShadow, difficulty) ->
	case new_block_difficulty_ok(BShadow) of
		false -> {error, {404, [], <<"Invalid Block Difficulty">>}};
		true  -> ok
	end;
verify_one_thing(_, _) ->
	{error, {500, <<"">>}}.

