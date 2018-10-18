%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface_server).

-export([start/0, start/1, start/2, start/3, start/4, start/5, handle/2, handle_event/3]).
-export([reregister/1, reregister/2]).

-include("ar.hrl").
-include_lib("lib/elli/include/elli.hrl").

%%%
%%% Public API.
%%%

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

%%%
%%% Server side functions.
%%%

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
	% Inform ar_bridge about new peer, performance rec will be updated  from ar_metrics
	% (this is leftover from update_performance_list)
	case ar_meta_db:get({peer, ar_util:parse_peer(elli_request:peer(Req))}) of
		not_found ->
			ar_bridge:add_remote_peer(whereis(http_bridge_node), ar_util:parse_peer(elli_request:peer(Req)));
		X ->
			X
	end,
	case handle(Req#req.method, elli_request:path(Req), Req) of
		{Status, Hdrs, Body} ->
			{Status, ?DEFAULT_RESPONSE_HEADERS ++ Hdrs, Body};
		{Status, Body} ->
			{Status, ?DEFAULT_RESPONSE_HEADERS, Body}
	end.

%% @doc Return network information from a given node.
%% GET request to endpoint /info
handle('GET', [], _Req) ->
	return_info();
handle('GET', [<<"info">>], _Req) ->
	return_info();

%% @doc Return permissive CORS headers for all endpoints
handle('OPTIONS', [<<"block">>], _) ->
	{200, [{<<"Access-Control-Allow-Methods">>, <<"GET, POST">>}], <<"OK">>};
handle('OPTIONS', [<<"tx">>], _) ->
	{200, [{<<"Access-Control-Allow-Methods">>, <<"GET, POST">>}], <<"OK">>};
handle('OPTIONS', [<<"peer">>|_], _) ->
	{200, [{<<"Access-Control-Allow-Methods">>, <<"GET, POST">>}], <<"OK">>};
handle('OPTIONS', _, _Req) ->
	{200, [{<<"Access-Control-Allow-Methods">>, <<"GET">>}], <<"OK">>};

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
	case hash_to_maybe_filename(tx, Hash) of
		{error, invalid} ->
			{400, [], <<"Invalid hash.">>};
		{error, ID, unavailable} ->
			case is_a_pending_tx(ID) of
				true ->
					{202, [], <<"Pending">>};
				false ->
					case ar_tx_db:get(ID) of
						not_found -> {404, [], <<"Not Found.">>};
						Err		  -> {410, [], list_to_binary(Err)}
					end
			end;
		{ok, Filename} ->
			{ok, [], {file, Filename}}
	end;

%% @doc Return the transaction IDs of all txs where the tags in post match the given set of key value pairs.
%% POST request to endpoint /arql with body of request being a logical expression valid in ar_parser.
%%
%% Example logical expression.
%%	{
%%		op:		{ and | or | equals }
%%		expr1:	{ string | logical expression }
%%		expr2:	{ string | logical expression }
%%	}
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
handle('GET', [<<"tx">>, Hash, << "data.", _/binary >>], _Req) ->
	case hash_to_maybe_filename(tx, Hash) of
		{error, invalid} ->
			{400, [], <<"Invalid hash.">>};
		{error, _, unavailable} ->
			{404, [], {file, "data/not_found.html"}};
		{ok, Filename} ->
			T = ar_storage:do_read_tx(Filename),
			{200, [], T#tx.data}
	end;

%% @doc Share a new block to a peer.
%% POST request to endpoint /block with the body of the request being a JSON encoded block
%% as specified in ar_serialize.
handle('POST', [<<"block">>], Req) ->
	post_block(request, Req);

%% @doc Share a new transaction with a peer.
%% POST request to endpoint /tx with the body of the request being a JSON encoded tx as
%% specified in ar_serialize.
handle('POST', [<<"tx">>], Req) ->
	case validate_post_tx(Req) of
		{error, Response} ->
			Response;
		{ok, TX} ->
			process_request(post_tx, TX)
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
				binary_to_integer(SizeInBytes),
				ar_node:get_current_diff(whereis(http_entrypoint_node))
			)
		)
	};

%% @doc Return the estimated reward cost of transactions with a data body size of 'bytes'.
%% GET request to endpoint /price/{bytes}/{address}
%% TODO: Change so current block does not need to be pulled to calculate cost
handle('GET', [<<"price">>, SizeInBytes, Addr], _Req) ->
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, [], <<"Invalid address.">>};
		{ok, AddrOK} ->
			{200, [],
				integer_to_binary(
					ar_tx:calculate_min_tx_cost(
						binary_to_integer(SizeInBytes),
						ar_node:get_current_diff(whereis(http_entrypoint_node)),
						ar_node:get_wallet_list(whereis(http_entrypoint_node)),
						AddrOK
					)
				)
			}
	end;

%% @doc Return the current hash list held by the node.
%% GET request to endpoint /hash_list
handle('GET', [<<"hash_list">>], _Req) ->
	HashList = ar_node:get_hash_list(whereis(http_entrypoint_node)),
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
	add_remote_peer(Req);
handle('POST', [<<"peers">>, <<"port">>, RawPort], Req) ->
	add_remote_peer(Req, RawPort);

%% @doc Return the balance of the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/balance
handle('GET', [<<"wallet">>, Addr, <<"balance">>], _Req) ->
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, [], <<"Invalid address.">>};
		{ok, AddrOK} ->
			case ar_node:get_balance(whereis(http_entrypoint_node), AddrOK) of
				node_unavailable ->
					{504, [], <<"Temporary timeout.">>};
				Balance ->
					{200, [], integer_to_binary(Balance)}
			end
	end;

%% @doc Return the last transaction ID (hash) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/last_tx
handle('GET', [<<"wallet">>, Addr, <<"last_tx">>], _Req) ->
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, [], <<"Invalid address.">>};
		{ok, AddrOK} ->
			{200, [],
				ar_util:encode(
					ar_node:get_last_tx(whereis(http_entrypoint_node), AddrOK)
				)
			}
	end;

%% @doc Return the encrypted blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/encrypted
%handle('GET', [<<"block">>, <<"hash">>, Hash, <<"encrypted">>], _Req) ->
%	ar:d({resp_block_hash, Hash}),
%	ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
%	case ar_key_db:get(ar_util:decode(Hash)) of
%		[{Key, Nonce}] ->
%			return_encrypted_block(
%				ar_node:get_block(
%					whereis(http_entrypoint_node),
%					ar_util:decode(Hash)
%				),
%				Key,
%				Nonce
%			);
%		not_found ->
%			ar:d(not_found_block),
%			return_encrypted_block(unavailable)
%	 end;
%return_encrypted_block(unavailable) -> {404, [], <<"Block not found.">>}.
%return_encrypted_block(unavailable, _, _) -> {404, [], <<"Block not found.">>};
%return_encrypted_block(B, Key, Nonce) ->
%	{200, [],
%		ar_util:encode(ar_block:encrypt_block(B, Key, Nonce))
%	}.


%% @doc Return the blockshadow corresponding to the indep_hash / height.
%% GET request to endpoint /block/{height|hash}/{indep_hash|height}
handle('GET', [<<"block">>, Type, IDBin], Req) ->
	case validate_get_block_type_id(Type, IDBin) of
		{error, Response} ->
			Response;
		{ok, ID} ->
			XVersion = elli_request:get_header(<<"X-Version">>, Req, <<"7">>),
			process_request(get_block, [Type, ID], XVersion)
	end;

%% @doc Return block or block field.
handle('GET', [<<"block">>, Type, IDBin, Field], _Req) ->
	case validate_get_block_type_id(Type, IDBin) of
		{error, Response} ->
			Response;
		{ok, ID} ->
			process_request(get_block, [Type, ID, Field])
	end;

%% @doc Return the current block.
%% GET request to endpoint /current_block
%% GET request to endpoint /block/current
handle('GET', [<<"block">>, <<"current">>], Req) ->
	case ar_node:get_hash_list(whereis(http_entrypoint_node)) of
		[] -> {404, [], <<"Block not found.">>};
		[IndepHash|_] ->
			handle('GET', [<<"block">>, <<"hash">>, ar_util:encode(IndepHash)], Req)
	end;

%% DEPRECATED (12/07/2018)
handle('GET', [<<"current_block">>], Req) ->
	handle('GET', [<<"block">>, <<"current">>], Req);

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
	case hash_to_maybe_filename(tx, Hash) of
		{error, invalid} ->
			{400, [], <<"Invalid hash.">>};
		{error, ID, unavailable} ->
			case is_a_pending_tx(ID) of
				true ->
					{202, [], <<"Pending">>};
				false ->
					{404, [], <<"Not Found.">>}
			end;
		{ok, Filename} ->
			{ok, JSONBlock} = file:read_file(Filename),
			{TXJSON} = ar_serialize:dejsonify(JSONBlock),
			Res = val_for_key(Field, TXJSON),
			{200, [], Res}
	end;

%% @doc Share the location of a given service with a peer.
%% POST request to endpoint /services where the body of the request is a JSON encoded serivce as
%% specified in ar_serialize.
handle('POST', [<<"services">>], Req) ->
	BodyBin = elli_request:body(Req),
	{ServicesJSON} = ar_serialize:jsonify(BodyBin),
	ar_services:add(
		whereis(http_service_node),
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
handle_event(elli_startup, _Args, _Config) -> ok;
handle_event(Type, Args, Config)
		when (Type == request_throw)
		or (Type == request_error)
		or (Type == request_exit) ->
	ar:report([{elli_event, Type}, {args, Args}, {config, Config}]),
	%ar:report_console([{elli_event, Type}, {args, Args}, {config, Config}]),
	ok;
%% Uncomment to show unhandeled message types.
handle_event(_Type, _Args, _Config) ->
	%ar:report_console([{elli_event, Type}, {args, Args}, {config, Config}]),
	ok.

%%%
%%% Private functions.
%%%

%% @doc Add a requesting peer if its network and time are valid.
add_remote_peer(Req) ->
	add_remote_peer(Req, ?DEFAULT_HTTP_IFACE_PORT).

add_remote_peer(Req, Port) when is_integer(Port) ->
	BlockJSON = elli_request:body(Req),
	{Struct} = ar_serialize:dejsonify(BlockJSON),
	Network = lists:keyfind(<<"network">>, 1, Struct),
	add_remote_peer(Req, Port, Network);
add_remote_peer(Req, RawPort) ->
	Port = binary_to_integer(RawPort),
	add_remote_peer(Req, Port).

add_remote_peer(Req, Port, {<<"network">>, NetworkName}) when NetworkName == <<?NETWORK_NAME>> ->
	NetPeer = elli_request:peer(Req),
	Peer = ar_util:parse_peer({NetPeer, Port}),
	case is_valid_peer_time(Peer) of
		true ->
			ar_bridge:add_remote_peer(whereis(http_bridge_node), Peer),
			{200, [], []};
		false ->
			{400, [], "Invalid peer time."}
	end;
add_remote_peer(_Req, _Port, _) ->
	{400, [], "Wrong network."}.

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

%% @doc http validator wrapper around ar_bridge:is_id_ignored/1
check_is_id_ignored(Type, ID) ->
	case ar_bridge:is_id_ignored(ID) of
		undefined ->
			{error, {429, <<"Too many requests.">>}};
		true ->
			Msg = case Type of
				block -> <<"Block already processed.">>;
				tx    -> <<"Transaction already processed.">>
			end,
			{error, {208, Msg}};
		false ->
			ok
	end.

%% @doc Returns block bits for POST /block.
get_block_bits(ReqStruct, BShadow, OrigPeer) ->
	RecallSize = val_for_key(<<"recall_size">>, ReqStruct),
	B = ar_block:generate_block_from_shadow(BShadow, RecallSize),
	LastB = ar_node:get_current_block(whereis(http_entrypoint_node)),
	JSONRecallB = val_for_key(<<"recall_block">>, ReqStruct),
	RecallHash = ar_util:decode(JSONRecallB),
	Key = ar_util:decode(val_for_key(<<"key">>, ReqStruct)),
	Nonce = ar_util:decode(val_for_key(<<"nonce">>, ReqStruct)),
	RecallB = ar_block:get_recall_block(OrigPeer, RecallHash, B, Key, Nonce),
	{B, LastB, RecallB, Key, Nonce}.

%% @doc Checks if hash is valid & if so returns filename.
hash_to_maybe_filename(Type, Hash) ->
	case safe_decode(Hash) of
		{error, invalid} ->
			{error, invalid};
		{ok, ID} ->
			id_to_filename(Type, ID)
	end.

%% @doc Returns filename for ID.
id_to_filename(Type, ID) ->
	{Mod, Fun} = type_to_mf({Type, lookup_filename}),
	F = apply(Mod, Fun, [ID]),
	case F of
		unavailable -> {error, ID, unavailable};
		Filename    -> {ok, Filename}
	end.

%% @doc Return true if ID is a pending tx.
is_a_pending_tx(ID) ->
	lists:member(ID, ar_node:get_pending_txs(whereis(http_entrypoint_node))).

%% @doc Retrieve the system time from a peer and compare to
%% local time.
is_valid_peer_time({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	% Peer has API endpoint. Check time.
	PeerT = binary_to_integer(Body),
	LocalT = os:system_time(second),
	LocalT >= (PeerT + ?NODE_TIME_DIFF_TOLERANCE);
is_valid_peer_time({ok, {{<<"400">>, _}, _, <<"Request type not found.">>, _, _}}) ->
	% Peer is not yet updated. Be tolerant.
	true;
is_valid_peer_time(Peer) ->
	try
		is_valid_peer_time(ar_httpc:request(<<"GET">>, Peer, "/time", []))
	catch
		_:_ ->
			false
	end.

%% @doc Validates the difficulty of an incoming block.
new_block_difficulty_ok(B) ->
	B#block.diff =:= ar_node:get_current_diff(whereis(http_entrypoint_node)).

%% @doc Given a request, returns a blockshadow.
request_to_struct_with_blockshadow(Req) ->
	try
		BlockJSON = elli_request:body(Req),
		{Struct} = ar_serialize:dejsonify(BlockJSON),
		JSONB = val_for_key(<<"new_block">>, Struct),
		BShadow = ar_serialize:json_struct_to_block(JSONB),
		{ok, {Struct, BShadow}}
	catch
		Exception:Reason ->
			{error, {Exception, Reason}}
	end.

%% @doc Given a request, returns a tx (or http error).
request_to_tx(Req) ->
	try
		TXJSON = elli_request:body(Req),
		TX = ar_serialize:json_struct_to_tx(TXJSON),
		{ok, TX}
	catch
		_:_ ->
			{error, {400, <<"Invalid request body.">>}}
	end.

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

%% @doc Generate and return an informative JSON object regarding
%% the state of the node.
return_info() ->
	{Time, HL} =
		timer:tc(fun() -> ar_node:get_hash_list(whereis(http_entrypoint_node)) end),
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
					},
					{node_state_latency, Time}
				]
			}
		)
	}.

%% @doc wrapper aound util:decode catching exceptions for invalid base64url encoding.
safe_decode(X) ->
	try
		D = ar_util:decode(X),
		{ok, D}
	catch
		_:_ ->
			{error, invalid}
	end.

%% @doc converts a tuple of atoms to a {Module, Function} tuple.
type_to_mf({tx, lookup_filename}) ->
	{ar_storage, lookup_tx_filename};
type_to_mf({block, lookup_filename}) ->
	{ar_storage, lookup_block_filename}.

%% @doc Converts a list to tx ids to tx records retrieved from local db.
%% If any of the txs can't be retrieved, return the atom not_found
txids_maybe_to_txs(TXIDs) ->
	MyTXIDs = [MyTX#tx.id
				|| MyTX <- ar_node:get_all_known_txs(whereis(http_entrypoint_node))],
	M = txids_maybe_to_txs(TXIDs, MyTXIDs),
	case lists:member(not_found, M) of
		true  -> not_found;
		false -> M
	end.
txids_maybe_to_txs([_|_], []) -> [not_found];
txids_maybe_to_txs([], _)     -> [];
txids_maybe_to_txs([H|T], MyTXIDs) ->
	case lists:member(H, MyTXIDs) of
		false -> [not_found];
		true  -> [ar_storage:read_tx(H) | txids_maybe_to_txs(T, MyTXIDs)]
	end.

%% @doc Convenience function for lists:keyfind(Key, 1, List).
%% returns Value not {Key, Value}.
val_for_key(K, L) ->
	{K, V} = lists:keyfind(K, 1, L),
	V.

%% @doc Handle multiple steps of POST /block. First argument is a subcommand,
%% second the argument for that subcommand.
post_block(request, Req) ->
	% Convert request to struct and block shadow.
	case request_to_struct_with_blockshadow(Req) of
		{error, {_, _}} ->
			{400, [], <<"Invalid block.">>};
		{ok, {ReqStruct, BShadow}} ->
			Port = val_for_key(<<"port">>, ReqStruct),
			Peer = bitstring_to_list(elli_request:peer(Req)) ++ ":" ++ integer_to_list(Port),
			OrigPeer = ar_util:parse_peer(Peer),
			post_block(check_is_ignored, {ReqStruct, BShadow, OrigPeer})
	end;
post_block(check_is_ignored, {ReqStruct, BShadow, OrigPeer}) ->
	% Check if block is already known.
	case check_is_id_ignored(block, BShadow#block.indep_hash) of
		{error, Response} ->
			Response;
		ok ->
			ar_bridge:ignore_id(BShadow#block.indep_hash),
			post_block(check_is_joined, {ReqStruct, BShadow, OrigPeer})
	end;
post_block(check_is_joined, {ReqStruct, BShadow, OrigPeer}) ->
	% Check if node is joined.
	case ar_node:is_joined(whereis(http_entrypoint_node)) of
		false ->
			{503, [], <<"Not joined.">>};
		true ->
			post_block(check_timestamp, {ReqStruct, BShadow, OrigPeer})
	end;
post_block(check_timestamp, {ReqStruct, BShadow, OrigPeer}) ->
	% Verify the timestamp of the block shadow.
	case ar_block:verify_timestamp(BShadow) of
		false ->
			{400, [], <<"Invalid block.">>};
		true ->
			post_block(check_difficulty, {ReqStruct, BShadow, OrigPeer})
	end;
post_block(check_difficulty, {ReqStruct, BShadow, OrigPeer}) ->
    % Verify the difficulty of the block shadow.
	case new_block_difficulty_ok(BShadow) of
		false -> {400, [], <<"Invalid Block Difficulty.">>};
		true  ->
			post_block(check_txs, {ReqStruct, BShadow, OrigPeer})
	end;
post_block(check_txs, {ReqStruct, BShadow, OrigPeer}) ->
	TXids = BShadow#block.txs,
	case txids_maybe_to_txs(TXids) of
		not_found -> {400, [], <<"Cannot verify transactions.">>};
		TXs  ->
			post_block(check_height, {ReqStruct, BShadow, OrigPeer, TXs})
	end;
post_block(check_height, {ReqStruct, BShadow, OrigPeer, TXs}) ->
	NewBHeight = BShadow#block.height,
	LastB = ar_node:get_current_block(whereis(http_entrypoint_node)),
	MyHeight = LastB#block.height,
	HDiff = NewBHeight - MyHeight,
	case {HDiff, HDiff > 1} of
		{1,_}     -> % just right
			post_block(check_pow, {ReqStruct, BShadow, OrigPeer, TXs});
		{_,false} -> % too low
			{400, [], <<"Invalid block height.">>};
		{_,true}  -> % too high
			{500, [], <<"Too high. Need fork recovery.">>}
			% iau TODO failing pending feedback
			%{B, LastB, RecallB, Key, Nonce} = get_block_bits(ReqStruct, BShadow, OrigPeer),
			%post_block(post_block, {B, LastB, RecallB, OrigPeer, Key, Nonce})
	end;
post_block(check_pow, {ReqStruct, BShadow, OrigPeer, TXs}) ->
    % Verify the pow of the block shadow.
	{B, LastB, RecallB, Key, Nonce} = get_block_bits(ReqStruct, BShadow, OrigPeer),
	DataSegment = ar_block:generate_block_data_segment(
		LastB,
		RecallB,
		TXs,
		B#block.reward_addr,
		B#block.timestamp,
		B#block.tags
	),

    case ar_mine:validate(DataSegment, B#block.nonce, B#block.diff) of
        false -> {400, [], <<"Invalid Block Work">>};
        _  -> post_block(post_block, {B, LastB, RecallB, OrigPeer, Key, Nonce})
    end;
post_block(post_block, {NewB, CurrentB, RecallB, OrigPeer, Key, Nonce}) ->
	% Everything fine, post block.
	spawn(
		fun() ->
			case (not is_atom(CurrentB)) andalso
					(NewB#block.height > CurrentB#block.height) andalso
					(NewB#block.height =< (CurrentB#block.height + 50)) andalso
					(NewB#block.diff >= ?MIN_DIFF) of
				true ->
					ar:report(
						[
							{sending_external_block_to_bridge, ar_util:encode(NewB#block.indep_hash)}
						]
					),
					ar_bridge:add_block(whereis(http_bridge_node), OrigPeer, NewB, RecallB, Key, Nonce);
				_ ->
					ok
			end
		end
	),
	{200, [], <<"OK">>}.

%% @doc Return block or not found
process_request(get_block, [<<"hash">>, ID], XVersion) ->
	case id_to_filename(block, ID) of
		{error, _, unavailable} ->
			{404, [], <<"Block not found.">>};
		{ok, Filename} ->
			get_block_by_filename(Filename, XVersion)
	end;
process_request(get_block, [<<"height">>, ID], XVersion) ->
	Filename = ar_storage:lookup_block_filename(ID),
	get_block_by_filename(Filename, XVersion).

get_block_by_filename(Filename, <<"8">>) ->
	{ok, [], {file, Filename}};
get_block_by_filename(Filename, <<"7">>) ->
	% Support for legacy nodes (pre-1.5).
	BHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	try ar_storage:do_read_block(Filename, BHL) of
		B ->
			{JSONStruct} =
				ar_serialize:block_to_json_struct(
					B#block {
						txs =
							[
								if is_binary(TX) -> TX; true -> TX#tx.id end
							||
								TX <- B#block.txs
							]
					}
				),
			{200, [],
				ar_serialize:jsonify(
					{
						[
							{
								<<"hash_list">>,
								ar_serialize:hash_list_to_json_struct(B#block.hash_list)
							}
						|
							JSONStruct
						]
					}
				)
			}
	catch error:cannot_generate_block_hash_list ->
		{404, [], <<"Requested block not found on block hash list.">>}
	end.


%% @doc Return the block hash list associated with a block.
process_request(get_block, [Type, ID, <<"hash_list">>]) ->
	CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	Hash =
		case Type of
			<<"height">> ->
				B =
					ar_node:get_block(whereis(http_entrypoint_node),
					ID,
					CurrentBHL),
				B#block.indep_hash;
			<<"hash">> -> ID
		end,
	case lists:member(Hash, CurrentBHL) of
		true ->
			BlockBHL = ar_block:generate_hash_list_for_block(Hash, CurrentBHL),
			{200, [],
				ar_serialize:jsonify(
					ar_serialize:hash_list_to_json_struct(
						BlockBHL
					)
				)
			};
		false ->
			{404, [], <<"Block not found.">>}
	end;
%% @doc Return the wallet list associated with a block.
process_request(get_block, [Type, ID, <<"wallet_list">>]) ->
	HTTPEntryPointPid = whereis(http_entrypoint_node),
	B =
		case Type of
			<<"height">> ->
				CurrentBHL = ar_node:get_hash_list(HTTPEntryPointPid),
				ar_node:get_block(
					HTTPEntryPointPid,
					ID,
					CurrentBHL);
			<<"hash">> ->
				ar_storage:read_block(ID, ar_node:get_hash_list(HTTPEntryPointPid))
		end,
	case ?IS_BLOCK(B) of
		true ->
			{200, [],
				ar_serialize:jsonify(
					ar_serialize:wallet_list_to_json_struct(
						B#block.wallet_list
					)
				)
			};
		false ->
			{404, [], <<"Block not found.">>}
	end;
%% @doc Return a given field for the the blockshadow corresponding to the block height, 'height'.
%% GET request to endpoint /block/hash/{hash|height}/{field}
%%
%% {field} := { nonce | previous_block | timestamp | last_retarget | diff | height | hash | indep_hash
%%				txs | hash_list | wallet_list | reward_addr | tags | reward_pool }
%%
process_request(get_block, [Type, ID, Field]) ->
	case ar_meta_db:get(subfield_queries) of
		true ->
			Block =
				case Type of
					<<"height">> ->
						CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
						ar_node:get_block(whereis(http_entrypoint_node),
							ID,
							CurrentBHL);
					<<"hash">> ->
						ar_storage:read_block(ID, ar_node:get_hash_list(whereis(http_entrypoint_node)))
				end,
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
process_request(post_tx, TX) ->
	ar_bridge:ignore_id(TX#tx.id),
	ar_bridge:add_tx(whereis(http_bridge_node), TX),
	{200, [], <<"OK">>}.

validate_get_block_type_id(<<"height">>, ID) ->
	try binary_to_integer(ID) of
		Int -> {ok, Int}
	catch _:_ ->
		{error, {400, [], <<"Invalid height.">>}}
	end;
validate_get_block_type_id(<<"hash">>, ID) ->
	case safe_decode(ID) of
		{ok, Hash} -> {ok, Hash};
		invalid    -> {error, {400, [], <<"Invalid hash.">>}}
	end.

validate_post_tx(Req) ->
	case request_to_tx(Req) of
		{error, Response} -> Response;
		{ok,TX} ->
			case check_is_id_ignored(tx, TX#tx.id) of
				{error, Response} -> Response;
				ok ->
					case ar_node:get_current_diff(whereis(http_entrypoint_node)) of
						unavailable -> {503, <<"Transaction verification failed.">>};
						Diff ->
							FloatingWalletList = ar_node:get_wallet_list(whereis(http_entrypoint_node)),
							case validate_txs_by_wallet(TX, FloatingWalletList) of
								{error, Response} -> Response;
								ok ->
									case ar_tx:verify(TX, Diff, FloatingWalletList) of
										false ->
											{400, <<"Transaction verification failed.">>};
										true ->
											{ok, TX}
									end
							end
					end
			end
	end.

validate_txs_by_wallet(TX, FloatingWalletList) ->
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
			ar:report([
				{offered_txs_for_wallet_exceed_balance, ar_util:encode(OwnerAddr)},
				{balance, B},
				{in_queue, WinstonInQueue},
				{cost, TX#tx.reward + TX#tx.quantity}
			]),
			{error, {400, <<"Waiting TXs exceed balance for wallet.">>}};
		_ -> ok
	end.
