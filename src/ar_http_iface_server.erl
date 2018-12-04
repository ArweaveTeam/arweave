%%%
%%% @doc Handle http requests.
%%%

-module(ar_http_iface_server).

-export([start/0, start/1, start/2, start/3, start/4, start/5, handle/2, handle_event/3]).
-export([reregister/1, reregister/2]).

-include("ar.hrl").
-include_lib("lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").

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
	Peer = ar_util:parse_peer(elli_request:peer(Req)),
	case ar_meta_db:get(http_logging) of
		true ->
			ar:report(
				[
					http_request,
					{method, Req#req.method},
					{path, elli_request:path(Req)},
					{peer, Peer}
				]
			);
		_ ->
			do_nothing
	end,
	case ar_meta_db:get({peer, Peer}) of
		not_found ->
			ar_bridge:add_remote_peer(whereis(http_bridge_node), Peer);
		_ -> do_nothing
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

%% @doc Some load balancers use 'HEAD's rather than 'GET's to tell if a node
%% is alive. Appease them.
handle('HEAD', [], _Req) ->
	{200, [], <<>>};
handle('HEAD', [<<"info">>], _Req) ->
	{200, [], <<>>};

%% @doc Return permissive CORS headers for all endpoints
handle('OPTIONS', [<<"block">>], _) ->
	{200, [{<<"Access-Control-Allow-Methods">>, <<"GET, POST">>}], <<"OK">>};
handle('OPTIONS', [<<"tx">>], _) ->
	{200, [{<<"Access-Control-Allow-Methods">>, <<"GET, POST">>}], <<"OK">>};
handle('OPTIONS', [<<"peer">>|_], _) ->
	{200, [{<<"Access-Control-Allow-Methods">>, <<"GET, POST">>}], <<"OK">>};
handle('OPTIONS', _, _Req) ->
	{200, [{<<"Access-Control-Allow-Methods">>, <<"GET">>}], <<"OK">>};

%% @doc Return the current universal time in seconds.
handle('GET', [<<"time">>], _Req) ->
	{200, [], integer_to_binary(os:system_time(second))};

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
			{
				200,
				[
					{
						<<"Content-Type">>,
						proplists:get_value(<<"Content-Type">>, T#tx.tags, "text/html")
					}
				],
				T#tx.data
			}
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
					Port = binary_to_integer(RawPort),
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
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, [], <<"Invalid address.">>};
		{ok, AddrOK} ->
			case ar_node:get_balance(whereis(http_entrypoint_node), AddrOK) of
				node_unavailable ->
					{503, [], <<"Temporary timeout.">>};
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
	%ar:d({resp_block_hash, Hash}),
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	%case ar_key_db:get(ar_util:decode(Hash)) of
	%	[{Key, Nonce}] ->
	%		return_encrypted_block(
	%			ar_node:get_block(
	%				whereis(http_entrypoint_node),
	%				ar_util:decode(Hash)
	%			),
	%			Key,
	%			Nonce
	%		);
	%	not_found ->
	%		ar:d(not_found_block),
	%		return_encrypted_block(unavailable)
	% end;

%% @doc Return the blockshadow corresponding to the indep_hash / height.
%% GET request to endpoint /block/{height|hash}/{indep_hash|height}
handle('GET', [<<"block">>, Type, ID], Req) ->
	Filename =
		case Type of
			<<"hash">> ->
				case hash_to_maybe_filename(block, ID) of
					{error, invalid}        -> invalid_hash;
					{error, _, unavailable} -> unavailable;
					{ok, Fn}                -> Fn
				end;
			<<"height">> ->
				try binary_to_integer(ID) of
					Int ->
						ar_storage:lookup_block_filename(Int)
				catch _:_ ->
					invalid_height
				end
		end,
	case Filename of
		invalid_hash ->
			{400, [], <<"Invalid height.">>};
		invalid_height ->
			{400, [], <<"Invalid hash.">>};
		unavailable ->
			{404, [], <<"Block not found.">>};
		_  ->
			case {ar_meta_db:get(api_compat), elli_request:get_header(<<"X-Block-Format">>, Req, <<"1">>)} of
				{false, <<"1">>} ->
					{426, [], <<"Client version incompatible.">>};
				{_, <<"1">>} ->
					% Supprt for legacy nodes (pre-1.5).
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
					end;
				{_, _} ->
					{ok, [], {file, Filename}}
			end
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
			case Field of
				<<"tags">> ->
					TX = ar_storage:do_read_tx(Filename),
					{200, [], ar_serialize:jsonify(
						lists:map(
							fun({Name, Value}) ->
								{
									[
										{name, ar_util:encode(Name)},
										{value, ar_util:encode(Value)}
									]
								}
							end,
							TX#tx.tags
						)
					)};
				_ ->
					{ok, JSONBlock} = file:read_file(Filename),
					{TXJSON} = ar_serialize:dejsonify(JSONBlock),
					Res = val_for_key(Field, TXJSON),
					{200, [], Res}
			end
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

%% @doc If we are given a hash with no specifier (block, tx, etc), assume that
%% the user is requesting the data from the TX associated with that hash.
%% Optionally allow a file extension.
handle('GET', [<< Hash:43/binary, MaybeExt/binary >>], Req) ->
	Ext =
		case MaybeExt of
			<< ".", Part/binary >> -> Part;
			<<>> -> <<"html">>
		end,
	handle('GET', [<<"tx">>, Hash, <<"data.", Ext/binary>>], Req);

%% @doc Return the current block hieght, or 500
handle(Method, [<<"height">>], _Req) when (Method == 'GET') or (Method == 'HEAD') ->
	case ar_node:get_height(whereis(http_entrypoint_node)) of
		-1 -> {503, [], <<"Node has not joined the network yet.">>};
		H -> {200, [], integer_to_binary(H)}
	end;

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

%%% Private functions

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

%% @doc checks if hash is valid & if so returns filename.
hash_to_maybe_filename(Type, Hash) ->
	case safe_decode(Hash) of
		{error, invalid} ->
			{error, invalid};
		{ok, ID} ->
			{Mod, Fun} = type_to_mf({Type, lookup_filename}),
			F = apply(Mod, Fun, [ID]),
			case F of
				unavailable ->
					{error, ID, unavailable};
				Filename ->
					{ok, Filename}
			end
	end.

%% @doc Return true if ID is a pending tx.
is_a_pending_tx(ID) ->
	lists:member(ID, ar_node:get_pending_txs(whereis(http_entrypoint_node))).

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

%% @doc Generate and return an informative JSON object regarding
%% the state of the node.
return_info() ->
	{Time, Current} =
		timer:tc(fun() -> ar_node:get_current_block_hash(whereis(http_entrypoint_node)) end),
	{Time2, Height} =
		timer:tc(fun() -> ar_node:get_height(whereis(http_entrypoint_node)) end),
	{200, [],
		ar_serialize:jsonify(
			{
				[
					{network, list_to_binary(?NETWORK_NAME)},
					{version, ?CLIENT_VERSION},
					{release, ?RELEASE_NUMBER},
					{height,
						case Height of
							not_joined -> -1;
							H -> H
						end
					},
					{current,
						case is_atom(Current) of
							true -> atom_to_binary(Current, utf8);
							false -> ar_util:encode(Current)
						end
					},
					{blocks, ar_storage:blocks_on_disk()},
					{peers, length(ar_bridge:get_remote_peers(whereis(http_bridge_node)))},
					{queue_length,
						element(
							2,
							erlang:process_info(whereis(http_entrypoint_node), message_queue_len)
						)
					},
					{node_state_latency, (Time + Time2) div 2}
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
	case ar_bridge:is_id_ignored(BShadow#block.indep_hash) of
		undefined ->
			{429, <<"Too many requests.">>};
		true ->
			{208, <<"Block already processed.">>};
		false ->
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
	case ar_block:verify_timestamp(os:system_time(seconds), BShadow) of
		false ->
			{404, [], <<"Invalid block.">>};
		true ->
			post_block(post_block, {ReqStruct, BShadow, OrigPeer})
	end;
post_block(post_block, {ReqStruct, BShadow, OrigPeer}) ->
	% Everything fine, post block.
	spawn(
		fun() ->
			JSONRecallB = val_for_key(<<"recall_block">>, ReqStruct),
			RecallSize = val_for_key(<<"recall_size">>, ReqStruct),
			KeyEnc = val_for_key(<<"key">>, ReqStruct),
			NonceEnc = val_for_key(<<"nonce">>, ReqStruct),
			Key = ar_util:decode(KeyEnc),
			Nonce = ar_util:decode(NonceEnc),
			CurrentB = ar_node:get_current_block(whereis(http_entrypoint_node)),
			B = ar_block:generate_block_from_shadow(BShadow, RecallSize),
			RecallIndepHash = ar_util:decode(JSONRecallB),
			case (not is_atom(CurrentB)) andalso
				(B#block.height > CurrentB#block.height) andalso
				(B#block.height =< (CurrentB#block.height + 50)) andalso
				(B#block.diff >= ?MIN_DIFF) of
				true ->
					ar:report(
						[
							{sending_external_block_to_bridge, ar_util:encode(BShadow#block.indep_hash)}
						]
					),
					ar_bridge:add_block(whereis(http_bridge_node), OrigPeer, B, {RecallIndepHash, Key, Nonce});
				_ ->
					ok
			end
		end
	),
	{200, [], <<"OK">>}.

%% @doc Return the block hash list associated with a block.
process_request(get_block, [Type, ID, <<"hash_list">>]) ->
	CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	case is_block_known(Type, ID, CurrentBHL) of
		true ->
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
%% @doc Return the wallet list associated with a block (as referenced by hash
%% or height).
process_request(get_block, [Type, ID, <<"wallet_list">>]) ->
	HTTPEntryPointPid = whereis(http_entrypoint_node),
	CurrentBHL = ar_node:get_hash_list(HTTPEntryPointPid),
	case is_block_known(Type, ID, CurrentBHL) of
		false -> {404, [], <<"Block not found.">>};
		true ->
			B = find_block(Type, ID, CurrentBHL),
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
			end
	end;
%% @doc Return a given field for the the blockshadow corresponding to the block height, 'height'.
%% GET request to endpoint /block/hash/{hash|height}/{field}
%%
%% {field} := { nonce | previous_block | timestamp | last_retarget | diff | height | hash | indep_hash
%%				txs | hash_list | wallet_list | reward_addr | tags | reward_pool }
%%
process_request(get_block, [Type, ID, Field]) ->
	CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	case ar_meta_db:get(subfield_queries) of
		true ->
			case find_block(Type, ID, CurrentBHL) of
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
	end.

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

%% @doc Take a block type specifier, an ID, and a BHL, returning whether the
%% given block is part of the BHL.
is_block_known(<<"height">>, RawHeight, BHL) when is_binary(RawHeight) ->
	is_block_known(<<"height">>, binary_to_integer(RawHeight), BHL);
is_block_known(<<"height">>, Height, BHL) ->
	Height < length(BHL);
is_block_known(<<"hash">>, ID, BHL) ->
	lists:member(ID, BHL).

%% @doc Find a block, given a type and a specifier.
find_block(<<"height">>, RawHeight, BHL) ->
	ar_node:get_block(
		whereis(http_entrypoint_node),
		binary_to_integer(RawHeight),
		BHL
	);
find_block(<<"hash">>, ID, BHL) ->
	ar_storage:read_block(ID, BHL).
