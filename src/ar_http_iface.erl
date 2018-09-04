-module(ar_http_iface).
-export([start/0, start/1, start/2, start/3, start/4, start/5, handle/2, handle_event/3]).
-export([send_new_block/3, send_new_block/4, send_new_block/6, send_new_tx/2, get_block/3]).
-export([get_tx/2, get_full_block/3, get_block_subfield/3, add_peer/1]).
-export([get_encrypted_block/2, get_encrypted_full_block/2]).
-export([get_info/1, get_info/2, get_peers/1, get_pending_txs/1, has_tx/2]).
-export([get_wallet_list/2, get_hash_list/1, get_hash_list/2]).
-export([get_current_block/1]).
-export([reregister/1, reregister/2]).
-export([get_txs_by_send_recv_test_slow/0, get_full_block_by_hash_test_slow/0]).
-include("ar.hrl").
-include_lib("lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Exposes access to an internal Arweave client to external nodes on the network.

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
	%inform ar_bridge about new peer, performance rec will be updated  from ar_metrics (this is leftover from update_performance_list)
	case ar_meta_db:get({peer, ar_util:parse_peer(elli_request:peer(Req))}) of
		not_found -> ar_bridge:add_remote_peer(whereis(http_bridge_node), ar_util:parse_peer(elli_request:peer(Req)));
		X -> X
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
						Err       -> {410, [], list_to_binary(Err)}
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
	OrigPeer = ar_util:parse_peer(bitstring_to_list(elli_request:peer(Req))
		++ ":" ++ integer_to_list(Port)),
	case ar_block:verify_timestamp(os:system_time(seconds), BShadow) of
		false -> {404, [], <<"Invalid block.">>};
		true  ->
			case ar_bridge:is_id_ignored(BShadow#block.indep_hash) of
				undefined -> {429, <<"Too many requests.">>};
				true -> {208, <<"Block already processed.">>};
				false ->
					ar_bridge:ignore_id(BShadow#block.indep_hash),
					spawn(
						fun() ->
							B = ar_block:generate_block_from_shadow(BShadow,RecallSize),
							RecallHash = ar_util:decode(JSONRecallB),
							RecallB = ar_block:get_recall_block(OrigPeer,RecallHash,B,Key,Nonce),
							CurrentB = ar_node:get_current_block(whereis(http_entrypoint_node)),
							% mue: keep block distance for later tests
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
									ar_bridge:add_block(whereis(http_bridge_node), OrigPeer, B, RecallB, Key, Nonce);
								_ ->
									ok
							end
						end
					),
					{200, [], <<"OK">>}
			end
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
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, [], <<"Invalid address.">>};
		{ok, AddrOK} ->
			{200, [],
				integer_to_binary(
					ar_node:get_balance(whereis(http_entrypoint_node), AddrOK)
				)
			}
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
				ar_storage:lookup_block_filename(ar_util:decode(ID));
			<<"height">> ->
				ar_storage:lookup_block_filename(binary_to_integer(ID))
		end,
	case Filename of
		unavailable ->
			{404, [], <<"Block not found.">>};
		Filename  ->
			case elli_request:get_header(<<"X-Version">>, Req, <<"7">>) of
				<<"7">> ->
					B =
						ar_storage:do_read_block(
							Filename,
							ar_node:get_hash_list(whereis(http_entrypoint_node))
						),
					{JSONStruct} = ar_serialize:full_block_to_json_struct(B),
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
					};
				<<"8">> ->
					{ok, [], {file, Filename}}
			end
	end;

%% @doc Return the block hash list associated with a block.
handle('GET', [<<"block">>, Type, ID, <<"hash_list">>], _Req) ->
	CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	Hash =
		case Type of
			<<"height">> ->
				B =
					ar_node:get_block(whereis(http_entrypoint_node),
						list_to_integer(binary_to_list(ID)),
						CurrentBHL),
				B#block.indep_hash;
			<<"hash">> -> ar_util:decode(ID)
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
handle('GET', [<<"block">>, Type, ID, <<"wallet_list">>], _Req) ->
    HTTPEntryPointPid = whereis(http_entrypoint_node),
	B =
		case Type of
			<<"height">> ->
				CurrentBHL = ar_node:get_hash_list(HTTPEntryPointPid),
				ar_node:get_block(
                    HTTPEntryPointPid,
					list_to_integer(binary_to_list(ID)),
					CurrentBHL);
			<<"hash">> ->
                ar_storage:read_block(ar_util:decode(ID), ar_node:get_hash_list(HTTPEntryPointPid))
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
handle('GET', [<<"block">>, Type, ID, Field], _Req) ->
	case ar_meta_db:get(subfield_queries) of
		true ->
			Block =
				case Type of
					<<"height">> ->
						CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
						ar_node:get_block(whereis(http_entrypoint_node),
							list_to_integer(binary_to_list(ID)),
							CurrentBHL);
					<<"hash">> ->
						ar_storage:read_block(ar_util:decode(ID), ar_node:get_hash_list(whereis(http_entrypoint_node)))
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

%%% Client functions

%% @doc Send a new transaction to an Arweave HTTP node.
send_new_tx(Peer, TX) ->
	if
		byte_size(TX#tx.data < 50000) ->
			do_send_new_tx(Peer, TX);
		true ->
			case has_tx(Peer, TX#tx.id) of
				false -> do_send_new_tx(Peer, TX);
				true -> not_sent
			end
	end.

do_send_new_tx(Peer, TX) ->
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/tx",
		ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
	).

%% @doc Check whether a peer has a given transaction
has_tx(Peer, ID) ->
	case
		ar_httpc:request(
				<<"GET">>,
				Peer,
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
send_new_block(Peer, Port, NewB, RecallB) ->
	%ar:report_console([{sending_new_block, NewB#block.height}, {stack, erlang:get_stacktrace()}]),
	NewBShadow = NewB#block { wallet_list = [], hash_list = lists:sublist(NewB#block.hash_list,1,?STORE_BLOCKS_BEHIND_CURRENT)},
	RecallBHash =
		case ?IS_BLOCK(RecallB) of
			true ->  RecallB#block.indep_hash;
			false -> <<>>
		end,
	{TempJSONStruct} = ar_serialize:block_to_json_struct(NewBShadow),
	JSONStruct =
		{
			[{<<"hash_list">>, NewBShadow#block.hash_list }|TempJSONStruct]
		},
	case ar_key_db:get(RecallBHash) of
		[{Key, Nonce}] ->
			ar_httpc:request(
				<<"POST">>,
				Peer,
				"/block",
				ar_serialize:jsonify(
					{
						[
							{<<"new_block">>, JSONStruct},
							{<<"recall_block">>, ar_util:encode(RecallBHash)},
							{<<"recall_size">>, RecallB#block.block_size},
							{<<"port">>, Port},
							{<<"key">>, ar_util:encode(Key)},
							{<<"nonce">>, ar_util:encode(Nonce)}
						]
					}
				)

			);
		% TODO: Clean up function, duplication of code unneccessary.
		_ ->
			ar_httpc:request(
			<<"POST">>,
			Peer,
			"/block",
			ar_serialize:jsonify(
				{
					[
						{<<"new_block">>, JSONStruct},
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
send_new_block(Peer, Port, NewB, RecallB, Key, Nonce) ->
	NewBShadow = NewB#block { wallet_list= [], hash_list = lists:sublist(NewB#block.hash_list,1,?STORE_BLOCKS_BEHIND_CURRENT)},
	RecallBHash =
		case ?IS_BLOCK(RecallB) of
			true ->  RecallB#block.indep_hash;
			false -> <<>>
		end,
		ar_httpc:request(
			<<"POST">>,
			Peer,
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

%% @doc Request to be added as a peer to a remote host.
add_peer(Peer) ->
	ar_httpc:request(
		<<"POST">>,
		Peer,
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
get_current_block(Peer) ->
	handle_block_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/current_block",
			[]
		)
	).

%% @doc Get the minimum cost that a remote peer would charge for
%% a transaction of the given data size in bytes.
get_tx_reward(Peer, Size) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/price/" ++ integer_to_list(Size),
			[]
		),
	list_to_integer(binary_to_list(Body)).

%% @doc Retreive a block by height or hash from a remote peer.
get_block(Peer, ID, BHL) ->
	get_full_block(Peer, ID, BHL).

%% @doc Get an encrypted block from a remote peer.
%% Used when the next block is the recall block.
get_encrypted_block(Peer, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_encrypted_block_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/encrypted",
			[]
		)
	).

%% @doc Get a specified subfield from the block with the given hash
get_block_subfield(Peer, Hash, Subfield) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host,[] Host}, {hash, Hash}]),
	handle_block_field_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/" ++ Subfield,
			[]
		)
	);
%% @doc Get a specified subfield from the block with the given height
get_block_subfield(Peer, Height, Subfield) when is_integer(Height) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_block_field_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/height/" ++integer_to_list(Height) ++ "/" ++ Subfield,
			[]
		)
	).

%% @doc Generate an appropriate URL for a block by its identifier.
prepare_block_id(ID) when is_binary(ID) ->
	"/block/hash/" ++ binary_to_list(ar_util:encode(ID));
prepare_block_id(ID) when is_integer(ID) ->
	"/block/height/" ++ integer_to_list(ID).

%% @doc Retreive a full block (full transactions included in body)
%% by hash from a remote peer.
get_full_block(Peer, ID, BHL) ->
	B =
		handle_block_response(
			ar_httpc:request(
				<<"GET">>,
				Peer,
				prepare_block_id(ID),
				[]
			)
		),
	case ?IS_BLOCK(B) of
		true ->
			WalletList =
				case is_binary(WL = B#block.wallet_list) of
					true ->
						case ar_storage:read_wallet_list(WL) of
							{error, _} ->
								get_wallet_list(Peer, WL);
							ReadWL -> ReadWL
						end;
					false -> WL
				end,
			HashList =
				case B#block.hash_list of
					undefined ->
						ar_block:generate_hash_list_for_block(B, BHL);
					HL -> HL
				end,
			FullB =
				B#block {
					txs = [ get_tx(Peer, TXID) || TXID <- B#block.txs ],
					hash_list = HashList,
					wallet_list = WalletList
				},
			case lists:any(fun erlang:is_atom/1, FullB#block.txs) of
				false -> FullB;
				true -> unavailable
			end;
		false -> B
	end.

%% @doc Get a wallet list (by its hash) from the external peer.
get_wallet_list(Peer, Hash) ->
	Response =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/wallet_list",
			[]
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:dejsonify(ar_serialize:json_struct_to_wallet_list(Body));
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found
	end.


%% @doc Get a block hash list (by its hash) from the external peer.
get_hash_list(Peer) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/hash_list",
			[]
		),
	ar_serialize:dejsonify(ar_serialize:json_struct_to_hash_list(Body)).
get_hash_list(Peer, Hash) ->
	Response =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/hash_list",
			[]
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:dejsonify(ar_serialize:json_struct_to_hash_list(Body));
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found
	end.

%% @doc Retreive a full block (full transactions included in body)
%% by hash from a remote peer in an encrypted form
get_encrypted_full_block(Peer, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_encrypted_full_block_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/all/encrypted",
			[]
		)
	).

%% @doc Retreive a tx by hash from a remote peer
get_tx(Peer, Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_new_block, {host, Host}, {hash, Hash}]),
	handle_tx_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
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
					Peer,
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
			Peer,
			"/info",
			[]
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
				Peer,
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
	ReleaseNumber =
		case lists:keyfind(<<"release">>, 1, Struct) of
			false -> 0;
			R -> R
		end,
	{_, Height} = lists:keyfind(<<"height">>, 1, Struct),
	{_, Blocks} = lists:keyfind(<<"blocks">>, 1, Struct),
	{_, Peers} = lists:keyfind(<<"peers">>, 1, Struct),
	[
		{name, NetworkName},
		{version, ClientVersion},
		{height, Height},
		{blocks, Blocks},
		{peers, Peers},
		{release, ReleaseNumber}
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
	ar_serialize:json_struct_to_hash_list(ar_serialize:dejsonify(Body));
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

%% @doc stdlib function composition.
binary_to_existing_atom(B) ->
	list_to_existing_atom(binary_to_list(B)).

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

%%%
%%% Tests.
%%%

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_info_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	reregister(http_bridge_node, BridgeNode),
	?assertEqual(<<?NETWORK_NAME>>, get_info({127, 0, 0, 1, 1984}, name)),
	?assertEqual({<<"release">>, ?RELEASE_NUMBER}, get_info({127, 0, 0, 1, 1984}, release)),
	?assertEqual(?CLIENT_VERSION, get_info({127, 0, 0, 1, 1984}, version)),
	?assertEqual(1, get_info({127, 0, 0, 1, 1984}, peers)),
	?assertEqual(1, get_info({127, 0, 0, 1, 1984}, blocks)),
	?assertEqual(0, get_info({127, 0, 0, 1, 1984}, height)).

%% @doc Ensure transaction reward can be retrieved via http iface.
get_tx_reward_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	% Hand calculated result for 1000 bytes.
	ExpectedPrice = ar:d(ar_tx:calculate_min_tx_cost(1000, B0#block.diff)),
	ExpectedPrice = ar:d(get_tx_reward({127, 0, 0, 1, 1984}, 1000)).

%% @doc Ensurte that objects are only re-gossiped once.
single_resgossip_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	TX = ar_tx:new(<<"TEST DATA">>),
	Responses =
		ar_util:pmap(
			fun(_) ->
				send_new_tx({127, 0, 0, 1, 1984}, TX)
			end,
			lists:seq(1, 100)
		),
	1 = length([ processed || {ok, {{<<"200">>, _}, _, _, _, _}} <- Responses ]).

%% @doc Test that nodes sending too many requests are temporarily blocked.
node_blacklisting_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	reregister(http_entrypoint_node, Node1),
	Responses =
		ar_util:pmap(
			fun(_) -> get_info({127, 0, 0, 1, 1984}) end,
			lists:seq(1, ?MAX_REQUESTS + 1)
		),
	ar_blacklist:reset_counters(),
	?assert(
		length([ blocked || info_unavailable <- Responses ]) == 1
	).

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_unjoined_info_test() ->
	ar_storage:clear(),
	Node1 = ar_node:start([]),
	reregister(Node1),
	BridgeNode = ar_bridge:start([]),
	reregister(http_bridge_node, BridgeNode),
	?assertEqual(<<?NETWORK_NAME>>, get_info({127, 0, 0, 1, 1984}, name)),
	?assertEqual(?CLIENT_VERSION, get_info({127, 0, 0, 1, 1984}, version)),
	?assertEqual(0, get_info({127, 0, 0, 1, 1984}, peers)),
	?assertEqual(0, get_info({127, 0, 0, 1, 1984}, blocks)),
	?assertEqual(0, get_info({127, 0, 0, 1, 1984}, height)).

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
			{127, 0, 0, 1, 1984},
			"/wallet/"++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/balance",
			[]
		),
	?assertEqual(10000, list_to_integer(binary_to_list(Body))).

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
			{127, 0, 0, 1, 1984},
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/balance",
			[]
		),
	?assertEqual(10000, list_to_integer(binary_to_list(Body))).

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
			{127, 0, 0, 1, 1984},
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx",
			[]
		),
	?assertEqual(<<"TEST_ID">>, ar_util:decode(Body)).

%% @doc Ensure that blocks can be received via a hash.
get_block_by_hash_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	receive after 200 -> ok end,
	?assertEqual(B0, get_block({127, 0, 0, 1, 1984}, B0#block.indep_hash, B0#block.hash_list)).

% get_recall_block_by_hash_test() ->
%	ar_storage:clear(),
%	  [B0] = ar_weave:init([]),
%	  ar_storage:write_block(B0),
%	  [B1|_] = ar_weave:add([B0], []),
%	ar_storage:write_block(B1),
%	Node1 = ar_node:start([], [B1, B0]),
%	reregister(Node1),
%	receive after 200 -> ok end,
%	not_found = get_block({127, 0, 0, 1, 1984}, B0#block.indep_hash).

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
	send_new_tx({127, 0, 0, 1, 1984}, SignedTX),
	receive after 200 -> ok end,
	send_new_tx({127, 0, 0, 1, 1984}, SignedTX2),
	receive after 200 -> ok end,
	ar_node:mine(Node),
	receive after 200 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	B2 = get_block({127, 0, 0, 1, 1984}, B1, B0#block.hash_list),
	B3 = get_full_block({127, 0, 0, 1, 1984}, B1, B2#block.hash_list),
	?assertEqual(B3, B2#block {txs = [SignedTX, SignedTX2]}).

%% @doc Ensure that blocks can be received via a height.
get_block_by_height_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	?assertEqual(B0, get_block({127, 0, 0, 1, 1984}, 0, B0#block.hash_list)).

get_current_block_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	ar_util:do_until(
		fun() -> B0 == ar_node:get_current_block(Node1) end,
		100,
		2000
	),
	?assertEqual(B0, get_current_block({127, 0, 0, 1, 1984})).

%% @doc Test adding transactions to a block.
add_external_tx_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	Bridge = ar_bridge:start([], Node),
	reregister(http_bridge_node, Bridge),
	ar_node:add_peers(Node, Bridge),
	send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	TXID = TX#tx.id,
	?assertEqual([TXID], (ar_storage:read_block(B1, ar_node:get_hash_list(Node)))#block.txs).

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
	send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),
	% write a get_tx function like get_block
	FoundTXID = (get_tx({127, 0, 0, 1, 1984}, TX#tx.id))#tx.id,
	?assertEqual(FoundTXID, TX#tx.id).

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
	send_new_tx({127, 0, 0, 1, 1984}, ar_tx:new(<<"DATA">>)),
	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),
	BadTX = ar_tx:new(<<"BADDATA">>),
	?assertEqual(not_found, get_tx({127, 0, 0, 1, 1984}, BadTX#tx.id)).

%% @doc Ensure that blocks can be added to a network from outside
%% a single node.
add_external_block_test_() ->
	%% TODO: faulty test: fails as Genesis block is used as recall block
	{timeout, 60, fun() ->
		ar_storage:clear(),
		[BGen] = ar_weave:init([]),
		Node1 = ar_node:start([], [BGen]),
		reregister(Node1),
		Bridge = ar_bridge:start([], Node1),
		reregister(http_bridge_node, Bridge),
		ar_node:add_peers(Node1, Bridge),
		Node2 = ar_node:start([], [BGen]),
		% Generate enough blocks to rise difference.
		lists:foreach(
			fun(_) ->
				ar_node:mine(Node2),
				timer:sleep(500)
			end,
			lists:seq(1, ?RETARGET_BLOCKS + 1)
		),
		ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node2)) > (?RETARGET_BLOCKS + 1)
			end,
			1000,
			10 * 1000
		),
		[BTest|_] = ar_node:get_blocks(Node2),
		reregister(Node1),
		send_new_block(
			{127, 0, 0, 1, 1984},
			?DEFAULT_HTTP_IFACE_PORT,
			ar_storage:read_block(BTest, ar_node:get_hash_list(Node2)),
			BGen
		),
		% Wait for test block and assert.
		?assert(ar_util:do_until(
			fun() ->
				length(ar_node:get_blocks(Node1)) > 1
			end,
			1000,
			10 * 1000
		)),
		[HB | TBs] = ar_node:get_blocks(Node1),
		?assertEqual(HB, BTest),
		LB = lists:last(TBs),
		?assertEqual(BGen, ar_storage:read_block(LB, ar_node:get_hash_list(Node1)))
	end}.

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
	send_new_tx({127, 0, 0, 1, 1984}, SignedTX),
	timer:sleep(500),
	ar_node:mine(Node),
	timer:sleep(500),
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/wallet/" ++ binary_to_list(ar_util:encode(ar_wallet:to_address(Pub1))) ++ "/last_tx",
			[]
		),
	?assertEqual(ID, ar_util:decode(Body)).

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
	send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA">>)),
	timer:sleep(1000),
	ar_node:mine(Node),
	timer:sleep(1000),
	%write a get_tx function like get_block
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)) ++ "/data",
			[]
		),
	Orig = TX#tx.data,
	?assertEqual(Orig, ar_util:decode(Body)).

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
	io:format("~p\n",[
		send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA1">>))
		]),
	timer:sleep(1000),
	%write a get_tx function like get_block
	{ok, {{<<"202">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			{127, 0, 0, 1, 1984},
			"/tx/" ++ binary_to_list(ar_util:encode(TX#tx.id)),
			[]
		),
	?assertEqual(<<"Pending">>, Body).

%% @doc Find all pending transactions in the network
%% TODO: Fix test to send txs from different wallets
get_multiple_pending_txs_test_() ->
	%% TODO: faulty test: having multiple txs against a single wallet
	%% in a single block is problematic.
	{timeout, 60, fun() ->
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
		send_new_tx({127, 0, 0, 1,1984}, TX1 = ar_tx:new(<<"DATA1">>)),
		send_new_tx({127, 0, 0, 1,1984}, TX2 = ar_tx:new(<<"DATA2">>)),
		% Wait for pending blocks.
		{ok, PendingTXs} = ar_util:do_until(
			fun() ->
				{ok, {{<<"200">>, _}, _, Body, _, _}} =
					ar_httpc:request(
						<<"GET">>,
						{127, 0, 0, 1, 1984},
						"/tx/pending",
						[]
					),
				PendingTXs = ar_serialize:dejsonify(Body),
				case length(PendingTXs) of
					2 -> {ok, PendingTXs};
					_ -> false
				end
			end,
			1000,
			30000
		),
		?assertEqual(
			[
				ar_util:encode(TX1#tx.id),
				ar_util:encode(TX2#tx.id)
			],
			PendingTXs
		)
	end}.

%% @doc Spawn a network with two nodes and a chirper server.
get_tx_by_tag_test() ->
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
			{127, 0, 0, 1, 1984},
			"/arql",
			QueryJSON
		),
	TXs = ar_serialize:dejsonify(Body),
	?assertEqual(true, lists:member(
			TX#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
	)).

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
			{127, 0, 0, 1, 1984},
			"/arql",
			QueryJSON
		),
	TXs = ar_serialize:dejsonify(Res),
	?assertEqual(true,
		lists:member(
			SignedTX#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		)),
	?assertEqual(true,
		lists:member(
			SignedTX2#tx.id,
			lists:map(
				fun ar_util:decode/1,
				TXs
			)
		)).

% get_encrypted_block_test() ->
%	ar_storage:clear(),
%	[B0] = ar_weave:init([]),
%	Node1 = ar_node:start([], [B0]),
%	reregister(Node1),
%	receive after 200 -> ok end,
%	Enc0 = get_encrypted_block({127, 0, 0, 1, 1984}, B0#block.indep_hash),
%	ar_storage:write_encrypted_block(B0#block.indep_hash, Enc0),
%	ar_cleanup:remove_invalid_blocks([]),
%	send_new_block(
%		{127, 0, 0, 1, 1984},
%		B0,
%		B0
%	),
%	receive after 500 -> ok end,
%	B0 = ar_node:get_current_block(whereis(http_entrypoint_node)),
%	ar_node:mine(Node1).

% get_encrypted_full_block_test() ->
%	ar_storage:clear(),
%	  B0 = ar_weave:init([]),
%	  ar_storage:write_block(B0),
%	TX = ar_tx:new(<<"DATA1">>),
%	TX1 = ar_tx:new(<<"DATA2">>),
%	ar_storage:write_tx([TX, TX1]),
%	Node = ar_node:start([], B0),
%	reregister(Node),
%	ar_node:mine(Node),
%	receive after 500 -> ok end,
%	[B1|_] = ar_node:get_blocks(Node),
%	Enc0 = get_encrypted_full_block({127, 0, 0, 1, 1984}, (hd(B0))#block.indep_hash),
%	ar_storage:write_encrypted_block((hd(B0))#block.indep_hash, Enc0),
%	ar_cleanup:remove_invalid_blocks([B1]),
%	send_new_block(
%		{127, 0, 0, 1, 1984},
%		hd(B0),
%		hd(B0)
%	),
%	receive after 1000 -> ok end,
%	ar_node:mine(Node).
	% ar_node:add_peers(Node, Bridge),
	% receive after 200 -> ok end,
	% send_new_tx({127, 0, 0, 1, 1984}, TX = ar_tx:new(<<"DATA1">>)),
	% receive after 200 -> ok end,
	% send_new_tx({127, 0, 0, 1, 1984}, TX1 = ar_tx:new(<<"DATA2">>)),
	% receive after 200 -> ok end,
	% ar_node:mine(Node),
	% receive after 200 -> ok end,
	% [B1|_] = ar_node:get_blocks(Node),
	% B2 = get_block({127, 0, 0, 1, 1984}, B1),
	% ar:d(get_encrypted_full_block({127, 0, 0, 1, 1984}, B2#block.indep_hash)),
	% B2 = get_block({127, 0, 0, 1, 1984}, B1),
	% B3 = get_full_block({127, 0, 0, 1, 1984}, B1),
	% B3 = B2#block {txs = [TX, TX1]},

%%%
%%% EOF
%%%
