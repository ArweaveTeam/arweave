-module(ar_http_iface).
-export([start/0, start/1, start/2, start/3, start/4, start/5, handle/2, handle_event/3]).
-export([send_new_block/3, send_new_block/4, send_new_tx/2, get_block/2, get_tx/2, get_full_block/2, get_block_subfield/3, add_peer/1]).
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

%% @doc Main function to handle a request to the server.
%% Requests are HTTP requests to an IP. For example a GET request to
%% http://192.168.0.0/block/height where the body of the GET request is the
%% block height.
%%
%% Similarly, you can use POST or send transactions and blocks to be added
%% and use GET requests to to obtain peers, get a block by its height, hash or
%% or simply obtain information about it. In this way, it is possible to
%% perform many actions on the archain purely via platform agnostic HTTP.
%%
%% NB: Blocks and transactions are transmitted between HTTP nodes in JSON
%% format.
handle(Req, _Args) ->
	update_performance_list(Req),
	handle(Req#req.method, elli_request:path(Req), Req).

handle('GET', [], _Req) ->
	return_info();
% Get information about the network from node in JSON format.
handle('GET', [<<"info">>], _Req) ->
	return_info();
% Get all pending transactions
handle('GET', [<<"tx">>, <<"pending">>], _Req) ->
	{200, [],
		list_to_binary(
			ar_serialize:jsonify(
				{array,
					ar_node:get_pending_txs(whereis(http_entrypoint_node))
				}
			)
		)
	};
% Get a transaction by hash
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
% Get a transaction by hash and return the associated data.
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
% Add block specified in HTTP body.
handle('POST', [<<"block">>], Req) ->
	BlockJSON = elli_request:body(Req),
	{ok, {struct, Struct}} = json2:decode_string(binary_to_list(BlockJSON)),
	{"recall_block", JSONRecallB} = lists:keyfind("recall_block", 1, Struct),
	{"new_block", JSONB} = lists:keyfind("new_block", 1, Struct),
	{"port", Port} = lists:keyfind("port", 1, Struct),
	BShadow = ar_serialize:json_struct_to_block(JSONB),
	OrigPeer =
		ar_util:parse_peer(
			bitstring_to_list(elli_request:peer(Req))
			++ ":"
			++ integer_to_list(Port)
			),
	case ar_node:get_current_block(whereis(http_entrypoint_node)) of
		unavailable ->
			B = ar_http_iface:get_block(OrigPeer, BShadow#block.indep_hash),
			RecallB = unavailable;
		CurrentBlock ->
			B = BShadow#block {
				wallet_list = ar_node:apply_txs(
					CurrentBlock#block.wallet_list,
					ar_storage:read_tx(BShadow#block.txs)
				),
				hash_list =
					[
						CurrentBlock#block.indep_hash
						|
						CurrentBlock#block.hash_list
					]
				},
			RecallB = ar_storage:read_block(ar_util:decode(JSONRecallB))
			%ar_bridge:ignore_id(whereis(http_bridge_node), {B#block.indep_hash, OrigPeer}),
			%ar:report_console([{recvd_block, B#block.height}, {port, Port}]),
	end,
	ar_bridge:add_block(
		whereis(http_bridge_node),
		OrigPeer,
		B,
		RecallB
	),
	{200, [], <<"OK">>};



% Add transaction specified in body.
handle('POST', [<<"tx">>], Req) ->
	TXJSON = elli_request:body(Req),
	TX = ar_serialize:json_struct_to_tx(binary_to_list(TXJSON)),
	B = ar_node:get_current_block(whereis(http_entrypoint_node)),
	case ar_tx:verify(TX, B#block.diff) of
		false ->
			ar:d({rejected_tx , ar_util:encode(TX#tx.id)}),
			{400, [], <<"Transaction signature not valid.">>};
		true ->
			ar:d({accepted_tx , ar_util:encode(TX#tx.id)}),
			timer:apply_after(
				calculate_delay(byte_size(ar_tx:to_binary(TX))),
				ar_bridge,
				add_tx,
				[whereis(http_bridge_node), TX]
				),
			{200, [], <<"OK">>}
	end;


% Get peers.
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
% Get price of adding data of SizeInByes
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
% Get hashlist.
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
% Get walletlist.
handle('GET', [<<"wallet_list">>], _Req) ->
    Node = whereis(http_entrypoint_node),
    WalletList = ar_node:get_wallet_list(Node),
    %ar:d(list_to_binary(ar_serialize:jsonify(ar_serialize:wallet_list_to_json_struct(WalletList)))),
    {200, [], 
        list_to_binary(
            ar_serialize:jsonify(
                ar_serialize:wallet_list_to_json_struct(WalletList)
            )
        )
    };

% TODO: Return remaining timeout on a failed request
% TODO: Optionally, allow adding self on a non-default port
% TODO: Nicer way of segregating different networks
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
% Get last TX ID hash
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
% Gets a block by block hash.
% TODO: Currently doesn't return blocks not on the hashlist, this should be a lower
% level responsibility
handle('GET', [<<"block">>, <<"hash">>, Hash], _Req) ->
	%ar:d({resp_block_hash, Hash}),
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	CurrentBlock = ar_node:get_current_block(whereis(http_entrypoint_node)),
	case CurrentBlock of
		unavailable -> return_block(unavailable);
		_ ->
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
	end;
% Get a full block by hash
handle('GET', [<<"block">>, <<"hash">>, Hash, <<"all">>], _Req) ->
	ar:d({resp_block_hash, Hash}),
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	CurrentBlock = ar_node:get_current_block(whereis(http_entrypoint_node)),
	case CurrentBlock of
		unavailable -> return_block(unavailable);
		_ ->
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
	end;
		% Gets a block by block height.
handle('GET', [<<"block">>, <<"height">>, Height], _Req) ->
	return_block(
		ar_node:get_block(whereis(http_entrypoint_node),
			list_to_integer(binary_to_list(Height)))
	);
% Get the top, current block.
handle('GET', [<<"block">>, <<"current">>], _Req) ->
	return_block(ar_node:get_current_block(whereis(http_entrypoint_node)));
% Get the top, current block.
handle('GET', [<<"current_block">>], _Req) ->
	return_block(ar_node:get_current_block(whereis(http_entrypoint_node)));
% Return a list of known services, when asked.
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
%% Return a subfield of the tx with the given hash
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

% Add reports of service locations
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
%Handles otherwise unhandles HTTP requests and returns 500.
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
									(ar_storage:read_block(H))#block.height;
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
							{port, Port}
						]
					}
				)
			)
		}, [{timeout, ?NET_TIMEOUT}], []
	).

%% @doc Add peer (self) to host.
add_peer(Host) ->
	%ar:d({adding_host, Host}),
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
			bytes = P#performance.bytes + Bytes,
			timestamp = os:system_time()
		}
	).

%% @doc Calculate the delay before a transaction should be released to the network
%% Currently based on 40s for 95% of peers in the bitcoin network to recieve a
%% 1mb block

calculate_delay(0) -> 0;
calculate_delay(Bytes) -> (Bytes * 40) div 1000.
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
	receive after 1000 -> ok end,
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
	SignedTX = ar_tx:sign(TX#tx{ id = <<"TEST">> }, Priv1, Pub1),
	send_new_tx({127, 0, 0, 1}, SignedTX),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	{ok, {{_, 200, _}, _, Body}} =
		ar_httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/wallet/"
		 		++ ar_util:encode(ar_wallet:to_address(Pub1))
				++ "/last_tx"),
	<<"TEST">> = ar_util:decode(Body).

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
