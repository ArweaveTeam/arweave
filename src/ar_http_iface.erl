-module(ar_http_iface).
-export([start/0, start/1, start/2, handle/2, handle_event/3]).
-export([send_new_block/4, send_new_tx/2, get_block/2]).
-include("ar.hrl").
-include("../lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Exposes access to an internal Archain network to external nodes.

%% @doc Start the archain HTTP API and Returns a process ID.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) ->
	spawn(
		fun() ->
			{ok, PID} = elli:start_link([{callback, ?MODULE}, {port, Port}]),
			receive stop -> elli:stop(PID) end
		end
	).
start(Port, Node) ->
	reregister(Node),
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
	handle(Req#req.method, elli_request:path(Req), Req).

handle('GET', [], _Req) ->
	return_info();
% Get information about a block in JSON format.
handle('GET', [<<"info">>], _Req) ->
	return_info();
% Add block specified in HTTP body.
handle('POST', [<<"block">>], Req) ->
	BlockJSON = elli_request:body(Req),
	{ok, {struct, Struct}} = json2:decode_string(binary_to_list(BlockJSON)),
	{"recall_block", JSONRecallB} = lists:keyfind("recall_block", 1, Struct),
	{"new_block", JSONB} = lists:keyfind("new_block", 1, Struct),
	{"port", Port} = lists:keyfind("port", 1, Struct),
	B = ar_serialize:json_struct_to_block(JSONB),
	RecallB = ar_serialize:json_struct_to_block(JSONRecallB),
	%ar:report_console([{recvd_block, B#block.height}, {port, Port}]),
	ar_node:add_block(
		whereis(http_entrypoint_node),
		ar_util:parse_peer(
			bitstring_to_list(elli_request:peer(Req))
			++ ":"
			++ integer_to_list(Port)
		),
		B,
		RecallB,
		B#block.height
	),
	{200, [], <<"OK">>};
% Add transaction specified in body.
handle('POST', [<<"tx">>], Req) ->
	TXJSON = elli_request:body(Req),
	TX = ar_serialize:json_struct_to_tx(binary_to_list(TXJSON)),
	%ar:report(TX),
	Node = whereis(http_entrypoint_node),
	ar_node:add_tx(Node, TX),
	{200, [], <<"OK">>};
% Get peers.
handle('GET', [<<"peers">>], _Req) ->
	{200, [],
		list_to_binary(
			ar_serialize:jsonify(
				{array,
					[
						ar_util:format_peer(P)
					||
						P <- ar_node:get_peers(whereis(http_entrypoint_node)),
						not is_pid(P)
					]
				}
			)
		)
	};
% Gets a block by block hash.
handle('GET', [<<"block">>, <<"hash">>, Hash], _Req) ->
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, elli_request:path(Req)}]),
	return_block(
		ar_node:get_block(whereis(http_entrypoint_node),
			ar_util:decode(Hash))
	);
% Gets a block by block height.
handle('GET', [<<"block">>, <<"height">>, Height], _Req) ->
	%ar:report_console([{resp_getting_block, list_to_integer(binary_to_list(Height))}]),
	return_block(
		ar_node:get_block(whereis(http_entrypoint_node),
			list_to_integer(binary_to_list(Height)))
	);
%Handles otherwise unhandles HTTP requests and returns 500.
handle(_, _, _) ->
	{500, [], <<"Request type not found.">>}.

%% @doc Handles all other elli metadata events.
handle_event(_Event, _Data, _Args) ->
	%ar:report([{elli_event, Event}, {data, Data}, {args, Args}]),
	ok.

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

%% @doc Generate and return an informative JSON object regarding
%% the state of the node.
return_info() ->
	Bs = [B|_] = ar_node:get_blocks(whereis(http_entrypoint_node)),
	Peers = ar_node:get_peers(whereis(http_entrypoint_node)),
	{200, [],
		list_to_binary(
			ar_serialize:jsonify(
				{struct,
					[
						{network, ?NETWORK_NAME},
						{version, ?CLIENT_VERSION},
						{height, B#block.height},
						{blocks, length(Bs)},
						{peers, length(Peers)}
					]
				}
			)
		)
	}.

%%% Client functions

%% @doc Send a new transaction to an Archain HTTP node.
send_new_tx(Host, TX) ->
	httpc:request(
		post,
		{
			"http://" ++ ar_util:format_peer(Host) ++ "/tx",
			[],
			"application/x-www-form-urlencoded",
			ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
		}, [], []
	).

%% @doc Distribute a newly found block to remote nodes.
send_new_block(Host, Port, NewB, RecallB) ->
	%ar:report_console([{sending_new_block, NewB#block.height}, {stack, erlang:get_stacktrace()}]),
	httpc:request(
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
								ar_serialize:block_to_json_struct(NewB)},
							{recall_block,
								ar_serialize:block_to_json_struct(RecallB)},
							{port, Port}
						]
					}
				)
			)
		}, [], []
	).

%% @doc Retreive a block by height or hash from a node.
get_block(Host, Height) when is_integer(Height) ->
	%ar:report_console([{req_getting_block_by_height, Height}]),
	handle_block_response(
		httpc:request(
			"http://"
				++ ar_util:format_peer(Host)
				++ "/block/height/"
				++ integer_to_list(Height)));
get_block(Host, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	handle_block_response(
		httpc:request(
			"http://"
				++ ar_util:format_peer(Host)
				++ "/block/hash/"
				++ ar_util:encode(Hash))).

%% @doc Process the response of an /block call.
handle_block_response({ok, {{_, 200, _}, _, Body}}) ->
	ar_serialize:json_struct_to_block(Body);
handle_block_response({ok, {{_, 404, _}, _, _}}) ->
	not_found.

%% @doc Helper function : registers a new node as the entrypoint.
reregister(Node) ->
	case erlang:whereis(http_entrypoint_node) of
		undefined -> do_nothing;
		_ -> erlang:unregister(http_entrypoint_node)
	end,
	erlang:register(http_entrypoint_node, Node).

%%% Tests

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_info_test() ->
	[B0] = ar_weave:init(),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	{ok, {{_, 200, _}, _, Body}} =
		httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/info"),
	{ok, {struct, Struct}} = json2:decode_string(Body),
	{_, ?NETWORK_NAME} = lists:keyfind("network", 1, Struct),
	{_, ?CLIENT_VERSION} = lists:keyfind("version", 1, Struct),
	{_, 1} = lists:keyfind("blocks", 1, Struct),
	{_, 0} = lists:keyfind("peers", 1, Struct).

%% @doc Ensure that server info can be retreived via the HTTP interface.
get_peers_test() ->
	[B0] = ar_weave:init(),
	Node1 = ar_node:start([{127,0,0,1,1984},{127,0,0,1,1985}], [B0]),
	reregister(Node1),
	{ok, {{_, 200, _}, _, Body}} =
		httpc:request(
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/peers"),
	{ok, {array, Array}} = json2:decode_string(Body),
	true = lists:member("127.0.0.1:1984", Array),
	true = lists:member("127.0.0.1:1985", Array).

%% @doc Ensure that blocks can be received via a hash.
get_block_by_hash_test() ->
	[B0] = ar_weave:init(),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	receive after 200 -> ok end,
	B0 = get_block({127, 0, 0, 1}, B0#block.indep_hash).

%% @doc Ensure that blocks can be received via a height.
get_block_by_height_test() ->
	[B0] = ar_weave:init(),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	B0 = get_block({127, 0, 0, 1}, 0).

%% @doc Test adding transactions to a block.
add_external_tx_test() ->
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	send_new_tx({127, 0, 0, 1}, TX = ar_tx:new(<<"DATA">>)),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	[B1|_] = ar_node:get_blocks(Node),
	[TX] = B1#block.txs.

%% @doc Ensure that blocks can be added to a network from outside
%% a single node.
add_external_block_test() ->
	[B0] = ar_weave:init(),
	Node1 = ar_node:start([], [B0]),
	reregister(Node1),
	Node2 = ar_node:start([], [B0]),
	ar_node:mine(Node2),
	receive after 1000 -> ok end,
	[B1|_] = ar_node:get_blocks(Node2),
	send_new_block({127, 0, 0, 1}, ?DEFAULT_HTTP_IFACE_PORT, B1, B0),
	receive after 500 -> ok end,
	[B1, B0] = ar_node:get_blocks(Node1).
