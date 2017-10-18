-module(ar_http_iface).
-export([start/0, start/1, handle/2, handle_event/3]).
-include("ar.hrl").
-include("../lib/elli/include/elli.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Exposes access to an internal Archain network to external nodes.

%% @doc Start the interface.
start() -> start(?DEFAULT_HTTP_IFACE_PORT).
start(Port) ->
	{ok, PID} = elli:start_link([{callback, ?MODULE}, {port, Port}]),
	PID.

%% @doc Handle a request to the server.
handle(Req, _Args) ->
	handle(Req#req.method, elli_request:path(Req), Req).

handle('GET', [<<"api">>], _Req) ->
	{200, [], <<"OK">>};
handle('POST', [<<"api">>, <<"add_block">>], Req) ->
	BlockJSON = elli_request:body(Req),
	{ok, {struct, Struct}} = json2:decode_string(binary_to_list(BlockJSON)),
	{"recall_block", JSONRecallB} = lists:keyfind("recall_block", 1, Struct),
	{"new_block", JSONB} = lists:keyfind("new_block", 1, Struct),
	B = ar_serialize:json_struct_to_block(JSONB),
	RecallB = ar_serialize:json_struct_to_block(JSONRecallB),
	Node = whereis(http_entrypoint_node),
	ar_node:add_block(Node, B, RecallB),
	{200, [], <<"OK">>};
handle('POST', [<<"api">>, <<"add_tx">>], Req) ->
	TXJSON = elli_request:body(Req),
	TX = ar_serialize:json_struct_to_tx(binary_to_list(TXJSON)),
	ar:report(TX),
	Node = whereis(http_entrypoint_node),
	ar_node:add_tx(Node, TX),
	{200, [], <<"OK">>};
handle(_, _, _) ->
	{500, [], <<"Request type not found.">>}.

%% @doc Handles elli metadata events.
handle_event(Event, Data, Args) ->
	ar:report([{elli_event, Event}, {data, Data}, {args, Args}]),
	ok.

%% @doc Test adding transactions to a block.
add_external_tx_test() ->
	[B0] = ar_weave:init(),
	Node = ar_node:start([], [B0]),
	reregister(Node),
	TX = ar_tx:new(<<"DATA">>),
	TXJSON = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
	httpc:request(
		post,
		{
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/api/add_tx",
			[],
			"application/x-www-form-urlencoded",
			TXJSON
		}, [], []
	),
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
	JSONB0 = ar_serialize:block_to_json_struct(B0),
	JSONB1 = ar_serialize:block_to_json_struct(B1),
	httpc:request(
		post,
		{
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/api/add_block",
			[],
			"application/x-www-form-urlencoded",
			lists:flatten(
				ar_serialize:jsonify(
					{struct,
						[
							{new_block, JSONB1},
							{recall_block, JSONB0}
						]
					}
				)
			)
		}, [], []
	),
	receive after 1000 -> ok end,
	[B1, B0] = ar_node:get_blocks(Node1).

%% @doc Helper function : registers a new node as the entrypoint.
reregister(Node) ->
	case erlang:whereis(http_entrypoint_node) of
		undefined -> do_nothing;
		_ -> erlang:unregister(http_entrypoint_node)
	end,
	erlang:register(http_entrypoint_node, Node).
