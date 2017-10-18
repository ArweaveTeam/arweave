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
	{"recall_block", JSONRecallB} = lists:keyfind(Struct, 1, "recall_block"),
	{"new_block", JSONB} = lists:keyfind(Struct, 1, "new_block"),
	B = ar_serialize:json_to_block(JSONB),
	RecallB = ar_serialize:json_to_block(JSONRecallB),
	Node = whereis(http_entrypoint_node),
	ar:report([{adding_block, B}, {recall_block, RecallB}]),
	ar_node:add_block(Node, B, RecallB),
	{200, [], <<"OK">>};
handle('POST', [<<"api">>, <<"add_tx">>], Req) ->
	TXJSON = elli_request:body(Req),
	TX = ar_serialize:json_to_tx(binary_to_list(TXJSON)),
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
	B0 = ar_weave:init(),
	Node = ar_node:start([], B0),
	register(http_entrypoint_node, Node),
	TX = ar_tx:new(<<"DATA">>),
	TXJSON = ar_serialize:tx_to_json(TX),
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
	%% TODO: Register node with router process.
	%register(http_entrypoint_node, Node1),
	[B1|_] = ar_weave:add([B0], []),
	%% Remember to string:join("|", lists:map(fun tx_to_field, TXs))
	%% in ar_serialize:block_to_fields.
	JSONB0 = ar_serialize:block_to_json(B0),
	JSONB1 = ar_serialize:block_to_json(B1),
	httpc:request(
		post,
		{
			"http://127.0.0.1:"
				++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
				++ "/api/add_block",
			[],
			"application/x-www-form-urlencoded",
			lists:flatten(
				json2:encode(
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
