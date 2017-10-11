-module(ar_http_iface).
-export([start/0, start/1, handle/2, handle_event/3]).
-include("ar.hrl").
-include_lib("elli.hrl").
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
handle('POST', [<<"api">>, <<"add_block">>], _Req) ->
	%% TODO: Get block fields from Req
	%% TODO: Make block record from fields
	Node = whereis(http_entrypoint_node),
	ar_node:add_block(Node, parsed_block),
	{200, [], <<"OK">>};
handle('POST', [<<"api">>, <<"add_tx">>], _Req) ->
	%% TODO: Implement
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
	Node = start([], B0),
	%% TODO: Register node with router process.
	register(http_entrypoint_node, Node),
	TX = ar_tx:new(<<"DATA">>),
	%% TODO: Implement ar_serialize.
	Fields = ar_serialize:tx_to_fields(TX),
	%% Remember to base64:encode/1 the binary fields!
	httpc:request(
		post,
		%% Check in docs for args!!!
		"http://127.0.0.1:"
			++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
			++ "/api/add_tx"
	),
	receive after 1000 -> ok end,
	ar_node:mine(Node),
	receive after 1000 -> ok end,
	[B1|_] = get_blocks(Node),
	[TX] = B1#block.txs.

%% @doc Ensure that blocks can be added to a network from outside
%% a single node.
add_external_block_test() ->
	B0 = ar_weave:init(),
	Node1 = start([], B0),
	%% TODO: Register node with router process.
	register(http_entrypoint_node, Node1),
	[B1|_] = ar_weave:add(B0, []),
	%% Remember to string:join("|", lists:map(fun tx_to_field, TXs))
	%% in ar_serialize:block_to_fields.
	Fields = ar_serialize:block_to_fields(B1),
	%% TODO: Send B1 to node, via HTTP interface.
	%% Remember to base64:encode/1 the binary fields!
	httpc:request(
		post,
		%% Check in docs for args!!!
		"http://127.0.0.1:"
			++ integer_to_list(?DEFAULT_HTTP_IFACE_PORT)
			++ "/api/new_block"
	),
	receive after 1000 -> ok end,
	B1 = get_blocks(Node2),
	1 = (hd(B1))#block.height.
