%%%
%%% @doc Handle http requests.
%%%

-module(ar_http_iface_server).

-export([start/5]).
-export([reregister/1, reregister/2]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%
%%% Public API.
%%%

%% @doc Start the Arweave HTTP API and returns a process ID.
start(Port, Node, SearchNode, ServiceNode, BridgeNode) ->
	reregister(http_entrypoint_node, Node),
	reregister(http_search_node, SearchNode),
	reregister(http_service_node, ServiceNode),
	reregister(http_bridge_node, BridgeNode),
	do_start(Port).

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

%%%
%%% Private functions
%%%

%% @doc Start the server
do_start(Port) ->
	ar_blacklist:start(),
	Routes = [
		{'_', [
			{"/metrics/[:registry]", prometheus_cowboy2_handler, []},
			{"/[...]", ar_http_iface_cowboy_handler, []}
		]}
	],
	Dispatch = cowboy_router:compile(Routes),
	ProtocolOpts =
		#{
			middlewares => [
				ar_blacklist,
				cowboy_router,
				cowboy_handler
			],
			env => #{dispatch => Dispatch},
			metrics_callback => fun prometheus_cowboy2_instrumenter:observe/1,
			stream_handlers => [cowboy_metrics_h, cowboy_stream_h]
		},

	{ok, _} =
		cowboy:start_clear(
			ar_cowboy_listener,
			[{port, Port}],
			ProtocolOpts
		).
