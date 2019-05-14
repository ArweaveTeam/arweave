%%%
%%% @doc Handle http requests.
%%%

-module(ar_http_iface_server).

-export([start/2]).
-export([reregister/1, reregister/2]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%
%%% Public API.
%%%

%% @doc Start the Arweave HTTP API and returns a process ID.
start(Port, HttpNodes) ->
	reregister_from_proplist([
		http_entrypoint_node,
		http_search_node,
		http_service_node,
		http_bridge_node
	], HttpNodes),
	do_start(Port).

reregister_from_proplist(Names, HttpNodes) ->
	lists:foreach(fun(Name) ->
		reregister(Name, proplists:get_value(Name, HttpNodes))
	end, Names).

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
			{"/[...]", ar_http_iface_h, []}
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
	{ok, _} = cowboy:start_clear(
		ar_http_iface_listener,
		[{port, Port}],
		ProtocolOpts
	).

