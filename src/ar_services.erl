-module(ar_services).
-export([start/0, start/1, get/1, add/2]).
-include("ar.hrl").
-compile({no_auto_import, [{get, 1}]}).
-include_lib("eunit/include/eunit.hrl").

%%% A module defining a process that keeps track of known services
%%% on an Archain network.

%% Server state.
-record(state, {
	miner,
	services = []
}).

%% How long to wait between attempts to prune expired service definitions
%% from the server.
-define(PRUNE_TIMEOUT, 60 * 1000).

start() -> start(whereis(http_entypoint_node)).
start(Node) -> spawn(fun() -> server(#state { miner = Node }) end).

%% Add a list of services to the system.
add(PID, Services) ->
	PID ! {add, Services},
	ok.

%% Get the current list of known services
get(PID) ->
	PID ! {get, self()},
	receive {services, Services} -> Services end.

%% Main server loop.
server(RawS) ->
	S = prune(RawS),
	receive
		{add, NewServices} ->
			server(add_services(S, NewServices));
		{get, PID} ->
			PID ! {services, S#state.services},
			server(S);
		% For testing purposes!!
		prune -> server(S)
	after ?PRUNE_TIMEOUT -> server(S)
	end.

%% Merge new service reports into the server state.
add_services(S, NewServices) ->
	S#state {
		services =
			lists:foldl(
				fun(New, Services) ->
					case lists:keyfind(New#service.name, #service.name, Services) of
						false -> [New|Services];
						Old when New#service.expires > Old#service.expires ->
							[New|(Services -- [Old])];
						_ -> Services
					end
				end,
				S#state.services,
				NewServices
			)
	}.

%% Remove services that have expired.
prune(S) ->
	S#state {
		services =
			prune(
				case ar_node:get_current_block(S#state.miner) of
					undefined -> 0;
					B -> B#block.height
				end,
				S#state.services
			)
	}.
prune(CurrHeight, Services) ->
	ar:d([{height, CurrHeight}, {services, Services}]),
	ar:d(lists:filter(fun(S) -> S#service.expires >= CurrHeight end, Services)).

%% Ensure that services can be added.
add_service_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	PID = start(Node1),
	add(PID, [#service { name = "test", host = {127,0,0,1,1984}, expires = 1 }]),
	ar_node:mine(Node1),
	receive after 1000 -> ok end,
	PID ! prune,
	[Serv] = get(PID),
	"test" = Serv#service.name.

%% Ensure that services are removed at the appropriate moment.
drop_service_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	Node1 = ar_node:start([], [B0]),
	PID = start(Node1),
	add(PID, [#service { name = "test", host = {127,0,0,1,1984}, expires = 1 }]),
	[_Serv] = get(PID),
	ar_node:mine(Node1),
	receive after 1000 -> ok end,
	ar_node:mine(Node1),
	receive after 1000 -> ok end,
	PID ! prune,
	[] = get(PID).
