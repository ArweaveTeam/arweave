-module(ar_firewall).
-export([start/0, scan/2]).

%%% Archain firewall implementation.
%%% TODO: Call out to av_* functions.

%% State definition. Should store compiled
%% binary scan objects.
-record(state, {}).

%% Start a firewall node.
start() ->
	spawn(fun server/0).

%% Check that a received object does not match
%% the firewall rules.
scan(PID, Obj) ->
	PID ! {scan, self(), Obj},
	receive
		{scanned, Obj, Response} -> Response
	end.

server() -> server(#state{}).
server(S) ->
	receive
		{scan, PID, Obj} ->
			PID ! {scanned, Obj, true},
			server(S)
	end.