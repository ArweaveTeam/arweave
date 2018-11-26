-module(app_skeleton).
-export([start/0, start/1, stop/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A framework for simply building Arweave apps.
%%% Simply add your code in the areas marked TODO.

%% Internal state record.
-record(state,{
	gossip % State of the gossip protocol.
	%% TODO: Add any extra fields needed by your app here.
}).

%% Start a node, optionally with a list of peers.
start() -> start([]).
start(Peers) ->
	spawn(
		fun() ->
			server(
				#state {
					gossip = ar_gossip:init(Peers)
					%% TODO: Initialise your state fields.
				}
			)
		end
	).

%% Stop the app.
stop(PID) ->
	PID ! stop.

%% TODO: (Optional) Add functions for interacting with your app.

%% The main app server loop.
server(S = #state { gossip = GS }) ->
	%% Listen for gossip and normal messages.
	%% Recurse through the message box, updating one's state each time.
	receive
		Msg when is_record(Msg, gs_msg) ->
			% We have received a gossip mesage. Use the library to process it.
			case ar_gossip:recv(GS, Msg) of
				{NewGS, ignore} ->
					server(S#state { gossip = NewGS });
				{NewGS, _GossipMessage} ->
					% TODO: Add your gossip message processing code here!
					server(S#state { gossip = NewGS })
			end;
		%% TODO:(Optional) Add code for processing a local message here.
		stop -> ok
	end.
