-module(ar_simple_reporter).
-export([start/0, start/1, start/2, stop/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A simple Archain app that reports when it hears about new blocks.
%%% The app will send a message to the process ID given to it as a
%%% starting argument, or it will print to the console.

%% Internal state record.
-record(state,{
	gossip,
	report
}).

%% @doc Start a node, optionally with a list of peers.
start() -> start(undefined).
start(ReportPID) -> start(ReportPID, []).
start(ReportPID, Peers) ->
	spawn(
		fun() ->
			server(
				#state {
					gossip = ar_gossip:init(Peers),
					report = ReportPID
				}
			)
		end
	).

%% @doc Stop the app.
stop(PID) ->
	PID ! stop.

%% @doc The main app server loop.
server(S = #state { gossip = GS, report = ReportPID }) ->
	%% Listen for gossip and normal messages.
	%% Recurse through the message box, updating one's state each time.
	receive
		Msg when is_record(Msg, gs_msg) ->
			case ar_gossip:recv(GS, Msg) of
				{NewGS, ignore} ->
					% Message not important. Ignore.
					server(S#state { gossip = NewGS });
				{NewGS, {new_block, _, NewHeight, _NewB}} ->
					% We are being told about a new block. Report its height.
					case ReportPID of
						undefined ->
							io:format("REP: Received new block ~w~n", [NewHeight]);
						_ ->
							ReportPID ! {new_block, NewHeight}
					end,
					% Recurse with the new gossip protocol state.
					server(S#state { gossip = NewGS })
			end;
		stop -> ok
	end.

%% @doc Start an Archain network with a simple monitor.
%% Ensure that new block notifications are received.
simple_test() ->
	% Create genesis block.
	B0 = ar_weave:init(),
	% Start an observer.
	Node1 = start(self()),
	% Start an Archain node with the new blockweave.
	Node2 = ar_node:start([Node1], B0),
	% Add the testing process to the gossip network.
	GS0 = ar_gossip:init([Node2]),
	% Create a new block in the weave.
	B1 = ar_weave:add(B0, [ar_tx:new(<<"HELLO WORLD">>)]),
	% Send the new block to the gossip node, which should inform our monitor.
	ar_gossip:send(GS0, {new_block, self(), (hd(B1))#block.height, hd(B1)}),
	% Receive the 'new block found; message for the first block.
	receive {new_block, 1} -> ok end.

%% @doc Test that multiple block notifications are received correctly.
multiple_test() ->
	% Create weave iterations.
	B0 = ar_weave:init(),
	B1 = ar_weave:add(B0, [ar_tx:new(<<"HELLO WORLD">>)]),
	B2 = ar_weave:add(B1, [ar_tx:new(<<"NEXT MESSAGE">>)]),
	B3 = ar_weave:add(B2, [ar_tx:new(<<"ANOTHER MESSAGE">>)]),
	% Start the blockweave node and gossip protocol.
	Node1 = start(self()),
	Node2 = ar_node:start([Node1], B0),
	GS0 = ar_gossip:init([Node2]),
	% Send new block messages to the the gossip network.
	{GS1, _} = ar_gossip:send(GS0, {new_block, self(), (hd(B1))#block.height, hd(B1)}),
	{GS2, _} = ar_gossip:send(GS1, {new_block, self(), (hd(B2))#block.height, hd(B2)}),
	ar_gossip:send(GS2, {new_block, self(), (hd(B3))#block.height, hd(B3)}),
	% Receive the new block notifications for all three gossiped blocks.
	receive {new_block, 1} -> ok end,
	receive {new_block, 2} -> ok end,
	receive {new_block, 3} -> ok end.
