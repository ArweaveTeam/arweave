-module(adt_simple).
-export([start/1, start/2, start/3, stop/1]).
-include("ar.hrl").

%%% A simple abstraction for building Archain apps.
%%% Provides a simple method (using a callback module) for interacting with
%%% the Archain.
%%%
%%% All callback functions in the modules that use this system are optional.
%%% Unimplemented callbacks will be ignored.
%%%
%%% Supported events and callbacks:
%%% 	new_transaction				Processes a new transaction that has been
%%% 								submitted to the weave.
%%% 	confirmed_transaction		Processes new transactions that have been
%%% 								accepted onto a block.
%%% 	new_block					Processes a new block that has been added
%%% 								to the weave.
%%% 	message						Called when non-gossip messages are received
%%% 								by the server.
%%% Each callback can take two optional arguments: the gossip server state
%%% and an arbitrary application state term. The required inputs and outputs of
%%% these functions are defined in the way:
%%% 	callback(NewData) -> _
%%% 	callback(AppState, NewData) -> NewAppState
%%% 	callback(AppState, GossipState, NewData) -> {NewAppState, GossipState}

%% Internal state record.
-record(state,{
	gossip, % State of the gossip protocol.
	mod, % The callback module of the app.
	app_state = undefined % The state variable associated with the app.
}).

%% Start a new adt_simple app. Takes the module that implements our callbacks,
%% and an optional peer list.
start(CallbackMod) -> start(CallbackMod, []).
start(CallbackMod, AppState) -> start(CallbackMod, AppState, []).
start(CallbackMod, AppState, Peers) ->
	spawn(
		fun() ->
			server(
				#state {
					gossip = ar_gossip:init(Peers),
					mod = CallbackMod,
					app_state = AppState
				}
			)
		end
	).

%% Stop the app.
stop(PID) ->
	PID ! stop.

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
				{NewGS, {add_tx, TX}} ->
					server(
						apply_callback(S#state { gossip = NewGS }, new_transaction, TX)
					);
				{NewGS, {new_block, _, B, _}} ->
					NewS =
						lists:foldl(
							fun(TX, NextS) ->
								apply_callback(NextS, confirmed_transaction, TX)
							end,
							S#state { gossip = NewGS },
							B#block.txs
						),
					server(apply_callback(NewS, new_block, B))
			end;
		stop -> ok;
		OtherMsg ->
			server(
				S#state {
					gossip =
						apply_callback(S, message, OtherMsg)
				}
			)
	end.

%%% Utility functions

%% Make a callback call. Returns the new gossip protocol state.
apply_callback(S = #state { app_state = AppS, gossip = GS }, Fun, Val) ->
	apply_callback(S, GS, AppS, Fun, Val).
apply_callback(S = #state { mod = Mod }, GS, AppS, Fun, Val) ->
	case lists:keyfind(Fun, 1, Mod:module_info(exports)) of
		{Fun, 3} ->
			{NewAppS, NewGS} = Mod:Fun(AppS, GS, Val),
			S#state { gossip = NewGS, app_state = NewAppS };
		{Fun, 2} ->
			NewAppS = Mod:Fun(AppS, Val),
			S#state { app_state = NewAppS };
		{Fun, 1} ->
			Mod:Fun(Val),
			S;
		false -> S
	end.
