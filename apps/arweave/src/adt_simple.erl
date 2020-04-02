-module(adt_simple).
-export([start/1, start/2, start/3, stop/1]).
-export([report/1]).
-include("ar.hrl").

%%% A simple abstraction for building Arweave apps.
%%% Provides a simple method (using a callback module) for interacting with
%%% the Arweave in real time.
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

%% @doc Start a new adt_simple app. Takes the module that implements our
%% callbacks, an optional state of the app and an optional peer list.
start(CallbackMod) ->
	start(CallbackMod, []).
start(CallbackMod, AppState) ->
	start(CallbackMod, AppState, []).
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

report(PID) ->
	PID ! {get_report, self()},
	receive
		{report, Report} -> Report
	end.

%% @doc Stop the app.
stop(PID) ->
	PID ! stop.

%% @doc The main app server loop.
server(S = #state { gossip = GS }) ->
	%% Listen for gossip and normal messages.
	%% Recurse through the message box, updating one's state each time.
	receive
		{get_report, From} ->
			From ! {report, S},
			server(S);
		Msg when is_record(Msg, gs_msg) ->
			% We have received a gossip mesage. Use the library to process it.
			case ar_gossip:recv(GS, Msg) of
				% New tx received.
				{NewGS, {add_tx, TX}} ->
					server(
						apply_callback(
							S#state { gossip = NewGS },
							new_transaction,
							TX
						)
					);
				{NewGS, {move_tx_to_mining_pool, TX}} ->
					server(
						apply_callback(
							S#state { gossip = NewGS },
							new_transaction,
							TX
						)
					);
				% New block and confirmed txs callback.
				{NewGS, {new_block, _, _, B, _}} ->
					S1 =
						apply_callback(
							S#state { gossip = NewGS },
							new_block,
							B
						),
					S2 =
						lists:foldl(
							fun(TXID, NextS) ->
								apply_callback(
									NextS,
									confirmed_transaction,
									ar_storage:read_tx(TXID)
								)
							end,
							S1,
							B#block.txs
						),
					server(S2);
				% Ignore gossip message.
				{NewGS, _} ->
					server(S#state { gossip = NewGS })
			end;
		stop -> ok;
		OtherMsg ->
			server(apply_callback(S, message, OtherMsg))
	end.

%%% Utility functions

%% @doc Make a callback call. Returns the new gossip protocol state.
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
