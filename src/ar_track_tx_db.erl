-module(ar_track_tx_db).
-export([start/0, remove_bad_txs/1]).
-compile({no_auto_import, [{get, 1}, {put, 2}]}).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
%%% Defines a small in-memory metadata table for Archain nodes.
%%% Typically used to store small peices of globally useful information
%%% (for example: the port number used by the node).

%% @doc Initialise the metadata storage service.
start() ->
	spawn(
		fun() ->
			ar:report([starting_track_tx_db]),
			ets:new(?MODULE, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	% Add a short wait to ensure that the table has been created
	% before returning.
	receive after 250 -> ok end.


%% @doc Filter out transactions that have been in nodes state for too long
remove_bad_txs(TXs) ->
	lists:filter(
		fun(T) ->
			case ets:lookup(?MODULE, T#tx.id) of
				[{_Key, 0}] -> 
                    ets:delete(?MODULE, T#tx.id),
                    false;
				[{Key, Value}] -> 
					ets:insert(?MODULE, {Key, Value-1}),
					true;
				_ -> 
					ets:insert(?MODULE, {T#tx.id, 3}),
					true
			end
		end,
		TXs
	).