-module(ar_meta_db).
-export([start/0, get/1, put/2, keys/0, remove_old/1,remove_old/2, increase/2]).
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
			ar:report([starting_meta_db]),
			ets:new(?MODULE, [set, public, named_table]),
			ets:new(blacklist, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	% Add a short wait to ensure that the table has been created
	% before returning.
	receive after 250 -> ok end.

%% @doc Put an Erlang term into the meta DB. Typically these are
%% write-once values.
put(Key, Val) -> ets:insert(?MODULE, {Key, Val}).
%% @doc Retreive a term from the meta db.
get(Key) ->
	case ets:lookup(?MODULE, Key) of
		[{Key, Obj}] -> Obj;
		[] -> not_found
	end.

%% @doc Increase the value associated by a key by Val
increase(Key, Val) ->
	put(Key, get(Key) + Val).

%% @doc Remove entries from the performance database older than
%% ?PEER_TMEOUT
remove_old(Time) ->
	case ets:first(?MODULE) of
		'$end_of_table' -> done;
		Key ->
			[{_, P}] = ets:lookup(?MODULE, Key),
			if (P#performance.timeout == 0) ->
				remove_old(Time, Key);
			true ->
				if (Time - P#performance.timeout) >= ?PEER_TIMEOUT ->
					remove_old(Time, Key),
					ets:delete(?MODULE, Key);
				true ->
					remove_old(Time, Key)
				end
			end
	end.
remove_old(Time, H) ->
	case ets:next(?MODULE, H) of
		'$end_of_table' -> done;
		Key ->
			[{_, P}] = ets:lookup(?MODULE, Key),
			if (P#performance.timeout == 0) ->
				remove_old(Time, Key);
			true ->
				if
					(Time - P#performance.timeout) >= ?PEER_TIMEOUT ->
					remove_old(Time, Key),
					ets:delete(?MODULE, Key);
				true ->
					remove_old(Time, Key)
				end
			end
	end.

%% @doc Return all of the keys available in the database.
keys() -> ets:foldl(fun({X, _}, Acc) -> Acc ++ [X] end, [], ?MODULE).

%% @doc Store and retreieve a test value.
basic_storage_test() ->
	put(test_key, 1),
	1 = get(test_key).

%% @doc Data older than ?PEER_TIMEOUT is removed, newer data is not
purge_old_peers_test() ->
		Time = os:system_time(seconds),
		P1 = #performance{timeout = Time - (?PEER_TIMEOUT + 1)},
		P2 = #performance{timeout = Time - 1},
		put({peer, {127,0,0,1,1984}}, P1),
		put({peer, {127,0,0,1,1985}}, P2),
		remove_old(Time),
		not_found = get({peer, {127,0,0,1,1984}}),
		P2 = get({peer, {127,0,0,1,1985}}).
