-module(ar_meta_db).
-export([start/0, get/1, put/2, keys/0]).
-compile({no_auto_import, [{get, 1}, {put, 2}]}).
-include_lib("eunit/include/eunit.hrl").

%%% Defines a small in-memory metadata table for Archain nodes.
%%% Typically used to store small peices of globally useful information
%%% (for example: the port number used by the node).

%% @doc Initialise the metadata storage service.
start() ->
	spawn(
		fun() ->
			ar:report([starting_meta_db]),
			ets:new(?MODULE, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	% Add a short wait to ensure that the table has been created
	% before returning.
	% TODO: Make this less gross.
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

%% @doc Return all of the keys available in the database.
keys() -> ets:foldl(fun({X, _}, Acc) -> Acc ++ [X] end, [], ?MODULE).

%% @doc Store and retreieve a test value.
basic_storage_test() ->
	put(test_key, 1),
	1 = get(test_key).
