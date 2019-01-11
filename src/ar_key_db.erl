-module(ar_key_db).
-export([start/0, get/1, put/2, remove/1]).
-compile({no_auto_import, [{get, 1}, {put, 2}]}).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
%%% Database for storing decryption keys from encrypted recall blocks. The
%%% entries has a TTL. The DB is a singleton.

%% @doc Create the DB. This will fail if a DB already exists.
start() ->
	spawn(
		fun() ->
			ar:report([starting_key_db]),
			ets:new(?MODULE, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	% Add a short wait to ensure that the table has been created
	% before returning.
	receive after 250 -> ok end.

%% @doc Write a term into the DB. It will be deleted after 30 minutes.
put(Key, Val) ->
	ets:insert(?MODULE, {Key, Val}),
	timer:apply_after(30 * 60 * 1000, ?MODULE, remove, [Key]).

%% @doc Retreive a term from the DB.
get(Key) ->
	case ets:lookup(?MODULE, Key) of
		[{Key, Obj}] -> Obj;
		[] -> not_found
	end.

remove(Key) ->
	ets:delete(?MODULE, Key).
