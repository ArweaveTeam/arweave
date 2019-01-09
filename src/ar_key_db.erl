-module(ar_key_db).
-export([start/0, get/1, put/2]).
-compile({no_auto_import, [{get, 1}, {put, 2}]}).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
%%% Defines a small in-memory metadata table for Arweave nodes.
%%% Typically used to store small peices of globally useful information
%%% (for example: the port number used by the node).

%% @doc Initialise the metadata storage service.
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

%% @doc Put an Erlang term into the meta DB. Typically these are
%% write-once values.
put(Key, Val) ->
    ets:insert(?MODULE, {Key, Val}),
    timer:apply_after(1800*1000, ?MODULE, remove, [Key]).
%% @doc Retreive a term from the meta db.
get(Key) ->
	case ets:lookup(?MODULE, Key) of
		[{Key, Obj}] -> Obj;
		[] -> not_found
	end.
