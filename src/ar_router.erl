-module(ar_router).
-export([start/0]).
-export([register/0, register/1]).
-export([lookup/1]).
-include_lib("eunit/include/eunit.hrl").

%%% A routing process that allows processes from other Erlang nodes to send
%%% messages to local processes. This involves generating an ID for each
%%% remotely-addressable process, then allowing these processes to be found
%%% later.

%% Defines the state of the server.
-record(state, {
	db = []
}).

%% @doc Start and register the router for this node.
start() ->
	erlang:register(?MODULE, spawn(fun() -> server(#state {}) end)).

%% @doc Register a process (by default, self()) to be remotely addressable.
register() -> register(self()).
register(PID) ->
	?MODULE ! {register, self(), PID},
	receive {registered, PID, ID} -> ID end.

%% @doc Lookup a local process identifier or registered ID.
lookup(PID) when is_pid(PID) ->
	?MODULE ! {lookup, pid, self(), PID},
	receive {lookedup, PID, ID} -> ID end;
lookup(ID) ->
	?MODULE ! {lookup, id, self(), ID},
	receive {lookedup, ID, PID} -> PID end.

server(S = #state { db = DB }) ->
	receive
		{lookup, id, ReplyPID, ID} ->
			ReplyPID ! {lookedup, ID, find(id, ID, DB)},
			server(S);
		{lookup, pid, ReplyPID, PID} ->
			ReplyPID ! {lookedup, PID, find(pid, PID, DB)},
			server(S);
		{register, ReplyPID, PID} ->
			{ID, NewDB} = add_pid(DB, PID),
			ReplyPID ! {registered, PID, ID},
			server(S#state { db = NewDB })
	end.

%%% Utility functions.

%% @doc Either find the PID/ID, or return not_found.
find(id, ID, DB) ->
	case lists:keyfind(ID, 1, DB) of
		false -> not_found;
		{ID, PID} -> PID
	end;
find(pid, PID, DB) ->
	case lists:keyfind(PID, 2, DB) of
		false -> not_found;
		{ID, PID} -> ID
	end.

%% @doc Add a process identifier to the register, returning the new ID and database.
add_pid(DB, PID) ->
	case find(pid, PID, DB) of
		not_found ->
			ID = rand:uniform(10000000000000000),
			{ID, [{ID, PID}|DB]};
		ID -> {ID, DB}
	end.

%%% Tests

register_lookup_reregister_test() ->
	start(),
	PID = spawn(fun() -> receive ok -> ok end end),
	ID = register(PID),
	PID = lookup(ID),
	ID = lookup(PID),
	ID = register(PID).
