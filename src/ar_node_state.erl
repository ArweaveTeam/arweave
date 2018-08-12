%%%
%%% @doc Server to maintain a node state.
%%%

-module(ar_node_state).

-export([start/0, stop/1, all/1, lookup/2, update/2]).

%%%
%%% Public API.
%%%

%% @doc Start a node state server.
start() ->
	Pid = spawn(fun() ->
		server(ets:new(ar_node_state, [set, private, {keypos, 1}]))
	end),
	{ok, Pid}.

%% @doc Stop a node worker.
stop(Pid) ->
	Pid ! stop,
	ok.

%% @doc Get all values from state, the return is a map of the keys
%% and values. The operation is atomic, all needed values must be
%% retrieved with one call. Between calls the state may change.
all(Pid) ->
	Pid ! {all, all, self()},
	receive
		{ok, Values} ->
			{ok, Values};
		{error, Error} ->
			{error, Error}
	after
		5000 ->
			{error, timeout}
	end.

%% @doc Get one or more values from state, the return is a map of the
%% keys and values. Non-existant keys will return 'undefined' as value.
%% The operation is atomic, all needed values must be retrieved with
%% one call. Between calls the state may change.
lookup(Pid, Keys) ->
	Pid ! {lookup, Keys, self()},
	receive
		{ok, Values} ->
			{ok, Values};
		{error, Error} ->
			{error, Error}
	after
		5000 ->
			{error, timeout}
	end.

%% @doc Set one or more values from state, input is a list of {Key, Value}
%% or a map. The operation is atomic, all needed values must be setted with
%% one call. Between calls the state may change.
update(Pid, KeyValues) ->
	Pid ! {update, KeyValues, self()},
	receive
		ok ->
			ok;
		{error, Error} ->
			{error, Error}
	after
		5000 ->
			{error, timeout}
	end.

%%%
%%% Server functions.
%%%

%% @doc Main server loop.
server(Tid) ->
	receive
		{Command, KeyValues, Sender} ->
			try handle(Tid, Command, KeyValues) of
				Result ->
					Sender ! Result,
					server(Tid)
			catch
				throw:Term ->
					ar:report( [ {'NodeStateEXCEPTION', {Term} } ]),
					server(Tid);
				exit:Term ->
					ar:report( [ {'NodeStateEXIT', Term} ] ),
					server(Tid);
				error:Term ->
					ar:report( [ {'NodeStateERROR', {Term, erlang:get_stacktrace()} } ]),
					server(Tid)
			end;
		stop ->
			ok
	end.

%% @doc Handle the individual server commands. Starving ets table has to be
%% avoided by any means. Only atoms are allowed as keys and changes have
%% to be done atomically.
handle(Tid, all, all) ->
	All = ets:match_object(Tid, '$1'),
	{ok, maps:from_list(All)};
handle(Tid, lookup, Keys) when is_list(Keys) ->
	case lists:all(fun is_atom/1, Keys) of
		true ->
			{ok, maps:from_list(lists:map(fun(Key) ->
				case ets:lookup(Tid, Key) of
					[{Key, Value}] -> {Key, Value};
					[]             -> {Key, undefined}
				end
			end, Keys))};
		_ ->
			{error, {invalid_node_state_keys, Keys}}
	end;
handle(Tid, lookup, Key) ->
	handle(Tid, lookup, [Key]);
handle(_Tid, update, []) ->
	ok;
handle(Tid, update, KeyValues) when is_list(KeyValues) ->
	case lists:all(fun({Key, _Value}) -> is_atom(Key) end, KeyValues) of
		true ->
			ets:insert_new(Tid, KeyValues),
			ok;
		_ ->
			{error, {invalid_node_state_keys, KeyValues}}
	end;
handle(Tid, update, {Key, Value}) ->
	handle(Tid, update, [{Key, Value}]);
handle(Tid, update, KeyValues) when is_map(KeyValues) ->
	handle(Tid, update, maps:to_list(KeyValues));
handle(_Tid, update, Any) ->
	{error, {invalid_node_state_values, Any}};
handle(_Tid, Command, _KeyValues) ->
	{error, {invalid_node_state_command, Command}}.

%%%
%%% EOF
%%%

