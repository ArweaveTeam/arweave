%% @doc Server to maintain a node state.
-module(ar_node_state).

-export([start/1, stop/1, insert/2, lookup/2]).

-include("ar.hrl").
-include("ar_node.hrl").

%% @doc Start a node state server. Pass a set of key/value tuples
%% for the state.
start(S) ->
	Tid = ets:new(ar_node_state, [set, private, {keypos, 1}]),
	ets:insert(Tid, S),
	Pid = spawn(fun() -> server(Tid) end),
	{ok, Pid}.

%% @doc Stop a node worker.
stop(Pid) ->
	Pid ! stop,
	ok.

%% @doc Set one or more values from state. The operation is atomic,
%% all needed values must be setted with one call. Between calls
%% the state may change.
insert(Pid, KeyValues) ->
	Pid ! {insert, KeyValues, self()},
	receive
		ok ->
			ok;
		{error, Error} ->
			{error, Error}
	after
		5000 ->
			{error, timeout}
	end.

%% @doc Get one or more values from state, the return is a list of
%% of values in the same order as the keys. Non-existant keys will
%% return 'false' as value. The operation is atomic, all needed values
%% must be retrieved with one call. Between calls the state may change.
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
handle(Tid, insert, KeyValues) when is_list(KeyValues) ->
	case lists:all(fun({Key, _Value}) when is_atom(Key) -> true end, KeyValues) of
		true ->
			ets:insert(Tid, KeyValues),
			ok;
		_ ->
			{error, {invalid_node_state_keys, KeyValues}}
	end;
handle(Tid, insert, {Key, Value}) ->
	handle(Tid, insert, [{Key, Value}]);
handle(_Tid, insert, Any) ->
	{error, {invalid_node_state_values, Any}};
handle(Tid, lookup, Keys) when is_list(Keys) ->
	Values = lists:map(
		fun
			(Key) when is_atom(Key) ->
				case ets:lookup(Tid, Key) of
					[{Key, Value}] -> Value;
					[]             -> undefined
				end;
			(Any) ->
				ar:d([{invalid_node_state_key, Any}]),
				{error, {invalid_node_state_key, Any}}
		end
	),
	{ok, Values};
handle(Tid, lookup, Key) ->
	handle(Tid, get, [Key]);
handle(_Tid, Command, _KeyValues) ->
	{error, {invalid_node_state_command, Command}}.
