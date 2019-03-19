-module(ar_rpc).

-export([ping/1, call/5, start_server/0]).

ping(Shortname) ->
	net_adm:ping(node_name_from_short_name(Shortname)).

%% Use ar_rpc:call rather than builtin rpc:call so that logs are captured properly.
%% Otherwise, we do not see logs in the log file.
call(Shortname, Module, Func, Args, Timeout) ->
	Name = node_name_from_short_name(Shortname),
	Ref = make_ref(),
	{rpc_server, Name} ! {rpc_call, self(), Ref, Module, Func, Args},
	receive
		{reply, Ref, Reply} ->
			Reply
	after Timeout ->
		timeout
	end.

start_server() ->
	register(rpc_server, spawn(fun server/0)).

server() ->
	receive
		{rpc_call, From, Ref, Module, Func, Args} ->
			From ! {reply, Ref, erlang:apply(Module, Func, Args)},
			server()
	end.

node_name_from_short_name(Shortname) ->
	{ok, Hostname} = inet:gethostname(),
	list_to_atom(atom_to_list(Shortname) ++ "@" ++ Hostname).
