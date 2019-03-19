-module(ar_rpc).

-export([ping/1, call/5, call_with_group_leader/4]).

ping(Shortname) ->
	net_adm:ping(node_name_from_short_name(Shortname)).

%% Use ar_rpc:call rather than builtin rpc:call so that logs from the process executing the handler
%% are marked with "** at node slave@host **". Also, the function converts the short name into the node name.
call(Shortname, Module, Func, Args, Timeout) ->
	Name = node_name_from_short_name(Shortname),
	rpc:call(Name, ar_rpc, call_with_group_leader, [erlang:group_leader(), Module, Func, Args], Timeout).

call_with_group_leader(MasterGroupLeader, Module, Func, Args) ->
	erlang:group_leader(MasterGroupLeader, self()),
	erlang:apply(Module, Func, Args).

node_name_from_short_name(Shortname) ->
	{ok, Hostname} = inet:gethostname(),
	list_to_atom(atom_to_list(Shortname) ++ "@" ++ Hostname).
