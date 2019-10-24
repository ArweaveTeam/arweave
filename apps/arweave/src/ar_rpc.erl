-module(ar_rpc).

-export([ping/1, call/5, call_with_group_leader/4]).

ping(Node) ->
	net_adm:ping(node_name(Node)).

%% Use ar_rpc:call rather than builtin rpc:call so that logs from the process executing the handler
%% are marked with "** at node slave@127.0.0.1 **".
call(Node, Module, Func, Args, Timeout) ->
	rpc:call(node_name(Node), ar_rpc, call_with_group_leader, [erlang:group_leader(), Module, Func, Args], Timeout).

call_with_group_leader(MasterGroupLeader, Module, Func, Args) ->
	erlang:group_leader(MasterGroupLeader, self()),
	erlang:apply(Module, Func, Args).

node_name(master) -> 'master@127.0.0.1';
node_name(slave) -> 'slave@127.0.0.1'.
