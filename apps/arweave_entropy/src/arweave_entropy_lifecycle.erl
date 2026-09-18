%%% Order entropy activation and shutdown relative to its host users.
%%% Entropy workers remain children of the entropy application's supervisor.
-module(arweave_entropy_lifecycle).
-behaviour(gen_server).
-export([child_spec/0, start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

child_spec() ->
    #{
        id => arweave_entropy,
        start => {?MODULE, start_link, []},
        shutdown => infinity
    }.

start_link() ->
    gen_server:start_link(?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    {ok, PID} = arweave_entropy_sup:activate(),
    Ref = monitor(process, PID),
    {ok, Ref}.

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Message, State) ->
    {noreply, State}.

handle_info({'DOWN', Ref, process, _PID, Reason}, Ref) ->
    {stop, {entropy_runtime_stopped, Reason}, Ref};
handle_info(_Message, State) ->
    {noreply, State}.

terminate(_Reason, Ref) ->
    demonitor(Ref, [flush]),
    arweave_entropy_sup:deactivate().
