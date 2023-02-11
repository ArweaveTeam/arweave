-module(ar_p3).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() ->
    ?LOG_ERROR("start_link"),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ?LOG_ERROR("init"),
    {ok, state}.

handle_call(Request, From, State) ->
    ?LOG_ERROR("handle_call Request: ~p, From: ~p, State: ~p", [Request, From, State]),
    Reply = State,
    {reply, Reply, State}.

handle_cast(Message, State) ->
    ?LOG_ERROR("handle_cast Message: ~p, State: ~p", [Message, State]),
    NewState = State,
    {noreply, NewState}.

handle_info(Info, State) ->
    ?LOG_ERROR("handle_info Info: ~p, State: ~p", [Info, State]),
    NewState = State,
    {noreply, NewState}.

terminate(Reason, State) ->
    ?LOG_ERROR("terminate Reason: ~p, State: ~p", [Reason, State]),
    ok.
