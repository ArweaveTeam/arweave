%%% @doc Process registry for throttling group processes.
%%%
%%% Map GroupIds to PIDs
%%% To make our lives easier we store GroupIDs as binaries, but allow strings
%%% to be passed as an argument as well.
-module(arweave_throttling_process).

-export([
         init/0,
         get/1,
         start_and_store/1,
         delete/1,
         cleanup/0
        ]).

init() ->
    ?MODULE = ets:new(?MODULE, [named_table, set, public, {read_concurrency, true}]),
    ok.

%% @doc Find process PID if it stored.
get(GroupID) when is_list(GroupID) ->
    %% We have this macro here, so the call is not ambigious, it's this module's
    %% get/1, not erlang:get/1.
    ?MODULE:get(list_to_binary(GroupID));
get(GroupID) when is_binary(GroupID) ->
    case ets:lookup(?MODULE, GroupID) of
        [{GroupID, Pid}] ->
            {ok, Pid};
        [] ->
            {error, group_not_found}
    end.

%% @doc Start a new process and store it for the Group.
start_and_store(GroupID) when is_list(GroupID) ->
    start_and_store(list_to_binary(GroupID));
start_and_store(GroupID) when is_binary(GroupID) ->
    maybe 
        {ok, Pid} ?= arweave_throttling_sup:start_throttling_group(GroupID),
        true ?= ets:insert(?MODULE, {GroupID, Pid}),
        {ok, Pid}
    end.

%% @doc Remove entry 
delete(GroupID) when is_list(GroupID) ->
    delete(list_to_binary(GroupID));
delete(GroupID) when is_binary(GroupID) ->
    ets:delete(?MODULE, GroupID).

%% @doc Delete ETS table, leaving no trace.
cleanup() ->
    catch ets:delete(?MODULE),
    ok.
