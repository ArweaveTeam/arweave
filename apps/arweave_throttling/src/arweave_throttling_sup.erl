%%% @doc Supervisor for `arweave_throttling'.
%%%
%%% Starts one `arweave_throttling_group' worker per group
%%% @end
-module(arweave_throttling_sup).
-behaviour(supervisor).

-export([start_link/0, all_info/0, start_throttling_group/1, reset_peer_in_all_groups/1,
         count_running/0]).
-export([init/1]).

-ifdef(AR_TEST).
-export([reset_all/0, all_off/0, all_on/0]).
-endif.

-include_lib("arweave/include/ar_sup.hrl").

-define(PMAP_TIMEOUT, 1000).

%% API
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

all_info() ->
    lists:filtermap(fun group_info/1, running_groups()).

reset_peer_in_all_groups(Peer) ->
    Groups = running_groups(),
    arweave_util:pmap(fun(GroupID) -> arweave_throttling_group:reset_peer(GroupID, Peer) end,
         Groups, ?PMAP_TIMEOUT).

%% @doc Number of running group processes; groups stopped after being
%% idle are not counted.
count_running() ->
    proplists:get_value(active, supervisor:count_children(?MODULE)).

%% Supervisor callbacks
init([]) ->
    {ok, {supervisor_spec(), []}}.

supervisor_spec() ->
    #{ strategy => one_for_one,
    intensity => 5,
    period => 10 }.

%% @doc Start the group process for `GroupID'. A group that stopped
%% itself after being idle keeps its child spec, so it is restarted.
start_throttling_group(GroupID) ->
    ChildSpec = child_spec_for_group(GroupID),
    case supervisor:start_child(?MODULE, ChildSpec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid};
        {error, already_present} ->
            restart_throttling_group(maps:get(id, ChildSpec));
        {error, _Reason} = E ->
            E
    end.

restart_throttling_group(ChildID) ->
    case supervisor:restart_child(?MODULE, ChildID) of
        {ok, Pid} ->
            {ok, Pid};
        {error, running} ->
            %% Restarted concurrently by another caller.
            arweave_throttling_process:get(ChildID);
        {error, _Reason} = E ->
            E
    end.

%% Child spec. Groups are `transient': a group that stops itself after
%% being idle exits with `normal' and is not restarted by the supervisor.
child_spec_for_group(GroupID) when is_binary(GroupID) ->
    child_spec_for_group(binary_to_list(GroupID));
child_spec_for_group(GroupID) when is_list(GroupID) ->
    Spec = #{id => GroupID},
    #{
         id => GroupID,
         start => {arweave_throttling_group, start_link, [Spec]},
         restart => transient,
         type => worker,
         shutdown => ?SHUTDOWN_TIMEOUT
     }.

%% Groups with a live process; excludes groups stopped after being idle.
running_groups() ->
    [ID || {ID, Pid, _Type, _Modules} <- supervisor:which_children(?MODULE),
           is_pid(Pid)].

%% A group may stop after being idle between listing and the call.
group_info(GroupID) ->
    case catch arweave_throttling_group:info(GroupID) of
        #{} = Info -> {true, {GroupID, Info}};
        _ -> false
    end.

%% Only used in tests
-ifdef(AR_TEST).
reset_all() ->
    [{ID, arweave_throttling_group:reset(ID)} || ID <- running_groups()].

all_off() ->
    [{ID, arweave_throttling_group:turn_off(ID)} || ID <- running_groups()].

all_on() ->
    [{ID, arweave_throttling_group:turn_on(ID)} || ID <- running_groups()].
-endif.

