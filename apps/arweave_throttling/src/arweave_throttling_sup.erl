%%% @doc Supervisor for `arweave_throttling'.
%%%
%%% Starts one `arweave_throttling_group' worker per group
%%% @end
-module(arweave_throttling_sup).
-behaviour(supervisor).

-export([start_link/0, all_info/0, start_throttling_group/1]).
-export([init/1]).

-ifdef(AR_TEST).
-export([reset_all/0, all_off/0, all_on/0]).
-endif.

-include_lib("arweave/include/ar_sup.hrl").

%% API
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

all_info() ->
    Children = supervisor:which_children(?MODULE),
    [{worker_to_group(ID), arweave_throttling_group:info(worker_to_group(ID))}  || {ID, _Child, _Type, _Modules} <- Children].

%% Supervisor callbacks
init([]) ->
    ok = arweave_throttling_metrics:register(),
    ok = arweave_throttling_router:init(),
    ok = arweave_throttling_distinct_group:init(),
    {ok, {supervisor_spec(), []}}.

supervisor_spec() ->
    #{ strategy => one_for_all,
       intensity => 5,
       period => 10 }.

start_throttling_group(GroupID) ->
    supervisor:start_child(?MODULE, child_spec_for_group(GroupID)).

%% Child spec
child_spec_for_group(GroupID) ->
    Spec = #{id => GroupID},
    #{
       id => arweave_throttling_group:registered_name(GroupID),
       start => {arweave_throttling_group, start_link, [Spec]},
       type => worker,
       shutdown => ?SHUTDOWN_TIMEOUT
     }.

%% Only used in tests
-ifdef(AR_TEST).
reset_all() ->
    Children = supervisor:which_children(?MODULE),
    [{worker_to_group(ID), arweave_throttling_group:reset(worker_to_group(ID))}  || {ID, _Child, _Type, _Modules} <- Children].

all_off() ->
    Children = supervisor:which_children(?MODULE),
    [{ID, arweave_throttling_group:turn_off(ID)}  || {ID, _Child, _Type, _Modules} <- Children].

all_on() ->
    Children = supervisor:which_children(?MODULE),
    [{ID, arweave_throttling_group:turn_on(ID)}  || {ID, _Child, _Type, _Modules} <- Children].
-endif.

worker_to_group(WorkerRef) ->
    Prefix = "arweave_throttling_group_",
    list_to_atom(string:prefix(atom_to_list(WorkerRef), Prefix)).
