-module(arweave_limiter_sup).
-behaviour(supervisor).

%% API
-export([start_link/0, all_info/0]).

-ifdef(AR_TEST).
-export([start_link/1, children_spec/1, children_spec_per_group/1, reset_all/0]).
-endif.

%% Supervisor callbacks
-export([init/1]).

-compile({nowarn_unused_function, [{reset_all, 0}]}).

-include_lib("arweave/include/ar_sup.hrl").
-include_lib("kernel/include/logger.hrl").

%% ===================================================================
%% API functions
%% ===================================================================

%% @doc Start the supervisor, building one child gen_server per
%% configured `[limiter, GroupID, number_of_workers]'. The list of groups and
%% per-group worker count come from `arweave_config'.
start_link() ->
    start_link(arweave_config:limiter_groups()).

%% @doc Test entry point — start with an explicit list of group IDs.
start_link(GroupIDs) when is_list(GroupIDs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [GroupIDs]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([GroupIDs]) ->
    ok = arweave_limiter_metrics:register(),
    {ok, {supervisor_spec(), children_spec(GroupIDs)}}.

supervisor_spec() ->
    #{ strategy => one_for_all,
       intensity => 5,
       period => 10 }.

%%--------------------------------------------------------------------
%% Child spec generation.
%%--------------------------------------------------------------------
children_spec(GroupIDs) ->
    lists:flatten([children_spec_per_group(ID) || ID <- GroupIDs]).

children_spec_per_group(GroupID) when is_atom(GroupID) ->
    NumberOfWorkers = arweave_config:get([limiter, GroupID, number_of_workers]),
    [single_child_spec(arweave_limiter_util:worker_name(GroupID, N), GroupID)
        || N <- lists:seq(0, NumberOfWorkers - 1)].

single_child_spec(Name, GroupID) ->
    #{ id => Name,
       start => {arweave_limiter_group, start_link, [Name, GroupID]},
       type => worker,
       shutdown => ?SHUTDOWN_TIMEOUT}.

all_info() ->
    [{ID, arweave_limiter_group:info(ID)}
        || ID <- arweave_config:limiter_groups()].

reset_all() ->
    Children = supervisor:which_children(?MODULE),
    [{ID, arweave_limiter_group:reset_all(ID)}
        || {ID, _Child, _Type, _Modules} <- Children].
