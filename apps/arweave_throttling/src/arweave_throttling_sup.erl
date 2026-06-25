%%% @doc Supervisor for `arweave_throttling'.
%%%
%%% Starts one `arweave_throttling_group' worker per group
%%% @end
-module(arweave_throttling_sup).
-behaviour(supervisor).

-export([start_link/0, start_link/1]).
-export([init/1, all_info/0]).

-ifdef(AR_TEST).
-export([reset_all/0, all_off/0, all_on/0]).
-endif.

-include_lib("arweave/include/ar_sup.hrl").

%% API
start_link() ->
    start_link(arweave_config:client_throttling_groups()).

start_link(GroupIDs) when is_list(GroupIDs) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [GroupIDs]).

all_info() ->
    [{ID, arweave_throttling_group:info(ID)}  ||
        ID <- arweave_config:client_throttling_groups()].

%% Supervisor callbacks
init([GroupIDs]) ->
    ok = arweave_throttling_metrics:register(),
    {ok, {supervisor_spec(), children_spec(GroupIDs)}}.

supervisor_spec() ->
    #{ strategy => one_for_all,
       intensity => 5,
       period => 10 }.

%% Child spec
children_spec(GroupIDs) ->
    lists:flatten([children_spec_per_group(ID) || ID <- GroupIDs]).

children_spec_per_group(GroupID) ->
    Initial = arweave_config:get([client_throttling, GroupID, initial_remaining]),
    MaxQueueLength = arweave_config:get([client_throttling, GroupID, max_queue_length]),
    ConcurrencyWindowMS = arweave_config:get([client_throttling, GroupID, concurrency_window_ms]),
    Spec = #{id => GroupID,
             initial_remaining => Initial,
             max_queue_length => MaxQueueLength,
             concurrency_window_ms => ConcurrencyWindowMS
            },
    [#{
       id => arweave_throttling_group:registered_name(GroupID),
       start => {arweave_throttling_group, start_link, [Spec]},
       type => worker,
       shutdown => ?SHUTDOWN_TIMEOUT
      }].

%% Only used in tests
-ifdef(AR_TEST).
reset_all() ->
    [{ID, arweave_throttling_group:reset(ID)}  || ID <- arweave_config:client_throttling_groups()].

all_off() ->
    Children = supervisor:which_children(?MODULE),
    [{ID, arweave_throttling_group:turn_off(ID)}  || {ID, _Child, _Type, _Modules} <- Children].

all_on() ->
    Children = supervisor:which_children(?MODULE),
    [{ID, arweave_throttling_group:turn_on(ID)}  || {ID, _Child, _Type, _Modules} <- Children].
-endif.
