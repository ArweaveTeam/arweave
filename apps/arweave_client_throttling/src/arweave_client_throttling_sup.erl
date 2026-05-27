%%% @doc Supervisor for `arweave_client_throttling'.
%%%
%%% Starts one `arweave_client_throttling_group' worker per group
%%% spec returned by `arweave_client_throttling_config:get_groups/0'.
%%% @end
-module(arweave_client_throttling_sup).
-behaviour(supervisor).

-export([start_link/0, start_link/1]).
-export([init/1, all_info/0]).

-ifdef(AR_TEST).
-export([all_off/0, all_on/0]).
-endif.

start_link() ->
    start_link(arweave_client_throttling_config:get_groups()).

start_link(Groups) when is_list(Groups) ->
    arweave_client_throttling_metrics:register(),
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Groups]).

init([Groups]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    Children = [child_spec(G) || G <- Groups],
    {ok, {SupFlags, Children}}.

child_spec(#{id := ID} = Group) ->
    Normalized = arweave_client_throttling_config:normalize_group(Group),
    #{
        id => arweave_client_throttling_group:registered_name(ID),
        start => {arweave_client_throttling_group, start_link, [Normalized]},
        type => worker,
        restart => permanent,
        shutdown => 5000,
        modules => [arweave_client_throttling_group]
    }.

all_info() ->
    Config = arweave_client_throttling_config:get_groups(),
    [{ID, arweave_client_throttling_group:info(ID)}  || #{id := ID} <- Config].

all_off() ->
    Children = supervisor:which_children(?MODULE),
    [{ID, arweave_client_throttling_group:turn_off(ID)}  || {ID, _Child, _Type, _Modules} <- Children].

all_on() ->
    Children = supervisor:which_children(?MODULE),
    [{ID, arweave_client_throttling_group:turn_on(ID)}  || {ID, _Child, _Type, _Modules} <- Children].
