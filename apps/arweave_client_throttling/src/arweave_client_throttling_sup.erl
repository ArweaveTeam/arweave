%%% @doc Supervisor for `arweave_client_throttling'.
%%%
%%% Starts one `arweave_client_throttling_group' worker per group
%%% spec returned by `arweave_client_throttling_config:get_groups/0'.
%%% @end
-module(arweave_client_throttling_sup).
-behaviour(supervisor).

-export([start_link/0, start_link/1]).
-export([init/1, all_info/0]).

start_link() ->
    start_link(arweave_client_throttling_config:get_groups()).

start_link(Groups) when is_list(Groups) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Groups]).

init([Groups]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    Children = [child_spec(G) || G <- Groups],
    {ok, {SupFlags, Children}}.

child_spec(#{id := Id} = Group) ->
    Normalized = arweave_client_throttling_config:normalize_group(Group),
    #{
        id => {arweave_client_throttling_group, Id},
        start => {arweave_client_throttling_group, start_link, [Normalized]},
        type => worker,
        restart => permanent,
        shutdown => 5000,
        modules => [arweave_client_throttling_group]
    }.

all_info() ->
    Config = arweave_client_throttling_config:get_groups(),
    [{Id, arweave_client_throttling_group:info(Id)}  || #{id := Id} <- Config].
