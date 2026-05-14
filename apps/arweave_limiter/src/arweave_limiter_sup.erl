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
start_link() ->
    start_link(arweave_limiter_config:get_config()).

start_link(Config) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Config]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([Config]) ->
    ok = arweave_limiter_metrics:register(),
    {ok, {supervisor_spec(Config), children_spec(Config)}}.

supervisor_spec(_Config) ->
    #{ strategy => one_for_all,
       intensity => 5,
       period => 10 }.

%%--------------------------------------------------------------------
%% Child spec generation based on Config.
%%--------------------------------------------------------------------
children_spec(Configs) ->
    lists:flatten([children_spec_per_group(Config) || Config <- Configs]).

children_spec_per_group(#{id := Id} = Config) ->
    NumberOfWorkers = arweave_limiter_config:get_number_of_workers(Id),
    n_child_spec(Config, NumberOfWorkers).

n_child_spec(#{id := Id} = Config, NumberOfWorkers) ->
    lists:foldl(
      fun(N, Acc) ->
              Name = arweave_limiter_util:worker_name(Id, N),
              [single_child_spec(Name, Config)|Acc]
      end, [], lists:seq(0, NumberOfWorkers-1)).

single_child_spec(Name, Config) ->
    #{ id => Name,
       start => {arweave_limiter_group, start_link, [Name, Config]},
       type => worker,
       shutdown => ?SHUTDOWN_TIMEOUT}.

all_info() ->
    Config = arweave_limiter_config:get_config(),
    [{Id, arweave_limiter_group:info(Id)}  || #{id := Id} <- Config].

reset_all() ->
    Children = supervisor:which_children(?MODULE),
    [{Id, arweave_limiter_group:reset_all(Id)}  || {Id, _Child, _Type, _Modules} <- Children].
