-module(ar_limiter_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_sup.hrl").

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([]) ->
	{ok, {supervisor_spec(), children_spec()}}.

supervisor_spec() ->
	#{ strategy => one_for_all,
           intensity => 5,
           period => 10 }.

%%--------------------------------------------------------------------
%% Child spec generation based on Config.
%%--------------------------------------------------------------------
children_spec() ->
    [child_spec(Config) || Config <- get_limiter_configs()].

child_spec(#{id := Id}) ->
    WorkerConfig = #{}, %% Turn
    #{ id => Id,
       start => {ar_limiter, start_link, [Id, WorkerConfig]},
       type => worker,
       shutdown => ?SHUTDOWN_TIMEOUT}.

get_limiter_configs() ->
    %% TODO: get from ar_config.
    [#{id => general},
     #{id => metrics}].
