-module(ar_limiter_sup).
-behaviour(supervisor).

%% API
-export([start_link/0, all_info/0]).

-ifdef(TEST).
-export([start_link/1, child_spec/1]).
-endif.

%% Supervisor callbacks
-export([init/1]).

-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_sup.hrl").

%% ===================================================================
%% API functions
%% ===================================================================
start_link() ->
    start_link(get_limiter_config()).

start_link(Config) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [Config]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================
init([Config]) ->
	{ok, {supervisor_spec(Config), children_spec(Config)}}.

supervisor_spec(_Config) ->
	#{ strategy => one_for_all,
           intensity => 5,
           period => 10 }.

%%--------------------------------------------------------------------
%% Child spec generation based on Config.
%%--------------------------------------------------------------------
children_spec(Configs) ->
    [child_spec(Config) || Config <- Configs].

child_spec(#{id := Id} = Config) ->
    #{ id => Id,
       start => {ar_limiter, start_link, [Id, Config]},
       type => worker,
       shutdown => ?SHUTDOWN_TIMEOUT}.

get_limiter_config() ->
    %% TODO: get from ar_config.
    [#{id => general},
     %% Very limited concurrency defaults
     #{id => metrics,
       leaky_rate_limit=> 1,
       sliding_window_limit => 3,
       concurrency_limit => 2}].

all_info() ->
    Children = supervisor:which_children(?MODULE),
    [{Id, ar_limiter:info(Id)}  || {Id, _Child, _Type, _Modules} <- Children].
