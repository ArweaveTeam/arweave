-module(ar_content_policy_provider_sup).

-behaviour(supervisor).

%%%===================================================================
%%% API
%%%===================================================================

-export([start_link/0]).

%%%===================================================================
%%% Callbacks
%%%===================================================================

-export([init/1]).

%%%===================================================================
%%% Public API
%%%===================================================================

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init(Args) ->
	SupFlags = #{strategy => one_for_one, intensity => 10, period => 1},
	ChildSpec = #{id => ar_content_policy_provider, start => {ar_content_policy_provider, start_link, [Args]}},
	{ok, {SupFlags, [ChildSpec]}}.
