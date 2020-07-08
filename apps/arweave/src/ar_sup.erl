-module(ar_sup).

-behaviour(supervisor).

-export([
	start_link/0,
	init/1
]).

start_link() ->
	supervisor:start_link({local, ar}, ?MODULE, []).

init([]) ->
	MaxRestart = 0,
	MaxTime = 1,
	{ok, {{one_for_one, MaxRestart, MaxTime}, []}}.
