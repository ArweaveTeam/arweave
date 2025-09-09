%%%===================================================================
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_arguments).
-behavior(gen_server).
-export([load/0]).
-export([start_link/0]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("kernel/include/logger.hrl").

load() ->
	todo.

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_) ->
	% 1. get the list of arguments from the entry-point
	% 2. parse arguments and validate them using specifications
	% 3. configure parameters
	% 4. if everything is find, stop.
	{stop, normal}.

terminate(_, _) -> ok.

handle_call(_, _, State) -> {noreply, State}.

handle_cast(_, State) -> {noreply, State}.

handle_info(_, State) -> {noreply, State}.
