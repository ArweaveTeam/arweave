%%%===================================================================
%%% @doc Arweave configuration arguments manager.
%%%
%%% This module/process will be in charge to load/reset/configure the
%%% parameters using the command line arguments. Those arguments
%%% should be found after `--' and (in theory) available from
%%% `init:get_plain_arguments/0'. Unfortunately, because we are
%%% parsing those arguments using the `main/1' entry-point, the
%%% arguments cannot be found in the previous function.
%%%
%%% If the process is crashing, it should be able to recover its state
%%% without any problem.
%%%
%%% == Procedure ==
%%%
%%% 1. the application loads arguments and parse them using
%%%    `init:get_plain_arguments/0'.
%%%
%%% 2. if the application is receiving arguments from `load/1', they
%%%    will overwrite the arguments configured from
%%%    `init:get_plain_arguments/0'.
%%%
%%% 3. in case of reset, the whole arguments list is reset and
%%%    parameters  are fetch  from `init:get_plain_arguments/0'.  This
%%%    means, if arguments were configured from `load/1', they will be
%%%    lost.
%%%
%%% == TODO ==
%%%
%%% @end
%%%===================================================================
-module(arweave_config_arguments).
-behavior(gen_server).
-export([load/0, load/1, reset/0]).
-export([start_link/0]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
load() ->
	gen_server:cast(?MODULE, load).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
load(Arguments) ->
	gen_server:cast(?MODULE, {load, Arguments}).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
reset() ->
	gen_server:cast(?MODULE, reset).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_) ->
	% 1. get the list of arguments from the entry-point
	% 2. parse arguments and validate them using specifications
	% 3. configure parameters
	% 4. if everything is find, stop.
	{stop, normal}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
terminate(_, _) -> ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(_, _, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(_, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(_, State) -> {noreply, State}.
