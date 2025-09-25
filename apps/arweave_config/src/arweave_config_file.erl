%%%===================================================================
%%% @doc Arweave configuration file manager.
%%%
%%% This module/process is in charge to manage arweave configuration
%%% file(s).
%%%
%%% == Procedure ==
%%%
%%% 1. the process will check if a configuration file can be found
%%% from the actual parameter if defined. This parameter can be set at
%%% startup.
%%%
%%% 2. if a configuration file is found, then it will try to parse it
%%% and those the parameters locally
%%%
%%% 3. finally, it will set each parameter with their values.
%%%
%%% == TODO ==
%%%
%%% @end
%%%===================================================================
-module(arweave_config_file).
-behavior(gen_server).
-export([load/0, load/1, reset/0]).
-export([start_link/0, start_link/1]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
start_link() ->
	start_link(#{}).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, Args, []).

%%--------------------------------------------------------------------
%% @doc load a configuration file from path configured in the
%% parameters.
%% @end
%%--------------------------------------------------------------------
load() ->
	gen_server:cast(?MODULE, load).

%%--------------------------------------------------------------------
%% @doc load a configuration file from a specific path.
%% @end
%%--------------------------------------------------------------------
load(Path) ->
	gen_server:cast(?MODULE, {load, Path}).

%%--------------------------------------------------------------------
%% @doc reset the parameters from configuration file.
%% @end
%%--------------------------------------------------------------------
reset() ->
	gen_server:cast(?MODULE, reset).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(#{ parameter := Parameter }) ->
	% 1. the configuration file should be configured from the
	%    parameters (via environment or arguments), if not
	%    simply stop the execution. it means we don't use it.
	% 2. check if the configuration file is present
	% 3. check the configuration format, usually json
	% 4. parse it
	% 5. configure the parameters based on the parsed data
	% 6. this process could stay up to do the link between
	%    the configuration file and the parameters, in particular
	%    for the export/import/save function. For now, stopping
	%    it if everything is fine should do the job.
	{stop, normal};
init(_) -> {stop, normal}.

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
