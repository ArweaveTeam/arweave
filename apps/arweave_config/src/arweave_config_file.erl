%%%===================================================================
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_file).
-behavior(gen_server).
-export([load/0]).
-export([start_link/0]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("kernel/include/logger.hrl").

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

load() ->
	todo.

init(_) ->
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
	{stop, normal}.

terminate(_, _) -> ok.

handle_call(_, _, State) -> {noreply, State}.

handle_cast(_, State) -> {noreply, State}.

handle_info(_, State) -> {noreply, State}.
