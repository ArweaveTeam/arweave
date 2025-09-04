%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_server).
-behavior(gen_server).
-compile(export_all).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
	{ok, []}.

handle_call(Msg, From, State) ->
	{noreply, State}.

handle_cast(Msg, State) ->
	{noreply, State}.

handle_info(Msg, State) ->
	{noreply, State}.
