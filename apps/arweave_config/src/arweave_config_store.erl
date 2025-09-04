%%%===================================================================
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_store).
-compile(export_all).
-behavior(gen_server).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init(_Args) ->
	erlang:process_flag(trap_exit, true),
	Ets = ets:new(?MODULE, [named_table, protected]),
	{ok, Ets}.

%%--------------------------------------------------------------------
%% ets:insert({["global", "debug"], {true, #{}}}).
%%--------------------------------------------------------------------
% handle_call({import, File, Format}, From, State) ->
%	{reply, ok, State};
% handle_call({export, json}, From, State) ->
%	List = ets:tab2list(?MODULE),
%	{reply, List, State};
handle_call({get, Key}, From, State) ->
	Return = ets:lookup(?MODULE, Key),
	{reply, {ok, Return}, State};
handle_call({add, Key, Value}, From, State) ->
	ets:insert(?MODULE, Key, {Value, #{}}),
	{reply, {ok, Value}, State};
handle_call({delete, Key}, From, State) ->
	Value = ets:take(?MODULE, Key),
	{reply, {ok, Value}, State};
handle_call(Msg, From, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
	{noreply, State}

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
	{noreply, State}.

