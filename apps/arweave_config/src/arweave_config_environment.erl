%%%===================================================================
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_environment).
-behavior(gen_server).
-export([load/0, get/0]).
-export([start_link/0]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-compile({no_auto_import,[get/0]}).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc start `arweave_config_environment' process.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc load environment variable into `arweave_config'.
%% @end
%%--------------------------------------------------------------------
load() ->
	gen_server:cast(?MODULE, load).

%%--------------------------------------------------------------------
%% @doc returns the environment variables stored.
%% @end
%%--------------------------------------------------------------------
get() ->
	ets:tab2list(?MODULE).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(_) ->
	% list environment variables available on the system
	% when arweave is started. These variables will need
	% to be stored.
	Ets = ets:new(?MODULE, [named_table, protected]),

	% Environments are list of string. They must be at least
	% splitted in half using '=' separator. the left part is the
	% key, the right part is the value. Environments are static,
	% they can't be modified during runtime, then, keeping them
	% inside an ETS already parsed to be reused later will avoid
	% some friction in the future.
	_Environment = [
		begin
			[K,V] = re:split(E, "=", [{parts, 2}, {return, list}]),
			ets:insert(Ets, {K, V}),
			{K,V}
		end ||
		E <- os:getenv()
	],
	{ok, Ets}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
terminate(_, _) -> ok.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_call(Msg, From, State) ->
	?LOG_WARNING([
		{module, ?MODULE},
		{function, handle_cast},
		{from, From},
		{message, Msg}
	]),
	{noreply, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_cast(load, State) ->
	lists:map(
		fun({Key, Value}) ->
			?LOG_DEBUG([
				{module, ?MODULE},
				{function, load},
				{key, Key},
				{value, Value}
			]),
			arweave_config_spec:set(environment, Key, Value)
		end,
		get()
	),
	{noreply, State};
handle_cast(Msg, State) ->
	?LOG_WARNING([
		{module, ?MODULE},
		{function, handle_cast},
		{message, Msg}
	]),
	{noreply, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
	?LOG_WARNING([
		{module, ?MODULE},
		{function, handle_cast},
		{message, Msg}
	]),
	{noreply, State}.
