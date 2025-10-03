%%%===================================================================
%%% @doc Manage and store local environment variable.
%%%
%%% This module has been created to be a frontend around the local
%%% system environment variable. Environment variables are set
%%% read-only after a program is started. In this case, there is no
%%% point to call `os:getenv/0' and parse all values everytime. This
%%% module is getting environment variables, parses them and store
%%% them in an ETS table called `arweave_config_environment'.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_environment).
-behavior(gen_server).
-export([load/0, get/0, reset/0]).
-export([start_link/0]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-compile({no_auto_import,[get/0]}).
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

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
%% @doc reset the environment variable. Remove all environment
%% variable set and reload them from the environment. Mostly used for
%% development and testing purpose.
%% @end
%%--------------------------------------------------------------------
reset() ->
	gen_server:cast(?MODULE, reset).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_) ->
	% list environment variables available on the system
	% when arweave is started. These variables will need
	% to be stored.
	Ets = ets:new(?MODULE, [named_table, protected]),
	reset(),
	{ok, Ets}.

init_test() ->
	{ok, Ets} = init([]),
	% All element of the ets should be in binary.
	[
	 	begin
			?assertEqual(true, is_binary(Key)),
			?assertEqual(true, is_binary(Value))
		end
		|| {Key, Value} <- ets:tab2list(Ets)
	].

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
terminate(_, _) -> ok.

%%--------------------------------------------------------------------
%% @hidden
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
%% @hidden
%%--------------------------------------------------------------------
handle_cast(load, State) ->
	Spec = arweave_config_spec:get_environments(),
	Mapping = [
		begin
			?LOG_DEBUG("found environment ~p", [EnvKey]),
			{Parameter, EnvValue}
		end
	||
		{EnvKey, EnvValue} <- get(),
		{EnvSpec, Parameter} <- Spec,
		EnvSpec =:= EnvKey
	],
	lists:map(
		fun({Parameter, Value}) ->
			arweave_config:set(Parameter, Value)
		end,
		Mapping
	),
	{noreply, State};
handle_cast(reset, State) ->
	% Environments are list of string. They must be at least
	% splitted in half using '=' separator. the left part is the
	% key, the right part is the value. Environments are static,
	% they can't be modified during runtime, then, keeping them
	% inside an ETS already parsed to be reused later will avoid
	% some friction in the future.
	ets:delete_all_objects(?MODULE),
	_Environment = [
		begin
			[K,V] = re:split(E, "=", [{parts, 2}, {return, list}]),
			BK = list_to_binary(K),
			VK = list_to_binary(V),
			ets:insert(?MODULE, {BK, VK}),
			{BK,VK}
		end ||
		E <- os:getenv()
	],
	{noreply, State};
handle_cast(Msg, State) ->
	?LOG_WARNING([
		{module, ?MODULE},
		{function, handle_cast},
		{message, Msg}
	]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(Msg, State) ->
	?LOG_WARNING([
		{module, ?MODULE},
		{function, handle_cast},
		{message, Msg}
	]),
	{noreply, State}.
