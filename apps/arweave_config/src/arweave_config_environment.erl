%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Manage and store local environment variable.
%%%
%%% This module has been created to be a frontend around the local
%%% system environment variable. Environment variables are set
%%% read-only after a program is started. In this case, there is no
%%% point to call `os:getenv/0' and parse all values everytime. This
%%% module is getting environment variables, parses them and store
%%% them in an ETS table called `arweave_config_environment'.
%%%
%%% All environment variables are stored as binary to display them
%%% easily in debug mode or in JSON/YAML format.
%%%
%%% ```
%%%  _____________
%%% |             |
%%% | os:getenv/0 |
%%% |_____________|
%%%     /_ _\
%%%      | |
%%%      | | [arweave_config_environment:init/0]
%%%      | | [arweave_config_environment:reset/0]
%%%  ____| |_____________________              _____
%%% |                            \            (     )
%%% | arweave_config_environment |--[state]-->| ets |
%%% \____________________________|            (_____)
%%%      | |
%%%      | | [arweave_config_environment:load/0]
%%%     _| |_
%%%  ___\___/________
%%% |                |
%%% | arweave_config |
%%% |________________|
%%%
%%% '''
%%%
%%% == TODO ==
%%%
%%% @todo store the configuration spec in the process and modify the
%%% `get/0' function to return it.
%%%
%%% @todo creates `get_environment/0' and `get_environment/1' to
%%% retrieve one environment value.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_environment).
-behavior(gen_server).
-compile(warnings_as_errors).
-compile({no_auto_import,[get/0]}).
-export([load/0, get/0, get/1, reset/0]).
-export([start_link/0]).
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
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
-spec load() -> ok.

load() ->
	gen_server:call(?MODULE, load, 10_000).

%%--------------------------------------------------------------------
%% @doc returns the environment variables stored.
%% @end
%%--------------------------------------------------------------------
-spec get() -> [{binary(), binary()}].

get() ->
	ets:tab2list(?MODULE).

%%--------------------------------------------------------------------
%% @doc returns the environment variables stored.
%%--------------------------------------------------------------------
-spec get(Key) -> Return when
	Key :: binary(),
	Return :: {ok, binary()} | {error, term()}.

get(Key) ->
	case ets:lookup(?MODULE, Key) of
		[{Key, Value}] -> {ok, Value};
		_ -> {error, not_found}
	end.

%%--------------------------------------------------------------------
%% @doc reset the environment variable. Remove all environment
%% variables set and reload them from the environment. Mostly used for
%% development and testing purpose.
%% @end
%%--------------------------------------------------------------------
-spec reset() -> {ok, [{binary(), binary()}]}.

reset() ->
	gen_server:call(?MODULE, reset, 1000).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec init(any()) -> {ok, reference() | atom()}.

init(_) ->
	% list environment variables available on the system
	% when arweave is started. These variables will need
	% to be stored.
	Ets = ets:new(?MODULE, [named_table, protected]),
	handle_reset(),
	{ok, Ets}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(Msg = load, From, State) ->
	?LOG_DEBUG("received: ~p", [Msg, From]),
	Spec = arweave_config_spec:get_environments(),
	Mapping = [
		begin
			?LOG_DEBUG("found environment ~p=~p", [EnvKey,EnvValue]),
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
	{reply, ok, State};
handle_call(Msg = reset, From, State) ->
	?LOG_DEBUG("received: ~p", [Msg, From]),
	Result = handle_reset(),
	{reply, {ok, Result}, State};
handle_call(Msg, From, State) ->
	?LOG_WARNING([
		{module, ?MODULE},
		{function, handle_cast},
		{from, From},
		{message, Msg}
	]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
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

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_reset() ->
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
	].
