%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @deprecated this module is a legacy compat layer.
%%% @doc Support for legacy arweave configuration.
%%%
%%% This module has been created to deal with legacy arguments parser
%%% from `ar.erl'. The goal is to slowly migrate to the new
%%% parser without breaking everything.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_arguments_legacy).
-behavior(gen_server).
-compile(warnings_as_errors).
-compile({no_auto_import,[get/0]}).
-export([
	 get/0,
	 get_args/0,
	 load/0,
	 parse/1,
	 set/1,
	 start_link/0
]).
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% local type definition.
%%--------------------------------------------------------------------
-type state() :: #{ args => [string()], config => #config{} }.

%%--------------------------------------------------------------------
%% @doc Uses `ar_cli_parser:parse/2' legacy parser to parse a list of
%% arguments. This is mostly an helper to get rid of the extra data
%% produced by this function.
%% @end
%%--------------------------------------------------------------------
-spec parse(Args) -> Return when
	Args :: [string()],
	Return :: {ok, #config{}} | {error, term()}.

parse(Args) ->
	try
		Config = #config{},
		ar_cli_parser:parse(Args, Config)
	of
		Result = {ok, _} -> Result;
		{error, _, _} -> {error, badarg};
		{error, _} -> {error, badarg};
		Else -> {error, Else}
	catch
		_Error:Reason ->
			{error, Reason}
	end.

%%--------------------------------------------------------------------
%% @doc Load parsed arguments into `arweave_config' process.
%%
%% ```
%% ok = arweave_config_arguments_legacy:load().
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec load() -> ok | {error, term()}.

load() ->
	gen_server:call(?MODULE, load, 10_000).

%%--------------------------------------------------------------------
%% @doc Set a new list of arguments, overwritting the old one if
%% present.
%%
%% ```
%% {ok, #config{}} = arweave_config_arguments_legacy:set([
%%   "debug"
%% ]).
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec set(Args) -> Return when
	Args :: [string()],
	Return :: {ok, #config{}} | {error, term()}.

set(Args) ->
	gen_server:call(?MODULE, {set, Args}, 10_000).

%%--------------------------------------------------------------------
%% @doc Returns the parsed arguments as `#config{}' record.
%%
%% ```
%% #config{} = arweave_config_arguments_legacy:get().
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec get() -> #config{}.

get() ->
	gen_server:call(?MODULE, get, 10_000).

%%--------------------------------------------------------------------
%% @doc Returns the arguments defined stored in
%% `arweave_config_arguments_legacy' process.
%%
%% ```
%% ["debug"] = arweave_config_arguments_legacy:get_args().
%% '''
%%
%% @end
%%--------------------------------------------------------------------
-spec get_args() -> [string()].

get_args() ->
	gen_server:call(?MODULE, {get, args}, 10_000).

%%--------------------------------------------------------------------
%% @doc Start `arweave_config_arguments_legacy' process.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec init(any()) -> {ok, state()}.

init(_) ->
	?LOG_INFO("start ~p  process", [?MODULE]),
	State = #{
		args => [],
		config => #config{}
	},
	{ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec handle_call
	({set, Args}, From, State) -> Return when
		Args :: [string()],
		From :: term(),
		State :: state(),
		Return :: {reply, Reply, State},
		Reply :: {ok, #config{}} | {error, term()};
	(get, From, State) -> Return when
		From :: term(),
		State :: state(),
		Return :: {reply, Reply, State},
		Reply :: #config{};
	({get, args}, From, State) -> Return when
		From :: term(),
		State :: state(),
		Return :: [string()];
	(load, From, State) -> Return when
		From :: term(),
		State :: state(),
		Return :: {reply, Reply, State},
		Reply :: ok | {error, term()};
	({merge, Config}, From, State) -> Return when
		Config :: #config{},
		From :: term(),
		State :: state(),
		Return :: {reply, Reply, State},
		Reply :: {ok, Config} | {error, term()};
	(term(), From, State) -> Return when
		From :: term(),
		State :: state(),
		Return :: {reply, ok, State}.

handle_call(Msg = {set, Args}, From, State) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	case parse(Args) of
		{ok, Config} ->
			NewState = State#{
				args => Args,
				config => Config
			},
			{reply, {ok, Config}, NewState};
		Else ->
			{reply, Else, State}
	end;
handle_call(Msg = get, From, State = #{ config := Config }) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, Config, State};
handle_call(Msg = {get,args}, From, State = #{ args := Args }) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	{reply, Args, State};
handle_call(Msg = load, From, State) ->
	?LOG_DEBUG([{message, Msg}, {from, From}]),
	handle_load(State);
handle_call(Msg, From, State) ->
	?LOG_WARNING([{process, self()}, {message, Msg}, {from, From}]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec handle_cast(any(), State) -> Return when
	State :: map(),
	Return :: {noreply, state()}.

handle_cast(Msg, State) ->
	?LOG_WARNING([{process, self()}, {message, Msg}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec handle_info(any(), State) -> Return when
	State :: map(),
	Return :: {noreply, state()}.

handle_info(Msg, State) ->
	?LOG_WARNING([{process, self()}, {message, Msg}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc this function is mostly a hack around legacy parameters.
%% Indeed, the current process is to let arweave_config set both
%% arweave_config and arweave_config_legacy from a list of parameters.
%% Because legacy parameters need to be loaded (and not all parameters
%% are supported yet), a way to set them is required. Legacy format is
%% "temporary" and will be removed after the complete migration to the
%% new format. The following code should be executed once anyway
%% (during arweave startup).
%% @end
%%--------------------------------------------------------------------
handle_load(State = #{ config := Config }) ->
	try
		% get the list of compatible legacy arguments
		SupportedMap = arweave_config_spec:get_legacy(),

		% convert the current configuration to a map
		ConfigMap = maps:from_list(
			arweave_config_legacy:config_to_proplist(Config)
		),

		% set arweave_config parameters one by one
		_ = maps:map(fun
			(LegacyKey, ParameterKey) ->
				case maps:get(LegacyKey, ConfigMap, undefined) of
					undefined ->
						undefined;
					Value ->
						arweave_config:set(ParameterKey, Value),
						Value
				end
			end,
			SupportedMap
		),

		handle_load2(State)
	catch
		_Error:Reason ->
			{reply, {error, Reason}, State}
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_load2(State = #{ config := Config }) ->
	try
		% to be sure the legacy configuration has been
		% configured, merge the current one.
		arweave_config_legacy:merge(Config)
	of
		{ok, C} ->
			{reply, {ok, C}, State};
		{error, Reason} ->
			{reply, {error, Reason}, State}
	catch
		_Error:Reason ->
			{reply, {error, Reason}, State}
	end.
