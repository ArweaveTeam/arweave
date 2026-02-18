%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Mathieu Kerjouan
%%% @author Arweave Team
%%% @doc Arweave Configuration Bootstrap module.
%%%
%%% This module is in charge to configure arweave parameters from
%%% different sources and in specific order. The only public interface
%%% is `start/1' function. All function prefixed by `init' are
%%% internal function callbacks.
%%%
%%% == Legacy Mode ==
%%%
%%% The legacy mode has been created to be compatible with the old
%%% static configuration format. The procedure has been a bit
%%% modified, but the execution path is globally the same.
%%%
%%% 1. load environment
%%%
%%% 2. find config_file parameter from arguments and load legacy
%%% configuration file
%%%
%%% 3. parse arguments and load them
%%%
%%% 4. switch to runtime mode.
%%%
%%% 5. start arweave
%%%
%%% == New Mode ==
%%%
%%% In the new configuration mode, every steps are modifying the
%%% stored configuration file in `arweave_config' step by step. The
%%% final step is to start arweave based on the final parsed
%%% configuration and in runtime mode.
%%%
%%% 1. set environment
%%%
%%% 2. set arguments
%%%
%%% 3. set configuration files if present in arguments
%%%
%%% 4. load configuration into arweave_config
%%%
%%% 5. switch to runtime mode
%%%
%%% 6. start arweave application and features.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_bootstrap).
-compile(warnings_as_errors).
-export([
	start/1,
	init_environment/1,
	init_config_file/1,
	init_arguments/1,
	init_load/1,
	init_runtime/1,
	init_final/1
]).
-include_lib("arweave_config/include/arweave_config.hrl").

%%--------------------------------------------------------------------
%% @doc Configure Arweave parameters from different sources.
%% @end
%%--------------------------------------------------------------------
-spec start(Args) -> Return when
	Args :: [string() | binary()],
	Return :: {ok, #config{}} | {error, term()}.

start(Args) ->
	% to ensure the compatibility with the legacy parsers, an
	% environment variable called AR_CONFIG_MODE can be set.
	% By default, the legacy format is used for now, but if an
	% user wants to switch to the new mode, this environment
	% variable needs to be set to "new".
	% @todo remove this environment variable when arweave_config
	% is fully operational.
	ArweaveConfigMode = os:getenv("AR_CONFIG_MODE"),
	Config = arweave_config_legacy:get(),
	State = #{
		mode => ArweaveConfigMode,
		config => Config,
		args => Args
	},

	% Let call the fsm loop.
	arweave_config_fsm:init(?MODULE, init_environment, State).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc init arweave configuration with environment variable.
%% @end
%%--------------------------------------------------------------------
init_environment(State = #{ mode := "new" }) ->
	arweave_config_environment:reset(),
	{next, init_arguments, State};
init_environment(State) ->
	arweave_config_environment:reset(),
	arweave_config_environment:load(),
	{next, init_config_file, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc init arweave configuration from configuration file.
%% @end
%%--------------------------------------------------------------------
init_config_file(State = #{ mode := "new" }) ->
	% the configuration file is directly loaded when it has been
	% found in the arguments.
	?LOG_WARNING("arweave_config will use new configuration format."),
	{next, init_load, State};
init_config_file(State = #{ args := Args, config := Config }) ->
	% @todo enable arweave_config_file_legacy.
	case ar_config:parse_config_file(Args, Config) of
		{ok, NewConfig} when is_record(NewConfig, config)  ->
			arweave_config_legacy:merge(Config),
			NewState = State#{
				config => NewConfig
			},
			{next, init_arguments, NewState};
		{error, Reason} ->
			{error, Reason}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc init arweave configuration from command line arguments.
%% @end
%%--------------------------------------------------------------------
init_arguments(State = #{ args := Args, mode := "new" }) ->
	?LOG_WARNING("arweave_config will use new argument format."),
	case arweave_config_arguments:set(Args) of
		{ok, _} ->
			{next, init_config_file, State};
		Else ->
			{error, Else}
	end;
init_arguments(State = #{ config := Config, args := Args }) ->
	case ar_cli_parser:parse(Args, Config) of
		{ok, NewConfig} ->
			arweave_config_legacy:set(NewConfig),
			NewState = State#{ config => NewConfig },
			{next, init_load, NewState};
		{error, Reason} ->
			{error, Reason};
		Else ->
			Else
	end.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init_load(State = #{ mode := "new" }) ->
	% in the new mode, this is where we defines which part of the
	% configuration is loaded before, between, environment,
	% arguments and configuration. Indeed, every part of the
	% configuration are being stored in individual processes. We
	% can merge them now
	arweave_config_environment:load(),
	arweave_config_file:load(),
	arweave_config_arguments:load(),
	{next, init_runtime, State};
init_load(State) ->
	{next, init_runtime, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc set arweave configuration in runtime mode to avoid setting
%% static parameters. Only dynamic parameters will be allowed to be
%% configured in this mode.
%% @end
%%--------------------------------------------------------------------
init_runtime(State) ->
	case arweave_config:runtime() of
		ok ->
			{next, init_final, State}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc finalize arweave configuration initialization.
%% @end
%%--------------------------------------------------------------------
init_final(_State = #{ mode := "new" })->
	% @todo this part of the code should not work like that.
	% there, we should retrieve all configuration using
	% Module:get/0 using the same format and them merging them
	% based on a specific order. The problem though, is to deal
	% with complex variable (like list).
	ok = arweave_config_environment:load(),
	ok = arweave_config_file:load(),
	ok = arweave_config_arguments:load(),
	LegacyConfig = arweave_config_legacy:get(),
	{ok, LegacyConfig};
init_final(_State = #{ config := Config }) ->
	% parse the arguments from command line and check if a
	% configuration file is defined, returns #config{} record.
	% Note: this function will halt the node and print helps if
	% the arguments or configuration file are wrong.
	% @todo: re-enable legacy parser
	% Config = ar_cli_parser:parse_config_file(Args)
	arweave_config_legacy:set(Config),
	{ok, Config}.
