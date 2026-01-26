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
%%% @end
%%%===================================================================
-module(arweave_config_bootstrap).
-compile(warnings_as_errors).
-export([
	start/1,
	init/2,
	init_environment/2,
	init_config_file/2,
	init_arguments/2,
	init_runtime/2,
	init_final/2
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
		config => Config
	},
	init(Args, State).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc start initialize arweave configuration, this function will
%% parse arguments and configuration file. If everything is fine, then
%% arweave can be started.
%% @end
%%--------------------------------------------------------------------
-spec init(Args, State) -> Return when
	Args :: [string() | binary()],
	State :: map(),
	Return :: {ok, #config{}} | {error, term()}.

init(Args, State) ->
	case do_loop(init_environment, Args, State) of
		{ok, #{ config := Config }} ->
			{ok, Config};
		Else ->
			Else
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc init arweave configuration with environment variable.
%% @end
%%--------------------------------------------------------------------
init_environment(_Args, State) ->
	arweave_config_environment:reset(),
	arweave_config_environment:load(),
	{next, init_config_file, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc init arweave configuration from configuration file.
%% @end
%%--------------------------------------------------------------------
init_config_file(_Args, State = #{ mode := "new" }) ->
	% @todo enable arweave_config_file.
	?LOG_WARNING("arweave_config does not support config file."),
	{next, init_arguments, State};
init_config_file(Args, State) ->
	% @todo enable arweave_config_file_legacy.
	case ar_config:parse_config_file(Args) of
		{ok, Config} when is_record(Config, config)  ->
			arweave_config_legacy:merge(Config),
			{next, init_arguments, State};
		{error, Reason, _} ->
			{error, Reason};
		{error, Reason} ->
			{error, Reason}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc init arweave configuration from command line arguments.
%% @end
%%--------------------------------------------------------------------
init_arguments(Args, State = #{ mode := "new" }) ->
	?LOG_WARNING("arweave_config will use new argument format."),
	case arweave_config_arguments:set(Args) of
		{ok, _} ->
			case arweave_config_arguments:load() of
				ok ->
					{next, init_runtime, State};
				Else ->
					{error, Else}
			end;
		Else ->
			{error, Else}
	end;
init_arguments(Args, State) ->
	case arweave_config_arguments_legacy:set(Args) of
		{ok, _} ->
			arweave_config_arguments_legacy:load(),
			{next, init_runtime, State};
		Else ->
			{error, Else}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc set arweave configuration in runtime mode to avoid setting
%% static parameters. Only dynamic parameters will be allowed to be
%% configured in this mode.
%% @end
%%--------------------------------------------------------------------
init_runtime(_Args, State) ->
	case arweave_config:runtime() of
		ok ->
			{next, init_final, State};
		Else ->
			{error, Else}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc finalize arweave configuration initialization.
%% @end
%%--------------------------------------------------------------------
init_final(_Args, State) ->
	% parse the arguments from command line and check if a
	% configuration file is defined, returns #config{} record.
	% Note: this function will halt the node and print helps if
	% the arguments or configuration file are wrong.
	% @todo: re-enable legacy parser
	% Config = ar_cli_parser:parse_config_file(Args)
	Config = arweave_config_legacy:get(),
	NewState = State#{
		config => Config
	},
	{ok, NewState}.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc main loop where all callbacks are executed.
%% @end
%%--------------------------------------------------------------------
-spec do_loop(Callback, Args, State) -> Return when
	Callback :: atom(),
	Args :: [string() | binary()],
	State :: map(),
	Return ::
		{ok, State} |
		{next, Callback, State} |
		{error, Reason},
	Reason :: term().

do_loop(Callback, Args, State) ->
	try
		?LOG_DEBUG("bootstrap ~p", [Callback]),
		erlang:apply(?MODULE, Callback, [Args, State])
	of
		{ok, NewState} ->
			{ok, NewState};
		{next, NextCallback, NewState} ->
			do_loop(NextCallback, Args, NewState);
		{error, Reason} ->
			{error, Reason};
		{error, Reason, _State} ->
			{error, Reason}
	catch
		_Error:Reason ->
			{error, Reason}
	end.
