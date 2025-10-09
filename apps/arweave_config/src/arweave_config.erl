%%%===================================================================
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Configuration Interface.
%%%
%%% `arweave_config' module is an interface to the Arweave
%%% configuration data store where all configuration parameters are
%%% stored and specified.
%%%
%%% WARNING: this module/application is in active development.
%%%
%%% @end
%%%
%%% ------------------------------------------------------------------
%%%
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%===================================================================
-module(arweave_config).
-vsn(1).
-behavior(application).
-export([
	start/0,
	stop/0,
	get_env/0,
	set_env/1
]).
% application behavior callbacks.
-export([start/2, stop/1]).
-include("arweave_config.hrl").
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc helper function to started `arweave_config' application.
%% @end
%%--------------------------------------------------------------------
-spec start() -> ok | {error, term()}.

start() ->
	case application:ensure_all_started(?MODULE, permanent) of
		{ok, Dependencies} ->
			?LOG_DEBUG("arweave_config started dependencies: ~p", Dependencies),
			ok;
		Elsewise ->
			Elsewise
	end.

%%--------------------------------------------------------------------
%% @doc help function to stop `arweave_config' application.
%% @end
%%--------------------------------------------------------------------
-spec stop() -> ok.

stop() ->
	application:stop(?MODULE).

%%--------------------------------------------------------------------
%% @doc A wrapper for `application:get_env/2'.
%% @deprecated this function is a temporary interface and will be
%%             replaced by `arweave_config:get/1' function.
%% @see application:get_env/2
%% @end
%%--------------------------------------------------------------------
-spec get_env() -> {ok, #config{}}.

get_env() ->
	application:get_env(arweave_config, config).

%%--------------------------------------------------------------------
%% @doc A wrapper for `application:set_env/3'.
%% @deprecated this function is a temporary interface and will be
%%             replaced by `arweave_config:set/2' function.
%% @see application:set_env/3
%% @end
%%--------------------------------------------------------------------
-spec set_env(term()) -> ok.

set_env(Value) ->
	application:set_env(arweave_config, config, Value).

%%--------------------------------------------------------------------
%% @hidden
%% @doc `application' callback.
%% @end
%%--------------------------------------------------------------------
start(_StartType, _StartArgs) ->
	?LOG_INFO("arweave_config application starting"),

	% start application supervisor
	arweave_config_sup:start_link().

%%--------------------------------------------------------------------
%% @hidden
%% @doc `application' callback.
%% @end
%%--------------------------------------------------------------------
stop(_Args) ->
	?LOG_INFO("arweave_config application stopped"),
	ok.

