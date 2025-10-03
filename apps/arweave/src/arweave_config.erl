%%%===================================================================
%%% @doc Arweave Configuration Interface.
%%%
%%% `arweave_config' module is an interface to the Arweave
%%% configuration data store where all configuration parameters are
%%% stored and specified.
%%%
%%% @end
%%%===================================================================
-module(arweave_config).
-export([
	get_env/0,
	set_env/1
]).

%%--------------------------------------------------------------------
%% @doc A wrapper for `application:get_env/2'.
%% @deprecated this function is a temporary interface and will be
%%             replaced by `arweave_config:get/1' function.
%% @see application:get_env/2
%% @end
%%--------------------------------------------------------------------
get_env() ->
	application:get_env(arweave, config).

%%--------------------------------------------------------------------
%% @doc A wrapper for `application:set_env/3'.
%% @deprecated this function is a temporary interface and will be
%%             replaced by `arweave_config:set/2' function.
%% @see application:set_env/3
%% @end
%%--------------------------------------------------------------------
set_env(Value) ->
	application:set_env(arweave, config, Value).

