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
%%% @doc get specification feature. insert a custom get feature inside
%%% a parameter.
%%% @end
%%%===================================================================
-module(arweave_config_spec_handle_get).
-export([init/2]).
-include("arweave_config_spec.hrl").
-include_lib("eunit/include/eunit.hrl").

init(#{ handle_get := Get }, State) when is_function(Get, 2) ->
	{ok, State#{ get => Get }};
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, handle_get, 2) of
		true ->
			{ok, State#{ get => fun Module:handle_get/2 }};
		false ->
			{ok, State}
	end.
