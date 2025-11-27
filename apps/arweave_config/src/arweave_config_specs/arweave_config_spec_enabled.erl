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
%%% @doc "Enabled" value specification
%%% @end
%%%===================================================================
-module(arweave_config_spec_enabled).
-export([init/2]).
-include("arweave_config_spec.hrl").
-include_lib("kernel/include/logger.hrl").

init(Map, State) when is_map(Map) ->
	fetch(Map, State);
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, enabled, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State}
	end.

fetch(#{ enabled := false }, State) ->
	PK = maps:get(parameter_key, State),
	?LOG_DEBUG("parameter_key: ~p disabled", [PK]),
	skip;
fetch(#{ enabled := {false, Reason} }, State) ->
	PK = maps:get(parameter_key, State),
	?LOG_DEBUG("parameter_key: ~p disabled (~p)", [PK, Reason]),
	skip;
fetch(_Module, State) ->
	{ok, State#{ enabled => true }}.

