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
%%% @doc "Default" value specification. Returns a default value if
%%% specified.
%%% @end
%%%===================================================================
-module(arweave_config_spec_default).
-export([init/2]).
-include("arweave_config_spec.hrl").
-include_lib("kernel/include/logger.hrl").

init(Map, State) when is_map(Map) ->
	fetch(Map, State);
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, default, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State }
	end.

fetch(#{ default := DefaultValue }, State) ->
	{ok, State#{ default => DefaultValue }};
fetch(Map, State) when is_map(Map) ->
	{ok, State};
fetch(Module, State) ->
	try Module:default() of
		Default ->
			NewState = State#{ default => Default },
			{ok, NewState};
		Elsewise ->
			{ok, State}
	catch
		E:R:S ->
			?LOG_ERROR([
				{module, ?MODULE},
				{parameter, Module},
				{state, State},
				{error, {E,R,S}}
			]),
			{ok, State}
	end.

