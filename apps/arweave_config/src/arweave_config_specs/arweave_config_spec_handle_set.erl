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
%%% @doc set specification feature. adds a custom set specification
%%% inside a parameter.
%%% @end
%%%===================================================================
-module(arweave_config_spec_handle_set).
-export([init/2]).
-include("arweave_config_spec.hrl").

init(#{ handle_set := Set }, State) when is_function(Set, 4) ->
	{ok, State#{ set => Set, set_args => [] }};
init(#{ handle_set := {Set, Args}}, State) when is_function(Set, 4) ->
	{ok, State#{ set => Set, set_args => Args }};
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, handle_set, 4) of
		true ->
			NewState = State#{set => fun Module:handle_set/4},
			init_args(Module, NewState);
		false ->
			{ok, State}
	end.

init_args(Module, State) ->
	case is_function_exported(Module, set_args, 0) of
		true ->
			Args = Module:set_args(),
			NewState = #{ set_args => Args },
			{ok, NewState};
		_Else ->
			{ok, State}
	end.
