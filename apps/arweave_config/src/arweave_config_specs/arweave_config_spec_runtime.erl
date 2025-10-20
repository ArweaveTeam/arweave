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
%%% @doc Runtime Specification Definition.
%%%
%%% Runtime callback has been created to deal with different kind of
%%% parameters. Some are static and can be set only at startup. Others
%%% are dynamic and can be set during runtime.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec_runtime).
-export([init/2]).
-include("arweave_config_spec.hrl").

default() -> false.

init(_Map = #{ runtime := Runtime }, State) ->
	{ok, State#{ runtime => Runtime }};
init(Map, State) when is_map(Map) ->
	{ok, State#{ runtime => default() }};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, runtime, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ runtime => default() }}
	end.

init2(Module, State) ->
	try Module:runtime() of
		false ->
			NewState = State#{ runtime => false },
			{ok, NewState};
		true ->
			NewState = State#{ runtime => true },
			{ok, NewState};
		Elsewise ->
			{error, Elsewise}
	catch
		E:R:S ->
			{error, #{
					module => Module,
					state => State,
					error => E,
					reason => R,
					stack => S
				}
			}
	end.
