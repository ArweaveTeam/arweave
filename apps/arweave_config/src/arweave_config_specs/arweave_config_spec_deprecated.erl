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
%%% @doc Deprecated specification feature. Returns a warning message
%%% when a deprecated flag is present.
%%% @end
%%%===================================================================
-module(arweave_config_spec_deprecated).
-export([default/0, init/2]).
-include("arweave_config_spec.hrl").
-include_lib("kernel/include/logger.hrl").

default() -> false.

init(#{ deprecated := Deprecated }, State) when is_boolean(Deprecated) ->
	{ok, State#{ deprecated => Deprecated }};
init(Map, State) when is_map(Map) ->
	{ok, State#{ deprecated => default() }};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, deprecated, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ deprecated => default() }}
	end.

fetch(Module, State) ->
	try Module:deprecated() of
		false ->
			NewState = State#{ deprecated => default() },
			{ok, NewState};
		true ->
			NewState = State#{ deprecated => true },
			?LOG_WARNING("~p~n is deprecated", [Module]),
			{ok, NewState};
		{true, Message} ->
			NewState = State#{ deprecated => true },
			?LOG_WARNING("~p~n is deprecated: ~p", [Module, Message]),
			{ok, NewState};
		Elsewise ->
			{error, Elsewise}
	catch
		_:Reason ->
			{error, Reason}
	end.

