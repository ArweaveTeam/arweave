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
%%% @doc Configuration specification type.
%%%
%%% A type MUST BE defined somewhere. This is a way to reuse already
%%% used format on the system.
%%%
%%% A type CAN CONVERT a value to an internal Erlang term.
%%%
%%% ```
%%% % type boolean:
%%% boolean(<<"true">>) -> {ok, true}.
%%% boolean("true") -> {ok, true}.
%%% '''
%%%
%%% == TODO ==
%%%
%%% 1. Configures a default generic  type (e.g. any) returning always,
%%% `ok' it  should be defined in  this module or any  other. At this,
%%% time ,  he default value  is set to  `undefined', but this  is not
%%% coherent with the rest of the code.
%%%
%%% ```
%%% any(_, _) -> ok.
%%% '''
%%%
%%% 2. Configures a default generic type (e.g. none) returning always
%%% `error'.
%%%
%%% ```
%%% none(_, _) -> {error, none}.
%%% '''
%%%
%%% 3. Configure a custom Module/Function type callback:
%%%
%%% ```
%%% {Module, Function}
%%% '''
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec_type).
-export([init/2]).
-include("arweave_config_spec.hrl").
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

init(#{ type := Type }, State) ->
	{ok, State#{ type => Type }};
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, type, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State}
	end.

fetch(Module, State) ->
	try erlang:apply(Module, type, []) of
		T when is_atom(T) ->
			case is_function_exported(arweave_config_type, T, 1) of
				true ->
					{ok, State#{ type => T }};
				false ->
					?LOG_WARNING("non existing type ~p", [T]),
					{ok, State#{ type => T }}
			end;
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
