%%%===================================================================
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
	
default() -> undefined.

init(Map, State) when is_map(Map) -> {ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, type, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ type => default() }}
	end.

fetch(Module, State) ->
	try erlang:apply(Module, type, []) of
		T when is_atom(T) ->
			{ok, State#{ type => T }};
		Elsewise ->
			{error, Elsewise}
	catch
		_:R ->
			{error, R}
	end.
