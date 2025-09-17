%%%===================================================================
%%% @doc Check Specification Definition.
%%%
%%% A check function is an extension of `type'. Sometimes, a type is
%%% not enough, because for example, more rules are required. A path
%%% or an ipv4 address should be filtered, or modified if necessary.
%%% The check function will allow an explicit modification of the
%%% value configured.
%%%
%%% == TODO ==
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec_check).
-export([init/2]).
-include("arweave_config_spec.hrl").
	
default() -> undefined.

init(Map, State) when is_map(Map) ->
	case is_map_key(check, Map) of
		true ->
			{ok, State#{ check => maps:get(check, Map) }};
		false ->
			{ok, State#{ check => default() }}
	end;
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, check, 2) of
		true ->
			{ok, State#{ check => fun Module:check/2 }};
		false ->
			{ok, State#{ check => default() }}
	end.
