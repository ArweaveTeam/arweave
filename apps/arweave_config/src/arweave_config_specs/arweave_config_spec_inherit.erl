%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%%------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc inherit specific values from another parameter.
%%% @end
%%%===================================================================
-module(arweave_config_spec_inherit).
-compile(warnings_as_errors).
-export([inherited_fields/0, init/2]).
-include("arweave_config_spec.hrl").
-include_lib("kernel/include/logger.hrl").

inherited_fields() ->
	[
		default,
		enabled,
		required,
		runtime,
		type
	].

init(Map, State)
	when is_map(Map); is_atom(Map) ->
		fetch(Map, State);
init(_, State) ->
	{ok, State}.

fetch(Map = #{ inherit := {Parameter, Fields}}, State)
	when is_list(Parameter); is_list(Fields) ->
		check(Map, {Parameter, Fields}, State);
fetch(Map = #{ inherit := Parameter }, State)
	when is_list(Parameter) ->
		Fields = inherited_fields(),
		check(Map, {Parameter, Fields}, State);
fetch(Map, State) when is_map(Map) ->
	{ok, State};
fetch(Module, State) ->
	try Module:inherit() of
		{Parameter, Fields} ->
			check(Module, {Parameter, Fields}, State);
		Parameter when is_list(Parameter) ->
			check(Module, {Parameter, inherited_fields()}, State);
		_Else ->
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

check(_, Inherit, State) ->
	{ok, State#{ inherit => Inherit }}.

