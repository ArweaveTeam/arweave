%%%===================================================================
%%% @doc nargs parameter interface from `argparse' module.
%%% @see argparse
%%% @end
%%%===================================================================
-module(arweave_config_spec_nargs).
-export([init/2]).
-include("arweave_config_spec.hrl").

init(Map = #{ nargs := Nargs }, State) ->
	check(Map, Nargs, State);
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, nargs, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State}
	end.

fetch(Module, State) ->
	Nargs = Module:nargs(),
	check(Module, Nargs, State).

check(Module, nonempty_list, State) ->
	{ok, State#{ nargs => nonempty_list }};
check(Module, list, State) ->
	{ok, State#{ nargs => list }};
check(Module, all, State) ->
	{ok, State#{ nargs => all }};
check(Module, 'maybe', State) ->
	{ok, State#{ nargs => 'maybe'}};
check(Module, {'maybe', Term}, State) ->
	{ok, State#{ nargs => {'maybe', Term}}};
check(Module, Nargs, State) when is_integer(Nargs), Nargs >= 0 ->
	{ok, State#{ nargs => Nargs }}.


