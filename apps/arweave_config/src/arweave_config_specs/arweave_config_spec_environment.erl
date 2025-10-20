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
%%%
%%%===================================================================
-module(arweave_config_spec_environment).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(#{ environment := Env }, State) ->
	{ok, State#{ environment => Env }};
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, environment, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
fetch(Module, State) ->
	try
		Env = erlang:apply(Module, environment, []),
		check(Module, Env, State)
	catch
		_:R ->
			{error, R}
	end.

%%--------------------------------------------------------------------
%% @doc environment check callback.
%% @end
%%--------------------------------------------------------------------
-spec check(Module, Environment, State) -> Return when
	  Module :: atom() | map(),
	  Environment :: string() | binary(),
	  State :: map(),
	  Return :: {ok, State} | {error, map()}.

check(Module, Environment, State) when is_list(Environment) ->
	check(Module, list_to_binary(Environment), State);
check(Module, Environment, State) when is_binary(Environment) ->
	{ok, State#{ environment => Environment }};
check(Module, Env, State) ->
	{error, #{
			reason => {invalid, Env},
			module => Module,
			callback => environment,
			state => State
		}
	}.
