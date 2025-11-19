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
%%% @doc Environment specification module callback.
%%%
%%% This module is a module callback used by `arweave_config_spec'
%%% module to parse and validate environment variables.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec_environment).
-compile(warnings_as_errors).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(Map = #{ environment := Env }, State) ->
	check(Map, Env, State);
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
%% @hidden
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

check(_Module, true, State = #{ parameter_key := PK }) ->
	case parameter_key_to_env(PK) of
		{ok, Env} ->
			{ok, State#{ environment => Env }};
		Else ->
			Else
	end;
check(Module, Environment, State) when is_list(Environment) ->
	check(Module, list_to_binary(Environment), State);
check(_Module, Environment, State) when is_binary(Environment) ->
	{ok, State#{ environment => Environment }};
check(Module, Env, State) ->
	{error, #{
			reason => {invalid, Env},
			module => Module,
			callback => environment,
			state => State
		}
	}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc default environment variable prefix to use.
%% @end
%%--------------------------------------------------------------------
prefix() -> <<"AR">>.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
parameter_key_to_env(PK) ->
	parameter_key_to_env(PK, []).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
parameter_key_to_env([], Buffer) ->
	Reversed = lists:reverse(Buffer),
	Prefixed = [prefix()|Reversed],
	Joined = lists:join(<<"_">>, Prefixed),
	Upper = string:uppercase(Joined),
	{ok, list_to_binary(Upper)};
parameter_key_to_env([H|T], Buffer) when is_integer(H) ->
	parameter_key_to_env(T, [integer_to_binary(H)|Buffer]);
parameter_key_to_env([H|T], Buffer) when is_atom(H) ->
	parameter_key_to_env(T, [atom_to_binary(H)|Buffer]);
parameter_key_to_env([H|T], Buffer) when is_list(H) ->
	parameter_key_to_env(T, [list_to_binary(H)|Buffer]);
parameter_key_to_env([H|T], Buffer) when is_binary(H) ->
	parameter_key_to_env(T, [H|Buffer]);
parameter_key_to_env([H|_], _) ->
	{error, #{
			reason => {invalid, H},
			module => ?MODULE,
			callback => environment
		}
	}.

