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
%%% @doc
%%% Define an environment variable to check or generate one
%%% automatically.
%%% @end
%%%===================================================================
-module(arweave_config_spec_environment).
-compile(warnings_as_errors).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(#{environment := true}, State) ->
	PK = maps:get(parameter_key, State),
	Env = convert(PK),
	{ok, State#{ environment => Env }};
init(#{environment := Env}, State) when is_binary(Env) ->
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
%%--------------------------------------------------------------------
-spec convert(PK) -> Return when
	PK :: [binary()|integer()|atom()],
	Return :: binary().

convert(PK) -> convert(PK, []).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec convert(PK, Buffer) -> Return when
	PK :: [binary()|integer()|atom()],
	Buffer :: [binary()],
	Return :: binary().

convert([], Buffer) ->
	Reverse = lists:reverse(Buffer),
	Join = lists:join(<<"_">>, [<<"AR">>|Reverse]),
	list_to_binary(Join);
convert([H|T], Buffer) when is_atom(H) ->
	Bin = atom_to_binary(H),
	Upper = string:uppercase(Bin),
	convert(T, [Upper|Buffer]);
convert([H|T], Buffer) when is_integer(H) ->
	Bin = integer_to_binary(H),
	convert(T, [Bin|Buffer]);
convert([H|T], Buffer) when is_binary(H) ->
	convert(T, [H|Buffer]).

