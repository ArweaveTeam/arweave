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
%%% @doc Arweave Configuration Specification Configuration Key.
%%%
%%% A specification for a specification key is a way to describe a
%%% parameter in arweave_config. The idea is to have something similar
%%% like a path/uri that can be checked before updated. A simple
%%% example with the debug parameter:
%%%
%%% ```
%%% [global,debug].
%%% '''
%%%
%%% How to configure a "dynamic" key, for example, with a peer or a
%%% storage module? It can be done by inserting a special term to
%%% define what kind of type is accepted.
%%%
%%% ```
%%% [peers,{peer},enabled].
%%% '''
%%%
%%% What if the variable parameter can have many types?
%%%
%%% ```
%%% [peers, {[peer,ipv4,ipv6]}, enabled].
%%% '''
%%%
%%% Now, how it's possible to match quickly the content of a parameter
%%% and this kind of key?
%%%
%%% ```
%%% RawKey = <<"peers.[127.0.0.1].enabled">>.
%%% Value = <<"true">>.
%%% FormattedKey = [peers, <<"127.0.0.1">>, enabled].
%%% Specification = [peers, {[peer,ipv4,ipv6]}, enabled].
%%%
%%% % an idea for an internal representation
%%% % InternalSpec = [peers, fun param/1, enabled].
%%%
%%% {ok, Spec} = find(FormattedKey).
%%% true = is_valid(FormattedKey, Spec).
%%% '''
%%%
%%% @todo if the provided key is a binary, it should probably be good
%%% to convert it.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec_parameter_key).
-export([init/2]).
-include("arweave_config_spec.hrl").
-include_lib("eunit/include/eunit.hrl").

init(Map = #{ parameter_key := CK }, State) ->
	fetch(Map, State);
init(Map, State) when is_map(Map) ->
	{error, #{
			module => Map,
			reason => missing_parameter_key,
			state => State
		}
	};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, parameter_key, 0) of
		true ->
			fetch(Module, State);
		false ->
			{error, #{
					callback => parameter_key,
					reason => parameter_key_not_defined,
					module => Module,
					state => State
				 }
			}
	end.

init_test() ->
	?assertEqual(
		{ok, #{ parameter_key => [test]}},
		init(#{ parameter_key => <<"test">> }, #{})
	),
	?assertEqual(
		{ok, #{ parameter_key => [test,1,2,3]}},
		init(#{ parameter_key => <<"test.1.2.3">> }, #{})
	),
	?assertEqual(
		{ok, #{ parameter_key => [test]}},
		init(#{ parameter_key => [test]}, #{})
	),
	?assertEqual(
		{ok, #{ parameter_key => [<<"test">>] }},
		init(#{ parameter_key => [<<"test">>] }, #{})
	),
	?assertMatch(
		{error, _},
		init(#{}, #{})
	),
	?assertMatch(
		{error, _},
		init(#{ parameter_key => []}, #{})
	),
	?assertMatch(
		{error, _},
		init(#{ parameter_key => test}, #{})
	),
	?assertMatch(
		{error, _},
		init(#{ parameter_key => [test, #{}]}, #{})
	).

fetch(Map = #{ parameter_key := CK }, State) ->
	check(Map, CK, State);
fetch(Module, State) when is_atom(Module) ->
	try
		CK = Module:parameter_key(),
		check(Module, CK, State)
	catch
		_:Reason ->
			{error, Reason}
	end.

check(Module, CK, State) when is_binary(CK) ->
	case arweave_config_parser:key(CK) of
		{ok, Value} ->
			{ok, State#{ parameter_key => Value }};
		{error, Reason} ->
			{error, #{
					module => Module,
					callback => parameter_key,
					reason => {Reason, CK},
					state => State
				}
			}
	end;
check(Module, CK, State) when is_list(CK) ->
	check2(Module, CK, CK, State);
check(Module, CK, State) ->
	{error, #{
			callback => parameter_key,
			reason => {invalid, CK},
			module => Module,
			state => State
		}
	}.

check2(Module, [], [], State) ->
	{error, #{
			reason => {invalid, []},
			module => Module,
			state => State,
			callback => parameter_key
		}
	};
check2(_Module, [], CK, State) ->
	{ok, State#{ parameter_key => CK }};
check2(Module, [Item|Rest], CK, State) when is_atom(Item) ->
	check2(Module, Rest, CK, State);
check2(Module, [Item|Rest], CK, State) when is_binary(Item) ->
	check2(Module, Rest, CK, State);
check2(Module, [{Variable}|Rest], CK, State) when is_atom(Variable) ->
	check2(Module, Rest, CK, State);
check2(Module, [Item|Rest], _CK, State) ->
	{error, #{
		  callback => parameter_key,
		  reason => {invalid, Item},
		  module => Module,
		  state => State,
		  rest => Rest
		}
	}.
