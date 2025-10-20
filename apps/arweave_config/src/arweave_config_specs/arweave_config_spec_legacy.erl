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
%%% @doc legacy specification feature. set the key used by the legacy
%%% configuration.
%%% @todo legacy configuration is kinda special, and instead of an
%%% atom, a function should probably be used sometimes.
%%% @end
%%%===================================================================
-module(arweave_config_spec_legacy).
-export([init/2]).
-include("arweave_config_spec.hrl").
-include("arweave_config.hrl").
-include_lib("eunit/include/eunit.hrl").

init(Map = #{ legacy := Legacy }, State) when is_map(Map) ->
	check(Map, Legacy, State);
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, legacy, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State}
	end.

init_test() ->
	?assertEqual(
		{ok, #{ legacy => init }},
		init(#{ legacy => init }, #{})
	),
	?assertMatch(
		{error, _},
		init(#{ legacy => 123 }, #{})
	),
	?assertMatch(
		{error, _},
		init(#{ legacy => does_not_exist }, #{})
	).

fetch(Module, State) when is_atom(Module) ->
	try
		L = erlang:apply(Module, legacy, []),
		check(Module, L, State)
	catch
		_:R ->
			{error, R}
	end.

check(Module, Legacy, State) when is_atom(Legacy) ->
	% ensure the presence of the field in config record.
	Fields = record_info(fields, config),
	case [ X || X <- Fields, X =:= Legacy ] of
		[Legacy] ->
			{ok, State#{ legacy => Legacy }};
		_ ->
			{error, #{
					reason => "does not exists",
					legacy_key => Legacy,
					module => Module
				 }
			}
	end;
check(Module, Legacy, State) ->
	{error, #{
			reason => {invalid, Legacy},
			module => Module,
			callback => legacy,
			state => State
		}
	}.
