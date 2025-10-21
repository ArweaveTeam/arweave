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
%%% @doc short argument specification feature.
%%% @end
%%%===================================================================
-module(arweave_config_spec_short_argument).
-export([init/2]).
-include("arweave_config_spec.hrl").
-include_lib("kernel/include/logger.hrl").

init(Map = #{ short_argument := SA }, State) ->
	check(Map, SA, State);
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, short_argument, 0) of
		true ->
			?LOG_DEBUG("~p is defined", [{Module, short_argument, []}]),
			fetch(Module, State);
		false ->
			?LOG_DEBUG("~p is undefined", [{Module, short_argument, []}]),
			{ok, State}
	end.

fetch(Module, State) ->
	try
		SA = erlang:apply(Module, short_argument, []),
		check(Module, SA, State)
	catch
		_:R ->
			{error, R}
	end.

check(Module, undefined, State) ->
	{ok, State};
check(Module, false, State) ->
	{ok, State};
check(Module, [SA], State) when is_integer(SA), SA > 0 ->
	check(Module, SA, State);
check(Module, SA, State)
	when integer(SA),
		( SA >= $0 andalso SA =< $9 );
		( SA >= $a andalso SA =< $z );
		( SA >= $A andalso SA =< $Z ) ->
	{ok, State#{ short_argument => SA }};
check(Module, SA, State) ->
	{error, #{
			reason => {invalid, SA},
			callback => short_argument,
			module => Module,
			state => State
		}
	}.
