%%%===================================================================
%%% @doc 
%%%
%%% == TODO ==
%%%
%%% Describe each elements by a small description or a variable. For
%%% example `--global.network.shutdown.mode=kill'
%%%
%%% ```
%%% elements() -> [Type].
%%% '''
%%%
%%% In the documentation (long description format):
%%%
%%% ```
%%%   Global Network Shutdown Mode Parameter:
%%%
%%%     Dynamic: true
%%%
%%%     Arguments:
%%%       -m Mode
%%%       --global.network.shutdown.mode Mode
%%%       --global.network.shutdown.mode=Mode
%%%
%%%     Environment Variable:
%%%       AR_GLOBAL_NETWORK_SHUTDOWN=Mode
%%%
%%%     Where:
%%%       Mode is "type description".
%%%
%%%     Description:
%%%       Configure the method to shutdown the network connection,
%%%       it can be set to shutdown or close.
%%%
%%%     Examples:
%%%       ...
%%%
%%%     See:
%%%       Links...
%%% '''
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec_elements).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
default() -> [].

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(Map = #{ elements := E }, State) ->
	check(Map, E, State);
init(Map, State) when is_map(Map) ->
	{ok, State#{ elements => default() }};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, elements, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ elements => default() }}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
fetch(Module, State) ->
	try erlang:apply(Module, elements, []) of
		E ->
			check(Module, E, State);
		Elsewise ->
			{error, #{
					module => Module,
					callback => elements,
					reason => {bad_value, Elsewise}
				}
			}
	catch
		_:R ->
			{error, R}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
check(Module, E, State) ->
	check(Module, E, E, State).

check(Module, [], [], State) ->
	{ok, State#{ elements => [] }};
check(Module, [], E, State) ->
	{ok, State#{ elements => E }};
check(Module, [Type|Rest], E, State) when is_atom(Type) ->
	check(Module, Rest, E, State);
check(Module, _, E, State) ->
	{error, #{
			module => Module,
			callback => elements,
			reason => {bad_value, E}
		}
	}.


