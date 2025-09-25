%%%===================================================================
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
%%% @end
%%%===================================================================
-module(arweave_config_spec_configuration_key).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(Map = #{ configuration_key := CK }, State) when is_list(CK) ->
	fetch(Map, State);
init(Map, State) when is_map(Map) ->
	{error, #{
			reason => missing_key
		}
	};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, configuration_key, 0) of
		true ->
			fetch(Module, State);
		false ->
			{error, #{
				  	callback => configuration_key,
					reason => configuration_key_not_defined,
					module => Module,
					state => State
				 }
			}
	end.

%%--------------------------------------------------------------------
%% retrieve the value returned by the callback.
%%--------------------------------------------------------------------
fetch(Map, State) when is_map(Map) ->
	CK = maps:get(configuration_key, Map),
	check(Map, CK, State);
fetch(Module, State) when is_atom(Module) ->
	try
		CK = Module:configuration_key(),
		check(Module, CK, State)
	catch
		_:Reason ->
			{error, Reason}
	end.

%%--------------------------------------------------------------------
%% check if the parameter is a list.
%%--------------------------------------------------------------------
check(Module, CK, State) when is_list(CK) ->
	check2(Module, CK, CK, State);
check(Module, CK, State) ->
	{error, #{
		  	callback => configuration_key,
			reason => {invalid, CK},
			module => Module,
			state => State
		}
	}.

%%--------------------------------------------------------------------
%% check if the items present in the list are atoms, binaries or
%% tuple/1.
%%--------------------------------------------------------------------
check2(Module, [], [], State) ->
	{error, #{
			reason => {invalid, []},
			module => Module,
			state => State,
			callback => configuration_key
		}
	};
check2(Module, [], CK, State) ->
	{ok, State#{ configuration_key => CK }};
check2(Module, [Item|Rest], CK, State) when is_atom(Item) ->
	check2(Module, Rest, CK, State);
check2(Module, [Item|Rest], CK, State) when is_binary(Item) ->
	check2(Module, Rest, CK, State);
check2(Module, [{Variable}|Rest], CK, State) when is_atom(Variable) ->
	check2(Module, Rest, CK, State);
check2(Module, [Item|Rest], CK, State) ->
	{error, #{
		  callback => configuration_key,
		  reason => {invalid, Item},
		  module => Module,
		  state => State
		}
	}.
