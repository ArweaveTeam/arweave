%%%===================================================================
%%% @doc
%%% @end
%%%===================================================================
-module(arweave_config_spec_configuration_key).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(Module, State) ->
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
fetch(Module, State) ->
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
