%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_spec_deprecated).
-export([default/0, init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
default() -> false.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(Module, State) ->
	case is_function_exported(Module, deprecated, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ deprecated => default() }}
	end.

fetch(Module, State) ->
	try Module:deprecated() of
		false ->
			NewState = State#{ deprecated => default() },
			{ok, NewState};
		true ->
			NewState = State#{ deprecated => true },
			{ok, NewState};
		{true, _Message} ->
			NewState = State#{ deprecated => true },
			{ok, NewState};
		Elsewise ->
			{error, Elsewise}
	catch
		_:Reason ->
			{error, Reason}
	end.

