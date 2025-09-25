%%%===================================================================
%%% @doc Runtime Specification Definition.
%%%
%%% Runtime callback has been created to deal with different kind of
%%% parameters. Some are static and can be set only at startup. Others
%%% are dynamic and can be set during runtime.
%%%
%%% == TODO ==
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec_runtime).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(Map = #{ runtime := Runtime }, State) ->
	{ok, State#{ runtime => Runtime }};
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, runtime, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ runtime => false }}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init2(Module, State) ->
	try Module:runtime() of
		false ->
			NewState = State#{ runtime => false },
			{ok, NewState};
		true ->
			NewState = State#{ runtime => true },
			{ok, NewState};
		Elsewise ->
			{error, Elsewise}
	catch
		_:Reason ->
			{error, Reason}
	end.
