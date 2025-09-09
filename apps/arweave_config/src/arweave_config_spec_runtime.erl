-module(arweave_config_spec_runtime).
-export([init/2]).
-include("arweave_config_spec.hrl").

init(Module, State) ->
	case is_function_exported(Module, runtime, 0) of
		true ->
			init2(Module, State);
		false ->
			{error, #{
					reason => undefined,
					function => runtime,
					module => Module
				}
			}
	end.

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
