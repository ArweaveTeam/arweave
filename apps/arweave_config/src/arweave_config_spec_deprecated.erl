-module(arweave_config_spec_deprecated).
-compile(export_all).
-import(arweave_config_spec, [is_function_exported/3]).

default() -> false.

init(Module, State) ->
	case is_function_exported(Module, deprecated, 0) of
		true ->
			init2(Module, State);
		false ->
			{ok, State#{ deprecated => default() }}
	end.

init2(Module, State) ->
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

