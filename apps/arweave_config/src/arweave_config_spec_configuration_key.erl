%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_spec_configuration_key).
-export([init/2]).
-import(arweave_config_spec, [is_function_exported/3]).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(Module, State) ->
	case is_function_exported(Module, configuration_key, 0) of
		true ->
			init2(Module, State);
		false ->
			{error, configuration_key_not_defined}
	end.

init2(Module, State) ->
	try Module:configuration_key() of
		{ok, CK} ->
			{ok, State#{ configuration_key => CK }};
		Elsewise ->
			{error, Elsewise}
	catch
		_:Reason ->
			{error, Reason}
	end.
