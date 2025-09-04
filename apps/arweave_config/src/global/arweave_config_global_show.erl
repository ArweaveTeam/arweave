-module(arweave_config_global_show).
-compile(export_all).

parent() -> arweave_config_global.

init(_, State) ->
	{ok, State}.

arguments(_, State) ->
	{ok, State}.

handle(State) ->
	Value = arweave_config_store:show(),
	{ok, Value}.
