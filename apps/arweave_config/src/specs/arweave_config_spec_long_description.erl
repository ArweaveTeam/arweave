%%%===================================================================
%%%
%%%===================================================================
-module(arweave_config_spec_long_description).
-export([init/2]).
-include("arweave_config_spec.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(#{ long_description := LD }, State) ->
	{ok, State#{ long_description => LD }};
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, long_description, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
fetch(Module, State) ->
	try
		LD = erlang:apply(Module, long_description, []),
		check(Module, LD, State)
	catch
		_:R ->
			{error, R}
	end.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
check(_Module, undefined, State) ->
	{ok, State#{ long_description => undefined }};
check(_Module, LD, State) when is_binary(LD); is_list(LD) ->
	{ok, State#{ long_description => LD }};
check(Module, LD, State) ->
	{error, #{
			reason => {invalid, LD},
			module => Module,
			state => State,
			callback => long_description
		}
	}.
