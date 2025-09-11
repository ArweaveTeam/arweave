-module(arweave_config_spec_short_description).
-export([init/2]).
-include("arweave_config_spec.hrl").

default() -> <<>>.

init(Module, State) ->
	case is_function_exported(Module, short_description, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ short_description => default() }}
	end.

fetch(Module, State) ->
	try
		SD = erlang:apply(Module, short_description, []),
		check(Module, SD, State)
	catch
		_:R ->
			{error, R}
	end.

check(_Module, undefined, State) ->
	{ok, State#{ short_description => undefined }};
check(_Module, SD, State) when is_binary(SD); is_list(SD) ->
	{ok, State#{ short_description => SD }};
check(Module, SD, State) ->
	{error, #{
			reason => {invalid, SD},
			module => Module,
			callback => short_description,
			state => State
		}
	}.
