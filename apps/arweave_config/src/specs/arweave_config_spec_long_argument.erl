%%%===================================================================
%%% @doc Long argument specification.
%%%
%%% ```
%%% % example
%%% Parameter = [global, debug].
%%% LongArgumentDefault = <<"--global.debug">>.
%%%
%%% % example
%%% Spec = [peers, {peer}, enabled].
%%% Parameter = [peers, <<"127.0.0.1:1984">>, enabled].
%%% Key = <<"peers.[127.0.0.1:1984].ebaled">>.
%%% LongArgumentDefault = <<"--peers.[peer].enabled">>.
%%% '''
%%%
%%% @end
%%%===================================================================
-module(arweave_config_spec_long_argument).
-export([init/2]).
-include("arweave_config_spec.hrl").

default() -> undefined.

init(Map, State) when is_map(Map) -> {ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, long_argument, 0) of
		true ->
			fetch(Module, State);
		false ->
			{ok, State#{ long_argument => default() }}
	end.

fetch(Module, State) ->
	try
		LA = erlang:apply(Module, long_argument, []),
		check(Module, LA, State)
	catch
		_:R ->
			{error, R}
	end.

check(Module, undefined, State) ->
	{ok, State#{ long_argument => undefined }};
check(Module, LA, State) when is_binary(LA) orelse is_list(LA) ->
	{ok, State#{ long_argument => LA }};
check(Module, LA, State) ->
	{error, #{
			reason => {invalid, LA},
			state => State,
			module => Module,
			callback => long_argument
		}
	}.
