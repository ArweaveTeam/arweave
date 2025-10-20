%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2025 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Long argument specification feature. It should be compatible
%%% with arguments modules in OTP.
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

init(Map = #{ long_argument := LA }, State) ->
	check(Map, LA, State);
init(Map, State) when is_map(Map) ->
	{ok, State};
init(Module, State) when is_atom(Module) ->
	case is_function_exported(Module, long_argument, 0) of
		true ->
			fetch(Module, State);
		false ->
			check(Module, undefined, State)
	end.

fetch(Module, State) ->
	try
		LA = erlang:apply(Module, long_argument, []),
		check(Module, LA, State)
	catch
		_:R ->
			{error, R}
	end.

check(Module, false, State) ->
	{ok, State};
check(Module, undefined, State = #{ parameter_key := CK }) ->
	{ok, State#{ long_argument => convert(CK) }};
check(Module, LA, State) when is_binary(LA) orelse is_list(LA) ->
	{ok, State#{ long_argument => convert(LA) }};
check(Module, LA, State) ->
	{error, #{
			reason => {invalid, LA},
			state => State,
			module => Module,
			callback => long_argument
		}
	}.

convert(List) when is_list(List) -> convert(List, []);
convert(<<"-", _/binary>> = Binary) -> Binary;
convert(Binary) when is_binary(Binary) -> <<"-", Binary/binary>>.

convert([], Buffer) ->
	Sep = application:get_env(arweave_config, long_argument_separator, "."),
	Bin = list_to_binary(lists:join(Sep, lists:reverse(Buffer))),
	<<"-", Bin/binary>>;
convert([H|T], Buffer) when is_integer(H) ->
	convert([integer_to_binary(H)|T], Buffer);
convert([H|T], Buffer) when is_atom(H) ->
	convert([atom_to_binary(H)|T], Buffer);
convert([H|T], Buffer) when is_list(H) ->
	convert([list_to_binary(H)|T], Buffer);
convert([H|T], Buffer) when is_binary(H) ->
	convert(T, [H|Buffer]).
