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
%%% @doc Arweave Configuration CLI Arguments Parser.
%%%
%%% This module is in charge of parsing the arguments from the command
%%% line, usually defined at startup. The parser will use the
%%% specifications from `arweave_config_spec' (this means, for now,
%%% `arweave_config' must be started to correctly parse something).
%%%
%%% The main idea is to have all arguments flags directly available
%%% from the specifications and parse the arguments from CLI using
%%% them. It should then return a list of actions to be executed.
%%%
%%% This module is also a process called `arweave_config_arguments',
%%% keeping the original command line passed by the user.
%%%
%%% @todo add support for more than one parameter taken from the flags
%%%
%%% @todo add support for short string flags (e.g. -def will look for
%%%       -d, -e and -f flags if they are boolean).
%%%
%%% @todo returns a comprehensive error message.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_arguments).
-behavior(gen_server).
-compile(warnings_as_errors).
-compile({no_auto_import,[get/0]}).
-export([
	start_link/0,
	% load/0,
	% load/1,
	% get/0,
	% reset/0
	parse/1
]).
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc Parses an argument from command line. Erlang is usually giving
%% us these arguments as a `[string()]', but we want it to be a
%% `[binary()]', to make our life easier when displaying this
%% information somewhere else (e.g. JSON).
%% @end
%%--------------------------------------------------------------------
-spec parse(Args) -> Return when
	Args :: [binary()],
	Return :: {ok, [{Spec, Values}]} | {error, Reason},
	Spec :: map(),
	Values :: [term()],
	Reason :: map().

parse(Args) ->
	parse_converter(Args, []).

%%--------------------------------------------------------------------
%% @hidden
%% @doc type converter, the parser only check binary data.
%% @end
%%--------------------------------------------------------------------
parse_converter([], Buffer) ->
	parse_final(lists:reverse(Buffer));
parse_converter([Arg|Rest], Buffer) when is_list(Arg) ->
	NewBuffer = [list_to_binary(Arg)|Buffer],
	parse_converter(Rest, NewBuffer);
parse_converter([Arg|Rest], Buffer) when is_integer(Arg) ->
	NewBuffer = [integer_to_binary(Arg)|Buffer],
	parse_converter(Rest, NewBuffer);
parse_converter([Arg|Rest], Buffer) when is_float(Arg) ->
	NewBuffer = [float_to_binary(Arg)|Buffer],
	parse_converter(Rest, NewBuffer);
parse_converter([Arg|Rest], Buffer) when is_atom(Arg) ->
	NewBuffer = [atom_to_binary(Arg)|Buffer],
	parse_converter(Rest, NewBuffer);
parse_converter([Arg|Rest], Buffer) when is_binary(Arg) ->
	NewBuffer = [Arg|Buffer],
	parse_converter(Rest, NewBuffer).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
parse_final(Args) ->
	try
		% @todo it's annoying to convert these values, longs/short
		% args from specifications should be returned as map directly.
		LongArgs = maps:from_list(
			arweave_config_spec:get_long_arguments()
		),
		ShortArgs = maps:from_list(
			arweave_config_spec:get_short_arguments()
		),
		State = #{
			args => Args,
			la => LongArgs,
			sa => ShortArgs,
			pos => 1,
			actions => []
		},
		parse(Args, State)
	catch
		_:R ->
			{error, #{
					reason => R
				}
			}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc loop over the arguments and check them.
%% @end
%%--------------------------------------------------------------------
-spec parse(Args, State) -> Return when
	Args :: [binary()],
	State :: map(),
	Return :: {ok, [{Spec, Values}]} | {error, Reason},
	Spec :: map(),
	Values :: [term()],
	Reason :: map().

parse([], #{ actions := Buffer }) ->
	{ok, lists:reverse(Buffer)};
parse([Arg = <<"---",_/binary>>|_], State) ->
	Pos = maps:get(pos, State),
	{error, #{
			reason => <<"bad_argument">>,
			argument => Arg,
			position => Pos
		}
	};
parse([Arg = <<"--",_/binary>>|Rest], State = #{la := LA})
	when is_map_key(Arg, LA) ->
		% by default, we assume the argument is a long
		% arguments and we try to find it.
		Spec = maps:get(Arg, LA),
		Pos = maps:get(pos, State),
		case apply_spec(Rest, Spec, State#{ pos => Pos+1 }) of
			{ok, NewRest, NewState} ->
				parse(NewRest, NewState);
			Else ->
				Else
		end;
parse([<<"-", Arg>>|Rest], State = #{sa := SA})
	when is_map_key(Arg, SA),
	     Arg =/= $- ->
		Spec = maps:get(Arg, SA),
		Pos = maps:get(pos, State),
		case apply_spec(Rest, Spec, State#{ pos => Pos+1 }) of
			{ok, NewRest, NewState} ->
				parse(NewRest, NewState);
			Else ->
				Else
		end;
parse([Unknown|_], #{ pos := Pos }) ->
	{error, #{
			reason => <<"unknown argument">>,
			argument => Unknown,
			position => Pos
		}
	}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc Take a value and check its type.
%% @end
%%--------------------------------------------------------------------
apply_spec([], Spec = #{type := boolean}, State) ->
	Buffer = maps:get(actions, State),
	NewBuffer = [{Spec, [true]}|Buffer],
	NewState = State#{
		actions => NewBuffer
	},
	{ok, [], NewState};
apply_spec([Value|Rest], Spec = #{type := boolean}, State) ->
	Buffer = maps:get(actions, State),
	case arweave_config_type:boolean(Value) of
		{ok, Return} ->
			Pos = maps:get(pos, State),
			NewBuffer = [{Spec, [Return]}|Buffer],
			NewState = State#{
				actions => NewBuffer,
				pos => Pos+1
			},
			{ok, Rest, NewState};
		_ ->
			NewBuffer = [{Spec, [true]}|Buffer],
			NewState = State#{
				actions => NewBuffer
			},
			{ok, [Value|Rest], NewState}
	end;
apply_spec([Value|Rest], Spec = #{type := Type}, State) ->
	Buffer = maps:get(actions, State),
	Pos = maps:get(pos, State),
	case arweave_config_type:Type(Value) of
		{ok, Return} ->
			NewBuffer = [{Spec, [Return]}|Buffer],
			NewState = State#{
				actions => NewBuffer,
				pos => Pos+1
			},
			{ok, Rest, NewState};
		_ ->
			{error, #{
					reason => <<"bad value">>,
					value => Value,
					type => Type,
					position => Pos
				}
			}
	end.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_) ->
	{ok, []}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(_, _, State) ->
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(_, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(_, State) ->
	{noreply, State}.
