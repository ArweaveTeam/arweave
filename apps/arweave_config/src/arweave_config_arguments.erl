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
-export([
	start_link/0,
	parse/1
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
parse(Args) ->
	% @todo it's annoying to convert these values, longs/short
	% args from specifications should be returned as map directly.
	LongArgs = maps:from_list(
		arweave_config_spec:get_long_arguments()
	),
	ShortArgs = maps:from_list(
		arweave_config_spec:get_short_arguments()
	),
	State = #{
		la => LongArgs,
		sa => ShortArgs,
		pos => 1
	},
	parse(Args, State, []).

%%--------------------------------------------------------------------
%% @hidden
%% @doc
%% @end
%%--------------------------------------------------------------------
parse([], _, Buffer) ->
	lists:reverse(Buffer);
parse([<<"-", Arg>>|Rest], State = #{sa := SA}, Buffer)
	when is_map_key(Arg, SA),
	     Arg =/= $- ->
		% parse the short arguments, if an element of the list is
		% starting with a single "-", then we can parse it.
		Spec = maps:get(Arg, SA),
		case apply_spec(Rest, Spec) of
			{ok, R, E} ->
				parse(R, State, [E|Buffer]);
			Else ->
				Else
		end;
parse([Arg|Rest], State = #{la := LA}, Buffer)
	when is_map_key(Arg, LA) ->
		% by default, we assume the argument is a long
		% arguments and we try to find it.
		Spec = maps:get(Arg, LA),
		case apply_spec(Rest, Spec) of
			{ok, R, E} ->
				parse(R, State, [E|Buffer]);
			Else ->
				Else
		end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc
%% @end
%%--------------------------------------------------------------------
apply_spec([], #{ type := boolean, set := Set }) ->
	{ok, [], {Set, [true]}};
apply_spec([Value|Rest], #{ type := boolean, set := Set }) ->
	case arweave_config_type:boolean(Value) of
		{ok, Return} ->
			{ok, Rest, {Set, [Return]}};
		_ ->
			{ok, [Value|Rest], {Set, [true]}}
	end;
apply_spec([Value|Rest], #{ set := Set }) ->
	{ok, Rest, {Set, [Value]}}.

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
