%%%===================================================================
%%% @doc Arweave configuration arguments manager.
%%%
%%% This module/process will be in charge to load/reset/configure the
%%% parameters using the command line arguments. Those arguments
%%% should be found after `--' and (in theory) available from
%%% `init:get_plain_arguments/0'. Unfortunately, because we are
%%% parsing those arguments using the `main/1' entry-point, the
%%% arguments cannot be found in the previous function.
%%%
%%% If the process is crashing, it should be able to recover its state
%%% without any problem.
%%%
%%% == Parsing ==
%%%
%%% This module also embeds an argument parser, an alternative to
%%% `argparse' module released in Erlang R26, with a more flexible
%%% interface mostly designed to be compatible with arweave. What not
%%% using `argparse'?
%%%
%%% (1) (strong) we are still  officially compatible with OTP R24, and
%%%     this module is not present in this release.
%%%
%%% (2) (weak)  `argparse' has  been designed  for `escript',  not for
%%%     long living process.
%%%
%%% (3) (weak) because we wants to support both environment variables,
%%%     arguments  and  dynamic  configuration  through  API  call,  a
%%%     database is required, this is  not offered by `argparse' right
%%%     now.
%%%
%%% (4) (weak) we are not controlling the implementation, it should be
%%%     okay because it has been commited into stdlib, but any change
%%%     could break the main entry-point
%%%
%%% A compatibility layer can be created if needed, the logic between
%%% the implementation present there and `argparse' module is quite
%%% similar.
%%%
%%% see: https://www.erlang.org/doc/apps/stdlib/argparse.html
%%%
%%% == Procedure ==
%%%
%%% 1. the application loads arguments and parse them using
%%%    `init:get_plain_arguments/0'.
%%%
%%% 2. if the application is receiving arguments from `load/1', they
%%%    will overwrite the arguments configured from
%%%    `init:get_plain_arguments/0'.
%%%
%%% 3. in case of reset, the whole arguments list is reset and
%%%    parameters  are fetch  from `init:get_plain_arguments/0'.  This
%%%    means, if arguments were configured from `load/1', they will be
%%%    lost.
%%%
%%% == TODO ==
%%%
%%% @TODO Create a compatible interface to argparse module.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_arguments).
-behavior(gen_server).
-export([load/0, load/1, reset/0]).
-export([start_link/0]).
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-compile(export_all).
-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
load() ->
	gen_server:cast(?MODULE, load).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
load(Arguments) ->
	gen_server:cast(?MODULE, {load, Arguments}).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
reset() ->
	gen_server:cast(?MODULE, reset).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(_) ->
	% 1. get the list of arguments from the entry-point
	% 2. parse arguments and validate them using specifications
	% 3. configure parameters
	% 4. if everything is find, stop.
	{stop, normal}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
terminate(_, _) -> ok.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(_, _, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(_, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(_, State) -> {noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc parse arguments from command lines. If successful, it will
%% returns a list of all arguments with their spec and other debugging
%% information. No checks are done their.
%% @end
%%--------------------------------------------------------------------
parse(Args) -> parse(Args, [], #{ position => 0 }).

parse([], Buffer, _) -> {ok, lists:reverse(Buffer)};
parse([Item|Rest], Buffer, State) when is_list(Item) ->
	% by default, we want to have only binaries, not list.
	parse([list_to_binary(Item)|Rest], Buffer, State);
parse([Item = <<"--", _/binary>>|Rest], Buffer, State) ->
	parse(Item, Rest, Buffer, State#{ kind => long });
parse([Item = <<"-", Char:8>>|Rest], Buffer, State) ->
 	% short argument, eg: -d
 	parse(Char, Rest, Buffer, State#{ kind => short });
% parse([Item = <<"-", Arguments/binary>>|Rest], Buffer, State) ->
% 	% short arguments, eg: -dv
% 	parse(Arguments, Rest, Buffer, State#{ kind => short });
parse([Item|Rest], Buffer, #{ position := Pos }) ->
	% wrong argument.
	{error, #{
			reason => badarg,
			arg => Item,
			rest => Rest,
			buffer => Buffer,
			position => Pos
		}
	}.

% parse item based on their kind
parse(Item, Rest, Buffer, State = #{ kind := short }) ->
	Position = maps:get(position, State),
	NewState = State#{
		item => Item,
		rest => Rest,
		position => Position+1
	},
	parse2(Rest, Buffer, NewState);
parse(Item, Rest, Buffer, State = #{ kind := long }) ->
	% long argument, eg: --global.debug
	% it supports the attribution with a "=" symbol
	% and then should be splitted. Only one "=" is allowed right
	% now, leaving us with only the argument or the argument and
	% its value.
	case re:split(Item, "=", [{parts, 2}]) of
		[Argument, Value] ->
			Position = maps:get(position, State),
			NewState = State#{
				item => Item,
				rest => Rest,
				position => Position+1,
				argument => Argument,
				value => [Value]
			 },
			parse2(Rest, Buffer, NewState);
		[Argument] -> 
			Position = maps:get(position, State),
			NewState = State#{
				item => Item,
				rest => Rest,
				position => Position+1,
				argument => Argument
			},
			parse2(Rest, Buffer, NewState)
	end.

% get arguments from arweave_config_spec
parse2(Rest, Buffer, State = #{ kind := short, item := Item }) ->
	case arweave_config_spec:get_short_argument(Item) of
		[{Argument, Spec}] ->
			CK = maps:get(configuration_key, Spec),
			E = maps:get(elements, Spec, 0),
			parse3(Rest, Buffer, State#{ spec => Spec });
		[] ->
			{error, #{
					reason => {badarg, Item},
					buffer => Buffer,
					state => State
				}
			}
	end;
parse2(Rest, Buffer, State = #{ kind := long, value := Value, argument := Arg }) ->
	case arweave_config_spec:get_long_argument(Arg) of
		[{Argument, Spec = #{ elements := E }}] when E =:= 1 ->
			Pos = maps:get(position, State),
			NewState = State#{ position => Pos+1 },
			parse3(Rest, Buffer, NewState#{ spec => Spec });
		[] ->
			{error, #{
					reason => {badarg, Arg},
					buffer => Buffer,
					state => State
				}
			}
	end;
parse2(Rest, Buffer, State = #{ kind := long, item := Item }) ->
	case arweave_config_spec:get_long_argument(Item) of
		[{Argument, Spec}] ->
			CK = maps:get(configuration_key, Spec),
			E = maps:get(elements, Spec, 0),
			parse3(Rest, Buffer, State#{ spec => Spec });
		[] ->
			{error, #{
					reason => {badarg, Item},
					buffer => Buffer,
					state => State
				}
			}
	end.

% final step to parse the argument
parse3(Rest, Buffer, State = #{ position := Pos, value := _ }) ->
	parse(Rest, [State|Buffer], #{ position => Pos+1 });
parse3(Rest, Buffer, State = #{ spec := Spec }) ->
	case take_arguments(Rest, [], State) of
		{ok, NewRest, Value, NewState} ->
			Pos = maps:get(position, NewState),
			L = length(Value),
			parse(NewRest, [NewState|Buffer], #{ position => Pos + L});
		{error, Reason} ->
			{error, #{
					argument => Reason,
					buffer => Buffer
		 		}
			}
	end.

take_arguments(Rest, Buffer, State = #{spec := #{elements := E}}) ->
	take_arguments(Rest, E, Buffer, State).

take_arguments(Rest, 0, Buffer, State) ->
	Value = lists:reverse(Buffer),
	{ok, Rest, Value, State#{ value => Value }};
take_arguments([], Counter, Buffer, State) ->
	{error, #{
		  	reason => missingarg,
			state => State,
			buffer => Buffer
		}
	};
take_arguments(Rest, Counter, Buffer, State) when Counter < 0 ->
	{error, #{}};
take_arguments([H|T], Counter, Buffer, State) ->
	take_arguments(T, Counter-1, [H|Buffer], State).

parse_test() ->
	% short argument parsing	{ok [{
	% {ok, [{[global,debug], true, #{ }}]} = parse(["-d"]),

	% long argument parsing
	% {ok, [{[global,debug], true}]} = parse(["--global.debug"]]),
	% {ok, [{[global,debug], true}]} = parse(["--global.debug", "true"]]),
	% {ok, [{[global,debug], true}]} = parse(["--global.debug=1"]),
	% {ok, [{[global,debug], true}]} = parse(["--global.debug=true"]),

	% variables
	% parse(["--peers.[127.0.0.1:1984].enabled=true"]),
	% parse(["--storage.3.enabled", "true"]),

	% multi elements
	% no example for now.

	% multi argument parsing
	% {ok, [
	%	{[global,debug], true},
	%	{[global,debug], true},
	%	{[global,data,directory], "./"}
	%]} = parse([
	%		"-d",
	%		"--global.debug",
	%		"--global.data.directory",
	%		"./"
	%]),

	% error management
	%{error, _} = parse(["-d", "test"]).
	ok.




