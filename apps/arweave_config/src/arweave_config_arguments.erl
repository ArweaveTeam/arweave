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
%%% @todo when parsing fails, the documentation of the last parameter
%%%       should be displayed, or the documentation of the whole
%%%       application.
%%%
%%% @todo sub-arguments parsing, a more complex way to parse certain
%%% kind of value will be required in some situation, for example for
%%% peers and storage modules. see section below.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_arguments).
-behavior(gen_server).
-compile(warnings_as_errors).
-compile({no_auto_import,[get/0]}).
-export([
	start_link/0,
	load/0,
	set/1,
	get/0,
	get_args/0,
	parse/1,
	parse/2
]).
-export([init/1]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%% @doc Parses command line arguments.
%% @see parse/2
%% @end
%%--------------------------------------------------------------------
-spec parse(Args) -> Return when
	Args :: [binary()],
	Return :: {ok, [{Spec, Values}]} | {error, Reason},
	Spec :: map(),
	Values :: [term()],
	Reason :: map().

parse(Args) ->
	parse(Args, #{}).

%%--------------------------------------------------------------------
%% @doc Parses an argument from command line. Erlang is usually giving
%% us these arguments as a `[string()]', but we want it to be a
%% `[binary()]', to make our life easier when displaying this
%% information somewhere else (e.g. JSON).
%%
%% Custom specifications can be set using `long_arguments' and
%% `short_arguments' options. Those are mostly used for testing and
%% debugging purpose, by default, this function will fetch
%% specifications from `arweave_config_spec' process.
%% @end
%%--------------------------------------------------------------------
-spec parse(Args, Opts) -> Return when
	Args :: [binary()],
	Opts :: #{
		long_arguments => #{},
		short_arguments => #{}
	},
	Return :: {ok, [{Spec, Values}]} | {error, Reason},
	Spec :: map(),
	Values :: [term()],
	Reason :: map().

parse(Args, Opts) ->
	parse_converter(Args, Opts).

%%--------------------------------------------------------------------
%% @hidden
%% @doc type converter, the parser only check binary data.
%% @end
%%--------------------------------------------------------------------
parse_converter(Args, Opts) ->
	parse_converter(Args, [], Opts).

parse_converter([], Buffer, Opts) ->
	Reverse = lists:reverse(Buffer),
	parse_final(Reverse, Opts);
parse_converter([Arg|Rest], Buffer, Opts) when is_list(Arg) ->
	NewBuffer = [list_to_binary(Arg)|Buffer],
	parse_converter(Rest, NewBuffer, Opts);
parse_converter([Arg|Rest], Buffer, Opts) when is_integer(Arg) ->
	NewBuffer = [integer_to_binary(Arg)|Buffer],
	parse_converter(Rest, NewBuffer, Opts);
parse_converter([Arg|Rest], Buffer, Opts) when is_float(Arg) ->
	NewBuffer = [float_to_binary(Arg)|Buffer],
	parse_converter(Rest, NewBuffer, Opts);
parse_converter([Arg|Rest], Buffer, Opts) when is_atom(Arg) ->
	NewBuffer = [atom_to_binary(Arg)|Buffer],
	parse_converter(Rest, NewBuffer, Opts);
parse_converter([Arg|Rest], Buffer, Opts) when is_binary(Arg) ->
	NewBuffer = [Arg|Buffer],
	parse_converter(Rest, NewBuffer, Opts).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
parse_final(Args, Opts) ->
	try
		LongArgs = maps:get(
			long_arguments,
			Opts,
			% @todo it's annoying to convert these values, longs/short
			% args from specifications should be returned as map directly.
			maps:from_list(
				arweave_config_spec:get_long_arguments()
			)
		),
		ShortArgs = maps:get(
			short_arguments,
			Opts,
			% @todo it's annoying to convert these values, longs/short
			% args from specifications should be returned as map directly.
			maps:from_list(
				arweave_config_spec:get_short_arguments()
			)
		),
		State = #{
			args => Args,
			la => LongArgs,
			sa => ShortArgs,
			pos => 1,
			actions => []
		},
		parse(Args, State, Opts)
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
-spec parse(Args, State, Opts) -> Return when
	Args :: [binary()],
	State :: map(),
	Opts :: map(),
	Return :: {ok, [{Spec, Values}]} | {error, Reason},
	Spec :: map(),
	Values :: [term()],
	Reason :: map().

parse([], #{ actions := Buffer }, _Opts) ->
	{ok, lists:reverse(Buffer)};
parse([Arg = <<"---",_/binary>>|_], State, _Opts) ->
	Pos = maps:get(pos, State),
	{error, #{
			reason => <<"bad_argument">>,
			argument => Arg,
			position => Pos
		}
	};
parse([Arg = <<"--",_/binary>>|Rest], State = #{la := LA}, Opts)
	when is_map_key(Arg, LA) ->
		% by default, we assume the argument is a long
		% arguments and we try to find it.
		Spec = maps:get(Arg, LA),
		Pos = maps:get(pos, State),
		case apply_spec(Rest, Spec, State#{ pos => Pos+1 }) of
			{ok, NewRest, NewState} ->
				parse(NewRest, NewState, Opts);
			Else ->
				Else
		end;
parse([<<"-", Arg>>|Rest], State = #{sa := SA}, Opts)
	when is_map_key(Arg, SA),
	     Arg =/= $- ->
		Spec = maps:get(Arg, SA),
		Pos = maps:get(pos, State),
		case apply_spec(Rest, Spec, State#{ pos => Pos+1 }) of
			{ok, NewRest, NewState} ->
				parse(NewRest, NewState, Opts);
			Else ->
				Else
		end;
parse([Unknown|_], #{ pos := Pos }, _Opts) ->
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
	end;
apply_spec(_, Spec, State) ->
	Type = maps:get(type, Spec),
	Pos = maps:get(pos, State),
	{error, #{
			reason => <<"missing value">>,
			type => Type,
			position => Pos
		}
	}.

%%--------------------------------------------------------------------
%% @doc starts arweave_config_arguments server.
%% @end
%%--------------------------------------------------------------------
-spec start_link() -> {ok, pid()}.

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

%%--------------------------------------------------------------------
%% @doc load arguments.
%% @end
%%--------------------------------------------------------------------
-spec load() -> ok | {error, term()}.

load() ->
	gen_server:call(?MODULE, load, 10_000).

%%--------------------------------------------------------------------
%% @doc set arguments from command line.
%% @end
%%--------------------------------------------------------------------
-spec set(Args) -> Return when
	Args :: [string() | binary()],
	Return :: ok.

set(Args) ->
	gen_server:call(?MODULE, {set, Args}, 10_000).

%%--------------------------------------------------------------------
%% @doc returns parsed arguments from process state.
%% @end
%%--------------------------------------------------------------------
-spec get() -> {ok, [map()]}.

get() ->
	gen_server:call(?MODULE, get, 10_000).

%%--------------------------------------------------------------------
%% @doc returns raw arguments from process state.
%% @end
%%--------------------------------------------------------------------
-spec get_args() -> [string() | binary()].

get_args() ->
	gen_server:call(?MODULE, {get, args}, 10_000).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec init(Args) -> Return when
	Args :: #{
		args => [string() | binary()]
	},
	Return :: {ok, State},
	State :: #{
		init_args => Args,
		args => Args,
		params => []
	}.

init(Args) ->
	State = #{ params => [] },
	init_args(Args, State).

%%--------------------------------------------------------------------
%% @hidden
%% @doc get arguments directly from the command line.
%%
%% ```
%% arweave_config_arguments:start(#{ args => [] }).
%% '''
%%
%% @end
%%--------------------------------------------------------------------
init_args(Args, State) ->
	RawArgs = maps:get(args, Args, []),
	NewState = State#{
		init_args => Args,
		args => RawArgs
	},
	init_final(Args, NewState).

%%--------------------------------------------------------------------
%% @hidden
%% @doc returns the final state, ready to be used by the process.
%% @end
%%--------------------------------------------------------------------
init_final(_, State) ->
	{ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec handle_call
	(get, From, State) -> Return when
		From :: term(),
		State :: map(),
		Return :: {reply, Reply, State},
		Reply :: [string()];
	({get, args}, From, State) -> Return when
		From :: term(),
		State :: map(),
		Return :: {reply, Reply, State},
		Reply :: [string()];
	({set, Args}, From, State) -> Return when
		Args :: [string() | binary()],
		From :: term(),
		State :: map(),
		Return :: {reply, Reply, State},
		Reply :: {ok, [map()]} | {error, term()};
	(load, From, State) -> Return when
		From :: term(),
		State :: map(),
		Return :: {reply, Reply, State},
		Reply :: ok | {error, term()};
	(any(), From, State) -> Return when
		From :: term(),
		State :: map(),
		Return :: {reply, ok, State}.

handle_call(get, _From, State) ->
	Args = maps:get(params, State, []),
	{reply, Args, State};
handle_call({get, args}, _From, State) ->
	RawArgs = maps:get(args, State, []),
	{reply, RawArgs, State};
handle_call({set, RawArgs}, _From, State) ->
	try
		parse(RawArgs)
	of
		{ok, Parsed} ->
			NewState = State#{
				     args => RawArgs,
				     params => Parsed
			},
			{reply, {ok, Parsed}, NewState};
		Else ->
			{reply, Else, State}
	catch
		_Error:Reason ->
			{reply, {error, Reason}, State}
	end;
handle_call(load, _From, State = #{ params := Params}) ->
	try
		lists:map(fun load_fun/1, Params),
		{reply, ok, State}
	catch
		_Error:Reason ->
			{reply, {error, Reason}, State}
	end;
handle_call(Msg, _, State) ->
	?LOG_WARNING("~p (~p) received: ~p", [?MODULE, self(), Msg]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec handle_cast(Msg, State) -> Return when
	Msg :: any(),
	State :: #{},
	Return :: {noreply, State}.

handle_cast(Msg, State) ->
	?LOG_WARNING("~p (~p) received: ~p", [?MODULE, self(), Msg]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
-spec handle_info(Msg, State) -> Return when
	Msg :: any(),
	State :: #{},
	Return :: {noreply, State}.

handle_info(Msg, State) ->
	?LOG_WARNING("~p (~p) received: ~p", [?MODULE, self(), Msg]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
load_fun({Spec, [Value]}) ->
	ParameterKey = maps:get(parameter_key, Spec),
	arweave_config:set(ParameterKey, Value).
