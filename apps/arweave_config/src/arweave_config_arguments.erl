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
%%% == TODO: sub-arguments parsing ==
%%%
%%% Note: the following section is a draft and acts as an example for
%%% the future implementation.
%%%
%%% Let say one wants to configure one specific peer. Instead of
%%% pushing this value in many different place, one can set some
%%% options on this peer. For example:
%%%
%%% ```
%%% --peer europe.arweave.xyz --trusted \
%%% --peer vdf-server-3.arweave.xyz --trusted --vdf
%%% '''
%%%
%%% In this code, the peer(s) `europe.arweave.xyz' will now be set as
%%% `trusted' and `vdf-server-3.arweave.xyz' will be set as `trusted'
%%% and `vdf'. The main information - here the peer - is not
%%% duplicated across many options.
%%%
%%% ```
%%% {
%%%   "peers": {
%%%     "europe.arweave.xyz": {
%%%       "trusted": true
%%%     },
%%%     "vdf-server-3.arweave.xyz": {
%%%       "trusted": true,
%%%       "vdf": true
%%%     }
%%%   }
%%% }
%%% '''
%%%
%%% Another example with storage modules. One wants to configure
%%% storages module with many different options.
%%%
%%% ```
%%% --storage.module 0 \
%%%   --protocol unpacked \
%%%   --enabled \
%%% --storage.module 0
%%%   --protocol replica.2.9 \
%%%   --start 0 \
%%%   --end 7200000000000 \
%%%   --pubkey L-cPRwGcnMFyQW-APh_fS4lMGioLg76-ECovqXIlmJ4 \
%%%   --enabled
%%% '''
%%%
%%% The first argument will create storage module `0' using unpacked
%%% data. The second storage module will use `replica.2.9' with custom
%%% options (e.g. offset and pubkey). The final representation of the
%%% data as JSON would be represented like the following snippet:
%%%
%%% ```
%%% {
%%%   "storage": {
%%%     {
%%%       "modules": [
%%%         {
%%%           "partition": "0",
%%%           "protocol": "unpacked",
%%%           "status": "enabled"
%%%         },
%%%         {
%%%           "partition": "0",
%%%           "protocol: "replica.2.9",
%%%           "start": "0",
%%%           "end": "7200000000000",
%%%           "pubkey": "L-cPRwGcnMFyQW-APh_fS4lMGioLg76-ECovqXIlmJ4",
%%%           "status": "enabled"
%%%         }
%%%      ]
%%%   }
%%% }
%%% '''
%%%
%%% On the specification side, a sub-argument could be represented by
%%% a function, returning a list of spec.
%%%
%%% ```
%%% #{
%%%   short_argument => {<<"-S">>, fun storage/0},
%%%   long_argument => {<<"--storage.module">>, {M,F,A}}
%%% }
%%%
%%% storage() ->
%%%   [
%%%     #{
%%%       % implicit definition of parameter key
%%%       % parameter_key => [storage,modules,0]
%%%       long_argument => <<"--enabled">>,
%%%       short_argument => $E,
%%%       type => boolean,
%%%       default => true
%%%     },
%%%     #{
%%%       long_argument => <<"--protocol">>,
%%%       type => storage_protocol
%%%       required => true
%%%     }
%%%   ].
%%% '''
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
	load/1,
	get/0,
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
	end.

%%--------------------------------------------------------------------
%% @doc starts arweave_config_arguments server.
%% @end
%%--------------------------------------------------------------------
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, #{}, []).

%%--------------------------------------------------------------------
%% @doc loads arguments from `init:get_plain_arguments/0'.
%% @see init:get_plain_arguments/0
%% @end
%%--------------------------------------------------------------------
load() ->
	gen_server:cast(?MODULE, load).

%%--------------------------------------------------------------------
%% @doc loads arguments from command line.
%% @end
%%--------------------------------------------------------------------
load(Args) ->
	gen_server:call(?MODULE, {load, Args}, 10_000).

%%--------------------------------------------------------------------
%% @doc returns parsed arguments from process state.
%% @end
%%--------------------------------------------------------------------
get() ->
	gen_server:call(?MODULE, get, 10_000).

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
init(Opts) ->
	RawArgs = maps:get(raw_args, Opts, []),
	State = #{
		raw_args => RawArgs
	},
	{ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_call(get, _From, State) ->
	Args = maps:get(args, State, []),
	{reply, Args, State};
handle_call({load, Args}, _From, State) ->
	try
		parse(Args)
	of
		Result = {ok, _Parsed} ->
			NewState = State#{ args => Args },
			{reply, Result, NewState};
		Else ->
			{reply, Else, State}
	catch
		Error:Reason ->
			{reply, {Error, Reason}}
	end;
handle_call(_, _, State) ->
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_cast(load, State) ->
	Args = init:get_plain_arguments(),
	case parse(Args) of
		{ok, Parsed} ->
			NewState = State#{
				args => Parsed,
				raw_args => Args
			},
			{noreply, NewState};
		_Else ->
			{noreply, State}
	end;
handle_cast(_, State) ->
	{noreply, State}.

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
handle_info(_, State) ->
	{noreply, State}.
