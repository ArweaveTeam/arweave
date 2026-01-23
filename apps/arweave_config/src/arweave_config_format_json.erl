%%%===================================================================
%%% GNU General Public License, version 2 (GPL-2.0)
%%% The GNU General Public License (GPL-2.0)
%%% Version 2, June 1991
%%%
%%% ------------------------------------------------------------------
%%%
%%% @copyright 2026 (c) Arweave
%%% @author Arweave Team
%%% @author Mathieu Kerjouan
%%% @doc Arweave Configuration JSON Format Support.
%%%
%%% @reference https://www.json.org/json-en.html
%%% @reference https://github.com/davisp/jiffy
%%% @end
%%%===================================================================
-module(arweave_config_format_json).
-compile(warnings_as_errors).
-export([
	parse/1,
	parse/2
]).
-export([
	decode_data/1,
	parse_config/1
]).

%%--------------------------------------------------------------------
%% @doc Parses JSON data.
%% @see parse/2
%% @end
%%--------------------------------------------------------------------
-spec parse(Data) -> Return when
	Data :: string() | binary(),
	Return :: {ok, map()} | {error, term()}.

parse(Data) ->
	parse(Data, #{}).

%%--------------------------------------------------------------------
%% @doc Parses JSON data.
%% @end
%%--------------------------------------------------------------------
-spec parse(Data, Opts) -> Return when
	Data :: string() | binary(),
	Opts :: map(),
	Return :: {ok, map()} | {error, term()}.

parse(Data, Opts) ->
	State = #{
		data => Data,
		opts => Opts
	},
	arweave_config_fsm:init(?MODULE, decode_data, State).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc parses a string or a binary with JSON decoder.
%% @end
%%--------------------------------------------------------------------
decode_data(_State = #{ data := <<>> }) ->
	{ok, #{}};
decode_data(_State = #{ data := [] }) ->
	{ok, #{}};
decode_data(State = #{ data := Data }) ->
	try
		Json = jiffy:decode(Data, [return_maps]),
		NewState = State#{
			json => Json
		},
		{next, parse_config, NewState}
	catch
		_Error:{Position, Reason} ->
			{error, #{
					reason => Reason,
					position => Position
				}
			}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc converts a json into a configuration file
%% @end
%%--------------------------------------------------------------------
parse_config(_State = #{ json := Json }) ->
	arweave_config_serializer:encode(Json).

