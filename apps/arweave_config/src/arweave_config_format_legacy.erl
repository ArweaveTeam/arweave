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
%%% @doc Arweave Configuration Legacy Format Support.
%%%
%%% @see ar_config
%%% @end
%%% @todo convert config record to arweave_config spec.
%%%===================================================================
-module(arweave_config_format_legacy).
-compile(warnings_as_errors).
-export([
	parse/1,
	parse/2
]).
-export([
	decode_data/1
]).
-include_lib("kernel/include/file.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

%%--------------------------------------------------------------------
%% @doc Parses a JSON using legacy parser.
%% @see parse/2
%% @end
%%--------------------------------------------------------------------
-spec parse(Data) -> Return when
	Data :: string() | binary(),
	Return :: {ok, map()} | {error, term()}.

parse(Data) ->
	parse(Data, #{}).

%%--------------------------------------------------------------------
%% @doc Parses a JSON using legacy parser.
%% @end
%%--------------------------------------------------------------------
-spec parse(Data, Opts) -> Return when
	Data :: string() | binary(),
	Opts :: map(),
	Return :: {ok, map()} | {error, term()}.

parse(Data, Opts) ->
	State = #{
		opts => Opts,
		data => Data
	},
	arweave_config_fsm:init(?MODULE, decode_data, State).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc
%% @end
%%--------------------------------------------------------------------
decode_data(_State = #{ data := ""}) ->
	{ok, #config{}};
decode_data(_State = #{ data := <<>>}) ->
	{ok, #config{}};
decode_data(State = #{ data := Data }) when is_list(Data) ->
	NewState = State#{
		data => list_to_binary(Data)
	},
	decode_data(NewState);
decode_data(_State = #{ data := Data }) ->
	case ar_config:parse(Data) of
		{ok, LegacyConfig} ->
			{ok, LegacyConfig};
		{error, Reason} ->
			{error, Reason};
		Else ->
			Else
	end.
