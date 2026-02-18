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
%%% @doc Arweave Configuration TOML Format Support.
%%%
%%% @reference https://toml.io/en/
%%% @reference https://github.com/filmor/tomerl
%%% @end
%%%===================================================================
-module(arweave_config_format_toml).
-compile(warnings_as_errors).
-export([
	parse/1,
	parse/2
]).
-export([
	decode_data/1,
	parse_config/1

]).
-include_lib("kernel/include/file.hrl").

%%--------------------------------------------------------------------
%% @doc Parse TOML data.
%% @see parse/2
%% @end
%%--------------------------------------------------------------------
-spec parse(Data) -> Return when
	Data :: string() | binary(),
	Return :: {ok, map()} | {error, term()}.

parse(Data) ->
	parse(Data, #{}).

%%--------------------------------------------------------------------
%% @doc Parse TOML data.
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
decode_data(State = #{ data := Data }) ->
	case tomerl:parse(Data) of
		{ok, Parsed} ->
			{next, parse_config, State#{ config => Parsed }};
		{error, Reason} ->
			{error, Reason};
		Else ->
			Else
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc
%% @end
%%--------------------------------------------------------------------
parse_config(#{ config := Config }) ->
	arweave_config_serializer:encode(Config).
