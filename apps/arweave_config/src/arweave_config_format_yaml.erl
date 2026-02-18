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
%%% @doc Arweave Configuration YAML Format Support.
%%%
%%% @reference https://yaml.org/
%%% @reference https://github.com/yakaz/yamerl/
%%% @end
%%%===================================================================
-module(arweave_config_format_yaml).
-compile(warnings_as_errors).
-export([
	parse/1,
	parse/2,
	proplist_to_map/1
]).
-export([
	decode_data/1,
	decode_proplist/1,
	parse_config/1

]).
-include_lib("kernel/include/file.hrl").
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% @doc Parse YAML data.
%% @see parse/2
%% @end
%%--------------------------------------------------------------------
-spec parse(Data) -> Return when
	Data :: string() | binary(),
	Return :: {ok, map()} | {error, term()}.

parse(Data) ->
	parse(Data, #{}).

%%--------------------------------------------------------------------
%% @doc Parse YAML data.
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
%% @doc convert yaml data to erlang terms.
%% @end
%%--------------------------------------------------------------------
decode_data(State = #{ data := Data }) ->
	try
		yamerl:decode(Data)
	of
		[] ->
			NewState = State#{ proplist => [] },
			{next, decode_proplist, NewState};
		[Proplist] ->
			NewState = State#{ proplist => Proplist },
			{next, decode_proplist, NewState};
		_Else ->
			{error, multi_yaml_unsupported}
	catch
		_:Reason ->
			{error, Reason}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc convert proplist to map.
%% @end
%%--------------------------------------------------------------------
decode_proplist(State = #{ proplist := Proplist }) ->
	try
		Parsed = proplist_to_map(Proplist),
		NewState = State#{ config => Parsed },
		{next, parse_config, NewState}
	catch
		_:Reason ->
			{error, Reason}
	end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc returns the final configuration spec.
%% @end
%%--------------------------------------------------------------------
parse_config(#{ config := Config }) ->
	arweave_config_serializer:encode(Config).

%%--------------------------------------------------------------------
%% @doc recursively convert a proplist to a map. `yamerl' does not
%% return a map and then we need to convert the proplist returned
%% recursively.
%% @end
%%--------------------------------------------------------------------
-spec proplist_to_map(Proplist) -> Return when
	Proplist :: proplists:proplist(),
	Return :: map().

proplist_to_map(Proplist) ->
	proplist_to_map(Proplist, #{}).

proplist_to_map_test() ->
	?assertEqual(
		#{},
		proplist_to_map([])
	),

	?assertEqual(
		#{ <<"1">> => 2 },
		proplist_to_map([{1,2}])
	),

	?assertEqual(
		#{
			<<"test">> => #{
				<<"a">> => <<"b">>,
				<<"c">> => <<"d">>
			}
		},
		proplist_to_map([
			{test,[
				{"a", "b"},
				{c, d}
			]}
		])
	).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc
%% @end
%%--------------------------------------------------------------------
proplist_to_map([], Buffer) ->
	Buffer;
proplist_to_map([{K,V = [{_,_}|_]}|Rest], Buffer) ->
	% if a value is made of tuple, we assume this is an object,
	% then, we convert it to map.
	Recurse = proplist_to_map(V),
	Key = converter_key(K),
	proplist_to_map(Rest, Buffer#{ Key => Recurse });
proplist_to_map([{K, V}|Rest], Buffer) when is_list(V) ->
	% if a value is a list we assume this is a string and it
	% is converted to binary.
	Key = converter_key(K),
	Value = converter_value(V),
	proplist_to_map(Rest, Buffer#{ Key => Value });
proplist_to_map([{K,V}|Rest], Buffer) ->
	Key = converter_key(K),
	Value = converter_value(V),
	proplist_to_map(Rest, Buffer#{ Key => Value }).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc
%% @end
%%--------------------------------------------------------------------
converter_key(Key) when is_integer(Key) ->
	integer_to_binary(Key);
converter_key(Key) when is_atom(Key) ->
	atom_to_binary(Key);
converter_key(Key) when is_list(Key) ->
	list_to_binary(Key);
converter_key(Key) ->
	Key.

converter_value(Value) when is_atom(Value) ->
	atom_to_binary(Value);
converter_value(Value) when is_list(Value) ->
	list_to_binary(Value);
converter_value(Value) ->
	Value.
