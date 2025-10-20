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
%%% @doc arweave configuration parser.
%%%
%%% This module exports all function required to parse keys and values
%%% for arweave configuration.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_parser).
-export([
	separator/0,
	key/1,
	is_parameter/1
]).
-include_lib("eunit/include/eunit.hrl").
-define(SEPARATOR, $.).

%%--------------------------------------------------------------------
%% @doc default separator used.
%% @end
%%--------------------------------------------------------------------
separator() -> ?SEPARATOR.

%%--------------------------------------------------------------------
%% @doc parses a string and converted it to a configuration key.
%% At this time, only ASCII characters are supported.
%%
%% == Examples ==
%%
%% ```
%% > arweave_config_parser:key("test.2.3.[127.0.0.1:1984].data").
%% {ok,[test,2,3,<<"127.0.0.1:1984">>,data]}
%% '''
%%
%% @TODO check indepth list.
%% @end
%%--------------------------------------------------------------------
-spec key(Key) -> Return when
	Key :: atom() | binary() | string(),
	Return :: {ok, [atom() | binary()]}
	        | {error, map()}.

key(Key) ->
	case is_parameter(Key) of
		true ->
			{ok, Key};
		false ->
			key2(Key)
	end.

key2(Atom) when is_atom(Atom) ->
	key2(atom_to_binary(Atom));
key2(List) when is_list(List) ->
	try
		key2(list_to_binary(List))
	catch
		_:_ ->
			{error, #{ reason => invalid_data }}
	end;
key2(Binary) when is_binary(Binary) ->
	key_parse(Binary, <<>>, [], 1);
key2(_Elsewise) ->
	{error, #{ reason => invalid_data }}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
is_parameter([]) -> false;
is_parameter(List) when is_list(List) ->
	try
		_ = list_to_binary(List),
		false
	catch
		_:_ ->
			is_parameter_list(List)
	end;
is_parameter(_) -> false.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
is_parameter_list([]) -> true;
is_parameter_list([<<>>|_]) -> false;
is_parameter_list([Item|Rest]) when is_atom(Item) ->
	is_parameter_list(Rest);
is_parameter_list([Item|Rest]) when is_integer(Item) ->
	is_parameter_list(Rest);
is_parameter_list([Item|Rest]) when is_binary(Item) ->
	is_parameter_list(Rest);
is_parameter_list(_) -> false.

%%--------------------------------------------------------------------
%% @hidden
%% @doc parse the key and convert it to parameter format.
%% @end
%%--------------------------------------------------------------------
key_parse(<<>>, <<>>, [], 1) ->
	{error, #{
			position => 1,
			reason => empty_key
		}
	};
key_parse(<<?SEPARATOR>>, _Buffer, _Key, Pos) ->
	{error, #{
			position => Pos,
			reason => separator_ending
		}
	};
key_parse(<<?SEPARATOR, ?SEPARATOR, _Rest/binary>>, _Buffer, _Key, Pos) ->
	{error, #{
			position => Pos,
			reason => multi_separators
		}
	};
key_parse(<<?SEPARATOR, Rest/binary>>, Buffer, [], 1) ->
	key_parse(Rest, Buffer, [], 2);
key_parse(<<>>, <<>>, Key, _Pos) ->
	key_convert(Key, []);
key_parse(<<>>, Buffer, Key, Pos) ->
	key_parse(<<>>, <<>>, [Buffer|Key], Pos);
key_parse(<<?SEPARATOR, "[", Rest/binary>>, <<>>, Key, Pos) ->
	key_parse_string(Rest, <<>>, Key, Pos+2);
key_parse(<<?SEPARATOR, "[", Rest/binary>>, Buffer, Key, Pos) ->
	key_parse_string(Rest, <<>>, [Buffer|Key], Pos+2);
key_parse(<<?SEPARATOR, Rest/binary>>, Buffer, Key, Pos) ->
	key_parse(Rest, <<>>, [Buffer|Key], Pos+1);
key_parse(<<C:8, Rest/binary>>, Buffer, Key, Pos)
	when C >= $0, C =< $9;
	     C >= $A, C =< $Z;
	     C >= $a, C =< $z;
	     C =:= $_ ->
	key_parse(Rest, <<Buffer/binary, C:8>>, Key, Pos+1);
key_parse(<<C:8, Rest/binary>>, Buffer, Key, Pos) ->
	{error, #{
			char => <<C>>,
			rest => Rest,
			buffer => Buffer,
			position => Pos,
			key => Key,
			reason => bad_char
		 }
	}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc parse a string enclosed by `[' and `]'.
%% @end
%%--------------------------------------------------------------------
key_parse_string(<<"]">>, Buffer, Key, Pos) ->
	key_parse(<<>>, <<>>, [{string, Buffer}|Key], Pos+1);
key_parse_string(<<"]", ?SEPARATOR, Rest/binary>>, Buffer, Key, Pos) ->
	key_parse(Rest, <<>>, [{string, Buffer}|Key], Pos+2);
key_parse_string(<<C:8, Rest/binary>>, Buffer, Key, Pos)
	when C >= $!, C =< $/;
	     C >= $0, C =< $9;
	     C >= $?, C =< $Z;
	     C >= $a, C =< $z;
	     C =:= $:;
	     C =:= $=;
	     C =:= $_;
	     C =:= $~ ->
	key_parse_string(Rest, <<Buffer/binary, C:8>>, Key, Pos+1);
key_parse_string(Binary, Buffer, Key, Pos) ->
	{error, #{
			rest => Binary,
			buffer => Buffer,
			position => Pos,
			key => Key,
			reason => bad_string
		 }
	}.

%%--------------------------------------------------------------------
%% @hidden
%% @doc convert a key to its final parameter form.
%% @end
%%--------------------------------------------------------------------
key_convert([], Buffer) ->
	{ok, Buffer};
key_convert([{string, Value}|Rest], Buffer) ->
	key_convert(Rest, [Value|Buffer]);
key_convert([Item|Rest], Buffer) ->
	try
		Integer = binary_to_integer(Item),
		key_convert(Rest, [Integer|Buffer])
	catch
		_:_ ->
			key_convert_to_atom(Item, Rest, Buffer)
	end;
key_convert(Rest, Buffer) ->
	{error, #{
			rest => Rest,
			buffer => Buffer,
			reason => bad_key
		 }
	}.

key_convert_to_atom(Item, Rest, Buffer) ->
	try
		Atom = binary_to_existing_atom(Item),
		key_convert(Rest, [Atom|Buffer])
	catch
		_:_ ->
			{error, #{
					reason => invalid_key,
					key => Item
				}
			}
	end.

key_test() ->
	?assertEqual(
		{ok, [global, debug]},
		key(<<
			?SEPARATOR,
			"global",
			?SEPARATOR,
			"debug"
		 >>)
	),
	?assertEqual(
		{ok, [global, debug]},
		key(
			[?SEPARATOR]
			++ "global"
			++ [?SEPARATOR]
			++ "debug"
		)
	),
	?assertEqual(
		{ok, [storage,3,unpacked,state]},
		key(<<
			?SEPARATOR,
			"storage",
			?SEPARATOR,
			"3",
			?SEPARATOR,
			"unpacked",
			?SEPARATOR,
			"state"
		    >>)
	),
	?assertEqual(
		{ok, [peers,<<"127.0.0.1:1984">>,trusted]},
		key(<<
			?SEPARATOR,
			"peers",
			?SEPARATOR,
			"[127.0.0.1:1984]",
			?SEPARATOR,
			"trusted"
		>>)
	),
	?assertEqual(
		{error, #{
				reason => bad_char,
				buffer => <<>>,
				char => <<"~">>,
				rest => <<>>,
				key => [],
				position => 1
			}
		},
		key(<<"~">>)
	),
	?assertEqual(
		{error, #{
				reason => empty_key,
				position => 1
			}
		},
		key("")
	),
	?assertEqual(
		{error, #{
				reason => invalid_key,
				key => <<"totally_random_key">>
			}
		},
		key("totally_random_key")
	),
	?assertEqual(
		{error, #{
				reason => multi_separators,
				position => 1
			}
		},
		key(<<
			?SEPARATOR,
			?SEPARATOR,
			"test"
		>>)
	),
	?assertEqual(
		{error, #{
				reason => multi_separators,
				position => 6
			}
		},
		key(<<
			?SEPARATOR,
			"test",
			?SEPARATOR, ?SEPARATOR,
			"test"
		>>)
	),
	?assertEqual(
		{error, #{
				reason => separator_ending,
				position => 5
			}
		},
		key(<<
			"test",
			?SEPARATOR
		>>)
	).
