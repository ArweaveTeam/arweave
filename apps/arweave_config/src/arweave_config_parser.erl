%%%===================================================================
%%% @doc arweave configuration parser.
%%%
%%% This module exports all function required to parse keys and values
%%% for arweave configuration.
%%%
%%% @end
%%%===================================================================
-module(arweave_config_parser).
-export([key/1]).
-include_lib("eunit/include/eunit.hrl").

%%--------------------------------------------------------------------
%% @doc parses a string and converted it to a configuration key. 
%% At this time, only ASCII characters are supported.
%% @end
%%--------------------------------------------------------------------
-spec key(String) -> Return when
	String :: binary() | string(),
	Return :: {ok, [atom() | binary()]}
	        | {error, term()}.

key(List) when is_list(List) ->
	key(list_to_binary(List));
key(Binary) when is_binary(Binary) ->
	key_parse(Binary, <<>>, []).

key_parse(<<>>, <<>>, Key) ->
	key_convert(Key, []);
key_parse(<<>>, Buffer, Key) ->
	key_parse(<<>>, <<>>, [Buffer|Key]);
key_parse(<<".[", Rest/binary>>, <<>>, Key) ->
	key_parse_string(Rest, <<>>, Key);
key_parse(<<".[", Rest/binary>>, Buffer, Key) ->
	key_parse_string(Rest, <<>>, [Buffer|Key]);
key_parse(<<".", Rest/binary>>, Buffer, Key) ->
	key_parse(Rest, <<>>, [Buffer|Key]);
key_parse(<<C:8, Rest/binary>>, Buffer, Key)
	when C >= $0, C =< $9;
	     C >= $A, C =< $Z;
	     C >= $a, C =< $z;
	     C =:= $_ ->
	key_parse(Rest, <<Buffer/binary, C:8>>, Key);
key_parse(<<C:8, Rest/binary>>, Buffer, Key) ->
	{error, #{
			char => <<C>>,
			rest => Rest,
			buffer => Buffer,
			key => Key,
			reason => bad_char
		 }
	}.

key_parse_string(<<"]">>, Buffer, Key) ->
	key_parse(<<>>, <<>>, [{string, Buffer}|Key]);
key_parse_string(<<"].", Rest/binary>>, Buffer, Key) ->
	key_parse(Rest, <<>>, [{string, Buffer}|Key]);
key_parse_string(<<C:8, Rest/binary>>, Buffer, Key) 
	when C >= $!, C =< $/;
	     C >= $0, C =< $9;
	     C >= $?, C =< $Z;
	     C >= $a, C =< $z;
	     C =:= $:;
	     C =:= $=;
	     C =:= $_;
	     C =:= $~ ->
	key_parse_string(Rest, <<Buffer/binary, C:8>>, Key);
key_parse_string(Binary, Buffer, Key) ->
	{error, #{
			rest => Binary,
			buffer => Buffer,
			key => Key,
			reason => bad_string
		 }
	}.

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
			Atom = binary_to_existing_atom(Item),
			key_convert(Rest, [Atom|Buffer])
	end;
key_convert(Rest, Buffer) ->
	{error, #{
			rest => Rest,
			buffer => Buffer,
			reason => bad_key
		 }
	}.

key_test() ->
	?assertEqual(
		{ok, [global, debug]},
		key(<<"global.debug">>)
	),
	?assertEqual(
		{ok, [global, debug]},
		key("global.debug")
	),
	?assertEqual(
		{ok, [storage,3,unpacked,state]},
		key(<<"storage.3.unpacked.state">>)
	),
	?assertEqual(
		{ok, [peers,<<"127.0.0.1:1984">>,trusted]},
		key(<<"peers.[127.0.0.1:1984].trusted">>)
	),
	?assertEqual(
		{error, #{reason => bad_char, buffer => <<>>,
			  char => <<"~">>, rest => <<>>,
			  key => []
			 }
		},
		key(<<"~">>)
	).
