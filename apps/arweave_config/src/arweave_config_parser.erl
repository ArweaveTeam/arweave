%%% @doc Arweave configuration key parser. Parses dotted option keys
%%% into the canonical list form and formats them back.
-module(arweave_config_parser).
-export([
    key/1,
    format_key/1,
    format_segment/1,
    is_parameter/1
]).
-define(SEPARATOR, $.).

%% @doc Parse a dotted string into a configuration key list. ASCII
%% only.
%%
%% == Examples ==
%%
%% ```
%% > arweave_config_parser:key("test.2.3.[127.0.0.1:1984].data").
%% {ok,[test,2,3,<<"127.0.0.1:1984">>,data]}
%% '''
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
key2(_Else) ->
    {error, #{ reason => invalid_data }}.

%% @doc Format an option_key list back to its canonical dotted-binary
%% form. Wildcard segments (atoms wrapped in a 1-tuple) format as
%% `<name>`.
-spec format_key(list()) -> binary().
format_key(Key) when is_list(Key) ->
    iolist_to_binary(
        lists:join(<<".">>, [format_segment(S) || S <- Key])).

%% @doc Format a single option_key segment.
-spec format_segment(atom() | integer() | binary() | list() | tuple()) -> binary().
format_segment(A) when is_atom(A) -> atom_to_binary(A);
format_segment(I) when is_integer(I) -> integer_to_binary(I);
format_segment(B) when is_binary(B) -> B;
format_segment(L) when is_list(L) -> unicode:characters_to_binary(L);
format_segment({V}) when is_atom(V) -> <<"<", (atom_to_binary(V))/binary, ">">>.

%% @doc Whether `Key` is already in canonical option_key list form.
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

is_parameter_list([]) -> true;
is_parameter_list([<<>>|_]) -> false;
is_parameter_list([Item|Rest]) when is_atom(Item) ->
    is_parameter_list(Rest);
is_parameter_list([Item|Rest]) when is_integer(Item) ->
    is_parameter_list(Rest);
is_parameter_list([Item|Rest]) when is_binary(Item) ->
    is_parameter_list(Rest);
is_parameter_list(_) -> false.

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

%% Parse a string enclosed by `[` and `]`.
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

%% Convert a parsed key list to its final option form.
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
