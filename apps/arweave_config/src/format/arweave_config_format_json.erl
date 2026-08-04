%%% @doc Arweave Configuration JSON Format Support.
%%%
%%% @reference https://www.json.org/json-en.html
%%% @reference https://github.com/davisp/jiffy
-module(arweave_config_format_json).
-compile(warnings_as_errors).
-export([
    parse/1,
    parse/2,
    decode_maybe/1,
    encode/1,
    encode/2,
    encode_value/1
]).

-define(is_octet(X), (is_integer(X) andalso X >= 0 andalso X =< 255)).

-spec parse(FileContents) -> Return when
    FileContents :: string() | binary(),
    Return :: {ok, map()} | {error, term()}.
parse(FileContents) ->
    parse(FileContents, #{}).

-spec parse(FileContents, Opts) -> Return when
    FileContents :: string() | binary(),
    Opts :: map(),
    Return :: {ok, map()} | {error, term()}.
parse(<<>>, _Opts) ->
    {ok, #{}};
parse([], _Opts) ->
    {ok, #{}};
parse(FileContents, _Opts) ->
    try
        Json = jiffy:decode(FileContents, [return_maps]),
        parse_map(Json)
    catch
        _Error:{Position, Reason} ->
            {error, #{
                    reason => Reason,
                    position => Position
                }
            }
    end.

parse_map(Json) when is_map(Json) ->
    arweave_config_format_dotted:to_leaf_map(Json);
parse_map(_) ->
    {error, #{ reason => root_not_object }}.

%% @doc Decode a single JSON-shaped value, falling back to the input.
%% A value whose first non-space character is `[' or `{' is decoded as
%% JSON (objects as maps), so list- and map-typed options can be given
%% as one quoted argument. Anything else — including a value that
%% starts JSON-shaped but fails to decode — is returned unchanged and
%% left to the option's scalar type coercion, so a typo surfaces as
%% the usual type error rather than a JSON error. Shared by the
%% startup CLI/env parsers and the runtime `config set' facade.
-spec decode_maybe(Value) -> Return when
    Value :: string() | binary() | term(),
    Return :: term().
decode_maybe(Value) when is_binary(Value); is_list(Value) ->
    case json_shaped(Value) of
        true ->
            try
                jiffy:decode(
                    unicode:characters_to_binary(Value),
                    [return_maps])
            catch
                _:_ -> Value
            end;
        false ->
            Value
    end;
decode_maybe(Value) ->
    Value.

json_shaped(Value) when is_binary(Value) ->
    case string:trim(Value, leading) of
        <<C, _/binary>> when C =:= $[; C =:= ${ -> true;
        _ -> false
    end;
json_shaped(Value) when is_list(Value) ->
    case string:trim(Value, leading) of
        [C | _] when C =:= $[; C =:= ${ -> true;
        _ -> false
    end.

-spec encode(Data) -> Return when
    Data :: map(),
    Return :: {ok, binary()} | {error, term()}.
encode(Data) ->
    encode(Data, #{}).

-spec encode(Data, Opts) -> Return when
    Data :: map(),
    Opts :: map(),
    Return :: {ok, binary()} | {error, term()}.
encode(Data, _Opts) when is_map(Data) ->
    try
        {ok, iolist_to_binary(jiffy:encode(encode_value(Data), [pretty]))}
    catch
        _:Reason ->
            {error, Reason}
    end;
encode(Data, _) ->
    {error, {invalid_data, Data}}.

%% @doc Convert a configuration value into a jiffy-encodable term
%% using the canonical config spellings. Handles file-shaped values
%% and the runtime-only shapes a live store holds: resolved peer
%% tuples render as `ip:port' and raw (non-printable) binaries —
%% addresses — render base64url. Also used by `arweave_config_cli' to
%% display values in the exact form `config set' accepts.
encode_value({A, B, C, D, Port})
        when ?is_octet(A), ?is_octet(B), ?is_octet(C), ?is_octet(D),
             is_integer(Port), Port >= 0, Port =< 65535 ->
    iolist_to_binary(io_lib:format("~b.~b.~b.~b:~b", [A, B, C, D, Port]));
encode_value(Map) when is_map(Map) ->
    maps:from_list(
        [
            {arweave_config_parser:format_segment(Key), encode_value(Value)}
            || {Key, Value} <- maps:to_list(Map)
        ]
    );
encode_value([]) ->
    %% An empty list encodes as an empty JSON array. Special-cased
    %% because `io_lib:printable_unicode_list([])' is `true', which
    %% would otherwise render `[]' as an empty string.
    [];
encode_value(List) when is_list(List) ->
    case io_lib:printable_unicode_list(List) of
        true ->
            unicode:characters_to_binary(List);
        false ->
            [encode_value(Value) || Value <- List]
    end;
encode_value(Binary) when is_binary(Binary) ->
    case unicode:characters_to_list(Binary) of
        L when is_list(L) ->
            case io_lib:printable_unicode_list(L) of
                true -> Binary;
                false -> b64fast:encode(Binary)
            end;
        _ ->
            b64fast:encode(Binary)
    end;
encode_value(true) ->
    true;
encode_value(false) ->
    false;
encode_value(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom);
encode_value(Value) ->
    Value.

