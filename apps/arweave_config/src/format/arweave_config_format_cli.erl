%%% @doc CLI argument parser for long-form flags (`--foo`, `--foo bar`,
%%% `--foo=bar`).
%%%
%%% `parse/1` is pure: it returns a `#{OptionKey => Value}` map suitable
%%% for `arweave_config:load/1`. Writes to the spec store happen
%%% downstream, not as a side effect of parsing. The `[config_file]`
%%% option is intentionally excluded from the returned map — bootstrap
%%% locates and loads it separately via `find_config_file/1`.
-module(arweave_config_format_cli).
-compile(warnings_as_errors).
-export([parse/1, parse/2]).
-export([find_config_file/1, has_long_flag/1]).

%% @doc Parse command line arguments into a `#{OptionKey => Value}` map.
%% @see parse/2
-spec parse(Args) -> Return when
    Args :: [term()],
    Return :: {ok, map()} | {error, map()}.
parse(Args) ->
    parse(Args, #{}).

%% @doc Parse command-line arguments. Arguments are coerced to
%% binaries internally so callers can pass strings, atoms, integers,
%% floats, or binaries interchangeably.
-spec parse(Args, Opts) -> Return when
    Args :: [term()],
    Opts :: #{long_arguments => map()},
    Return :: {ok, map()} | {error, map()}.
parse(Args, Opts) ->
    try
        BinaryArgs = args_to_binaries(Args),
        LongArgs = maps:get(
            long_arguments,
            Opts,
            maps:from_list(
                arweave_config_options_registry:get_long_arguments()
            )
        ),
        parse_args(BinaryArgs, #{
            la => LongArgs,
            pos => 1,
            buffer => #{}
        })
    catch
        _:R ->
            {error, #{reason => R}}
    end.

has_long_flag(Args) ->
    lists:any(fun is_long_flag/1, Args).

%% @doc Find at most one `--config_file' / `--config_file=PATH' in
%% the arg list. Returns `none' if no flag is present, `{ok, Path}'
%% for a single occurrence, or `{error, _}' if the flag appears more
%% than once or has no value. Accepts any-type args; normalizes
%% internally.
-spec find_config_file(Args) -> Return when
    Args :: [term()],
    Return :: none | {ok, binary()} | {error, term()}.
find_config_file(Args) ->
    find_config_file(args_to_binaries(Args), none, 1).

find_config_file([], Found, _Pos) ->
    Found;
find_config_file([Arg | Rest], Found, Pos) ->
    case config_file_arg(Arg, Rest, Pos) of
        {ok, Path, NewRest, Consumed} ->
            case Found of
                none -> find_config_file(NewRest, {ok, Path}, Pos + Consumed);
                {ok, _} -> {error, multiple_config_files}
            end;
        nomatch ->
            find_config_file(Rest, Found, Pos + 1);
        {error, _} = Err ->
            Err
    end.

config_file_arg(<<"--config_file=", Path/binary>>, Rest, _Pos) ->
    {ok, Path, Rest, 1};
config_file_arg(<<"--config_file">>, [Path | Rest], _Pos) ->
    {ok, Path, Rest, 2};
config_file_arg(<<"--config_file">>, [], Pos) ->
    {error, #{
        reason => <<"missing value">>,
        type => path,
        position => Pos + 1
    }};
config_file_arg(_Arg, _Rest, _Pos) ->
    nomatch.

%%====================================================================
%% Internal
%%====================================================================

%% Coerce mixed-type arg list to binaries before parsing.
args_to_binaries(Args) ->
    args_to_binaries(Args, []).

args_to_binaries([], Buffer) ->
    lists:reverse(Buffer);
args_to_binaries([Arg|Rest], Buffer) when is_list(Arg) ->
    args_to_binaries(Rest, [list_to_binary(Arg) | Buffer]);
args_to_binaries([Arg|Rest], Buffer) when is_integer(Arg) ->
    args_to_binaries(Rest, [integer_to_binary(Arg) | Buffer]);
args_to_binaries([Arg|Rest], Buffer) when is_float(Arg) ->
    args_to_binaries(Rest, [float_to_binary(Arg) | Buffer]);
args_to_binaries([Arg|Rest], Buffer) when is_atom(Arg) ->
    args_to_binaries(Rest, [atom_to_binary(Arg) | Buffer]);
args_to_binaries([Arg|Rest], Buffer) when is_binary(Arg) ->
    args_to_binaries(Rest, [Arg | Buffer]).

is_long_flag("--" ++ _) -> true;
is_long_flag(<<"--", _/binary>>) -> true;
is_long_flag(_) -> false.

-spec parse_args(Args, State) -> Return when
    Args :: [binary()],
    State :: map(),
    Return :: {ok, map()} | {error, map()}.
parse_args([], #{buffer := Buffer}) ->
    {ok, Buffer};
parse_args([Arg = <<"---", _/binary>> | _], #{pos := Pos}) ->
    {error, #{
        reason => <<"bad_argument">>,
        argument => Arg,
        position => Pos
    }};
parse_args([Arg = <<"--", _/binary>> | Rest],
        State = #{la := LA, pos := Pos, buffer := Buffer}) ->
    case split_arg(Arg) of
        %% --flag=value
        {Flag, Value} when is_map_key(Flag, LA) ->
            Spec = maps:get(Flag, LA),
            case decode_value(Spec, Value, Pos) of
                {ok, Decoded} ->
                    parse_args(Rest, State#{
                        pos => Pos + 1,
                        buffer => record(Spec, Decoded, Buffer)
                    });
                {error, _} = Err ->
                    Err
            end;
        %% --flag or --flag value
        Flag when is_map_key(Flag, LA) ->
            Spec = maps:get(Flag, LA),
            case consume_flag(Spec, Rest, Pos, Buffer) of
                {ok, NewRest, NewPos, NewBuffer} ->
                    parse_args(NewRest, State#{
                        pos => NewPos,
                        buffer => NewBuffer
                    });
                {error, _} = Err ->
                    Err
            end;
        _ ->
            unknown_argument(Arg, Pos)
    end;
parse_args([Unknown | _], #{pos := Pos}) ->
    unknown_argument(Unknown, Pos).

unknown_argument(Unknown, Pos) ->
    {error, #{
        reason => <<"unknown argument">>,
        argument => Unknown,
        position => Pos
    }}.

split_arg(Arg) ->
    case binary:split(Arg, <<"=">>) of
        [Flag, Value] -> {Flag, Value};
        [Flag] -> Flag
    end.

%% @doc Consume the next value according to the spec's declared type.
%% A boolean flag with no following value (or an unparseable one) is
%% recorded as `true`.
consume_flag(Spec = #{type := boolean}, [], Pos, Buffer) ->
    {ok, [], Pos + 1, record(Spec, true, Buffer)};
consume_flag(Spec = #{type := boolean}, [Value | Rest] = Args, Pos, Buffer) ->
    case arweave_config_type:boolean(Value) of
        {ok, Decoded} ->
            {ok, Rest, Pos + 2, record(Spec, Decoded, Buffer)};
        _ ->
            {ok, Args, Pos + 1, record(Spec, true, Buffer)}
    end;
consume_flag(Spec, [Value | Rest], Pos, Buffer) ->
    case decode_value(Spec, Value, Pos + 1) of
        {ok, Decoded} -> {ok, Rest, Pos + 2, record(Spec, Decoded, Buffer)};
        {error, _} = Err -> Err
    end;
consume_flag(Spec, _, Pos, _Buffer) ->
    Type = maps:get(type, Spec),
    {error, #{
        reason => <<"missing value">>,
        type => Type,
        position => Pos + 1
    }}.

decode_value(#{type := Type}, Value, Pos) ->
    case arweave_config_type:Type(Value) of
        {ok, Decoded} -> {ok, Decoded};
        _ ->
            {error, #{
                reason => <<"bad value">>,
                value => Value,
                type => Type,
                position => Pos
            }}
    end.

%% Record a parsed (Spec, Value) into the buffer. `[config_file]` is
%% intentionally dropped — bootstrap locates and loads it separately,
%% and including it here would clobber the cumulative path list that
%% bootstrap maintains via `arweave_config_store:set/2`.
record(#{option_key := [config_file]}, _Value, Buffer) ->
    Buffer;
record(Spec, Value, Buffer) ->
    Buffer#{maps:get(option_key, Spec) => Value}.
