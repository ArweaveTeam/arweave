%%% @doc CLI-facing facade for `./bin/arweave config get/set`.
%%%
%%% Parses the user-facing string key — dotted (`mining.address`) or
%%% single-segment (`debug`) — into the canonical option_key, then
%%% delegates to `arweave_config:get/1` or `arweave_config:set/2`.
%%%
%%% `get/1' returns a flat display string rather than the raw term:
%%% `erl_call -a' renders large binaries as opaque `#Bin<...>' dumps,
%%% so values are formatted here, where the full term is visible.
-module(arweave_config_cli).
-compile(warnings_as_errors).
-compile({no_auto_import,[get/1]}).
-export([get/1, set/2]).

%% @doc Read a configuration value by its canonical key in dotted
%% form (or as a single segment). Returns the value formatted as a
%% flat string: binaries render as their text (`http://...' instead
%% of `#Bin<...>'), lists and maps render with their elements
%% formatted recursively, and malformed keys render as the error
%% term.
-spec get(string()) -> string().
get(StringKey) ->
    Value =
        case arweave_config_parser:key(StringKey) of
            {ok, Key} -> arweave_config:get(Key);
            {error, _} = Err -> Err
        end,
    unicode:characters_to_list(format_value(Value)).

%% @doc Set a configuration value by its canonical key, passing the
%% value through the spec's type coercion.
-spec set(string(), string()) -> ok | {error, term()}.
set(StringKey, StringValue) ->
    case arweave_config_parser:key(StringKey) of
        {ok, Key} -> arweave_config:set(Key, StringValue);
        {error, _} = Err -> Err
    end.

%%====================================================================
%% Internal
%%====================================================================

%% Human-oriented value rendering. Text-like data prints as text; the
%% structure of lists and maps is kept but their elements are
%% formatted recursively; anything else falls back to `~tp'.
format_value(V) when is_binary(V) ->
    case unicode:characters_to_list(V) of
        L when is_list(L) ->
            case io_lib:printable_unicode_list(L) of
                true -> L;
                false -> io_lib:format("~tp", [V])
            end;
        _ ->
            io_lib:format("~tp", [V])
    end;
format_value(V) when is_atom(V) ->
    atom_to_list(V);
format_value(V) when is_integer(V) ->
    integer_to_list(V);
format_value(V) when is_float(V) ->
    float_to_list(V, [{decimals, 10}, compact]);
format_value(V) when is_list(V) ->
    case io_lib:printable_unicode_list(V) of
        true ->
            V;
        false ->
            ["[", lists:join(", ", [format_value(E) || E <- V]), "]"]
    end;
format_value(V) when is_map(V) ->
    Pairs = [
        [format_value(K), " => ", format_value(Val)]
     || {K, Val} <- lists:sort(maps:to_list(V))
    ],
    ["#{", lists:join(", ", Pairs), "}"];
format_value(V) ->
    io_lib:format("~tp", [V]).
