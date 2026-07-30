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
-export([get/1, get_base64/1, set/2, set_base64/2]).

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
%%
%% Scalar values pass through as the raw string. A value whose first
%% non-space character is `[' or `{' is decoded as JSON first, so
%% list- and map-typed runtime options are settable from the CLI:
%%
%%   config set peers.local '["1.2.3.4:1984", "peer.example:1984"]'
%%
%% A value that starts JSON-shaped but fails to decode falls back to
%% the raw string (and then fails the option's type coercion with the
%% usual error, rather than a JSON error masking a scalar typo).
-spec set(string(), string()) -> ok | {error, term()}.
set(StringKey, StringValue) ->
    case arweave_config_parser:key(StringKey) of
        {ok, Key} -> arweave_config:set(Key, decode_value(StringValue));
        {error, _} = Err -> Err
    end.

%% @doc `get/1' with the result base64-encoded. The shell CLI cannot
%% render erl_call's return value verbatim (the term printer quotes
%% strings and there is no bare-string term type), so the display
%% string travels base64 — quote-free by construction — and the shell
%% just decodes it. Symmetric with `set_base64/2'.
-spec get_base64(string()) -> string().
get_base64(StringKey) ->
    %% Returned as a charlist: erl_call renders charlists as quoted
    %% strings, but binaries as opaque #Bin<...> dumps.
    unicode:characters_to_list(
        base64:encode(unicode:characters_to_binary(get(StringKey)))).

%% @doc `set/2' with base64-encoded arguments. `./bin/arweave config
%% set' ships key and value through `erl_call -a', which parses its
%% argument string as Erlang terms — a value containing quotes (e.g. a
%% JSON array) shreds that parse no matter how the shell escapes it.
%% Base64 keeps the transported tokens quote-free; the real key/value
%% are recovered here.
-spec set_base64(string() | binary(), string() | binary()) ->
    ok | {error, term()}.
set_base64(KeyB64, ValueB64) ->
    Key = unicode:characters_to_list(
        base64:decode(iolist_to_binary(KeyB64))),
    Value = unicode:characters_to_list(
        base64:decode(iolist_to_binary(ValueB64))),
    set(Key, Value).

%%====================================================================
%% Internal
%%====================================================================

%% JSON-shaped set values decode into real terms; anything else stays
%% the raw string for scalar type coercion.
decode_value(StringValue) ->
    case string:trim(StringValue, leading) of
        [C | _] when C =:= $[; C =:= ${ ->
            try
                jiffy:decode(
                    unicode:characters_to_binary(StringValue),
                    [return_maps])
            catch
                _:_ -> StringValue
            end;
        _ ->
            StringValue
    end.

%% Value rendering, symmetric with `decode_value/1': scalars render
%% bare (`true', `200', `/opt/data'), containers render as **JSON** —
%% the exact form `config set' accepts — so `get' output pastes
%% straight back into `set'. Peers render as their canonical
%% `ip:port' spelling (hostnames are resolved at load time, so the
%% original hostname is not recoverable); non-printable binaries
%% (addresses) render base64url, matching their config-file spelling.
format_value({A, _, _, _, _} = Peer) when is_integer(A) ->
    case arweave_config_format_json:encode_value(Peer) of
        B when is_binary(B) -> B;
        _ -> io_lib:format("~tp", [Peer])
    end;
format_value([]) ->
    %% An empty list renders as the JSON empty array, not an empty
    %% string (`io_lib:printable_unicode_list([])' is `true', which
    %% would otherwise display nothing — and an empty display string
    %% breaks the base64 get transport).
    "[]";
format_value(V) when is_list(V) ->
    case io_lib:printable_unicode_list(V) of
        true -> V;
        false -> jiffy:encode(arweave_config_format_json:encode_value(V))
    end;
format_value(V) when is_map(V) ->
    jiffy:encode(arweave_config_format_json:encode_value(V));
format_value(V) when is_binary(V) ->
    unicode:characters_to_list(
        arweave_config_format_json:encode_value(V));
format_value(V) when is_atom(V) ->
    atom_to_list(V);
format_value(V) when is_integer(V) ->
    integer_to_list(V);
format_value(V) when is_float(V) ->
    float_to_list(V, [{decimals, 10}, compact]);
format_value(V) ->
    io_lib:format("~tp", [V]).
