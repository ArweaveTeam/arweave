%%% @doc Arweave Configuration Type Definition.
-module(arweave_config_type).
-compile(warnings_as_errors).
-export([
    boolean/1,
    integer/1,
    list/1,
    list_map/1,
    pos_integer/1,
    ipv4/1,
    file/1,
    tcp_port/1,
    path/1,
    atom/1,
    string/1,
    logging_template/1,
    peer_id/1,
    peers_list/1,
    resolved_peer_id/1,
    resolved_peers_list/1,
    address/1,
    storage_modules/1,
    repack_modules/1
]).
-include_lib("kernel/include/file.hrl").
-include_lib("kernel/include/logger.hrl").

-define(DEFAULT_PORT, 1984).
-define(is_octet(X), (is_integer(X) andalso X >= 0 andalso X =< 255)).

-type peer_id() :: {byte(), byte(), byte(), byte(), 0..65535} | binary().

%% @doc Validate as an atom, converting list/binary inputs through
%% `binary_to_existing_atom/1` / `list_to_existing_atom/1`.
-spec atom(Input) -> Return when
    Input :: string() | binary() | atom(),
    Return :: {ok, atom()} | {error, Input}.
atom(List) when is_list(List) ->
    try {ok, list_to_existing_atom(List)}
    catch _:_ -> {error, List}
    end;
atom(Binary) when is_binary(Binary) ->
    try {ok, binary_to_existing_atom(Binary)}
    catch _:_ -> {error, Binary}
    end;
atom(V) when is_atom(V) -> {ok, V};
atom(V) -> {error, V}.

%% @doc Validate as a boolean. Strings are matched case-insensitively
%% against `true` / `false` / `on` / `off`.
%%
%% == Examples ==
%%
%% ```
%% {ok, true} = boolean(true).
%% {ok, true} = boolean(<<"true">>).
%% {ok, true} = boolean("true").
%% {ok, true} = boolean("on").
%% {ok, true} = boolean(<<"TruE">>).
%% '''
%%
-spec boolean(Input) -> Return when
    Input :: string() | binary() | boolean(),
    Return :: {ok, boolean()} | {error, Input}.
boolean(true) -> {ok, true};
boolean(on) -> {ok, true};
boolean(false) -> {ok, false};
boolean(off) -> {ok, false};
boolean(String) when is_list(String); is_binary(String) ->
    Regexp = "^(?:(?<f>false|off)|(?<t>true|on))$",
    Opts = [extended, caseless, {capture, all_names, binary}],
    case re:run(String, Regexp, Opts) of
        {match, [<<>>, _True]} -> {ok, true};
        {match, [_False, <<>>]} -> {ok, false};
        _ -> {error, String}
    end;
boolean(V) -> {error, V}.

%% @doc Validate as a list of binaries; richer element validation
%% belongs in the owning option module's validator.
-spec list(Input) -> Return when
    Input :: [binary()],
    Return :: {ok, [binary()]} | {error, Input}.
list(Values) when is_list(Values) ->
    case lists:all(fun is_binary/1, Values) of
        true -> {ok, Values};
        false -> {error, Values}
    end;
list(Value) ->
    {error, Value}.

%% @doc Validate and normalize a list of peers, keeping hostnames as
%% `<<"host:port">>' binaries (IPv4 entries become `{A,B,C,D,Port}'
%% tuples). Accepts mixed binaries / strings / tuples.
-spec peers_list(Input) -> Return when
    Input :: [binary() | string() | tuple()] | binary() | tuple(),
    Return :: {ok, [peer_id()]} | {error, term()}.
peers_list(Input) ->
    do_peers_list(Input, false).

%% Shared list machinery for `peers_list/1' and `resolved_peers_list/1'.
%% `Resolve' selects the per-entry step: keep the spelling (`false') or
%% resolve a hostname to one-or-more IPv4 peers (`true').
do_peers_list([], _Resolve) ->
    %% An empty list is an empty peer set, not a bare single-peer string.
    %% `io_lib:printable_unicode_list([])' is `true', so this must be
    %% matched before the printable-string case below.
    {ok, []};
do_peers_list(Values, Resolve) when is_list(Values) ->
    case io_lib:printable_unicode_list(Values) of
        true ->
            %% Bare CLI/env string for a single peer (`--peers.trusted
            %% 1.2.3.4:1984') — wrap into a singleton list.
            do_singleton(Values, Resolve);
        false ->
            do_peers_list(Values, Resolve, [])
    end;
do_peers_list(Value, Resolve) when is_binary(Value); is_tuple(Value) ->
    %% Same single-peer case as above for binary or tuple input.
    do_singleton(Value, Resolve);
do_peers_list(Value, _Resolve) ->
    {error, {invalid_peer, Value}}.

%% A scalar input that entirely fails to parse is a typo, not a stale
%% DNS entry: reject it instead of warn-skipping into an empty list —
%% otherwise `config set peers.local <garbage>' silently wipes the
%% list while returning ok. (The per-entry warn-skip below still
%% applies to entries WITHIN a list.)
do_singleton(Value, Resolve) ->
    case do_peers_list([Value], Resolve, []) of
        {ok, []} -> {error, {invalid_peer, Value}};
        Other -> Other
    end.

do_peers_list([], _Resolve, Acc) ->
    %% usort mirrors the legacy writer (normalize_peers): duplicates
    %% collapse and the loaded order is canonical regardless of the
    %% spelling order in the config file.
    {ok, lists:usort(Acc)};
do_peers_list([Peer | Rest], true = Resolve, Acc) ->
    case resolve_peer(Peer) of
        {ok, PeerIDs} ->
            do_peers_list(Rest, Resolve, PeerIDs ++ Acc);
        {error, _} ->
            %% Mirror the legacy parser's tolerance: an entry that fails
            %% to resolve (e.g. a temporarily-dead hostname) is warned
            %% about and skipped, never fatal for the whole list — a
            %% stale DNS name must not stop a node from booting.
            ?LOG_WARNING([{event, invalid_peer_in_config},
                          {peer, Peer}, {action, ignored}]),
            do_peers_list(Rest, Resolve, Acc)
    end;
do_peers_list([Peer | Rest], false = Resolve, Acc) ->
    %% When we're not resolving a peer, a bad entry is a genuine typo and
    %% stays a hard error.
    case peer_id(Peer) of
        {ok, PeerID} -> do_peers_list(Rest, Resolve, [PeerID | Acc]);
        {error, _} = Err -> Err
    end.

%% @doc Validate the outer shape of a list of maps. The owning
%% list root spec validates fields using its `{list_item}` schema.
list_map(Values) when is_list(Values) ->
    case lists:all(fun is_map/1, Values) of
        true -> {ok, Values};
        false -> {error, Values}
    end;
list_map(Value) ->
    {error, Value}.

%% @doc Validate and normalize a `[storage_modules]` value: a list
%% whose entries are canonical maps (kept as-is) or runtime-dialect
%% `{RangeStart, RangeEnd, Packing}` tuples (converted to their
%% canonical map). Mirrors how `peers_list/1` normalizes peers at set
%% time, so callers can pass runtime tuples directly to `set`.
storage_modules(Values) when is_list(Values) ->
    try
        {ok, [arweave_config_options_storage_modules:normalize_entry(V)
            || V <- Values]}
    catch
        _:_ -> {error, Values}
    end;
storage_modules(Value) ->
    {error, Value}.

%% @doc Same as `storage_modules/1` for `[repack_modules]`: entries
%% are canonical maps or runtime-dialect `{{RangeStart, RangeEnd,
%% FromPacking}, ToPacking}` pairs.
repack_modules(Values) when is_list(Values) ->
    try
        {ok, [arweave_config_options_repack_modules:normalize_entry(V)
            || V <- Values]}
    catch
        _:_ -> {error, Values}
    end;
repack_modules(Value) ->
    {error, Value}.

%% @doc Validate as an integer.
-spec integer(Integer) -> Return when
    Integer :: list() | binary() | integer(),
    Return :: {ok, integer()} | {error, term()}.
integer(List) when is_list(List) ->
    try integer(list_to_integer(List))
    catch _:_ -> {error, List} end;
integer(Binary) when is_binary(Binary) ->
    try integer(binary_to_integer(Binary))
    catch _:_ -> {error, Binary} end;
integer(Integer) when is_integer(Integer) ->
    {ok, Integer};
integer(V) ->
    {error, V}.

%% @doc Validate as a positive integer, or the atom `infinity'.
%% `infinity' is accepted so options can carry a sentinel meaning
%% "no bound" without giving up the type's positivity guarantee.
-spec pos_integer(Integer) -> Return when
    Integer :: list() | binary() | pos_integer() | infinity,
    Return :: {ok, pos_integer() | infinity} | {error, term()}.
pos_integer(infinity) ->
    {ok, infinity};
pos_integer(<<"infinity">>) ->
    {ok, infinity};
pos_integer("infinity") ->
    {ok, infinity};
pos_integer(Data) ->
    case integer(Data) of
        {ok, Integer} when Integer >= 0 ->
            {ok, Integer};
        _Else ->
            {error, Data}
    end.

%% @doc Validate as an IPv4 address.
-spec ipv4(IPv4) -> Return when
    IPv4 :: inet:ip4_address() | binary() | list(),
    Return :: {ok, list()} | {error, term()}.
ipv4(Tuple = {_, _, _, _}) ->
    case inet:is_ipv4_address(Tuple) of
        true ->
            ipv4(inet:ntoa(Tuple));
        false ->
            {error, Tuple}
    end;
ipv4(Binary) when is_binary(Binary) ->
    ipv4(binary_to_list(Binary));
ipv4(List) when is_list(List) ->
    case inet:parse_strict_address(List, inet) of
        {ok, _} ->
            {ok, list_to_binary(List)};
        _Else ->
            {error, List}
    end;
ipv4(Else) ->
    {error, Else}.

%% @doc File path type. Note: unix socket paths longer than 108 bytes
%% will fail kernel-side; this validator does not catch that.
-spec file(File) -> Return when
    File :: binary() | list(),
    Return :: {ok, binary()} | {error, term()}.
file(List) when is_list(List) ->
    file(list_to_binary(List));
file(Binary) when is_binary(Binary) ->
    case filename:pathtype(Binary) of
        absolute ->
            file2(Binary);
        relative ->
            {ok, Cwd} = file:get_cwd(),
            Absolute = filename:join(Cwd, Binary),
            file2(Absolute)
    end;
file(Path) ->
    type_error(
        file,
        <<"unsupported format">>,
        #{ path => Path }
     ).

%% arweave_config does not create the parent directory.
file2(Path) ->
    Split = filename:split(Path),
    [_Filename|Reverse] = lists:reverse(Split),
    Directory = filename:join(lists:reverse(Reverse)),
    case filelib:is_dir(Directory) of
        true ->
            file3(Path, Directory);
        false ->
            type_error(
                file,
                <<"directory not found">>,
                #{
                    path => Path,
                    directory => Directory
                 }
            )
    end.

file3(Path, Directory) ->
    Split = filename:split(Path),
    [_Filename|_] = lists:reverse(Split),
    case file:read_file_info(Directory) of
        {ok, #file_info{access = read_write }} ->
            file4(Path);
        {error, Reason} ->
            type_error(
                file,
                Reason,
                #{ path => Path }
            )
    end.

file4(Path) when is_list(Path) ->
    {ok, list_to_binary(Path)};
file4(Path) ->
    {ok, Path}.

%% @doc Validate as a TCP port (0..65535).
-spec tcp_port(Port) -> Return when
    Port :: pos_integer(),
    Return :: {ok, pos_integer()} | {error, term()}.
tcp_port(Binary) when is_binary(Binary) ->
    tcp_port(binary_to_integer(Binary));
tcp_port(List) when is_list(List) ->
    tcp_port(list_to_integer(List));
tcp_port(Integer) when is_integer(Integer) ->
    case Integer of
        _ when Integer >= 0, Integer =< 65535 ->
            {ok, Integer};
        _ ->
            {error, Integer}
    end.

%% @doc Validate as a filesystem path.
path(List) when is_list(List) ->
    path(list_to_binary(List));
path(Binary) when is_binary(Binary) ->
    case filename:validate(Binary) of
        true ->
            path_relative(Binary);
        false ->
            {error, Binary}
    end.

path_relative(Path) ->
    case filename:pathtype(Path) of
        relative ->
            {ok, filename:absname(Path)};
        absolute ->
            {ok, Path}
    end.

%% @doc String type validator. Accepts proper character lists and
%% binaries (the latter for JSON/YAML inputs, which deserialize
%% strings as binaries).
-spec string(String) -> Return when
    String :: list() | binary(),
    Return :: {ok, list()} | {error, term()}.
string(Binary) when is_binary(Binary) -> string(binary_to_list(Binary));
string(String) when is_list(String) -> string(String, String);
string(Other) -> {error, Other}.

string([], String) -> {ok, String};
string([H|T], String) when is_integer(H) -> string(T, String);
string(_, String) -> {error, String}.

%% @doc Parse a logging template. Tab/space are the only separators;
%% words are ASCII printable; atoms (words prefixed with `%`) accept
%% `[a-zA-Z_]`. Templates are terminated with "\n".
%%
%% @see logger_formatter:template/0
%%
%% == Examples ==
%%
%% ```
%% {ok, ["test", "\n"]} = logging_template("test").
%% {ok, [test, "\n"]} = logging_template("%test").
%% {ok, ["message:", msg, "\n"]} = logging_template("message: %msg").
%% '''
%%
-spec logging_template(String) -> Return when
    String :: binary() | list(),
    Return :: {ok, [atom()|list()]} | {error, term()}.
logging_template(List) when is_list(List) ->
    logging_template_parse(list_to_binary(List));
logging_template(Binary) when is_binary(Binary) ->
    logging_template_parse(Binary).

logging_template_parse(Binary) ->
    logging_template_tokenizer(Binary, []).

logging_template_tokenizer(<<>>, Buffer) ->
    logging_template_parser(Buffer);
logging_template_tokenizer(<<Char, Rest/binary>>, Buffer)
    when Char =:= $ ; Char =:= $\t ->
        NewBuffer = [{null, Char}|Buffer],
        logging_template_tokenizer(Rest, NewBuffer);
logging_template_tokenizer(<<$%, Rest/binary>>, Buffer) ->
    case logging_template_token_atom(Rest) of
        {ok, Atom, NewRest} ->
            NewBuffer = [{atom, Atom}|Buffer],
            logging_template_tokenizer(NewRest, NewBuffer);
        Else ->
            Else
    end;
logging_template_tokenizer(Bin, Buffer) when is_binary(Bin) ->
    case logging_template_token_word(Bin) of
        {ok, Word, NewRest} ->
            logging_template_token_word(Bin),
            NewBuffer = [{word, Word}|Buffer],
            logging_template_tokenizer(NewRest, NewBuffer);
        Else ->
            Else
    end.

logging_template_token_atom(Binary) ->
    logging_template_token_atom(Binary, <<>>).
logging_template_token_atom(<<>>, Buffer) ->
    {ok, Buffer, <<>>};
logging_template_token_atom(Rest = <<Char, _/binary>>, Buffer)
    when Char =:= $ ; Char =:= $\t ->
        {ok, Buffer, Rest};
logging_template_token_atom(<<Char, Rest/binary>>, Buffer)
    when Char >= $a, Char =< $z;
         Char >= $A, Char =< $Z;
         Char >= $0, Char =< $9;
         Char =:= $_ ->
        logging_template_token_atom(Rest, <<Buffer/binary, Char>>);
logging_template_token_atom(<<Char, _/binary>>, _Buffer) ->
    {error, {atom, Char}}.

logging_template_token_word(Binary) ->
    logging_template_token_word(Binary, <<>>).
logging_template_token_word(<<>>, Buffer) ->
    {ok, Buffer, <<>>};
logging_template_token_word(Rest= <<Char, _/binary>>, Buffer)
    when Char =:= $ ; Char =:= $\t ->
        {ok, Buffer, Rest};
logging_template_token_word(<<Char, Rest/binary>>, Buffer)
    when Char >= 21, Char =< 126 ->
        logging_template_token_word(Rest, <<Buffer/binary, Char>>);
logging_template_token_word(<<Char, _/binary>>, _Buffer) ->
    {error, {word, Char}}.

logging_template_parser(Tokens) ->
    logging_template_parser(Tokens, []).
logging_template_parser([], Buffer) ->
    {ok, Buffer ++ ["\n"]};
logging_template_parser([{null, Null}|Rest], Buffer) ->
    NewBuffer = [[Null]|Buffer],
    logging_template_parser(Rest, NewBuffer);
logging_template_parser([{atom, Atom}|Rest], Buffer) ->
    try
        Result = binary_to_existing_atom(Atom),
        NewBuffer = [Result|Buffer],
        logging_template_parser(Rest, NewBuffer)
    catch
        _:_ ->
            {error, {atom, Atom}}
    end;
logging_template_parser([{word, Word}|Rest], Buffer) ->
    NewBuffer = [binary_to_list(Word)|Buffer],
    logging_template_parser(Rest, NewBuffer).

%% @doc Mining / wallet address. Accepts either a 32-byte binary
%% verbatim or a URL-safe Base64 string that decodes to exactly 32
%% bytes (43 base64 characters). Anything else is rejected.
-spec address(Input) -> Return when
    Input :: binary() | list(),
    Return :: {ok, binary()} | {error, term()}.
address(B) when is_binary(B), byte_size(B) =:= 32 ->
    {ok, B};
address(B) when is_binary(B) ->
    try b64fast:decode(B) of
        Decoded when byte_size(Decoded) =:= 32 -> {ok, Decoded};
        _ -> {error, {invalid_address_size, B}}
    catch
        _:_ -> {error, {invalid_address_base64, B}}
    end;
address(L) when is_list(L) ->
    try
        address(list_to_binary(L))
    catch
        _:_ -> {error, {invalid_address, L}}
    end;
address(V) ->
    {error, {invalid_address, V}}.

%% Common error shape for type validators.
type_error(Name, Reason, Data) ->
    {error, #{
            status => error,
            message => #{
                type => Name,
                reason => Reason,
                data => Data
            }
         }
    }.

%%% --------------------------------------------------------------------
%%% Peer-id canonicalization
%%% --------------------------------------------------------------------

%% @doc Normalize a peer spelling to canonical form. IPv4 peers become
%% `{A, B, C, D, Port}' 5-tuples; hostnames and bracketed IPv6 stay as
%% `<<"host:port">>' binaries. Accepts strings, binaries, 4-tuples
%% (default port applied), and 5-tuples.
-spec peer_id(Input) -> Return when
    Input :: binary() | string() | tuple(),
    Return :: {ok, peer_id()} | {error, term()}.
peer_id(Input) when is_binary(Input) ->
    peer_id_binary(Input);
peer_id(Input) when is_list(Input) ->
    try
        peer_id_binary(list_to_binary(Input))
    catch
        _:_ ->
            {error, {invalid_peer, Input}}
    end;
peer_id({A, B, C, D}) when ?is_octet(A), ?is_octet(B), ?is_octet(C), ?is_octet(D) ->
    {ok, {A, B, C, D, ?DEFAULT_PORT}};
peer_id({A, B, C, D, Port}) when ?is_octet(A), ?is_octet(B), ?is_octet(C),
        ?is_octet(D), is_integer(Port), Port >= 0, Port =< 65535 ->
    {ok, {A, B, C, D, Port}};
peer_id(Input) ->
    {error, {invalid_peer, Input}}.

%% @doc Like `peers_list/1' but resolves hostnames to IPv4 peers at
%% parse time (matching the legacy CLI/JSON parsers), expanding a
%% multi-record hostname into one peer per address. Used by the roles
%% that legacy resolves eagerly; VDF roles use `peers_list/1' so their
%% hostnames survive for the runtime resolver.
-spec resolved_peers_list(Input) -> Return when
    Input :: [binary() | string() | tuple()] | binary() | tuple(),
    Return :: {ok, [peer_id()]} | {error, term()}.
resolved_peers_list(Input) ->
    do_peers_list(Input, true).

%% @doc Resolve a single peer to one canonical IPv4 peer_id, taking the
%% first address when a hostname has several. Mirrors the legacy
%% `cm_exit_peer' handling.
-spec resolved_peer_id(Input) -> Return when
    Input :: binary() | string() | tuple(),
    Return :: {ok, peer_id()} | {error, term()}.
resolved_peer_id(Value) ->
    case resolve_peer(Value) of
        {ok, [PeerID | _]} -> {ok, PeerID};
        {error, _} = Err -> Err
    end.

%% Resolve one peer entry to one-or-more canonical IPv4 peer_ids.
%% Pre-formed tuples are canonicalized via `peer_id/1'; everything else
%% goes through the resolver.
resolve_peer(Peer) when is_tuple(Peer) ->
    case peer_id(Peer) of
        {ok, PeerID} -> {ok, [PeerID]};
        {error, _} = Err -> Err
    end;
resolve_peer(Peer) ->
    case arweave_util:safe_parse_peer(Peer) of
        {ok, [_ | _] = PeerIDs} -> {ok, PeerIDs};
        _ -> {error, {invalid_peer, Peer}}
    end.

peer_id_binary(<<>>) ->
    {error, empty_peer};
peer_id_binary(<<"[", Rest/binary>>) ->
    %% Bracketed IPv6: `[::1]:1984` or `[::1]`.
    case binary:split(Rest, <<"]">>) of
        [Host, <<>>] ->
            case validate_host(Host) of
                ok ->
                    {ok, iolist_to_binary([
                        <<"[">>, Host, <<"]:">>,
                        integer_to_binary(?DEFAULT_PORT)
                    ])};
                Error ->
                    Error
            end;
        [Host, <<":", PortBin/binary>>] ->
            case {validate_host(Host), parse_port(PortBin)} of
                {ok, {ok, Port}} ->
                    {ok, iolist_to_binary([
                        <<"[">>, Host, <<"]:">>,
                        integer_to_binary(Port)
                    ])};
                {{error, R}, _} -> {error, R};
                {_, {error, R}} -> {error, R}
            end;
        _ ->
            {error, {invalid_peer, <<"[", Rest/binary>>}}
    end;
peer_id_binary(Bin) ->
    %% Split on `:`: covers IPv4 with optional port and bare
    %% hostnames. Unbracketed IPv6 is ambiguous and rejected.
    case binary:split(Bin, <<":">>, [global]) of
        [Bin] ->
            %% Bare host: append default port.
            case validate_host(Bin) of
                ok -> finalize_peer(Bin, ?DEFAULT_PORT);
                Error -> Error
            end;
        [Host, PortBin] ->
            case {validate_host(Host), parse_port(PortBin)} of
                {ok, {ok, Port}} ->
                    finalize_peer(Host, Port);
                {{error, R}, _} -> {error, R};
                {_, {error, R}} -> {error, R}
            end;
        _ ->
            {error, {ambiguous_peer, Bin}}
    end.

%% @doc IPv4 hosts return as 5-tuples; hostnames stay as
%% `<<"host:port">>' binaries so the runtime resolver
%% (`ar_peers:resolve_and_cache_peer/2') can re-resolve them after DNS
%% changes.
finalize_peer(Host, Port) ->
    case parse_ipv4_octets(Host) of
        {ok, {A, B, C, D}} ->
            {ok, {A, B, C, D, Port}};
        error ->
            {ok, <<Host/binary, ":", (integer_to_binary(Port))/binary>>}
    end.

parse_ipv4_octets(Host) when is_binary(Host) ->
    try
        Parts = binary:split(Host, <<".">>, [global]),
        case [binary_to_integer(P) || P <- Parts] of
            [A, B, C, D] when ?is_octet(A), ?is_octet(B),
                    ?is_octet(C), ?is_octet(D) ->
                {ok, {A, B, C, D}};
            _ ->
                error
        end
    catch
        _:_ -> error
    end.

validate_host(<<>>) ->
    {error, empty_host};
validate_host(Host) when is_binary(Host) ->
    case re:run(Host, <<"^[A-Za-z0-9._:-]+$">>, [{capture, none}]) of
        match -> ok;
        nomatch -> {error, {invalid_host, Host}}
    end.

parse_port(<<>>) ->
    {error, empty_port};
parse_port(PortBin) ->
    try
        Port = binary_to_integer(PortBin),
        validate_port(Port)
    catch
        _:_ -> {error, {invalid_port, PortBin}}
    end.

validate_port(Port) when Port >= 0, Port =< 65535 -> {ok, Port};
validate_port(Port) -> {error, {port_out_of_range, Port}}.
