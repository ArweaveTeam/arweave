%%% @doc Parse peer strings from the configuration into peer tuples,
%%% resolving host names through DNS.
-module(arweave_config_peer).

-export([parse_peer/1, parse_peer/2, safe_parse_peer/1, safe_parse_peer/2]).

%% @doc Parse a peer string, binary or tuple into a list of
%% {A, B, C, D, Port} peers, one for each address a host name resolves to.
parse_peer(Hostname) ->
    parse_peer(Hostname, #{}).

%% @doc Parse a peer like parse_peer/1. Opts may name a module_resolve to use
%% instead of inet for resolving host names.
parse_peer("", _Opts) ->
    throw(empty_peer_string);
parse_peer(BitStr, Opts) when is_binary(BitStr) ->
    parse_peer(binary_to_list(BitStr), Opts);
parse_peer([{A, B, C, D, P}], _Opts) ->
    [{A, B, C, D, arweave_lib_util:parse_port(P)}];
parse_peer({A, B, C, D, P}, _Opts)
        when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
    [{A, B, C, D, arweave_lib_util:parse_port(P)}];
parse_peer(Str, Opts) when is_list(Str) ->
    ResolveModule = maps:get(module_resolve, Opts, inet),
    [Addr, PortStr] = arweave_lib_util:parse_port_split(Str),
    case ResolveModule:getaddrs(Addr, inet) of
        {ok, [{A, B, C, D}]} ->
            [{A, B, C, D, arweave_lib_util:parse_port(PortStr)}];
        {ok, AddrsList} when is_list(AddrsList) ->
            [{A, B, C, D, arweave_lib_util:parse_port(PortStr)}
                || {A, B, C, D} <- AddrsList];
        {error, Reason} ->
            throw({invalid_peer_string, Str, Reason})
    end;
parse_peer({{A, B, C, D}, P}, _Opts) ->
    [{A, B, C, D, arweave_lib_util:parse_port(P)}];
parse_peer({IP, Port}, Opts) ->
    {A, B, C, D} = parse_peer(IP, Opts),
    [{A, B, C, D, arweave_lib_util:parse_port(Port)}];
parse_peer(_Peer, _) ->
    throw(invalid_peer).

%% @doc Parse a peer like parse_peer/1, returning {error, invalid} instead of
%% throwing.
safe_parse_peer(Peer) ->
    safe_parse_peer(Peer, #{}).

%% @doc Parse a peer like parse_peer/2, returning {error, invalid} instead of
%% throwing.
safe_parse_peer(Peer, Opts) ->
    try
        {ok, parse_peer(Peer, Opts)}
    catch
        _:_ -> {error, invalid}
    end.
