-module(arweave_config_peer).
-export([safe_parse_peer/1, safe_parse_peer/2]).
-include_lib("arweave_lib/include/arweave_lib_constants.hrl").


parse_peer(Hostname) ->
    parse_peer(Hostname, #{}).


parse_peer("", _Opts) ->
    throw(empty_peer_string);
parse_peer(BitStr, Opts) when is_binary(BitStr) ->
    parse_peer(binary_to_list(BitStr), Opts);
parse_peer([{A,B,C,D,P}], _Opts) ->
    [{A, B, C, D, parse_port(P)}];
parse_peer({A,B,C,D,P}, _Opts)
        when is_integer(A), is_integer(B), is_integer(C), is_integer(D) ->
    [{A, B, C, D, parse_port(P)}];
parse_peer(Str, Opts) when is_list(Str) ->
    % useful to mock the resolver, instead of using
    % inet, any other custom module can be used.
    ResolveModule = maps:get(module_resolve, Opts, inet),
    [Addr, PortStr] = parse_port_split(Str),
    case ResolveModule:getaddrs(Addr, inet) of
        {ok, [{A, B, C, D}]} ->
            [{A, B, C, D, parse_port(PortStr)}];
        {ok, AddrsList} when is_list(AddrsList) ->
            [{A, B, C, D, parse_port(PortStr)} || {A, B, C, D} <- AddrsList];
        {error, Reason} ->
            throw({invalid_peer_string, Str, Reason})
    end;
parse_peer({{A,B,C,D},P}, _Opts) ->
    [{A, B, C, D, parse_port(P)}];
parse_peer({IP, Port}, Opts) ->
    {A, B, C, D} = parse_peer(IP, Opts),
    [{A, B, C, D, parse_port(Port)}];
parse_peer(_Peer, _) ->
    throw(invalid_peer).


%% @doc Parses a port string into an integer.
parse_port(Int) when is_integer(Int) -> Int;
parse_port("") -> ?DEFAULT_HTTP_IFACE_PORT;
parse_port(PortStr) ->
    {ok, [Port], ""} = io_lib:fread("~d", PortStr),
    Port.


parse_port_split(Str) ->
    case string:tokens(Str, ":") of
    [Addr] -> [Addr, ?DEFAULT_HTTP_IFACE_PORT];
    [Addr, Port] -> [Addr, Port];
    _ -> throw({invalid_peer_string, Str})
    end.


%%--------------------------------------------------------------------
%% @doc wrapper for parse_peer/1
%% @see safe_parse_peer/2
%% @end
%%--------------------------------------------------------------------
safe_parse_peer(Peer) ->
    safe_parse_peer(Peer, #{}).


safe_parse_peer(Peer, Opts) ->
    try
        {ok, parse_peer(Peer, Opts)}
    catch
        _:_ -> {error, invalid}
    end.
