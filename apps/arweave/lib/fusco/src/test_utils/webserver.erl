%%%-----------------------------------------------------------------------------
%%% @copyright (C) 1999-2013, Erlang Solutions Ltd
%%% @author Oscar Hellström <oscar@hellstrom.st>
%%% @author Magnus Henoch <magnus@erlang-consulting.com>
%%% @author Diana Parra Corbacho <diana.corbacho@erlang-solutions.com>
%%% @doc Simple web server for testing purposes
%%% @end
%%%-----------------------------------------------------------------------------
-module(webserver).
-copyright("2013, Erlang Solutions Ltd.").

-export([start/2, start/3, stop/2, stop/3]).
-export([acceptor/3]).

start(Module, Responders) ->
    start(Module, Responders, inet).

start(Module, Responders, Family) ->
    case get_addr("localhost", Family) of
        {ok, Addr} ->
            LS = listen(Module, Addr, Family),
            Pid = spawn(?MODULE, acceptor, [Module, LS, Responders]),
            {ok, Pid, LS, port(Module, LS)};
        Error ->
            Error
    end.

stop(Listener, LS) ->
    stop(gen_tcp, Listener, LS).

stop(Module, Listener, LS) ->
    (catch exit(kill, Listener)),
    Module:close(LS).

acceptor(Module, ListenSocket, Responders) ->
    case accept(Module, ListenSocket) of
        error ->
            ok;
        Socket ->
            spawn_link(fun() -> acceptor(Module, ListenSocket, Responders) end),
            server_loop(Module, Socket, nil, [], Responders)
    end.

server_loop(Module, Socket, _, _, []) ->
    Module:close(Socket);
server_loop(Module, Socket, Request, Headers, [H | T] = Responders) ->
    receive
        stop ->
            Module:close(Socket)
    after 0 ->
        case Module:recv(Socket, 0, 500) of
            {ok, {http_request, _, _, _} = NewRequest} ->
                server_loop(Module, Socket, NewRequest, Headers, Responders);
            {ok, {http_header, _, Field, _, Value}} when is_atom(Field) ->
                NewHeaders = [{atom_to_list(Field), Value} | Headers],
                server_loop(Module, Socket, Request, NewHeaders, Responders);
            {ok, {http_header, _, Field, _, Value}} when is_list(Field) ->
                NewHeaders = [{Field, Value} | Headers],
                server_loop(Module, Socket, Request, NewHeaders, Responders);
            {ok, http_eoh} ->
                RequestBody =
                    case proplists:get_value("Content-Length", Headers) of
                        undefined ->
                            <<>>;
                        "0" ->
                            <<>>;
                        SLength ->
                            Length = list_to_integer(SLength),
                            _ = setopts(Module, Socket, [{packet, raw}]),
                            {ok, Body} = Module:recv(Socket, Length),
                            _ = setopts(Module, Socket, [{packet, http}]),
                            Body
                    end,
                H(Module, Socket, Request, Headers, RequestBody),
                case proplists:get_value("Connection", Headers) of
                    "close" ->
                        Module:close(Socket);
                    _ ->
                        server_loop(Module, Socket, none, [], T)
                end;
            {error, timeout} ->
                server_loop(Module, Socket, Request, Headers, Responders);
            {error, closed} ->
                Module:close(Socket)
        end
    end.

listen(ssl, Addr, Family) ->
    KeyFile = code:where_is_file("key.pem"),
    CertFile = code:where_is_file("crt.pem"),
    Opts = [
        Family,
        {packet, http},
        binary,
        {active, false},
        {ip, Addr},
        {verify,0},
        {keyfile, KeyFile},
        {certfile, CertFile}
    ],
    {ok, LS} = ssl:listen(0, Opts),
    LS;
listen(Module, Addr, Family) ->
    {ok, LS} = Module:listen(0, [
            Family,
            {packet, http},
            binary,
            {active, false},
            {ip, Addr}
        ]),
    LS.

get_addr(Host, Family) ->
    case inet:getaddr(Host, Family) of
        {ok, Addr} ->
            {ok, Addr};
        _ ->
            {error, family_not_supported}
    end.

accept(ssl, ListenSocket) ->
    case ssl:transport_accept(ListenSocket, 1000000) of
        {ok, Socket} ->
            ok = ssl:ssl_accept(Socket),
            Socket;
        {error, _} ->
            error
    end;
accept(Module, ListenSocket) ->
    case Module:accept(ListenSocket, 100000) of
        {ok, Socket} ->
            Socket;
        {error, _} ->
            error
    end.

setopts(ssl, Socket, Options) ->
    ssl:setopts(Socket, Options);
setopts(_, Socket, Options) ->
    inet:setopts(Socket, Options).

port(ssl, Socket) ->
    {ok, {_, Port}} = ssl:sockname(Socket),
    Port;
port(_, Socket) ->
    {ok, Port} = inet:port(Socket),
    Port.
