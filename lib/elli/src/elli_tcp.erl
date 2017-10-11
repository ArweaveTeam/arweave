%% @doc: Wrapper for plain and SSL sockets. Based on
%% mochiweb_socket.erl

-module(elli_tcp).
-export([listen/3, accept/3, recv/3, send/2, close/1, setopts/2, sendfile/5, peername/1]).

-export_type([socket/0]).

-type socket() :: {plain, inet:socket()} | {ssl, ssl:sslsocket()}.

listen(plain, Port, Opts) ->
    case gen_tcp:listen(Port, Opts) of
        {ok, Socket} ->
            {ok, {plain, Socket}};
        {error, Reason} ->
            {error, Reason}
    end;

listen(ssl, Port, Opts) ->
    case ssl:listen(Port, Opts) of
        {ok, Socket} ->
            {ok, {ssl, Socket}};
        {error, Reason} ->
            {error, Reason}
    end.


accept({plain, Socket}, Server, Timeout) ->
    case gen_tcp:accept(Socket, Timeout) of
        {ok, S} ->
            gen_server:cast(Server, accepted),
            {ok, {plain, S}};
        {error, Reason} ->
            {error, Reason}
    end;
accept({ssl, Socket}, Server, Timeout) ->
    case ssl:transport_accept(Socket, Timeout) of
        {ok, S} ->
            gen_server:cast(Server, accepted),
            case ssl:ssl_accept(S, Timeout) of
                ok ->
                    {ok, {ssl, S}};
                {error, closed} ->
                    {error, econnaborted};
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


recv({plain, Socket}, Size, Timeout) ->
    gen_tcp:recv(Socket, Size, Timeout);
recv({ssl, Socket}, Size, Timeout) ->
    ssl:recv(Socket, Size, Timeout).

send({plain, Socket}, Data) ->
    gen_tcp:send(Socket, Data);
send({ssl, Socket}, Data) ->
    ssl:send(Socket, Data).

close({plain, Socket}) ->
    gen_tcp:close(Socket);
close({ssl, Socket}) ->
    ssl:close(Socket).

setopts({plain, Socket}, Opts) ->
    inet:setopts(Socket, Opts);
setopts({ssl, Socket}, Opts) ->
    ssl:setopts(Socket, Opts).

sendfile(Fd, {plain, Socket}, Offset, Length, Opts) ->
    file:sendfile(Fd, Socket, Offset, Length, Opts);
sendfile(_Fd, {ssl, _}, _Offset, _Length, _Opts) ->
    throw(ssl_sendfile_not_supported).

peername({plain, Socket}) ->
    inet:peername(Socket);
peername({ssl, Socket}) ->
    ssl:peername(Socket).
