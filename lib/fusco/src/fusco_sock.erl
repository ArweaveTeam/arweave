%%%-----------------------------------------------------------------------------
%%% @copyright (C) 1999-2013, Erlang Solutions Ltd
%%% @author Oscar Hellström <oscar@hellstrom.st>
%%% @author Diana Parra Corbacho <diana.corbacho@erlang-solutions.com>
%%% @doc This module implements wrappers for socket operations.
%%%
%%% Makes it possible to have the same interface to ssl and tcp sockets.
%%% @end
%%%-----------------------------------------------------------------------------
-module(fusco_sock).

-export([connect/5,
         recv/2,
         recv/3,
         send/3,
         close/2,
         setopts/3]).

-include("fusco_types.hrl").

%%==============================================================================
%% Exported functions
%%==============================================================================

%%------------------------------------------------------------------------------
%% @spec (Host, Port, Options, Timeout, SslFlag) -> {ok, Socket} |
%%                                                  {error, Reason}
%%   Host = string() | ip_address()
%%   Port = integer()
%%   Options = [{atom(), term()} | atom()]
%%   Timeout = infinity | integer()
%%   SslFlag = boolean()
%%   Socket = socket()
%%   Reason = atom()
%% @doc
%% Connects to `Host' and `Port'.
%% Will use the `ssl' module if `SslFlag' is `true' and gen_tcp otherwise.
%% `Options' are the normal `gen_tcp' or `ssl' Options.
%% @end
%%------------------------------------------------------------------------------
-spec connect(host(), integer(), socket_options(), timeout(), boolean()) ->
    {ok, socket()} | {error, atom()}.
connect(Host, Port, Options, Timeout, true) ->
    ssl:connect(Host, Port, Options, Timeout);
connect(Host, Port, Options, Timeout, false) ->
    gen_tcp:connect(Host, Port, Options, Timeout).

%%------------------------------------------------------------------------------
%% @spec (Socket, SslFlag) -> {ok, Data} | {error, Reason}
%%   Socket = socket()
%%   Length = integer()
%%   SslFlag = boolean()
%%   Data = term()
%%   Reason = atom()
%% @doc
%% Reads available bytes from `Socket'.
%% Will block untill data is available on the socket and return the first
%% packet.
%% @end
%%------------------------------------------------------------------------------
-spec recv(socket(), boolean()) ->
    {ok, any()} | {error, atom()} | {error, {http_error, string()}}.
recv(Socket, true) ->
    ssl:recv(Socket, 0);
recv(Socket, false) ->
    prim_inet:recv(Socket, 0).

-spec recv(socket(), boolean(), timeout()) ->
    {ok, any()} | {error, atom()} | {error, {http_error, string()}}.
recv(Socket, true, Timeout) ->
    ssl:recv(Socket, 0, Timeout);
recv(Socket, false, Timeout) ->
    prim_inet:recv(Socket, 0, Timeout).

%%------------------------------------------------------------------------------
%% @spec (Socket, Data, SslFlag) -> ok | {error, Reason}
%%   Socket = socket()
%%   Data = iolist()
%%   SslFlag = boolean()
%%   Reason = atom()
%% @doc
%% Sends data on a socket.
%% Will use the `ssl' module if `SslFlag' is set to `true', otherwise the
%% gen_tcp module.
%% @end
%%------------------------------------------------------------------------------
-spec send(socket(), iolist() | binary(), boolean()) -> ok | {error, atom()}.
send(Socket, Request, true) ->
    ssl:send(Socket, Request);
send(Socket, Request, false) ->
    prim_inet:send(Socket, Request, []).

%%------------------------------------------------------------------------------
%% @spec (Socket, SslFlag) -> ok | {error, Reason}
%%   Socket = socket()
%%   SslFlag = boolean()
%%   Reason = atom()
%% @doc
%% Closes a socket.
%% @end
%%------------------------------------------------------------------------------
-spec close(socket(), boolean()) -> ok | {error, atom()}.
close(Socket, true) ->
    ssl:close(Socket);
close(Socket, false) ->
    gen_tcp:close(Socket).

%%------------------------------------------------------------------------------
%% @spec (Socket, Opts, SslFlag) -> ok | {error, Reason}
%%   Socket = socket()
%%   Opts = list()
%%   SslFlag = boolean()
%%   Reason = atom()
%% @doc
%% Sets options for a socket.
%% Will use the `ssl' module if `SslFlag' is set to `true', otherwise the
%% inets module.
%% @end
%%------------------------------------------------------------------------------
-spec setopts(socket(), list(), boolean()) -> ok | {error, atom()}.
setopts(Socket, Opts, true) ->
    ssl:setopts(Socket, Opts);
setopts(Socket, Opts, false) ->
    inet:setopts(Socket, Opts).
