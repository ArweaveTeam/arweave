%%% ----------------------------------------------------------------------------
%%% @copyright (C) 1999-2013, Erlang Solutions Ltd
%%% @author Oscar Hellström <oscar@hellstrom.st>
%%% @author Diana Parra Corbacho <diana.corbacho@erlang-solutions.com>
%%% @author Ramon Lastres Guerrero <ramon.lastres@erlang-solutions.com>
%%% @doc Fast and Ultra Slim Connection Oriented HTTP Client
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(fusco).
-copyright("2013, Erlang Solutions Ltd.").

%exported functions
-export([start/2,
         start_link/2,
         connect/1,
         request/6,
         request/7,
         disconnect/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("fusco.hrl").
-include("fusco_types.hrl").

-export_type([header/0,
              headers/0,
              method/0,
              pos_timeout/0,
              socket/0,
              port_num/0,
              invalid_option/0,
              destination/0,
              option/0,
              options/0,
              host/0,
              socket_options/0,
              body/0,
              result/0]).

-define(HTTP_LINE_END, "\r\n").

-record(client_state, {
          host :: string(),
          port = 80 :: port_num(),
          ssl = false :: boolean(),
          socket,
          connect_timeout = 'infinity' :: timeout(),
          connect_options = [] :: [any()],
          %% next fields are specific to particular requests
          request :: iolist() | undefined,
          connection_header,
          requester,
          cookies = [] :: [#fusco_cookie{}],
          use_cookies = false :: boolean(),
          %% in case of infinity we read whatever data we can get from
          %% the wire at that point
          attempts = 0 :: integer(),
          proxy :: undefined | #fusco_url{},
          proxy_ssl_options = [] :: [any()],
          host_header,
          out_timestamp,
          in_timestamp,
          on_connect,
          recv_timeout = 'infinity' :: timeout()
         }).

-type client_state() :: #client_state{}.

%%==============================================================================
%% Exported functions
%%==============================================================================
start(Destination, Options) ->
    verify_options(Options),
    gen_server:start(?MODULE, {Destination, Options}, []).

start_link(Destination, Options) ->
    verify_options(Options),
    gen_server:start_link(?MODULE, {Destination, Options}, []).

%%------------------------------------------------------------------------------
%% @doc Starts a Client.
%% @end
%%------------------------------------------------------------------------------
connect(Client) ->
    gen_server:call(Client, connect).

%%------------------------------------------------------------------------------
%% @doc Stops a Client.
%% @end
%%------------------------------------------------------------------------------
-spec disconnect(pid()) -> ok.
disconnect(Client) ->
    gen_server:cast(Client, stop).

%%------------------------------------------------------------------------------
%% @doc Makes a request using a client already connected.
%% @end
%%------------------------------------------------------------------------------
-spec request(pid(), path(), method(), headers(), iodata(), pos_timeout()) ->
    result().
request(Client, Path, Method, Hdrs, Body, Timeout) ->
    request(Client, Path, Method, Hdrs, Body, 1, Timeout).


%%------------------------------------------------------------------------------
%% @spec (Client, Host, Method, Hdrs, RequestBody, RetryCount, Timeout) ->
%%     Result
%%   Host = string()
%%   Method = string() | atom()
%%   Hdrs = [{Header, Value}]
%%   Header = string() | binary() | atom()
%%   Value = string() | binary()
%%   RequestBody = iodata()
%%   RetryCount = integer()
%%   Timeout = integer() | infinity
%%   Result = {ok, {{StatusCode, ReasonPhrase}, Hdrs, ResponseBody}}
%%          | {error, Reason}
%%   StatusCode = integer()
%%   ReasonPhrase = string()
%%   ResponseBody = binary() | pid() | undefined
%%   Reason = connection_closed | connect_timeout | timeout
%% @doc Sends a request with a body.
%%
%% Instead of building and parsing URLs the target server is specified with
%% a host, port, weither SSL should be used or not and a path on the server.
%% For instance, if you want to request http://example.com/foobar you would
%% use the following:<br/>
%% `Host' = `"example.com"'<br/>
%% `Port' = `80'<br/>
%% `Ssl' = `false'<br/>
%% `Path' = `<<"/foobar">>'<br/>
%% `Path' must begin with a forward slash `/'.
%%
%% `Method' is either a string, stating the HTTP method exactly as in the
%% protocol, i.e: `"POST"' or `"GET"'. It could also be an atom, which is
%% then coverted to an uppercase (if it isn't already) string.
%%
%% `Hdrs' is a list of headers to send. Mandatory headers such as
%% `Host', `Content-Length' or `Transfer-Encoding' (for some requests)
%% are added automatically.
%%
%% `Body' is the entity to send in the request. Please don't include entity
%% bodies where there shouldn't be any (such as for `GET').
%%
%% `Timeout' is the timeout for the request in milliseconds.
%%
%% `Options' is a list of options.
%%
%% Options:
%%
%% `{connect_timeout, Milliseconds}' specifies how many milliseconds the
%% client can spend trying to establish a connection to the server. This
%% doesn't affect the overall request timeout. However, if it's longer than
%% the overall timeout it will be ignored. Also note that the TCP layer my
%% choose to give up earlier than the connect timeout, in which case the
%% client will also give up. The default value is infinity, which means that
%% it will either give up when the TCP stack gives up, or when the overall
%% request timeout is reached.
%%
%% `{connect_options, Options}' specifies options to pass to the socket at
%% connect time. This makes it possible to specify both SSL options and
%% regular socket options, such as which IP/Port to connect from etc.
%% Some options must not be included here, namely the mode, `binary'
%% or `list', `{active, boolean()}', `{active, once}' or `{packet, Packet}'.
%% These options would confuse the client if they are included.
%% Please note that these options will only have an effect on *new*
%% connections, and it isn't possible for different requests
%% to the same host uses different options unless the connection is closed
%% between the requests. Using HTTP/1.0 or including the "Connection: close"
%% header would make the client close the connection after the first
%% response is received.
%%
%% `{send_retry, N}' specifies how many times the client should retry
%% sending a request if the connection is closed after the data has been
%% sent. The default value is `1'.
%%
%% `{proxy, ProxyUrl}' if this option is specified, a proxy server is used as
%% an intermediary for all communication with the destination server. The link
%% to the proxy server is established with the HTTP CONNECT method (RFC2817).
%% Example value: {proxy, "http://john:doe@myproxy.com:3128"}
%%
%% `{proxy_ssl_options, SslOptions}' this is a list of SSL options to use for
%% the SSL session created after the proxy connection is established. For a
%% list of all available options, please check OTP's ssl module manpage.
%% @end
%%------------------------------------------------------------------------------
-spec request(pid(), path(), method(), headers(), iodata(), integer(),
              pos_timeout()) -> result().
request(Client, Path, Method, Hdrs, Body, SendRetry,
        Timeout) when is_binary(Path) ->
    gen_server:call(Client,
                    {request, Path, Method, Hdrs, Body, SendRetry, Timeout},
                    infinity);
request(_, _, _, _, _, _, _) ->
    {error, badarg}.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================
init({Destination, Options}) ->
    ConnectTimeout = fusco_lib:get_value(connect_timeout, Options, infinity),
    ConnectOptions = fusco_lib:get_value(connect_options, Options, []),
    UseCookies = fusco_lib:get_value(use_cookies, Options, false),
    ProxyInfo = fusco_lib:get_value(proxy, Options, false),
    ProxySsl = fusco_lib:get_value(proxy_ssl_options, Options, []),
    OnConnectFun = fusco_lib:get_value(on_connect, Options, fun(_) -> ok end),
    {Host, Port, Ssl} = case Destination of
        {H, P, S} ->
            {H, P, S};
        URL ->
            #fusco_url{host = H, port = P,
                        is_ssl = S} = fusco_lib:parse_url(URL),
            {H, P, S}
    end,
    Proxy = case ProxyInfo of
        false ->
            undefined;
        ProxyUrl when is_list(ProxyUrl), not Ssl ->
            %% The point of HTTP CONNECT proxying is to use TLS tunneled in
            %% a plain HTTP/1.1 connection to the proxy (RFC2817).
            throw(origin_server_not_https);
        ProxyUrl when is_list(ProxyUrl) ->
            fusco_lib:parse_url(ProxyUrl)
    end,
    State = #client_state{host = Host, port = Port, ssl = Ssl,
                          connect_timeout = ConnectTimeout,
                          connect_options = ConnectOptions,
                          use_cookies = UseCookies,
                          host_header = fusco_lib:host_header(Host, Port),
                          proxy = Proxy,
                          proxy_ssl_options = ProxySsl,
                          on_connect = OnConnectFun},
    {ok, State}.

%%------------------------------------------------------------------------------
%% @doc This function fills in the Client record used in the requests and
%% obtains the socket.
%% @end
%%------------------------------------------------------------------------------
handle_call(connect, _From, #client_state{socket = undefined} = State) ->
    % if we dont get a keep alive from the previous request, the socket is
    % undefined.
    case connect_socket(State) of
        {ok, NewState} ->
            {reply, ok, NewState};
        {Error, NewState} ->
            {reply, Error, NewState}
    end;
handle_call(connect, _From, State) ->
    {reply, ok, State};
handle_call({request, Path, Method, Hdrs, Body, SendRetry, Timeout}, From,
            State = #client_state{host_header = Host,
                                  use_cookies = UseCookies}) ->
    Cookies = delete_expired_cookies(State),
    {Request, ConHeader} =
    fusco_lib:format_request(Path, Method, Hdrs, Host, Body,
                             {UseCookies, Cookies}),
    send_request(State#client_state{ request = Request,
                                     requester = From,
                                     connection_header = ConHeader,
                                     attempts = SendRetry + 1,
                                     recv_timeout = Timeout}).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, #client_state{socket = Socket, ssl = Ssl}) ->
    case Socket of
        undefined ->
            ok;
        _ ->
            _ = fusco_sock:close(Socket, Ssl),
            ok
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%==============================================================================
%% Internal functions
%%==============================================================================
%%------------------------------------------------------------------------------
%% @private
%% @doc This function creates a new socket connection if needed, and it also
%% handles the proxy connection.
%% @end
%%------------------------------------------------------------------------------
send_request(#client_state{attempts = 0} = State) ->
    {reply, {error, connection_closed}, State};
send_request(#client_state{socket = undefined} = State) ->
    % if we dont get a keep alive from the previous request, the socket is
    % undefined.
    case connect_socket(State) of
        {ok, NewState} ->
            send_request(NewState);
        {Error, NewState} ->
            {reply, Error, NewState}
    end;
send_request(#client_state{socket = Socket, ssl = Ssl, request = Request,
                           attempts = Attempts, recv_timeout = RecvTimeout
                          } = State) ->
    Out = os:timestamp(),
    %If we have a timeout set then we need to ensure a timeout on sending too
    _ = fusco_sock:setopts(Socket, [{send_timeout, RecvTimeout},
                                    {send_timeout_close, true}], Ssl),
    case fusco_sock:send(Socket, Request, Ssl) of
        ok ->
            read_response(State#client_state{out_timestamp = Out});
        {error, closed} ->
            _ = fusco_sock:close(Socket, Ssl),
            send_request(State#client_state{socket = undefined,
                                            attempts = Attempts - 1});
        {error, _Reason} ->
            _ = fusco_sock:close(Socket, Ssl),
            { reply,
              {error, connection_closed},
              State#client_state{socket = undefined}
            }
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
request_first_destination(#client_state{proxy = #fusco_url{} = Proxy}) ->
    {Proxy#fusco_url.host, Proxy#fusco_url.port, Proxy#fusco_url.is_ssl};
request_first_destination(#client_state{host = Host, port = Port, ssl = Ssl}) ->
    {Host, Port, Ssl}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
read_proxy_connect_response(#client_state{recv_timeout = RecvTimeout
                                         } = State) ->
    Socket = State#client_state.socket,
    ProxyIsSsl = (State#client_state.proxy)#fusco_url.is_ssl,
    case fusco_protocol:recv(Socket, ProxyIsSsl, RecvTimeout) of
        #response{status_code = <<$1,_,_>>} ->
            %% RFC 2616, section 10.1:
            %% A client MUST be prepared to accept one or more
            %% 1xx status responses prior to a regular
            %% response, even if the client does not expect a
            %% 100 (Continue) status message. Unexpected 1xx
            %% status responses MAY be ignored by a user agent.
            read_proxy_connect_response(State);
        #response{status_code = <<$2,_,_>>} ->
            %% RFC2817, any 2xx code means success.
            ConnectOptions = State#client_state.connect_options,
            SslOptions = State#client_state.proxy_ssl_options,
            Timeout = State#client_state.connect_timeout,
            case ssl:connect(Socket, SslOptions ++ ConnectOptions, Timeout) of
                {ok, SslSocket} ->
                    {ok, SslSocket};
                {error, Reason} ->
                    _ = fusco_sock:close(State#client_state.socket,
                                         State#client_state.ssl),
                    {error, {proxy_connection_failed, Reason}}
            end;
        #response{status_code = StatusCode, reason = Reason} ->
            {error, {proxy_connection_refused, StatusCode, Reason}};
        {error, closed} ->
            _ = fusco_sock:close(Socket, ProxyIsSsl),
            {error, proxy_connection_closed};
        {error, Reason} ->
            {error, {proxy_connection_failed, Reason}}
    end.

%%------------------------------------------------------------------------------
%% @private
%% @doc @TODO This does not handle redirects at the moment.
%% @end
%%------------------------------------------------------------------------------
-spec read_response(client_state()) -> {any(), socket()} | no_return().
read_response(#client_state{socket = Socket, ssl = Ssl,
                            use_cookies = UseCookies,
                            connection_header = ConHdr, cookies = Cookies,
                            requester = From, out_timestamp = Out,
                            attempts = Attempts,
                            recv_timeout = RecvTimeout} = State) ->
    case fusco_protocol:recv(Socket, Ssl, RecvTimeout) of
        #response{status_code = <<$1,_,_>>} ->
            %% RFC 2616, section 10.1:
            %% A client MUST be prepared to accept one or more
            %% 1xx status responses prior to a regular
            %% response, even if the client does not expect a
            %% 100 (Continue) status message. Unexpected 1xx
            %% status responses MAY be ignored by a user agent.
            read_response(State);
        #response{version = Vsn, cookies = NewCookies, connection = Connection,
                  status_code = Status, reason = Reason, headers = Headers,
                  body = Body, size = Size, in_timestamp = In}->
            gen_server:reply(From, {ok, {{Status, Reason}, Headers, Body, Size,
                timer:now_diff(In, Out)}}),
            case maybe_close_socket(Connection, State, Vsn, ConHdr) of
                undefined ->
                    maybe_use_cookie_with_conn(UseCookies, State, NewCookies,
                                               Cookies, In);
                _ ->
                    maybe_use_cookie_without_conn(UseCookies, State, NewCookies,
                                                  Cookies, In)
            end;
        {error, closed} ->
            % Either we only noticed that the socket was closed after we
            % sent the request, the server closed it just after we put
            % the request on the wire or the server has some isses and is
            % closing connections without sending responses.
            % If this the first attempt to send the request, we will try again.
            _ = fusco_sock:close(Socket, Ssl),
            send_request(State#client_state{socket = undefined,
                                            attempts = Attempts - 1});
        {error, Reason} ->
            _ = fusco_sock:close(Socket, Ssl),
            {reply, {error, Reason}, State#client_state{socket = undefined}}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
maybe_use_cookie_with_conn(true, State, NewCookies, Cookies, In) ->
    { noreply,
      State#client_state{
         socket = undefined,
         cookies = fusco_lib:update_cookies(NewCookies, Cookies),
         in_timestamp = In
      }
    };
maybe_use_cookie_with_conn(false, State, _, _, _) ->
    {noreply, State#client_state{socket = undefined}}.

maybe_use_cookie_without_conn(true, State, NewCookies, Cookies, In) ->
    { noreply,
      State#client_state{
          cookies = fusco_lib:update_cookies(NewCookies, Cookies),
          in_timestamp = In
    }};
maybe_use_cookie_without_conn(_, State, _, _, _) ->
    {noreply, State}.

maybe_close_socket(<<"close">>, #client_state{socket = Socket} = State,
                   {1, 1}, _) ->
    _ = fusco_sock:close(Socket, State#client_state.ssl),
    undefined;
maybe_close_socket(_, #client_state{socket = Socket}, {1, 1}, undefined) ->
    Socket;
maybe_close_socket(_, #client_state{socket = Socket} = State, {1, 1}, ConHdr) ->
    case fusco_lib:is_close(ConHdr) of
        true ->
            _ = fusco_sock:close(Socket, State#client_state.ssl),
            undefined;
        false ->
            Socket
    end;
maybe_close_socket(<<"keep-alive">>, #client_state{socket = Socket}, _,
                   undefined) -> Socket;
maybe_close_socket(C, #client_state{socket = Socket} = State, _, _)
  when C =/= <<"keep-alive">> ->
    _ = fusco_sock:close(Socket, State#client_state.ssl),
    undefined;
maybe_close_socket(_, #client_state{socket = Socket} = State, _, ConHdr) ->
    case fusco_lib:is_close(ConHdr) of
        true ->
            _ = fusco_sock:close(Socket, State#client_state.ssl),
            undefined;
        false ->
            Socket
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec is_ipv6_host(host()) -> boolean().
is_ipv6_host(Host) ->
    case inet_parse:address(Host) of
        {ok, {_, _, _, _, _, _, _, _}} ->
            true;
        {ok, {_, _, _, _}} ->
            false;
        _ ->
            % Prefer IPv4 over IPv6.
            case inet:getaddr(Host, inet) of
                {ok, _} ->
                    false;
                _ ->
                    is_ok_response(inet:getaddr(Host, inet6))
            end
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
is_ok_response({ok, _}) -> true;
is_ok_response(_) -> false.

% What about the timeout?
%%------------------------------------------------------------------------------
%% @private
%% Creates a new socket.
%% @end
%%------------------------------------------------------------------------------
connect_socket(State) ->
    case ensure_proxy_tunnel(new_socket(State), State) of
        {ok, Socket} ->
            {ok, State#client_state{socket = Socket}};
        Error ->
            {Error, State}
    end.

%%------------------------------------------------------------------------------
%% @private
%% @doc Creates a new socket using the options included in the client state.
%% end
%%------------------------------------------------------------------------------
new_socket(#client_state{connect_timeout = Timeout,
                         connect_options = ConnectOptions,
                         on_connect = OnConnectFun} = State) ->
    {Host, Port, Ssl} = request_first_destination(State),
    ConnectOptions2 = case (not lists:member(inet, ConnectOptions)) andalso
        (not lists:member(inet6, ConnectOptions)) andalso
        is_ipv6_host(Host) of
        true ->
            [inet6 | ConnectOptions];
        false ->
            ConnectOptions
    end,
    SocketOptions = [binary, {packet, raw}, {nodelay, true}, {reuseaddr, true},
                     {active, false} | ConnectOptions2],
    Reply = connect(Host, Port, SocketOptions, Timeout, Ssl),
    OnConnectFun(Reply),
    case Reply of
        {ok, Socket, _ConnTime} ->
            {ok, Socket};
        _ ->
            Reply
    end.

connect(Host, Port, SocketOptions, Timeout, Ssl) ->
    TimeB = os:timestamp(),
    try fusco_sock:connect(Host, Port, SocketOptions, Timeout, Ssl) of
        {ok, Socket} ->
            TimeA = os:timestamp(),
            ConnectionTime = timer:now_diff(TimeA, TimeB),
            {ok, Socket, ConnectionTime};
        {error, etimedout} ->
            %% TCP stack decided to give up
            {error, connect_timeout};
        {error, timeout} ->
            {error, connect_timeout};
        {error, 'record overflow'} ->
            {error, ssl_error};
        {error, _} = Error ->
            Error
    catch
        exit:{{{badmatch, {error, {asn1, _}}}, _}, _} ->
            {error, ssl_decode_error};
        Type:Error ->
            error_logger:error_msg("Socket connection error: ~p ~p, ~p",
                                   [Type, Error, erlang:get_stacktrace()]),
            {error, connection_error}
    end.

ensure_proxy_tunnel({error, _} = Error, _State) ->
    Error;
ensure_proxy_tunnel({ok, Socket},
                    #client_state{proxy = #fusco_url{user = User,
                                                     password = Passwd,
                                                     is_ssl = Ssl},
                                  host = DestHost, port = Port} = State) ->
    %% Proxy tunnel connection http://tools.ietf.org/html/rfc2817#section-5.2
    %% Draft http://www.web-cache.com/Writings/Internet-Drafts/
    %%       draft-luotonen-web-proxy-tunneling-01.txt
    %% IPv6 address literals are enclosed by square brackets (RFC2732)
    Host = [fusco_lib:maybe_ipv6_enclose(DestHost), $:, integer_to_list(Port)],
    ConnectRequest = [
        <<"CONNECT ">>, Host, <<" HTTP/1.1">>, ?HTTP_LINE_END, <<"Host: ">>,
        Host, ?HTTP_LINE_END,
        case User of
            [] ->
                [];
            _ ->
                [<<"Proxy-Authorization: Basic ">>,
                 base64:encode(User ++ ":" ++ Passwd), ?HTTP_LINE_END]
        end, ?HTTP_LINE_END],
    case fusco_sock:send(Socket, ConnectRequest, Ssl) of
        ok ->
            read_proxy_connect_response(State#client_state{socket = Socket});
        {error, closed} ->
            _ = fusco_sock:close(Socket, Ssl),
            {error, proxy_connection_closed};
        {error, _Reason} ->
            _ = fusco_sock:close(Socket, Ssl),
            {error, proxy_connection_closed}
    end;
ensure_proxy_tunnel(Socket, _State) ->
    Socket.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec verify_options(options()) -> ok | any().
verify_options([{connect_timeout, infinity} | Options]) ->
    verify_options(Options);
verify_options([{connect_timeout, MS} | Options])
        when is_integer(MS), MS >= 0 ->
    verify_options(Options);
verify_options([{connect_options, List} | Options]) when is_list(List) ->
    verify_options(Options);
verify_options([{proxy, List} | Options]) when is_list(List) ->
    verify_options(Options);
verify_options([{proxy_ssl_options, List} | Options]) when is_list(List) ->
    verify_options(Options);
verify_options([{use_cookies, B} | Options]) when is_boolean(B) ->
    verify_options(Options);
verify_options([{on_connect, F} | Options]) when is_function(F) ->
    verify_options(Options);
verify_options([Option | _Rest]) ->
    erlang:error({bad_option, Option});
verify_options([]) ->
    ok.

delete_expired_cookies(#client_state{use_cookies = false}) ->
    [];
delete_expired_cookies(#client_state{in_timestamp = undefined,
                                     cookies = Cookies}) ->
    Cookies;
delete_expired_cookies(#client_state{in_timestamp = In,
                                     cookies = Cookies}) ->
    fusco_lib:delete_expired_cookies(Cookies, In).
