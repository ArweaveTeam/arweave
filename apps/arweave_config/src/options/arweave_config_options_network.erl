%%% @doc Specs for the `network` option group. Options for
%%% shaping HTTP transport behavior at the node boundary.
-module(arweave_config_options_network).
-behaviour(arweave_config_options).
-export([specs/0, group_description/0, validate/0]).
-export([handle_set_protocol_opt/4, handle_set_max_connections/4]).
-include("arweave_config.hrl").

specs() ->
    [
        %-----------------------------------------------------
        % HTTP server (Cowboy) knobs
        %-----------------------------------------------------
        #{
            enabled => true,
            option_key => [network, server, http, active_n],
            runtime => true,
            default => ?DEFAULT_COWBOY_HTTP_ACTIVE_N,
            type => pos_integer,
            legacy => 'http_api.http.active_n',
            short_description =>
                <<"HTTP server: number of packets requested per "
                  "socket before flow control kicks in.">>,
            handle_set => fun ?MODULE:handle_set_protocol_opt/4
        },
        #{
            enabled => true,
            option_key => [network, server, http, inactivity_timeout],
            runtime => true,
            default => ?DEFAULT_COWBOY_HTTP_INACTIVITY_TIMEOUT,
            type => pos_integer,
            legacy => 'http_api.http.inactivity_timeout',
            short_description =>
                <<"HTTP server inactivity timeout in milliseconds.">>,
            handle_set => fun ?MODULE:handle_set_protocol_opt/4
        },
        #{
            enabled => true,
            option_key => [network, server, http, linger_timeout],
            runtime => true,
            default => ?DEFAULT_COWBOY_HTTP_LINGER_TIMEOUT,
            type => pos_integer,
            legacy => 'http_api.http.linger_timeout',
            short_description =>
                <<"HTTP server linger timeout in milliseconds.">>,
            handle_set => fun ?MODULE:handle_set_protocol_opt/4
        },
        #{
            enabled => true,
            option_key => [network, server, http, request_timeout],
            runtime => true,
            default => ?DEFAULT_COWBOY_HTTP_REQUEST_TIMEOUT,
            type => pos_integer,
            legacy => 'http_api.http.request_timeout',
            short_description =>
                <<"HTTP server request timeout in milliseconds.">>,
            handle_set => fun ?MODULE:handle_set_protocol_opt/4
        },
        #{
            enabled => true,
            option_key => [network, server, socket, backlog],
            default => ?DEFAULT_COWBOY_TCP_BACKLOG,
            type => pos_integer,
            legacy => 'http_api.tcp.backlog',
            short_description =>
                <<"HTTP server TCP backlog (queued unaccepted "
                  "connections).">>
        },
        #{
            enabled => true,
            option_key => [network, server, socket, delay_send],
            default => ?DEFAULT_COWBOY_TCP_DELAY_SEND,
            type => boolean,
            legacy => 'http_api.tcp.delay_send',
            short_description =>
                <<"HTTP server TCP delay_send socket option.">>
        },
        #{
            enabled => true,
            option_key => [network, server, socket, keepalive],
            default => ?DEFAULT_COWBOY_TCP_KEEPALIVE,
            type => boolean,
            legacy => 'http_api.tcp.keepalive',
            short_description =>
                <<"HTTP server TCP keepalive socket option.">>
        },
        #{
            enabled => true,
            option_key => [network, server, socket, linger],
            default => ?DEFAULT_COWBOY_TCP_LINGER,
            type => boolean,
            legacy => 'http_api.tcp.linger',
            short_description =>
                <<"HTTP server TCP linger socket option.">>
        },
        #{
            enabled => true,
            option_key => [network, server, socket, linger_timeout],
            default => ?DEFAULT_COWBOY_TCP_LINGER_TIMEOUT,
            type => pos_integer,
            legacy => 'http_api.tcp.linger_timeout',
            short_description =>
                <<"HTTP server TCP linger timeout in seconds.">>
        },
        #{
            enabled => true,
            option_key => [network, server, http, listener_shutdown],
            default => ?DEFAULT_COWBOY_TCP_LISTENER_SHUTDOWN,
            type => pos_integer,
            legacy => 'http_api.tcp.listener_shutdown',
            short_description =>
                <<"HTTP server listener shutdown timeout in "
                  "seconds.">>
        },
        #{
            enabled => true,
            option_key => [network, server, http, max_connections],
            runtime => true,
            default => ?DEFAULT_COWBOY_TCP_MAX_CONNECTIONS,
            type => pos_integer,
            legacy => 'http_api.tcp.max_connections',
            short_description =>
                <<"Number of connections handled concurrently by "
                  "the HTTP server.">>,
            long_description =>
                <<"Limits the maximum allowed simultaneous TCP "
                  "connections to prevent the system from being "
                  "overloaded.">>,
            handle_set => fun ?MODULE:handle_set_max_connections/4
        },
        #{
            enabled => true,
            option_key => [network, server, socket, nodelay],
            default => ?DEFAULT_COWBOY_TCP_NODELAY,
            type => boolean,
            legacy => 'http_api.tcp.nodelay',
            short_description =>
                <<"HTTP server TCP nodelay (Nagle disable) socket "
                  "option.">>
        },
        #{
            enabled => true,
            option_key => [network, server, http, num_acceptors],
            default => ?DEFAULT_COWBOY_TCP_NUM_ACCEPTORS,
            type => pos_integer,
            legacy => 'http_api.tcp.num_acceptors',
            short_description =>
                <<"HTTP server number of TCP acceptor processes.">>
        },
        #{
            enabled => true,
            option_key => [network, server, socket, send_timeout],
            default => ?DEFAULT_COWBOY_TCP_SEND_TIMEOUT,
            type => pos_integer,
            legacy => 'http_api.tcp.send_timeout',
            short_description =>
                <<"HTTP server TCP send timeout in milliseconds.">>
        },
        #{
            enabled => true,
            option_key => [network, server, socket, send_timeout_close],
            default => ?DEFAULT_COWBOY_TCP_SEND_TIMEOUT_CLOSE,
            type => boolean,
            legacy => 'http_api.tcp.send_timeout_close',
            short_description =>
                <<"HTTP server TCP send_timeout_close socket "
                  "option.">>
        },
        #{
            enabled => true,
            option_key => [network, server, http, idle_timeout],
            runtime => true,
            default => ?DEFAULT_COWBOY_TCP_IDLE_TIMEOUT_SECOND * 1000,
            type => pos_integer,
            legacy => http_api_transport_idle_timeout,
            short_description =>
                <<"Time allowed for incoming API client connections "
                  "to be idle before closing them, in "
                  "milliseconds.">>,
            long_description =>
                <<"Do not set this value too low as it will "
                  "negatively affect node performance. Legacy JSON / "
                  "CLI `http_api.tcp.idle_timeout_seconds` (value × "
                  "1000).">>,
            handle_set => fun ?MODULE:handle_set_protocol_opt/4
        },
        #{
            enabled => true,
            option_key => [network, server, shutdown, connection_timeout],
            runtime => true,
            default => ?SHUTDOWN_TCP_CONNECTION_TIMEOUT,
            type => pos_integer,
            legacy => shutdown_tcp_connection_timeout,
            short_description =>
                <<"Shutdown TCP connection timeout in seconds.">>
        },
        #{
            enabled => true,
            option_key => [network, server, shutdown, mode],
            runtime => true,
            default => ?SHUTDOWN_TCP_MODE,
            type => atom,
            legacy => shutdown_tcp_mode,
            short_description =>
                <<"Shutdown TCP mode (shutdown or close).">>
        },
        #{
            enabled => true,
            option_key => [network, server, socket, backend],
            default => ?DEFAULT_SOCKET_BACKEND,
            type => atom,
            legacy => 'socket.backend',
            short_description =>
                <<"Erlang default socket backend (inet or socket).">>
        },

        %-----------------------------------------------------
        % HTTP client (Gun) knobs
        %-----------------------------------------------------
        #{
            enabled => true,
            option_key => [network, client, http, closing_timeout],
            runtime => true,
            default => ?DEFAULT_GUN_HTTP_CLOSING_TIMEOUT,
            type => pos_integer,
            legacy => 'http_client.http.closing_timeout',
            short_description =>
                <<"HTTP client connection closing timeout in "
                  "milliseconds.">>
        },
        #{
            enabled => true,
            option_key => [network, client, http, keepalive],
            runtime => true,
            default => ?DEFAULT_GUN_HTTP_KEEPALIVE,
            type => pos_integer,
            legacy => 'http_client.http.keepalive',
            short_description =>
                <<"HTTP client keepalive interval in seconds (or "
                  "infinity).">>
        },
        #{
            enabled => true,
            option_key => [network, client, http, connections_per_peer],
            runtime => true,
            default => ?DEFAULT_HTTP_CONNECTIONS_PER_PEER,
            type => pos_integer,
            short_description =>
                <<"Maximum parallel HTTP client connections per peer. The pool "
                  "grows toward this only for peers under sustained load and "
                  "shrinks idle peers back to one.">>
        },
        #{
            enabled => true,
            option_key => [network, client, socket, delay_send],
            runtime => true,
            default => ?DEFAULT_GUN_TCP_DELAY_SEND,
            type => boolean,
            legacy => 'http_client.tcp.delay_send',
            short_description =>
                <<"HTTP client TCP delay_send socket option.">>
        },
        #{
            enabled => true,
            option_key => [network, client, socket, keepalive],
            runtime => true,
            default => ?DEFAULT_GUN_TCP_KEEPALIVE,
            type => boolean,
            legacy => 'http_client.tcp.keepalive',
            short_description =>
                <<"HTTP client TCP keepalive socket option.">>
        },
        #{
            enabled => true,
            option_key => [network, client, socket, linger],
            runtime => true,
            default => ?DEFAULT_GUN_TCP_LINGER,
            type => boolean,
            legacy => 'http_client.tcp.linger',
            short_description =>
                <<"HTTP client TCP linger socket option.">>
        },
        #{
            enabled => true,
            option_key => [network, client, socket, linger_timeout],
            runtime => true,
            default => ?DEFAULT_GUN_TCP_LINGER_TIMEOUT,
            type => pos_integer,
            legacy => 'http_client.tcp.linger_timeout',
            short_description =>
                <<"HTTP client TCP linger timeout in seconds.">>
        },
        #{
            enabled => true,
            option_key => [network, client, socket, nodelay],
            runtime => true,
            default => ?DEFAULT_GUN_TCP_NODELAY,
            type => boolean,
            legacy => 'http_client.tcp.nodelay',
            short_description =>
                <<"HTTP client TCP nodelay (Nagle disable) socket "
                  "option.">>
        },
        #{
            enabled => true,
            option_key => [network, client, socket, send_timeout],
            runtime => true,
            default => ?DEFAULT_GUN_TCP_SEND_TIMEOUT,
            type => pos_integer,
            legacy => 'http_client.tcp.send_timeout',
            short_description =>
                <<"HTTP client TCP send timeout in milliseconds.">>
        },
        #{
            enabled => true,
            option_key => [network, client, socket, send_timeout_close],
            runtime => true,
            default => ?DEFAULT_GUN_TCP_SEND_TIMEOUT_CLOSE,
            type => boolean,
            legacy => 'http_client.tcp.send_timeout_close',
            short_description =>
                <<"HTTP client TCP send_timeout_close socket "
                  "option.">>
        }
    ].

validate() ->
    ok.

group_description() ->
    <<"Tune HTTP server and client connection behavior.">>.

%% @doc handle_set for the cowboy protocol options: forward to the listener,
%% which rebuilds and applies the full protocol opts to new connections.
handle_set_protocol_opt(K, V, _S, _A) ->
    ok = ar_http_iface_server:set_protocol_opt(K, V),
    {store, V}.

%% @doc handle_set for the listener's max-connections cap.
handle_set_max_connections(_K, V, _S, _A) ->
    ok = ar_http_iface_server:set_max_connections(V),
    {store, V}.
