%%% @doc Tests for the HTTP version ar_http speaks to a peer and how it sizes
%%% the peer's connection pool, against a stub HTTP server.
-module(ar_http_protocol_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

%% A peer that accepts HTTP/2 is reached over it. The default request headers
%% (X-Network, ...) arrive, so their names were lowercased as HTTP/2 requires.
http2_by_default_test() ->
    with_server(#{}, fun(Peer) ->
        ?assertMatch(
            {'HTTP/2', Network} when is_binary(Network),
            get_echo(Peer)
        ),
        ?assert(prometheus_gauge:value(outbound_connections, [http2]) >= 1)
    end).

%% A peer that only accepts HTTP/1.1 refuses the HTTP/2 preface: the request
%% is retried over HTTP/1.1, and the next connection opened to the peer (a
%% pool grows with every request up to its ceiling) uses it too.
http1_fallback_test() ->
    with_server(#{protocols => [http]}, fun(Peer) ->
        ?assertMatch({'HTTP/1.1', _}, get_echo(Peer)),
        ?assertMatch({'HTTP/1.1', _}, get_echo(Peer))
    end).

%% Releases before this one send rate limit headers with uppercase names,
%% which an HTTP/2 client rejects: the request is retried over HTTP/1.1, and
%% the peer is reached that way afterwards.
uppercase_response_header_fallback_test() ->
    Routes = #{
        {<<"GET">>, <<"/echo">>} =>
            fun(Req) ->
                {200, Headers, Body} = echo(Req),
                {200, Headers#{<<"RateLimit-Remaining">> => <<"1">>}, Body}
            end
    },
    with_server(Routes, #{}, fun(Peer) ->
        ?assertMatch({'HTTP/1.1', _}, get_echo(Peer)),
        ?assertMatch({'HTTP/1.1', _}, get_echo(Peer))
    end).

%% Servers that are not Arweave nodes are reached over HTTP/1.1.
non_peer_request_test() ->
    with_server(#{}, fun(Peer) ->
        ?assertMatch(
            {'HTTP/1.1', undefined},
            get_echo(Peer, #{is_peer_request => false})
        )
    end).

%% Changing the protocol option moves a peer's pool over on its next request.
protocol_option_test() ->
    with_server(#{}, fun(Peer) ->
        ?assertMatch({'HTTP/2', _}, get_echo(Peer)),
        ok = arweave_config:set([network, client, http, protocol], http),
        try
            ?assertMatch({'HTTP/1.1', _}, get_echo(Peer))
        after
            ok = arweave_config:set([network, client, http, protocol], http2)
        end,
        ?assertMatch({'HTTP/2', _}, get_echo(Peer)),
        ?assertMatch(
            {error, _},
            arweave_config:set([network, client, http, protocol], ftp)
        )
    end).

%% @doc HTTP/2 multiplexes held requests across the configured connection cap.
http2_pool_grows_to_ceiling_test() ->
    %% 64 requests exceed the default eight-connection cap eightfold.
    ClientPorts = ets:new(client_ports, [set, public]),
    Parent = self(),
    Hold =
        fun(Req) ->
            {_IP, Port} = cowboy_req:peer(Req),
            ets:insert(ClientPorts, {Port}),
            Parent ! {pool_request_held, self()},
            receive release -> ok after 3000 -> error(not_released) end,
            {200, #{}, atom_to_binary(cowboy_req:version(Req))}
        end,
    Routes = #{{<<"GET">>, <<"/hold">>} => Hold},
    with_server(Routes, #{}, fun(Peer) ->
        Request = #{
            method => get,
            peer => Peer,
            path => "/hold",
            timeout => 10_000
        },
        [
            spawn_link(fun() -> Parent ! {reply, ar_http:req(Request)} end)
         || _ <- lists:seq(1, 64)
        ],
        %% Release only after every request arrived; no host-speed assumption.
        Handlers = [receive
            {pool_request_held, PID} -> PID
        after 3000 -> error(missing_pool_request)
        end || _ <- lists:seq(1, 64)],
        lists:foreach(fun(PID) -> PID ! release end, Handlers),
        Replies = [
            receive
                {reply, Reply} -> Reply
            end
         || _ <- lists:seq(1, 64)
        ],
        [
            ?assertMatch({ok, {{<<"200">>, _}, _, <<"HTTP/2">>, _, _}}, Reply)
         || Reply <- Replies
        ],
        ?assertEqual(
            arweave_config:get([network, client, http, connections_per_peer]),
            ets:info(ClientPorts, size)
        )
    end),
    ets:delete(ClientPorts).

%% @doc A bounded HTTP/2 burst gets a real 429, updates quota, and recovers.
http2_rate_limit_test() ->
    Parent = self(),
    Calls = atomics:new(1, []),
    OldLimit = arweave_config:get([limiter, metrics, concurrency_limit]),
    OldLocalPeers = arweave_config:get([peers, local]),
    %% One held request exhausts concurrency; a second request must get 429.
    ok = arweave_config:set([limiter, metrics, concurrency_limit], 1),
    %% Use the endpoint's limiter, not the separate local-peer limiter.
    ok = arweave_config:set([peers, local], []),
    HoldFirst = fun(Req) ->
        case atomics:add_get(Calls, 1, 1) of
            1 ->
                Parent ! {limited_request_held, self()},
                receive release -> ok after 3000 -> error(not_released) end;
            _ -> ok
        end,
        echo(Req)
    end,
    %% Use real limiting on /metrics, but echo the version instead of metrics.
    Routes = #{{<<"GET">>, <<"/metrics">>} => HoldFirst},
    Opts = #{middlewares => [ar_http_iface_rate_limiter_middleware,
        cowboy_router, cowboy_handler]},
    try
        with_server(Routes, Opts, fun(Peer) ->
            %% The test server keys quota by this header, isolating repeat runs.
            Request = #{method => get, peer => Peer, path => "/metrics",
                timeout => 3000, headers => [{<<"x-p2p-port">>,
                    integer_to_binary(element(5, Peer))}]},
            %% Wait until the first request occupies the sole concurrency slot.
            First = start_request(Request),
            Handler = receive
                {limited_request_held, PID} -> PID
            after 3000 -> error(no_held_request)
            end,
            try
                %% The middleware must reject this request before the handler.
                Reply = ar_http:req(Request),
                ?assertMatch({ok, {{<<"429">>, _}, _, _, _, _}}, Reply),
                {ok, {_, Headers, _, _, _}} = Reply,
                ?assertEqual(<<"0">>, proplists:get_value(
                    <<"ratelimit-remaining">>, Headers)),
                %% Concurrency rejection advertises a one-second retry delay.
                ?assertEqual(<<"1">>, proplists:get_value(
                    <<"retry-after">>, Headers)),
                %% Check that ar_http passed the response quota to throttling.
                ?assert(arweave_throttling:is_throttled(Peer, "/metrics")),
                %% Completion supplies quota headers that can unblock requests;
                %% this tests recovery, not the exact one-second backoff.
                Handler ! release,
                ?assertMatch({'HTTP/2', _}, request_result(First)),
                %% A 429 must not make later requests fall back to HTTP/1.
                Next = start_request(Request),
                ?assertMatch({'HTTP/2', _}, request_result(Next)),
                %% Only the first and recovered requests reach the handler.
                ?assertEqual(2, atomics:get(Calls, 1))
            after
                Handler ! release
            end
        end)
    after
        ok = arweave_config:set([limiter, metrics, concurrency_limit], OldLimit),
        ok = arweave_config:set([peers, local], OldLocalPeers)
    end.

%%% Helpers

%% @doc Start a request without blocking the test process.
start_request(Request) ->
    Parent = self(),
    spawn_link(fun() ->
        Reply = ar_http:req(Request),
        Parent ! {request_result, self(), Reply}
    end).

%% @doc Decode the version returned by a held echo request.
request_result(Caller) ->
    receive
        {request_result, Caller, {ok, {{<<"200">>, _}, _, Body, _, _}}} ->
            binary_to_term(Body);
        {request_result, Caller, Error} -> Error
    after 3000 -> error({no_result, Caller})
    end.

with_server(Opts, Fun) ->
    with_server(#{{<<"GET">>, <<"/echo">>} => fun echo/1}, Opts, Fun).

with_server(Routes, Opts, Fun) ->
    {ok, Peer, Ref, Table} = ar_test_http_server:start(Routes, Opts),
    try
        Fun(Peer)
    after
        ar_test_http_server:stop(Ref, Table)
    end.

echo(Req) ->
    Version = cowboy_req:version(Req),
    Network = cowboy_req:header(<<"x-network">>, Req),
    {200, #{}, term_to_binary({Version, Network})}.

get_echo(Peer) ->
    get_echo(Peer, #{}).

get_echo(Peer, Args) ->
    {ok, {{<<"200">>, _}, _, Body, _, _}} = ar_http:req(Args#{
        method => get,
        peer => Peer,
        path => "/echo",
        timeout => 5_000
    }),
    binary_to_term(Body).
