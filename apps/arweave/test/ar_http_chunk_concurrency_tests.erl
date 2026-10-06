%%% @doc Tests that a node serves a peer's concurrent chunk requests at once.
%%% Cowboy 2.18+ answers the requests pipelined on an HTTP/1.1 connection one
%%% at a time, which halved the sync rate of nodes fetching from a busy peer;
%%% only HTTP/2 lets a peer have more requests served at once than it holds
%%% connections.
-module(ar_http_chunk_concurrency_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

%% Long enough that every request is still being served when the last one
%% arrives.
-define(SERVE_MS, 200).
%% Syncing nodes have been seen keeping 25-37 chunk requests in flight to one
%% peer, four times the default pool ceiling of 8 connections.
-define(IN_FLIGHT, 32).

chunk_requests_are_served_concurrently_test_() ->
    {timeout, 120, fun test_chunk_requests_are_served_concurrently/0}.

test_chunk_requests_are_served_concurrently() ->
    [B0] = ar_weave:init(),
    ar_test_node:start(B0),
    %% 1: requests being served, 2: the most served at once.
    Counters = atomics:new(2, []),
    Chunk = binary:copy(<<7>>, ?DATA_CHUNK_SIZE),
    Mocks = [
        {ar_sync_record, is_recorded, fun
            (_Offset, ar_data_sync) ->
                {{true, unpacked}, "default"};
            (Offset, ID) ->
                meck:passthrough([Offset, ID])
        end},
        {ar_data_sync, get_chunk, fun
            (Offset, #{origin := http}) ->
                serve(Counters, Offset, Chunk);
            (Offset, Args) ->
                meck:passthrough([Offset, Args])
        end}
    ],
    ar_test_node:run_with_mocked([main], Mocks, fun() ->
        %% The node requests chunks from itself: the client and the server
        %% are the ones peers use.
        Peer = ar_test_node:peer_ip(main),
        Replies = arweave_lib_util:pmap(
            fun(I) ->
                ar_http_iface_client:get_chunk_binary(
                    Peer,
                    I * ?DATA_CHUNK_SIZE,
                    unpacked
                )
            end,
            lists:seq(1, ?IN_FLIGHT)
        ),
        [?assertMatch({ok, _Proof, _Time, _Size}, Reply) || Reply <- Replies],
        Connections = arweave_config:get(
            [network, client, http, connections_per_peer]
        ),
        %% Over HTTP/1.1, cowboy 2.18+ works on one request per connection at
        %% a time, so serving more requests at once than we hold connections
        %% shows that requests on one connection were served concurrently.
        ?assert(atomics:get(Counters, 2) > Connections)
    end).

%% @doc Serve a chunk in ?SERVE_MS, counting the requests served at once.
serve(Counters, Offset, Chunk) ->
    Serving = atomics:add_get(Counters, 1, 1),
    record_most_served(Counters, Serving),
    timer:sleep(?SERVE_MS),
    atomics:sub(Counters, 1, 1),
    {ok, #{
        chunk => Chunk,
        tx_path => <<>>,
        data_path => <<>>,
        absolute_end_offset => Offset
    }}.

record_most_served(Counters, Serving) ->
    Most = atomics:get(Counters, 2),
    case
        Serving > Most andalso
            atomics:compare_exchange(Counters, 2, Most, Serving) /= ok
    of
        true -> record_most_served(Counters, Serving);
        false -> ok
    end.
