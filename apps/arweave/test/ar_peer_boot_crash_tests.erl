%%% @doc Full-node regression test for the boot crash caused by an
%%% unresolved hostname peer.
%%%
%%% A trusted peer configured as a hostname must be resolved to an
%%% `{A,B,C,D,Port}' tuple before boot. Left as a `<<"host:port">>' binary,
%%% `ar_node_worker:validate_trusted_peers/0' (run during boot) feeds it to
%%% `ar_http:req/1', which `badarg's at `element(5, Peer)' and brings the
%%% node down. `localhost:<peer1 port>' resolves to the running peer1, so
%%% with peer resolution in place the node boots and joins; without it the
%%% boot crashes.
-module(ar_peer_boot_crash_tests).
-test_peers([peer1]).
-include_lib("eunit/include/eunit.hrl").

hostname_trusted_peer_boots_node_test_() ->
    {timeout, 300, fun hostname_trusted_peer_boots_node/0}.

hostname_trusted_peer_boots_node() ->
    [B0] = ar_weave:init(),
    %% peer1 is the trusted peer `main' probes during boot, so it must be up
    %% first. It runs in its own BEAM and survives `main's clean restart.
    ar_test_node:start_peer(peer1, B0),
    %% `localhost' resolves to 127.0.0.1 (peer1's address) without external
    %% DNS, but stays a `<<"host:port">>' binary when peer resolution is off.
    {_, _, _, _, Port} = ar_test_node:peer_ip(peer1),
    Hostname = list_to_binary("localhost:" ++ integer_to_list(Port)),
    %% Boot `main' trusting the hostname. `validate_trusted_peers/0' runs
    %% during boot and GETs each trusted peer's info; an unresolved binary
    %% badargs in `ar_http:req2/1' and the node fails to start.
    ar_test_node:start(#{
        b0 => B0,
        [peers, trusted] => [Hostname]
    }),
    ?assertEqual(ok, ar_test_await:node_joined(main)).
