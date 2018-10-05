-module(app_net_explore).
-export([get_all_nodes/0, get_live_nodes/0]).
-export([filter_offline_nodes/1]).

%%% Tools for building a map of connected peers.

%% @doc Return a list of nodes that are active and connected to the network.
get_live_nodes() ->
    filter_offline_nodes(get_all_nodes()).

%% @doc Return a list of all nodes that are claimed to be in the network.
get_all_nodes() ->
    get_all_nodes([], ar_bridge:get_remote_peers(whereis(http_bridge_node))).
get_all_nodes(Done, []) -> Done;
get_all_nodes(Done, [Next|Peers]) ->
    io:format("Getting peers from ~s... ", [ar_util:format_peer(Next)]),
    NewPeers = ar_http_iface_client:get_peers(Next),
    io:format(" got ~w!~n", [length(NewPeers)]),
    get_all_nodes(
        [Next|Done],
        (ar_util:unique(Peers ++ NewPeers)) -- [Next|Done]
    ).

%% @doc Remove offline nodes from a list of peers.
filter_offline_nodes(Peers) ->
    lists:filter(
        fun(Peer) ->
            ar_http_iface:get_info(Peer) =/= info_unavailable
        end,
        Peers
    ).
