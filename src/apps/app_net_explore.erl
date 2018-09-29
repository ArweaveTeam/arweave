-module(app_net_explore).
-export([graph/0, graph/1]).
-export([get_all_nodes/0, get_live_nodes/0]).
-export([filter_offline_nodes/1]).

%%% Tools for building a map of connected peers.
%%% Requires graphviz for visualisation.

%% The directory graphs and map files should be saved to.
-define(MAP_DIR, "maps").

%% @doc Build a snapshot graph in PNG form of the current state of the network.
graph() ->
    io:format("Getting live peers...~n"),
    graph(get_live_nodes()).
graph(Nodes) ->
    io:format("Generating connection map...~n"),
    Map = generate_map(Nodes),
    ar:d(Map),
    io:format("Generating dot file...~n"),
    {{Year, Month, Day}, {Hour, Minute, Second}} =
        calendar:now_to_datetime(erlang:now()),
    StrTime =
        lists:flatten(
            io_lib:format(
                "~4..0w-~2..0w-~2..0wT~2..0w:~2..0w:~2..0w",
                [Year, Month, Day, Hour, Minute, Second]
            )
        ),
    DotFile =
        lists:flatten(
            io_lib:format(
                "~s/~s.~s",
                [?MAP_DIR, StrTime, "dot"]
            )
        ),
    PngFile =
        lists:flatten(
            io_lib:format(
                "~s/~s.~s",
                [?MAP_DIR, StrTime, "png"]
            )
        ),
    ok = filelib:ensure_dir(DotFile),
    ok = generate_dot_file(DotFile, Map),
    io:format("Generating PNG image...~n"),
    os:cmd("dot -Tpng " ++ DotFile ++ " -o " ++ PngFile),
    io:format("Done! Image written to: '" ++ PngFile ++ "'~n").


%% @doc Return a list of nodes that are active and connected to the network.
get_live_nodes() ->
    filter_offline_nodes(get_all_nodes()).

%% @doc Return a list of all nodes that are claimed to be in the network.
get_all_nodes() ->
    get_all_nodes([], ar_bridge:get_remote_peers(whereis(http_bridge_node))).
get_all_nodes(Done, []) -> Done;
get_all_nodes(Done, [Next|Peers]) ->
    io:format("Getting peers from ~s... ", [ar_util:format_peer(Next)]),
    NewPeers = ar_http_iface:get_peers(Next),
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

%% @doc Return a map of every peers connections.
%% Returns a list of tuples with arity 2. The first element is the local peer,
%% the second element is the list of remote peers it talks to.
generate_map(Peers) ->
    lists:map(
        fun(Peer) ->
            {
                Peer,
                lists:filter(
                    fun(RemotePeer) ->
                        lists:member(RemotePeer, Peers)
                    end,
                    ar_http_iface:get_peers(Peer)
                )
            } 
        end,
        Peers
    ).

%% @doc Generate a dot file that can be rendered into a PNG.
generate_dot_file(File, Map) ->
    case file:open(File, [write]) of
        {ok, FileRef} ->
            io:fwrite(FileRef, "digraph network_map { ~n", []),
            io:fwrite(FileRef, 
                      "    init [style=filled,color=\".7 .3 .9\"];~n", []),
            do_generate_dot_file(Map, FileRef),
            ok;
        _ ->
            io:format("Failed to open file for writing.~n"),
            io_error
    end.

do_generate_dot_file([], File) ->
    io:fwrite(File, "} ~n", []),
    file:close(File);
do_generate_dot_file([Host|Rest], File) ->
    {IP, Peers} = Host,
    lists:foreach(
        fun(Peer) ->
            io:fwrite(
                File,
                "\t\"~s\"  ->  \"~s\";  ~n",
                [ar_util:format_peer(IP), ar_util:format_peer(Peer)])
        end,
        Peers
    ),
    do_generate_dot_file(Rest, File).
