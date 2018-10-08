-module(app_page_archiver).
-export([start/3, start/4, stop/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% An example Arweave repeated page archiver.
%%% This utility archives a given URL every given number of seconds.
%%% CAUTION: Please be careful only to archive pages that you have the legal
%%% right to access and copy.

-record(state, {
    queue,
    wallet,
    url,
    interval, % time between archives, in seconds
    last_archive_height
}).


%% Takes a wallet, a URL and an interval time, starting a service to archive the
%% page.
start(Wallet, URL, Interval) ->
    start(whereis(http_entrypoint_node), Wallet, URL, Interval).
start(Node, Wallet, URL, Interval) when not is_tuple(Wallet) ->
    start(Node, ar_wallet:load_keyfile(Wallet), URL, Interval);
start(Node, Wallet, BaseURL, Interval) ->
    spawn(
        fun() ->
            Queue = app_queue:start(Node, Wallet),
            server(
                #state {
                    queue = Queue,
                    wallet = Wallet,
                    url = BaseURL,
                    interval = Interval
                }
            )
        end
    ).

%% Halt the server.
stop(PID) -> PID ! stop.

server(S) ->
    get_pages(S),
    receive stop -> stopping
    after (S#state.interval * 1000) -> server(S)
    end.

get_pages(S = #state { url = URLs }) when is_list(hd(URLs)) ->
    lists:foreach(fun(URL) -> get_pages(S#state { url = URL}) end, URLs);
get_pages(S) ->
    case httpc:request(URL = S#state.url) of
        {ok, {{_, 200, _}, _, Body}} ->
            archive_page(S, Body);
        _ ->
            ar:report_console([{could_not_get_url, URL}])
    end.

archive_page(S, Body) when is_list(Body) ->
    archive_page(S, list_to_binary(Body));
archive_page(S = #state { queue = Queue }, Body) ->
    UnsignedTX =
        ar_tx:new(
            Body,
            0,
            <<>>
        ),
    app_queue:add(Queue, UnsignedTX),
    ar:report(
        [
            {app, ?MODULE},
            {archiving_page, S#state.url}
        ]
    ).

%% @doc Test archiving of data.
archive_data_test() ->
	ar_storage:clear(),
	Wallet = {_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	Node1 = ar_node:start([], Bs),
    Queue = app_queue:start(Node1, Wallet),
    archive_page(#state { queue = Queue }, Dat = <<"TEST">>),
    receive after 1000 -> ok end,
    ar_node:mine(Node1),
    receive after 1000 -> ok end,
    B = ar_node:get_current_block(Node1),
    Dat = (ar_storage:read_tx(hd(ar:d(B#block.txs))))#tx.data.

%% @doc Test full operation.
archive_multiple_times_test() ->
    {timeout, 60, fun() ->
        ar_storage:clear(),
        Wallet = {_Priv1, Pub1} = ar_wallet:new(),
        Bs = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
        Node1 = ar_node:start([], Bs),
        Archiver = start(Node1, Wallet, "http://127.0.0.1:1984/info", 1),
        Res = lists:map(
            fun(_) ->
                ar_node:mine(Node1),
                receive after 500 -> ok end
            end,
            lists:seq(1, 15)
        ),
        ?assert(length(lists:flatten(Res)) >= 2)
    end}.

%% @doc Test operation with multiple URLs.
archive_multiple_urls_at_once_test() ->
    {timeout, 60, fun() ->
        ar_storage:clear(),
        Wallet = {_Priv1, Pub1} = ar_wallet:new(),
        Bs = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
        Node1 = ar_node:start([], Bs),
        Archiver = start(Node1, Wallet, ["http://127.0.0.1:1984/info", "http://127.0.0.1:1984/info"], 100),
        Res = lists:map(
            fun(_) ->
                ar_node:mine(Node1),
                receive after 500 -> ok end
            end,
            lists:seq(1, 15)
        ),
        ?assert(length(lists:flatten(Res)) >= 2)
    end}.