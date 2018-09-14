-module(app_page_archiver).
-export([start/3, start/4, stop/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% An example Arweave repeated page archiver.
%%% This utility archives a given URL every given number of seconds.
%%% CAUTION: Please be careful only to archive pages that you have the legal
%%% right to access and copy.

-record(state, {
    node,
    wallet,
    url,
    interval, % time between archives, in seconds
    last_archive_height
}).


%% Takes a wallet, a URL and an interval time, starting a service to archive the
%% page.
start(Wallet, URL, Interval) ->
    start(whereis(http_entypoint_node), Wallet, URL, Interval).
start(Node, Wallet, URL, Interval) when not is_tuple(Wallet) ->
    start(Node, ar_wallet:load_keyfile(Wallet), URL, Interval);
start(Node, Wallet, BaseURL, Interval) ->
    spawn(
        fun() ->
            server(
                #state {
                    node = Node,
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
    case httpc:request(URL = S#state.url) of
        {ok, {{_, 200, _}, _, Body}} ->
            archive_page(S, Body);
        _ ->
            ar:report_console([{could_not_get_url, URL}])
    end,
    receive stop -> stopping
    after (S#state.interval * 1000) -> server(S)
    end.

archive_page(S, Body) when is_list(Body) ->
    archive_page(S, list_to_binary(Body));
archive_page(S = #state { node = Node }, Body) ->
    Price =
        ar_tx:calculate_min_tx_cost(
            Sz = byte_size(Body),
            ar_node:get_current_diff(Node)
        ),
    Addr = ar_wallet:to_address(S#state.wallet),
    Balance = ar_node:get_balance(Node, Addr),
    if Balance > Price ->
        UnsignedTX =
            ar_tx:new(
                Body,
                Price,
                ar_node:get_last_tx(Node, Addr)
            ),
        SignedTX =
            ar_tx:sign(
                UnsignedTX,
                S#state.wallet
            ),
        ar_node:add_tx(Node, SignedTX),
        ar:report(
            [
                {app, ?MODULE},
                {archiving_page, S#state.url},
                {cost, Price / ?AR(1)},
                {submitted_tx, ar_util:encode(SignedTX#tx.id)},
                {size, Sz}
            ]
        );
    true ->
        ar:report(
            [
                {app, ?MODULE},
                {archiving_page, S#state.url},
                {problem, insufficient_of_funds}
            ]
        )
    end.

%% @doc Test archiving of data.
archive_data_test() ->
	ar_storage:clear(),
	Wallet = {_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	Node1 = ar_node:start([], Bs),
    archive_page(#state { node = Node1, wallet = Wallet }, Dat = <<"TEST">>),
    receive after 1000 -> ok end,
    ar_node:mine(Node1),
    receive after 1000 -> ok end,
    B = ar_node:get_current_block(Node1),
    Dat = (ar_storage:read_tx(hd(ar:d(B#block.txs))))#tx.data.

%% @doc Test full operation.
archive_multiple_test() ->
	ar_storage:clear(),
	Wallet = {_Priv1, Pub1} = ar_wallet:new(),
	Bs = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	Node1 = ar_node:start([], Bs),
    Archiver = start(Node1, Wallet, "http://127.0.0.1:1984/info", 1),
    receive after 500 -> ok end,
    ar_node:mine(Node1),
    receive after 1500 -> ok end,
    B1 = ar_node:get_current_block(Node1),
    ar_node:mine(Node1),
    receive after 1000 -> ok end,
    B2 = ar_node:get_current_block(Node1),
    stop(Archiver),
    1 = ar:d(B1#block.height),
    2 = B2#block.height,
    [_|_] = B1#block.txs,
    [_|_] = B2#block.txs.