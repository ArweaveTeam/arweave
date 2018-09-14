-module(app_page_archiver).
-export([start/3, start/4, stop/1]).
-include("ar.hrl").

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
    case httpc:get(URL = S#state.url) of
        {ok, {{<<"200">>, _}, _, Body, _, _}} ->
            archive_page(S, Body);
        _ ->
            ar:report_console([{could_not_get_base_page, URL}])
    end,
    receive stop -> stopping
    after (S#state.interval * 1000) -> server(S)
    end.

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