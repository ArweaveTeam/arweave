-module(ar_v1_denomination0_tx_tests).
-test_peers([peer1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Format-1 transactions signed without a denomination are deprecated. Nodes
%%% drop them on arrival, only process them inside blocks, and serve them
%%% publicly only once they are deep enough. This behavior precedes the fork
%%% 2.9.6 rejection of all format-1 transactions (height 0 under the test
%%% profile's FORKS_RESET), so every test runs with fork 2.9.6 mocked away.

standalone_v1_denomination0_tx_is_dropped_test_() ->
    with_fork_2_9_6_disabled(
            fun test_standalone_v1_denomination0_tx_is_dropped/0).

v1_denomination0_tx_in_block_test_() ->
    with_fork_2_9_6_disabled(fun test_v1_denomination0_tx_in_block/0).

load_from_disk_drops_v1_denomination0_txs_test_() ->
    with_fork_2_9_6_disabled(
            fun test_load_from_disk_drops_v1_denomination0_txs/0).

genesis_v1_denomination0_tx_is_served_test_() ->
    with_fork_2_9_6_disabled(fun test_genesis_v1_denomination0_tx_is_served/0).

polled_v1_denomination0_tx_is_dropped_once_test_() ->
    with_fork_2_9_6_disabled(
            fun test_polled_v1_denomination0_tx_is_dropped_once/0).

with_fork_2_9_6_disabled(TestFun) ->
    ar_test_node:test_with_all_nodes_mocked(
            [{ar_fork, height_2_9_6, fun() -> infinity end}],
            TestFun, ?TEST_NODE_TIMEOUT).

test_standalone_v1_denomination0_tx_is_dropped() ->
    {Key, B0} = new_funded_wallet(),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:connect_to_peer(peer1),
    V1TX = ar_test_node:sign_v1_tx(Key, #{ denomination => 0 }),
    ?assert(ar_tx:is_v1_denomination0_tx(V1TX)),
    ID = V1TX#tx.id,
    %% The node rejects the transaction and forgets it.
    ?assertMatch({ok, {{<<"400">>, _}, _, ?V1_DENOMINATION0_TX_REJECTED, _, _}},
                 post_tx(main, V1TX)),
    ?assertNot(ar_mempool:has_tx(ID)),
    ?assertNot(ar_ignore_registry:member(ID)),
    ?assertEqual(<<"404">>, http_get_status(main, tx_path(ID))),
    ?assertEqual(<<"404">>, http_get_status(main, tx_path(ID) ++ "/status")),
    %% A format-1 transaction signed with a denomination is not deprecated
    %% and is accepted as usual.
    SafeTX = ar_test_node:sign_v1_tx(Key, #{ denomination => 1 }),
    ?assertNot(ar_tx:is_v1_denomination0_tx(SafeTX)),
    ar_test_node:assert_post_tx_to_peer(main, SafeTX),
    ar_test_node:mine(main),
    {ok, [{H, _, _} | _]} = ar_test_await:node_height(main, 1),
    ?assertEqual([SafeTX#tx.id], block_txids(H)),
    ?assertEqual(<<"200">>, http_get_status(main, tx_path(SafeTX#tx.id))).

test_v1_denomination0_tx_in_block() ->
    {Key, B0} = new_funded_wallet(),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:connect_to_peer(peer1),
    V1TX = ar_test_node:sign_v1_tx(Key, #{ denomination => 0 }),
    ID = V1TX#tx.id,
    %% peer1 stands in for a node that still mines these transactions.
    with_v1_txs_accepted([peer1], fun() ->
        ar_test_node:assert_post_tx_to_peer(peer1, V1TX),
        ar_test_node:mine(peer1),
        {ok, [{H, _, _} | _]} = ar_test_await:node_height(main, 1),
        ?assertEqual([ID], block_txids(H)),
        %% The block is applied but the transaction stays out of the
        %% mempool and is stored under its block identifier.
        ?assertNot(ar_mempool:has_tx(ID)),
        ok = ar_test_await:until(v1_tx_stored, fun() ->
            case ar_storage:read_tx(ID) of
                #tx{} = TX -> TX#tx.signature == V1TX#tx.signature;
                _ -> false
            end
        end),
        ?assertMatch({ok, {1, H}}, ar_storage:get_tx_confirmation_data(ID)),
        ?assertEqual(<<"404">>, http_get_status(main, tx_path(ID))),
        ?assertEqual(<<"404">>, http_get_status(main, tx2_path(ID))),
        ?assertEqual(<<"404">>, http_get_status(main, tx_path(ID) ++ "/data")),
        ?assertEqual(<<"404">>,
                     http_get_status(main, tx_path(ID) ++ "/status")),
        %% Peers still fetch block transactions through GET /unconfirmed_tx.
        ?assertEqual(<<"200">>, http_get_status(main, unconfirmed_tx_path(ID))),
        %% The transaction, confirmed at height 1, is served once it has
        %% ?V1_DENOMINATION0_TX_MIN_CONFIRMATIONS confirmations. Mining that
        %% many blocks is too slow, so pretend the chain grew.
        at_height(?V1_DENOMINATION0_TX_MIN_CONFIRMATIONS - 1, fun() ->
            ?assertEqual(<<"404">>, http_get_status(main, tx_path(ID)))
        end),
        at_height(?V1_DENOMINATION0_TX_MIN_CONFIRMATIONS, fun() ->
            ?assertEqual(<<"200">>, http_get_status(main, tx_path(ID))),
            ?assertEqual(<<"200">>, http_get_status(main, tx2_path(ID))),
            ?assertEqual(<<"200">>,
                         http_get_status(main, tx_path(ID) ++ "/data")),
            ?assertEqual(<<"200">>,
                         http_get_status(main, tx_path(ID) ++ "/status"))
        end)
    end).

test_load_from_disk_drops_v1_denomination0_txs() ->
    {Key, B0} = new_funded_wallet(),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    V1TX = ar_test_node:sign_v1_tx(Key, #{ denomination => 0 }),
    SafeTX = ar_test_node:sign_v1_tx(Key, #{ denomination => 1 }),
    SerializedTXs = #{
        V1TX#tx.id => {ar_serialize:tx_to_binary(V1TX), waiting},
        SafeTX#tx.id => {ar_serialize:tx_to_binary(SafeTX), waiting}
    },
    ok = ar_storage:write_term(mempool, {SerializedTXs, {0, 0}}),
    ar_mempool:reset(),
    ar_mempool:load_from_disk(),
    ?assertNot(ar_mempool:has_tx(V1TX#tx.id)),
    ?assert(ar_mempool:has_tx(SafeTX#tx.id)),
    ?assertEqual([SafeTX#tx.id], ar_mempool:get_all_txids()).

test_genesis_v1_denomination0_tx_is_served() ->
    %% Mainnet's genesis transactions are format-1 transactions without a
    %% denomination stored from the repository files, without confirmation
    %% data. ar_storage records them as confirmed by the genesis block when
    %% the node starts, after which they are served like any other deep
    %% enough transaction. Exercise that record with the test genesis
    %% transaction.
    {_Key, B0} = new_funded_wallet(),
    [GenesisTX] = B0#block.txs,
    ?assert(ar_tx:is_v1_denomination0_tx(GenesisTX)),
    ID = GenesisTX#tx.id,
    ar_test_node:start(B0),
    ok = ar_test_await:until(genesis_tx_confirmed, fun() ->
        ar_storage:get_tx_confirmation_data(ID) == {ok, {0, B0#block.indep_hash}}
    end),
    ok = ar_kv:delete(tx_confirmation_db, ID),
    at_height(?V1_DENOMINATION0_TX_MIN_CONFIRMATIONS, fun() ->
        ?assertEqual(<<"404">>, http_get_status(main, tx_path(ID))),
        ok = ar_storage:update_confirmation_index(B0),
        ?assertEqual(<<"200">>, http_get_status(main, tx_path(ID))),
        ?assertEqual(<<"200">>, http_get_status(main, tx2_path(ID))),
        ?assertEqual(<<"200">>,
                     http_get_status(main, tx_path(ID) ++ "/status"))
    end).

test_polled_v1_denomination0_tx_is_dropped_once() ->
    %% peer1 keeps a format-1 transaction without a denomination in its
    %% mempool. main learns of it by polling peer1's mempool and drops it.
    %% It must not download and verify it again on every poll.
    {Key, B0} = new_funded_wallet(),
    ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:disconnect_from(peer1),
    V1TX = ar_test_node:sign_v1_tx(peer1, Key, #{ denomination => 0 }),
    ID = V1TX#tx.id,
    Self = self(),
    Mocks = [{ar_http_iface_client, get_tx_from_remote_peers,
              fun(Peers, TXID, RatePeer) ->
                  Self ! {get_tx, TXID},
                  meck:passthrough([Peers, TXID, RatePeer])
              end}],
    %% peer1 stands in for a node that still accepts these transactions.
    with_v1_txs_accepted([peer1], fun() ->
      ar_test_node:run_with_mocked([main], Mocks, fun() ->
        ar_test_node:assert_post_tx_to_peer(peer1, V1TX),
        ar_test_node:connect_to_peer(peer1),
        receive
            {get_tx, ID} ->
                ok
        after 60_000 ->
            ?assert(false, "main did not poll the transaction from peer1.")
        end,
        %% Polls run every 500 ms in tests.
        receive
            {get_tx, ID} ->
                ?assert(false, "The dropped transaction was downloaded again.")
        after 5_000 ->
            ok
        end,
        ?assertNot(ar_mempool:has_tx(ID))
      end)
    end).

%% @doc Run Fun while the nodes stand in for nodes that still accept format-1
%% transactions without a denomination into their mempools.
with_v1_txs_accepted(Nodes, Fun) ->
    ar_test_node:run_with_mocked(Nodes,
            [{ar_tx, is_v1_denomination0_tx, fun(_TX) -> false end}], Fun).

%% @doc Run Fun while main pretends its chain is Height blocks high.
at_height(Height, Fun) ->
    ar_test_node:run_with_mocked([main],
            [{ar_node, get_height, fun() -> Height end}], Fun).

new_funded_wallet() ->
    Key = {_, Pub} = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(1000), <<>>}]),
    {Key, B0}.

block_txids(H) ->
    B = ar_block_cache:get(block_cache, H),
    [case TX of #tx{ id = TXID } -> TXID; TXID -> TXID end || TX <- B#block.txs].

tx_path(ID) ->
    "/tx/" ++ binary_to_list(arweave_util:encode(ID)).

tx2_path(ID) ->
    "/tx2/" ++ binary_to_list(arweave_util:encode(ID)).

unconfirmed_tx_path(ID) ->
    "/unconfirmed_tx/" ++ binary_to_list(arweave_util:encode(ID)).

post_tx(Node, TX) ->
    ar_test_node:post_tx_json(Node,
            ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))).

http_get_status(Node, Path) ->
    {ok, {{Status, _}, _, _, _, _}} = ar_http:req(#{
        method => get,
        peer => ar_test_node:peer_ip(Node),
        path => Path
    }),
    Status.
