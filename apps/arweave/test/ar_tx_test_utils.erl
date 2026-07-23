-module(ar_tx_test_utils).

-export([
    one_wallet_list_one_block_anchored_txs/2,
    two_block_anchored_txs/2,
    empty_tx_set/2,
    block_anchor_txs_spending_balance_plus_one_more/2,
    mixed_anchor_txs_spending_balance_plus_one_more/2,
    grouped_txs/0,
    mine_blocks/2,
    post_tx_to_peer_once/2
]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

one_wallet_list_one_block_anchored_txs(Key, B0) ->
    %% Return thunks so the caller signs after the node starts, when the price
    %% estimate is available.
    {_, {KeyType, _}} = Key,
    TX1Fun = fun() ->
        case KeyType of
            ?RSA_KEY_TYPE ->
                ar_test_node:sign_v1_tx(Key, #{ reward => ?AR(1) });
            ?ECDSA_KEY_TYPE ->
                ar_test_node:sign_tx(Key, #{ reward => ?AR(1), last_tx => <<>> })
        end end,
    TX2Fun = fun() ->
        case KeyType of
            ?RSA_KEY_TYPE ->
                ar_test_node:sign_v1_tx(Key, #{ reward => ?AR(1),
                        last_tx => B0#block.indep_hash });
            ?ECDSA_KEY_TYPE ->
                ar_test_node:sign_tx(Key, #{ reward => ?AR(1),
                        last_tx => B0#block.indep_hash })
        end end,
    [TX1Fun, TX2Fun].

two_block_anchored_txs(Key, B0) ->
    %% Return thunks so the caller signs after the node starts, when the price
    %% estimate is available.
    {_, {KeyType, _}} = Key,
    TX1Fun = fun() ->
        case KeyType of
            ?RSA_KEY_TYPE ->
                ar_test_node:sign_v1_tx(Key, #{ reward => ?AR(1),
                        last_tx => B0#block.indep_hash });
            ?ECDSA_KEY_TYPE ->
                ar_test_node:sign_tx(Key, #{ reward => ?AR(1),
                        last_tx => B0#block.indep_hash })
        end end,
    TX2Fun = fun() ->
        case KeyType of
            ?RSA_KEY_TYPE ->
                ar_test_node:sign_v1_tx(Key, #{ reward => ?AR(1),
                        last_tx => B0#block.indep_hash });
            ?ECDSA_KEY_TYPE ->
                ar_test_node:sign_tx(Key, #{ reward => ?AR(1),
                        last_tx => B0#block.indep_hash,
                        %% A tag to distinguish deterministic ECDSA transactions.
                        tags => [{<<"id">>, <<>>}] })
        end end,
    [TX1Fun, TX2Fun].

empty_tx_set(_Key, _B0) ->
    [].

block_anchor_txs_spending_balance_plus_one_more(Key, B0) ->
    TX1 = ar_test_node:sign_v1_tx(Key, #{ denomination => 1,
            reward => ?AR(10), last_tx => B0#block.indep_hash }),
    TX2 = ar_test_node:sign_v1_tx(Key, #{ denomination => 1,
            reward => ?AR(10), last_tx => B0#block.indep_hash }),
    TX3 = ar_test_node:sign_v1_tx(Key, #{ denomination => 1,
            reward => ?AR(1), last_tx => B0#block.indep_hash }),
    [TX1, TX2, TX3].

mixed_anchor_txs_spending_balance_plus_one_more(Key, B0) ->
    TX1 = ar_test_node:sign_v1_tx(Key, #{ denomination => 1, reward => ?AR(10), last_tx => <<>> }),
    TX2 = ar_test_node:sign_v1_tx(Key, #{ denomination => 1, reward => ?AR(5),
            last_tx => B0#block.indep_hash }),
    TX3 = ar_test_node:sign_v1_tx(Key, #{ denomination => 1, reward => ?AR(2),
            last_tx => B0#block.indep_hash }),
    TX4 = ar_test_node:sign_v1_tx(Key, #{ denomination => 1,
            reward => ?AR(3), last_tx => B0#block.indep_hash }),
    TX5 = ar_test_node:sign_v1_tx(Key, #{ denomination => 1,
            reward => ?AR(1), last_tx => B0#block.indep_hash }),
    [TX1, TX2, TX3, TX4, TX5].

grouped_txs() ->
    Key1 = {_, Pub1} = ar_wallet:new(),
    Key2 = {_, Pub2} = ar_wallet:new(),
    Wallets = [
        {ar_wallet:to_address(Pub1), ?AR(100), <<>>},
        {ar_wallet:to_address(Pub2), ?AR(100), <<>>}
    ],
    [B0] = ar_weave:init(Wallets),
    Chunk1 = ar_test_node:random_v1_data(?TX_DATA_SIZE_LIMIT),
    Chunk2 = <<"a">>,
    TX1 = ar_test_node:sign_v1_tx(Key1, #{ reward => ?AR(1), data => Chunk1, last_tx => <<>> }),
    TX2 = ar_test_node:sign_v1_tx(Key2, #{ reward => ?AR(1), data => Chunk2,
            last_tx => B0#block.indep_hash }),
    %% TX1 is mined first: it is wallet-list-anchored (mined ahead of block
    %% anchors) and both TXs pay the same minimum price per byte.
    {B0, [[TX1], [TX2]]}.

mine_blocks(Node, TargetHeight) ->
    mine_blocks(Node, 1, TargetHeight).

mine_blocks(_Node, Height, TargetHeight) when Height == TargetHeight + 1 ->
    ok;
mine_blocks(Node, Height, TargetHeight) ->
    ar_test_node:mine(Node),
    ?assertMatch({ok, _}, ar_test_await:node_height(Node, Height)),
    mine_blocks(Node, Height + 1, TargetHeight).

%% @doc Post TX to Node via a single raw HTTP request, with no retries or assertions.
post_tx_to_peer_once(Node, TX) ->
    ar_http:req(#{
        method => post,
        peer => ar_test_node:peer_ip(Node),
        path => "/tx",
        body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
    }).
