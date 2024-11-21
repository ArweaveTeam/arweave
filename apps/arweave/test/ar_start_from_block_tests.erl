-module(ar_start_from_block_tests).

-include_lib("arweave/include/ar_config.hrl").
-include_lib("eunit/include/eunit.hrl").


start_from_block_test_() ->
    [
		{timeout, 240, fun test_start_from_block/0}
	].

test_start_from_block() ->
    [B0] = ar_weave:init([], 0), %% Set difficulty to 0 to speed up tests
	ar_test_node:start(B0),
    ar_test_node:start_peer(peer1, B0),
    ar_test_node:start_peer(peer2, B0),
    ar_test_node:connect_to_peer(peer1),
    ar_test_node:connect_to_peer(peer2),
   
    %% Mine a few blocks, shared by both peers
    ar_test_node:mine(peer1),
    ar_test_node:wait_until_height(peer1, 1),
    ar_test_node:wait_until_height(peer2, 1),
    ar_test_node:mine(peer2),
    ar_test_node:wait_until_height(peer1, 2),
    ar_test_node:wait_until_height(peer2, 2),
    ar_test_node:mine(peer1),
    ar_test_node:wait_until_height(peer1, 3),
    ar_test_node:wait_until_height(peer2, 3),

    %% Disconnect peers, and have peer1 mine 1 block, and peer2 mine 3
    ar_test_node:disconnect_from(peer1),
    ar_test_node:disconnect_from(peer2),

    ar_test_node:mine(peer1),
    ar_test_node:wait_until_height(peer1, 4),

    ar_test_node:mine(peer2),
    ar_test_node:wait_until_height(peer2, 4),
    ar_test_node:mine(peer2),
    ar_test_node:wait_until_height(peer2, 5),
    ar_test_node:mine(peer2),
    ar_test_node:wait_until_height(peer2, 6),

    %% Reconnect the peers. This will orphan peer1's block
    ar_test_node:connect_to_peer(peer1),
    ar_test_node:connect_to_peer(peer2),

    ar_test_node:wait_until_height(peer1, 6),
    ar_test_node:wait_until_height(peer2, 6),
    ar_test_node:wait_until_height(6),

    ar_test_node:disconnect_from(peer1),
    ar_test_node:disconnect_from(peer2),

    MainBI = ar_node:get_blocks(),

    StartFrom = get_block_hash(4, MainBI),
    StartMinus1 = get_block_hash(3, MainBI),
    StartPlus1 = get_block_hash(5, MainBI),

    assert_block_index(peer1, 6, MainBI),
    assert_block_index(peer2, 6, MainBI),
    assert_reward_history(main, peer1, StartFrom),
    assert_reward_history(main, peer2, StartFrom),
    assert_reward_history(main, peer1, StartMinus1),
    assert_reward_history(main, peer2, StartMinus1),

    %% Have peer1 start_from_block
    restart_from_block(peer1, StartFrom),
    assert_start_from(main, peer1, 4),
    restart_from_block(peer1, StartMinus1),
    assert_start_from(main, peer1, 3),

    %% Restart peer2 off of peer1
    ar_test_node:start_peer(peer2, B0),
    ar_test_node:remote_call(peer2, ar_test_node, connect_to_peer, [peer1]),
    ar_test_node:wait_until_height(peer2, 3),

    assert_start_from(main, peer1, 3),
    assert_start_from(main, peer2, 3),

    %% disconnect peer2 and mine a block on peer1
    ar_test_node:remote_call(peer2, ar_test_node, disonnect_from, [peer1]),
    ar_test_node:mine(peer1),
    ar_test_node:wait_until_height(peer1, 4),

    %% Confirm legacy block index still matches
    assert_start_from(main, peer1, 3),

    %% Restart peer2 off of peer1
    ar_test_node:start_peer(peer2, B0),
    ar_test_node:remote_call(peer2, ar_test_node, connect_to_peer, [peer1]),
    ar_test_node:wait_until_height(peer2, 4),

    assert_start_from(peer1, peer2, 4),

    %% Mine a block on peer2
    ar_test_node:mine(peer2),
    ar_test_node:wait_until_height(peer2, 5),
    ar_test_node:wait_until_height(peer1, 5),

    assert_start_from(peer2, peer1, 5),

    %% Have peer1 start_from_block one last time
    Peer1BI = get_block_index(peer1),
    restart_from_block(peer1, get_block_hash(4, Peer1BI)),
    assert_start_from(peer2, peer1, 4),
    ok.


restart_from_block(Peer, BH) ->
    {ok, Config} = ar_test_node:get_config(Peer),
    ok = ar_test_node:set_config(Peer, Config#config{
        start_from_latest_state = false,
        start_from_block = BH }),
    ar_test_node:restart(Peer),
    ar_test_node:remote_call(Peer, ar_test_node, wait_until_syncs_genesis_data, []).

assert_start_from(ExpectedPeer, Peer, Height) ->
    ?LOG_ERROR([{event, assert_start_from}, {expected_peer, ExpectedPeer}, {peer, Peer}, {height, Height}]),
    BI = get_block_index(Peer),
    StartFrom = get_block_hash(Height, BI),
    StartMinus1 = get_block_hash(Height-1, BI),

    assert_block_index(Peer, Height, BI),
    assert_reward_history(ExpectedPeer, Peer, StartFrom),
    assert_reward_history(ExpectedPeer, Peer, StartMinus1).

assert_block_index(Peer, Height, ExpectedBI) ->
    BI = get_block_index(Peer),
    BITail = lists:nthtail(length(BI)-Height-1, BI),
    ExpectedBITail = lists:nthtail(length(ExpectedBI)-Height-1, ExpectedBI),
    ?assertEqual(ExpectedBITail, BITail,
        io:format("Block Index mismatch for peer ~s", [Peer])).

assert_reward_history(ExpectedPeer, Peer, H) ->
    RewardHistory = get_reward_history(Peer, H),
    {B, _} = ar_test_node:remote_call(ExpectedPeer, ar_block_cache, get_block_and_status, [block_cache, H]),
    ExpectedRewardHistory = B#block.reward_history,

    ?assertEqual(ExpectedRewardHistory, RewardHistory).

get_block_hash(Height, BI) ->
    {H, _, _} = lists:nth(length(BI) - Height, BI),
    H.

get_block_index(Peer) ->
    ar_test_node:remote_call(Peer, ar_node, get_blocks, []).

get_reward_history(Peer, H) ->
    PeerIP = ar_test_node:peer_ip(Peer),
    case ar_http:req(#{
        peer => PeerIP,
        method => get,
        path => "/reward_history/" ++ binary_to_list(ar_util:encode(H)),
        timeout => 30000
    }) of
        {ok, {{<<"200">>, _}, _, Body, _, _}} ->
            case ar_serialize:binary_to_reward_history(Body) of
                {ok, RewardHistory} ->
                    RewardHistory;
                {error, Error} ->
                    Error
            end;
        Reply ->
            Reply
    end.
