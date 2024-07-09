-module(ar_info_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

recent_blocks_test_() ->
	[
		{timeout, 120, fun test_recent_blocks_post/0},
		{timeout, 120, fun test_recent_blocks_announcement/0}
	].

% recent_forks_test_() ->
% 	[
% 		{timeout, 120, fun test_recent_forks/0}
% 	].

%% -------------------------------------------------------------------------------------------
%% Recent blocks tests
%% -------------------------------------------------------------------------------------------
test_recent_blocks_post() ->
	test_recent_blocks(post).

test_recent_blocks_announcement() ->
	test_recent_blocks(announcement).

test_recent_blocks(Type) ->
	[B0] = ar_weave:init([], 0), %% Set difficulty to 0 to speed up tests
	ar_test_node:start_peer(peer1, B0),
	GenesisBlock = [#{
		<<"id">> => ar_util:encode(B0#block.indep_hash),
		<<"received">> => <<"pending">>
	}],
	?assertEqual(GenesisBlock, get_recent(ar_test_node:peer_ip(peer1), blocks)),

	TargetHeight = ?CHECKPOINT_DEPTH+2,
	PeerBI = lists:foldl(
		fun(Height, _Acc) ->
			ar_test_node:mine(peer1),
			ar_test_node:wait_until_height(peer1, Height)
		end,
		ok,
		lists:seq(1, TargetHeight)
	),
	%% Peer1 recent has no timestamps since it hasn't received any of its own blocks
	%% gossipped back
	?assertEqual(expected_blocks(peer1, PeerBI, true), 
		get_recent(ar_test_node:peer_ip(peer1), blocks)),

	%% Share blocks to peer1
	lists:foreach(
		fun({H, _WeaveSize, _TXRoot}) ->
			B = ar_test_node:remote_call(peer1, ar_block_cache, get, [block_cache, H]),
			case Type of
				post ->
					ar_test_node:send_new_block(ar_test_node:peer_ip(peer1), B);
				announcement ->
					Announcement = #block_announcement{ indep_hash = H,
						previous_block = B#block.previous_block,
						recall_byte = B#block.recall_byte,
						recall_byte2 = B#block.recall_byte2,
						solution_hash = B#block.hash,
						tx_prefixes = [] },
					ar_http_iface_client:send_block_announcement(
						ar_test_node:peer_ip(peer1), Announcement)
			end
		end,
		lists:sublist(PeerBI, TargetHeight)
	),

	%% Peer1 recent should now have timestamps, but also black out the most recent
	%% ones.
	?assertEqual(expected_blocks(peer1, PeerBI), 
		get_recent(ar_test_node:peer_ip(peer1), blocks)).

expected_blocks(Node, BI) ->
	expected_blocks(Node, BI, false).
expected_blocks(Node, BI, ForcePending) ->
	lists:foldl(
		fun({H, _WeaveSize, _TXRoot}, Acc) ->
			B = ar_test_node:remote_call(Node, ar_block_cache, get, [block_cache, H]),
			Timestamp = case ForcePending of
				true -> <<"pending">>;
				false ->
					case length(Acc) > (?CHECKPOINT_DEPTH - ?RECENT_BLOCKS_WITHOUT_TIMESTAMP - 1) of
						true -> <<"pending">>;
						false -> ar_util:timestamp_to_seconds(B#block.receive_timestamp)
					end
				end,
			[#{
				<<"id">> => ar_util:encode(H),
				<<"received">> => Timestamp
			} | Acc]
		end,
		[],
		lists:reverse(lists:sublist(BI, ?CHECKPOINT_DEPTH))
	).

%% -------------------------------------------------------------------------------------------
%% Recent forks tests
%% -------------------------------------------------------------------------------------------
test_recent_forks() ->
	StartTime = os:system_time(seconds),
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

	?LOG_ERROR(get_recent(ar_test_node:peer_ip(peer1), forks)).

get_recent(Peer, Type) ->
	case get_recent(Peer) of
		info_unavailable -> info_unavailable;
		Info ->
			maps:get(atom_to_binary(Type), Info)
	end.
get_recent(Peer) ->
	case
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/recent",
			connect_timeout => 1000,
			timeout => 2 * 1000
		})
	of
		{ok, {{<<"200">>, _}, _, JSON, _, _}} -> 
			case ar_serialize:json_decode(JSON, [return_maps]) of
				{ok, JsonMap} ->
					JsonMap;
				{error, _} ->
					info_unavailable
			end;
		_ -> info_unavailable
	end.