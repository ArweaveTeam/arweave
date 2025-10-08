-module(ar_info_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_chain_stats.hrl").

recent_blocks_test_() ->
	[
		{timeout, 300, fun test_recent_blocks_post/0},
		{timeout, 300, fun test_recent_blocks_announcement/0}
	].

recent_forks_test_() ->
	[
		{timeout, 300, fun test_get_recent_forks/0},
		{timeout, 300, fun test_recent_forks/0}
	].

%% -------------------------------------------------------------------------------------------
%% Recent blocks tests
%% -------------------------------------------------------------------------------------------
test_recent_blocks_post() ->
	test_recent_blocks(post).

test_recent_blocks_announcement() ->
	test_recent_blocks(announcement).

test_recent_blocks(Type) ->
	[B0] = ar_weave:init(),
	ar_test_node:start_peer(peer1, B0),
	GenesisBlock = [#{
		<<"id">> => ar_util:encode(B0#block.indep_hash),
		<<"received">> => <<"pending">>,
		<<"height">> => 0
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
			timer:sleep(1000),
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
		%% Reverse the list so that the peer receives the blocks in the same order they
		%% were mined.
		lists:reverse(lists:sublist(PeerBI, TargetHeight))
	),

	%% Peer1 recent should now have timestamps, but also black out the most recent
	%% ones.
	?assertEqual(expected_blocks(peer1, PeerBI), 
		get_recent(ar_test_node:peer_ip(peer1), blocks)).
		
expected_blocks(Node, BI) ->
	expected_blocks(Node, BI, false).
expected_blocks(Node, BI, ForcePending) ->
	%% There are a few list reversals that happen here:
	%% 1. BI has the blocks in reverse chronological order (latest block first)
	%% 2. [Element | Acc] reverses the list into chronological order (latest block last)
	%% 3. The final lists:reverse puts the list back into reverse chronological order
	%%	(latest block first)
	Blocks = lists:foldl(
		fun({H, _WeaveSize, _TXRoot}, Acc) ->
			B = ar_test_node:remote_call(Node, ar_block_cache, get, [block_cache, H]),
			Timestamp = case ForcePending of
				true -> <<"pending">>;
				false ->
					case length(Acc) < ?RECENT_BLOCKS_WITHOUT_TIMESTAMP of
						true -> <<"pending">>;
						false -> ar_util:timestamp_to_seconds(B#block.receive_timestamp)
					end
				end,
			[#{
				<<"id">> => ar_util:encode(H),
				<<"received">> => Timestamp,
				<<"height">> => B#block.height
			} | Acc]
		end,
		[],
		lists:sublist(BI, ?CHECKPOINT_DEPTH)
	),
	lists:reverse(Blocks).

%% -------------------------------------------------------------------------------------------
%% Recent forks tests
%% -------------------------------------------------------------------------------------------
test_get_recent_forks() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),

	ForkRootB1 = #block{ indep_hash = <<"1">>, height = 1 },
	ForkRootB2= #block{ indep_hash = <<"2">>, height = 2 },
	ForkRootB3= #block{ indep_hash = <<"3">>, height = 3 },

	Orphans1 = [<<"a">>],
	timer:sleep(5),
	ar_chain_stats:log_fork(Orphans1, ForkRootB1),
	ExpectedFork1 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans1)),
		height = 2,
		block_ids = Orphans1
	},
	assert_forks_json_equal([ExpectedFork1]),

	Orphans2 = [<<"b">>, <<"c">>],
	timer:sleep(5),
	ar_chain_stats:log_fork(Orphans2, ForkRootB1),
	ExpectedFork2 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans2)),
		height = 2,
		block_ids = Orphans2
	},
	assert_forks_json_equal([ExpectedFork2, ExpectedFork1]),

	Orphans3 = [<<"b">>, <<"c">>, <<"d">>],
	timer:sleep(5),
	ar_chain_stats:log_fork(Orphans3, ForkRootB1),
	ExpectedFork3 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans3)),
		height = 2,
		block_ids = Orphans3
	},
	assert_forks_json_equal([ExpectedFork3, ExpectedFork2, ExpectedFork1]),

	Orphans4 = [<<"e">>, <<"f">>, <<"g">>],
	timer:sleep(5),
	ar_chain_stats:log_fork(Orphans4, ForkRootB2),
	ExpectedFork4 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans4)),
		height = 3,
		block_ids = Orphans4
	},
	assert_forks_json_equal([ExpectedFork4, ExpectedFork3, ExpectedFork2, ExpectedFork1]),

	%% Same fork seen again - not sure this is possible, but since we're just tracking
	%% forks based on when they occur, it should be handled.
	timer:sleep(5),
	ar_chain_stats:log_fork(Orphans3, ForkRootB1),
	assert_forks_json_equal(
		[ExpectedFork3, ExpectedFork4, ExpectedFork3, ExpectedFork2, ExpectedFork1]),

	%% If the fork is empty, ignore it.
	timer:sleep(5),
	ar_chain_stats:log_fork([], ForkRootB2),
	assert_forks_json_equal(
		[ExpectedFork3, ExpectedFork4, ExpectedFork3, ExpectedFork2, ExpectedFork1]),

	%% Confirm that limiting the number of forks returned is handled correctly (e.g.
	%% the oldest fork is not returned)
	Orphans5 = [<<"h">>, <<"i">>, <<"j">>],
	timer:sleep(5),
	ar_chain_stats:log_fork(Orphans5, ForkRootB3),
	ExpectedFork5 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans5)),
		height = 4,
		block_ids = Orphans5
	},
	assert_forks_json_equal(
		[ExpectedFork5, ExpectedFork3, ExpectedFork4, ExpectedFork3, ExpectedFork2]),

	ok.

test_recent_forks() ->
	[B0] = ar_weave:init(),
	ar_test_node:start(B0),
	ar_test_node:start_peer(peer1, B0),
	ar_test_node:start_peer(peer2, B0),
	ar_test_node:connect_to_peer(peer1),
	ar_test_node:connect_to_peer(peer2),
	ar_test_node:connect_peers(peer1, peer2),
   
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
	ar_test_node:disconnect_peers(peer1, peer2),

	ar_test_node:mine(peer1),
	BI1 = ar_test_node:wait_until_height(peer1, 4),
	Orphans1 = [ID || {ID, _, _} <- lists:sublist(BI1, 1)],
	Fork1 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans1)),
		height = 4,
		block_ids = Orphans1
	},
		
	ar_test_node:mine(peer2),
	ar_test_node:wait_until_height(peer2, 4),
	ar_test_node:mine(peer2),
	ar_test_node:wait_until_height(peer2, 5),
	ar_test_node:mine(peer2),
	ar_test_node:wait_until_height(peer2, 6),

	%% Reconnect the peers. This will orphan peer1's block
	ar_test_node:connect_to_peer(peer2),
	ar_test_node:wait_until_height(main, 6),

	ar_test_node:connect_to_peer(peer1),
	ar_test_node:wait_until_height(peer1, 6),

	ar_test_node:connect_peers(peer1, peer2),
	ar_test_node:wait_until_height(peer2, 6),

	%% Disconnect peers, and have peer1 mine 2 block2, and peer2 mine 3
	ar_test_node:disconnect_from(peer1),
	ar_test_node:disconnect_from(peer2),
	ar_test_node:disconnect_peers(peer1, peer2),

	ar_test_node:mine(peer1),
	ar_test_node:wait_until_height(peer1, 7),
	ar_test_node:mine(peer1),
	BI2 = ar_test_node:wait_until_height(peer1, 8),
	Orphans2 = [ID || {ID, _, _} <- lists:reverse(lists:sublist(BI2, 2))],
	Fork2 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans2)),
		height = 7,
		block_ids = Orphans2
	},

	ar_test_node:mine(peer2),
	ar_test_node:wait_until_height(peer2, 7),
	ar_test_node:mine(peer2),
	ar_test_node:wait_until_height(peer2, 8),
	ar_test_node:mine(peer2),
	ar_test_node:wait_until_height(peer2, 9),

	%% Reconnect the peers. This will create a second fork as peer1's blocks are orphaned
	ar_test_node:connect_to_peer(peer2),
	ar_test_node:wait_until_height(main, 9),

	ar_test_node:connect_to_peer(peer1),
	ar_test_node:wait_until_height(peer1, 9),

	ar_test_node:connect_peers(peer1, peer2),
	ar_test_node:wait_until_height(peer2, 9),


	ar_test_node:disconnect_from(peer1),
	ar_test_node:disconnect_from(peer2),
	ar_test_node:disconnect_peers(peer1, peer2),

	assert_forks_json_equal([Fork2, Fork1], get_recent(ar_test_node:peer_ip(peer1), forks)),
	ok.

assert_forks_json_equal(ExpectedForks) ->
	assert_forks_json_equal(ExpectedForks, get_recent(ar_test_node:peer_ip(main), forks)).

assert_forks_json_equal(ExpectedForks, ActualForks) ->
	ExpectedForksStripped = [ 
		#{
			<<"id">> => ar_util:encode(Fork#fork.id),
			<<"height">> => Fork#fork.height,
			<<"blocks">> => [ ar_util:encode(BlockID) || BlockID <- Fork#fork.block_ids ]
		} 
		|| Fork <- ExpectedForks],
	ActualForksStripped = [ maps:remove(<<"timestamp">>, Fork) || Fork <- ActualForks ],
	?assertEqual(ExpectedForksStripped, ActualForksStripped).
	
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