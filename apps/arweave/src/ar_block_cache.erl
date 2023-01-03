%%% @doc The module maintains a DAG of blocks that have passed the PoW validation, in ETS.
%%% NOTE It is not safe to call functions which modify the state from different processes.
-module(ar_block_cache).

-export([new/2, initialize_from_list/2, add/2, mark_nonce_limiter_validated/2,
		mark_nonce_limiter_validation_scheduled/2, add_validated/2,
		mark_tip/2, get/2, get_earliest_not_validated_from_longest_chain/1,
		get_longest_chain_block_txs_pairs/1,
		get_block_and_status/2, remove/2, prune/2, get_by_solution_hash/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% The expiration time in seconds for every "alternative" block (a block with non-unique
%% solution).
-define(ALTERNATIVE_BLOCK_EXPIRATION_TIME_SECONDS, 5).

%%%===================================================================
%%% Public API.
%%%===================================================================

%% @doc Create a cache, initialize it with the given block. The block is marked as on-chain
%% and as a tip block.
new(Tab, B) ->
	#block{ indep_hash = H, hash = SolutionH, cumulative_diff = CDiff, height = Height } = B,
	ets:delete_all_objects(Tab),
	ar_ignore_registry:add(H),
	ets:insert(Tab, [
		{max_cdiff, {CDiff, H}},
		{links, gb_sets:from_list([{Height, H}])},
		{{solution, SolutionH}, sets:from_list([H])},
		{tip, H},
		{{block, H}, {B, on_chain, erlang:timestamp(), sets:new()}}
	]).

%% @doc Initialize a cache from the given list of validated blocks. Mark the latest
%% block as the tip block. The given blocks must be sorted from newest to oldest.
initialize_from_list(Tab, [B]) ->
	new(Tab, B);
initialize_from_list(Tab, [#block{ indep_hash = H } = B | Blocks]) ->
	initialize_from_list(Tab, Blocks),
	add_validated(Tab, B),
	mark_tip(Tab, H).

%% @doc Add a block to the cache. The block is marked as not validated yet.
%% If the block is already present in the cache and has not been yet validated, it is
%% overwritten. If the block is validated, we do nothing and issue a warning.
add(Tab,
		#block{
			indep_hash = H,
			hash = SolutionH,
			previous_block = PrevH,
			cumulative_diff = CDiff,
			height = Height
		} = B) ->
	Status = case B#block.height >= ar_fork:height_2_6() of true ->
			{not_validated, awaiting_nonce_limiter_validation};
			false -> {not_validated, awaiting_validation} end,
	case ets:lookup(Tab, {block, H}) of
		[] ->
			ar_ignore_registry:add(H),
			SolutionSet =
				case ets:lookup(Tab, {solution, SolutionH}) of
					[] ->
						sets:new();
					[{_, SolutionSet2}] ->
						SolutionSet2
				end,
			Remaining = remove_expired_alternative_blocks(Tab, sets:to_list(SolutionSet)),
			SolutionSet3 = sets:from_list([H | Remaining]),
			[{_, Tip}] = ets:lookup(Tab, tip),
			[{_, Set}] = ets:lookup(Tab, links),
			[{_, C = {MaxCDiff, _H}}] = ets:lookup(Tab, max_cdiff),
			[{_, {PrevB, PrevStatus, PrevTimestamp,
					Children}}] = ets:lookup(Tab, {block, PrevH}),
			C2 = case CDiff > MaxCDiff of true -> {CDiff, H}; false -> C end,
			Set2 = gb_sets:insert({Height, H}, Set),
			ets:insert(Tab, [
				{max_cdiff, C2},
				{links, Set2},
				{{solution, SolutionH}, SolutionSet3},
				{tip, Tip},
				{{block, H}, {B, Status, erlang:timestamp(), sets:new()}},
				{{block, PrevH},
						{PrevB, PrevStatus, PrevTimestamp, sets:add_element(H, Children)}}
			]);
		[{_, {_B, {not_validated, _} = CurrentStatus, CurrentTimestamp, Children}}] ->
			ets:insert(Tab, {{block, H}, {B, CurrentStatus, CurrentTimestamp, Children}});
		_ ->
			?LOG_WARNING([{event, attempt_to_update_already_validated_cached_block},
					{h, ar_util:encode(H)}, {height, Height},
					{previous_block, ar_util:encode(PrevH)}]),
			ok
	end.

remove_expired_alternative_blocks(_Tab, []) ->
	[];
remove_expired_alternative_blocks(Tab, [H | Hs]) ->
	[{_, {_B, Status, Timestamp, Children}}] = ets:lookup(Tab, {block, H}),
	case Status of
		on_chain ->
			[H | remove_expired_alternative_blocks(Tab, Hs)];
		_ ->
			LifetimeSeconds = get_alternative_block_lifetime(Tab, Children),
			{MegaSecs, Secs, MicroSecs} = Timestamp,
			ExpirationTimestamp = {MegaSecs, Secs + LifetimeSeconds, MicroSecs},
			case timer:now_diff(erlang:timestamp(), ExpirationTimestamp) >= 0 of
				true ->
					remove(Tab, H),
					remove_expired_alternative_blocks(Tab, Hs);
				false ->
					[H | remove_expired_alternative_blocks(Tab, Hs)]
			end
	end.

get_alternative_block_lifetime(Tab, Children) ->
	ForkLen = get_fork_length(Tab, sets:to_list(Children)),
	(?ALTERNATIVE_BLOCK_EXPIRATION_TIME_SECONDS) * ForkLen.

get_fork_length(Tab, Branches) when is_list(Branches) ->
	1 + lists:max([0 | [get_fork_length(Tab, Branch) || Branch <- Branches]]);
get_fork_length(Tab, Branch) ->
	[{_, {_B, _Status, _Timestamp, Children}}] = ets:lookup(Tab, {block, Branch}),
	case sets:size(Children) == 0 of
		true ->
			1;
		false ->
			1 + get_fork_length(Tab, sets:to_list(Children))
	end.

%% @doc Update the status of the given block to 'nonce_limiter_validated'.
%% Do nothing if the block is not found in cache or if its status is
%% not 'nonce_limiter_validation_scheduled'.
mark_nonce_limiter_validated(Tab, H) ->
	case ets:lookup(Tab, {block, H}) of
		[{_, {B, {not_validated, nonce_limiter_validation_scheduled}, Timestamp, Children}}] ->
			ets:insert(Tab, {{block, H}, {B,
					{not_validated, nonce_limiter_validated}, Timestamp, Children}});
		_ ->
			ok
	end.

%% @doc Update the status of the given block to 'nonce_limiter_validation_scheduled'.
%% Do nothing if the block is not found in cache or if its status is
%% not 'awaiting_nonce_limiter_validation'.
mark_nonce_limiter_validation_scheduled(Tab, H) ->
	case ets:lookup(Tab, {block, H}) of
		[{_, {B, {not_validated, awaiting_nonce_limiter_validation}, Timestamp, Children}}] ->
			ets:insert(Tab, {{block, H}, {B,
					{not_validated, nonce_limiter_validation_scheduled}, Timestamp,
					Children}});
		_ ->
			ok
	end.

%% @doc Add a validated block to the cache. If the block is already in the cache, it
%% is overwritten. However, the function assumes the height, hash, previous hash, and
%% the cumulative difficulty do not change.
%% Raises previous_block_not_found if the previous block is not in the cache.
%% Raises previous_block_not_validated if the previous block is not validated.
add_validated(Tab, B) ->
	#block{ indep_hash = H, hash = SolutionH, previous_block = PrevH, height = Height } = B,
	case ets:lookup(Tab, {block, PrevH}) of
		[] ->
			error(previous_block_not_found);
		[{_, {_PrevB, {not_validated, _}, _Timestamp, _Children}}] ->
			error(previous_block_not_validated);
		[{_, {PrevB, PrevStatus, PrevTimestamp, PrevChildren}}] ->
			case ets:lookup(Tab, {block, H}) of
				[] ->
					CDiff = B#block.cumulative_diff,
					SolutionSet =
						case ets:lookup(Tab, {solution, SolutionH}) of
							[] ->
								sets:new();
							[{_, SolutionSet2}] ->
								SolutionSet2
						end,
					Remaining = remove_expired_alternative_blocks(Tab,
							sets:to_list(SolutionSet)),
					SolutionSet3 = sets:from_list([H | Remaining]),
					[{_, Set}] = ets:lookup(Tab, links),
					[{_, C = {MaxCDiff, _H}}] = ets:lookup(Tab, max_cdiff),
					ets:insert(Tab, [
						{{block, PrevH}, {PrevB, PrevStatus, PrevTimestamp,
								sets:add_element(H, PrevChildren)}},
						{{block, H}, {B, validated, erlang:timestamp(), sets:new()}},
						{max_cdiff,
								case CDiff > MaxCDiff of true -> {CDiff, H}; false -> C end},
						{links, gb_sets:insert({Height, H}, Set)},
						{{solution, SolutionH}, SolutionSet3}
					]);
				[{_, {_B, on_chain, Timestamp, Children}}] ->
					ets:insert(Tab, [
						{{block, PrevH}, {PrevB, PrevStatus, PrevTimestamp,
								sets:add_element(H, PrevChildren)}},
						{{block, H}, {B, on_chain, Timestamp, Children}}
					]);
				[{_, {_B, _Status, Timestamp, Children}}] ->
					ets:insert(Tab, [
						{{block, PrevH}, {PrevB, PrevStatus, PrevTimestamp,
								sets:add_element(H, PrevChildren)}},
						{{block, H}, {B, validated, Timestamp, Children}}
					])
			end
	end.

%% @doc Get the block from cache. Returns not_found if the block is not in cache.
get(Tab, H) ->
	case ets:lookup(Tab, {block, H}) of
		[] ->
			not_found;
		[{_, {B, _Status, _Timestamp, _Children}}] ->
			B
	end.

%% @doc Get a {block, previous blocks} pair for the earliest block from
%% the longest chain, which has not been validated yet. The previous blocks are
%% sorted from newest to oldest. The last one is a block from the current fork.
get_earliest_not_validated_from_longest_chain(Tab) ->
	[{_, Tip}] = ets:lookup(Tab, tip),
	[{_, {CDiff, H}}] = ets:lookup(Tab, max_cdiff),
	[{_, {#block{ cumulative_diff = TipCDiff }, _, _, _}}] = ets:lookup(Tab, {block, Tip}),
	case TipCDiff >= CDiff of
		true ->
			not_found;
		false ->
			[{_, {B, Status, Timestamp, _Children}}] = ets:lookup(Tab, {block, H}),
			case Status of
				{not_validated, _} ->
					get_earliest_not_validated(Tab, B, Status, Timestamp);
				_ ->
					not_found
			end
	end.

%% @doc Return the list of {BH, TXIDs} pairs corresponding to the top up to the
%% ?STORE_BLOCKS_BEHIND_CURRENT blocks of the longest chain and the number of blocks
%% in this list that are not on chain yet.
get_longest_chain_block_txs_pairs(Tab) ->
	[{_, {_CDiff, H}}] = ets:lookup(Tab, max_cdiff),
	get_longest_chain_block_txs_pairs(Tab, H, ?STORE_BLOCKS_BEHIND_CURRENT, none, none, [], 0).

get_longest_chain_block_txs_pairs(_Tab, _H, 0, _PrevStatus, _PrevH, Pairs, NotOnChainCount) ->
	{lists:reverse(Pairs), NotOnChainCount};
get_longest_chain_block_txs_pairs(Tab, H, N, PrevStatus, PrevH, Pairs, NotOnChainCount) ->
	case ets:lookup(Tab, {block, H}) of
		[{_, {B, Status, _Timestamp, _Children}}] ->
			case PrevStatus == on_chain andalso Status /= on_chain of
				true ->
					%% A reorg should have happened in the meantime - an unlikely
					%% event, retry.
					get_longest_chain_block_txs_pairs(Tab);
				false ->
					NotOnChainCount2 =
						case Status of
							on_chain ->
								NotOnChainCount;
							_ ->
								NotOnChainCount + 1
						end,
					Pairs2 = [{B#block.indep_hash, [tx_id(TX) || TX <- B#block.txs]} | Pairs],
					get_longest_chain_block_txs_pairs(Tab, B#block.previous_block, N - 1,
							Status, H, Pairs2, NotOnChainCount2)
			end;
		[] ->
			case PrevStatus of
				on_chain ->
					case ets:lookup(Tab, {block, PrevH}) of
						[] ->
							%% The block has been pruned -
							%% an unlikely race condition so we retry.
							get_longest_chain_block_txs_pairs(Tab);
						[_] ->
							%% Pairs already contains the deepest block of the cache.
							{lists:reverse(Pairs), NotOnChainCount}
					end;
				_ ->
					%% The block has been invalidated -
					%% an unlikely race condition so we retry.
					get_longest_chain_block_txs_pairs(Tab)
			end
	end.

tx_id(#tx{ id = ID }) ->
	ID;
tx_id(TXID) ->
	TXID.

%% @doc Get the block and its status from cache.
%% Returns not_found if the block is not in cache.
get_block_and_status(Tab, H) ->
	case ets:lookup(Tab, {block, H}) of
		[] ->
			not_found;
		[{_, {B, Status, _Timestamp, _Children}}] ->
			{B, Status}
	end.

%% @doc Mark the given block as the tip block. Mark the previous blocks as on-chain.
%% Mark the on-chain blocks from other forks as validated. Raises invalid_tip if
%% one of the preceeding blocks is not validated. Raises not_found if the block
%% is not found.
mark_tip(Tab, H) ->
	case ets:lookup(Tab, {block, H}) of
		[{_, {B, _Status, Timestamp, Children}}] ->
			ets:insert(Tab, [
				{tip, H},
				{{block, H}, {B, on_chain, Timestamp, Children}} |
				mark_on_chain(Tab, B)
			]);
		[] ->
			error(not_found)
	end.

%% @doc Remove the block and all the blocks on top from the cache.
remove(Tab, H) ->
	case ets:lookup(Tab, {block, H}) of
		[] ->
			ok;
		[{_, {#block{ previous_block = PrevH }, _Status, _Timestamp, _Children}}] ->
			[{_, C = {_, H2}}] = ets:lookup(Tab, max_cdiff),
			[{_, {PrevB, PrevBStatus, PrevTimestamp, PrevBChildren}}] = ets:lookup(Tab,
					{block, PrevH}),
			remove2(Tab, H),
			ets:insert(Tab, [
				{max_cdiff, case ets:lookup(Tab, {block, H2}) of
								[] ->
									find_max_cdiff(Tab);
								_ ->
									C
							end},
				{{block, PrevH}, {PrevB, PrevBStatus, PrevTimestamp,
						sets:del_element(H, PrevBChildren)}}
			]),
			ar_ignore_registry:remove(H),
			ok
	end.

%% @doc Prune the cache. Keep the blocks no deeper than the given prune depth from the tip.
prune(Tab, Depth) ->
	[{_, Tip}] = ets:lookup(Tab, tip),
	[{_, {#block{ height = Height }, _Status, _Timestamp, _Children}}] = ets:lookup(Tab,
			{block, Tip}),
	prune(Tab, Depth, Height).

%% @doc Return a block from the block cache with the given solution hash.
%% Return not_found if one is not found. If there are several blocks with the
%% same solution hash, one of them is returned, the choice is not defined.
get_by_solution_hash(Tab, SolutionH) ->
	case ets:lookup(Tab, {solution, SolutionH}) of
		[] ->
			not_found;
		[{_, Set}] ->
			H = hd(sets:to_list(Set)),
			case get(Tab, H) of
				not_found ->
					%% An extremely unlikely race condition - simply retry.
					get_by_solution_hash(Tab, SolutionH);
				B ->
					B
			end
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_earliest_not_validated(Tab, #block{ previous_block = PrevH } = B, Status, Timestamp) ->
	[{_, {PrevB, PrevStatus, PrevTimestamp, _Children}}] = ets:lookup(Tab, {block, PrevH}),
	case PrevStatus of
		{not_validated, _} ->
			get_earliest_not_validated(Tab, PrevB, PrevStatus, PrevTimestamp);
		_ ->
			{B, get_fork_blocks(Tab, B), {Status, Timestamp}}
	end.

get_fork_blocks(Tab, #block{ previous_block = PrevH }) ->
	[{_, {PrevB, Status, _Timestamp, _Children}}] = ets:lookup(Tab, {block, PrevH}),
	case Status of
		on_chain ->
			[PrevB];
		_ ->
			[PrevB | get_fork_blocks(Tab, PrevB)]
	end.

mark_on_chain(Tab, #block{ previous_block = PrevH, indep_hash = H }) ->
	case ets:lookup(Tab, {block, PrevH}) of
		[{_, {_PrevB, {not_validated, _}, _Timestamp, _Children}}] ->
			error(invalid_tip);
		[{_, {_PrevB, on_chain, _Timestamp, Children}}] ->
			%% Mark the blocks from the previous main fork as validated, not on-chain.
			mark_off_chain(Tab, sets:del_element(H, Children));
		[{_, {PrevB, validated, Timestamp, Children}}] ->
			[{{block, PrevH}, {PrevB, on_chain, Timestamp, Children}}
					| mark_on_chain(Tab, PrevB)]
	end.

mark_off_chain(Tab, Set) ->
	sets:fold(
		fun(H, Acc) ->
			case ets:lookup(Tab, {block, H}) of
				[{_, {B, on_chain, Timestamp, Children}}] ->
					[{{block, H}, {B, validated, Timestamp, Children}}
							| mark_off_chain(Tab, Children)];
				_ ->
					Acc
			end
		end,
		[],
		Set
	).

remove2(Tab, H) ->
	[{_, Set}] = ets:lookup(Tab, links),
	case ets:lookup(Tab, {block, H}) of
		not_found ->
			ok;
		[{_, {#block{ hash = SolutionH, txs = TXs, height = Height }, _Status, _Timestamp,
				Children}}] ->
			ets:delete(Tab, {block, H}),
			ar_ignore_registry:remove(H),
			remove_tx_prefixes(TXs),
			remove_solution(Tab, H, SolutionH),
			ets:insert(Tab, {links, gb_sets:del_element({Height, H}, Set)}),
			sets:fold(
				fun(Child, ok) ->
					remove2(Tab, Child)
				end,
				ok,
				Children
			)
	end.

remove_tx_prefixes([]) ->
	ok;
remove_tx_prefixes([#tx{ id = TXID } | TXs]) ->
	ets:delete_object(tx_prefixes, {ar_node_worker:tx_id_prefix(TXID), TXID}),
	remove_tx_prefixes(TXs);
remove_tx_prefixes([TXID | TXs]) ->
	ets:delete_object(tx_prefixes, {ar_node_worker:tx_id_prefix(TXID), TXID}),
	remove_tx_prefixes(TXs).

remove_solution(Tab, H, SolutionH) ->
	[{_, SolutionSet}] = ets:lookup(Tab, {solution, SolutionH}),
	case sets:size(SolutionSet) of
		1 ->
			ets:delete(Tab, {solution, SolutionH});
		_ ->
			SolutionSet2 = sets:del_element(H, SolutionSet),
			ets:insert(Tab, {{solution, SolutionH}, SolutionSet2})
	end.

find_max_cdiff(Tab) ->
	[{_, Set}] = ets:lookup(Tab, links),
	gb_sets:fold(
		fun ({_Height, H}, not_set) ->
				[{_, {#block{ cumulative_diff = CDiff }, _, _, _}}] = ets:lookup(Tab,
						{block, H}),
				{CDiff, H};
			({_Height, H}, {MaxCDiff, _CH} = Acc) ->
				[{_, {#block{ cumulative_diff = CDiff }, _, _, _}}] = ets:lookup(Tab,
						{block, H}),
				case CDiff > MaxCDiff of
					true ->
						{CDiff, H};
					false ->
						Acc
				end
		end,
		not_set,
		Set
	).

prune(Tab, Depth, TipHeight) ->
	[{_, Set}] = ets:lookup(Tab, links),
	case gb_sets:is_empty(Set) of
		true ->
			ok;
		false ->
			{{Height, H}, Set2} = gb_sets:take_smallest(Set),
			case Height >= TipHeight - Depth of
				true ->
					ok;
				false ->
					ets:insert(Tab, {links, Set2}),
					%% The lowest block must be on-chain by construction.
					[{_, {B, on_chain, _Timestamp, Children}}] = ets:lookup(Tab, {block, H}),
					#block{ hash = SolutionH, txs = TXs } = B,
					sets:fold(
						fun(Child, ok) ->
							[{_, {_, Status, _, _}}] = ets:lookup(Tab, {block, Child}),
							case Status of
								on_chain ->
									ok;
								_ ->
									remove(Tab, Child)
							end
						end,
						ok,
						Children
					),
					remove_solution(Tab, H, SolutionH),
					ets:delete(Tab, {block, H}),
					remove_tx_prefixes(TXs),
					ar_ignore_registry:remove(H),
					prune(Tab, Depth, TipHeight)
			end
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

block_cache_pre_fork_2_6_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> infinity end}],
			fun() -> test_block_cache(fork_2_5) end).

block_cache_test_() ->
	ar_test_node:test_with_mocked_functions([{ar_fork, height_2_6, fun() -> 0 end}],
			fun() -> test_block_cache(fork_2_6) end).

test_block_cache(Fork) ->
	?debugFmt("Test block cache: fork ~p.", [Fork]),
	ets:new(bcache_test, [set, named_table]),
	new(bcache_test, B1 = random_block(0)),
	?assertEqual(not_found, get(bcache_test, crypto:strong_rand_bytes(48))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, crypto:strong_rand_bytes(32))),
	?assertEqual(B1, get(bcache_test, block_id(B1))),
	?assertEqual(B1, get_by_solution_hash(bcache_test, B1#block.hash)),
	add(bcache_test, B1#block{ txs = [<<>>] }),
	?assertEqual(B1#block{ txs = [<<>>] }, get(bcache_test, block_id(B1))),
	?assertEqual(B1#block{ txs = [<<>>] }, get_by_solution_hash(bcache_test, B1#block.hash)),
	add(bcache_test, B1),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	add(bcache_test, B2 = on_top(random_block(1), B1)),
	ExpectedStatus = case Fork of fork_2_6 -> awaiting_nonce_limiter_validation;
			_ -> awaiting_validation end,
	?assertMatch({B2, [B1], {{not_validated, ExpectedStatus}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	remove(bcache_test, block_id(B2)),
	?assertEqual(not_found, get(bcache_test, block_id(B2))),
	remove(bcache_test, block_id(B2)),
	?assertEqual(B1, get(bcache_test, block_id(B1))),
	add(bcache_test, B2),
	add(bcache_test, B1_2 = (on_top(random_block(2), B1))#block{ hash = B1#block.hash }),
	?assert(lists:member(get_by_solution_hash(bcache_test, B1#block.hash), [B1, B1_2])),
	mark_tip(bcache_test, block_id(B2)),
	?assertEqual(B1_2, get(bcache_test, block_id(B1_2))),
	remove(bcache_test, block_id(B1_2)),
	?assertEqual(not_found, get(bcache_test, block_id(B1_2))),
	?assertEqual(B1, get_by_solution_hash(bcache_test, B1#block.hash)),
	prune(bcache_test, 1),
	?assertEqual(B1, get(bcache_test, block_id(B1))),
	?assertEqual(B1, get_by_solution_hash(bcache_test, B1#block.hash)),
	prune(bcache_test, 0),
	?assertEqual(not_found, get(bcache_test, block_id(B1))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B1#block.hash)),
	prune(bcache_test, 0),
	?assertEqual(not_found, get(bcache_test, block_id(B1_2))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B1_2#block.hash)),
	new(bcache_test, B1),
	add(bcache_test, B1_2),
	add(bcache_test, B2),
	mark_tip(bcache_test, block_id(B2)),
	add(bcache_test, B2_2 = on_top(random_block(1), B2)),
	?assertMatch({B1_2, [B1], {{not_validated, ExpectedStatus}, _Timestamp}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	add(bcache_test, B2_3 = on_top(random_block(3), B2_2)),
	?assertMatch({B2_2, [B2], {{not_validated, ExpectedStatus}, _Timestamp}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	?assertException(error, invalid_tip, mark_tip(bcache_test, block_id(B2_3))),
	add_validated(bcache_test, B2_2),
	?assertEqual({B2_2, validated}, get_block_and_status(bcache_test, B2_2#block.indep_hash)),
	?assertMatch({B2_3, [B2_2, B2], {{not_validated, ExpectedStatus}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	B3 = on_top(random_block(4), B2),
	B3ID = block_id(B3),
	add(bcache_test, B3),
	add_validated(bcache_test, B3),
	mark_tip(bcache_test, B3ID),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	mark_tip(bcache_test, block_id(B2_2)),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	add(bcache_test, B4 = on_top(random_block(5), B3)),
	?assertMatch({B4, [B3, B2], {{not_validated, ExpectedStatus}, _Timestamp}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	prune(bcache_test, 1),
	?assertEqual(not_found, get(bcache_test, block_id(B1))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B1#block.hash)),
	mark_tip(bcache_test, block_id(B2_3)),
	prune(bcache_test, 1),
	?assertEqual(not_found, get(bcache_test, block_id(B2))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B2#block.hash)),
	prune(bcache_test, 1),
	?assertEqual(not_found, get(bcache_test, block_id(B3))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B3#block.hash)),
	prune(bcache_test, 1),
	?assertEqual(not_found, get(bcache_test, block_id(B4))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B4#block.hash)),
	prune(bcache_test, 1),
	?assertEqual(B2_2, get(bcache_test, block_id(B2_2))),
	?assertEqual(B2_2, get_by_solution_hash(bcache_test, B2_2#block.hash)),
	prune(bcache_test, 1),
	?assertEqual(B2_3, get(bcache_test, block_id(B2_3))),
	?assertEqual(B2_3, get_by_solution_hash(bcache_test, B2_3#block.hash)),
	remove(bcache_test, block_id(B3)),
	?assertEqual(not_found, get(bcache_test, block_id(B3))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B3#block.hash)),
	remove(bcache_test, block_id(B3)),
	?assertEqual(not_found, get(bcache_test, block_id(B4))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B4#block.hash)),
	new(bcache_test, B11 = random_block(0)),
	add(bcache_test, on_top(random_block(1), B11)),
	add_validated(bcache_test, B13 = on_top(random_block(1), B11)),
	mark_tip(bcache_test, block_id(B13)),
	%% Although the first block at height 1 was the one added in C12, B13 then
	%% became the tip so we should not reorganize.
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	add(bcache_test, B14 = on_top(random_block_after_repacking(2), B13)),
	case B14#block.height >= ar_fork:height_2_6() of
		true ->
			test_block_cache_after_fork_2_6(B13, B14);
		false ->
			ok
	end.

test_block_cache_after_fork_2_6(B13, B14) ->
	?assertMatch({B14, [B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	mark_nonce_limiter_validation_scheduled(bcache_test, crypto:strong_rand_bytes(48)),
	mark_nonce_limiter_validated(bcache_test, crypto:strong_rand_bytes(48)),
	mark_nonce_limiter_validation_scheduled(bcache_test, block_id(B13)),
	mark_nonce_limiter_validated(bcache_test, block_id(B13)),
	?assertEqual({B13, on_chain}, get_block_and_status(bcache_test, block_id(B13))),
	?assertMatch({B14, {not_validated, awaiting_nonce_limiter_validation}},
			get_block_and_status(bcache_test, block_id(B14))),
	mark_nonce_limiter_validation_scheduled(bcache_test, block_id(B14)),
	?assertMatch({B14, {not_validated, nonce_limiter_validation_scheduled}},
			get_block_and_status(bcache_test, block_id(B14))),
	?assertMatch({B14, [B13], {{not_validated, nonce_limiter_validation_scheduled}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	mark_nonce_limiter_validated(bcache_test, block_id(B14)),
	?assertMatch({B14, {not_validated, nonce_limiter_validated}},
			get_block_and_status(bcache_test, block_id(B14))),
	?assertMatch({B14, [B13], {{not_validated, nonce_limiter_validated}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	add(bcache_test, B15 = on_top(random_block_after_repacking(3), B14)),
	?assertMatch({B14, [B13], {{not_validated, nonce_limiter_validated}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	add_validated(bcache_test, B14),
	?assertMatch({B15, [B14, B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	?assertMatch({B14, validated}, get_block_and_status(bcache_test, block_id(B14))),
	add(bcache_test, B16 = on_top(random_block_after_repacking(4), B15)),
	mark_nonce_limiter_validation_scheduled(bcache_test, block_id(B16)),
	?assertMatch({B15, [B14, B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	mark_nonce_limiter_validated(bcache_test, block_id(B16)),
	?assertMatch({B15, [B14, B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	?assertMatch({B16, {not_validated, nonce_limiter_validated}},
			get_block_and_status(bcache_test, block_id(B16))),
	mark_tip(bcache_test, block_id(B14)),
	?assertEqual({B14, on_chain}, get_block_and_status(bcache_test, block_id(B14))),
	?assertMatch({B15, [B14], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)).

random_block(CDiff) ->
	#block{ indep_hash = crypto:strong_rand_bytes(48), height = 0, cumulative_diff = CDiff,
			hash = crypto:strong_rand_bytes(32) }.

random_block_after_repacking(CDiff) ->
	#block{ indep_hash = crypto:strong_rand_bytes(48), height = 0, cumulative_diff = CDiff,
			hash = crypto:strong_rand_bytes(32) }.

block_id(#block{ indep_hash = H }) ->
	H.

on_top(B, PrevB) ->
	B#block{ previous_block = PrevB#block.indep_hash, height = PrevB#block.height + 1 }.
