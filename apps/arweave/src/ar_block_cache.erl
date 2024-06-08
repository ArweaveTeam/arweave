%%% @doc The module maintains a DAG of blocks that have passed the PoW validation, in ETS.
%%% NOTE It is not safe to call functions which modify the state from different processes.
-module(ar_block_cache).

-export([new/2, initialize_from_list/2, add/2, mark_nonce_limiter_validated/2,
		add_validated/2,
		mark_tip/2, get/2, get_earliest_not_validated_from_longest_chain/1,
		get_longest_chain_cache/1,
		get_block_and_status/2, remove/2, get_checkpoint_block/1, prune/2,
		get_by_solution_hash/5, is_known_solution_hash/2,
		get_siblings/2, get_fork_blocks/2, update_timestamp/3]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% The expiration time in seconds for every "alternative" block (a block with non-unique
%% solution).
-define(ALTERNATIVE_BLOCK_EXPIRATION_TIME_SECONDS, 5).

%% @doc Block validation status
%% on_chain: block is validated and belongs to the tip fork
%% validated: block is validated but does not belong to the tip fork
%% not_validated: block is not validated yet
%% none: null status

%% @doc ETS table: block_cache
%% {block, BlockHash} => {#block{}, block_status(), Timestamp, set(Children)}
%%   - Children is a set of all blocks that have this block as their previous block. Children is
%%     used to track branches in the chain that fork off this block (i.e. they are DAG children)
%% max_cdiff => {CDiff, BlockHash}
%%   - maximum cumulative difficulty encountered and its BlockHash. This is used to determine
%%     whether we need to switch from the current tip to a fork tip.
%% {solution, SolutionHash} => set(BlockHash)
%%   - all blocks with the same solution hash
%% longest_chain => [{BlockHash, [TXIDs]}]
%%  - the top ?STORE_BLOCKS_BEHIND_CURRENT blocks of the longest chain
%% tip -> BlockHash
%%   - curent block chain tip
%% links -> gb_set({Height, BlockHash})
%%   - all blocks in the cache sorted by height. This is used when pruning the cache and
%%     discarding all blocks below a certain height (and all off-chain children of those blocks
%%     regardless of their height)

%%%===================================================================
%%% Public API.
%%%===================================================================

%% @doc Create a cache, initialize it with the given block. The block is marked as on-chain
%% and as a tip block.
new(Tab, B) ->
	#block{ indep_hash = H, hash = SolutionH, cumulative_diff = CDiff, height = Height } = B,
	ets:delete_all_objects(Tab),
	ar_ignore_registry:add(H),
	insert(Tab, [
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
			height = Height
		} = B) ->
	case ets:lookup(Tab, {block, H}) of
		[] ->
			ar_ignore_registry:add(H),
			RemainingHs = remove_expired_alternative_blocks(Tab, SolutionH),
			SolutionSet = sets:from_list([H | RemainingHs]),
			[{_, Set}] = ets:lookup(Tab, links),
			[{_, {PrevB, PrevStatus, PrevTimestamp, Children}}] = ets:lookup(Tab, {block, PrevH}),
			Set2 = gb_sets:insert({Height, H}, Set),
			Status = {not_validated, awaiting_nonce_limiter_validation},
			%% If CDiff > MaxCDiff it means this block belongs to the heaviest fork we're aware
			%% of. If our current tip is not on this fork, ar_node_worker may switch to this fork.
			insert(Tab, [
				{max_cdiff, maybe_increase_max_cdiff(Tab, B, Status)},
				{links, Set2},
				{{solution, SolutionH}, SolutionSet},
				{{block, H}, {B, Status, erlang:timestamp(), sets:new()}},
				{{block, PrevH},
						{PrevB, PrevStatus, PrevTimestamp, sets:add_element(H, Children)}}
			]);
		[{_, {_B, {not_validated, _} = CurrentStatus, CurrentTimestamp, Children}}] ->
			insert(Tab, {{block, H}, {B, CurrentStatus, CurrentTimestamp, Children}});
		_ ->
			?LOG_WARNING([{event, attempt_to_update_already_validated_cached_block},
					{h, ar_util:encode(H)}, {height, Height},
					{previous_block, ar_util:encode(PrevH)}]),
			ok
	end.

%% @doc Check all blocks that share the same solution and remove those that expired.
remove_expired_alternative_blocks(Tab, SolutionH) ->
	SolutionSet =
		case ets:lookup(Tab, {solution, SolutionH}) of
			[] ->
				sets:new();
			[{_, SolutionSet2}] ->
				SolutionSet2
		end,
	remove_expired_alternative_blocks2(Tab, sets:to_list(SolutionSet)).

remove_expired_alternative_blocks2(_Tab, []) ->
	[];
remove_expired_alternative_blocks2(Tab, [H | Hs]) ->
	[{_, {_B, Status, Timestamp, Children}}] = ets:lookup(Tab, {block, H}),
	case Status of
		on_chain ->
			[H | remove_expired_alternative_blocks2(Tab, Hs)];
		_ ->
			LifetimeSeconds = get_alternative_block_lifetime(Tab, Children),
			{MegaSecs, Secs, MicroSecs} = Timestamp,
			ExpirationTimestamp = {MegaSecs, Secs + LifetimeSeconds, MicroSecs},
			case timer:now_diff(erlang:timestamp(), ExpirationTimestamp) >= 0 of
				true ->
					remove(Tab, H),
					remove_expired_alternative_blocks2(Tab, Hs);
				false ->
					[H | remove_expired_alternative_blocks2(Tab, Hs)]
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
%% not 'awaiting_nonce_limiter_validation'.
mark_nonce_limiter_validated(Tab, H) ->
	case ets:lookup(Tab, {block, H}) of
		[{_, {B, {not_validated, awaiting_nonce_limiter_validation}, Timestamp, Children}}] ->
			insert(Tab, {{block, H}, {B,
					{not_validated, nonce_limiter_validated}, Timestamp, Children}});
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
					RemainingHs = remove_expired_alternative_blocks(Tab, SolutionH),
					SolutionSet = sets:from_list([H | RemainingHs]),
					[{_, Set}] = ets:lookup(Tab, links),
					Status = validated,
					insert(Tab, [
						{{block, PrevH}, {PrevB, PrevStatus, PrevTimestamp,
								sets:add_element(H, PrevChildren)}},
						{{block, H}, {B, Status, erlang:timestamp(), sets:new()}},
						{max_cdiff, maybe_increase_max_cdiff(Tab, B, Status)},
						{links, gb_sets:insert({Height, H}, Set)},
						{{solution, SolutionH}, SolutionSet}
					]);
				[{_, {_B, on_chain, Timestamp, Children}}] ->
					insert(Tab, [
						{{block, PrevH}, {PrevB, PrevStatus, PrevTimestamp,
								sets:add_element(H, PrevChildren)}},
						{{block, H}, {B, on_chain, Timestamp, Children}}
					]);
				[{_, {_B, _Status, Timestamp, Children}}] ->
					insert(Tab, [
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

%% @doc Get a {block, previous blocks, status} tuple for the earliest block from
%% the longest chain, which has not been validated yet. The previous blocks are
%% sorted from newest to oldest. The last one is a block from the current fork.
%% status is a tuple that indicates where in the validation process block is.
get_earliest_not_validated_from_longest_chain(Tab) ->
	[{_, Tip}] = ets:lookup(Tab, tip),
	[{_, {CDiff, H}}] = ets:lookup(Tab, max_cdiff),
	[{_, {#block{ cumulative_diff = TipCDiff }, _, _, _}}] = ets:lookup(Tab, {block, Tip}),
	case TipCDiff >= CDiff of
		true ->
			%% Current Tip is tip of the longest chain
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
%%
%% The cache is updated via update_longest_chain_cache/1 which calls
%% get_longest_chain_block_txs_pairs/7
get_longest_chain_cache(Tab) ->
	[{longest_chain, LongestChain}] = ets:lookup(Tab, longest_chain),
	LongestChain.

get_longest_chain_block_txs_pairs(_Tab, _H, 0, _PrevStatus, _PrevH, Pairs, NotOnChainCount) ->
	{lists:reverse(Pairs), NotOnChainCount};
get_longest_chain_block_txs_pairs(Tab, H, N, PrevStatus, PrevH, Pairs, NotOnChainCount) ->
	case ets:lookup(Tab, {block, H}) of
		[{_, {B, {not_validated, awaiting_nonce_limiter_validation}, _Timestamp,
				_Children}}] ->
			get_longest_chain_block_txs_pairs(Tab, B#block.previous_block,
					?STORE_BLOCKS_BEHIND_CURRENT, none, none, [], 0);
		[{_, {B, Status, _Timestamp, _Children}}] ->
			case PrevStatus == on_chain andalso Status /= on_chain of
				true ->
					%% A reorg should have happened in the meantime - an unlikely
					%% event, retry.
					get_longest_chain_cache(Tab);
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
							get_longest_chain_cache(Tab);
						[_] ->
							%% Pairs already contains the deepest block of the cache.
							{lists:reverse(Pairs), NotOnChainCount}
					end;
				_ ->
					%% The block has been invalidated -
					%% an unlikely race condition so we retry.
					get_longest_chain_cache(Tab)
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
		[{_, {B, Status, Timestamp, _Children}}] ->
			{B, {Status, Timestamp}}
	end.

%% @doc Mark the given block as the tip block. Mark the previous blocks as on-chain.
%% Mark the on-chain blocks from other forks as validated. Raises invalid_tip if
%% one of the preceeding blocks is not validated. Raises not_found if the block
%% is not found.
%%
%% Setting a new tip can cause some branches to be invalidated by the checkpoint, so we need
%% to recalculate max_cdiff.
mark_tip(Tab, H) ->
	case ets:lookup(Tab, {block, H}) of
		[{_, {B, Status, Timestamp, Children}}] ->
			case is_valid_fork(Tab, B, Status) of
				true ->
					insert(Tab, [
						{tip, H},
						{{block, H}, {B, on_chain, Timestamp, Children}} |
						mark_on_chain(Tab, B)
					]),
					%% We would only update max_cdiff if somehow the old max_cdiff was on a branch
					%% that has been invalidated due to the new tip causing the checkpoint to move.
					%% In practice we would not expect this to happen.
					[{_, {_CDiff, CDiffH}}] = ets:lookup(Tab, max_cdiff),
					case is_valid_fork(Tab, CDiffH) of
						true ->
							ok;
						false ->
							insert(Tab, {max_cdiff, find_max_cdiff(Tab, B#block.height)})
					end;
				false ->
					error(invalid_tip)
			end;
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
			insert(Tab, [
				{max_cdiff, case ets:lookup(Tab, {block, H2}) of
								[] ->
									find_max_cdiff(Tab, get_tip_height(Tab));
								_ ->
									C
							end},
				{{block, PrevH}, {PrevB, PrevBStatus, PrevTimestamp,
						sets:del_element(H, PrevBChildren)}}
			]),
			ar_ignore_registry:remove(H),
			ok
	end.

get_checkpoint_block(RecentBI) ->
	get_checkpoint_block2(RecentBI, 1, ?CHECKPOINT_DEPTH).

%% @doc Prune the cache.  Discard all blocks deeper than Depth from the tip and
%% all of their children that are not on_chain.
%% 
%% Height 99              A    B' C
%%                         \  /   |
%%        98                D'    E
%%						      \  /
%%        97                   F' 
%%
%% B' is the Tip. prune(Tab, 1) will remove F', E, and C from the cache.             
prune(Tab, Depth) ->
	prune2(Tab, Depth, get_tip_height(Tab)).

%% @doc Return true if there is at least one block in the cache with the given solution hash.
is_known_solution_hash(Tab, SolutionH) ->
	case ets:lookup(Tab, {solution, SolutionH}) of
		[] ->
			false;
		[{_, _Set}] ->
			true
	end.

%% @doc Return a block from the block cache meeting the following requirements:
%% - hash == SolutionH;
%% - indep_hash /= H.
%%
%% If there are several blocks, choose one with the same cumulative difficulty
%% or CDiff > PrevCDiff2 and CDiff2 > PrevCDiff (double-signing). If there are no
%% such blocks, return any other block matching the conditions above. Return not_found
%% if there are no blocks matching those conditions.
get_by_solution_hash(Tab, SolutionH, H, CDiff, PrevCDiff) ->
	case ets:lookup(Tab, {solution, SolutionH}) of
		[] ->
			not_found;
		[{_, Set}] ->
			get_by_solution_hash(Tab, SolutionH, H, CDiff, PrevCDiff, sets:to_list(Set), none)
	end.

get_by_solution_hash(_Tab, _SolutionH, _H, _CDiff, _PrevCDiff, [], B) ->
	case B of
		none ->
			not_found;
		_ ->
			B
	end;
get_by_solution_hash(Tab, SolutionH, H, CDiff, PrevCDiff, [H | L], B) ->
	get_by_solution_hash(Tab, SolutionH, H, CDiff, PrevCDiff, L, B);
get_by_solution_hash(Tab, SolutionH, H, CDiff, PrevCDiff, [H2 | L], _B) ->
	case get(Tab, H2) of
		not_found ->
			%% An extremely unlikely race condition - simply retry.
			get_by_solution_hash(Tab, SolutionH, H, CDiff, PrevCDiff);
		#block{ cumulative_diff = CDiff } = B2 ->
			B2;
		#block{ cumulative_diff = CDiff2, previous_cumulative_diff = PrevCDiff2 } = B2
				when CDiff2 > PrevCDiff, CDiff > PrevCDiff2 ->
			B2;
		B2 ->
			get_by_solution_hash(Tab, SolutionH, H, CDiff, PrevCDiff, L, B2)
	end.

%% @doc Return the list of siblings of the given block, if any.
get_siblings(Tab, B) ->
	H = B#block.indep_hash,
	PrevH = B#block.previous_block,
	case ets:lookup(Tab, {block, PrevH}) of
		[] ->
			[];
		[{_, {_B, _Status, _CurrentTimestamp, Children}}] ->
			sets:fold(
				fun(SibH, Acc) ->
					case SibH of
						H ->
							Acc;
						_ ->
							case ets:lookup(Tab, {block, SibH}) of
								[] ->
									Acc;
								[{_, {Sib, _, _, _}}] ->
									[Sib | Acc]
							end
					end
				end,
				[],
				Children
			)
	end.

update_timestamp(Tab, H, ReceiveTimestamp) ->
	case ets:lookup(Tab, {block, H}) of
		[{_, {B, Status, Timestamp, Children}}] ->
			case B#block.receive_timestamp of
				undefined ->
					insert(Tab, {
							{block, H},
							{
								B#block{receive_timestamp = ReceiveTimestamp},
								Status,
								Timestamp,
								Children
							}
						}, false);
				_ ->
					ok
			end;
		[] ->
			?LOG_ERROR([
				{event, ignored_block_missing_from_cache}, {block, ar_util:encode(H)}]),
			not_found
	end.

%%%===================================================================
%%% Private functions.
%%%===================================================================

insert(Tab, Args) ->
	insert(Tab, Args, true).
insert(Tab, Args, UpdateCache) ->
	ets:insert(Tab, Args),
	case UpdateCache of
		true ->
			update_longest_chain_cache(Tab);
		false ->
			ok
	end.

delete(Tab, Args) ->
	delete(Tab, Args, true).
delete(Tab, Args, UpdateCache) ->
	ets:delete(Tab, Args),
	case UpdateCache of
		true ->
			update_longest_chain_cache(Tab);
		false ->
			ok
	end.

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
		[{_, {#block{ hash = SolutionH, height = Height }, _Status, _Timestamp, Children}}] ->
			%% Don't update the cache here. remove/2 will do it.
			delete(Tab, {block, H}, false), 
			ar_ignore_registry:remove(H),
			remove_solution(Tab, H, SolutionH),
			insert(Tab, {links, gb_sets:del_element({Height, H}, Set)}, false),
			sets:fold(
				fun(Child, ok) ->
					remove2(Tab, Child)
				end,
				ok,
				Children
			)
	end.

remove_solution(Tab, H, SolutionH) ->
	[{_, SolutionSet}] = ets:lookup(Tab, {solution, SolutionH}),
	case sets:size(SolutionSet) of
		1 ->
			delete(Tab, {solution, SolutionH}, false);
		_ ->
			SolutionSet2 = sets:del_element(H, SolutionSet),
			insert(Tab, {{solution, SolutionH}, SolutionSet2}, false)
	end.

get_tip_height(Tab) ->
	[{_, Tip}] = ets:lookup(Tab, tip),
	[{_, {#block{ height = Height }, _Status, _Timestamp, _Children}}] = ets:lookup(Tab,
			{block, Tip}),
	Height.

get_checkpoint_height(TipHeight) ->
	TipHeight - ?CHECKPOINT_DEPTH + 1.

get_checkpoint_block2([{H, _, _}], _N, _CheckpointDepth) ->
	%% The genesis block.
	ar_block_cache:get(block_cache, H);
get_checkpoint_block2([{H, _, _} | BI], N, CheckpointDepth) ->
	 B = ar_block_cache:get(block_cache, H),
	 get_checkpoint_block2(BI, N + 1, B, CheckpointDepth).

get_checkpoint_block2([{H, _, _}], _N, B, _CheckpointDepth) ->
	%% The genesis block.
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			B;
		B2 ->
			B2
	end;
get_checkpoint_block2([{H, _, _} | _], N, B, CheckpointDepth) when N == CheckpointDepth ->
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			B;
		B2 ->
			B2
	end;
get_checkpoint_block2([{H, _, _} | BI], N, B, CheckpointDepth) ->
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			B;
		B2 ->
			get_checkpoint_block2(BI, N + 1, B2, CheckpointDepth)
	end.

%% @doc Return true if B is either on the main fork or on a fork which branched off at the
%% checkpoint height or higher.
is_valid_fork(Tab, H) ->
	[{_, {B, Status, _Timestamp, _Children}}] = ets:lookup(Tab, {block, H}),
	is_valid_fork(Tab, B, Status).
is_valid_fork(Tab, B, Status) ->
	CheckpointHeight = get_checkpoint_height(get_tip_height(Tab)),
	is_valid_fork(Tab, B, Status, CheckpointHeight).

is_valid_fork(_Tab, #block{ height = Height, indep_hash = H }, _Status, CheckpointHeight)
  		when Height < CheckpointHeight ->
	?LOG_WARNING([{event, found_invalid_heavy_fork}, {hash, ar_util:encode(H)},
				{height, Height}, {checkpoint_height, CheckpointHeight}]),
	false;
is_valid_fork(_Tab, _B, on_chain, _CheckpointHeight) ->
	true;
is_valid_fork(Tab, B, _Status, CheckpointHeight) ->
	[{_, {PrevB, PrevStatus, _, _}}] = ets:lookup(Tab, {block, B#block.previous_block}),
	is_valid_fork(Tab, PrevB, PrevStatus, CheckpointHeight).

maybe_increase_max_cdiff(Tab, B, Status) ->
	[{_, C}] = ets:lookup(Tab, max_cdiff),
	maybe_increase_max_cdiff(Tab, B, Status, C).

maybe_increase_max_cdiff(Tab, B, Status, {MaxCDiff, _H} = C) ->
	case B#block.cumulative_diff > MaxCDiff andalso is_valid_fork(Tab, B, Status) of
		true ->
			{B#block.cumulative_diff, B#block.indep_hash};
		false ->
			C
	end.

find_max_cdiff(Tab, TipHeight) ->
	CheckpointHeight = get_checkpoint_height(TipHeight),
	[{_, Set}] = ets:lookup(Tab, links),
	gb_sets:fold(
		fun ({Height, _H}, Acc) when Height < CheckpointHeight ->
				Acc;
			({_Height, H}, not_set) ->
				[{_, {#block{ cumulative_diff = CDiff }, _, _, _}}] = ets:lookup(Tab,
						{block, H}),
				{CDiff, H};
			({_Height, H}, Acc) ->
				[{_, {B, Status, _, _}}] = ets:lookup(Tab, {block, H}),
				maybe_increase_max_cdiff(Tab, B, Status, Acc)
		end,
		not_set,
		Set
	).

prune2(Tab, Depth, TipHeight) ->
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
					insert(Tab, {links, Set2}, false),
					%% The lowest block must be on-chain by construction.
					[{_, {B, on_chain, _Timestamp, Children}}] = ets:lookup(Tab, {block, H}),
					#block{ hash = SolutionH } = B,
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
					delete(Tab, {block, H}),
					ar_ignore_registry:remove(H),
					prune2(Tab, Depth, TipHeight)
			end
	end.

update_longest_chain_cache(Tab) ->
	[{_, {_CDiff, H}}] = ets:lookup(Tab, max_cdiff),
	Result = get_longest_chain_block_txs_pairs(Tab, H, ?STORE_BLOCKS_BEHIND_CURRENT,
			none, none, [], 0),
	case ets:update_element(Tab, longest_chain, {2, Result}) of
		true -> ok;
		false ->
			%% if insert_new fails it means another process added the longest_chain key
			%% between when we called update_element here. Extremely unlikely, really only
			%% possible when the node first starts up, and ultimately not super relevant since
			%% the cache will likely be refreshed again shortly. So we'll ignore.
			ets:insert_new(Tab, {longest_chain, Result})
	end,
	Result.

%%%===================================================================
%%% Tests.
%%%===================================================================

checkpoint_test() ->
	ets:new(bcache_test, [set, named_table]),

	%% Height	Block/Status
	%%
	%% 3		B3/on_chain
	%%			     | 
	%% 2		B2/on_chain        B2B/not_validated
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated
	%%					  \            /
	%% 0					B0/on_chain
	new(bcache_test, B0 = random_block(0)),
	add(bcache_test, B1 = on_top(random_block(1), B0)),
	add(bcache_test, B1B = on_top(random_block(1), B0)),
	add(bcache_test, B2 = on_top(random_block(2), B1)),
	add(bcache_test, B2B = on_top(random_block(2), B1B)),
	add(bcache_test, B3 = on_top(random_block(3), B2)),
	mark_tip(bcache_test, block_id(B1)),
	mark_tip(bcache_test, block_id(B2)),
	mark_tip(bcache_test, block_id(B3)),

	?assertMatch(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B3, B2, B1, B0], 0),
	assert_tip(block_id(B3)),
	assert_max_cdiff({3, block_id(B3)}),
	assert_is_valid_fork(true, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, not_validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),

	%% Add B4 as not_validated. No blocks are pushed beneath the checkpoint height since B4
	%% has not been validated.
	%%
	%% Height	Block/Status
	%%
	%% 4		B4/not_validated
	%%			     |
	%% 3		B3/on_chain
	%%			     | 
	%% 2		B2/on_chain        B2B/not_validated/invalid_fork
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated/invalid_fork
	%%					  \            /
	%% 0					B0/on_chain/invalid_fork
	add(bcache_test, B4 = on_top(random_block(4), B3)),

	?assertMatch({B4, [B3], {{not_validated, awaiting_nonce_limiter_validation}, _}},
		get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B3, B2, B1, B0], 0),
	assert_tip(block_id(B3)),
	assert_max_cdiff({4, block_id(B4)}),
	assert_is_valid_fork(true, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, not_validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),
	assert_is_valid_fork(true, not_validated, B4),

	%% Mark B4 as the tip, this pushes B0 below the checkpoint height and invalidates B1B and B2B.
	%%
	%% Height	Block/Status
	%%
	%% 4		B4/on_chain
	%%			     |
	%% 3		B3/on_chain
	%%			     | 
	%% 2		B2/on_chain        B2B/not_validated/invalid_fork
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated/invalid_fork
	%%					  \            /
	%% 0					B0/on_chain/invalid_fork
	mark_tip(bcache_test, block_id(B4)),

	?assertMatch(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B4, B3, B2, B1, B0], 0),
	assert_tip(block_id(B4)),
	assert_max_cdiff({4, block_id(B4)}),
	assert_is_valid_fork(false, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(false, not_validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(false, not_validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),
	assert_is_valid_fork(true, on_chain, B4),

	%% Add B3B with cdiff 5 to the invalid fork. Nothing should change.
	%%
	%% Height	Block/Status
	%%
	%% 4		B4/on_chain
	%%			     |
	%% 3		B3/on_chain        B3B/not_validated/invalid_fork
	%%			     |                  |
	%% 2		B2/on_chain        B2B/not_validated/invalid_fork
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated/invalid_fork
	%%					  \            /
	%% 0					B0/on_chain/invalid_fork
	add(bcache_test, B3B = on_top(random_block(5), B2B)),

	?assertMatch(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B4, B3, B2, B1, B0], 0),
	assert_tip(block_id(B4)),
	assert_max_cdiff({4, block_id(B4)}),
	assert_is_valid_fork(false, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(false, not_validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(false, not_validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),
	assert_is_valid_fork(true, on_chain, B4),
	assert_is_valid_fork(false, not_validated, B3B),
	
	%% Remove B4, this should revalidate the fork and make it the max_cdiff
	%%
	%% Height	Block/Status
	%%
	%% 3		B3/on_chain        B3B/not_validated
	%%			     |                  |
	%% 2		B2/on_chain        B2B/not_validated
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated
	%%					  \            /
	%% 0					B0/on_chain
	mark_tip(bcache_test, block_id(B3)),
	remove(bcache_test, block_id(B4)),
	
	?assertMatch({B1B, [B0], {{not_validated, awaiting_nonce_limiter_validation}, _}},
		get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B0], 0),
	assert_tip(block_id(B3)),
	assert_max_cdiff({5, block_id(B3B)}),
	assert_is_valid_fork(true, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, not_validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),
	assert_is_valid_fork(true, not_validated, B3B),

	%% We should not be able to mark blocks on the invalid fork as the tip.
	%%
	%% Height	Block/Status
	%%
	%% 4		B4/on_chain
	%%			     |
	%% 3		B3/on_chain        B3B/not_validated/invalid_fork
	%%			     |                  |
	%% 2		B2/on_chain        B2B/not_validated/invalid_fork
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated/invalid_fork
	%%					  \            /
	%% 0					B0/on_chain/invalid_fork
	add(bcache_test, B4),
	mark_tip(bcache_test, block_id(B4)),

	?assertException(error, invalid_tip, mark_tip(bcache_test, block_id(B1B))),
	?assertMatch(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B4, B3, B2, B1, B0], 0),
	assert_tip(block_id(B4)),
	assert_max_cdiff({4, block_id(B4)}),
	assert_is_valid_fork(false, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(false, not_validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(false, not_validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),
	assert_is_valid_fork(false, not_validated, B3B),

	ets:delete(bcache_test).

checkpoint_invalidate_max_cdiff_test() ->
	ets:new(bcache_test, [set, named_table]),

	%% B2B is heaviest.
	%%
	%% Height	Block/Status
	%%
	%% 4		B4/not_validated
	%%			     |
	%% 3		B3/on_chain
	%%			     | 
	%% 2		B2/on_chain        B2B/not_validated
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated
	%%					  \            /
	%% 0					B0/on_chain
	new(bcache_test, B0 = random_block(0)),
	add(bcache_test, B1 = on_top(random_block(1), B0)),
	add(bcache_test, B2 = on_top(random_block(2), B1)),
	add(bcache_test, B3 = on_top(random_block(3), B2)),
	add(bcache_test, B4 = on_top(random_block(4), B3)),
	add(bcache_test, B1B = on_top(random_block(1), B0)),
	add(bcache_test, B2B = on_top(random_block(5), B1B)),	
	mark_tip(bcache_test, block_id(B1)),
	mark_tip(bcache_test, block_id(B2)),
	mark_tip(bcache_test, block_id(B3)),

	?assertMatch({B1B, [B0], {{not_validated, awaiting_nonce_limiter_validation}, _}},
		get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B0], 0),
	assert_tip(block_id(B3)),
	assert_max_cdiff({5, block_id(B2B)}),
	assert_is_valid_fork(true, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, not_validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),
	assert_is_valid_fork(true, not_validated, B4),

	%% B2B is still heaviest, but since B4 is the new tip, B2B's branch has been pushed below
	%% the checkpoint.
	%%
	%% Height	Block/Status
	%%
	%% 4		B4/on_chain
	%%			     |
	%% 3		B3/on_chain
	%%			     | 
	%% 2		B2/on_chain        B2B/not_validated/invalid_fork
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated/invalid_fork
	%%					  \            /
	%% 0					B0/on_chain/invalid_fork
	mark_tip(bcache_test, block_id(B4)),

	?assertMatch(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B4, B3, B2, B1, B0], 0),
	assert_tip(block_id(B4)),
	assert_max_cdiff({4, block_id(B4)}),
	assert_is_valid_fork(false, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(false, not_validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(false, not_validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),
	assert_is_valid_fork(true, on_chain, B4),

	ets:delete(bcache_test).

checkpoint_invalidate_tip_test() ->
	ets:new(bcache_test, [set, named_table]),

	%% B2B is the tip
	%%
	%% Height	Block/Status
	%%
	%% 4		B4/not_validated
	%%			     |
	%% 3		B3/validated
	%%			     | 
	%% 2		B2/validated       B2B/on_chain
	%%			     |                  |
	%% 1		B1/validated       B1B/on_chain
	%%					  \            /
	%% 0					B0/on_chain
	new(bcache_test, B0 = random_block(0)),
	add(bcache_test, B1 = on_top(random_block(1), B0)),
	add(bcache_test, B2 = on_top(random_block(2), B1)),
	add(bcache_test, B3 = on_top(random_block(3), B2)),
	add(bcache_test, B4 = on_top(random_block(4), B3)),
	add(bcache_test, B1B = on_top(random_block(1), B0)),
	add(bcache_test, B2B = on_top(random_block(5), B1B)),
	mark_tip(bcache_test, block_id(B1)),
	mark_tip(bcache_test, block_id(B2)),
	mark_tip(bcache_test, block_id(B3)),
	mark_tip(bcache_test, block_id(B1B)),
	mark_tip(bcache_test, block_id(B2B)),

	?assertMatch(not_found,
		get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B2B, B1B, B0], 0),
	assert_tip(block_id(B2B)),
	assert_max_cdiff({5, block_id(B2B)}),
	assert_is_valid_fork(true, on_chain, B0),
	assert_is_valid_fork(true, validated, B1),
	assert_is_valid_fork(true, on_chain, B1B),
	assert_is_valid_fork(true, validated, B2),
	assert_is_valid_fork(true, on_chain, B2B),
	assert_is_valid_fork(true, validated, B3),
	assert_is_valid_fork(true, not_validated, B4),

	%% When we mark B4 as the tip it will also invalidate the B2B branch.
	%%
	%% Height	Block/Status
	%%
	%% 4		B4/on_chain
	%%			     |
	%% 3		B3/on_chain
	%%			     | 
	%% 2		B2/on_chain        B2B/not_validated/invalid_fork
	%%			     |                  |
	%% 1		B1/on_chain        B1B/not_validated/invalid_fork
	%%					  \            /
	%% 0					B0/on_chain/invalid_fork
	mark_tip(bcache_test, block_id(B4)),

	?assertMatch(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B4, B3, B2, B1, B0], 0),
	assert_tip(block_id(B4)),
	assert_max_cdiff({4, block_id(B4)}),
	assert_is_valid_fork(false, on_chain, B0),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(false, validated, B1B),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(false, validated, B2B),
	assert_is_valid_fork(true, on_chain, B3),
	assert_is_valid_fork(true, on_chain, B4),

	ets:delete(bcache_test).

block_cache_test() ->
	ets:new(bcache_test, [set, named_table]),

	%% Initialize block_cache from B1
	%%
	%% Height		Block/Status
	%%
	%% 0			B1/on_chain
	new(bcache_test, B1 = random_block(0)),
	?assertEqual(not_found, get(bcache_test, crypto:strong_rand_bytes(48))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, crypto:strong_rand_bytes(32),
			crypto:strong_rand_bytes(32), 1, 1)),
	?assertEqual(B1, get(bcache_test, block_id(B1))),
	?assertEqual(B1, get_by_solution_hash(bcache_test, B1#block.hash,
			crypto:strong_rand_bytes(32), 1, 1)),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B1#block.hash,
			block_id(B1), 1, 1)),
	assert_longest_chain([B1], 0),
	assert_tip(block_id(B1)),
	assert_max_cdiff({0, block_id(B1)}),
	assert_is_valid_fork(true, on_chain, B1),
	?assertEqual([], get_siblings(bcache_test, B1)),

	%% Re-adding B1 shouldn't change anything - i.e. nothing should be updated because the
	%% block is already on chain
	%%
	%% Height		Block/Status
	%%
	%% 0			B1/on_chain
	add(bcache_test, B1#block{ txs = [crypto:strong_rand_bytes(32)] }),
	?assertEqual(B1#block{ txs = [] }, get(bcache_test, block_id(B1))),
	?assertEqual(B1#block{ txs = [] }, get_by_solution_hash(bcache_test, B1#block.hash,
			crypto:strong_rand_bytes(32), 1, 1)),
	assert_longest_chain([B1], 0),
	assert_max_cdiff({0, block_id(B1)}),
	assert_is_valid_fork(true, on_chain, B1),

	%% Same as above.
	%%
	%% Height		Block/Status
	%%
	%% 0			B1/on_chain
	add(bcache_test, B1),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B1], 0),
	assert_tip(block_id(B1)),
	assert_max_cdiff({0, block_id(B1)}),
	assert_is_valid_fork(true, on_chain, B1),

	%% Add B2 as not_validated
	%%
	%% Height	Block/Status
	%%
	%% 1		B2/not_validated
	%%				|
	%% 0		B1/on_chain
	add(bcache_test, B2 = on_top(random_block(1), B1)),
	ExpectedStatus = awaiting_nonce_limiter_validation,
	?assertMatch({B2, [B1], {{not_validated, ExpectedStatus}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B1], 0),
	assert_tip(block_id(B1)),
	assert_max_cdiff({1, block_id(B2)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, not_validated, B2),
	?assertEqual([], get_siblings(bcache_test, B2)),

	%% Add a TXID to B2, but still don't mark as validated
	%%
	%% Height	Block/Status
	%%
	%% 1		B2/not_validated
	%%				|
	%% 0		B1/on_chain
	TXID = crypto:strong_rand_bytes(32),
	add(bcache_test, B2#block{ txs = [TXID] }),
	?assertEqual(B2#block{ txs = [TXID] }, get(bcache_test, block_id(B2))),
	?assertEqual(B2#block{ txs = [TXID] }, get_by_solution_hash(bcache_test, B2#block.hash,
			crypto:strong_rand_bytes(32), 1, 1)),
	?assertEqual(B2#block{ txs = [TXID] }, get_by_solution_hash(bcache_test, B2#block.hash,
			block_id(B1), 1, 1)),
	assert_longest_chain([B1], 0),
	assert_tip(block_id(B1)),
	assert_max_cdiff({1, block_id(B2)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, not_validated, B2),

	%% Remove B2
	%%
	%% Height	Block/Status
	%%
	%% 0		B1/on_chain
	remove(bcache_test, block_id(B2)),
	?assertEqual(not_found, get(bcache_test, block_id(B2))),
	?assertEqual(B1, get(bcache_test, block_id(B1))),
	assert_longest_chain([B1], 0),
	assert_tip(block_id(B1)),
	assert_max_cdiff({0, block_id(B1)}),
	assert_is_valid_fork(true, on_chain, B1),

	%% Add B and B1_2 creating a fork, with B1_2 at a higher difficulty. Nether are validated.
	%%
	%% Height	Block/Status
	%%
	%% 1		B2/not_validated    B1_2/not_validated
	%%					  \             /
	%% 0					B1/on_chain
	add(bcache_test, B2),
	add(bcache_test, B1_2 = (on_top(random_block(2), B1))#block{ hash = B1#block.hash }),
	?assertEqual(B1, get_by_solution_hash(bcache_test, B1#block.hash, block_id(B1_2),
			1, 1)),
	?assertEqual(B1, get_by_solution_hash(bcache_test, B1#block.hash,
			crypto:strong_rand_bytes(32), B1#block.cumulative_diff, 1)),
	?assertEqual(B1_2, get_by_solution_hash(bcache_test, B1#block.hash,
			crypto:strong_rand_bytes(32), B1_2#block.cumulative_diff, 1)),
	?assert(lists:member(get_by_solution_hash(bcache_test, B1#block.hash, <<>>, 1, 1),
			[B1, B1_2])),
	assert_longest_chain([B1], 0),
	assert_tip(block_id(B1)),
	assert_max_cdiff({2, block_id(B1_2)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, not_validated, B2),
	assert_is_valid_fork(true, not_validated, B1_2),
	?assertEqual([B2], get_siblings(bcache_test, B1_2)),
	?assertEqual([B1_2], get_siblings(bcache_test, B2)),

	%% Even though B2 is marked as a tip, it is still lower difficulty than B1_2 so will
	%% not be included in the longest chain
	%%
	%% Height	Block/Status
	%%
	%% 1		B2/on_chain      B1_2/not_validated
	%%					  \             /
	%% 0					B1/on_chain
	mark_tip(bcache_test, block_id(B2)),
	?assertEqual(B1_2, get(bcache_test, block_id(B1_2))),
	assert_longest_chain([B1], 0),
	assert_tip(block_id(B2)),
	assert_max_cdiff({2, block_id(B1_2)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B1_2),

	%% Remove B1_2, causing B2 to now be the tip of the heaviest chain
	%%
	%% Height	Block/Status
	%%
	%% 1		B2/on_chain 
	%%					  \ 
	%% 0					B1/on_chain
	remove(bcache_test, block_id(B1_2)),
	?assertEqual(not_found, get(bcache_test, block_id(B1_2))),
	?assertEqual(B1, get_by_solution_hash(bcache_test, B1#block.hash,
			crypto:strong_rand_bytes(32), 0, 0)),
	assert_longest_chain([B2, B1], 0),
	assert_tip(block_id(B2)),
	assert_max_cdiff({1, block_id(B2)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),

	%% Height	Block/Status
	%%
	%% 1		B2/on_chain 
	%%					  \ 
	%% 0					B1/on_chain
	prune(bcache_test, 1),
	?assertEqual(B1, get(bcache_test, block_id(B1))),
	?assertEqual(B1, get_by_solution_hash(bcache_test, B1#block.hash,
			crypto:strong_rand_bytes(32), 0, 0)),
	assert_longest_chain([B2, B1], 0),
	assert_tip(block_id(B2)),
	assert_max_cdiff({1, block_id(B2)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),

	%% Height	Block/Status
	%%
	%% 1		B2/on_chain 
	prune(bcache_test, 0),
	?assertEqual(not_found, get(bcache_test, block_id(B1))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B1#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2], 0),
	assert_tip(block_id(B2)),
	assert_max_cdiff({1, block_id(B2)}),
	assert_is_valid_fork(true, on_chain, B2),

	prune(bcache_test, 0),
	?assertEqual(not_found, get(bcache_test, block_id(B1_2))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B1_2#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2], 0),
	assert_tip(block_id(B2)),
	assert_max_cdiff({1, block_id(B2)}),
	assert_is_valid_fork(true, on_chain, B2),

	%% B1_2->B1 fork is the heaviest, but only B1 is validated. B2_2->B2->B1 is longer but
	%% has a lower cdiff.
	%%
	%% Height	Block/Status
	%%
	%% 2		B2_2/not_validated
	%%               |
	%% 1		B2/on_chain      B1_2/not_validated
	%%					  \             /
	%% 0					B1/on_chain
	new(bcache_test, B1),
	add(bcache_test, B1_2),
	add(bcache_test, B2),
	mark_tip(bcache_test, block_id(B2)),
	add(bcache_test, B2_2 = on_top(random_block(1), B2)),
	?assertMatch({B1_2, [B1], {{not_validated, ExpectedStatus}, _Timestamp}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B1], 0),
	assert_tip(block_id(B2)),
	assert_max_cdiff({2, block_id(B1_2)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B1_2),
	assert_is_valid_fork(true, not_validated, B2_2),

	%% B2_3->B2_2->B2->B1 is no longer and heavier but only B2->B1 are validated.
	%%
	%% Height	Block/Status
	%%
	%% 3		B2_3/not_validated
	%%               |
	%% 2		B2_2/not_validated
	%%               |
	%% 1		B2/on_chain      B1_2/not_validated
	%%					  \             /
	%% 0					B1/on_chain
	add(bcache_test, B2_3 = on_top(random_block(3), B2_2)),
	?assertMatch({B2_2, [B2], {{not_validated, ExpectedStatus}, _Timestamp}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	?assertException(error, invalid_tip, mark_tip(bcache_test, block_id(B2_3))),
	assert_longest_chain([B2, B1], 0),
	assert_tip(block_id(B2)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B1_2),
	assert_is_valid_fork(true, not_validated, B2_2),
	assert_is_valid_fork(true, not_validated, B2_3),

	%% Now B2_2->B2->B1 are validated.
	%%
	%% Height	Block/Status
	%%
	%% 3		B2_3/not_validated
	%%               |
	%% 2		B2_2/validated
	%%               |
	%% 1		B2/on_chain      B1_2/not_validated
	%%					  \             /
	%% 0					B1/on_chain
	add_validated(bcache_test, B2_2),
	?assertMatch({B2_2, {validated, _}},
			get_block_and_status(bcache_test, B2_2#block.indep_hash)),
	?assertMatch({B2_3, [B2_2, B2], {{not_validated, ExpectedStatus}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B2_2, B2, B1], 1),
	assert_tip(block_id(B2)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B1_2),
	assert_is_valid_fork(true, validated, B2_2),
	assert_is_valid_fork(true, not_validated, B2_3),

	%% Now the B3->B2->B1 fork is heaviest
	%%
	%% Height	Block/Status
	%%
	%% 3		B2_3/not_validated
	%%               |
	%% 2		B2_2/validated          B3/on_chain
	%%                    \            /
	%% 1                   B2/on_chain      B1_2/not_validated
	%%			                    \           /
	%% 0                             B1/on_chain
	B3 = on_top(random_block(4), B2),
	B3ID = block_id(B3),
	add(bcache_test, B3),
	add_validated(bcache_test, B3),
	mark_tip(bcache_test, B3ID),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B3, B2, B1], 0),
	assert_tip(block_id(B3)),
	assert_max_cdiff({4, block_id(B3)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B1_2),
	assert_is_valid_fork(true, validated, B2_2),
	assert_is_valid_fork(true, not_validated, B2_3),
	assert_is_valid_fork(true, on_chain, B3),
	?assertEqual([B2_2], get_siblings(bcache_test, B3)),
	?assertEqual([B3], get_siblings(bcache_test, B2_2)),
	?assertEqual([B2], get_siblings(bcache_test, B1_2)),

	%% B3->B2->B1 fork is still heaviest
	%%
	%% Height	Block/Status
	%%
	%% 3		B2_3/not_validated
	%%               |
	%% 2		B2_2/on_chain        B3/validated
	%%                    \            /
	%% 1                   B2/on_chain      B1_2/not_validated
	%%			                    \           /
	%% 0                             B1/on_chain
	mark_tip(bcache_test, block_id(B2_2)),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B3, B2, B1], 1),
	assert_tip(block_id(B2_2)),
	assert_max_cdiff({4, block_id(B3)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B1_2),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, not_validated, B2_3),
	assert_is_valid_fork(true, validated, B3),

	%% Height	Block/Status
	%%
	%% 3		B2_3/not_validated   B4/not_validated
	%%               |                     |
	%% 2		B2_2/on_chain        B3/validated
	%%                    \            /
	%% 1                   B2/on_chain      B1_2/not_validated
	%%			                    \           /
	%% 0                             B1/on_chain
	add(bcache_test, B4 = on_top(random_block(5), B3)),
	?assertMatch({B4, [B3, B2], {{not_validated, ExpectedStatus}, _Timestamp}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B3, B2, B1], 1),
	assert_tip(block_id(B2_2)),
	assert_max_cdiff({5, block_id(B4)}),
	assert_is_valid_fork(true, on_chain, B1),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, not_validated, B1_2),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, not_validated, B2_3),
	assert_is_valid_fork(true, validated, B3),
	assert_is_valid_fork(true, not_validated, B4),

	%% Height	Block/Status
	%%
	%% 3		B2_3/not_validated   B4/not_validated
	%%               |                     |
	%% 2		B2_2/on_chain        B3/validated
	%%                    \            /
	%% 1                   B2/on_chain
	prune(bcache_test, 1),
	?assertEqual(not_found, get(bcache_test, block_id(B1))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B1#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B3, B2], 1),
	assert_tip(block_id(B2_2)),
	assert_max_cdiff({5, block_id(B4)}),
	assert_is_valid_fork(true, on_chain, B2),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, not_validated, B2_3),
	assert_is_valid_fork(true, validated, B3),
	assert_is_valid_fork(true, not_validated, B4),
	?assertEqual([], get_siblings(bcache_test, B2_3)),
	?assertEqual([], get_siblings(bcache_test, B4)),

	%% Height	Block/Status
	%%
	%% 3		B2_3/on_chain
	%%               |
	%% 2		B2_2/on_chain
	mark_tip(bcache_test, block_id(B2_3)),
	prune(bcache_test, 1),
	?assertEqual(not_found, get(bcache_test, block_id(B2))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B2#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2_3, B2_2], 0),
	assert_tip(block_id(B2_3)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, on_chain, B2_3),

	%% Height	Block/Status
	%%
	%% 3		B2_3/on_chain
	%%               |
	%% 2		B2_2/on_chain
	prune(bcache_test, 1),
	?assertEqual(not_found, get(bcache_test, block_id(B3))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B3#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2_3, B2_2], 0),
	assert_tip(block_id(B2_3)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, on_chain, B2_3),

	%% Height	Block/Status
	%%
	%% 3		B2_3/on_chain
	%%               |
	%% 2		B2_2/on_chain
	prune(bcache_test, 1),
	?assertEqual(not_found, get(bcache_test, block_id(B4))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B4#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2_3, B2_2], 0),
	assert_tip(block_id(B2_3)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, on_chain, B2_3),

	%% Height	Block/Status
	%%
	%% 3		B2_3/on_chain
	%%               |
	%% 2		B2_2/on_chain
	prune(bcache_test, 1),
	?assertEqual(B2_2, get(bcache_test, block_id(B2_2))),
	?assertEqual(B2_2, get_by_solution_hash(bcache_test, B2_2#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2_3, B2_2], 0),
	assert_tip(block_id(B2_3)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, on_chain, B2_3),

	%% Height	Block/Status
	%%
	%% 3		B2_3/on_chain
	%%               |
	%% 2		B2_2/on_chain
	prune(bcache_test, 1),
	?assertEqual(B2_3, get(bcache_test, block_id(B2_3))),
	?assertEqual(B2_3, get_by_solution_hash(bcache_test, B2_3#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2_3, B2_2], 0),
	assert_tip(block_id(B2_3)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, on_chain, B2_3),

	%% Height	Block/Status
	%%
	%% 3		B2_3/on_chain
	%%               |
	%% 2		B2_2/on_chain
	remove(bcache_test, block_id(B3)),
	?assertEqual(not_found, get(bcache_test, block_id(B3))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B3#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2_3, B2_2], 0),
	assert_tip(block_id(B2_3)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, on_chain, B2_3),

	%% Height	Block/Status
	%%
	%% 3		B2_3/on_chain
	%%               |
	%% 2		B2_2/on_chain
	remove(bcache_test, block_id(B3)),
	?assertEqual(not_found, get(bcache_test, block_id(B4))),
	?assertEqual(not_found, get_by_solution_hash(bcache_test, B4#block.hash, <<>>, 0, 0)),
	assert_longest_chain([B2_3, B2_2], 0),
	assert_tip(block_id(B2_3)),
	assert_max_cdiff({3, block_id(B2_3)}),
	assert_is_valid_fork(true, on_chain, B2_2),
	assert_is_valid_fork(true, on_chain, B2_3),

	%% Height	Block/Status
	%%
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	new(bcache_test, B11 = random_block(0)),
	add(bcache_test, B12 = on_top(random_block(1), B11)),
	add_validated(bcache_test, B13 = on_top(random_block(1), B11)),
	mark_tip(bcache_test, block_id(B13)),
	%% Although the first block at height 1 was the one added in B12, B13 then
	%% became the tip so we should not reorganize.
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(bcache_test)),
	%% The longest chain starts a the max_cdiff block which in this case is B12 since B13
	%% was added second and has the same cdiff. So the longest chain stays as just [B11]
	assert_longest_chain([B11], 0),
	assert_tip(block_id(B13)),
	assert_max_cdiff({1, block_id(B12)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),

	%% Height	Block/Status
	%%
	%% 2							 B14/not_validated
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	add(bcache_test, B14 = on_top(random_block_after_repacking(2), B13)),
	?assertMatch({B14, [B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B13, B11], 0),
	assert_tip(block_id(B13)),
	assert_max_cdiff({2, block_id(B14)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, not_validated, B14),

	%% Height	Block/Status
	%%
	%% 2							 B14/not_validated
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	mark_nonce_limiter_validated(bcache_test, crypto:strong_rand_bytes(48)),
	mark_nonce_limiter_validated(bcache_test, block_id(B13)),
	?assertMatch({B13, {on_chain, _}},
			get_block_and_status(bcache_test, block_id(B13))),
	?assertMatch({B14, {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_block_and_status(bcache_test, block_id(B14))),
	assert_longest_chain([B13, B11], 0),
	assert_tip(block_id(B13)),
	assert_max_cdiff({2, block_id(B14)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, not_validated, B14),

	%% Height	Block/Status
	%%
	%% 2							 B14/not_validated
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	?assertMatch({B14, {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_block_and_status(bcache_test, block_id(B14))),
	?assertMatch({B14, [B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B13, B11], 0),
	assert_tip(block_id(B13)),
	assert_max_cdiff({2, block_id(B14)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, not_validated, B14),

	%% Height	Block/Status
	%%
	%% 2							 B14/nonce_limiter_validated
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	mark_nonce_limiter_validated(bcache_test, block_id(B14)),
	?assertMatch({B14, {{not_validated, nonce_limiter_validated}, _}},
			get_block_and_status(bcache_test, block_id(B14))),
	?assertMatch({B14, [B13], {{not_validated, nonce_limiter_validated}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	%% Longest chain now includes B14 because its status changed to nonce_limiter_validated
	assert_longest_chain([B14, B13, B11], 1),
	assert_tip(block_id(B13)),
	assert_max_cdiff({2, block_id(B14)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, not_validated, B14),

	%% Height	Block/Status
	%%
	%% 3							 B15/not_validated
	%%									  |
	%% 2							 B14/nonce_limiter_validated
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	add(bcache_test, B15 = on_top(random_block_after_repacking(3), B14)),
	?assertMatch({B14, [B13], {{not_validated, nonce_limiter_validated}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B14, B13, B11], 1),
	assert_tip(block_id(B13)),
	assert_max_cdiff({3, block_id(B15)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, not_validated, B14),
	assert_is_valid_fork(true, not_validated, B15),

	%% Height	Block/Status
	%%
	%% 3							 B15/not_validated
	%%									  |
	%% 2							 B14/validated
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	add_validated(bcache_test, B14),
	?assertMatch({B15, [B14, B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	?assertMatch({B14, {validated, _}}, get_block_and_status(bcache_test, block_id(B14))),
	assert_longest_chain([B14, B13, B11], 1),
	assert_tip(block_id(B13)),
	assert_max_cdiff({3, block_id(B15)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, validated, B14),
	assert_is_valid_fork(true, not_validated, B15),

	%% Height	Block/Status
	%%
	%% 3							 B16/not_validated
	%%									  |
	%% 3							 B15/not_validated
	%%									  |
	%% 2							 B14/validated
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	add(bcache_test, B16 = on_top(random_block_after_repacking(4), B15)),
	?assertMatch({B15, [B14, B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B14, B13, B11], 1),
	assert_tip(block_id(B13)),
	assert_max_cdiff({4, block_id(B16)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, validated, B14),
	assert_is_valid_fork(true, not_validated, B15),
	assert_is_valid_fork(true, not_validated, B16),	

	%% Height	Block/Status
	%%
	%% 3							 B16/nonce_limiter_validated
	%%									  |
	%% 3							 B15/not_validated
	%%									  |
	%% 2							 B14/validated
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	mark_nonce_limiter_validated(bcache_test, block_id(B16)),
	?assertMatch({B15, [B14, B13], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	?assertMatch({B16, {{not_validated, nonce_limiter_validated}, _}},
			get_block_and_status(bcache_test, block_id(B16))),
	assert_longest_chain([B14, B13, B11], 1),
	assert_tip(block_id(B13)),
	assert_max_cdiff({4, block_id(B16)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, validated, B14),
	assert_is_valid_fork(true, not_validated, B15),
	assert_is_valid_fork(true, not_validated, B16),		

	%% Height	Block/Status
	%%
	%% 3							 B16/nonce_limiter_validated
	%%									  |
	%% 3							 B15/not_validated
	%%									  |
	%% 2							 B14/on_chain
	%%									  |
	%% 1		B12/not_validated    B13/on_chain
	%%					  \              /
	%% 0					B11/on_chain
	mark_tip(bcache_test, block_id(B14)),
	?assertMatch({B14, {on_chain, _}}, get_block_and_status(bcache_test, block_id(B14))),
	?assertMatch({B15, [B14], {{not_validated, awaiting_nonce_limiter_validation}, _}},
			get_earliest_not_validated_from_longest_chain(bcache_test)),
	assert_longest_chain([B14, B13, B11], 0),
	assert_tip(block_id(B14)),
	assert_max_cdiff({4, block_id(B16)}),
	assert_is_valid_fork(true, on_chain, B11),
	assert_is_valid_fork(true, not_validated, B12),
	assert_is_valid_fork(true, on_chain, B13),
	assert_is_valid_fork(true, on_chain, B14),
	assert_is_valid_fork(true, not_validated, B15),
	assert_is_valid_fork(true, not_validated, B16),		

	ets:delete(bcache_test).

assert_longest_chain(Chain, NotOnChainCount) ->
	ExpectedPairs =  [{B#block.indep_hash, []} || B <- Chain],
	?assertEqual({ExpectedPairs, NotOnChainCount}, get_longest_chain_cache(bcache_test)).

assert_max_cdiff(ExpectedMaxCDiff) ->
	[{_, MaxCDiff}] = ets:lookup(bcache_test, max_cdiff),
	?assertEqual(ExpectedMaxCDiff, MaxCDiff).

assert_is_valid_fork(ExpectedFork, ExpectedStatus, B) ->
	[{_, {_, Status, _, _}}] = ets:lookup(bcache_test, {block, block_id(B)}),
	case ExpectedStatus of
		not_validated ->
			?assertMatch({not_validated, _}, Status);
		_ ->
			?assertEqual(ExpectedStatus, Status)
	end,
	?assertEqual(ExpectedFork, is_valid_fork(bcache_test, B, Status)).	

assert_tip(ExpectedTip) ->
	[{_, Tip}] = ets:lookup(bcache_test, tip),
	?assertEqual(ExpectedTip,Tip).

random_block(CDiff) ->
	#block{ indep_hash = crypto:strong_rand_bytes(48), height = 0, cumulative_diff = CDiff,
			hash = crypto:strong_rand_bytes(32) }.

random_block_after_repacking(CDiff) ->
	#block{ indep_hash = crypto:strong_rand_bytes(48), height = 0, cumulative_diff = CDiff,
			hash = crypto:strong_rand_bytes(32) }.

block_id(#block{ indep_hash = H }) ->
	H.

on_top(B, PrevB) ->
	B#block{ previous_block = PrevB#block.indep_hash, height = PrevB#block.height + 1,
			previous_cumulative_diff = PrevB#block.cumulative_diff }.
