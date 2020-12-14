%%% @doc The module maintains a DAG of block shadows that have passed the PoW validation.
-module(ar_block_cache).

-export([
	new/1, from_list/1,
	add/2, add_validated/2, mark_tip/2,
	get/2, get_earliest_not_validated_from_longest_chain/1, get_block_and_status/2,
	remove/2, prune/2
]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Public API.
%%%===================================================================

%% @doc Create a cache, initialize it with the given block. The block is marked as on-chain
%% and as a tip block.
new(#block{ indep_hash = H, cumulative_diff = CDiff, height = Height } = B) ->
	{#{ H => {B, on_chain, sets:new()} }, {CDiff, H}, gb_sets:from_list([{Height, H}]), H}.

%% @doc Initialize a cache from the given list of validated blocks. Mark the latest
%% block as the tip block. The given blocks must be sorted from newest to oldest.
from_list([B]) ->
	new(B);
from_list([#block{ indep_hash = H } = B | Blocks]) ->
	mark_tip(add_validated(from_list(Blocks), B), H).

%% @doc Add a block shadow to the cache. The block is marked as not validated yet.
%% If the block already exists in the cache, it is overwritten, its status does not change.
%% Although the block can be overwritten, the function assumes the height, hash, previous hash,
%% and the cumulative difficulty do not change.
add(Cache,
		#block{
			indep_hash = H,
			previous_block = PrevH,
			cumulative_diff = CDiff,
			height = Height
		} = B) ->
	{Map, {MaxCDiff, _H} = C, Set, Tip} = Cache,
	case maps:get(H, Map, not_found) of
		not_found ->
			Map2 = maps:put(H, {B, not_validated, sets:new()}, Map),
			{PrevB, Status, Children} = maps:get(PrevH, Map),
			Map3 = maps:put(PrevH, {PrevB, Status, sets:add_element(H, Children)}, Map2),
			C2 = case CDiff > MaxCDiff of true -> {CDiff, H}; false -> C end,
			Set2 = gb_sets:insert({Height, H}, Set),
			{Map3, C2, Set2, Tip};
		{_B, Status, Children} ->
			Map2 = maps:put(H, {B, Status, Children}, Map),
			{Map2, C, Set, Tip}
	end.

%% @doc Add a validated block to the cache. If the block is already in the cache, it
%% is overwritten. However, the function assumes the height, hash, previous hash, and
%% the cumulative difficulty do not change.
%% Raises previous_block_not_found if the previous block is not in the cache.
%% Raises previous_block_not_validated if the previous block is not validated.
add_validated(Cache, #block{ indep_hash = H, previous_block = PrevH, height = Height } = B) ->
	{Map, {MaxCDiff, _H} = C, Set, Tip} = Cache,
	case maps:get(PrevH, Map, not_found) of
		not_found ->
			error(previous_block_not_found);
		{_PrevB, not_validated, _Children} ->
			error(previous_block_not_validated);
		{PrevB, Status, PrevChildren} ->
			Map2 = maps:put(PrevH, {PrevB, Status, sets:add_element(H, PrevChildren)}, Map),
			{Map3, C2, Set2} =
				case maps:get(H, Map2, not_found) of
					not_found ->
						CDiff = B#block.cumulative_diff,
						{maps:put(H, {B, validated, sets:new()}, Map2),
							case CDiff > MaxCDiff of true -> {CDiff, H}; false -> C end,
							gb_sets:insert({Height, H}, Set)};
					{_B, on_chain, Children} ->
						{maps:put(H, {B, on_chain, Children}, Map2), C, Set};
					{_B, _Status, Children} ->
						{maps:put(H, {B, validated, Children}, Map2), C, Set}
				end,
			{Map3, C2, Set2, Tip}
	end.

%% @doc Get the block shadow from cache. Returns not_found if the block is not in cache.
get(Cache, H) ->
	element(1, maps:get(H, element(1, Cache), {not_found, not_found, not_found})).

%% @doc Get a {block shadow, previous block shadows} pair for the earliest block from
%% the longest chain, which has not been validated yet. The previous block shadows are
%% sorted from newest to oldest. The last one is a block shadow from the current fork.
get_earliest_not_validated_from_longest_chain(Cache) ->
	{Map, {_CDiff, H}, _Set, _Tip} = Cache,
	{B, Status, _Children} = maps:get(H, Map),
	case Status of
		not_validated ->
			get_earliest_not_validated(B, Map);
		_ ->
			not_found
	end.

%% @doc Get the block shadow and its status from cache.
%% Returns not_found if the block is not in cache.
get_block_and_status(Cache, H) ->
	{B, Status, _Children} = maps:get(H, element(1, Cache), {not_found, not_found, not_found}),
	{B, Status}.

%% @doc Mark the given block as the tip block. Mark the previous blocks as on-chain.
%% Mark the on-chain blocks from other forks as validated. Raises invalid_tip if
%% one of the preceeding blocks is not validated. Raises not_found if the block
%% is not found.
mark_tip(Cache, H) ->
	{Map, C, Set, _Tip} = Cache,
	case maps:get(H, Map, not_found) of
		{B, _Status, Children} ->
			Tip2 = H,
			Map2 = maps:put(H, {B, on_chain, Children}, Map),
			Map3 = mark_on_chain(B, Map2),
			{Map3, C, Set, Tip2};
		not_found ->
			error(not_found)
	end.

%% @doc Remove the block and all the blocks on top from the cache.
%% Raises cannot_remove_tip if the tip is removed.
%% Returns the cache as-is if the given block is not found.
remove(Cache, H) ->
	case remove2(H, Cache) of
		{Map, not_set, Set, Tip} ->
			C = find_max_cdiff(Set, Map),
			{Map, C, Set, Tip};
		Cache2 ->
			Cache2
	end.

%% @doc Prune the cache. Keep the blocks no deeper than the given prune depth from the tip.
%% Raises cannot_remove_tip if the tip is removed.
prune(Cache, Depth) ->
	{Map, _C, _Set, Tip} = Cache,
	{#block{ height = Height }, _Status, _Children} = maps:get(Tip, Map),
	prune(Cache, Depth, Height).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_earliest_not_validated(#block{ previous_block = PrevH } = B, Map) ->
	{PrevB, Status, _Children} = maps:get(PrevH, Map),
	case Status of
		not_validated ->
			get_earliest_not_validated(PrevB, Map);
		_ ->
			{B, get_fork_blocks(B, Map)}
	end.

get_fork_blocks(#block{ previous_block = PrevH }, Map) ->
	{PrevB, Status, _Children} = maps:get(PrevH, Map),
	case Status of
		on_chain ->
			[PrevB];
		_ ->
			[PrevB | get_fork_blocks(PrevB, Map)]
	end.

mark_on_chain(#block{ previous_block = PrevH, indep_hash = H }, Map) ->
	case maps:get(PrevH, Map) of
		{_PrevB, not_validated, _Children} ->
			error(invalid_tip);
		{_PrevB, on_chain, Children} ->
			%% Mark the blocks from the previous main fork as validated, not on-chain.
			mark_off_chain(sets:del_element(H, Children), Map);
		{PrevB, validated, Children} ->
			mark_on_chain(PrevB, maps:put(PrevH, {PrevB, on_chain, Children}, Map))
	end.

mark_off_chain(Set, Map) ->
	sets:fold(
		fun(H, Acc) ->
			case maps:get(H, Acc) of
				{B, on_chain, Children} ->
					mark_off_chain(Children, maps:put(H, {B, validated, Children}, Acc));
				_ ->
					Acc
			end
		end,
		Map,
		Set
	).

remove2(H, Cache) ->
	{Map, C, Set, Tip} = Cache,
	case maps:get(H, Map, not_found) of
		not_found ->
			Cache;
		{#block{ indep_hash = Tip }, _Status, _Children} ->
			error(cannot_remove_tip);
		{#block{ height = Height, previous_block = PrevH }, _Status, Children} ->
			C2 = case C of {_, H} -> not_set; _ -> C end,
			Map2 = maps:remove(H, Map),
			Map3 =
				case maps:get(PrevH, Map2, not_found) of
					not_found ->
						Map2;
					{PrevB, Status, PrevChildren} ->
						maps:put(PrevH, {PrevB, Status, sets:del_element(H, PrevChildren)}, Map2)
				end,
			Set2 = gb_sets:del_element({Height, H}, Set),
			sets:fold(
				fun(Child, Acc) ->
					remove2(Child, Acc)
				end,
				{Map3, C2, Set2, Tip},
				Children
			)
	end.

find_max_cdiff(Set, Map) ->
	gb_sets:fold(
		fun ({_Height, H}, not_set) ->
				CDiff = (element(1, maps:get(H, Map)))#block.cumulative_diff,
				{CDiff, H};
			({_Height, H}, {MaxCDiff, _CH} = Acc) ->
				CDiff = (element(1, maps:get(H, Map)))#block.cumulative_diff,
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

prune(Cache, Depth, TipHeight) ->
	{Map, C, Set, Tip} = Cache,
	case gb_sets:is_empty(Set) of
		true ->
			Cache;
		false ->
			{{Height, H}, Set2} = gb_sets:take_smallest(Set),
			case Height >= TipHeight - Depth of
				true ->
					Cache;
				false ->
					%% The lowest block must be on-chain by construction.
					{_B, on_chain, Children} = maps:get(H, Map),
					{Map2, C2, Set3, Tip} = sets:fold(
						fun(Child, Acc) ->
							{_ChildB, Status, _Children} = maps:get(Child, Map),
							case Status of
								on_chain ->
									Acc;
								_ ->
									remove(Acc, Child)
							end
						end,
						{Map, C, Set2, Tip},
						Children
					),
					Map3 = maps:remove(H, Map2),
					prune({Map3, C2, Set3, Tip}, Depth, TipHeight)
			end
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

block_cache_test() ->
	C1 = new(B1 = random_block(0)),
	?assertEqual(not_found, get(C1, crypto:strong_rand_bytes(32))),
	?assertEqual(B1, get(C1, block_id(B1))),
	?assertEqual(B1#block{ txs = [<<>>] }, get(add(C1, B1#block{ txs = [<<>>] }), block_id(B1))),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(C1)),
	C2 = add(C1, B2 = on_top(random_block(1), B1)),
	?assertEqual({B2, [B1]}, get_earliest_not_validated_from_longest_chain(C2)),
	?assertException(error, cannot_remove_tip, remove(C2, block_id(B1))),
	?assertEqual(not_found, get(remove(C2, block_id(B2)), block_id(B2))),
	?assertEqual(B1, get(remove(C2, block_id(B2)), block_id(B1))),
	C3 = mark_tip(add(C2, B1_2 = on_top(random_block(2), B1)), block_id(B2)),
	?assertEqual(B1_2, get(C3, block_id(B1_2))),
	?assertEqual(not_found, get(remove(C3, block_id(B1_2)), block_id(B1_2))),
	?assertEqual(B1, get(prune(C3, 1), block_id(B1))),
	?assertEqual(not_found, get(prune(C3, 0), block_id(B1))),
	?assertEqual(not_found, get(prune(C3, 0), block_id(B1_2))),
	C4 = add(C3, B2_2 = on_top(random_block(1), B2)),
	?assertEqual({B1_2, [B1]}, get_earliest_not_validated_from_longest_chain(C4)),
	C5 = add(C4, B2_3 = on_top(random_block(3), B2_2)),
	?assertEqual({B2_2, [B2]}, get_earliest_not_validated_from_longest_chain(C5)),
	?assertException(error, invalid_tip, mark_tip(C5, block_id(B2_3))),
	C6 = add_validated(C5, B2_2),
	?assertEqual({B2_3, [B2_2, B2]}, get_earliest_not_validated_from_longest_chain(C6)),
	B3 = on_top(random_block(4), B2),
	B3ID = block_id(B3),
	C7 = mark_tip(add_validated(add(C6, B3), B3), B3ID),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(C7)),
	C8 = mark_tip(C7, block_id(B2_2)),
	?assertEqual(not_found, get_earliest_not_validated_from_longest_chain(C8)),
	C9 = add(C8, B4 = on_top(random_block(5), B3)),
	?assertEqual({B4, [B3, B2]}, get_earliest_not_validated_from_longest_chain(C9)),
	?assertEqual(not_found, get(prune(C9, 1), block_id(B1))),
	C10 = mark_tip(C9, block_id(B2_3)),
	?assertEqual(not_found, get(prune(C10, 1), block_id(B2))),
	?assertEqual(not_found, get(prune(C10, 1), block_id(B3))),
	?assertEqual(not_found, get(prune(C10, 1), block_id(B4))),
	?assertEqual(B2_2, get(prune(C10, 1), block_id(B2_2))),
	?assertEqual(B2_3, get(prune(C10, 1), block_id(B2_3))),
	?assertEqual(not_found, get(remove(C10, block_id(B3)), block_id(B3))),
	?assertEqual(not_found, get(remove(C10, block_id(B3)), block_id(B4))).

random_block(CDiff) ->
	#block{ indep_hash = crypto:strong_rand_bytes(32), height = 0, cumulative_diff = CDiff }.

block_id(#block{ indep_hash = H }) ->
	H.

on_top(B, PrevB) ->
	B#block{ previous_block = PrevB#block.indep_hash, height = PrevB#block.height + 1 }.
