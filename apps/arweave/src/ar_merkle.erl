%%% @doc Generates annotated merkle trees, paths inside those trees, as well
%%% as verification of those proofs.
-module(ar_merkle).

-export([generate_tree/1, generate_path/3, validate_path/4, validate_path/5,
		extract_note/1, extract_root/1]).

-export([get/2, hash/1, note_to_binary/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% @doc Generates annotated merkle trees, paths inside those trees, as well
%%% as verification of those proofs.
-record(node, {
	id,
	type = branch,	% root | branch | leaf
	data,			% The value (for leaves).
	note,			% The offset, a number less than 2^256.
	left,			% The (optional) ID of a node to the left.
	right,			% The (optional) ID of a node to the right.
	max,			% The maximum observed note at this point.
	is_rebased = false
}).

-define(HASH_SIZE, ?CHUNK_ID_HASH_SIZE).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Generate a Merkle tree from a list of pairs of IDs (of length 32 bytes)
%% and labels -- offsets. The list may be arbitrarily nested - the inner lists then
%% contain the leaves of the sub trees with the rebased (on 0) starting offsets.
generate_tree(Elements) ->
	generate_tree(Elements, queue:new(), []).

%% @doc Generate a Merkle path for the given offset Dest from the tree Tree
%% with the root ID.
generate_path(ID, Dest, Tree) ->
	binary:list_to_bin(generate_path_parts(ID, Dest, Tree, 0)).

%% @doc Validate the given merkle path.
validate_path(ID, Dest, RightBound, Path) ->
	validate_path(ID, Dest, RightBound, Path, basic_ruleset).

%% @doc Validate the given merkle path using the given set of rules.
validate_path(ID, Dest, RightBound, _Path, _Ruleset) when RightBound =< 0 ->
	?LOG_ERROR([{event, validate_path_called_with_non_positive_right_bound},
			{root, ar_util:encode(ID)}, {dest, Dest}, {right_bound, RightBound}]),
	throw(invalid_right_bound);
validate_path(ID, Dest, RightBound, Path, Ruleset) when Dest >= RightBound ->
	validate_path(ID, RightBound - 1, RightBound, Path, Ruleset);
validate_path(ID, Dest, RightBound, Path, Ruleset) when Dest < 0 ->
	validate_path(ID, 0, RightBound, Path, Ruleset);
validate_path(ID, Dest, RightBound, Path, Ruleset) ->
	validate_path(ID, Dest, 0, RightBound, Path, Ruleset).

validate_path(ID, Dest, LeftBound, RightBound, Path, basic_ruleset) ->
	CheckBorders = false,
	CheckSplit = false,
	AllowRebase = false,
	validate_path(ID, Dest, LeftBound, RightBound, Path, CheckBorders, CheckSplit, AllowRebase);

validate_path(ID, Dest, LeftBound, RightBound, Path, strict_borders_ruleset) ->
	CheckBorders = true,
	CheckSplit = false,
	AllowRebase = false,
	validate_path(ID, Dest, LeftBound, RightBound, Path, CheckBorders, CheckSplit, AllowRebase);

validate_path(ID, Dest, LeftBound, RightBound, Path, strict_data_split_ruleset) ->
	CheckBorders = true,
	CheckSplit = strict,
	AllowRebase = false,
	validate_path(ID, Dest, LeftBound, RightBound, Path, CheckBorders, CheckSplit, AllowRebase);

validate_path(ID, Dest, LeftBound, RightBound, Path, offset_rebase_support_ruleset) ->
	CheckBorders = true,
	CheckSplit = relaxed,
	AllowRebase = true,
	validate_path(ID, Dest, LeftBound, RightBound, Path, CheckBorders, CheckSplit, AllowRebase).


validate_path(ID, Dest, LeftBound, RightBound, Path, CheckBorders, CheckSplit, AllowRebase) ->
	DataSize = RightBound,
	%% Will be set to true only if we only take right branches from the root to the leaf. In this
	%% case we know the leaf chunk is the final chunk in the range represented by the merkle tree.
	IsRightMostInItsSubTree = undefined, 
	%% Set to non-zero when AllowRebase is true and we begin processing a subtree.
	LeftBoundShift = 0,
	validate_path(ID, Dest, LeftBound, RightBound, Path,
		DataSize, IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, AllowRebase).

%% Validate the leaf of the merkle path (i.e. the data chunk)
validate_path(ID, _Dest, LeftBound, RightBound,
		<< Data:?HASH_SIZE/binary, EndOffset:(?NOTE_SIZE*8) >>,
		DataSize, IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, _AllowRebase) ->
	AreBordersValid = case CheckBorders of
		true ->
			%% Borders are only valid if every offset does not exceed the previous offset
			%% by more than ?DATA_CHUNK_SIZE
			EndOffset - LeftBound =< ?DATA_CHUNK_SIZE andalso
				RightBound - LeftBound =< ?DATA_CHUNK_SIZE;
		false ->
			%% Borders are always valid if we don't need to check them
			true
	end,
	IsSplitValid = case CheckSplit of
		strict ->
			ChunkSize = EndOffset - LeftBound,
			ValidateSplit =
				case validate_strict_split of
					_ when ChunkSize == (?DATA_CHUNK_SIZE) ->
						LeftBound rem (?DATA_CHUNK_SIZE) == 0;
					_ when EndOffset == DataSize ->
						Border = ar_util:floor_int(RightBound, ?DATA_CHUNK_SIZE),
						RightBound rem (?DATA_CHUNK_SIZE) > 0
								andalso LeftBound =< Border;
					_ when PathSize > ChunkSize ->
						false;
					_ ->
						LeftBound rem (?DATA_CHUNK_SIZE) == 0
								andalso DataSize - LeftBound > (?DATA_CHUNK_SIZE)
								andalso DataSize - LeftBound < 2 * (?DATA_CHUNK_SIZE)
				end,
			case ValidateSplit of
				false ->
					false;
				true ->
					%% The last chunk may either start at the bucket start or
					%% span two buckets.
					Bucket0 = ShiftedLeftBound div (?DATA_CHUNK_SIZE),
					Bucket1 = ShiftedEndOffset div (?DATA_CHUNK_SIZE),
					(ShiftedLeftBound rem (?DATA_CHUNK_SIZE) == 0)
							%% Make sure each chunk "steps" at least 1 byte into
							%% its own bucket, which is to the right from the right border
							%% cause since this chunk does not start at the left border,
							%% the bucket on the left from the right border belongs to
							%% the preceding chunk.
							orelse (Bucket0 + 1 == Bucket1
									andalso ShiftedEndOffset rem ?DATA_CHUNK_SIZE /= 0);
				_ ->
					%% May also be the only chunk of a single-chunk subtree.
					ShiftedLeftBound rem (?DATA_CHUNK_SIZE) == 0
			end;
		_ ->
			%% Split is always valid if we don't need to check it
			true
	end,
	case AreBordersValid andalso IsSplitValid of
		true ->
			validate_leaf(ID, Data, EndOffset, LeftBound, RightBound, LeftBoundShift);
		false ->
			false
	end;

%% Validate the given merkle path where any subtrees may have 0-based offset.
validate_path(ID, Dest, LeftBound, RightBound,
		<< 0:(?HASH_SIZE*8), L:?HASH_SIZE/binary, R:?HASH_SIZE/binary,
			Note:(?NOTE_SIZE*8), Rest/binary >>,
		DataSize, _IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, true) ->
	case hash([hash(L), hash(R), hash(note_to_binary(Note))]) of
		ID ->
			{Path, NextLeftBound, NextRightBound, Dest2, NextLeftBoundShift} =
				case Dest < Note of
					true ->
						Note2 = min(RightBound, Note),
						{L, 0, Note2 - LeftBound, Dest - LeftBound,
								LeftBoundShift + LeftBound};
					false ->
						Note2 = max(LeftBound, Note),
						{R, 0, RightBound - Note2,
								Dest - Note2,
								LeftBoundShift + Note2}
				end,
			validate_path(Path, Dest2, NextLeftBound, NextRightBound, Rest, DataSize,
				undefined, NextLeftBoundShift, CheckBorders, CheckSplit, true);
		_ ->
			false
	end;

%% Validate a non-leaf node in the merkle path
validate_path(ID, Dest, LeftBound, RightBound,
		<< L:?HASH_SIZE/binary, R:?HASH_SIZE/binary, Note:(?NOTE_SIZE*8), Rest/binary >>,
		DataSize, IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, AllowRebase) ->
	validate_node(ID, Dest, LeftBound, RightBound, L, R, Note, Rest,
		DataSize, IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, AllowRebase);

%% Invalid merkle path
validate_path(_, _, _, _, _, _, _, _, _, _, _) ->
	false.

validate_node(ID, Dest, LeftBound, RightBound, L, R, Note, RemainingPath,
		DataSize, IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, AllowRebase) ->
	case hash([hash(L), hash(R), hash(note_to_binary(Note))]) of
		ID ->
			{BranchID, NextLeftBound, NextRightBound, IsRightMostInItsSubTree2} =
				case Dest < Note of
					true ->
						%% Traverse left branch (at this point we know the leaf chunk will never
						%% be the right most in the subtree)
						{L, LeftBound, min(RightBound, Note), false};
					false ->
						%% Traverse right branch
						{R, max(LeftBound, Note), RightBound,
								case IsRightMostInItsSubTree of undefined -> true;
										_ -> IsRightMostInItsSubTree end}
				end,
			validate_path(BranchID, Dest, NextLeftBound, NextRightBound, RemainingPath,
				DataSize, IsRightMostInItsSubTree2, LeftBoundShift,
				CheckBorders, CheckSplit, AllowRebase);
		_ ->
			false
	end.

validate_leaf(ID, Data, EndOffset, LeftBound, RightBound, LeftBoundShift) ->
	case hash([hash(Data), hash(note_to_binary(EndOffset))]) of
		ID ->
			{Data, LeftBoundShift + LeftBound,
				LeftBoundShift + max(min(RightBound, EndOffset), LeftBound + 1)};
		_ ->
			false
	end.

%% @doc Get the note (offset) attached to the leaf from a path.
extract_note(Path) ->
	binary:decode_unsigned(
		binary:part(Path, byte_size(Path) - ?NOTE_SIZE, ?NOTE_SIZE)
	).

%% @doc Get the Merkle root from a path.
extract_root(<< Data:?HASH_SIZE/binary, EndOffset:(?NOTE_SIZE*8) >>) ->
	{ok, hash([hash(Data), hash(note_to_binary(EndOffset))])};
extract_root(<< L:?HASH_SIZE/binary, R:?HASH_SIZE/binary, Note:(?NOTE_SIZE*8), _/binary >>) ->
	{ok, hash([hash(L), hash(R), hash(note_to_binary(Note))])};
extract_root(_) ->
	{error, invalid_proof}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

generate_tree([Element | Elements], Stack, Tree) when is_list(Element) ->
	{SubRoot, SubTree} = generate_tree(Element),
	SubTree2 = [mark_rebased(Node, SubRoot) || Node <- SubTree],
	SubRootN = get(SubRoot, SubTree2),
	generate_tree(Elements, queue:in(SubRootN, Stack), Tree ++ SubTree2);
generate_tree([Element | Elements], Stack, Tree) ->
	Leaf = generate_leaf(Element),
	generate_tree(Elements, queue:in(Leaf, Stack), [Leaf | Tree]);
generate_tree([], Stack, Tree) ->
	case queue:to_list(Stack) of
		[] ->
			{<<>>, []};
		_ ->
			generate_all_rows(queue:to_list(Stack), Tree)
	end.

mark_rebased(#node{ id = RootID } = Node, RootID) ->
	Node#node{ is_rebased = true };
mark_rebased(Node, _RootID) ->
	Node.

generate_leaf({Data, Note}) ->
	Hash = hash([hash(Data), hash(note_to_binary(Note))]),
	#node{
		id = Hash,
		type = leaf,
		data = Data,
		note = Note,
		max = Note
	}.

%% Note: This implementation leaves some duplicates in the tree structure.
%% The produced trees could be a little smaller if these duplicates were 
%% not present, but removing them with ar_util:unique takes far too long.
generate_all_rows([RootN], Tree) ->
	RootID = RootN#node.id,
	{RootID, Tree};
generate_all_rows(Row, Tree) ->
	NewRow = generate_row(Row, 0),
	generate_all_rows(NewRow, NewRow ++ Tree).

generate_row([], _Shift) -> [];
generate_row([Left], _Shift) -> [Left];
generate_row([L, R | Rest], Shift) ->
	{N, Shift2} = generate_node(L, R, Shift),
	[N | generate_row(Rest, Shift2)].

generate_node(Left, empty, Shift) ->
	{Left, Shift};
generate_node(L, R, Shift) ->
	LMax = L#node.max,
	LMax2 = case L#node.is_rebased of true -> Shift + LMax; _ -> LMax end,
	RMax = R#node.max,
	RMax2 = case R#node.is_rebased of true -> LMax2 + RMax; _ -> RMax end,
	{#node{
		id = hash([hash(L#node.id), hash(R#node.id), hash(note_to_binary(LMax2))]),
		type = branch,
		left = L#node.id,
		right = R#node.id,
		note = LMax2,
		max = RMax2
	}, RMax2}.

generate_path_parts(ID, Dest, Tree, PrevNote) ->
	case get(ID, Tree) of
		N when N#node.type == leaf ->
			[N#node.data, note_to_binary(N#node.note)];
		N when N#node.type == branch ->
			Note = N#node.note,
			{Direction, NextID} =
				case Dest < Note of
					true ->
						{left, N#node.left};
					false ->
						{right, N#node.right}
				end,
			NextN = get(NextID, Tree),
			{RebaseMark, Dest2} =
				case {NextN#node.is_rebased, Direction} of
					{false, _} ->
						{<<>>, Dest};
					{true, right} ->
						{<< 0:(?HASH_SIZE * 8) >>, Dest - Note};
					{true, left} ->
						{<< 0:(?HASH_SIZE * 8) >>, Dest - PrevNote}
				end,
			[RebaseMark, N#node.left, N#node.right, note_to_binary(Note)
					| generate_path_parts(NextID, Dest2, Tree, Note)]
	end.

get(ID, Map) ->
	case lists:keyfind(ID, #node.id, Map) of
		false -> false;
		Node -> Node
	end.

note_to_binary(Note) ->
	<< Note:(?NOTE_SIZE * 8) >>.

hash(Parts) when is_list(Parts) ->
	crypto:hash(sha256, binary:list_to_bin(Parts));
hash(Binary) ->
	crypto:hash(sha256, Binary).

make_tags_cumulative(L) ->
	lists:reverse(
		element(2,
			lists:foldl(
				fun({X, Tag}, {AccTag, AccL}) ->
					Curr = AccTag + Tag,
					{Curr, [{X, Curr} | AccL]}
				end,
				{0, []},
				L
			)
		)
	).

%%%===================================================================
%%% Tests.
%%%===================================================================

-define(TEST_SIZE, 64 * 1024).
-define(UNEVEN_TEST_SIZE, 35643).
-define(UNEVEN_TEST_TARGET, 33271).

generate_and_validate_balanced_tree_path_test_() ->
	{timeout, 30, fun test_generate_and_validate_balanced_tree_path/0}.

test_generate_and_validate_balanced_tree_path() ->
	Tags = make_tags_cumulative([{<< N:256 >>, 1} || N <- lists:seq(0, ?TEST_SIZE - 1)]),
	{MR, Tree} = ar_merkle:generate_tree(Tags),
	?assertEqual(length(Tree), (?TEST_SIZE * 2) - 1),
	lists:foreach(
		fun(_TestCase) ->
			RandomTarget = rand:uniform(?TEST_SIZE) - 1,
			Path = ar_merkle:generate_path(MR, RandomTarget, Tree),
			{Leaf, StartOffset, EndOffset} =
				ar_merkle:validate_path(MR, RandomTarget, ?TEST_SIZE, Path),
			{Leaf, StartOffset, EndOffset} =
				ar_merkle:validate_path(MR, RandomTarget, ?TEST_SIZE, Path,
						strict_borders_ruleset),
			?assertEqual(RandomTarget, binary:decode_unsigned(Leaf)),
			?assert(RandomTarget < EndOffset),
			?assert(RandomTarget >= StartOffset)
		end,
		lists:seq(1, 100)
	).

generate_and_validate_tree_with_rebase_test_() ->
	[
		{timeout, 30, fun test_tree_with_rebase_shallow/0},
		{timeout, 30, fun test_tree_with_rebase_nested/0},
		{timeout, 30, fun test_tree_with_rebase_bad_paths/0},
		{timeout, 30, fun test_tree_with_rebase_partial_chunk/0},
		{timeout, 30, fun test_tree_with_rebase_subtree_ids/0}
	].

test_tree_with_rebase_shallow() ->
	Leaf1 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf2 = crypto:strong_rand_bytes(?HASH_SIZE),

	%%    Root1
	%%    /   \
	%% Leaf1  Leaf2 (with offset reset)
	Tags0 = [
		{Leaf1, ?DATA_CHUNK_SIZE},
		{Leaf2, 2 * ?DATA_CHUNK_SIZE}
	],
	{Root0, Tree0} = ar_merkle:generate_tree(Tags0),
	assert_tree([
			{branch, undefined, ?DATA_CHUNK_SIZE, false},
			{leaf, Leaf2, 2*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, false}
		], Tree0),

	Tags1 = [{Leaf1, ?DATA_CHUNK_SIZE}, [{Leaf2, ?DATA_CHUNK_SIZE}]],
	{Root1, Tree1} = ar_merkle:generate_tree(Tags1),
	assert_tree([
			{branch, undefined, ?DATA_CHUNK_SIZE, false},
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, false},
			{leaf, Leaf2, ?DATA_CHUNK_SIZE, true}
		], Tree1),
	?assertNotEqual(Root1, Root0),

	Path0_1 = ar_merkle:generate_path(Root0, 0, Tree0),
	Path1_1 = ar_merkle:generate_path(Root1, 0, Tree1),
	?assertNotEqual(Path0_1, Path1_1),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root0, 0, 2 * ?DATA_CHUNK_SIZE,
			Path0_1, offset_rebase_support_ruleset),	
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root1, 0, 2 * ?DATA_CHUNK_SIZE,
			Path1_1, offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(Root1, 0, 2 * ?DATA_CHUNK_SIZE,
			Path0_1, offset_rebase_support_ruleset)),
	?assertEqual(false, ar_merkle:validate_path(Root0, 0, 2 * ?DATA_CHUNK_SIZE,
			Path1_1, offset_rebase_support_ruleset)),

	Path0_2 = ar_merkle:generate_path(Root0, ?DATA_CHUNK_SIZE, Tree0),
	Path1_2 = ar_merkle:generate_path(Root1, ?DATA_CHUNK_SIZE, Tree1),
	?assertNotEqual(Path1_2, Path0_2),
	{Leaf2, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root0, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE, Path0_2, offset_rebase_support_ruleset),
	{Leaf2, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root1, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE, Path1_2, offset_rebase_support_ruleset),
	{Leaf2, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root1, 2 * ?DATA_CHUNK_SIZE - 1, 2 * ?DATA_CHUNK_SIZE, Path1_2, 
			offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(
			Root1, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE, Path0_2, offset_rebase_support_ruleset)),
	?assertEqual(false, ar_merkle:validate_path(
			Root0, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE, Path1_2, offset_rebase_support_ruleset)),
	?assertEqual(false, ar_merkle:validate_path(
			Root1, ?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE, Path1_1, offset_rebase_support_ruleset)),
	?assertEqual(false, ar_merkle:validate_path(
			Root1, 0, 2 * ?DATA_CHUNK_SIZE, Path1_2, offset_rebase_support_ruleset)),

	%%     ________Root2_________
	%%    /                      \
	%% Leaf1 (with offset reset)  Leaf2 (with offset reset)
	Tags2 = [
		[
			{Leaf1, ?DATA_CHUNK_SIZE}
		],
		[
			{Leaf2, ?DATA_CHUNK_SIZE}
		]
	],
	{Root2, Tree2} = ar_merkle:generate_tree(Tags2),
	assert_tree([
			{branch, undefined, ?DATA_CHUNK_SIZE, false},
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, true},
			{leaf, Leaf2, ?DATA_CHUNK_SIZE, true}
		], Tree2),

	Path2_1 = ar_merkle:generate_path(Root2, 0, Tree2),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root2, 0,
			2 * ?DATA_CHUNK_SIZE, Path2_1, offset_rebase_support_ruleset),

	Path2_2 = ar_merkle:generate_path(Root2, ?DATA_CHUNK_SIZE, Tree2),
	{Leaf2, ?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root2,
			?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE, Path2_2, offset_rebase_support_ruleset),
	
	{Leaf2, ?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root2,
			2*?DATA_CHUNK_SIZE - 1, 2*?DATA_CHUNK_SIZE, Path2_2, offset_rebase_support_ruleset),

	?assertEqual(false, ar_merkle:validate_path(Root2, ?DATA_CHUNK_SIZE,
			2*?DATA_CHUNK_SIZE, Path2_1, offset_rebase_support_ruleset)),
	?assertEqual(false, ar_merkle:validate_path(Root2, 0,
			2*?DATA_CHUNK_SIZE, Path2_2, offset_rebase_support_ruleset)).

test_tree_with_rebase_nested() ->
	%%                       _________________Root3________________
	%%                      /                                      \
	%%             _____SubTree1______________                    Leaf6
	%%            /                           \           
	%%       SubTree2              ________SubTree3_________
	%%       /       \           /                           \  
	%%    Leaf1    Leaf2     SubTree4 (with offset reset)   Leaf5
	%%                       /       \
	%%                    Leaf3    Leaf4 (with offset reset)
	Leaf1 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf2 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf3 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf4 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf5 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf6 = crypto:strong_rand_bytes(?HASH_SIZE),
	Tags3 = [
		{Leaf1, ?DATA_CHUNK_SIZE},
		{Leaf2, 2*?DATA_CHUNK_SIZE},
		[
			{Leaf3, ?DATA_CHUNK_SIZE},
			[
				{Leaf4, ?DATA_CHUNK_SIZE}
			]
		],
		{Leaf5, 5*?DATA_CHUNK_SIZE},
		{Leaf6, 6*?DATA_CHUNK_SIZE}
	],
	{Root3, Tree3} = ar_merkle:generate_tree(Tags3),
	assert_tree([
			{branch, undefined, 5*?DATA_CHUNK_SIZE, false},  %% Root
			{branch, undefined, 2*?DATA_CHUNK_SIZE, false},  %% SubTree1
			{leaf, Leaf6, 6*?DATA_CHUNK_SIZE, false},
			{branch, undefined, ?DATA_CHUNK_SIZE, false},    %% SubTree2
			{branch, undefined, 4*?DATA_CHUNK_SIZE, false},  %% SubTree3
			{leaf, Leaf6, 6*?DATA_CHUNK_SIZE, false},        %% duplicates are safe and expected
			{leaf, Leaf6, 6*?DATA_CHUNK_SIZE, false},        %% duplicates are safe and expected
			{leaf, Leaf5, 5*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf2, 2*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, false},
			{branch, undefined, ?DATA_CHUNK_SIZE, true},     %% SubTree4
			{leaf, Leaf3, ?DATA_CHUNK_SIZE, false},
			{leaf, Leaf4, ?DATA_CHUNK_SIZE, true}
		], Tree3),

	BadRoot = crypto:strong_rand_bytes(32),
	Path3_1 = ar_merkle:generate_path(Root3, 0, Tree3),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
		Root3, 0, 6*?DATA_CHUNK_SIZE, Path3_1, offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(
		BadRoot, 0, 6*?DATA_CHUNK_SIZE, Path3_1, offset_rebase_support_ruleset)),
	
	Path3_2 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE, Tree3),
	{Leaf2, ?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
		Root3, ?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_2, offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(
		BadRoot, ?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_2, offset_rebase_support_ruleset)),
	
	Path3_3 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE * 2, Tree3),
	{Leaf3, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
		Root3, 2*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_3, offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(
		BadRoot, 2*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_3, offset_rebase_support_ruleset)),
	
	Path3_4 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE * 3, Tree3),
	{Leaf4, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
		Root3, 3*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_4, offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(
		BadRoot, 3*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_4, offset_rebase_support_ruleset)),
	
	Path3_5 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE * 4, Tree3),
	{Leaf5, 4*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
		Root3, 4*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_5, offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(
		BadRoot, 4*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_5, offset_rebase_support_ruleset)),

	Path3_6 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE * 5, Tree3),
	{Leaf6, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
		Root3, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_6, offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(
		BadRoot, 5*?DATA_CHUNK_SIZE, 6*?DATA_CHUNK_SIZE, Path3_6, offset_rebase_support_ruleset)),

	%%           ________Root4_________
	%%          /                      \
	%%      SubTree1         _______SubTree2____________
	%%     /    \           /                           \
	%%   Leaf1 Leaf2    SubTree3 (with offset reset)  SubTree4 (with offset reset)
	%%                  /   \                          /     \
	%%               Leaf3  Leaf4                   Leaf5   Leaf6
	Tags4 = [
				{Leaf1, ?DATA_CHUNK_SIZE},
				{Leaf2, 2*?DATA_CHUNK_SIZE},
				[
					{Leaf3, ?DATA_CHUNK_SIZE},
					{Leaf4, 2*?DATA_CHUNK_SIZE}
				],
				[
					{Leaf5, ?DATA_CHUNK_SIZE},
					{Leaf6, 2*?DATA_CHUNK_SIZE}
				]
			],
	{Root4, Tree4} = ar_merkle:generate_tree(Tags4),
	assert_tree([
			{branch, undefined, 2*?DATA_CHUNK_SIZE, false},  %% Root
			{branch, undefined, ?DATA_CHUNK_SIZE, false},    %% SubTree1
			{branch, undefined, 4*?DATA_CHUNK_SIZE, false},  %% SubTree2
			{leaf, Leaf2, 2*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, false},
			{branch, undefined, ?DATA_CHUNK_SIZE, true},     %% SubTree3
			{leaf, Leaf4, 2*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf3, ?DATA_CHUNK_SIZE, false},
			{branch, undefined, ?DATA_CHUNK_SIZE, true},     %% SubTree4
			{leaf, Leaf6, 2*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf5, ?DATA_CHUNK_SIZE, false}
		], Tree4),

	Path4_1 = ar_merkle:generate_path(Root4, 0, Tree4),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root4, 0, 6 * ?DATA_CHUNK_SIZE,
			Path4_1, offset_rebase_support_ruleset),

	Path4_2 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE, Tree4),
	{Leaf2, ?DATA_CHUNK_SIZE, Right4_2} = ar_merkle:validate_path(Root4, ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_2, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Right4_2),

	Path4_3 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE * 2, Tree4),
	{Leaf3, Left4_3, Right4_3} = ar_merkle:validate_path(Root4, 2 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_3, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Left4_3),
	?assertEqual(3 * ?DATA_CHUNK_SIZE, Right4_3),

	Path4_4 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE * 3, Tree4),
	{Leaf4, Left4_4, Right4_4} = ar_merkle:validate_path(Root4, 3 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_4, offset_rebase_support_ruleset),
	?assertEqual(3 * ?DATA_CHUNK_SIZE, Left4_4),
	?assertEqual(4 * ?DATA_CHUNK_SIZE, Right4_4),

	Path4_5 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE * 4, Tree4),
	{Leaf5, Left4_5, Right4_5} = ar_merkle:validate_path(Root4, 4 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_5, offset_rebase_support_ruleset),
	?assertEqual(4 * ?DATA_CHUNK_SIZE, Left4_5),
	?assertEqual(5 * ?DATA_CHUNK_SIZE, Right4_5),

	Path4_6 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE * 5, Tree4),
	{Leaf6, Left4_6, Right4_6} = ar_merkle:validate_path(Root4, 5 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_6, offset_rebase_support_ruleset),
	?assertEqual(5 * ?DATA_CHUNK_SIZE, Left4_6),
	?assertEqual(6 * ?DATA_CHUNK_SIZE, Right4_6),

	%%             ______________Root__________________
	%%            /                                    \
	%%     ____SubTree1                               Leaf5
	%%    /            \
	%% Leaf1         SubTree2 (with offset reset)
	%%                /                   \
	%%           SubTree3               Leaf4
	%%           /      \
	%%        Leaf2    Leaf3 
	Tags5 = [
				{Leaf1, ?DATA_CHUNK_SIZE},
				[
					{Leaf2, ?DATA_CHUNK_SIZE},
					{Leaf3, 2*?DATA_CHUNK_SIZE},
					{Leaf4, 3*?DATA_CHUNK_SIZE}
				],
				{Leaf5, 5*?DATA_CHUNK_SIZE}
			],
	{Root5, Tree5} = ar_merkle:generate_tree(Tags5),
	assert_tree([
			{branch, undefined, 4*?DATA_CHUNK_SIZE, false}, %% Root
			{branch, undefined, ?DATA_CHUNK_SIZE, false},   %% SubTree1
			{leaf, Leaf5, 5*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf5, 5*?DATA_CHUNK_SIZE, false},       %% Duplicates are safe and expected
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, false},
			{branch, undefined, 2*?DATA_CHUNK_SIZE, true},   %% SubTree2
			{branch, undefined, ?DATA_CHUNK_SIZE, false},    %% SubTree3
			{leaf, Leaf4, 3*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf4, 3*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf3, 2*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf2, ?DATA_CHUNK_SIZE, false}
		], Tree5),

	Path5_1 = ar_merkle:generate_path(Root5, 0, Tree5),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root5, 0, 5*?DATA_CHUNK_SIZE,
			Path5_1, offset_rebase_support_ruleset),

	Path5_2 = ar_merkle:generate_path(Root5, ?DATA_CHUNK_SIZE, Tree5),
	{Leaf2, ?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root5, ?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, Path5_2, offset_rebase_support_ruleset),

	Path5_3 = ar_merkle:generate_path(Root5, 2*?DATA_CHUNK_SIZE, Tree5),
	{Leaf3, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root5, 2*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, Path5_3, offset_rebase_support_ruleset),

	Path5_4 = ar_merkle:generate_path(Root5, 3*?DATA_CHUNK_SIZE, Tree5),
	{Leaf4, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root5, 3*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, Path5_4, offset_rebase_support_ruleset),

	Path5_5 = ar_merkle:generate_path(Root5, 4*?DATA_CHUNK_SIZE, Tree5),
	{Leaf5, 4*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root5, 4*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, Path5_5, offset_rebase_support_ruleset),

	%%             ______________Root__________________
	%%            /                                    \
	%%     ____SubTree1                               Leaf5
	%%    /            \
	%% Leaf1         SubTree2 (with offset reset)
	%%                /                   \
	%%           Leaf2               SubTree3 (with offset reset)
	%%                                     /      \
	%%                                  Leaf3    Leaf4 
	Tags6 = [
				{Leaf1, ?DATA_CHUNK_SIZE},
				[
					{Leaf2, ?DATA_CHUNK_SIZE},
					[
						{Leaf3, ?DATA_CHUNK_SIZE},
						{Leaf4, 2*?DATA_CHUNK_SIZE}
					]
				],
				{Leaf5, 5*?DATA_CHUNK_SIZE}
			],
	{Root6, Tree6} = ar_merkle:generate_tree(Tags6),
	assert_tree([
			{branch, undefined, 4*?DATA_CHUNK_SIZE, false}, %% Root
			{branch, undefined, ?DATA_CHUNK_SIZE, false},   %% SubTree1
			{leaf, Leaf5, 5*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf5, 5*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, false},
			{branch, undefined, ?DATA_CHUNK_SIZE, true},    %% SubTree2
			{leaf, Leaf2, ?DATA_CHUNK_SIZE, false},
			{branch, undefined, ?DATA_CHUNK_SIZE, true},    %% SubTree3
			{leaf, Leaf4, 2*?DATA_CHUNK_SIZE, false},
			{leaf, Leaf3, ?DATA_CHUNK_SIZE, false}
		], Tree6),

	Path6_1 = ar_merkle:generate_path(Root6, 0, Tree6),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root6, 0, 5*?DATA_CHUNK_SIZE,
			Path6_1, offset_rebase_support_ruleset),

	Path6_2 = ar_merkle:generate_path(Root6, ?DATA_CHUNK_SIZE, Tree6),
	{Leaf2, ?DATA_CHUNK_SIZE, 2*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root6, ?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, Path6_2, offset_rebase_support_ruleset),

	Path6_3 = ar_merkle:generate_path(Root6, 2*?DATA_CHUNK_SIZE, Tree6),
	{Leaf3, 2*?DATA_CHUNK_SIZE, 3*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root6, 2*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, Path6_3, offset_rebase_support_ruleset),

	Path6_4 = ar_merkle:generate_path(Root6, 3*?DATA_CHUNK_SIZE, Tree6),
	{Leaf4, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root6, 3*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, Path6_4, offset_rebase_support_ruleset),

	Path6_5 = ar_merkle:generate_path(Root6, 4*?DATA_CHUNK_SIZE, Tree6),
	{Leaf5, 4*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root6, 4*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, Path6_5, offset_rebase_support_ruleset).

test_tree_with_rebase_bad_paths() ->
	%%             ______________Root__________________
	%%            /                                    \
	%%     ____SubTree1                               Leaf5
	%%    /            \
	%% Leaf1         SubTree2 (with offset reset)
	%%                /                   \
	%%           Leaf2               SubTree3 (with offset reset)
	%%                                     /      \
	%%                                  Leaf3    Leaf4 
	Leaf1 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf2 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf3 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf4 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf5 = crypto:strong_rand_bytes(?HASH_SIZE),
	Tags = [
				{Leaf1, ?DATA_CHUNK_SIZE},
				[
					{Leaf2, ?DATA_CHUNK_SIZE},
					[
						{Leaf3, ?DATA_CHUNK_SIZE},
						{Leaf4, 2*?DATA_CHUNK_SIZE}
					]
				],
				{Leaf5, 5*?DATA_CHUNK_SIZE}
			],
	{Root, Tree} = ar_merkle:generate_tree(Tags),
	GoodPath = ar_merkle:generate_path(Root, 3*?DATA_CHUNK_SIZE, Tree),
	{Leaf4, 3*?DATA_CHUNK_SIZE, 4*?DATA_CHUNK_SIZE} = ar_merkle:validate_path(
			Root, 3*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, GoodPath, offset_rebase_support_ruleset),

	BadPath1 = change_path(GoodPath, 0), %% Change L
	?assertEqual(false, ar_merkle:validate_path(
		Root, 3*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, BadPath1, offset_rebase_support_ruleset)),

	BadPath2 = change_path(GoodPath, 2*?HASH_SIZE + 1), %% Change note
	?assertEqual(false, ar_merkle:validate_path(
		Root, 3*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, BadPath2, offset_rebase_support_ruleset)),

	BadPath3 = change_path(GoodPath, 2*?HASH_SIZE + ?NOTE_SIZE + 1), %% Change offset rebase zeros
	?assertEqual(false, ar_merkle:validate_path(
		Root, 3*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, BadPath3, offset_rebase_support_ruleset)),

	BadPath4 = change_path(GoodPath, byte_size(GoodPath) - ?NOTE_SIZE - 1), %% Change leaf data hash
	?assertEqual(false, ar_merkle:validate_path(
		Root, 3*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, BadPath4, offset_rebase_support_ruleset)),

	BadPath5 = change_path(GoodPath, byte_size(GoodPath) - 1), %% Change leaf note
	?assertEqual(false, ar_merkle:validate_path(
		Root, 3*?DATA_CHUNK_SIZE, 5*?DATA_CHUNK_SIZE, BadPath5, offset_rebase_support_ruleset)).

test_tree_with_rebase_partial_chunk() ->
	Leaf1 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf2 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf3 = crypto:strong_rand_bytes(?HASH_SIZE),

	%%    Root5
	%%    /   \
	%% Leaf1  Leaf2 (with offset reset, < 256 KiB)
	Tags5 = [
				{Leaf1, ?DATA_CHUNK_SIZE},
				[
					{Leaf2, 100}
				]
			],
	{Root5, Tree5} = ar_merkle:generate_tree(Tags5),
	assert_tree([
			{branch, undefined, ?DATA_CHUNK_SIZE, false},  %% Root
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, false},
			{leaf, Leaf2, 100, true}
		], Tree5),

	Path5_1 = ar_merkle:generate_path(Root5, 0, Tree5),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root5, 0,
			?DATA_CHUNK_SIZE + 100, Path5_1, offset_rebase_support_ruleset),

	Path5_2 = ar_merkle:generate_path(Root5, ?DATA_CHUNK_SIZE, Tree5),
	{Leaf2, ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE+100} = ar_merkle:validate_path(Root5,
			?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE+100, Path5_2, offset_rebase_support_ruleset),

	%%              Root6__________________
	%%             /                       \
	%%     SubTree1 (with offset reset)   Leaf3
	%%         /               \
	%% Leaf1 (< 256 KiB)  Leaf2 (< 256 KiB, spans two buckets)
	Tags6 = [
				[
					{Leaf1, 131070},
					{Leaf2, 393213}
				],
				{Leaf3, 655355}
			],
	{Root6, Tree6} = ar_merkle:generate_tree(Tags6),
	assert_tree([
			{branch, undefined, 393213, false},  %% Root
			{leaf, Leaf3, 655355, false},
			{branch, undefined, 131070, true},  %% SubTree1
			{leaf, Leaf2, 393213, false},
			{leaf, Leaf1, 131070, false}
		], Tree6),

	Path6_1 = ar_merkle:generate_path(Root6, 0, Tree6),
	{Leaf1, 0, 131070} = ar_merkle:validate_path(Root6, 0,
			1000000, % an arbitrary bound > 655355
			Path6_1, offset_rebase_support_ruleset),

	Path6_2 = ar_merkle:generate_path(Root6, 131070, Tree6),
	{Leaf2, 131070, 393213} = ar_merkle:validate_path(Root6, 131070 + 5,
			655355, Path6_2, offset_rebase_support_ruleset),

	Path6_3 = ar_merkle:generate_path(Root6, 393213 + 1, Tree6),
	{Leaf3, 393213, 655355} = ar_merkle:validate_path(Root6, 393213 + 2, 655355, Path6_3,
			offset_rebase_support_ruleset),

    %%                Root6  (with offset reset)
	%%                  /                     \
	%%          ____SubTree1___              Leaf3
	%%         /               \
	%% Leaf1 (< 256 KiB)  Leaf2 (< 256 KiB, spans two buckets)
	Tags8 = [
				[
					{Leaf1, 131070},
					{Leaf2, 393213},
					{Leaf3, 655355}
				]
			],
	{Root8, Tree8} = ar_merkle:generate_tree(Tags8),
	assert_tree([
			{branch, undefined, 393213, true},  %% Root
			{branch, undefined, 131070, false},  %% SubTree1
			{leaf, Leaf3, 655355, false},
			{leaf, Leaf3, 655355, false},
			{leaf, Leaf2, 393213, false},
			{leaf, Leaf1, 131070, false}
		], Tree8),

	%% Path to first chunk in data set (even if it's a small chunk) will validate
	Path8_1 = ar_merkle:generate_path(Root8, 0, Tree8),
	{Leaf1, 0, 131070} = ar_merkle:validate_path(Root8, 0,
			1000000, % an arbitrary bound > 655355
			Path8_1, offset_rebase_support_ruleset),

	Path8_2 = ar_merkle:generate_path(Root8, 131070, Tree8),
	?assertEqual(false,
		ar_merkle:validate_path(Root8, 131070+5, 655355, Path8_2, offset_rebase_support_ruleset)),

	Path8_3 = ar_merkle:generate_path(Root8, 393213 + 1, Tree8),
	{Leaf3, 393213, 655355} = ar_merkle:validate_path(Root8, 393213 + 2, 655355, Path8_3,
			offset_rebase_support_ruleset),

    %%                           Root9
	%%                  /                     \
	%%              SubTree1             Leaf3 (1 B)
	%%         /               \
	%% Leaf1 (1 B)         Leaf2 (1 B)
	Tags9 = [
				[
					{Leaf1, 1}
				],
				[
					{Leaf2, 1}
				],
				[
					{Leaf3, 1}
				]
			],
	{Root9, Tree9} = ar_merkle:generate_tree(Tags9),
	assert_tree([
			{branch, undefined, 2, false},  %% Root
			{branch, undefined, 1, false},  %% SubTree1
			{leaf, Leaf3, 1, true},
			{leaf, Leaf1, 1, true},
			{leaf, Leaf2, 1, true},
			{leaf, Leaf3, 1, true}          %% Duplicates are safe and expected
		], Tree9),

	%% Path to first chunk in data set (even if it's a small chunk) will validate
	Path9_1 = ar_merkle:generate_path(Root9, 0, Tree9),
	{Leaf1, 0, 1} = ar_merkle:validate_path(Root9, 0,
			1,
			Path9_1, offset_rebase_support_ruleset),

	Path9_2 = ar_merkle:generate_path(Root9, 1, Tree9),
	?assertEqual(false,
		ar_merkle:validate_path(Root9, 1, 2, Path9_2, offset_rebase_support_ruleset)),

	Path9_3 = ar_merkle:generate_path(Root9, 2, Tree9),
	?assertEqual(false,
		ar_merkle:validate_path(Root9, 2, 3, Path9_3, offset_rebase_support_ruleset)),

	%%                           Root9
	%%                  /                     \
	%%              SubTree1             Leaf3 (256 KiB)
	%%         /               \
	%% Leaf1 (256 KiB)         Leaf2 (1 B)
	%%
	%% Every chunk in a subtree following a small-chunk subtree should fail to validated. When
	%% bundling, bundlers are required to bad small chunks out to a chunk boundary.
	Tags10 = [
				[
					{Leaf1, ?DATA_CHUNK_SIZE}
				],
				[
					{Leaf2, 1}
				],
				[
					{Leaf3, ?DATA_CHUNK_SIZE}
				]
			],
	{Root10, Tree10} = ar_merkle:generate_tree(Tags10),
	assert_tree([
			{branch, undefined, ?DATA_CHUNK_SIZE+1, false},  %% Root
			{branch, undefined, ?DATA_CHUNK_SIZE, false},  %% SubTree1
			{leaf, Leaf3, ?DATA_CHUNK_SIZE, true},
			{leaf, Leaf1, ?DATA_CHUNK_SIZE, true},
			{leaf, Leaf2, 1, true},
			{leaf, Leaf3, ?DATA_CHUNK_SIZE, true}          %% Duplicates are safe and expected
		], Tree10),

	Path10_1 = ar_merkle:generate_path(Root10, 0, Tree10),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root10, 0,
			?DATA_CHUNK_SIZE,
			Path10_1, offset_rebase_support_ruleset),

	Path10_2 = ar_merkle:generate_path(Root10, ?DATA_CHUNK_SIZE, Tree10),
	{Leaf2, ?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE+1} = ar_merkle:validate_path(Root10,
			?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE+1,
			Path10_2, offset_rebase_support_ruleset),

	Path10_3 = ar_merkle:generate_path(Root10, ?DATA_CHUNK_SIZE+1, Tree10),
	?assertEqual(false,
		ar_merkle:validate_path(Root10, ?DATA_CHUNK_SIZE+1, (2*?DATA_CHUNK_SIZE)+1,
			Path10_3, offset_rebase_support_ruleset)),
	ok.

test_tree_with_rebase_subtree_ids() ->
	%% Assert that the all the tree IDs are preserved when the tree is added as a subtree within
	%% a larger tree
	Leaf1 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf2 = crypto:strong_rand_bytes(?HASH_SIZE),
	Leaf3 = crypto:strong_rand_bytes(?HASH_SIZE),
	SubTreeTags = [
				{Leaf1, ?DATA_CHUNK_SIZE},
				{Leaf2, 2 * ?DATA_CHUNK_SIZE}
			],
	{SubTreeRoot, SubTree} = ar_merkle:generate_tree(SubTreeTags),

	TreeTags = [
		{Leaf3, ?DATA_CHUNK_SIZE},
		[
			{Leaf1, ?DATA_CHUNK_SIZE},
			{Leaf2, 2 * ?DATA_CHUNK_SIZE}
		]
	],

	{_TreeRoot, Tree} = ar_merkle:generate_tree(TreeTags),

	TreeNodes = lists:nthtail(length(Tree) - length(SubTree), Tree),
	TreeSubTreeRoot = lists:nth(1, TreeNodes),
	TreeLeaf1 = lists:nth(2, TreeNodes),
	SubTreeLeaf1 = lists:nth(2, SubTree),
	TreeLeaf2 = lists:nth(3, TreeNodes),
	SubTreeLeaf2 = lists:nth(3, SubTree),
	?assertEqual(SubTreeRoot, TreeSubTreeRoot#node.id),
	?assertEqual(SubTreeLeaf1#node.id, TreeLeaf1#node.id),
	?assertEqual(SubTreeLeaf2#node.id, TreeLeaf2#node.id).

generate_and_validate_uneven_tree_path_test() ->
	Tags = make_tags_cumulative([{<<N:256>>, 1}
			|| N <- lists:seq(0, ?UNEVEN_TEST_SIZE - 1)]),
	{MR, Tree} = ar_merkle:generate_tree(Tags),
	%% Make sure the target is in the 'uneven' ending of the tree.
	Path = ar_merkle:generate_path(MR, ?UNEVEN_TEST_TARGET, Tree),
	{Leaf, StartOffset, EndOffset} =
		ar_merkle:validate_path(MR, ?UNEVEN_TEST_TARGET, ?UNEVEN_TEST_SIZE, Path),
	{Leaf, StartOffset, EndOffset} =
		ar_merkle:validate_path(MR, ?UNEVEN_TEST_TARGET, ?UNEVEN_TEST_SIZE,
				Path, strict_borders_ruleset),
	?assertEqual(?UNEVEN_TEST_TARGET, binary:decode_unsigned(Leaf)),
	?assert(?UNEVEN_TEST_TARGET < EndOffset),
	?assert(?UNEVEN_TEST_TARGET >= StartOffset).

reject_invalid_tree_path_test_() ->
	{timeout, 30, fun test_reject_invalid_tree_path/0}.

test_reject_invalid_tree_path() ->
	Tags = make_tags_cumulative([{<<N:256>>, 1} || N <- lists:seq(0, ?TEST_SIZE - 1)]),
	{MR, Tree} =
		ar_merkle:generate_tree(Tags),
	RandomTarget = rand:uniform(?TEST_SIZE) - 2,
	?assertEqual(
		false,
		ar_merkle:validate_path(
			MR, RandomTarget,
			?TEST_SIZE,
			ar_merkle:generate_path(MR, RandomTarget+1, Tree)
		)
	).

assert_node({Id, Type, Data, Note, IsRebased}, Node) ->
	?assertEqual(Id, Node#node.id),
	assert_node({Type, Data, Note, IsRebased}, Node);
assert_node({Type, Data, Note, IsRebased}, Node) ->
	?assertEqual(Type, Node#node.type),
	?assertEqual(Data, Node#node.data),
	?assertEqual(Note, Node#node.note),
	?assertEqual(IsRebased, Node#node.is_rebased).

assert_tree([], []) ->
	ok;
assert_tree([], _RestOfTree) ->
	?assert(false);
assert_tree(_RestOfValues, []) ->
	?assert(false);
assert_tree([ExpectedValues | RestOfValues], [Node | RestOfTree]) ->
	assert_node(ExpectedValues, Node),
	assert_tree(RestOfValues, RestOfTree).

change_path(Path, Index) ->
	NewByte = (binary:at(Path, Index) + 1) rem 256,
	List = binary_to_list(Path),
	UpdatedList = lists:sublist(List, Index) ++ [NewByte] ++ lists:nthtail(Index+1, List),
	list_to_binary(UpdatedList).
