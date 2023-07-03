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

validate_path(ID, Dest, LeftBound, RightBound, Path, strict_borders_ruleset) ->
	CheckBorders = true,
	CheckSplit = true,
	AllowRebase = false,
	validate_path(ID, Dest, LeftBound, RightBound, Path, CheckBorders, CheckSplit, AllowRebase);

validate_path(ID, Dest, LeftBound, RightBound, Path, offset_rebase_support_ruleset) ->
	CheckBorders = true,
	CheckSplit = true,
	AllowRebase = true,
	validate_path(ID, Dest, LeftBound, RightBound, Path, CheckBorders, CheckSplit, AllowRebase).


validate_path(ID, Dest, LeftBound, RightBound, Path, CheckBorders, CheckSplit, AllowRebase) ->
	PathSize = byte_size(Path),
	DataSize = RightBound,
	%% Will be set to true only if we only take right branches from the root to the leaf. In this
	%% case we know the leaf chunk is the final chunk in the range reprsented by the merkle tree.
	IsRightMostInItsSubTree = undefined, 
	%% Set to non-zero when AllowRebase is true and we begin processing a subtree.
	LeftBoundShift = 0,
	validate_path(ID, Dest, LeftBound, RightBound, Path,
		PathSize, DataSize, IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, AllowRebase).

%% Validate the leaf of the merkle path (i.e. the data chunk)
validate_path(ID, _Dest, LeftBound, RightBound,
		<< Data:?HASH_SIZE/binary, EndOffset:(?NOTE_SIZE*8) >>,
		PathSize, _DataSize, IsRightMostInItsSubTree, LeftBoundShift,
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
		true ->
			%% Reject chunks smaller than 256 KiB unless they are the last or the only chunks
			%% of their datasets or the second last chunks which do not exceed 256 KiB when
			%% combined with the following (last) chunks. Finally, reject chunks smaller than
			%% their Merkle proofs unless they are the last chunks of their datasets.
			ChunkSize = EndOffset - LeftBound,
			case IsRightMostInItsSubTree of
				true ->
					%% The last chunk may either start at the bucket start or
					%% span two buckets.
					Bucket0 = LeftBound div (?DATA_CHUNK_SIZE),
					Bucket1 = EndOffset div (?DATA_CHUNK_SIZE),
					(LeftBound rem (?DATA_CHUNK_SIZE) == 0) orelse Bucket0 + 1 == Bucket1;
				_ ->
					%% May also be the only chunk of a single-chunk subtree.
					LeftBound rem (?DATA_CHUNK_SIZE) == 0 andalso PathSize =< ChunkSize
			end;
		false ->
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
		PathSize, DataSize, _IsRightMostInItsSubTree, LeftBoundShift,
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
			validate_path(Path, Dest2, NextLeftBound, NextRightBound, Rest, PathSize, DataSize,
				undefined, NextLeftBoundShift, CheckBorders, CheckSplit, true);
		_ ->
			false
	end;

%% Validate a non-leaf node in the merkle path
validate_path(ID, Dest, LeftBound, RightBound,
		<< L:?HASH_SIZE/binary, R:?HASH_SIZE/binary, Note:(?NOTE_SIZE*8), Rest/binary >>,
		PathSize, DataSize, IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, AllowRebase) ->
	validate_node(ID, Dest, LeftBound, RightBound, L, R, Note, Rest,
		PathSize, DataSize, IsRightMostInItsSubTree, LeftBoundShift,
		CheckBorders, CheckSplit, AllowRebase);

%% Invalid merkle path
validate_path(_, _, _, _, _, _, _, _, _, _, _, _) ->
	false.

validate_node(ID, Dest, LeftBound, RightBound, L, R, Note, RemainingPath,
		PathSize, DataSize, IsRightMostInItsSubTree, LeftBoundShift,
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
						%% Traverse right branch)
						{R, max(LeftBound, Note), RightBound,
								case IsRightMostInItsSubTree of undefined -> true;
										_ -> IsRightMostInItsSubTree end}
				end,
			validate_path(BranchID, Dest, NextLeftBound, NextRightBound, RemainingPath,
				PathSize, DataSize, IsRightMostInItsSubTree2, LeftBoundShift,
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

generate_and_validate_tree_with_rebase_test() ->
	%%    Root1
	%%    /   \
	%% Leaf1  Leaf2 (with offset reset)
	Leaf1 = crypto:strong_rand_bytes(32),
	Leaf2 = crypto:strong_rand_bytes(32),
	Tags1 = [{Leaf1, ?DATA_CHUNK_SIZE}, [{Leaf2, ?DATA_CHUNK_SIZE}]],
	{Root1, Tree1} = ar_merkle:generate_tree(Tags1),
	Tags0 = [{Leaf1, ?DATA_CHUNK_SIZE}, {Leaf2, 2 * ?DATA_CHUNK_SIZE}],
	{Root0, Tree0} = ar_merkle:generate_tree(Tags0),
	?assertNotEqual(Root1, Root0),
	Path0_1 = ar_merkle:generate_path(Root0, 0, Tree0),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root0, 0, 2 * ?DATA_CHUNK_SIZE,
			Path0_1, offset_rebase_support_ruleset),
	Path1_1 = ar_merkle:generate_path(Root1, 0, Tree1),
	?assertNotEqual(Path0_1, Path1_1),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root1, 0, 2 * ?DATA_CHUNK_SIZE,
			Path1_1, offset_rebase_support_ruleset),
	Path0_2 = ar_merkle:generate_path(Root0, ?DATA_CHUNK_SIZE, Tree0),
	Path1_2 = ar_merkle:generate_path(Root1, ?DATA_CHUNK_SIZE, Tree1),
	?assertNotEqual(Path1_2, Path0_2),
	{Leaf2, ?DATA_CHUNK_SIZE, Right0_2} = ar_merkle:validate_path(Root0, ?DATA_CHUNK_SIZE,
			2 * ?DATA_CHUNK_SIZE, Path0_2, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Right0_2),
	{Leaf2, ?DATA_CHUNK_SIZE, Right1_2} = ar_merkle:validate_path(Root1, ?DATA_CHUNK_SIZE,
			2 * ?DATA_CHUNK_SIZE, Path1_2, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Right1_2),
	{Leaf2, ?DATA_CHUNK_SIZE, Right1_2} = ar_merkle:validate_path(Root1,
			2 * ?DATA_CHUNK_SIZE - 1, 2 * ?DATA_CHUNK_SIZE, Path1_2,
			offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(Root1, ?DATA_CHUNK_SIZE,
			2 * ?DATA_CHUNK_SIZE, Path1_1, offset_rebase_support_ruleset)),
	?assertEqual(false, ar_merkle:validate_path(Root1, 0,
			2 * ?DATA_CHUNK_SIZE, Path1_2, offset_rebase_support_ruleset)),
	%%              Root2
	%%    /                      \
	%% Leaf1 (with offset reset)  Leaf2 (with offset reset)
	?debugMsg("Tree 2"),
	Tags2 = [[{Leaf1, ?DATA_CHUNK_SIZE}], {Leaf2, ?DATA_CHUNK_SIZE * 2}],
	{Root2, Tree2} = ar_merkle:generate_tree(Tags2),
	Path2_1 = ar_merkle:generate_path(Root2, 0, Tree2),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root2, 0,
			2 * ?DATA_CHUNK_SIZE, Path2_1, offset_rebase_support_ruleset),
	Path2_2 = ar_merkle:generate_path(Root2, ?DATA_CHUNK_SIZE, Tree2),
	{Leaf2, ?DATA_CHUNK_SIZE, Right2_2} = ar_merkle:validate_path(Root2,
			?DATA_CHUNK_SIZE, 2 * ?DATA_CHUNK_SIZE, Path2_2, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Right2_2),
	{Leaf2, ?DATA_CHUNK_SIZE, Right2_2} = ar_merkle:validate_path(Root2,
			2 * ?DATA_CHUNK_SIZE - 1, 2 * ?DATA_CHUNK_SIZE, Path2_2,
			offset_rebase_support_ruleset),
	?assertEqual(false, ar_merkle:validate_path(Root2, ?DATA_CHUNK_SIZE,
			2 * ?DATA_CHUNK_SIZE, Path2_1, offset_rebase_support_ruleset)),
	?assertEqual(false, ar_merkle:validate_path(Root2, 0,
			2 * ?DATA_CHUNK_SIZE, Path2_2, offset_rebase_support_ruleset)),
	%%                   Root3
	%%          /                      \
	%%      SubTree1                      SubTree2
	%%     /    \           /                                       \
	%%   Leaf1 Leaf2    SubTree3 (with offset reset)               SubTree4
	%%                  /   \                                      /     \
	%%               Leaf3  Leaf4 (with nested offset reset)    Leaf5   Leaf6
	?debugMsg("Tree 3"),
	Leaf3 = crypto:strong_rand_bytes(32),
	Leaf4 = crypto:strong_rand_bytes(32),
	Leaf5 = crypto:strong_rand_bytes(32),
	Leaf6 = crypto:strong_rand_bytes(32),
	Tags3 = [{Leaf1, ?DATA_CHUNK_SIZE}, {Leaf2, ?DATA_CHUNK_SIZE * 2},
			[{Leaf3, ?DATA_CHUNK_SIZE}, [{Leaf4, ?DATA_CHUNK_SIZE}]],
			{Leaf5, ?DATA_CHUNK_SIZE * 5}, {Leaf6, ?DATA_CHUNK_SIZE * 6}],
	{Root3, Tree3} = ar_merkle:generate_tree(Tags3),
	Path3_1 = ar_merkle:generate_path(Root3, 0, Tree3),
	?debugMsg("Path 1"),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root3, 0, 6 * ?DATA_CHUNK_SIZE,
			Path3_1, offset_rebase_support_ruleset),
	Path3_2 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE, Tree3),
	?debugMsg("Path 2"),
	{Leaf2, ?DATA_CHUNK_SIZE, Right3_2} = ar_merkle:validate_path(Root3, ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path3_2, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Right3_2),
	Path3_3 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE * 2, Tree3),
	?debugMsg("Path 3"),
	{Leaf3, Left3_3, Right3_3} = ar_merkle:validate_path(Root3, 2 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path3_3, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Left3_3),
	?assertEqual(3 * ?DATA_CHUNK_SIZE, Right3_3),
	Path3_4 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE * 3, Tree3),
	?debugMsg("Path 4"),
	{Leaf4, Left3_4, Right3_4} = ar_merkle:validate_path(Root3, 3 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path3_4, offset_rebase_support_ruleset),
	?assertEqual(3 * ?DATA_CHUNK_SIZE, Left3_4),
	?assertEqual(4 * ?DATA_CHUNK_SIZE, Right3_4),
	Path3_5 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE * 4, Tree3),
	?debugMsg("Path 5"),
	{Leaf5, Left3_5, Right3_5} = ar_merkle:validate_path(Root3, 4 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path3_5, offset_rebase_support_ruleset),
	?assertEqual(4 * ?DATA_CHUNK_SIZE, Left3_5),
	?assertEqual(5 * ?DATA_CHUNK_SIZE, Right3_5),
	Path3_6 = ar_merkle:generate_path(Root3, ?DATA_CHUNK_SIZE * 5, Tree3),
	?debugMsg("Path 6"),
	{Leaf6, Left3_6, Right3_6} = ar_merkle:validate_path(Root3, 5 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path3_6, offset_rebase_support_ruleset),
	?assertEqual(5 * ?DATA_CHUNK_SIZE, Left3_6),
	?assertEqual(6 * ?DATA_CHUNK_SIZE, Right3_6),
	%%                   Root4
	%%          /                      \
	%%      SubTree1                SubTree2
	%%     /    \           /                           \
	%%   Leaf1 Leaf2    SubTree3 (with offset reset)  SubTree4 (with offset reset)
	%%                  /   \                          /     \
	%%               Leaf3  Leaf4                   Leaf5   Leaf6
	?debugMsg("Tree 4"),
	Tags4 = [{Leaf1, ?DATA_CHUNK_SIZE}, {Leaf2, ?DATA_CHUNK_SIZE * 2},
			[{Leaf3, ?DATA_CHUNK_SIZE}, {Leaf4, 2 * ?DATA_CHUNK_SIZE}],
			[{Leaf5, ?DATA_CHUNK_SIZE}, {Leaf6, ?DATA_CHUNK_SIZE * 2}]],
	{Root4, Tree4} = ar_merkle:generate_tree(Tags4),
	Path4_1 = ar_merkle:generate_path(Root4, 0, Tree4),
	?debugMsg("Path 1"),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root4, 0, 6 * ?DATA_CHUNK_SIZE,
			Path4_1, offset_rebase_support_ruleset),
	Path4_2 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE, Tree4),
	?debugMsg("Path 2"),
	{Leaf2, ?DATA_CHUNK_SIZE, Right4_2} = ar_merkle:validate_path(Root4, ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_2, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Right4_2),
	Path4_3 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE * 2, Tree4),
	?debugMsg("Path 3"),
	{Leaf3, Left4_3, Right4_3} = ar_merkle:validate_path(Root4, 2 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_3, offset_rebase_support_ruleset),
	?assertEqual(2 * ?DATA_CHUNK_SIZE, Left4_3),
	?assertEqual(3 * ?DATA_CHUNK_SIZE, Right4_3),
	Path4_4 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE * 3, Tree4),
	?debugMsg("Path 4"),
	{Leaf4, Left4_4, Right4_4} = ar_merkle:validate_path(Root4, 3 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_4, offset_rebase_support_ruleset),
	?assertEqual(3 * ?DATA_CHUNK_SIZE, Left4_4),
	?assertEqual(4 * ?DATA_CHUNK_SIZE, Right4_4),
	Path4_5 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE * 4, Tree4),
	?debugMsg("Path 5"),
	{Leaf5, Left4_5, Right4_5} = ar_merkle:validate_path(Root4, 4 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_5, offset_rebase_support_ruleset),
	?assertEqual(4 * ?DATA_CHUNK_SIZE, Left4_5),
	?assertEqual(5 * ?DATA_CHUNK_SIZE, Right4_5),
	Path4_6 = ar_merkle:generate_path(Root4, ?DATA_CHUNK_SIZE * 5, Tree4),
	?debugMsg("Path 6"),
	{Leaf6, Left4_6, Right4_6} = ar_merkle:validate_path(Root4, 5 * ?DATA_CHUNK_SIZE,
			6 * ?DATA_CHUNK_SIZE, Path4_6, offset_rebase_support_ruleset),
	?assertEqual(5 * ?DATA_CHUNK_SIZE, Left4_6),
	?assertEqual(6 * ?DATA_CHUNK_SIZE, Right4_6),
	%%    Root5
	%%    /   \
	%% Leaf1  Leaf2 (with offset reset, < 256 KiB)
	?debugMsg("Tree 5"),
	Tags5 = [{Leaf1, ?DATA_CHUNK_SIZE}, [{Leaf2, 100}]],
	{Root5, Tree5} = ar_merkle:generate_tree(Tags5),
	Path5_1 = ar_merkle:generate_path(Root5, 0, Tree5),
	{Leaf1, 0, ?DATA_CHUNK_SIZE} = ar_merkle:validate_path(Root5, 0,
			?DATA_CHUNK_SIZE + 100, Path5_1, offset_rebase_support_ruleset),
	Path5_2 = ar_merkle:generate_path(Root5, ?DATA_CHUNK_SIZE, Tree5),
	{Leaf2, ?DATA_CHUNK_SIZE, Right5_2} = ar_merkle:validate_path(Root5,
			?DATA_CHUNK_SIZE, ?DATA_CHUNK_SIZE + 100, Path5_2, offset_rebase_support_ruleset),
	?assertEqual(?DATA_CHUNK_SIZE + 100, Right5_2),
	%%                Root6
	%%             /        \
	%%         SubTree1      Leaf3
	%%    /               \
	%% Leaf1 (< 256 KiB)  Leaf2 (< 256 KiB, spans two buckets)
	?debugMsg("Tree 6"),
	Tags6 = [[{Leaf1, 131070}, {Leaf2, 393213}], {Leaf3, 655355}],
	{Root6, Tree6} = ar_merkle:generate_tree(Tags6),
	Path6_1 = ar_merkle:generate_path(Root6, 0, Tree6),
	{Leaf1, 0, 131070} = ar_merkle:validate_path(Root6, 0,
			1000000, % an arbitrary bound > 655355
			Path6_1, offset_rebase_support_ruleset),
	Path6_2 = ar_merkle:generate_path(Root6, 131070, Tree6),
	{Leaf2, 131070, Right6_2} = ar_merkle:validate_path(Root6, 131070 + 5,
			655355, Path6_2, offset_rebase_support_ruleset),
	?assertEqual(393213, Right6_2),
	Path6_3 = ar_merkle:generate_path(Root6, 393213 + 1, Tree6),
	{Leaf3, 393213, Right6_3} = ar_merkle:validate_path(Root6, 393213 + 2, 655355, Path6_3,
			offset_rebase_support_ruleset),
	?assertEqual(655355, Right6_3).

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
	RandomTarget = rand:uniform(?TEST_SIZE) - 1,
	?assertEqual(
		false,
		ar_merkle:validate_path(
			MR, RandomTarget,
			?TEST_SIZE,
			ar_merkle:generate_path(MR, 1000, Tree)
		)
	).
