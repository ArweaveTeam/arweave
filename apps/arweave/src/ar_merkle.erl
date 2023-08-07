%%% @doc Generates annotated merkle trees, paths inside those trees, as well
%%% as verification of those proofs.
-module(ar_merkle).

-export([generate_tree/1, generate_path/3, validate_path/4, validate_path_strict_borders/4,
		validate_path_strict_data_split/4, extract_note/1, extract_root/1]).

-include_lib("arweave/include/ar.hrl").
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
	max				% The maximum observed note at this point.
}).

-define(HASH_SIZE, ?CHUNK_ID_HASH_SIZE).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Generate a Merkle tree from a list of pairs of IDs (of length 32 bytes)
%% and labels -- most often offsets.
generate_tree(Elements) ->
	generate_all_rows(generate_leaves(Elements)).

%% @doc Generate a Merkle path for the given offset Dest from the tree Tree
%% with the root ID.
generate_path(ID, Dest, Tree) ->
	binary:list_to_bin(generate_path_parts(ID, Dest, Tree)).

%% @doc Validate the given merkle path.
validate_path(ID, Dest, RightBound, _Path) when RightBound =< 0 ->
	?LOG_ERROR([{event, validate_path_called_with_not_positive_right_bound},
			{root, ar_util:encode(ID)}, {dest, Dest}, {right_bound, RightBound}]),
	throw(invalid_right_bound);
validate_path(ID, Dest, RightBound, Path) when Dest >= RightBound ->
	validate_path(ID, RightBound - 1, RightBound, Path);
validate_path(ID, Dest, RightBound, Path) when Dest < 0 ->
	validate_path(ID, 0, RightBound, Path);
validate_path(ID, Dest, RightBound, Path) ->
	validate_path(ID, Dest, 0, RightBound, Path).

validate_path(ID, _Dest, LeftBound, RightBound,
		<< Data:?HASH_SIZE/binary, EndOffset:(?NOTE_SIZE*8) >>) ->
	case hash([hash(Data), hash(note_to_binary(EndOffset))]) of
		ID -> {Data, LeftBound, max(min(RightBound, EndOffset), LeftBound + 1)};
		_ -> false
	end;
validate_path(ID, Dest, LeftBound, RightBound,
		<< L:?HASH_SIZE/binary, R:?HASH_SIZE/binary, Note:(?NOTE_SIZE*8), Rest/binary >>) ->
	case hash([hash(L), hash(R), hash(note_to_binary(Note))]) of
		ID ->
			{Path, NextLeftBound, NextRightBound} =
				case Dest < Note of
					true ->
						{L, LeftBound, min(RightBound, Note)};
					false ->
						{R, max(LeftBound, Note), RightBound}
				end,
			validate_path(Path, Dest, NextLeftBound, NextRightBound, Rest);
		_ ->
			false
	end;
validate_path(_, _, _, _, _) ->
	false.

%% @doc Validate the given merkle path and ensure every offset does not
%% exceed the previous offset by more than ?DATA_CHUNK_SIZE.
validate_path_strict_borders(ID, Dest, RightBound, _Path) when RightBound =< 0 ->
	?LOG_ERROR([{event, validate_path_strict_borders_called_with_not_positive_right_bound},
			{root, ar_util:encode(ID)}, {dest, Dest}, {right_bound, RightBound}]),
	throw(invalid_right_bound);
validate_path_strict_borders(ID, Dest, RightBound, Path) when Dest >= RightBound ->
	validate_path_strict_borders(ID, RightBound - 1, RightBound, Path);
validate_path_strict_borders(ID, Dest, RightBound, Path) when Dest < 0 ->
	validate_path_strict_borders(ID, 0, RightBound, Path);
validate_path_strict_borders(ID, Dest, RightBound, Path) ->
	validate_path_strict_borders(ID, Dest, 0, RightBound, Path).

validate_path_strict_borders(ID, _Dest, LeftBound, RightBound,
		<< Data:?HASH_SIZE/binary, EndOffset:(?NOTE_SIZE*8) >>) ->
	case EndOffset - LeftBound > ?DATA_CHUNK_SIZE
			orelse RightBound - LeftBound > ?DATA_CHUNK_SIZE of
		true ->
			false;
		false ->
			case hash([hash(Data), hash(note_to_binary(EndOffset))]) of
				ID ->
					{Data, LeftBound, max(min(RightBound, EndOffset), LeftBound + 1)};
				_ ->
					false
			end
	end;
validate_path_strict_borders(ID, Dest, LeftBound, RightBound,
		<< L:?HASH_SIZE/binary, R:?HASH_SIZE/binary, Note:(?NOTE_SIZE*8), Rest/binary >>) ->
	case hash([hash(L), hash(R), hash(note_to_binary(Note))]) of
		ID ->
			{Path, NextLeftBound, NextRightBound} =
				case Dest < Note of
					true ->
						{L, LeftBound, min(RightBound, Note)};
					false ->
						{R, max(LeftBound, Note), RightBound}
				end,
			validate_path_strict_borders(Path, Dest, NextLeftBound, NextRightBound, Rest);
		_ ->
			false
	end;
validate_path_strict_borders(_ID, _Dest, _LeftBound, _RightBound, _Path) ->
	false.

%% @doc Validate the given merkle path and ensure every offset does not
%% exceed the previous offset by more than ?DATA_CHUNK_SIZE. Additionally,
%% reject chunks smaller than 256 KiB unless they are the last or the only chunks
%% of their datasets or the second last chunks which do not exceed 256 KiB when
%% combined with the following (last) chunks. Finally, reject chunks smaller than
%% their Merkle proofs unless they are the last chunks of their datasets.
validate_path_strict_data_split(ID, Dest, RightBound, _Path) when RightBound =< 0 ->
	?LOG_ERROR([{event, validate_path_called_with_not_positive_right_bound},
			{root, ar_util:encode(ID)}, {dest, Dest}, {right_bound, RightBound}]),
	throw(invalid_right_bound);
validate_path_strict_data_split(ID, Dest, RightBound, Path) when Dest >= RightBound ->
	validate_path_strict_data_split(ID, RightBound - 1, RightBound, Path);
validate_path_strict_data_split(ID, Dest, RightBound, Path) when Dest < 0 ->
	validate_path_strict_data_split(ID, 0, RightBound, Path);
validate_path_strict_data_split(ID, Dest, RightBound, Path) ->
	validate_path_strict_data_split(ID, Dest, 0, RightBound, Path, byte_size(Path), RightBound).

validate_path_strict_data_split(ID, _Dest, LeftBound, RightBound,
		<< Data:?HASH_SIZE/binary, EndOffset:(?NOTE_SIZE*8) >>, PathSize, DataSize) ->
	case EndOffset - LeftBound > ?DATA_CHUNK_SIZE
			orelse RightBound - LeftBound > ?DATA_CHUNK_SIZE of
		true ->
			false;
		false ->
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
					case hash([hash(Data), hash(note_to_binary(EndOffset))]) of
						ID ->
							{Data, LeftBound, max(min(RightBound, EndOffset), LeftBound + 1)};
						_ ->
							false
					end
			end
	end;
validate_path_strict_data_split(ID, Dest, LeftBound, RightBound,
		<< L:?HASH_SIZE/binary, R:?HASH_SIZE/binary, Note:(?NOTE_SIZE*8), Rest/binary >>,
		PathSize, DataSize) ->
	case hash([hash(L), hash(R), hash(note_to_binary(Note))]) of
		ID ->
			{Path, NextLeftBound, NextRightBound} =
				case Dest < Note of
					true ->
						{L, LeftBound, min(RightBound, Note)};
					false ->
						{R, max(LeftBound, Note), RightBound}
				end,
			validate_path_strict_data_split(Path, Dest, NextLeftBound, NextRightBound, Rest,
					PathSize, DataSize);
		_ ->
			false
	end;
validate_path_strict_data_split(_ID, _Dest, _LeftBound, _RightBound, _Path, _PathSize,
		_DataSize) ->
	false.

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

generate_leaves(Elements) ->
	lists:foldr(
		fun({Data, Note}, Nodes) ->
			Hash = hash([hash(Data), hash(note_to_binary(Note))]),
			insert(
				#node {
					id = Hash,
					type = leaf,
					data = Data,
					note = Note,
					max = Note
				},
				Nodes
			)
		end,
		new(),
		Elements
	).

%% Note: This implementation leaves some duplicates in the tree structure.
%% The produced trees could be a little smaller if these duplicates were 
%% not present, but removing them with ar_util:unique takes far too long.
generate_all_rows([]) ->
	{<<>>, []};
generate_all_rows(Leaves) ->
	generate_all_rows(Leaves, Leaves).

generate_all_rows([RootN], Tree) ->
	{RootN#node.id, Tree};
generate_all_rows(Row, Tree) ->
	NewRow = generate_row(Row),
	generate_all_rows(NewRow, NewRow ++ Tree).

generate_row([]) -> [];
generate_row([Left]) -> [generate_node(Left, empty)];
generate_row([L, R | Rest]) ->
	[generate_node(L, R) | generate_row(Rest)].

generate_node(Left, empty) ->
	Left;
generate_node(L, R) ->
	#node {
		id = hash([hash(L#node.id), hash(R#node.id), hash(note_to_binary(L#node.max))]),
		type = branch,
		left = L#node.id,
		right = R#node.id,
		note = L#node.max,
		max = R#node.max
	}.

generate_path_parts(ID, Dest, Tree) ->
	case get(ID, Tree) of
		N when N#node.type == leaf ->
			[N#node.data, note_to_binary(N#node.note)];
		N when N#node.type == branch ->
			[
				N#node.left, N#node.right, note_to_binary(N#node.note)
			|
				generate_path_parts(
					case Dest < N#node.note of
						true -> N#node.left;
						false -> N#node.right
					end,
					Dest,
					Tree
				)
			]
	end.

new() ->
	[].

insert(Node, Map) ->
	[Node | Map].

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

generate_and_validate_balanced_tree_path_test() ->
	Tags = make_tags_cumulative([{<<N:256>>, 1} || N <- lists:seq(0, ?TEST_SIZE - 1)]),
	{MR, Tree} = ar_merkle:generate_tree(Tags),
	?assertEqual(length(Tree), (?TEST_SIZE * 2) - 1),
	lists:foreach(
		fun(_TestCase) ->
			RandomTarget = rand:uniform(?TEST_SIZE) - 1,
			Path = ar_merkle:generate_path(MR, RandomTarget, Tree),
			{Leaf, StartOffset, EndOffset} =
				ar_merkle:validate_path(MR, RandomTarget, ?TEST_SIZE, Path),
			{Leaf, StartOffset, EndOffset} =
				ar_merkle:validate_path_strict_borders(MR, RandomTarget, ?TEST_SIZE, Path),
			?assertEqual(RandomTarget, binary:decode_unsigned(Leaf)),
			?assert(RandomTarget < EndOffset),
			?assert(RandomTarget >= StartOffset)
		end,
		lists:seq(1, 100)
	).

generate_and_validate_uneven_tree_path_test() ->
	Tags = make_tags_cumulative([{<<N:256>>, 1} || N <- lists:seq(0, ?UNEVEN_TEST_SIZE - 1)]),
	{MR, Tree} = ar_merkle:generate_tree(Tags),
	%% Make sure the target is in the 'uneven' ending of the tree.
	Path = ar_merkle:generate_path(MR, ?UNEVEN_TEST_TARGET, Tree),
	{Leaf, StartOffset, EndOffset} =
		ar_merkle:validate_path(MR, ?UNEVEN_TEST_TARGET, ?UNEVEN_TEST_SIZE, Path),
	{Leaf, StartOffset, EndOffset} =
		ar_merkle:validate_path_strict_borders(MR, ?UNEVEN_TEST_TARGET, ?UNEVEN_TEST_SIZE, Path),
	?assertEqual(?UNEVEN_TEST_TARGET, binary:decode_unsigned(Leaf)),
	?assert(?UNEVEN_TEST_TARGET < EndOffset),
	?assert(?UNEVEN_TEST_TARGET >= StartOffset).

reject_invalid_tree_path_test() ->
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
