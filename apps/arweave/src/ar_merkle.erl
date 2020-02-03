-module(ar_merkle).
-export([generate_tree/1, generate_path/3]).
-export([validate_path/3]).
-export([extract_note/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Generates annotated merkle trees, paths inside those trees, as well 
%%% as verification of those proofs.

-record(node, {
    id,
    type = branch, % root | branch | leaf
    data, % The value (for leaves)
    note, % A number less than 2^64
    left, % The (optional) ID of a node to the left
    right, % The (optional) ID of a node to the right
    max % The maximum observed note at this point
}).

-define(HASH_SIZE, ?CHUNK_ID_HASH_SIZE).
-define(NOTE_SIZE, 8).

%%% Tree generation.
%%% Returns the merkle root and the tree data structure.

%% @doc Generate a tree from a list of pairs of IDs (of length 32 bytes)
%% and labels -- most often sizes.
generate_tree(Elements) ->
    generate_all_rows(generate_leaves(Elements)).

generate_leaves(Elements) ->
	lists:foldr(
		fun({Data, Note}, Nodes) ->
			Hash = hash([Data, note_to_binary(Note)]),
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

%% TODO: This implementation leaves some duplicates in the tree structure.
%% The produced trees could be a little smaller if these duplicates were 
%% not present, but removing them with `ar_util:unique` takes far too long.
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
        id = hash([L#node.id, R#node.id, note_to_binary(L#node.max)]),
        type = branch,
        left = L#node.id,
        right = R#node.id,
        note = L#node.max,
        max = R#node.max
    }.

%%% Merkle path generation and verification functions.

generate_path(ID, Dest, Tree) ->
    binary:list_to_bin(generate_path_parts(ID, Dest, Tree)).

generate_path_parts(ID, Dest, Tree) ->
    case get(ID, Tree) of
        N when N#node.type == leaf ->
            [N#node.data, note_to_binary(N#node.note)];
        N when N#node.type == branch ->
            [
                N#node.left, N#node.right, note_to_binary(N#node.note)
            |
                generate_path_parts(
                    case Dest =< N#node.note of
                        true -> N#node.left;
                        false -> N#node.right
                    end,
                    Dest,
                    Tree
                )
            ]
    end.

validate_path(ID, _Dest, << Data:?HASH_SIZE/binary, Note:(?NOTE_SIZE*8) >>) ->
    case hash([Data, note_to_binary(Note)]) of
        ID -> Data;
        _ -> false
    end;
validate_path(ID, Dest,
        << L:?HASH_SIZE/binary, R:?HASH_SIZE/binary, Note:(?NOTE_SIZE*8), Rest/binary >>) ->
    case hash([L, R, note_to_binary(Note)]) of
        ID ->
            validate_path(
                case Dest =< Note of
                    true -> L;
                    false -> R
                end,
                Dest,
                Rest
            );
        _ -> false
    end.

%% @doc Get the note attached to the final node from a path.
extract_note(Path) ->
    binary:decode_unsigned(
        binary:part(Path, byte_size(Path) - ?NOTE_SIZE, ?NOTE_SIZE)
    ).

%%% Helper functions for managing the tree data structure.
%%% Abstracted so that the concrete data type can be replaced later.

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

hash(Parts) ->
    crypto:hash(sha256, binary:list_to_bin(Parts)).

%%% Helpers

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

%%% Tests

-define(TEST_SIZE, 64 * 1024).
-define(UNEVEN_TEST_SIZE, 35643).
-define(UNEVEN_TEST_TARGET, 33271).

generate_balanced_tree_test() ->
    {_MR, Tree} = ar_merkle:generate_tree(make_tags_cumulative([{<<N:256>>, 1} || N <- lists:seq(1, ?TEST_SIZE)])),
    ?assertEqual(length(Tree), (?TEST_SIZE * 2) - 1).

generate_and_validate_balanced_tree_path_test() ->
    {MR, Tree} = ar_merkle:generate_tree(make_tags_cumulative([{<<N:256>>, 1} || N <- lists:seq(1, ?TEST_SIZE)])),
    RandomTarget = rand:uniform(?TEST_SIZE),
    ?assertEqual(
        RandomTarget,
        binary:decode_unsigned(
            ar_merkle:validate_path(
                MR, RandomTarget,
                ar_merkle:generate_path(MR, RandomTarget, Tree)
            )
        )
    ).

generate_and_validate_uneven_tree_path_test() ->
    {MR, Tree} = ar_merkle:generate_tree(make_tags_cumulative([{<<N:256>>, 1} || N <- lists:seq(1, ?UNEVEN_TEST_SIZE)])),
    %% Make sure the target is in the 'uneven' ending of the tree.
    ?assertEqual(
        ?UNEVEN_TEST_TARGET,
        binary:decode_unsigned(
            ar_merkle:validate_path(
                MR, ?UNEVEN_TEST_TARGET,
                ar_merkle:generate_path(MR, ?UNEVEN_TEST_TARGET, Tree)
            )
        )
    ).

reject_invalid_tree_path_test() ->
    {MR, Tree} = ar_merkle:generate_tree(make_tags_cumulative([{<<N:256>>, 1} || N <- lists:seq(1, ?TEST_SIZE)])),
    RandomTarget = rand:uniform(?TEST_SIZE),
    ?assertEqual(
        false,
        ar_merkle:validate_path(
            MR, RandomTarget,
            ar_merkle:generate_path(MR, 1000, Tree)
        )
    ).
