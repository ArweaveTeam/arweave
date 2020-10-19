-module(ar_unbalanced_merkle).

-export([
	root/2, root/3,
	hash_list_to_merkle_root/1,
	block_index_to_merkle_root/1,
	hash_block_index_entry/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Module for building and manipulating generic and specific unbalanced merkle trees.

%% @doc Take a prior merkle root and add a new piece of data to it, optionally
%% providing a conversion function prior to hashing.
root(OldRoot, Data, Fun) -> root(OldRoot, Fun(Data)).
root(OldRoot, Data) ->
	crypto:hash(?MERKLE_HASH_ALG, << OldRoot/binary, Data/binary >>).

%% @doc Generate a new entire merkle tree from a hash list.
hash_list_to_merkle_root(HL) ->
	lists:foldl(
		fun(BH, MR) -> root(MR, BH) end,
		<<>>,
		lists:reverse(HL)
	).

%% @doc Generate a new entire merkle tree from a block index.
block_index_to_merkle_root(HL) ->
	lists:foldl(
		fun(BIEntry, MR) -> root(MR, BIEntry, fun hash_block_index_entry/1) end,
		<<>>,
		lists:reverse(HL)
	).

hash_block_index_entry({BH, WeaveSize, TXRoot}) ->
	ar_deep_hash:hash([BH, integer_to_binary(WeaveSize), TXRoot]).

%%% TESTS

basic_hash_root_generation_test() ->
	BH0 = crypto:strong_rand_bytes(32),
	BH1 = crypto:strong_rand_bytes(32),
	BH2 = crypto:strong_rand_bytes(32),
	MR0 = test_hash(BH0),
	MR1 = test_hash(<<MR0/binary, BH1/binary>>),
	MR2 = test_hash(<<MR1/binary, BH2/binary>>),
	?assertEqual(MR2, hash_list_to_merkle_root([BH2, BH1, BH0])).

test_hash(Bin) -> crypto:hash(?MERKLE_HASH_ALG, Bin).

root_update_test() ->
	BH0 = crypto:strong_rand_bytes(32),
	BH1 = crypto:strong_rand_bytes(32),
	BH2 = crypto:strong_rand_bytes(32),
	BH3 = crypto:strong_rand_bytes(32),
	Root =
		root(
			root(
				hash_list_to_merkle_root([BH1, BH0]),
				BH2
			),
			BH3
		),
	?assertEqual(
		hash_list_to_merkle_root([BH3, BH2, BH1, BH0]),
		Root
	).
