-module(ar_weave).
-export([init/0, init/1, add/2, add/3, add/4, add/5]).
-export([hash/3, indep_hash/1]).
-export([verify/1, verify_indep/2]).
-export([calculate_recall_block/1, calculate_recall_block/2]).
-export([generate_block_data/1, generate_hash_list/1]).
-export([is_data_on_block_list/2, is_tx_on_block_list/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Utilities for manipulating the ARK weave datastructure.

%% Start a new block list. Optionally takes a list of wallet values
%% for the genesis block.
init() -> init([]).
init(WalletList) ->
	B =
		#block{
			height = 0,
			hash = crypto:strong_rand_bytes(32),
			nonce = crypto:strong_rand_bytes(32),
			txs = [],
			wallet_list = WalletList,
			hash_list = []
		},
	[B#block { indep_hash = indep_hash(B) }].

%% Add a new block to the weave, with assiocated TXs and archive data.
add(Bs, TXs) ->
	add(Bs, TXs, mine(hd(Bs), TXs)).
add(Bs, TXs, Nonce) ->
	add(Bs, generate_hash_list(Bs), TXs, Nonce).
add(Bs, HashList, TXs, Nonce) ->
	add(Bs, HashList, [], TXs, Nonce).
add(Bs = [B|_], HashList, WalletList, TXs, Nonce) ->
	NewB =
		#block {
			nonce = Nonce,
			height = B#block.height + 1,
			hash = hash(B, TXs, Nonce),
			hash_list = HashList,
			wallet_list = ar_node:apply_txs(WalletList, TXs),
			txs = TXs
		},
	[NewB#block { indep_hash = indep_hash(NewB) }|Bs].

%% Take a complete block list and return a list of block hashes.
%% Throws an error if the block list is not complete.
generate_hash_list([]) -> [];
generate_hash_list(Bs = [B|_]) ->
	lists:reverse(generate_hash_list(Bs, B#block.height + 1)).
generate_hash_list([], 0) -> [];
generate_hash_list([B|Bs], N) ->
	[B#block.hash|generate_hash_list(Bs, N - 1)].

%% Verify that a list of blocks is valid.
verify([_GenesisBlock]) -> true;
verify([B|Rest]) ->
	(
		B#block.hash =:=
			ar_mine:validate(
				(hd(Rest))#block.hash,
				B#block.diff,
				generate_block_data(B),
				B#block.nonce
			)
	) andalso verify(Rest).

%% Verify a block from a hash list. Hash lists are stored in reverse order
verify_indep(#block{ height = 0 }, []) -> true;
verify_indep(B = #block { height = Height }, HashList) ->
	lists:nth(Height + 1, HashList) == indep_hash(B#block { indep_hash = undefined }).

%% Generate a recall block number from a block or a hash and block height.
calculate_recall_block(B) when is_record(B, block) ->
	calculate_recall_block(B#block.indep_hash, B#block.height).
calculate_recall_block(IndepHash, Height) ->
	binary:decode_unsigned(IndepHash) rem Height.

%% Return a binary of all of the information stored in the block.
generate_block_data(B) when is_record(B, block) -> generate_block_data(B#block.txs);
generate_block_data(TXs) ->
	<<
		(
			binary:list_to_bin(
				lists:map(
					fun ar_tx:to_binary/1,
					lists:sort(TXs)
				)
			)
		)/binary
	>>.

%% Create the hash of the next block in the list, given a previous block,
%% and the TX and data lists.
hash(B, TXs, Nonce) when is_record(B, block) ->
	hash(B#block.hash, generate_block_data(TXs), Nonce);
hash(Hash, TXs, Nonce) ->
	crypto:hash(
		?HASH_ALG,
		<< Hash/binary, TXs/binary, Nonce/binary >>
	).

%% Create an independent hash from a block. Independent hashes verify a block's
%% contents in isolation and are stored in a node's hash list.
indep_hash(B) ->
	% TODO: Making hashing independent of the serialisation format.
	crypto:hash(?HASH_ALG, << (term_to_binary(B))/binary >>).

%% Spawn a miner and mine the current block synchronously. Used for testing.
%% Returns the nonce to use to add the block to the list.
mine(B, TXs) ->
	ar_mine:start(B#block.hash, B#block.diff, generate_block_data(TXs)),
	receive
		{work_complete, _Hash, _NewHash, _Diff, Nonce} ->
			Nonce
	end.

%% Return whether or not a transaction is found on a block list.
is_tx_on_block_list([], _) -> false;
is_tx_on_block_list([#block { txs = TXs }|Bs], TXID) ->
	case lists:member(TXID, [ T#tx.id || T <- TXs ]) of
		true -> true;
		false -> is_tx_on_block_list(Bs, TXID)
	end.

is_data_on_block_list(_, _) -> false.

%%% Block list validity tests.

%% Test validation of newly initiated block list.
init_verify_test() ->
	true = verify(init()).

%% Ensure the verification of block lists with a single empty block+genesis.
init_addempty_verify_test() ->
	true = verify(add(init(), [])).

%% Test verification of blocks with data and transactions attached.
init_add_verify_test() ->
	true = verify(add(init(), [ar_tx:new(<<"TEST TX">>), ar_tx:new(<<"TEST DATA1">>), ar_tx:new(<<"TESTDATA2">>)])).

%% Ensure the detection of forged blocks.
init_add_add_forge_add_verify_test() ->
	B2 = add(add(init(), []), [ar_tx:new(<<"TEST TX">>), ar_tx:new(<<"TEST DATA1">>), ar_tx:new(<<"TEST DATA2">>)]),
	ForgedB3 =
		[
			#block {
				nonce = <<>>,
				height = 3,
				hash = crypto:hash(?HASH_ALG, <<"NOT THE CORRECT HASH">>),
				txs = []
			}
		|B2],
	false = verify(add(ForgedB3, [ar_tx:new(<<"TEST TX2">>), ar_tx:new(<<"TEST DATA3">>)])).

%% A more 'subtle' version of above. Re-heahes the previous block, but with data removed.
init_add_add_forge_add_verify_subtle_test() ->
	B1 = add(init(), [ar_tx:new(<<"TEST TX0">>), ar_tx:new(<<"TEST DATA0">>)]),
	B2 = add(B1, [ar_tx:new(<<"TEST TX1">>), ar_tx:new(<<"TEST DATA1">>), ar_tx:new(<<"TEST DATA2">>)]),
	ForgedB3 =
		[
			#block {
				nonce = <<>>,
				height = 3,
				hash = hash(hd(B1), [], <<>>),
				txs = []
			}
		|B2],
	false = verify(add(ForgedB3, [ar_tx:new(<<"TEST TX2">>), ar_tx:new(<<"TEST DATA3">>)])).

%% Ensure that blocks with an invalid nonce are detect.
detect_invalid_nonce_test() ->
	B1 = add(init(), [ar_tx:new(<<"TEST TX">>), ar_tx:new(<<"TEST DATA1">>), ar_tx:new(<<"TESTDATA2">>)]),
	ForgedB2 = add(B1, [ar_tx:new(<<"FILTHY LIES">>)], <<"INCORRECT NONCE">>),
	false = verify(add(ForgedB2, [ar_tx:new(<<"NEW DATA">>)])).
