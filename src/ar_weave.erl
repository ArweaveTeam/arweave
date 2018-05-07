-module(ar_weave).
-export([init/0, init/1, init/2, add/1, add/2, add/3, add/4, add/5, add/6, add/10]).
-export([hash/2, indep_hash/1]).
-export([verify_indep/2]).
-export([calculate_recall_block/1, calculate_recall_block/2]).
-export([generate_hash_list/1]).
-export([is_data_on_block_list/2, is_tx_on_block_list/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Utilities for manipulating the ARK weave datastructure.

%% @doc Start a new block list. Optionally takes a list of wallet values
%% for the genesis block.
init() -> init(ar_util:genesis_wallets()).
init(WalletList) -> init(WalletList, ?DEFAULT_DIFF).
init(WalletList, StartingDiff) ->
	B0 =
		#block{
			height = 0,
			hash = crypto:strong_rand_bytes(32),
			nonce = crypto:strong_rand_bytes(32),
			txs = [],
			wallet_list = WalletList,
			hash_list = [],
			diff = StartingDiff
        },
    B1 = B0#block { last_retarget = B0#block.timestamp },
    [B1#block { indep_hash = indep_hash(B1) }].

%% @doc Add a new block to the weave, with assiocated TXs and archive data.
add(Bs) ->
    add(Bs, []).
add(Bs, TXs) ->
    add(Bs, TXs, generate_hash_list(Bs)).
add(Bs, TXs, HashList) ->
    add(Bs, TXs, HashList, <<>>).
add(Bs, TXs, HashList, unclaimed) ->
    add(Bs, TXs, HashList, <<>>);
add([B|Bs], TXs, HashList, RewardAddr) ->
    WalletList = ar_node:apply_mining_reward(
        ar_node:apply_txs(B#block.wallet_list, TXs),
        RewardAddr,
        TXs,
        length(HashList) - 1
    ),
    add([B|Bs], TXs, HashList, RewardAddr, WalletList).
add(Bs, TXs, HashList, RewardAddr, WalletList) ->
    add(Bs, TXs, HashList, RewardAddr, WalletList, []).
add([Hash|Bs], TXs, HashList, RewardAddr, WalletList, Tags) when is_binary(Hash) ->
    add(
        [ar_storage:read_block(Hash)|Bs],
        TXs,
        HashList,
        RewardAddr,
        WalletList,
        Tags
    );
add(Bs, TXs, HashList, RewardAddr, WalletList, Tags) ->
    RecallHash = ar_util:get_recall_hash(hd(Bs), HashList),
    RecallB = ar_storage:read_block(RecallHash),
    {Nonce, Timestamp, Diff} = mine(hd(Bs), RecallB, TXs, RewardAddr, Tags),
    add(
        Bs,
        TXs,
        HashList,
        RewardAddr,
        WalletList,
        Tags,
        RecallB,
        Diff,
        Nonce,
        Timestamp
    ).
add([Hash|Bs], RawTXs, HashList, RewardAddr, WalletList, Tags, RecallB, Diff, Nonce, Timestamp) when is_binary(Hash) ->
    add(
        [ar_storage:read_block(Hash)|Bs],
        RawTXs,
        HashList,
        RewardAddr,
        WalletList,
        Tags,
        RecallB,
        Diff,
        Nonce,
        Timestamp
    );
add([B|_Bs], RawTXs, HashList, RewardAddr, WalletList, Tags, RecallB, Diff, Nonce, Timestamp) ->
    % ar:d({ar_weave_add,{hashlist, HashList}, {walletlist, WalletList}, {txs, RawTXs}, {nonce, Nonce}, {diff, Diff}, {reward, RewardAddr}, {ts, Timestamp}, {tags, Tags} }),
    RecallB = ar_node:find_recall_block(HashList),
    TXs = [T#tx.id || T <- RawTXs],
	NewB =
		#block {
			nonce = Nonce,
			previous_block = B#block.indep_hash,
            timestamp = Timestamp,
            last_retarget =
                case ar_retarget:is_retarget_height(B#block.height + 1) of
                    true -> Timestamp;
                    false -> B#block.last_retarget
                end,
            diff = Diff,
            height = B#block.height + 1,
            hash = hash(
                ar_block:generate_block_data_segment(
                    B,
                    RecallB,
                    RawTXs,
                    RewardAddr,
                    Timestamp,
                    Tags
                ),
                Nonce
            ),
            % indep hash
            txs = TXs,
			hash_list = HashList,
			wallet_list = WalletList,
            reward_addr = RewardAddr,
            tags = Tags
        },
	[NewB#block { indep_hash = indep_hash(NewB) }|HashList].

%% @doc Take a complete block list and return a list of block hashes.
%% Throws an error if the block list is not complete.
generate_hash_list(undefined) -> [];
generate_hash_list([]) -> [];
generate_hash_list(Bs = [B|_]) ->
	generate_hash_list(Bs, B#block.height + 1).

generate_hash_list([B = #block { hash_list = BHL }|_], _) when is_list(BHL) ->
	[B#block.indep_hash|BHL];
generate_hash_list([], 0) -> [];
generate_hash_list([B|Bs], N) when is_record(B, block) ->
	[B#block.indep_hash|generate_hash_list(Bs, N - 1)];
generate_hash_list([Hash|Bs], N) when is_binary(Hash) ->
	[Hash|generate_hash_list(Bs, N - 1)].

%% @doc Verify a block from a hash list. Hash lists are stored in reverse order
verify_indep(#block{ height = 0 }, []) -> true;
verify_indep(B = #block { height = Height }, HashList) ->
	lists:nth(Height + 1, lists:reverse(HashList)) == indep_hash(B).

%% @doc Generate a recall block number from a block or a hash and block height.
calculate_recall_block(Hash) when is_binary(Hash) ->
	calculate_recall_block(ar_storage:read_block(Hash));
calculate_recall_block(B) when is_record(B, block) ->
	case B#block.height of
		0 -> 0;
		_ -> calculate_recall_block(B#block.indep_hash, B#block.height)
	end.
calculate_recall_block(IndepHash, Height) ->
	%ar:d({recall_indep_hash, binary:decode_unsigned(IndepHash)}),
	%ar:d({recall_height, Height}),
	binary:decode_unsigned(IndepHash) rem Height.

%% @doc Create the hash of the next block in the list, given a previous block,
%% and the TXs and the nonce.
hash(DataSegment, Nonce) ->
    % ar:d({hash, {data, DataSegment}, {nonce, Nonce}, {timestamp, Timestamp}}),
	crypto:hash(
		?HASH_ALG,
		<< Nonce/binary, DataSegment/binary >>
	).

%% @doc Create an independent hash from a block. Independent hashes
%% verify a block's contents in isolation and are stored in a node's hash list.
%% TODO: Check logic
indep_hash(B) ->
	crypto:hash(
		?HASH_ALG,
		list_to_binary(
			ar_serialize:jsonify(
				ar_serialize:block_to_json_struct(
					B#block { indep_hash = <<>> }
				)
			)
		)
	).

%% @doc Spawn a miner and mine the current block synchronously. Used for testing.
%% Returns the nonce to use to add the block to the list.
mine(B, RecallB, TXs, RewardAddr, Tags) ->
    ar:d({weave_mine, {block, B}, {recall, RecallB}, {tx, TXs}, {reward, RewardAddr}, {tags, Tags}}),
    ar_mine:start(B, RecallB, TXs, RewardAddr, Tags),
	receive
        {work_complete, TXs, _Hash, Diff, Nonce, Timestamp} ->
			{Nonce, Timestamp, Diff}
	end.

%% @doc Return whether or not a transaction is found on a block list.
is_tx_on_block_list([], _) -> false;
is_tx_on_block_list([Hash|Bs], TXID) when is_binary(Hash) ->
	is_tx_on_block_list([ar_storage:read_block(Hash)|Bs], TXID);
is_tx_on_block_list([#block { txs = TXs }|Bs], TXID) ->
	case lists:member(TXID, TXs) of
		true -> true;
		false -> is_tx_on_block_list(Bs, TXID)
	end.

is_data_on_block_list(_, _) -> false.


%%% Block list validity tests.

%% @doc Test validation of newly initiated block list.
% init_verify_test() ->
% 	true = verify(init()).

% %% @doc Ensure the verification of block lists with a single empty block+genesis.
% init_addempty_verify_test() ->
%     ar_node:start([], [B0] = init()),
% 	true = verify(add([B0], [])).

% %% @doc Test verification of blocks with data and transactions attached.
% init_add_verify_test() ->
% 	ar_storage:clear(),
% 	ar_storage:write_tx([TX1 = ar_tx:new(<<"TEST TX">>),TX2 = ar_tx:new(<<"TEST DATA1">>),TX3 = ar_tx:new(<<"TESTDATA2">>)]),
% 	true = verify(add(init(), [TX1, TX2, TX3])).

% %% @doc Ensure the detection of forged blocks.
% init_add_forge_add_verify_test() ->
% 	ar_storage:clear(),
% 	ar_storage:write_tx(
% 		[
% 			TX1 = ar_tx:new(<<"TEST TX">>),
% 			TX2 = ar_tx:new(<<"TEST DATA1">>),
% 			TX3 = ar_tx:new(<<"TESTDATA2">>),
% 			TX4 = ar_tx:new(<<"TESTDATA3">>)
% 		]
% 	),
% 	B2 = add(add(init(), []), [TX1, TX2, TX3]),
% 	ForgedB3 =
% 		[
% 			#block {
% 				nonce = <<>>,
% 				previous_block = (hd(B2))#block.indep_hash,
% 				height = 3,
% 				hash = crypto:hash(?HASH_ALG, <<"NOT THE CORRECT HASH">>),
% 				txs = [],
% 				last_retarget = ar:timestamp()
% 			}
% 		|B2],
% 	false = verify(add(ForgedB3, [TX3, TX4])).

% %% @doc A more 'subtle' version of above. Re-heahes the previous block, but with data removed.
% init_add_add_forge_add_verify_subtle_test() ->
% 	ar_storage:clear(),
% 	ar_storage:write_tx(
% 		[
% 			TX1 = ar_tx:new(<<"TEST TX0">>),
% 			TX2 = ar_tx:new(<<"TEST DATA0">>),
% 			TX3 = ar_tx:new(<<"TEST TX1">>),
% 			TX4 = ar_tx:new(<<"TEST DATA1">>),
% 			TX5 = ar_tx:new(<<"TEST DATA2">>),
% 			TX6 = ar_tx:new(<<"TEST TX2">>),
% 			TX7 = ar_tx:new(<<"TEST DATA3">>)
% 		]
% 	),
% 	B1 = add(init(), [TX1, TX2]),
% 	B2 = add(B1, [TX3, TX4, TX5]),
% 	ForgedB3 =
% 		[
% 			#block {
% 				nonce = <<>>,
% 				previous_block = (hd(B2))#block.indep_hash,
% 				height = 3,
% 				hash = hash(hd(B1), [], <<>>),
% 				txs = [],
% 				last_retarget = ar:timestamp()
% 			}
% 		|B2],
% 	false = verify(add(ForgedB3, [TX6, TX7])).

% %% @doc Ensure that blocks with an invalid nonce are detect.
% detect_invalid_nonce_test() ->
% 	ar_storage:clear(),
% 	ar_storage:write_tx(
% 		[
% 			TX1 = ar_tx:new(<<"TEST TX">>),
% 			TX2 = ar_tx:new(<<"TEST DATA1">>),
% 			TX3 = ar_tx:new(<<"TESTDATA2">>),
% 			TX4 = ar_tx:new(<<"FILTHY LIES">>),
% 			TX5 = ar_tx:new(<<"NEW DATA">>)
% 		]
% 	),
% 	B1 = add(init([]), [TX1, TX2, TX3]),
% 	ForgedB2 = add(B1, [TX4], <<"INCORRECT NONCE">>),
% 	[B|Bs] = add(ForgedB2, [TX5]),
% 	false = verify([B#block{nonce = <<"INCORRECT NONCE">>}|Bs]).

% no_tx_fail_verify_test() ->
% 	ar_storage:clear(),
% 	TX1 = ar_tx:new(<<"TEST TX0">>),
% 	TX2 = ar_tx:new(<<"TEST DATA0">>),
% 	B1 = add(init(), [TX1, TX2]),
% 	ar_storage:clear(),
% 	false = verify(B1).
