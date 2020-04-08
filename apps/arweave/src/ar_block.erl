-module(ar_block).

-export([block_to_binary/1, block_field_size_limit/1]).
-export([verify_dep_hash/2, verify_indep_hash/1, verify_timestamp/1]).
-export([verify_height/2, verify_last_retarget/2, verify_previous_block/2]).
-export([verify_block_hash_list/2, verify_wallet_list/4, verify_weave_size/3]).
-export([verify_cumulative_diff/2, verify_block_hash_list_merkle/3]).
-export([verify_tx_root/1]).
-export([hash_wallet_list/2, hash_wallet_list/3, hash_wallet_list_without_reward_wallet/2]).
-export([hash_wallet_list_pre_2_0/1]).
-export([generate_block_data_segment/1, generate_block_data_segment/3, generate_block_data_segment_base/1]).
-export([generate_hash_list_for_block/2]).
-export([generate_tx_root_for_block/1, generate_size_tagged_list_from_txs/1]).
-export([generate_tx_tree/1, generate_tx_tree/2]).
-export([compute_hash_list_merkle/2]).

%% NOT used. Exported for the historical record.
-export([generate_block_data_segment_pre_2_0/6]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

hash_wallet_list(_Height, _RewardAddr, WalletListHash) when is_binary(WalletListHash) ->
	WalletListHash;
hash_wallet_list(Height, RewardAddr, WalletList) ->
	case Height < ar_fork:height_2_0() of
		true ->
			hash_wallet_list_pre_2_0(WalletList);
		false ->
			{RewardWallet, NoRewardWalletListHash} =
				hash_wallet_list_without_reward_wallet(RewardAddr, WalletList),
			hash_wallet_list(RewardWallet, NoRewardWalletListHash)
	end.

hash_wallet_list_pre_2_0(WalletList) ->
	Bin =
		<<
			<< Addr/binary, (binary:encode_unsigned(Balance))/binary, LastTX/binary >>
		||
			{Addr, Balance, LastTX} <- WalletList
		>>,
	crypto:hash(?HASH_ALG, Bin).

hash_wallet_list_without_reward_wallet(RewardAddr, WalletList) ->
	{RewardWallet, NoRewardWalletList} =
		case lists:keytake(RewardAddr, 1, WalletList) of
			{value, RW, NewWalletList} ->
				{RW, NewWalletList};
			false ->
				{unclaimed, WalletList}
		end,
	NoRewardWLH = ar_deep_hash:hash([
		[A, binary:encode_unsigned(B), Anchor] || {A, B, Anchor} <- NoRewardWalletList
	]),
	{RewardWallet, NoRewardWLH}.

hash_wallet_list(RewardWallet, NoRewardWalletListHash) ->
	ar_deep_hash:hash([
		NoRewardWalletListHash,
		case RewardWallet of
			unclaimed ->
				<<"unclaimed">>;
			{Address, Balance, Anchor} ->
				[Address, binary:encode_unsigned(Balance), Anchor]
		end
	]).

%% @doc Generate the TX tree and set the TX root for a block.
generate_tx_tree(B) ->
	generate_tx_tree(B, generate_size_tagged_list_from_txs(B#block.txs)).
generate_tx_tree(B, SizeTaggedTXs) ->
	{Root, Tree} = ar_merkle:generate_tree(SizeTaggedTXs),
	B#block { tx_tree = Tree, tx_root = Root }.

generate_size_tagged_list_from_txs(TXs) ->
	lists:reverse(
		element(2,
			lists:foldl(
				fun(TX, {Pos, List}) ->
					End = Pos + TX#tx.data_size,
					{End, [{get_tx_data_root(TX), End} | List]}
				end,
				{0, []},
				lists:sort(TXs)
			)
		)
	).

%% @doc Find the appropriate block hash list for a block, from a
%% block index.
generate_hash_list_for_block(_BlockOrHash, []) -> [];
generate_hash_list_for_block(B, BI) when ?IS_BLOCK(B) ->
	generate_hash_list_for_block(B#block.indep_hash, BI);
generate_hash_list_for_block(Hash, BI) ->
	do_generate_hash_list_for_block(Hash, BI).

do_generate_hash_list_for_block(_, []) ->
	error(cannot_generate_hash_list);
do_generate_hash_list_for_block(IndepHash, [{IndepHash, _, _} | BI]) -> ?BI_TO_BHL(BI);
do_generate_hash_list_for_block(IndepHash, [_ | Rest]) ->
	do_generate_hash_list_for_block(IndepHash, Rest).

%% @doc Generate a hashable binary from a #block object.
block_to_binary(B) ->
	{ok, WalletList} = ar_storage:read_wallet_list(B#block.wallet_list),
	<<
		(B#block.nonce)/binary,
		(B#block.previous_block)/binary,
		(list_to_binary(integer_to_list(B#block.timestamp)))/binary,
		(list_to_binary(integer_to_list(B#block.last_retarget)))/binary,
		(list_to_binary(integer_to_list(B#block.diff)))/binary,
		(list_to_binary(integer_to_list(B#block.height)))/binary,
		(B#block.hash)/binary,
		(B#block.indep_hash)/binary,
		(
			binary:list_to_bin(
				lists:map(
					fun ar_tx:tx_to_binary/1,
					lists:sort(ar_storage:read_tx(B#block.txs))
				)
			)
		)/binary,
		(list_to_binary(B#block.hash_list))/binary,
		(
			binary:list_to_bin(
				lists:map(
					fun ar_wallet:to_binary/1,
					WalletList
				)
			)
		)/binary,
		(
			case is_atom(B#block.reward_addr) of
				true -> <<>>;
				false -> B#block.reward_addr
			end
		)/binary,
		(list_to_binary(B#block.tags))/binary,
		(list_to_binary(integer_to_list(B#block.weave_size)))/binary
	>>.

%% @doc Given a block checks that the lengths conform to the specified limits.
block_field_size_limit(B = #block { reward_addr = unclaimed }) ->
	block_field_size_limit(B#block { reward_addr = <<>> });
block_field_size_limit(B) ->
	DiffBytesLimit = case ar_fork:height_1_8() of
		H when B#block.height >= H ->
			78;
		_ ->
			10
	end,
	{ChunkSize, DataPathSize} =
		case B#block.poa of
			POA when is_record(POA, poa) ->
				{
					byte_size((B#block.poa)#poa.chunk),
					byte_size((B#block.poa)#poa.data_path)
				};
			_ -> {0, 0}
		end,
	Check = (byte_size(B#block.nonce) =< 512) and
		(byte_size(B#block.previous_block) =< 48) and
		(byte_size(integer_to_binary(B#block.timestamp)) =< 12) and
		(byte_size(integer_to_binary(B#block.last_retarget)) =< 12) and
		(byte_size(integer_to_binary(B#block.diff)) =< DiffBytesLimit) and
		(byte_size(integer_to_binary(B#block.height)) =< 20) and
		(byte_size(B#block.hash) =< 48) and
		(byte_size(B#block.indep_hash) =< 48) and
		(byte_size(B#block.reward_addr) =< 32) and
		(byte_size(list_to_binary(B#block.tags)) =< 2048) and
		(byte_size(integer_to_binary(B#block.weave_size)) =< 64) and
		(byte_size(integer_to_binary(B#block.block_size)) =< 64) and
		(ChunkSize =< ?DATA_CHUNK_SIZE) and
		(DataPathSize =< ?MAX_PATH_SIZE),
	case Check of
		false ->
			ar:info(
				[
					{event, received_block_with_invalid_field_size},
					{nonce, byte_size(B#block.nonce)},
					{previous_block, byte_size(B#block.previous_block)},
					{timestamp, byte_size(integer_to_binary(B#block.timestamp))},
					{last_retarget, byte_size(integer_to_binary(B#block.last_retarget))},
					{diff, byte_size(integer_to_binary(B#block.diff))},
					{height, byte_size(integer_to_binary(B#block.height))},
					{hash, byte_size(B#block.hash)},
					{indep_hash, byte_size(B#block.indep_hash)},
					{reward_addr, byte_size(B#block.reward_addr)},
					{tags, byte_size(list_to_binary(B#block.tags))},
					{weave_size, byte_size(integer_to_binary(B#block.weave_size))},
					{block_size, byte_size(integer_to_binary(B#block.block_size))}
				]
			);
		_ ->
			ok
	end,
	Check.

compute_hash_list_merkle(B, BI) ->
	NewHeight = B#block.height + 1,
	Fork_2_0 = ar_fork:height_2_0(),
	case NewHeight of
		_ when NewHeight < ?FORK_1_6 ->
			<<>>;
		?FORK_1_6 ->
			ar_unbalanced_merkle:hash_list_to_merkle_root(B#block.hash_list);
		_ when NewHeight < Fork_2_0 ->
			ar_unbalanced_merkle:root(B#block.hash_list_merkle, B#block.indep_hash);
		Fork_2_0 ->
			ar_unbalanced_merkle:block_index_to_merkle_root(BI);
		_ ->
			ar_unbalanced_merkle:root(
				B#block.hash_list_merkle,
				{B#block.indep_hash, B#block.weave_size, B#block.tx_root},
				fun ar_unbalanced_merkle:hash_block_index_entry/1
			)
	end.

%% @doc Generate a block data segment.
%% Block data segment is combined with a nonce to compute a PoW hash.
%% Also, it is combined with a nonce and the corresponding PoW hash
%% to produce the independent hash.
generate_block_data_segment(B) ->
	generate_block_data_segment(
		generate_block_data_segment_base(B),
		B#block.hash_list_merkle,
		#{
			timestamp => B#block.timestamp,
			last_retarget => B#block.last_retarget,
			diff => B#block.diff,
			cumulative_diff => B#block.cumulative_diff,
			reward_pool => B#block.reward_pool,
			wallet_list_hash => B#block.wallet_list_hash
		}
	).

generate_block_data_segment(BDSBase, BlockIndexMerkle, TimeDependentParams) ->
	#{
		timestamp := Timestamp,
		last_retarget := LastRetarget,
		diff := Diff,
		cumulative_diff := CDiff,
		reward_pool := RewardPool,
		wallet_list_hash := WLH
	} = TimeDependentParams,
	ar_deep_hash:hash([
		BDSBase,
		integer_to_binary(Timestamp),
		integer_to_binary(LastRetarget),
		integer_to_binary(Diff),
		integer_to_binary(CDiff),
		integer_to_binary(RewardPool),
		WLH,
		BlockIndexMerkle
	]).

%% @doc Generate a hash, which is used to produce a block data segment
%% when combined with the time-dependent parameters, which frequently
%% change during mining - timestamp, last retarget timestamp, difficulty,
%% cumulative difficulty, miner's wallet, reward pool. Also excludes
%% the merkle root of the block index, which is hashed with the rest
%% as the last step, to allow verifiers to quickly validate PoW against
%% the current state.
generate_block_data_segment_base(B) ->
	BDSBase = ar_deep_hash:hash([
		integer_to_binary(B#block.height),
		B#block.previous_block,
		B#block.tx_root,
		lists:map(fun ar_weave:tx_id/1, B#block.txs),
		integer_to_binary(B#block.block_size),
		integer_to_binary(B#block.weave_size),
		case B#block.reward_addr of
			unclaimed ->
				<<"unclaimed">>;
			_ ->
				B#block.reward_addr
		end,
		ar_tx:tags_to_list(B#block.tags),
		poa_to_list(B#block.poa)
	]),
	BDSBase.

poa_to_list(POA) ->
	[
		integer_to_binary(POA#poa.option),
		POA#poa.tx_path,
		POA#poa.data_path,
		POA#poa.chunk
	].

%% @doc Generate a hashable data segment for a block from the preceding block,
%% the preceding block's recall block, TXs to be mined, reward address and tags.
generate_block_data_segment_pre_2_0(PrecedingB, PrecedingRecallB, [unavailable], RewardAddr, Time, Tags) ->
	generate_block_data_segment_pre_2_0(
		PrecedingB,
		PrecedingRecallB,
		[],
		RewardAddr,
		Time,
		Tags
	);
generate_block_data_segment_pre_2_0(PrecedingB, PrecedingRecallB, TXs, unclaimed, Time, Tags) ->
	generate_block_data_segment_pre_2_0(
		PrecedingB,
		PrecedingRecallB,
		TXs,
		<<>>,
		Time,
		Tags
	);
generate_block_data_segment_pre_2_0(PrecedingB, PrecedingRecallB, TXs, RewardAddr, Time, Tags) ->
	{_, BDS} = generate_block_data_segment_and_pieces(PrecedingB, PrecedingRecallB, TXs, RewardAddr, Time, Tags),
	BDS.

generate_block_data_segment_and_pieces(PrecedingB, PrecedingRecallB, TXs, RewardAddr, Time, Tags) ->
	NewHeight = PrecedingB#block.height + 1,
	Retarget =
		case ar_retarget:is_retarget_height(NewHeight) of
			true -> Time;
			false -> PrecedingB#block.last_retarget
		end,
	WeaveSize = PrecedingB#block.weave_size +
		lists:foldl(
			fun(TX, Acc) ->
				Acc + TX#tx.data_size
			end,
			0,
			TXs
		),
	NewDiff = ar_retarget:maybe_retarget(
		PrecedingB#block.height + 1,
		PrecedingB#block.diff,
		Time,
		PrecedingB#block.last_retarget
	),
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			PrecedingB#block.reward_pool,
			TXs,
			RewardAddr,
			PrecedingRecallB,
			WeaveSize,
			PrecedingB#block.height + 1,
			NewDiff,
			Time
		),
	NewWalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(PrecedingB#block.wallet_list, TXs, PrecedingB#block.height),
			RewardAddr,
			FinderReward,
			length(PrecedingB#block.hash_list) - 1
		),
	MR =
		case PrecedingB#block.height >= ?FORK_1_6 of
			true -> PrecedingB#block.hash_list_merkle;
			false -> <<>>
		end,
	Pieces = [
		<<
			(PrecedingB#block.indep_hash)/binary,
			(PrecedingB#block.hash)/binary
		>>,
		<<
			(integer_to_binary(Time))/binary,
			(integer_to_binary(Retarget))/binary
		>>,
		<<
			(integer_to_binary(PrecedingB#block.height + 1))/binary,
			(
				list_to_binary(
					[PrecedingB#block.indep_hash | PrecedingB#block.hash_list]
				)
			)/binary
		>>,
		<<
			(
				binary:list_to_bin(
					lists:map(
						fun ar_wallet:to_binary/1,
						NewWalletList
					)
				)
			)/binary
		>>,
		<<
			(
				case is_atom(RewardAddr) of
					true -> <<>>;
					false -> RewardAddr
				end
			)/binary,
			(list_to_binary(Tags))/binary
		>>,
		<<
			(integer_to_binary(RewardPool))/binary
		>>,
		<<
			(block_to_binary(PrecedingRecallB))/binary,
			(
				binary:list_to_bin(
					lists:map(
						fun ar_tx:tx_to_binary/1,
						TXs
					)
				)
			)/binary,
			MR/binary
		>>
	],
	{Pieces, crypto:hash(
		?MINING_HASH_ALG,
		<< Piece || Piece <- Pieces >>
	)}.

%% @doc Verify the independant hash of a given block is valid
verify_indep_hash(Block = #block { indep_hash = Indep }) ->
	Indep == ar_weave:indep_hash(Block).

%% @doc Verify the dependent hash of a given block is valid
verify_dep_hash(NewB, BDSHash) ->
	NewB#block.hash == BDSHash.

verify_tx_root(B) ->
	B#block.tx_root == generate_tx_root_for_block(B).

%% @doc Given a list of TXs in various formats, or a block, generate the
%% correct TX merkle tree root.
generate_tx_root_for_block(B) when is_record(B, block) ->
	generate_tx_root_for_block(B#block.txs);
generate_tx_root_for_block(TXIDs = [TXID | _]) when is_binary(TXID) ->
	generate_tx_root_for_block(ar_storage:read_tx(TXIDs));
generate_tx_root_for_block([]) ->
	<<>>;
generate_tx_root_for_block(TXs = [TX | _]) when is_record(TX, tx) ->
	TXSizePairs = generate_size_tagged_list_from_txs(TXs),
	{Root, _Tree} = ar_merkle:generate_tree(TXSizePairs),
	Root.

get_tx_data_root(#tx{ format = 2, data_root = DataRoot }) ->
	DataRoot;
get_tx_data_root(TX) ->
	(ar_tx:generate_chunk_tree(TX))#tx.data_root.

%% @doc Verify the block timestamp is not too far in the future nor too far in
%% the past. We calculate the maximum reasonable clock difference between any
%% two nodes. This is a simplification since there is a chaining effect in the
%% network which we don't take into account. Instead, we assume two nodes can
%% deviate JOIN_CLOCK_TOLERANCE seconds in the opposite direction from each
%% other.
verify_timestamp(B) ->
	CurrentTime = os:system_time(seconds),
	MaxNodesClockDeviation = ?JOIN_CLOCK_TOLERANCE * 2 + ?CLOCK_DRIFT_MAX,
	(
		B#block.timestamp =< CurrentTime + MaxNodesClockDeviation
		andalso
		B#block.timestamp >= CurrentTime - lists:sum([
			?MINING_TIMESTAMP_REFRESH_INTERVAL,
			?MAX_BLOCK_PROPAGATION_TIME,
			MaxNodesClockDeviation
		])
	).

%% @doc Verify the height of the new block is the one higher than the
%% current height.
verify_height(NewB, OldB) ->
	NewB#block.height == (OldB#block.height + 1).

%% @doc Verify the retarget timestamp on NewB is correct.
verify_last_retarget(NewB, OldB) ->
	case ar_retarget:is_retarget_height(NewB#block.height) of
		true ->
			NewB#block.last_retarget == NewB#block.timestamp;
		false ->
			NewB#block.last_retarget == OldB#block.last_retarget
	end.

%% @doc Verify that the previous_block hash of the new block is the indep_hash
%% of the current block.
verify_previous_block(NewB, OldB) ->
	OldB#block.indep_hash == NewB#block.previous_block.

%% @doc Verify that the new block's hash_list is the current block's
%% hash_list + indep_hash, until ?FORK_1_6.
verify_block_hash_list(NewB, OldB) when NewB#block.height < ?FORK_1_6 ->
	NewB#block.hash_list == [OldB#block.indep_hash | OldB#block.hash_list];
verify_block_hash_list(_NewB, _OldB) -> true.

%% @doc Verify that the new blocks wallet_list and reward_pool matches that
%% generated by applying, the block miner reward and mined TXs to the current
%% (old) blocks wallet_list and reward pool.
verify_wallet_list(NewB, OldB, POA, NewTXs) ->
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			OldB#block.reward_pool,
			NewTXs,
			NewB#block.reward_addr,
			POA,
			NewB#block.weave_size,
			OldB#block.height + 1,
			NewB#block.diff,
			NewB#block.timestamp
		),
	RewardAddress = case OldB#block.reward_addr of
		unclaimed -> <<"unclaimed">>;
		_         -> ar_util:encode(OldB#block.reward_addr)
	end,
	ar:info(
		[
			{event, verifying_finder_reward},
			{finder_reward, FinderReward},
			{new_reward_pool, RewardPool},
			{reward_address, RewardAddress},
			{old_reward_pool, OldB#block.reward_pool},
			{txs, length(NewTXs)},
			{weave_size, NewB#block.weave_size},
			{height, OldB#block.height + 1}
		]
	),
	(NewB#block.reward_pool == RewardPool) and
	((NewB#block.wallet_list) ==
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(OldB#block.wallet_list, NewTXs, OldB#block.height),
			NewB#block.reward_addr,
			FinderReward,
			NewB#block.height
		)).

verify_weave_size(NewB, OldB, TXs) ->
	NewB#block.weave_size == lists:foldl(
		fun(TX, Acc) ->
			Acc + TX#tx.data_size
		end,
		OldB#block.weave_size,
		TXs
	).

%% @doc Ensure that after the 1.6 release cumulative difficulty is enforced.
verify_cumulative_diff(NewB, OldB) ->
	NewB#block.cumulative_diff ==
		ar_difficulty:next_cumulative_diff(
			OldB#block.cumulative_diff,
			NewB#block.diff,
			NewB#block.height
		).

%% @doc After 1.6 fork check that the given merkle root in a new block is valid.
verify_block_hash_list_merkle(NewB, CurrentB, BI) when NewB#block.height > ?FORK_1_6 ->
	Fork_2_0 = ar_fork:height_2_0(),
	case NewB#block.height of
		H when H < Fork_2_0 ->
			NewB#block.hash_list_merkle ==
				ar_unbalanced_merkle:root(CurrentB#block.hash_list_merkle, CurrentB#block.indep_hash);
		Fork_2_0 ->
			NewB#block.hash_list_merkle == ar_unbalanced_merkle:block_index_to_merkle_root(BI);
		_ ->
			NewB#block.hash_list_merkle ==
				ar_unbalanced_merkle:root(
					CurrentB#block.hash_list_merkle,
					{CurrentB#block.indep_hash, CurrentB#block.weave_size, CurrentB#block.tx_root},
					fun ar_unbalanced_merkle:hash_block_index_entry/1
				)
	end;
verify_block_hash_list_merkle(NewB, _CurrentB, _) when NewB#block.height < ?FORK_1_6 ->
	NewB#block.hash_list_merkle == <<>>;
verify_block_hash_list_merkle(NewB, CurrentB, _) when NewB#block.height == ?FORK_1_6 ->
	NewB#block.hash_list_merkle == ar_unbalanced_merkle:hash_list_to_merkle_root(CurrentB#block.hash_list).

%% Tests: ar_block

hash_list_gen_test() ->
	ar_storage:clear(),
	B0s = [B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	B1s = [B1 | _] = ar_weave:add(B0s, []),
	ar_storage:write_block(B1),
	B2s = [B2 | _] = ar_weave:add(B1s, []),
	ar_storage:write_block(B2),
	[_ | BI] = ar_weave:add(B2s, []),
	HL1 = B1#block.hash_list,
	HL2 = B2#block.hash_list,
	HL1 = generate_hash_list_for_block(B1, BI),
	HL2 = generate_hash_list_for_block(B2#block.indep_hash, BI).
