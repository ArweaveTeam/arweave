-module(ar_block).
-export([block_to_binary/1, block_field_size_limit/1]).
-export([get_recall_block/5, get_recall_block/6]).
-export([verify_dep_hash/2, verify_indep_hash/1, verify_timestamp/2]).
-export([verify_height/2, verify_last_retarget/1, verify_previous_block/2]).
-export([verify_block_hash_list/2, verify_wallet_list/4, verify_weave_size/3]).
-export([hash_wallet_list/1]).
-export([encrypt_block/2, encrypt_block/3]).
-export([encrypt_full_block/2, encrypt_full_block/3]).
-export([decrypt_block/4]).
-export([decrypt_full_block/4]).
-export([generate_block_key/2]).
-export([generate_block_from_shadow/2, generate_block_data_segment/6]).
-export([generate_hash_list_for_block/2]).
-export([verify_cumulative_diff/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%% @doc Generate a re-producible hash from a wallet list.
hash_wallet_list(WalletList) ->
	Bin =
		<<
			<< Addr/binary, (binary:encode_unsigned(Balance))/binary, LastTX/binary >>
		||
			{Addr, Balance, LastTX} <- WalletList
		>>,
	crypto:hash(?HASH_ALG, Bin).

%% @doc Find the appropriate block hash list for a block/indep. hash, from a
%% block hash list further down the weave.
generate_hash_list_for_block(_Block1IndepHash, []) -> [];
generate_hash_list_for_block(B, CurrentB) when ?IS_BLOCK(CurrentB) ->
	generate_hash_list_for_block(B, CurrentB#block.indep_hash);
generate_hash_list_for_block(B, BHL) when ?IS_BLOCK(B) ->
	generate_hash_list_for_block(B#block.indep_hash, BHL);
generate_hash_list_for_block(IndepHash, BHL) ->
	do_generate_hash_list_for_block(IndepHash, BHL).

do_generate_hash_list_for_block(_, []) ->
	error(cannot_generate_block_hash_list);
do_generate_hash_list_for_block(IndepHash, [IndepHash|BHL]) -> BHL;
do_generate_hash_list_for_block(IndepHash, [_|Rest]) ->
	do_generate_hash_list_for_block(IndepHash, Rest).

%% @doc Encrypt a recall block. Encryption key is derived from
%% the contents of the recall block and the hash of the current block
encrypt_block(R, B) when ?IS_BLOCK(B) -> encrypt_block(R, B#block.indep_hash);
encrypt_block(R, Hash) ->
	Recall =
		ar_serialize:jsonify(
			ar_serialize:block_to_json_struct(R)
		),
	encrypt_block(
		Recall,
		crypto:hash(?HASH_ALG,<<Hash/binary, Recall/binary>>),
		_Nonce = binary:part(Hash, 0, 16)
	).
encrypt_block(R, Key, Nonce) when ?IS_BLOCK(R) ->
	encrypt_block(
		ar_serialize:jsonify(
			ar_serialize:block_to_json_struct(R)
		),
		Key,
		Nonce
	);
encrypt_block(Recall, Key, Nonce) ->
	PlainText = pad_to_length(Recall),
	CipherText =
		crypto:block_encrypt(
			aes_cbc,
			Key,
			Nonce,
			PlainText
		),
	CipherText.

%% @doc Decrypt a recall block
decrypt_block(B, CipherText, Key, Nonce)
		when ?IS_BLOCK(B)->
	decrypt_block(B#block.indep_hash, CipherText, Key, Nonce);
decrypt_block(_Hash, CipherText, Key, Nonce) ->
	if
		(Key == <<>>) or (Nonce == <<>>) -> unavailable;
		true ->
			PaddedPlainText =
				crypto:block_decrypt(
					aes_cbc,
					Key,
					Nonce,
					CipherText
				),
			PlainText = binary_to_list(unpad_binary(PaddedPlainText)),
			RJSON = ar_serialize:dejsonify(PlainText),
			ar_serialize:json_struct_to_block(RJSON)
	end.

%% @doc Encrypt a recall block. Encryption key is derived from
%% the contents of the recall block and the hash of the current block
encrypt_full_block(R, B) when ?IS_BLOCK(B) ->
	encrypt_full_block(R, B#block.indep_hash);
encrypt_full_block(R, Hash) ->
	Recall =
		ar_serialize:jsonify(
			ar_serialize:full_block_to_json_struct(R)
		),
	encrypt_full_block(
		Recall,
		crypto:hash(?HASH_ALG,<<Hash/binary, Recall/binary>>),
		_Nonce = binary:part(Hash, 0, 16)
	).
encrypt_full_block(R, Key, Nonce) when ?IS_BLOCK(R) ->
	encrypt_full_block(
		ar_serialize:jsonify(
			ar_serialize:full_block_to_json_struct(R)
		),
		Key,
		Nonce
	);
encrypt_full_block(Recall, Key, Nonce) ->
	PlainText = pad_to_length(Recall),
	CipherText =
		crypto:block_encrypt(
			aes_cbc,
			Key,
			Nonce,
			PlainText
		),
	CipherText.

%% @doc Decrypt a recall block
decrypt_full_block(B, CipherText, Key, Nonce) when ?IS_BLOCK(B)->
	decrypt_full_block(B#block.indep_hash, CipherText, Key, Nonce);
decrypt_full_block(_Hash, CipherText, Key, Nonce) ->
	if
		(Key == <<>>) or (Nonce == <<>>) -> unavailable;
		true ->
			PaddedPlainText =
				crypto:block_decrypt(
					aes_cbc,
					Key,
					Nonce,
					CipherText
				),
			PlainText = binary_to_list(unpad_binary(PaddedPlainText)),
			RJSON = ar_serialize:dejsonify(PlainText),
			ar_serialize:json_struct_to_full_block(RJSON)
	end.


%% @doc derive the key for a given recall block, given the
%% recall block and current block
generate_block_key(R, B) when ?IS_BLOCK(B) ->
	generate_block_key(R, B#block.indep_hash);
generate_block_key(R, Hash) ->
	Recall =
		ar_serialize:jsonify(
			ar_serialize:full_block_to_json_struct(R)
		),
	crypto:hash(?HASH_ALG,<<Hash/binary, Recall/binary>>).

%% @doc Pad a binary to the nearest mutliple of the block
%% cipher length (32 bytes)
pad_to_length(Binary) ->
	Pad = (32 - ((byte_size(Binary)+1) rem 32)),
	<<Binary/binary, 1, 0:(Pad*8)>>.

%% @doc Unpad a binary padded using the method above
unpad_binary(Binary) ->
	ar_util:rev_bin(do_unpad_binary(ar_util:rev_bin(Binary))).
do_unpad_binary(Binary) ->
	case Binary of
		<< 0:8, Rest/binary >> -> do_unpad_binary(Rest);
		<< 1:8, Rest/binary >> -> Rest
	end.

%% @doc Generate a hashable binary from a #block object.
block_to_binary(B) ->
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
					B#block.wallet_list
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
	Check = (byte_size(B#block.nonce) =< 512) and
	(byte_size(B#block.previous_block) =< 48) and
	(byte_size(integer_to_binary(B#block.timestamp)) =< 12) and
	(byte_size(integer_to_binary(B#block.last_retarget)) =< 12) and
	(byte_size(integer_to_binary(B#block.diff)) =< 10) and
	(byte_size(integer_to_binary(B#block.height)) =< 20) and
	(byte_size(B#block.hash) =< 48) and
	(byte_size(B#block.indep_hash) =< 48) and
	(byte_size(B#block.reward_addr) =< 32) and
	(byte_size(list_to_binary(B#block.tags)) =< 2048) and
	(byte_size(integer_to_binary(B#block.weave_size)) =< 64) and
	(byte_size(integer_to_binary(B#block.block_size)) =< 64),
	% mue: report of wrong field size.
	case Check of
		false ->
			ar:report(
				[
					invalid_block_field_size,
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

%% @docs Generate a hashable data segment for a block from the current
%% block, recall block, TXs to be mined, reward address and tags.
generate_block_data_segment(CurrentB, RecallB, [unavailable], RewardAddr, Time, Tags) ->
	generate_block_data_segment(
		CurrentB,
		RecallB,
		[],
		RewardAddr,
		Time,
		Tags
	);
generate_block_data_segment(CurrentB, RecallB, TXs, unclaimed, Time, Tags) ->
	generate_block_data_segment(
		CurrentB,
		RecallB,
		TXs,
		<<>>,
		Time,
		Tags
	);
generate_block_data_segment(CurrentB, RecallB, TXs, RewardAddr, Time, Tags) ->
	Retarget = case
		ar_retarget:is_retarget_height(CurrentB#block.height + 1)
	of
		true -> Time;
		false -> CurrentB#block.last_retarget
	end,
	WeaveSize = CurrentB#block.weave_size +
		lists:foldl(
			fun(TX, Acc) ->
				Acc + byte_size(TX#tx.data)
			end,
			0,
			TXs
		),
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			CurrentB#block.reward_pool,
			TXs,
			RewardAddr,
			ar_node_utils:calculate_proportion(
				RecallB#block.block_size,
				WeaveSize,
				CurrentB#block.height + 1
			)
		),
	NewWalletList =
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(CurrentB#block.wallet_list, TXs),
			RewardAddr,
			FinderReward,
			length(CurrentB#block.hash_list) - 1
		),
	% ar:d({indep, CurrentB#block.indep_hash}),
	% ar:d({retarget, integer_to_binary(Retarget)}),
	% ar:d({height, integer_to_binary(CurrentB#block.height + 1)}),
	% ar:d({wallets,
	%	  binary:list_to_bin(
	%		  lists:map(fun ar_wallet:to_binary/1, NewWalletList)
	%	  )
	% }),
	% ar:d({reward,
	%	  case is_atom(RewardAddr) of
	%		  true -> <<>>;
	%		  false -> RewardAddr
	%	  end
	% }),
	% ar:d({tags, list_to_binary(Tags)}),
	% ar:d({recall, byte_size(block_to_binary(RecallB))}),
	% ar:d({txs, binary:list_to_bin(lists:map(fun ar_tx:tx_to_binary/1, TXs))}),
	crypto:hash(
		?MINING_HASH_ALG,
		<<
			(CurrentB#block.indep_hash)/binary,
			(CurrentB#block.hash)/binary,
			(integer_to_binary(Time))/binary,
			(integer_to_binary(Retarget))/binary,
			(integer_to_binary(CurrentB#block.height + 1))/binary,
			(
				list_to_binary(
					[CurrentB#block.indep_hash | CurrentB#block.hash_list]
				)
			)/binary,
			(
				binary:list_to_bin(
					lists:map(
						fun ar_wallet:to_binary/1,
						NewWalletList
					)
				)
			)/binary,
			(
				case is_atom(RewardAddr) of
					true -> <<>>;
					false -> RewardAddr
				end
			)/binary,
			(list_to_binary(Tags))/binary,
			(integer_to_binary(RewardPool))/binary,
			(block_to_binary(RecallB))/binary,
			(
				binary:list_to_bin(
					lists:map(
						fun ar_tx:tx_to_binary/1,
						TXs
					)
				)
			)/binary
		>>
	).

%% @doc Verify the independant hash of a given block is valid
verify_indep_hash(Block = #block { indep_hash = Indep }) ->
	Indep == ar_weave:indep_hash(Block).

%% @doc Verify the dependent hash of a given block is valid
verify_dep_hash(NewB, BlockDataSegmentHash) ->
	NewB#block.hash == BlockDataSegmentHash.

%% @doc Verify that the block was created within the last ten minutes
verify_timestamp(Timestamp, NewB) ->
	(NewB#block.timestamp - Timestamp) =< 600.

%% @doc Verify the height of the new block is the one higher than the
%% current height.
verify_height(NewB, OldB) ->
	NewB#block.height == (OldB#block.height + 1).

%% @doc Verify that the last retarget timestamp is older or as old as the
%%	blocks timestamp.
verify_last_retarget(NewB) ->
	(NewB#block.timestamp - NewB#block.last_retarget) >= 0.

%% @doc Verify that the previous_block hash of the new block is the indep_hash
%% of the current block.
verify_previous_block(NewB, OldB) ->
	OldB#block.indep_hash == NewB#block.previous_block.

%% @doc Verify that the new blocks hash_list is the current blocks
%% hash_list + indep_hash.
verify_block_hash_list(NewB, OldB) ->
	NewB#block.hash_list == ([OldB#block.indep_hash | OldB#block.hash_list]).

%% @doc Verify that the new blocks wallet_list and reward_pool matches that
%% generated by applying, the block miner reward and mined TXs to the current
%% (old) blocks wallet_list and reward pool.
verify_wallet_list(NewB, OldB, RecallB, NewTXs) ->
	{FinderReward, RewardPool} =
		ar_node_utils:calculate_reward_pool(
			OldB#block.reward_pool,
			NewTXs,
			NewB#block.reward_addr,
			ar_node_utils:calculate_proportion(
				RecallB#block.block_size,
				NewB#block.weave_size,
				length(NewB#block.hash_list)
			)
		),
	RewardAddress = case OldB#block.reward_addr of
		unclaimed -> <<"unclaimed">>;
		_         -> ar_util:encode(OldB#block.reward_addr)
	end,
	ar:report(
		[
			verifying_finder_reward,
			{finder_reward, FinderReward},
			{new_reward_pool, RewardPool},
			{reward_address, RewardAddress},
			{old_reward_pool, OldB#block.reward_pool},
			{txs, length(NewTXs)},
			{recall_block_size, RecallB#block.block_size},
			{weave_size, NewB#block.weave_size},
			{length, length(NewB#block.hash_list)}
		]
	),
	(NewB#block.reward_pool == RewardPool) and
	((NewB#block.wallet_list) ==
		ar_node_utils:apply_mining_reward(
			ar_node_utils:apply_txs(OldB#block.wallet_list, NewTXs),
			NewB#block.reward_addr,
			FinderReward,
			NewB#block.height
		)).

verify_weave_size(NewB, OldB, TXs) ->
	NewB#block.weave_size == lists:foldl(
		fun(TX, Acc) ->
			Acc + byte_size(TX#tx.data)
		end,
		OldB#block.weave_size,
		TXs
	).

%% @doc Ensure that after the 1.6 release cumulative difficulty is enforced.
verify_cumulative_diff(NewB, _OldB) when NewB#block.height < ?FORK_1_6 -> true;
verify_cumulative_diff(NewB, OldB) ->
	NewB#block.cumulative_diff ==
		(OldB#block.cumulative_diff + (NewB#block.diff * NewB#block.diff)).

% Block shadow functions

generate_block_from_shadow(BShadow, RecallSize) ->
	TXs =
		% Check if the node state contains the referenced TX.
		lists:foldr(fun(T, Acc) ->
		case
			[
				TX || TX <- ar_node:get_all_known_txs(
					whereis(http_entrypoint_node)
				),
				TX#tx.id == T
			]
		of
			[] ->
				case ar_storage:read_tx(T) of
					unavailable ->
						Acc;
					TX -> [TX | Acc]
				end;
			[TX | _] -> [TX | Acc]
		end
	end, [], BShadow#block.txs),
	{FinderPool, _} = ar_node_utils:calculate_reward_pool(
		ar_node:get_reward_pool(whereis(http_entrypoint_node)),
		TXs,
		BShadow#block.reward_addr,
		ar_node_utils:calculate_proportion(
			RecallSize,
			BShadow#block.weave_size,
			BShadow#block.height
		)
	),
	HashList =
		case
			{
				BShadow#block.hash_list,
				ar_node:get_hash_list(whereis(http_entrypoint_node))
			}
		of
			{[], []} -> [];
			{[], OldHashList} -> OldHashList;
			{ShadowHashList, []} -> ShadowHashList;
			{ShadowHashList, OldHashList} ->
				NewL =
					lists:dropwhile(
						fun(X) -> X =/= lists:last(ShadowHashList) end,
						OldHashList
					),
				ShadowHashList ++
					case NewL of
						[] -> OldHashList;
						NewL -> tl(NewL)
					end
		end,
	WalletList = ar_node_utils:apply_mining_reward(
		ar_node_utils:apply_txs(
			ar_node:get_wallet_list(whereis(http_entrypoint_node)),
			TXs
		),
		BShadow#block.reward_addr,
		FinderPool,
		BShadow#block.height
	),
	BShadow#block { wallet_list = WalletList, hash_list = HashList }.


get_recall_block(OrigPeer,RecallHash,B,Key,Nonce) ->
	get_recall_block(OrigPeer,RecallHash,B,Key,Nonce, B#block.hash_list).
get_recall_block(OrigPeer,RecallHash,B,Key,Nonce, BHL) ->
	case ar_storage:read_block(RecallHash, BHL) of
		unavailable ->
			case ar_storage:read_encrypted_block(RecallHash) of
				unavailable ->
					ar:report([{downloading_recall_block, ar_util:encode(RecallHash)}]),
					FullBlock =
						ar_http_iface:get_full_block(OrigPeer, RecallHash, B#block.hash_list),
					case ?IS_BLOCK(FullBlock)  of
						true ->
							Recall = FullBlock#block {
								txs = [ T#tx.id || T <- FullBlock#block.txs]
							},
							ar_storage:write_tx(FullBlock#block.txs),
							ar_storage:write_block(Recall),
							Recall;
						false -> unavailable
					end;
				EncryptedRecall ->
					FBlock =
						ar_block:decrypt_full_block(
							B,
							EncryptedRecall,
							Key,
							Nonce
						),
					case FBlock of
						unavailable -> unavailable;
						FullBlock ->
							Recall = FullBlock#block {
								txs = [ T#tx.id || T <- FullBlock#block.txs]
							},
							ar_storage:write_tx(FullBlock#block.txs),
							ar_storage:write_block(Recall),
							Recall
					end
			end;
		Recall -> Recall
	end.


%% Tests: ar_block

hash_list_gen_test() ->
	ar_storage:clear(),
	B0s = [B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	B1s = [B1|_] = ar_weave:add(B0s, []),
	ar_storage:write_block(B1),
	B2s = [B2|_] = ar_weave:add(B1s, []),
	ar_storage:write_block(B2),
	[B3|_] = ar_weave:add(B2s, []),
	BHL1 = B1#block.hash_list,
	BHL2 = B2#block.hash_list,
	BHL1 = generate_hash_list_for_block(B1, B3#block.hash_list),
	BHL2 = generate_hash_list_for_block(B2#block.indep_hash, B3#block.hash_list).

pad_unpad_roundtrip_test() ->
	Pad = pad_to_length(<<"abcdefghabcdefghabcd">>),
	UnPad = unpad_binary(Pad),
	Pad == UnPad.

% encrypt_decrypt_block_test() ->
%	  B0 = ar_weave:init([]),
%	  ar_storage:write_block(B0),
%	  B1 = ar_weave:add(B0, []),
%	  CipherText = encrypt_block(hd(B0), hd(B1)),
%	  Key = generate_block_key(hd(B0), hd(B1)),
%	  B0 = [decrypt_block(hd(B1), CipherText, Key)].

% encrypt_decrypt_full_block_test() ->
%	  ar_storage:clear(),
%	  B0 = ar_weave:init([]),
%	  ar_storage:write_block(B0),
%	  B1 = ar_weave:add(B0, []),
%	TX = ar_tx:new(<<"DATA1">>),
%	TX1 = ar_tx:new(<<"DATA2">>),
%	ar_storage:write_tx([TX, TX1]),
%	  B0Full = (hd(B0))#block{ txs = [TX, TX1] },
%	  CipherText = encrypt_full_block(B0Full, hd(B1)),
%	  Key = generate_block_key(B0Full, hd(B1)),
%	  B0Full = decrypt_full_block(hd(B1), CipherText, Key).
