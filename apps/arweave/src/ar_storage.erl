-module(ar_storage).

-behaviour(gen_server).

-export([start_link/0, read_block_index/0, read_reward_history/1, read_block_time_history/2,
		store_block_index/1, update_block_index/3,
		store_reward_history_part/1, store_reward_history_part2/1,
		store_block_time_history_part/2, store_block_time_history_part2/1,
		write_full_block/2, read_block/1, read_block/2, write_tx/1,
		read_tx/1, read_tx_data/1, update_confirmation_index/1, get_tx_confirmation_data/1,
		read_wallet_list/1, write_wallet_list/2,
		delete_blacklisted_tx/1, lookup_tx_filename/1,
		wallet_list_filepath/1, tx_filepath/1, tx_data_filepath/1, read_tx_file/1,
		read_migrated_v1_tx_file/1, ensure_directories/1, write_file_atomic/2,
		write_term/2, write_term/3, read_term/1, read_term/2, delete_term/1, is_file/1,
		migrate_tx_record/1, migrate_block_record/1, read_account/2, read_block_from_file/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_wallets.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Read the entire stored block index.
read_block_index() ->
	case block_index_tip() of
		not_found ->
			not_found;
		{Height,{H, _, _, PrevH}} ->
			{ok, Map} = ar_kv:get_range(block_index_db, << 0:256 >>, << Height:256 >>),
			read_block_index_from_map(Map, 0, Height, <<>>, [])
	end.

read_block_index_from_map(_Map, Height, End, _PrevH, BI) when Height > End ->
	BI;
read_block_index_from_map(Map, Height, End, PrevH, BI) ->
	V = maps:get(<< Height:256 >>, Map, not_found),
	case V of
		not_found ->
			ar:console("The stored block index is invalid. Height ~B not found.~n", [Height]),
			not_found;
		_ ->
			case binary_to_term(V) of
				{H, WeaveSize, TXRoot, PrevH} ->
					read_block_index_from_map(Map, Height + 1, End, H, [{H, WeaveSize, TXRoot} | BI]);
				{_, _, _, PrevH2} ->
					ar:console("The stored block index is invalid. Height: ~B, "
							"stored previous hash: ~s, expected previous hash: ~s.~n",
							[Height, ar_util:encode(PrevH2), ar_util:encode(PrevH)]),
					not_found
			end
	end.

%% @doc Return the reward history for the given block index part or not_found.
read_reward_history([]) ->
	[];
read_reward_history([{H, _WeaveSize, _TXRoot} | BI]) ->
	case read_reward_history(BI) of
		not_found ->
			not_found;
		History ->
			case ar_kv:get(reward_history_db, H) of
				not_found ->
					not_found;
				{ok, Bin} ->
					Element = binary_to_term(Bin),
					[Element | History]
			end
	end.

%% @doc Return the block time history for the given block index part or not_found.
read_block_time_history(_Height, []) ->
	[];
read_block_time_history(Height, [{H, _WeaveSize, _TXRoot} | BI]) ->
	case Height < ar_fork:height_2_7() of
	true ->
			[];
		false ->
			case read_block_time_history(Height - 1, BI) of
				not_found ->
					not_found;
				History ->
					case ar_kv:get(block_time_history_db, H) of
						not_found ->
							not_found;
						{ok, Bin} ->
							Element = binary_to_term(Bin),
							[Element | History]
					end
			end
	end.

%% @doc Record the entire block index on disk.
%% Return {error, block_index_no_recent_intersection} if the local state forks away
%% at more than ?STORE_BLOCKS_BEHIND_CURRENT blocks ago.
store_block_index(BI) ->
	%% Use a key that is bigger than any << Height:256 >> (<<"a">> > << Height:256 >>)
	%% to retrieve the largest stored Height.
	NewHeight = length(BI) - 1,
	case ar_kv:get_prev(block_index_db, <<"a">>) of
		none ->
			update_block_index(NewHeight, 0, lists:reverse(BI));
		{ok, << StoredHeight:256 >>, _V} ->
			%% RootHeight should a historical height shared by both the stored BI and the
			%% new BI
			RootHeight = max(0, min(StoredHeight, NewHeight) - ?STORE_BLOCKS_BEHIND_CURRENT),
			{ok, V} = ar_kv:get(block_index_db, << RootHeight:256 >>),
			{H, WeaveSize, TXRoot} = lists:nth(NewHeight - RootHeight + 1, BI),
			case binary_to_term(V) of
				{H, WeaveSize, TXRoot, _PrevH} ->
					BI2 = lists:reverse(lists:sublist(BI, NewHeight - RootHeight)),
					update_block_index(NewHeight, StoredHeight - RootHeight, BI2);
				{H2, _, _, _} ->
					?LOG_ERROR([{event, failed_to_store_block_index},
							{reason, no_intersection},
							{height, RootHeight},
							{stored_hash, ar_util:encode(H2)},
							{expected_hash, ar_util:encode(H)}]),
					{error, block_index_no_recent_intersection}
			end;
		Error ->
			Error
	end.

%% @doc Record the block index update on disk. Remove the orphans, if any.

update_block_index(NewTipHeight, OrphanCount, _BI) 
		when NewTipHeight < 0 orelse OrphanCount < 0 ->
	{error, badarg};
update_block_index(NewTipHeight, OrphanCount, BI) ->
	%% Record the contents of BI starting at this height - the entry at IndexHeight will
	%% be the first entry written (perhaps replacing an existing entry at that height)
	CurTipHeight = case block_index_tip() of
		not_found ->
			-1;
		{Height, _} ->
			Height
	end,
	%% IndexHeight is by default one beyond the current tip, this only changes if we have
	%% orphans (which will be deleted).
	IndexHeight = (CurTipHeight + 1) - OrphanCount,

	case IndexHeight + length(BI) - 1 == NewTipHeight of
		true ->
			update_block_index2(IndexHeight, OrphanCount, BI);
		false ->
			?LOG_ERROR([{event, failed_to_update_block_index},
				{reason, block_index_gap},
				{cur_tip_height, CurTipHeight},
				{new_tip_height, NewTipHeight},
				{index_height, IndexHeight},
				{orphan_count, OrphanCount},
				{block_count, length(BI)}]),
			{error, not_found}
	end.

update_block_index2(IndexHeight, OrphanCount, BI) ->
	%% 1. Delete all the orphaned blocks from the block index
	case ar_kv:delete_range(block_index_db,
			<< IndexHeight:256 >>, << (IndexHeight + OrphanCount):256 >>) of
		ok ->
			case IndexHeight of
				0 ->
					update_block_index3(0, <<>>, BI);
				_ ->
					%% 2. Add all the entries in BI to the block index
					%% BI will include the new tip block at the current height, as well as any new
					%% history blocks if the tip is on a new branch.
					case ar_kv:get(block_index_db, << (IndexHeight - 1):256 >>) of
						not_found ->
							?LOG_ERROR([{event, failed_to_update_block_index},
									{reason, prev_element_not_found},
									{prev_height, IndexHeight - 1}]),
							{error, not_found};
						{ok, Bin} ->
							{PrevH, _, _, _} = binary_to_term(Bin),
							update_block_index3(IndexHeight, PrevH, BI)
					end
			end;
		{error, Error} ->
			?LOG_ERROR([{event, failed_to_update_block_index},
					{reason, failed_to_remove_orphaned_range},
					{range_start, IndexHeight},
					{range_end, IndexHeight + OrphanCount},
					{reason, io_lib:format("~p", [Error])}]),
			{error, Error}
	end.

update_block_index3(_Height, _PrevH, []) ->
	ok;
update_block_index3(Height, PrevH, [{H, WeaveSize, TXRoot} | BI]) ->
	Bin = term_to_binary({H, WeaveSize, TXRoot, PrevH}),
	case ar_kv:put(block_index_db, << Height:256 >>, Bin) of
		ok ->
			update_block_index3(Height + 1, H, BI);
		Error ->
			?LOG_ERROR([{event, failed_to_update_block_index},
					{height, Height},
					{reason, io_lib:format("~p", [Error])}]),
			{error, Error}
	end.

store_reward_history_part([]) ->
	ok;
store_reward_history_part(Blocks) ->
	store_reward_history_part2([{B#block.indep_hash, {B#block.reward_addr,
			ar_difficulty:get_hash_rate_fixed_ratio(B), B#block.reward,
			B#block.denomination}} || B <- Blocks]).

store_reward_history_part2([]) ->
	ok;
store_reward_history_part2([{H, El} | History]) ->
	Bin = term_to_binary(El),
	case ar_kv:put(reward_history_db, H, Bin) of
		ok ->
			store_reward_history_part2(History);
		Error ->
			?LOG_ERROR([{event, failed_to_update_reward_history},
					{reason, io_lib:format("~p", [Error])},
					{block, ar_util:encode(H)}]),
			{error, not_found}
	end.

store_block_time_history_part([], _PrevB) ->
	ok;
store_block_time_history_part(Blocks, PrevB) ->
	History = ar_block_time_history:get_history_from_blocks(Blocks, PrevB),
	store_block_time_history_part2(History).

store_block_time_history_part2([]) ->
	ok;
store_block_time_history_part2([{H, El} | History]) ->
	Bin = term_to_binary(El),
	case ar_kv:put(block_time_history_db, H, Bin) of
		ok ->
			store_block_time_history_part2(History);
		Error ->
			?LOG_ERROR([{event, failed_to_update_block_time_history},
					{reason, io_lib:format("~p", [Error])},
					{block, ar_util:encode(H)}]),
			{error, not_found}
	end.

-if(?NETWORK_NAME == "arweave.N.1").
write_full_block(#block{ height = 0 } = BShadow, TXs) ->
	%% Genesis transactions are stored in data/genesis_txs; they are part of the repository.
	write_full_block2(BShadow, TXs);
write_full_block(BShadow, TXs) ->
	case update_confirmation_index(BShadow#block{ txs = TXs }) of
		ok ->
			case write_tx([TX || TX <- TXs, not is_blacklisted(TX)]) of
				ok ->
					write_full_block2(BShadow, TXs);
				Error ->
					Error
			end;
		Error ->
			Error
	end.
-else.
write_full_block(BShadow, TXs) ->
	case update_confirmation_index(BShadow#block{ txs = TXs }) of
		ok ->
			case write_tx([TX || TX <- TXs, not is_blacklisted(TX)]) of
				ok ->
					write_full_block2(BShadow, TXs);
				Error ->
					Error
			end;
		Error ->
			Error
	end.
-endif.

is_blacklisted(#tx{ format = 2 }) ->
	false;
is_blacklisted(#tx{ id = TXID }) ->
	ar_tx_blacklist:is_tx_blacklisted(TXID).

update_confirmation_index(B) ->
	put_tx_confirmation_data(B).

put_tx_confirmation_data(B) ->
	Data = term_to_binary({B#block.height, B#block.indep_hash}),
	lists:foldl(
		fun	(TX, ok) ->
				ar_kv:put(tx_confirmation_db, TX#tx.id, Data);
			(_TX, Acc) ->
				Acc
		end,
		ok,
		B#block.txs
	).

%% @doc Return {BlockHeight, BlockHash} belonging to the block where
%% the given transaction was included.
get_tx_confirmation_data(TXID) ->
	case ar_kv:get(tx_confirmation_db, TXID) of
		{ok, Binary} ->
			{ok, binary_to_term(Binary)};
		not_found ->
			not_found
	end.

%% @doc Read a block from disk, given a height
%% and a block index (used to determine the hash by height).
read_block(Height, BI) when is_integer(Height) ->
	case Height of
		_ when Height < 0 ->
			unavailable;
		_ when Height > length(BI) - 1 ->
			unavailable;
		_ ->
			{H, _, _} = lists:nth(length(BI) - Height, BI),
			read_block(H)
	end;
read_block(H, _BI) ->
	read_block(H).

%% @doc Read a block from disk, given a hash, a height, or a block index entry.
read_block(unavailable) ->
	unavailable;
read_block(B) when is_record(B, block) ->
	B;
read_block(Blocks) when is_list(Blocks) ->
	lists:map(fun(B) -> read_block(B) end, Blocks);
read_block({H, _, _}) ->
	read_block(H);
read_block(BH) ->
	case ar_disk_cache:lookup_block_filename(BH) of
		{ok, {Filename, Encoding}} ->
			%% The cache keeps a rotated number of recent headers when the
			%% node is out of disk space.
			read_block_from_file(Filename, Encoding);
		_ ->
			case ar_kv:get(block_db, BH) of
				not_found ->
					case lookup_block_filename(BH) of
						unavailable ->
							unavailable;
						{Filename, Encoding} ->
							read_block_from_file(Filename, Encoding)
					end;
				{ok, V} ->
					parse_block_kv_binary(V);
				{error, Reason} ->
					?LOG_WARNING([{event, error_reading_block_from_kv_storage},
							{block, ar_util:encode(BH)},
							{error, io_lib:format("~p", [Reason])}])
			end
	end.

%% @doc Read the account information for the given address and
%% root hash of the account tree. Return {0, <<>>} if the given address does not belong
%% to the tree. The balance may be also 0 when the address exists in the tree. Return
%% not_found if some of the files with the account data are missing.
read_account(Addr, Key) ->
	read_account(Addr, Key, <<>>).

read_account(Addr, Key, Prefix) ->
	case get_account_tree_value(Key, Prefix) of
		{ok, << Key:48/binary, _/binary >>, V} ->
			case binary_to_term(V) of
				{K, Val} when K == Addr ->
					Val;
				{_, _} ->
					{0, <<>>};
				[_ | _] = SubTrees ->
					case find_key_by_matching_longest_prefix(Addr, SubTrees) of
						not_found ->
							{0, <<>>};
						{H, Prefix2} ->
							read_account(Addr, H, Prefix2)
					end
			end;
		_ ->
			read_account2(Addr, Key)
	end.

find_key_by_matching_longest_prefix(Addr, Keys) ->
	find_key_by_matching_longest_prefix(Addr, Keys, {<<>>, -1}).

find_key_by_matching_longest_prefix(_Addr, [], {Key, Prefix}) ->
	case Key of
		<<>> ->
			not_found;
		_ ->
			{Key, Prefix}
	end;
find_key_by_matching_longest_prefix(Addr, [{_, Prefix} | Keys], {Key, KeyPrefix})
		when Prefix == <<>> orelse byte_size(Prefix) =< byte_size(KeyPrefix) ->
	find_key_by_matching_longest_prefix(Addr, Keys, {Key, KeyPrefix});
find_key_by_matching_longest_prefix(Addr, [{H, Prefix} | Keys], {Key, KeyPrefix}) ->
	case binary:match(Addr, Prefix) of
		{0, _} ->
			find_key_by_matching_longest_prefix(Addr, Keys, {H, Prefix});
		_ ->
			find_key_by_matching_longest_prefix(Addr, Keys, {Key, KeyPrefix})
	end.

read_account2(Addr, RootHash) ->
	%% Unfortunately, we do not have an easy access to the information about how many
	%% accounts there were in the given tree so we perform the binary search starting
	%% from the number in the latest block.
	Size = ar_wallets:get_size(),
	MaxFileCount = Size div ?WALLET_LIST_CHUNK_SIZE + 1,
	{ok, Config} = application:get_env(arweave, config),
	read_account(Addr, RootHash, 0, MaxFileCount, Config#config.data_dir, false).

read_account(_Addr, _RootHash, Left, Right, _DataDir, _RightFileFound) when Left == Right ->
	not_found;
read_account(Addr, RootHash, Left, Right, DataDir, RightFileFound) ->
	Pos = Left + (Right - Left) div 2,
	Filepath = wallet_list_chunk_relative_filepath(Pos * ?WALLET_LIST_CHUNK_SIZE, RootHash),
	case filelib:is_file(filename:join(DataDir, Filepath)) of
		false ->
			read_account(Addr, RootHash, Left, Pos, DataDir, false);
		true ->
			{ok, L} = ar_storage:read_term(Filepath),
			read_account2(Addr, RootHash, Pos, Left, Right, DataDir, L, RightFileFound)
	end.

wallet_list_chunk_relative_filepath(Position, RootHash) ->
	binary_to_list(iolist_to_binary([
		?WALLET_LIST_DIR,
		"/",
		ar_util:encode(RootHash),
		"-",
		integer_to_binary(Position),
		"-",
		integer_to_binary(?WALLET_LIST_CHUNK_SIZE)
	])).

read_account2(Addr, _RootHash, _Pos, _Left, _Right, _DataDir, [last, {LargestAddr, _} | _L],
		_RightFileFound) when Addr > LargestAddr ->
	{0, <<>>};
read_account2(Addr, RootHash, Pos, Left, Right, DataDir, [last | L], RightFileFound) ->
	read_account2(Addr, RootHash, Pos, Left, Right, DataDir, L, RightFileFound);
read_account2(Addr, RootHash, Pos, _Left, Right, DataDir, [{LargestAddr, _} | _L],
		RightFileFound) when Addr > LargestAddr ->
	case Pos + 1 == Right of
		true ->
			case RightFileFound of
				true ->
					{0, <<>>};
				false ->
					not_found
			end;
		false ->
			read_account(Addr, RootHash, Pos, Right, DataDir, RightFileFound)
	end;
read_account2(Addr, RootHash, Pos, Left, _Right, DataDir, L, _RightFileFound) ->
	case Addr < element(1, lists:last(L)) of
		true ->
			case Pos == Left of
				true ->
					{0, <<>>};
				false ->
					read_account(Addr, RootHash, Left, Pos, DataDir, true)
			end;
		false ->
			case lists:search(fun({Addr2, _}) -> Addr2 == Addr end, L) of
				{value, {Addr, Data}} ->
					Data;
				false ->
					{0, <<>>}
			end
	end.

lookup_block_filename(H) ->
	{ok, Config} = application:get_env(arweave, config),
	Name = filename:join([Config#config.data_dir, ?BLOCK_DIR,
			binary_to_list(ar_util:encode(H))]),
	NameJSON = iolist_to_binary([Name, ".json"]),
	case is_file(NameJSON) of
		true ->
			{NameJSON, json};
		false ->
			NameBin = iolist_to_binary([Name, ".bin"]),
			case is_file(NameBin) of
				true ->
					{NameBin, binary};
				false ->
					unavailable
			end
	end.

%% @doc Delete the blacklisted tx with the given hash from disk. Return {ok, BytesRemoved} if
%% the removal is successful or the file does not exist. The reported number of removed
%% bytes does not include the migrated v1 data. The removal of migrated v1 data is requested
%% from ar_data_sync asynchronously. The v2 headers are not removed.
delete_blacklisted_tx(Hash) ->
	case ar_kv:get(tx_db, Hash) of
		{ok, V} ->
			TX = parse_tx_kv_binary(V),
			case TX#tx.format == 1 andalso TX#tx.data_size > 0 of
				true ->
					case ar_kv:delete(tx_db, Hash) of
						ok ->
							{ok, byte_size(V)};
						Error ->
							Error
					end;
				_ ->
					{ok, 0}
			end;
		{error, _} = DBError ->
			DBError;
		not_found ->
			case lookup_tx_filename(Hash) of
				{Status, Filename} ->
					case Status of
						migrated_v1 ->
							case file:read_file_info(Filename) of
								{ok, FileInfo} ->
									case file:delete(Filename) of
										ok ->
											{ok, FileInfo#file_info.size};
										Error ->
											Error
									end;
								Error ->
									Error
							end;
						_ ->
							{ok, 0}
					end;
				unavailable ->
					{ok, 0}
			end
	end.

parse_tx_kv_binary(Bin) ->
	case catch ar_serialize:binary_to_tx(Bin) of
		{ok, TX} ->
			TX;
		_ ->
			migrate_tx_record(binary_to_term(Bin))
	end.

%% Convert the stored tx record to its latest state in the code
%% (assign the default values to all missing fields). Since the version introducing
%% the fork 2.6, the transactions are serialized via ar_serialize:tx_to_binary/1, which
%% is maintained compatible with all past versions, so this code is only used
%% on the nodes synced before the corresponding release.
migrate_tx_record(#tx{} = TX) ->
	TX;
migrate_tx_record({tx, Format, ID, LastTX, Owner, Tags, Target, Quantity, Data,
		DataSize, DataTree, DataRoot, Signature, Reward}) ->
	#tx{ format = Format, id = ID, last_tx = LastTX,
			owner = Owner, tags = Tags, target = Target, quantity = Quantity,
			data = Data, data_size = DataSize, data_root = DataRoot,
			signature = Signature, signature_type = ?DEFAULT_KEY_TYPE,
			reward = Reward, data_tree = DataTree }.

parse_block_kv_binary(Bin) ->
	case catch ar_serialize:binary_to_block(Bin) of
		{ok, B} ->
			B;
		_ ->
			migrate_block_record(binary_to_term(Bin))
	end.

%% Convert the stored block record to its latest state in the code
%% (assign the default values to all missing fields). Since the version introducing
%% the fork 2.6, the blocks are serialized via ar_serialize:block_to_binary/1, which
%% is maintained compatible with all past block versions, so this code is only used
%% on the nodes synced before the corresponding release.
migrate_block_record(#block{} = B) ->
	B;
migrate_block_record({block, Nonce, PrevH, TS, Last, Diff, Height, Hash, H,
		TXs, TXRoot, TXTree, HL, HLMerkle, WL, RewardAddr, Tags, RewardPool,
		WeaveSize, BlockSize, CDiff, SizeTaggedTXs, PoA, Rate, ScheduledRate,
		Packing_2_5_Threshold, StrictDataSplitThreshold}) ->
	PoA_2 =
		case PoA of
			{poa, O, TXPath, DataPath, Chunk} ->
				#poa{ option = O, tx_path = TXPath, data_path = DataPath,
						chunk = Chunk };
			#poa{} ->
				PoA
		end,
	#block{ nonce = Nonce, previous_block = PrevH, timestamp = TS,
			last_retarget = Last, diff = Diff, height = Height, hash = Hash,
			indep_hash = H, txs = TXs, tx_root = TXRoot, tx_tree = TXTree,
			hash_list = HL, hash_list_merkle = HLMerkle, wallet_list = WL,
			reward_addr = RewardAddr, tags = Tags, reward_pool = RewardPool,
			weave_size = WeaveSize, block_size = BlockSize, cumulative_diff = CDiff,
			size_tagged_txs = SizeTaggedTXs, poa = PoA_2, usd_to_ar_rate = Rate,
			scheduled_usd_to_ar_rate = ScheduledRate,
			packing_2_5_threshold = Packing_2_5_Threshold,
			strict_data_split_threshold = StrictDataSplitThreshold }.

write_tx(TXs) when is_list(TXs) ->
	lists:foldl(
		fun (TX, ok) ->
				write_tx(TX);
			(_TX, Acc) ->
				Acc
		end,
		ok,
		TXs
	);
write_tx(#tx{ format = Format, id = TXID } = TX) ->
	case write_tx_header(TX) of
		ok ->
			DataSize = byte_size(TX#tx.data),
			case DataSize > 0 of
				true ->
					case {DataSize == TX#tx.data_size, Format} of
						{false, 2} ->
							?LOG_ERROR([{event, failed_to_store_tx_data},
									{reason, size_mismatch}, {tx, ar_util:encode(TX#tx.id)}]),
							ok;
						{true, 1} ->
							case write_tx_data(no_expected_data_root, TX#tx.data, TXID) of
								ok ->
									ok;
								{error, Reason} ->
									?LOG_WARNING([{event, failed_to_store_tx_data},
											{reason, Reason}, {tx, ar_util:encode(TX#tx.id)}]),
									%% We have stored the data in the tx_db table
									%% so we return ok here.
									ok
							end;
						{true, 2} ->
							case ar_tx_blacklist:is_tx_blacklisted(TX#tx.id) of
								true ->
									ok;
								false ->
									case write_tx_data(TX#tx.data_root, TX#tx.data, TXID) of
										ok ->
											ok;
										{error, Reason} ->
											%% v2 data is not part of the header. We have to
											%% report success here even if we failed to store
											%% the attached data.
											?LOG_WARNING([{event, failed_to_store_tx_data},
													{reason, Reason},
													{tx, ar_util:encode(TX#tx.id)}]),
											ok
									end
							end
					end;
				false ->
					ok
			end;
		NotOk ->
			NotOk
	end.

write_tx_header(TX) ->
	TX2 =
		case TX#tx.format of
			1 ->
				TX;
			_ ->
				TX#tx{ data = <<>> }
		end,
	ar_kv:put(tx_db, TX#tx.id, ar_serialize:tx_to_binary(TX2)).

write_tx_data(ExpectedDataRoot, Data, TXID) ->
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, Data),
	SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	SizeTaggedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(SizeTaggedChunks),
	case {ExpectedDataRoot, ar_merkle:generate_tree(SizeTaggedChunkIDs)} of
		{no_expected_data_root, {DataRoot, DataTree}} ->
			write_tx_data(DataRoot, DataTree, Data, SizeTaggedChunks, TXID);
		{_, {ExpectedDataRoot, DataTree}} ->
			write_tx_data(ExpectedDataRoot, DataTree, Data, SizeTaggedChunks, TXID);
		_ ->
			{error, [invalid_data_root]}
	end.

write_tx_data(DataRoot, DataTree, Data, SizeTaggedChunks, TXID) ->
	Errors = lists:foldl(
		fun
			({<<>>, _}, Acc) ->
				%% Empty chunks are produced by ar_tx:chunk_binary/2, when
				%% the data is evenly split by the given chunk size. They are
				%% the last chunks of the corresponding transactions and have
				%% the same end offsets as their preceding chunks. They are never
				%% picked as recall chunks because recall byte has to be strictly
				%% smaller than the end offset. They are an artifact of the original
				%% chunking implementation. There is no value in storing them.
				Acc;
			({Chunk, Offset}, Acc) ->
				DataPath = ar_merkle:generate_path(DataRoot, Offset - 1, DataTree),
				TXSize = byte_size(Data),
				case ar_data_sync:add_chunk(DataRoot, DataPath, Chunk, Offset - 1, TXSize) of
					ok ->
						Acc;
					temporary ->
						Acc;
					{error, Reason} ->
						?LOG_WARNING([{event, failed_to_write_tx_chunk},
								{tx, ar_util:encode(TXID)},
								{reason, io_lib:format("~p", [Reason])}]),
						[Reason | Acc]
				end
		end,
		[],
		SizeTaggedChunks
	),
	case Errors of
		[] ->
			ok;
		_ ->
			{error, Errors}
	end.

%% @doc Read a tx from disk, given a hash.
read_tx(unavailable) ->
	unavailable;
read_tx(TX) when is_record(TX, tx) ->
	TX;
read_tx(TXs) when is_list(TXs) ->
	lists:map(fun read_tx/1, TXs);
read_tx(ID) ->
	case read_tx_from_disk_cache(ID) of
		unavailable ->
			read_tx2(ID);
		TX ->
			TX
	end.

read_tx2(ID) ->
	case ar_kv:get(tx_db, ID) of
		not_found ->
			read_tx_from_file(ID);
		{ok, Binary} ->
			TX = parse_tx_kv_binary(Binary),
			case TX#tx.format == 1 andalso TX#tx.data_size > 0
					andalso byte_size(TX#tx.data) == 0 of
				true ->
					case read_tx_data_from_kv_storage(TX#tx.id) of
						{ok, Data} ->
							TX#tx{ data = Data };
						Error ->
							?LOG_WARNING([{event, error_reading_tx_from_kv_storage},
									{tx, ar_util:encode(ID)},
									{error, io_lib:format("~p", [Error])}]),
							unavailable
					end;
				_ ->
					TX
			end
	end.

read_tx_from_disk_cache(ID) ->
	case ar_disk_cache:lookup_tx_filename(ID) of
		unavailable ->
			unavailable;
		{ok, Filename} ->
			case read_tx_file(Filename) of
				{ok, TX} ->
					TX;
				_Error ->
					unavailable
			end
	end.

read_tx_from_file(ID) ->
	case lookup_tx_filename(ID) of
		{ok, Filename} ->
			case read_tx_file(Filename) of
				{ok, TX} ->
					TX;
				_Error ->
					unavailable
			end;
		{migrated_v1, Filename} ->
			case read_migrated_v1_tx_file(Filename) of
				{ok, TX} ->
					TX;
				_Error ->
					unavailable
			end;
		unavailable ->
			unavailable
	end.

read_tx_file(Filename) ->
	case read_file_raw(Filename) of
		{ok, <<>>} ->
			file:delete(Filename),
			?LOG_WARNING([{event, empty_tx_file},
					{filename, Filename}]),
			{error, tx_file_empty};
		{ok, Binary} ->
			case catch ar_serialize:json_struct_to_tx(Binary) of
				TX when is_record(TX, tx) ->
					{ok, TX};
				_ ->
					file:delete(Filename),
					?LOG_WARNING([{event, failed_to_parse_tx},
							{filename, Filename}]),
					{error, failed_to_parse_tx}
			end;
		Error ->
			Error
	end.

read_file_raw(Filename) ->
	case file:open(Filename, [read, raw, binary]) of
		{ok, File} ->
			case file:read(File, 20000000) of
				{ok, Bin} ->
					file:close(File),
					{ok, Bin};
				Error ->
					Error
			end;
		Error ->
			Error
	end.

read_migrated_v1_tx_file(Filename) ->
	case read_file_raw(Filename) of
		{ok, Binary} ->
			case catch ar_serialize:json_struct_to_v1_tx(Binary) of
				#tx{ id = ID } = TX ->
					case read_tx_data_from_kv_storage(ID) of
						{ok, Data} ->
							{ok, TX#tx{ data = Data }};
						Error ->
							Error
					end
			end;
		Error ->
			Error
	end.

read_tx_data_from_kv_storage(ID) ->
	case ar_data_sync:get_tx_data(ID) of
		{ok, Data} ->
			{ok, Data};
		{error, not_found} ->
			{error, data_unavailable};
		{error, timeout} ->
			{error, data_fetch_timeout};
		Error ->
			Error
	end.

read_tx_data(TX) ->
	case read_file_raw(tx_data_filepath(TX)) of
		{ok, Data} ->
			{ok, ar_util:decode(Data)};
		Error ->
			Error
	end.

write_wallet_list(Height, Tree) ->
	{RootHash, _UpdatedTree, UpdateMap} = ar_block:hash_wallet_list(Tree, Height),
	store_account_tree_update(Height, RootHash, UpdateMap),
	erlang:garbage_collect(),
	RootHash.

%% @doc Read a given wallet list (by hash) from the disk.
read_wallet_list(<<>>) ->
	{ok, ar_patricia_tree:new()};
read_wallet_list(WalletListHash) when is_binary(WalletListHash) ->
	Key = WalletListHash,
	read_wallet_list(get_account_tree_value(Key, <<>>), ar_patricia_tree:new(), [],
			WalletListHash, WalletListHash).

read_wallet_list({ok, << K:48/binary, _/binary >>, Bin}, Tree, Keys, RootHash, K) ->
	case binary_to_term(Bin) of
		{Key, Value} ->
			Tree2 = ar_patricia_tree:insert(Key, Value, Tree),
			case Keys of
				[] ->
					{ok, Tree2};
				[{H, Prefix} | Keys2] ->
					read_wallet_list(get_account_tree_value(H, Prefix), Tree2, Keys2,
							RootHash, H)
			end;
		[{H, Prefix} | Hs] ->
			read_wallet_list(get_account_tree_value(H, Prefix), Tree, Hs ++ Keys, RootHash,
					H)
	end;
read_wallet_list({ok, _, _}, _Tree, _Keys, RootHash, _K) ->
	read_wallet_list_from_chunk_files(RootHash);
read_wallet_list(none, _Tree, _Keys, RootHash, _K) ->
	read_wallet_list_from_chunk_files(RootHash);
read_wallet_list(Error, _Tree, _Keys, _RootHash, _K) ->
	Error.

read_wallet_list_from_chunk_files(WalletListHash) when is_binary(WalletListHash) ->
	case read_wallet_list_chunk(WalletListHash) of
		not_found ->
			Filename = wallet_list_filepath(WalletListHash),
			case file:read_file(Filename) of
				{ok, JSON} ->
					parse_wallet_list_json(JSON);
				{error, enoent} ->
					not_found;
				Error ->
					Error
			end;
		{ok, Tree} ->
			{ok, Tree};
		{error, _Reason} = Error ->
			Error
	end;
read_wallet_list_from_chunk_files(WL) when is_list(WL) ->
	{ok, ar_patricia_tree:from_proplist([{get_wallet_key(T), get_wallet_value(T)}
			|| T <- WL])}.

get_wallet_key(T) ->
	element(1, T).

get_wallet_value({_, Balance, LastTX}) ->
	{Balance, LastTX};
get_wallet_value({_, Balance, LastTX, Denomination, MiningPermission}) ->
	{Balance, LastTX, Denomination, MiningPermission}.

read_wallet_list_chunk(RootHash) ->
	read_wallet_list_chunk(RootHash, 0, ar_patricia_tree:new()).

read_wallet_list_chunk(RootHash, Position, Tree) ->
	{ok, Config} = application:get_env(arweave, config),
	Filename =
		binary_to_list(iolist_to_binary([
			Config#config.data_dir,
			"/",
			?WALLET_LIST_DIR,
			"/",
			ar_util:encode(RootHash),
			"-",
			integer_to_binary(Position),
			"-",
			integer_to_binary(?WALLET_LIST_CHUNK_SIZE)
		])),
	case read_term(".", Filename) of
		{ok, Chunk} ->
			{NextPosition, Wallets} =
				case Chunk of
					[last | Tail] ->
						{last, Tail};
					_ ->
						{Position + ?WALLET_LIST_CHUNK_SIZE, Chunk}
				end,
			Tree2 =
				lists:foldl(
					fun({K, V}, Acc) -> ar_patricia_tree:insert(K, V, Acc) end,
					Tree,
					Wallets
				),
			case NextPosition of
				last ->
					{ok, Tree2};
				_ ->
					read_wallet_list_chunk(RootHash, NextPosition, Tree2)
			end;
		{error, Reason} = Error ->
			?LOG_ERROR([
				{event, failed_to_read_wallet_list_chunk},
				{reason, Reason}
			]),
			Error;
		not_found ->
			not_found
	end.

parse_wallet_list_json(JSON) ->
	case ar_serialize:json_decode(JSON) of
		{ok, JiffyStruct} ->
			{ok, ar_serialize:json_struct_to_wallet_list(JiffyStruct)};
		{error, Reason} ->
			{error, {invalid_json, Reason}}
	end.

lookup_tx_filename(ID) ->
	Filepath = tx_filepath(ID),
	case is_file(Filepath) of
		true ->
			{ok, Filepath};
		false ->
			MigratedV1Path = filepath([?TX_DIR, "migrated_v1", tx_filename(ID)]),
			case is_file(MigratedV1Path) of
				true ->
					{migrated_v1, MigratedV1Path};
				false ->
					unavailable
			end
	end.

%% @doc A quick way to lookup the file without using the Erlang file server.
%% Helps take off some IO load during the busy times.
is_file(Filepath) ->
	case file:read_file_info(Filepath, [raw]) of
		{ok, #file_info{ type = Type }} when Type == regular orelse Type == symlink ->
			true;
		_ ->
			false
	end.

read_block_from_file(Filename, Encoding) ->
	case read_file_raw(Filename) of
		{ok, Bin} ->
			case Encoding of
				json ->
					parse_block_json(Bin);
				binary ->
					parse_block_binary(Bin)
			end;
		{error, Reason} ->
			?LOG_WARNING([{event, error_reading_block},
					{error, io_lib:format("~p", [Reason])}]),
			unavailable
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	ensure_directories(Config#config.data_dir),
	%% Copy genesis transactions (snapshotted in the repo) into data_dir/txs
	ar_weave:add_mainnet_v1_genesis_txs(),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "ar_storage_tx_confirmation_db"),
			tx_confirmation_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "ar_storage_tx_db"), tx_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "ar_storage_block_db"), block_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "reward_history_db"), reward_history_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "block_time_history_db"),
			block_time_history_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "block_index_db"), block_index_db),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "account_tree_db"), account_tree_db),
	ets:insert(?MODULE, [{same_disk_storage_modules_total_size,
			get_same_disk_storage_modules_total_size()}]),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({store_account_tree_update, Height, RootHash, Map}, State) ->
	store_account_tree_update(Height, RootHash, Map),
	erlang:garbage_collect(),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

block_index_tip() ->
	%% Use a key that is bigger than any << Height:256 >> (<<"a">> > << Height:256 >>)
	%% to retrieve the largest stored Height.
	case ar_kv:get_prev(block_index_db, <<"a">>) of
		none ->
			not_found;
		{ok, << Height:256 >>, V} ->
			{Height, binary_to_term(V)}
	end.

write_block(B) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(disk_logging, Config#config.enable) of
		true ->
			?LOG_INFO([{event, writing_block_to_disk},
					{block, ar_util:encode(B#block.indep_hash)}]);
		_ ->
			do_nothing
	end,
	TXIDs = lists:map(fun(TXID) when is_binary(TXID) -> TXID;
			(#tx{ id = TXID }) -> TXID end, B#block.txs),
	ar_kv:put(block_db, B#block.indep_hash, ar_serialize:block_to_binary(B#block{
			txs = TXIDs })).

write_full_block2(BShadow, _) ->
	case write_block(BShadow) of
		ok ->
			ok;
		Error ->
			Error
	end.

parse_block_json(JSON) ->
	case catch ar_serialize:json_decode(JSON) of
		{ok, JiffyStruct} ->
			case catch ar_serialize:json_struct_to_block(JiffyStruct) of
				B when is_record(B, block) ->
					B;
				Error ->
					?LOG_WARNING([{event, error_parsing_block_json},
							{error, io_lib:format("~p", [Error])}]),
					unavailable
			end;
		Error ->
			?LOG_WARNING([{event, error_parsing_block_json},
					{error, io_lib:format("~p", [Error])}]),
			unavailable
	end.

parse_block_binary(Bin) ->
	case catch ar_serialize:binary_to_block(Bin) of
		{ok, B} ->
			B;
		Error ->
			?LOG_WARNING([{event, error_parsing_block_bin},
					{error, io_lib:format("~p", [Error])}]),
			unavailable
	end.

filepath(PathComponents) ->
	{ok, Config} = application:get_env(arweave, config),
	to_string(filename:join([Config#config.data_dir | PathComponents])).

to_string(Bin) when is_binary(Bin) ->
	binary_to_list(Bin);
to_string(String) ->
	String.

%% @doc Ensure that all of the relevant storage directories exist.
ensure_directories(DataDir) ->
	%% Append "/" to every path so that filelib:ensure_dir/1 creates a directory
	%% if it does not exist.
	filelib:ensure_dir(filename:join(DataDir, ?TX_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?BLOCK_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?WALLET_LIST_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?HASH_LIST_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?STORAGE_MIGRATIONS_DIR) ++ "/"),
	filelib:ensure_dir(filename:join([DataDir, ?TX_DIR, "migrated_v1"]) ++ "/").

get_same_disk_storage_modules_total_size() ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	{ok, Info} = file:read_file_info(DataDir),
	Device = Info#file_info.major_device,
	get_same_disk_storage_modules_total_size(0, Config#config.storage_modules, DataDir,
			Device).

get_same_disk_storage_modules_total_size(TotalSize, [], _DataDir, _Device) ->
	TotalSize;
get_same_disk_storage_modules_total_size(TotalSize,
		[{Size, _Bucket, _Packing} = Module | StorageModules], DataDir, Device) ->
	Path = filename:join([DataDir, "storage_modules", ar_storage_module:id(Module)]),
	filelib:ensure_dir(Path ++ "/"),
	{ok, Info} = file:read_file_info(Path),
	TotalSize2 =
		case Info#file_info.major_device == Device of
			true ->
				TotalSize + Size;
			false ->
				TotalSize
		end,
	get_same_disk_storage_modules_total_size(TotalSize2, StorageModules, DataDir, Device).

tx_filepath(TX) ->
	filepath([?TX_DIR, tx_filename(TX)]).

tx_data_filepath(TX) when is_record(TX, tx) ->
	tx_data_filepath(TX#tx.id);
tx_data_filepath(ID) ->
	filepath([?TX_DIR, tx_data_filename(ID)]).

tx_filename(TX) when is_record(TX, tx) ->
	tx_filename(TX#tx.id);
tx_filename(TXID) when is_binary(TXID) ->
	iolist_to_binary([ar_util:encode(TXID), ".json"]).

tx_data_filename(TXID) ->
	iolist_to_binary([ar_util:encode(TXID), "_data.json"]).

wallet_list_filepath(Hash) when is_binary(Hash) ->
	filepath([?WALLET_LIST_DIR, iolist_to_binary([ar_util:encode(Hash), ".json"])]).

write_file_atomic(Filename, Data) ->
	SwapFilename = Filename ++ ".swp",
	case file:open(SwapFilename, [write, raw]) of
		{ok, F} ->
			case file:write(F, Data) of
				ok ->
					case file:close(F) of
						ok ->
							file:rename(SwapFilename, Filename);
						Error ->
							Error
					end;
				Error ->
					Error
			end;
		Error ->
			Error
	end.

write_term(Name, Term) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	write_term(DataDir, Name, Term, override).

write_term(Dir, Name, Term) when is_atom(Name) ->
	write_term(Dir, atom_to_list(Name), Term, override);
write_term(Dir, Name, Term) ->
	write_term(Dir, Name, Term, override).

write_term(Dir, Name, Term, Override) ->
	Filepath = filename:join(Dir, Name),
	case Override == do_not_override andalso filelib:is_file(Filepath) of
		true ->
			ok;
		false ->
			case write_file_atomic(Filepath, term_to_binary(Term)) of
				ok ->
					ok;
				{error, Reason} = Error ->
					?LOG_ERROR([{event, failed_to_write_term}, {name, Name},
							{reason, Reason}]),
					Error
			end
	end.

read_term(Name) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	read_term(DataDir, Name).

read_term(Dir, Name) when is_atom(Name) ->
	read_term(Dir, atom_to_list(Name));
read_term(Dir, Name) ->
	case file:read_file(filename:join(Dir, Name)) of
		{ok, Binary} ->
			{ok, binary_to_term(Binary)};
		{error, enoent} ->
			not_found;
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_read_term}, {name, Name}, {reason, Reason}]),
			Error
	end.

delete_term(Name) ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	file:delete(filename:join(DataDir, atom_to_list(Name))).

store_account_tree_update(Height, RootHash, Map) ->
	?LOG_INFO([{event, storing_account_tree_update}, {updated_key_count, map_size(Map)},
			{height, Height}, {root_hash, ar_util:encode(RootHash)}]),
	maps:map(
		fun({H, Prefix} = Key, Value) ->
			Prefix2 = case Prefix of root -> <<>>; _ -> Prefix end,
			DBKey = << H/binary, Prefix2/binary >>,
			case ar_kv:get(account_tree_db, DBKey) of
				not_found ->
					case ar_kv:put(account_tree_db, DBKey, term_to_binary(Value)) of
						ok ->
							ok;
						{error, Reason} ->
							?LOG_ERROR([{event, failed_to_store_account_tree_key},
									{key_hash, ar_util:encode(element(1, Key))},
									{key_prefix, case element(2, Key) of root -> root;
											Prefix -> ar_util:encode(Prefix) end},
									{height, Height},
									{root_hash, ar_util:encode(RootHash)},
									{reason, io_lib:format("~p", [Reason])}])
					end;
				{ok, _} ->
					ok;
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_read_account_tree_key},
							{key_hash, ar_util:encode(element(1, Key))},
							{key_prefix, case element(2, Key) of root -> root;
									Prefix -> ar_util:encode(Prefix) end},
							{height, Height},
							{root_hash, ar_util:encode(RootHash)},
							{reason, io_lib:format("~p", [Reason])}])
			end
		end,
		Map
	),
	?LOG_INFO([{event, stored_account_tree}]).

%% @doc Ignore the prefix when querying a key since the prefix might depend on the order of
%% insertions and is only used to optimize certain lookups.
get_account_tree_value(Key, Prefix) ->
	ar_kv:get_prev(account_tree_db, << Key/binary, Prefix/binary >>).
	% does not work:
	%ar_kv:get_next(account_tree_db, << Key/binary, Prefix/binary >>).
	% works:
	%<< N:(48 * 8) >> = Key,
	%Key2 = << (N + 1):(48 * 8) >>,
	%ar_kv:get_prev(account_tree_db, Key2).

%%%===================================================================
%%% Tests
%%%===================================================================

%% @doc Test block storage.
store_and_retrieve_block_test_() ->
	{timeout, 60, fun test_store_and_retrieve_block/0}.

test_store_and_retrieve_block() ->
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0),
	TXIDs = [TX#tx.id || TX <- B0#block.txs],
	FetchedB0 = read_block(B0#block.indep_hash),
	FetchedB01 = FetchedB0#block{ txs = [tx_id(TX) || TX <- FetchedB0#block.txs] },
	FetchedB02 = read_block(B0#block.height, [{B0#block.indep_hash, B0#block.weave_size,
			B0#block.tx_root}]),
	FetchedB03 = FetchedB02#block{ txs = [tx_id(TX) || TX <- FetchedB02#block.txs] },
	?assertEqual(B0#block{ size_tagged_txs = unset, txs = TXIDs, reward_history = [],
			block_time_history = [], account_tree = undefined }, FetchedB01),
	?assertEqual(B0#block{ size_tagged_txs = unset, txs = TXIDs, reward_history = [],
			block_time_history = [], account_tree = undefined }, FetchedB03),
	ar_test_node:mine(),
	ar_test_node:wait_until_height(1),
	ar_test_node:mine(),
	BI1 = ar_test_node:wait_until_height(2),
	[{_, BlockCount}] = ets:lookup(ar_header_sync, synced_blocks),
	ar_util:do_until(
		fun() ->
			3 == BlockCount
		end,
		100,
		2000
	),
	BH1 = element(1, hd(BI1)),
	?assertMatch(#block{ height = 2, indep_hash = BH1 }, read_block(BH1)),
	?assertMatch(#block{ height = 2, indep_hash = BH1 }, read_block(2, BI1)).

tx_id(#tx{ id = TXID }) ->
	TXID;
tx_id(TXID) ->
	TXID.

store_and_retrieve_wallet_list_test_() ->
	[
		{timeout, 20, fun test_store_and_retrieve_wallet_list/0},
		{timeout, 240, fun test_store_and_retrieve_wallet_list_permutations/0}
	].

test_store_and_retrieve_wallet_list() ->
	[B0] = ar_weave:init(),
	[TX] = B0#block.txs,
	Addr = ar_wallet:to_address(TX#tx.owner, {?RSA_SIGN_ALG, 65537}),
	write_block(B0),
	TXID = TX#tx.id,
	ExpectedWL = ar_patricia_tree:from_proplist([{Addr, {0, TXID}}]),
	WalletListHash = write_wallet_list(0, ExpectedWL),
	{ok, ActualWL} = read_wallet_list(WalletListHash),
	assert_wallet_trees_equal(ExpectedWL, ActualWL),
	Addr2 = binary:part(Addr, 0, 16),
	TXID2 = crypto:strong_rand_bytes(32),
	ExpectedWL2 = ar_patricia_tree:from_proplist([{Addr, {0, TXID}}, {Addr2, {0, TXID2}}]),
	WalletListHash2 = write_wallet_list(0, ExpectedWL2),
	{ok, ActualWL2} = read_wallet_list(WalletListHash2),
	?assertEqual({0, TXID}, read_account(Addr, WalletListHash2)),
	?assertEqual({0, TXID2}, read_account(Addr2, WalletListHash2)),
	assert_wallet_trees_equal(ExpectedWL2, ActualWL2),
	{WalletListHash, ActualWL3, _UpdateMap} = ar_block:hash_wallet_list(ActualWL, 0),
	Addr3 = << (binary:part(Addr, 0, 3))/binary, (crypto:strong_rand_bytes(29))/binary >>,
	TXID3 = crypto:strong_rand_bytes(32),
	TXID4 = crypto:strong_rand_bytes(32),
	ActualWL4 = ar_patricia_tree:insert(Addr3, {100, TXID3},
			ar_patricia_tree:insert(Addr2, {0, TXID4}, ActualWL3)),
	{WalletListHash3, ActualWL5, UpdateMap2} = ar_block:hash_wallet_list(ActualWL4, 0),
	store_account_tree_update(1, WalletListHash3, UpdateMap2),
	?assertEqual({100, TXID3}, read_account(Addr3, WalletListHash3)),
	?assertEqual({0, TXID4}, read_account(Addr2, WalletListHash3)),
	?assertEqual({0, TXID}, read_account(Addr, WalletListHash3)),
	{ok, ActualWL6} = read_wallet_list(WalletListHash3),
	assert_wallet_trees_equal(ActualWL5, ActualWL6).

test_store_and_retrieve_wallet_list_permutations() ->
	lists:foreach(
		fun(Permutation) ->
			store_and_retrieve_wallet_list(Permutation, 0)
		end,
		permutations([ <<"a">>, <<"aa">>, <<"ab">>, <<"bb">>, <<"b">>, <<"aaa">> ])),
	lists:foreach(
		fun(Permutation) ->
			store_and_retrieve_wallet_list(Permutation, 0)
		end,
		permutations([ <<"a">>, <<"aa">>, <<"aaa">>, <<"aaaa">>, <<"aaaaa">> ])),
	store_and_retrieve_wallet_list([ <<"a">>, <<"aa">>, <<"ab">>, <<"b">> ], 0),
	store_and_retrieve_wallet_list([ <<"aa">>, <<"a">>, <<"ab">> ], 0),
	store_and_retrieve_wallet_list([ <<"aaa">>, <<"bbb">>, <<"aab">>, <<"ab">>, <<"a">> ], 0),
	store_and_retrieve_wallet_list([
		<<"aaaa">>, <<"aaab">>, <<"aaac">>,
		<<"aaa">>, <<"aab">>, <<"aac">>,
		<<"aa">>, <<"ab">>, <<"ac">>,
		<<"a">>, <<"b">>, <<"c">>
	], 0),
	store_and_retrieve_wallet_list([
		<<"a">>, <<"b">>, <<"c">>,
		<<"aa">>, <<"ab">>, <<"ac">>,
		<<"aaa">>, <<"aab">>, <<"aac">>,
		<<"aaaa">>, <<"aaab">>, <<"aaac">>,
		<<"a">>, <<"b">>, <<"c">>,
		<<"aa">>, <<"ab">>, <<"ac">>,
		<<"aaa">>, <<"aab">>, <<"aac">>,
		<<"aaaa">>, <<"aaab">>, <<"aaac">>
	], 0),
	store_and_retrieve_wallet_list([
		<<"aaaa">>, <<"aaa">>, <<"aa">>, <<"a">>,
		<<"aaab">>, <<"aab">>, <<"ab">>, <<"b">>,
		<<"aaac">>, <<"aac">>, <<"ac">>, <<"c">>,
		<<"aaaa">>, <<"aaa">>, <<"aa">>, <<"a">>,
		<<"aaab">>, <<"aab">>, <<"ab">>, <<"b">>,
		<<"aaac">>, <<"aac">>, <<"ac">>, <<"c">>
	], 0),
	store_and_retrieve_wallet_list([
		<<"aaaa">>, <<"aaab">>, <<"aaac">>,
		<<"a">>, <<"aa">>, <<"aaa">>,
		<<"aaaa">>, <<"aaab">>, <<"aaac">>
	], 0),
	ok.

store_and_retrieve_wallet_list(Keys, Height) ->
	MinBinary = <<>>,
	MaxBinary = << <<1:1>> || _ <- lists:seq(1, 512) >>,
	ar_kv:delete_range(account_tree_db, MinBinary, MaxBinary),
	store_and_retrieve_wallet_list(Keys, ar_patricia_tree:new(), maps:new(), false, Height).

store_and_retrieve_wallet_list([], Tree, InsertedKeys, IsUpdate, Height) ->
	store_and_retrieve_wallet_list2(Tree, InsertedKeys, IsUpdate, Height);
store_and_retrieve_wallet_list([Key | Keys], Tree, InsertedKeys, IsUpdate, Height) ->
	TXID = crypto:strong_rand_bytes(32),
	Balance = rand:uniform(1000000000),
	Tree2 = ar_patricia_tree:insert(Key, {Balance, TXID}, Tree),
	InsertedKeys2 = maps:put(Key, {Balance, TXID}, InsertedKeys),
	case rand:uniform(2) of
		1 ->
			Tree3 = store_and_retrieve_wallet_list2(Tree2, InsertedKeys2, IsUpdate, Height),
			store_and_retrieve_wallet_list(Keys, Tree3, InsertedKeys2, true, Height);
		_ ->
			store_and_retrieve_wallet_list(Keys, Tree2, InsertedKeys2, IsUpdate, Height)
	end.

store_and_retrieve_wallet_list2(Tree, InsertedKeys, IsUpdate, Height) ->
	{WalletListHash, Tree2} =
		case IsUpdate of
			false ->
				{write_wallet_list(0, Tree), Tree};
			_ ->
				{R, T, Map} = ar_block:hash_wallet_list(Tree, Height),
				store_account_tree_update(0, R, Map),
				{R, T}
		end,
	{ok, ActualTree} = read_wallet_list(WalletListHash),
	maps:foreach(
		fun(Key, {Balance, TXID}) ->
			?assertEqual({Balance, TXID}, read_account(Key, WalletListHash))
		end,
		InsertedKeys
	),
	assert_wallet_trees_equal(Tree, ActualTree),
	assert_wallet_trees_equal(Tree2, ActualTree),
	Tree2.

%% From: https://www.erlang.org/doc/programming_examples/list_comprehensions.html#permutations
permutations([]) -> [[]];
permutations(L)  -> [[H|T] || H <- L, T <- permutations(L--[H])].

assert_wallet_trees_equal(Expected, Actual) ->
	?assertEqual(
		ar_patricia_tree:foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], Expected),
		ar_patricia_tree:foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], Actual)
	).

read_wallet_list_chunks_test() ->
	TestCases = [
		[random_wallet()], % < chunk size
		[random_wallet() || _ <- lists:seq(1, ?WALLET_LIST_CHUNK_SIZE)], % == chunk size
		[random_wallet() || _ <- lists:seq(1, ?WALLET_LIST_CHUNK_SIZE + 1)], % > chunk size
		[random_wallet() || _ <- lists:seq(1, 10 * ?WALLET_LIST_CHUNK_SIZE)],
		[random_wallet() || _ <- lists:seq(1, 10 * ?WALLET_LIST_CHUNK_SIZE + 1)]
	],
	lists:foreach(
		fun(TestCase) ->
			Tree = ar_patricia_tree:from_proplist(TestCase),
			RootHash = write_wallet_list(0, Tree),
			{ok, ReadTree} = read_wallet_list(RootHash),
			assert_wallet_trees_equal(Tree, ReadTree)
		end,
		TestCases
	).

random_wallet() ->
	{crypto:strong_rand_bytes(32), {rand:uniform(1000000000), crypto:strong_rand_bytes(32)}}.

update_block_index_test_() ->
	[
		{timeout, 20, fun test_update_block_index/0}
	].

test_update_block_index() ->
	ar_kv:delete_range(block_index_db, <<0:256>>, <<"a">>),

	?assertEqual(
		{error, not_found},
		update_block_index(2, 0, [
			{<<"hash_a">>, 0, <<"root_a">>}
		]),
		"Gap on empty index"
	),

	?assertEqual(
		{error, badarg},
		update_block_index(-1, 0, [
			{<<"hash_a">>, 0, <<"root_a">>}
		]),
		"Negative tip"
	),
	
	?assertEqual(
		{error, badarg},
		update_block_index(0, -1, [
			{<<"hash_a">>, 0, <<"root_a">>}
		]),
		"Negative orphan count"
	),

	?assertEqual(
		{error, not_found},
		update_block_index(0, 1, [
			{<<"hash_a">>, 0, <<"root_a">>}
		]),
		"Orphan on empty index"
	),
	?assertEqual(
		ok,
		update_block_index(0, 0, [
			{<<"hash_a">>, 0, <<"root_a">>}
		])
	),
	?assertEqual([
		{<<"hash_a">>, 0, <<"root_a">>}
	], read_block_index()),

	?assertEqual(
		{error, not_found},
		update_block_index(2, 0, [
			{<<"hash_b">>, 0, <<"root_b">>}
		]),
		"Gap on non-empty index"
	),

	?assertEqual(
		ok,
		update_block_index(1, 0, [
			{<<"hash_b">>, 0, <<"root_b">>}
		])
	),
	?assertEqual([
		{<<"hash_b">>, 0, <<"root_b">>},
		{<<"hash_a">>, 0, <<"root_a">>}
	], read_block_index()),

	?assertEqual(
		{error, not_found},
		update_block_index(0, 3, [
			{<<"hash_c">>, 0, <<"root_c">>}
		]),
		"Too many orphans"
	),

	?assertEqual(
		ok,
		update_block_index(2, 0, [
			{<<"hash_c">>, 0, <<"root_c">>}
		])
	),
	?assertEqual(
		ok,
		update_block_index(3, 0, [
			{<<"hash_d">>, 0, <<"root_d">>}
		])
	),
	?assertEqual([
		{<<"hash_d">>, 0, <<"root_d">>},
		{<<"hash_c">>, 0, <<"root_c">>},
		{<<"hash_b">>, 0, <<"root_b">>},
		{<<"hash_a">>, 0, <<"root_a">>}
	], read_block_index()),

	%% Orphan: 2 for 1
	?assertEqual(
		ok,
			update_block_index(4, 1, [
				{<<"hash_e">>, 0, <<"root_e">>},
				{<<"hash_f">>, 0, <<"root_f">>}
		])
	),
	?assertEqual([
		{<<"hash_f">>, 0, <<"root_f">>},
		{<<"hash_e">>, 0, <<"root_e">>},
		{<<"hash_c">>, 0, <<"root_c">>},
		{<<"hash_b">>, 0, <<"root_b">>},
		{<<"hash_a">>, 0, <<"root_a">>}
	], read_block_index()),

	%% Orphan: 1 for 1
	?assertEqual(
		ok,
			update_block_index(4, 1, [
				{<<"hash_g">>, 0, <<"root_g">>}
		])
	),
	?assertEqual([
		{<<"hash_g">>, 0, <<"root_g">>},
		{<<"hash_e">>, 0, <<"root_e">>},
		{<<"hash_c">>, 0, <<"root_c">>},
		{<<"hash_b">>, 0, <<"root_b">>},
		{<<"hash_a">>, 0, <<"root_a">>}
	], read_block_index()),

	%% Orphan: 1 for 2
	?assertEqual(
		ok,
			update_block_index(3, 2, [
				{<<"hash_h">>, 0, <<"root_h">>}
		])
	),
	?assertEqual([
		{<<"hash_h">>, 0, <<"root_h">>},
		{<<"hash_c">>, 0, <<"root_c">>},
		{<<"hash_b">>, 0, <<"root_b">>},
		{<<"hash_a">>, 0, <<"root_a">>}
	], read_block_index()),

	%% Orphan: 1 for 3
	?assertEqual(
		ok,
			update_block_index(1, 3, [
				{<<"hash_i">>, 0, <<"root_i">>}
		])
	),
	?assertEqual([
		{<<"hash_i">>, 0, <<"root_i">>},
		{<<"hash_a">>, 0, <<"root_a">>}
	], read_block_index()),

	%% Orphan: 2 for 2
	?assertEqual(
		ok,
			update_block_index(1, 2, [
				{<<"hash_j">>, 0, <<"root_j">>},
				{<<"hash_k">>, 0, <<"root_k">>}
		])
	),
	?assertEqual([
		{<<"hash_k">>, 0, <<"root_k">>},
		{<<"hash_j">>, 0, <<"root_j">>}
	], read_block_index()),


	%% Orphan: 3 for 1
	?assertEqual(
		ok,
			update_block_index(3, 1, [
				{<<"hash_l">>, 0, <<"root_l">>},
				{<<"hash_m">>, 0, <<"root_m">>},
				{<<"hash_n">>, 0, <<"root_n">>}
		])
	),
	?assertEqual([
		{<<"hash_n">>, 0, <<"root_n">>},
		{<<"hash_m">>, 0, <<"root_m">>},
		{<<"hash_l">>, 0, <<"root_l">>},
		{<<"hash_j">>, 0, <<"root_j">>}
	], read_block_index()),


	%% Replace all but genesis
	?assertEqual(
		ok,
			update_block_index(2, 3, [
				{<<"hash_o">>, 0, <<"root_o">>},
				{<<"hash_p">>, 0, <<"root_p">>}
		])
	),
	?assertEqual([
		{<<"hash_p">>, 0, <<"root_p">>},
		{<<"hash_o">>, 0, <<"root_o">>},
		{<<"hash_j">>, 0, <<"root_j">>}
	], read_block_index()).




