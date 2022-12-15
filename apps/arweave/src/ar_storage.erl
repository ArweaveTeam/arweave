-module(ar_storage).

-behaviour(gen_server).

-export([start_link/0, write_full_block/2, read_block/1, read_block/2, write_tx/1,
		read_tx/1, read_tx_data/1, update_confirmation_index/1, get_tx_confirmation_data/1,
		get_wallet_list_range/2, read_wallet_list/1, write_wallet_list/4, write_wallet_list/2,
		write_wallet_list_chunk/3, write_block_index/1, write_block_index_and_price_history/2,
		read_block_index/0, read_block_index_and_price_history/0,
		delete_blacklisted_tx/1, get_sufficient_space_for_headers/0, lookup_tx_filename/1,
		wallet_list_filepath/1, tx_filepath/1, tx_data_filepath/1, read_tx_file/1,
		read_migrated_v1_tx_file/1, ensure_directories/1, write_file_atomic/2,
		write_term/2, write_term/3, read_term/1, read_term/2, delete_term/1, is_file/1,
		migrate_tx_record/1, migrate_block_record/1, update_price_history/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_wallets.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

-record(state, {
	tx_confirmation_db,
	tx_db,
	block_db,
	price_history_db
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

write_full_block(#block{ height = 0 } = BShadow, TXs) when ?NETWORK_NAME == "arweave.N.1" ->
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

update_confirmation_index(B) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(arql_tags_index, Config#config.enable) of
		true ->
			ar_arql_db:insert_full_block(B, store_tags);
		false ->
			put_tx_confirmation_data(B)
	end.

put_tx_confirmation_data(B) ->
	[{_, TXConfirmationDB}] = ets:lookup(?MODULE, tx_confirmation_db),
	Data = term_to_binary({B#block.height, B#block.indep_hash}),
	lists:foldl(
		fun	(TX, ok) ->
				ar_kv:put(TXConfirmationDB, TX#tx.id, Data);
			(_TX, Acc) ->
				Acc
		end,
		ok,
		B#block.txs
	).

%% @doc Return {BlockHeight, BlockHash} belonging to the block where
%% the given transaction was included.
get_tx_confirmation_data(TXID) ->
	[{_, TXConfirmationDB}] = ets:lookup(?MODULE, tx_confirmation_db),
	case ar_kv:get(TXConfirmationDB, TXID) of
		{ok, Binary} ->
			{ok, binary_to_term(Binary)};
		not_found ->
			case catch ar_arql_db:select_block_by_tx_id(ar_util:encode(TXID)) of
				{ok, #{
					height := Height,
					indep_hash := EncodedIndepHash
				}} ->
					{ok, {Height, ar_util:decode(EncodedIndepHash)}};
				not_found ->
					not_found;
				{'EXIT', {timeout, {gen_server, call, [ar_arql_db, _]}}} ->
					{error, timeout}
			end
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
			case catch ets:lookup(?MODULE, block_db) of
				[{_, DB}] ->
					case ar_kv:get(DB, BH) of
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
					end;
				_ ->
					?LOG_WARNING([{event, cannot_read_block},
							{block, ar_util:encode(BH)},
							{reason, kv_storage_not_initialized}]),
					unavailable
			end
	end.

%% @doc Return available disk space, in bytes.
get_sufficient_space_for_headers() ->
	{ok, Config} = application:get_env(arweave, config),
	DataDir = Config#config.data_dir,
	{_, KByteSize, CapacityKByteSize} = get_disk_data(DataDir),
	FreeDiskSpace =
		case Config#config.disk_space of
			undefined ->
				CapacityKByteSize * 1024;
			Limit ->
				max(0, Limit - (KByteSize - CapacityKByteSize) * 1024)
		end,
	DiskPoolSize = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
	DiskCacheSize = Config#config.disk_cache_size * 1048576,
	[{_, SameDriveStorageModulesTotalSize}] = ets:lookup(?MODULE,
			same_disk_storage_modules_total_size),
	BufferSize = 50000000000,
	ReservedSpaceSize = max(BufferSize,
			SameDriveStorageModulesTotalSize + DiskPoolSize + DiskCacheSize + BufferSize),
	max(0, FreeDiskSpace - ReservedSpaceSize).

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

delete_wallet_list(RootHash) ->
	WalletListFile = wallet_list_filepath(RootHash),
	case filelib:is_file(WalletListFile) of
		true ->
			case file:read_file_info(WalletListFile) of
				{ok, FileInfo} ->
					case file:delete(WalletListFile) of
						ok ->
							{ok, FileInfo#file_info.size};
						Error ->
							Error
					end;
				Error ->
					Error
			end;
		false ->
			delete_wallet_list_chunks(0, RootHash, 0)
	end.

delete_wallet_list_chunks(Position, RootHash, BytesRemoved) ->
	WalletListChunkFile = wallet_list_chunk_filepath(Position, RootHash),
	case filelib:is_file(WalletListChunkFile) of
		true ->
			case file:read_file_info(WalletListChunkFile) of
				{ok, FileInfo} ->
					case file:delete(WalletListChunkFile) of
						ok ->
							delete_wallet_list_chunks(
								Position + ?WALLET_LIST_CHUNK_SIZE,
								RootHash,
								BytesRemoved + FileInfo#file_info.size
							);
						Error ->
							Error
					end;
				Error ->
					Error
			end;
		false ->
			{ok, BytesRemoved}
	end.

%% @doc Delete the blacklisted tx with the given hash from disk. Return {ok, BytesRemoved} if
%% the removal is successful or the file does not exist. The reported number of removed
%% bytes does not include the migrated v1 data. The removal of migrated v1 data is requested
%% from ar_data_sync asynchronously. The v2 headers are not removed.
delete_blacklisted_tx(Hash) ->
	case ets:lookup(?MODULE, tx_db) of
		[{_, DB}] ->
			case ar_kv:get(DB, Hash) of
				{ok, V} ->
					TX = parse_tx_kv_binary(V),
					case TX#tx.format == 1 andalso TX#tx.data_size > 0 of
						true ->
							ar_data_sync:request_tx_data_removal(Hash),
							case ar_kv:delete(DB, Hash) of
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
									ar_data_sync:request_tx_data_removal(Hash),
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
	#block{ nonce = Nonce, previous_block = PrevH, timestamp = TS,
			last_retarget = Last, diff = Diff, height = Height, hash = Hash,
			indep_hash = H, txs = TXs, tx_root = TXRoot, tx_tree = TXTree,
			hash_list = HL, hash_list_merkle = HLMerkle, wallet_list = WL,
			reward_addr = RewardAddr, tags = Tags, reward_pool = RewardPool,
			weave_size = WeaveSize, block_size = BlockSize, cumulative_diff = CDiff,
			size_tagged_txs = SizeTaggedTXs, poa = PoA, usd_to_ar_rate = Rate,
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
	case ets:lookup(?MODULE, tx_db) of
		[{_, DB}] ->
			TX2 =
				case TX#tx.format of
					1 ->
						TX;
					_ ->
						TX#tx{ data = <<>> }
				end,
			ar_kv:put(DB, TX#tx.id, ar_serialize:tx_to_binary(TX2));
		_ ->
			{error, not_initialized}
	end.

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
	case catch ets:lookup(?MODULE, tx_db) of
		[{_, DB}] ->
			case ar_kv:get(DB, ID) of
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
			end;
		_ ->
			?LOG_WARNING([{event, cannot_read_transaction}, {tx, ar_util:encode(ID)},
					{reason, kv_storage_not_initialized}]),
			unavailable
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

%% @doc Write the givne block index to disk.
%% Read when a node starts with the start_from_block_index flag.
write_block_index(BI) ->
	?LOG_INFO([{event, writing_block_index_to_disk}]),
	Bin = ar_serialize:block_index_to_binary(BI),
	File = block_index_filepath(),
	case write_file_atomic(File, Bin) of
		ok ->
			ok;
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_write_block_index_to_disk}, {reason, Reason}]),
			Error
	end.

%% @doc Write the given block index and price history data to disk. Read when a
%% node starts with the start_from_block_index flag.
write_block_index_and_price_history(BI, PriceHistory) ->
	?LOG_INFO([{event, writing_block_index_and_price_history_to_disk}]),
	File = block_index_and_price_history_filepath(),
	case write_file_atomic(File, term_to_binary({BI, PriceHistory})) of
		ok ->
			ok;
		{error, Reason} = Error ->
			?LOG_ERROR([{event, failed_to_write_block_index_and_price_history_to_disk},
					{reason, Reason}]),
			Error
	end.

write_wallet_list(RootHash, Tree) ->
	write_wallet_list_chunks(RootHash, Tree, first, 0).

write_wallet_list_chunks(RootHash, Tree, Cursor, Position) ->
	{NextCursor, Range} = get_wallet_list_range(Tree, Cursor),
	StoredRange = case NextCursor of last -> [last | Range]; _ -> Range end,
	case {write_wallet_list_chunk(RootHash, StoredRange, Position), NextCursor} of
		{ok, last} ->
			ok;
		{ok, _} ->
			NextPosition = Position + ?WALLET_LIST_CHUNK_SIZE,
			write_wallet_list_chunks(RootHash, Tree, NextCursor, NextPosition);
		{{error, _Reason} = Error, _} ->
			Error
	end.

write_wallet_list_chunk(RootHash, Range, Position) ->
	{ok, Config} = application:get_env(arweave, config),
	Name = wallet_list_chunk_relative_filepath(Position, RootHash),
	case write_term(Config#config.data_dir, Name, Range, do_not_override) of
		ok ->
			ok;
		{error, Reason} = Error ->
			?LOG_ERROR([
				{event, failed_to_write_wallet_list_chunk},
				{reason, Reason}
			]),
			Error
	end.

%% Write a block hash list to disk for retrieval later.
write_wallet_list(ID, RewardAddr, IsRewardAddrNew, WalletList) ->
	JSON = ar_serialize:jsonify(
		ar_serialize:wallet_list_to_json_struct(RewardAddr, IsRewardAddrNew, WalletList)
	),
	Filepath = wallet_list_filepath(ID),
	case filelib:is_file(Filepath) of
		true ->
			ok;
		false ->
			write_file_atomic(Filepath, JSON)
	end.

%% @doc Read a list of block hashes from the disk.
read_block_index() ->
	case file:read_file(block_index_filepath()) of
		{ok, Binary} ->
			case ar_serialize:binary_to_block_index(Binary) of
				{ok, BI} ->
					BI;
				{error, _} ->
					case ar_serialize:json_struct_to_block_index(
							ar_serialize:dejsonify(Binary)) of
						[H | _] = HL when is_binary(H) ->
							[{BH, not_set, not_set} || BH <- HL];
						BI ->
							BI
					end
			end;
		Error ->
			Error
	end.

%% @doc Read the latest stored block index and price history data from disk.
read_block_index_and_price_history() ->
	case file:read_file(block_index_and_price_history_filepath()) of
		{ok, Binary} ->
			binary_to_term(Binary, [safe]);
		Error ->
			Error
	end.

get_wallet_list_range(Tree, Cursor) ->
	Range =
		case Cursor of
			first ->
				ar_patricia_tree:get_range(?WALLET_LIST_CHUNK_SIZE + 1, Tree);
			_ ->
				ar_patricia_tree:get_range(Cursor, ?WALLET_LIST_CHUNK_SIZE + 1, Tree)
		end,
	case length(Range) of
		?WALLET_LIST_CHUNK_SIZE + 1 ->
			{element(1, hd(Range)), tl(Range)};
		_ ->
			{last, Range}
	end.

%% @doc Read a given wallet list (by hash) from the disk.
read_wallet_list(<<>>) ->
	{ok, ar_patricia_tree:new()};
read_wallet_list(WalletListHash) when is_binary(WalletListHash) ->
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
read_wallet_list(WL) when is_list(WL) ->
	{ok, ar_patricia_tree:from_proplist([{get_wallet_key(T), get_wallet_value(T)}
			|| T <- WL])}.

get_wallet_key(T) ->
	element(1, T).

get_wallet_value({_, Balance, LastTX}) ->
	{Balance, LastTX};
get_wallet_value({_, Balance, LastTX, Denomination}) ->
	{Balance, LastTX, Denomination}.

read_wallet_list_chunk(RootHash) ->
	read_wallet_list_chunk(RootHash, 0, ar_patricia_tree:new()).

read_wallet_list_chunk(RootHash, Position, Tree) ->
	Filename =
		case ar_disk_cache:lookup_wallet_list_chunk_filename(RootHash, Position) of
			{ok, Name} ->
				Name;
			_ ->
				{ok, Config} = application:get_env(arweave, config),
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
				]))
		end,
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

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	ensure_directories(Config#config.data_dir),
	{ok, DB} = ar_kv:open_without_column_families(
			filename:join(?ROCKS_DB_DIR, "ar_storage_tx_confirmation_db"), []),
	{ok, TXDB} = ar_kv:open_without_column_families(
			filename:join(?ROCKS_DB_DIR, "ar_storage_tx_db"), []),
	{ok, BlockDB} = ar_kv:open_without_column_families(
			filename:join(?ROCKS_DB_DIR, "ar_storage_block_db"), []),
	{ok, PriceHistoryDB} = ar_kv:open_without_column_families(
			filename:join(?ROCKS_DB_DIR, "price_history_db"), []),
	ets:insert(?MODULE, [{tx_confirmation_db, DB}, {tx_db, TXDB}, {block_db, BlockDB},
			{price_history_db, PriceHistoryDB}]),
	ets:insert(?MODULE, [{same_disk_storage_modules_total_size,
			get_same_disk_storage_modules_total_size()}]),
	{ok, #state{ tx_confirmation_db = DB, tx_db = TXDB, block_db = BlockDB,
			price_history_db = PriceHistoryDB }}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, #state{ tx_confirmation_db = DB, tx_db = TXDB, block_db = BlockDB,
		price_history_db = PriceHistoryDB }) ->
	ar_kv:close(DB),
	ar_kv:close(TXDB),
	ar_kv:close(BlockDB),
	ar_kv:close(PriceHistoryDB),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

write_block(B) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(disk_logging, Config#config.enable) of
		true ->
			?LOG_INFO([{event, writing_block_to_disk},
					{block, ar_util:encode(B#block.indep_hash)}]);
		_ ->
			do_nothing
	end,
	case ets:lookup(?MODULE, block_db) of
		[{_, DB}] ->
			TXIDs = lists:map(fun(TXID) when is_binary(TXID) -> TXID;
					(#tx{ id = TXID }) -> TXID end, B#block.txs),
			case ar_kv:put(DB, B#block.indep_hash, ar_serialize:block_to_binary(B#block{
					txs = TXIDs })) of
				ok ->
					update_price_history(B);
				Error ->
					Error
			end;
		_ ->
			{error, not_initialized}
	end.

update_price_history(B) ->
	case B#block.height >= ar_fork:height_2_6() of
		true ->
			case ets:lookup(?MODULE, price_history_db) of
				[] ->
					{error, not_initialized};
				[{_, PriceHistoryDB}] ->
					HashRate = ar_difficulty:get_hash_rate(B#block.diff),
					Bin = term_to_binary({HashRate, B#block.reward}),
					ar_kv:put(PriceHistoryDB, B#block.indep_hash, Bin)
			end;
		false ->
			ok
	end.

is_blacklisted(#tx{ format = 2 }) ->
	false;
is_blacklisted(#tx{ id = TXID }) ->
	ar_tx_blacklist:is_tx_blacklisted(TXID).

write_full_block2(BShadow, TXs) ->
	case write_block(BShadow) of
		ok ->
			app_ipfs:maybe_ipfs_add_txs(TXs),
			ok;
		Error ->
			Error
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

get_disk_data(Dir) ->
	[DiskData | _] = select_drive(ar_disksup:get_disk_data(), filename:absname(Dir)),
	DiskData.

select_drive(Disks, []) ->
	CWD = "/",
	case
		Drives = lists:filter(
			fun({Name, _, _}) ->
				case Name == CWD of
					false -> false;
					true -> true
				end
			end,
			Disks
		)
	of
		[] -> false;
		Drives ->
			Drives
	end;
select_drive(Disks, CWD) ->
	try
		case
			Drives = lists:filter(
				fun({Name, _, _}) ->
					try
						case string:find(Name, CWD) of
							nomatch -> false;
							_ -> true
						end
					catch _:_ -> false
					end
				end,
				Disks
			)
		of
			[] -> select_drive(Disks, hd(string:split(CWD, "/", trailing)));
			Drives -> Drives
		end
	catch _:_ -> select_drive(Disks, [])
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

block_index_filepath() ->
	filepath([?HASH_LIST_DIR, <<"last_block_index.json">>]).

block_index_and_price_history_filepath() ->
	filepath([?HASH_LIST_DIR, <<"last_block_index_and_price_history.bin">>]).

wallet_list_filepath(Hash) when is_binary(Hash) ->
	filepath([?WALLET_LIST_DIR, iolist_to_binary([ar_util:encode(Hash), ".json"])]).

wallet_list_chunk_filepath(Position, RootHash) when is_binary(RootHash) ->
	{ok, Config} = application:get_env(arweave, config),
	filename:join(
		Config#config.data_dir,
		wallet_list_chunk_relative_filepath(Position, RootHash)
	).

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
					?LOG_ERROR([{event, failed_to_write_term}, {name, Name}, {reason, Reason}]),
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
	?assertEqual(B0#block{ size_tagged_txs = unset, txs = TXIDs, price_history = [] },
			FetchedB01),
	?assertEqual(B0#block{ size_tagged_txs = unset, txs = TXIDs, price_history = [] },
			FetchedB03),
	ar_node:mine(),
	ar_test_node:wait_until_height(1),
	ar_node:mine(),
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

store_and_retrieve_block_block_index_test() ->
	RandomEntry =
		fun() ->
			{crypto:strong_rand_bytes(48), rand:uniform(10000),
					crypto:strong_rand_bytes(32)}
		end,
	BI = [RandomEntry() || _ <- lists:seq(1, 100)],
	write_block_index(BI),
	ReadBI = read_block_index(),
	?assertEqual(BI, ReadBI).

store_and_retrieve_wallet_list_test() ->
	[B0] = ar_weave:init(),
	[TX] = B0#block.txs,
	Addr = ar_wallet:to_address(TX#tx.owner, {?RSA_SIGN_ALG, 65537}),
	write_block(B0),
	ExpectedWL = ar_patricia_tree:from_proplist([{Addr, {0, TX#tx.id}}]),
	{WalletListHash, _} = ar_block:hash_wallet_list(ExpectedWL),
	{ok, ActualWL} = read_wallet_list(WalletListHash),
	assert_wallet_trees_equal(ExpectedWL, ActualWL).

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
			{RootHash, _} = ar_block:hash_wallet_list(Tree),
			%% Chunked write.
			ok = write_wallet_list(RootHash, Tree),
			{ok, ReadTree} = read_wallet_list(RootHash),
			assert_wallet_trees_equal(Tree, ReadTree),
			?assertEqual(true, filelib:is_file(wallet_list_chunk_filepath(0, RootHash))),
			?assertMatch({ok, _}, delete_wallet_list(RootHash)),
			?assertEqual({ok, 0}, delete_wallet_list(RootHash)),
			?assertEqual(not_found, read_wallet_list(RootHash)),
			?assertEqual(false, filelib:is_file(wallet_list_chunk_filepath(0, RootHash))),
			%% Not chunked write - wallets before the fork 2.2.
			ok = write_wallet_list(RootHash, unclaimed, false, Tree),
			?assertEqual(false, filelib:is_file(wallet_list_chunk_filepath(0, RootHash))),
			?assertEqual(true, filelib:is_file(wallet_list_filepath(RootHash))),
			{ok, ReadTree2} = read_wallet_list(RootHash),
			assert_wallet_trees_equal(Tree, ReadTree2),
			?assertMatch({ok, _}, delete_wallet_list(RootHash)),
			?assertEqual({ok, 0}, delete_wallet_list(RootHash)),
			?assertEqual(false, filelib:is_file(wallet_list_filepath(RootHash))),
			?assertEqual(not_found, read_wallet_list(RootHash))
		end,
		TestCases
	).

random_wallet() ->
	{crypto:strong_rand_bytes(32), {rand:uniform(1000000000), crypto:strong_rand_bytes(32)}}.
