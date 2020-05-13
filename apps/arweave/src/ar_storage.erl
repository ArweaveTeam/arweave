-module(ar_storage).

-export([start/0]).
-export([write_block/1, write_block/2, write_full_block/1, write_full_block/2]).
-export([read_block/1, read_block/2, read_block_shadow/1]).
-export([invalidate_block/1, blocks_on_disk/0]).
-export([write_tx/1, write_tx_data/3, read_tx/1, read_tx_data/1]).
-export([read_wallet_list/1]).
-export([write_block_index/1, read_block_index/0]).
-export([delete_tx/1]).
-export([enough_space/1, select_drive/2]).
-export([calculate_disk_space/0, calculate_used_space/0, start_update_used_space/0]).
-export([lookup_block_filename/1, lookup_tx_filename/1, wallet_list_filepath/1]).
-export([tx_data_filepath/1]).
-export([read_tx_file/1]).
-export([ensure_directories/0, clear/0]).
-export([write_file_atomic/2]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%%% Reads and writes blocks from disk.

-define(DIRECTORY_SIZE_TIMER, 300000).

%% @doc Ready the system for block/tx reading and writing.
%% %% This function should block.
start() ->
	ar_firewall:start(),
	ensure_directories(),
	ok = migrate_block_filenames(),
	count_blocks_on_disk(),
	case ar_meta_db:get(disk_space) of
		undefined ->
			%% Add some margin for filesystem overhead.
			DiskSpaceWithMargin = round(calculate_disk_space() * 0.98),
			ar_meta_db:put(disk_space, DiskSpaceWithMargin),
			ok;
		_ ->
			ok
	end.

migrate_block_filenames() ->
	case v1_migration_file_exists() of
		true ->
			ok;
		false ->
			DataDir = ar_meta_db:get(data_dir),
			BlockDir = filename:join(DataDir, ?BLOCK_DIR),
			{ok, Filenames} = file:list_dir(BlockDir),
			lists:foreach(
				fun(Filename) ->
					case string:split(Filename, ".json") of
						[Height_Hash, []] ->
							case string:split(Height_Hash, "_") of
								[_, H] when length(H) == 64 ->
									rename_block_file(BlockDir, Filename, H ++ ".json");
								_ ->
									noop
							end;
						_ ->
							noop
					end
				end,
				Filenames
			),
			complete_v1_migration()
	end.

v1_migration_file_exists() ->
	filelib:is_file(v1_migration_file()).

v1_migration_file() ->
	filename:join([ar_meta_db:get(data_dir), ?STORAGE_MIGRATIONS_DIR, "v1"]).

rename_block_file(BlockDir, Source, Dest) ->
	file:rename(filename:join(BlockDir, Source), filename:join(BlockDir, Dest)).

complete_v1_migration() ->
	write_file_atomic(v1_migration_file(), <<>>).

count_blocks_on_disk() ->
	spawn(
		fun() ->
			DataDir = ar_meta_db:get(data_dir),
			case file:list_dir(filename:join(DataDir, ?BLOCK_DIR)) of
				{ok, List} ->
					ar_meta_db:increase(blocks_on_disk, length(List));
				{error, Reason} ->
					ar:warn([
						{event, failed_to_count_blocks_on_disk},
						{reason, Reason}
					]),
					error
			end
		end
	).

%% @doc Ensure that all of the relevant storage directories exist.
ensure_directories() ->
	DataDir = ar_meta_db:get(data_dir),
	%% Append "/" to every path so that filelib:ensure_dir/1 creates a directory if it does not exist.
	filelib:ensure_dir(filename:join(DataDir, ?TX_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?BLOCK_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?WALLET_LIST_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?HASH_LIST_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?STORAGE_MIGRATIONS_DIR) ++ "/").

%% @doc Clear the cache of saved blocks.
clear() ->
	lists:map(
		fun file:delete/1,
		filelib:wildcard(filename:join([ar_meta_db:get(data_dir), ?BLOCK_DIR, "*.json"]))
	),
	ar_meta_db:put(blocks_on_disk, 0).

%% @doc Returns the number of blocks stored on disk.
blocks_on_disk() ->
	ar_meta_db:get(blocks_on_disk).

%% @doc Move a block into the 'invalid' block directory.
invalidate_block(B) ->
	ar_meta_db:increase(blocks_on_disk, -1),
	TargetFile = invalid_block_filepath(B),
	filelib:ensure_dir(TargetFile),
	file:rename(block_filepath(B), TargetFile).

write_block(Blocks) when is_list(Blocks) -> lists:foreach(fun write_block/1, Blocks);
write_block(B) ->
	write_block(B, write_wallet_list).

write_block(B, WriteWalletList) ->
	case ar_meta_db:get(disk_logging) of
		true ->
			ar:info([
				{event, writing_block_to_disk},
				{block, ar_util:encode(B#block.indep_hash)}
			]);
		_ ->
			do_nothing
	end,
	case WriteWalletList of
		do_not_write_wallet_list ->
			noop;
		write_wallet_list ->
			ok = write_wallet_list(B)
	end,
	BlockJSON = ar_serialize:jsonify(ar_serialize:block_to_json_struct(B)),
	ByteSize = byte_size(BlockJSON),
	case enough_space(ByteSize) of
		true ->
			Filepath = block_filepath(B),
			IsOverwrite = filelib:is_file(Filepath),
			write_file_atomic(Filepath, BlockJSON),
			case not IsOverwrite of
				true ->
					ar_meta_db:increase(blocks_on_disk, 1),
					ar_meta_db:increase(used_space, ByteSize);
				false ->
					noop
			end,
			ok;
		false ->
			ar:err(
				[
					{event, not_enough_space_to_write_block}
				]
			),
			{error, not_enough_space}
	end.

write_full_block(B) ->
	BShadow = B#block{ txs = [T#tx.id || T <- B#block.txs] },
	write_full_block(BShadow, B#block.txs).

write_full_block(BShadow, TXs) ->
	ar_cleanup:cleanup_blocks_on_disck(BShadow),
	write_tx(TXs),
	write_block(BShadow),
	ar_arql_db:insert_full_block(BShadow#block{ txs = TXs }),
	app_ipfs:maybe_ipfs_add_txs(TXs).

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
read_block(H) ->
	case read_block_shadow(H) of
		unavailable ->
			unavailable;
		BShadow ->
			WalletList = case BShadow#block.wallet_list of
				WL when is_list(WL) ->
					WL;
				WL when is_binary(WL) ->
					case read_wallet_list(WL) of
						{ok, ReadWL} ->
							ReadWL;
						{error, _Type} ->
							not_found
					end
			end,
			case WalletList of
				not_found ->
					unavailable;
				_ ->
					BShadow#block {
						wallet_list = WalletList
					}
			end
	end.

%% @doc Read block shadow from disk, given a hash.
read_block_shadow(BH) ->
	case lookup_block_filename(BH) of
		unavailable ->
			unavailable;
		Filename ->
			case file:read_file(Filename) of
				{ok, JSON} ->
					case parse_block_shadow_json(JSON) of
						{error, _} ->
							invalidate_block(BH),
							unavailable;
						{ok, B} ->
							B
					end;
				{error, _} ->
					unavailable
			end
	end.

parse_block_shadow_json(JSON) ->
	case ar_serialize:json_decode(JSON) of
		{ok, JiffyStruct} ->
			case catch ar_serialize:json_struct_to_block(JiffyStruct) of
				B when is_record(B, block) ->
					{ok, B};
				_ ->
					{error, json_struct_to_block}
			end;
		Error ->
			Error
	end.

%% @doc Recalculate the used space in bytes of the data directory disk.
start_update_used_space() ->
	spawn(
		fun() ->
			UsedSpace = ar_meta_db:get(used_space),
			catch ar_meta_db:put(used_space, max(calculate_used_space(), UsedSpace)),
			timer:apply_after(
				?DIRECTORY_SIZE_TIMER,
				?MODULE,
				start_update_used_space,
				[]
			)
		end
	).

lookup_block_filename(H) ->
	Name = filename:join([
		ar_meta_db:get(data_dir),
		?BLOCK_DIR,
		binary_to_list(ar_util:encode(H)) ++ ".json"
	]),
	case filelib:is_file(Name) of
		true ->
			Name;
		false ->
			unavailable
	end.

%% @doc Delete the tx with the given hash from disk.
delete_tx(Hash) ->
	file:delete(tx_filepath(Hash)).

write_tx(TXs) when is_list(TXs) -> lists:foreach(fun write_tx/1, TXs);
write_tx(#tx{ format = 1 } = TX) ->
	write_tx_header(TX);
write_tx(#tx{ format = 2 } = TX) ->
	case write_tx_header(TX#tx{ data = <<>> }) of
		ok ->
			case byte_size(TX#tx.data) > 0 of
				true ->
					write_tx_data(TX#tx.id, TX#tx.data_root, TX#tx.data);
				false ->
					ok
			end;
		NotOk ->
			NotOk
	end;
write_tx(_TX) ->
	{error, unsupported_tx_format}.

write_tx_header(TX) ->
	%% Only store data that passes the firewall configured by the miner.
	case ar_firewall:scan_tx(TX) of
		accept ->
			write_tx_header_after_scan(TX);
		reject ->
			{error, firewall_check}
	end.

write_tx_header_after_scan(TX) ->
	TXJSON = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
	ByteSize = byte_size(TXJSON),
	case enough_space(ByteSize) of
		true ->
			write_file_atomic(
				tx_filepath(TX),
				TXJSON
			),
			spawn(
				ar_meta_db,
				increase,
				[used_space, ByteSize]
			),
			ok;
		false ->
			ar:err(
				[
					{event, not_enough_space_to_write_tx}
				]
			),
			{error, not_enough_space}
	end.

write_tx_data(ID, ExpectedDataRoot, Data) ->
	case (ar_tx:generate_chunk_tree(#tx{ data = Data }))#tx.data_root of
		ExpectedDataRoot ->
			write_tx_data(tx_data_filepath(ID), ar_util:encode(Data));
		_ ->
			{error, invalid_data_root}
	end.

write_tx_data(Filepath, EncodedData) ->
	ByteSize = byte_size(EncodedData),
	case enough_space(ByteSize) of
		true ->
			write_file_atomic(Filepath, EncodedData),
			ar_meta_db:increase(used_space, ByteSize),
			ok;
		false ->
			ar:err(
				[
					{event, not_enough_space_to_write_tx_data}
				]
			),
			{error, not_enough_space}
	end.

%% @doc Read a tx from disk, given a hash.
read_tx(unavailable) -> unavailable;
read_tx(TX) when is_record(TX, tx) -> TX;
read_tx(TXs) when is_list(TXs) ->
	lists:map(fun read_tx/1, TXs);
read_tx(ID) ->
	case read_tx_file(tx_filepath(ID)) of
		{ok, TX} ->
			TX;
		_Error ->
			unavailable
	end.

read_tx_file(Filename) ->
	case file:read_file(Filename) of
		{ok, <<>>} ->
			file:delete(Filename),
			ar:err([
				{event, empty_tx_file},
				{filename, Filename}
			]),
			{error, tx_file_empty};
		{ok, Binary} ->
			case catch ar_serialize:json_struct_to_tx(Binary) of
				TX when is_record(TX, tx) ->
					{ok, TX};
				_ ->
					file:delete(Filename),
					ar:err([
						{event, failed_to_parse_tx},
						{filename, Filename}
					]),
					{error, failed_to_parse_tx}
			end;
		Error ->
			Error
	end.

read_tx_data(TX) ->
	case file:read_file(tx_data_filepath(TX)) of
		{ok, Data} ->
			{ok, ar_util:decode(Data)};
		Error ->
			Error
	end.

%% Write a block index to disk for retreival later (in emergencies).
write_block_index(BI) ->
	ar:info([{event, writing_block_index_to_disk}]),
	JSON = ar_serialize:jsonify(ar_serialize:block_index_to_json_struct(BI)),
	File = block_index_filepath(),
	case write_file_atomic(File, JSON) of
		ok ->
			ok;
		{error, Reason} = Error ->
			ar:err([{event, failed_to_write_block_index_to_disk}, {reason, Reason}]),
			Error
	end.

%% Write a block hash list to disk for retreival later.
write_wallet_list(B) ->
	WalletList = B#block.wallet_list,
	ID = case B#block.wallet_list_hash of
		not_set ->
			ar_block:hash_wallet_list(B#block.height, B#block.reward_addr, WalletList);
		WalletListHash ->
			WalletListHash
	end,
	JSON = ar_serialize:jsonify(ar_serialize:wallet_list_to_json_struct(WalletList)),
	write_file_atomic(wallet_list_filepath(ID), JSON),
	ok.

%% @doc Read a list of block hashes from the disk.
read_block_index() ->
	case file:read_file(block_index_filepath()) of
		{ok, Binary} ->
			case ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(Binary)) of
				[H | _] = HL when is_binary(H) ->
					[{BH, not_set, not_set} || BH <- HL];
				BI ->
					BI
			end;
		Error ->
			Error
	end.

%% @doc Read a given wallet list (by hash) from the disk.
read_wallet_list([]) -> {ok, []};
read_wallet_list(WL = [{_, _, _} | _]) -> {ok, WL};
read_wallet_list(WalletListHash) ->
	Filename = wallet_list_filepath(WalletListHash),
	case file:read_file(Filename) of
		{ok, JSON} ->
			parse_wallet_list_json(JSON);
		{error, Reason} ->
			{error, {failed_reading_file, Filename, Reason}}
	end.

parse_wallet_list_json(JSON) ->
	case ar_serialize:json_decode(JSON) of
		{ok, JiffyStruct} ->
			{ok, ar_serialize:json_struct_to_wallet_list(JiffyStruct)};
		{error, Reason} ->
			{error, {invalid_json, Reason}}
	end.

lookup_tx_filename(ID) ->
	Filename = tx_filepath(ID),
	case filelib:is_file(Filename) of
		false ->
			unavailable;
		true ->
			Filename
	end.

% @doc Check that there is enough space to write Bytes bytes of data
enough_space(Bytes) ->
	(ar_meta_db:get(disk_space)) >= (Bytes + ar_meta_db:get(used_space)).

%% @doc Calculate the available space in bytes on the data directory disk.
calculate_disk_space() ->
	{_, KByteSize, _} = get_data_dir_disk_data(),
	KByteSize * 1024.

%% @doc Calculate the used space in bytes on the data directory disk.
calculate_used_space() ->
	{_, KByteSize, UsedPercentage} = get_data_dir_disk_data(),
	math:ceil(KByteSize * UsedPercentage / 100 * 1024).

get_data_dir_disk_data() ->
	application:ensure_started(sasl),
	application:ensure_started(os_mon),
	DataDir = filename:absname(ar_meta_db:get(data_dir)),
	[DiskData | _] = select_drive(disksup:get_disk_data(), DataDir),
	DiskData.

%% @doc Calculate the root drive in which the Arweave server resides
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
	to_string(filename:join([ar_meta_db:get(data_dir) | PathComponents])).

to_string(Bin) when is_binary(Bin) ->
	binary_to_list(Bin);
to_string(String) ->
	String.

block_filename(B) when is_record(B, block) ->
	block_filename(B#block.indep_hash);
block_filename(BH) when is_binary(BH) ->
	iolist_to_binary([ar_util:encode(BH), ".json"]).

block_filepath(B) ->
	filepath([?BLOCK_DIR, block_filename(B)]).

invalid_block_filepath(B) ->
	filepath([?BLOCK_DIR, "invalid", block_filename(B)]).

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

wallet_list_filepath(Hash) when is_binary(Hash) ->
	filepath([?WALLET_LIST_DIR, iolist_to_binary([ar_util:encode(Hash), ".json"])]).

write_file_atomic(Filename, Data) ->
	SwapFilename = Filename ++ ".swp",
	case file:write_file(SwapFilename, Data) of
		ok ->
			file:rename(SwapFilename, Filename);
		Error ->
			Error
	end.

%% @doc Test block storage.
store_and_retrieve_block_test() ->
	ar_storage:clear(),
	?assertEqual(0, blocks_on_disk()),
	B0s = [B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	?assertEqual(
		B0#block{ hash_list = unset },
		read_block(B0#block.indep_hash, ar_weave:generate_block_index([B0]))
	),
	B1s = [B1 | _] = ar_weave:add(B0s, []),
	ar_storage:write_block(B1),
	[B2 | _] = ar_weave:add(B1s, []),
	ar_storage:write_block(B2),
	ar_util:do_until(
		fun() ->
			3 == blocks_on_disk()
		end,
		100,
		2000
	),
	?assertEqual(
		B1#block{ hash_list = unset },
		read_block(B1#block.indep_hash)
	),
	?assertEqual(
		B1#block{ hash_list = unset },
		read_block(B1#block.height, ar_weave:generate_block_index([B1, B0]))
	).

store_and_retrieve_tx_test() ->
	Tx0 = ar_tx:new(<<"DATA1">>),
	write_tx(Tx0),
	Tx0 = read_tx(Tx0),
	Tx0 = read_tx(Tx0#tx.id),
	file:delete(tx_filepath(Tx0)).

%% @doc Ensure blocks can be written to disk, then moved into the 'invalid'
%% block directory.
invalidate_block_test() ->
	[B] = ar_weave:init(),
	write_full_block(B),
	invalidate_block(B),
	unavailable = read_block(B#block.indep_hash, ar_weave:generate_block_index([B])),
	TargetFile = filename:join([
		ar_meta_db:get(data_dir),
		?BLOCK_DIR,
		"invalid",
		binary_to_list(ar_util:encode(B#block.indep_hash)) ++ ".json"
	]),
	{ok, Binary} = file:read_file(TargetFile),
	{ok, JSON} = ar_serialize:json_decode(Binary),
	?assertEqual(B#block.indep_hash, (ar_serialize:json_struct_to_block(JSON))#block.indep_hash).

store_and_retrieve_block_block_index_test() ->
	[B0] = ar_weave:init([]),
	write_block(B0),
	[B1 | _] = ar_weave:add([B0], []),
	BI = ar_weave:generate_block_index([B1, B0]),
	write_block_index(BI),
	ReadBI = read_block_index(),
	?assertEqual(BI, ReadBI).

store_and_retrieve_wallet_list_test() ->
	[B0] = ar_weave:init(),
	write_wallet_list(B0),
	Height = B0#block.height,
	RewardAddr = B0#block.reward_addr,
	WL = B0#block.wallet_list,
	?assertEqual({ok, WL}, read_wallet_list(ar_block:hash_wallet_list(Height, RewardAddr, WL))).

handle_corrupted_wallet_list_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	Height = B0#block.height,
	RewardAddr = B0#block.reward_addr,
	?assertEqual(
		B0#block{ hash_list = unset },
		read_block(B0#block.indep_hash, ar_weave:generate_block_index([B0]))
	),
	WalletListHash = ar_block:hash_wallet_list(Height, RewardAddr, B0#block.wallet_list),
	ok = file:write_file(wallet_list_filepath(WalletListHash), <<>>),
	?assertEqual(unavailable, read_block(B0#block.indep_hash, ar_weave:generate_block_index([B0]))).
