-module(ar_storage).

-export([start/0]).
-export([write_block/1, write_full_block/1, write_full_block/2, read_block/2, clear/0]).
-export([write_encrypted_block/2, read_encrypted_block/1, invalidate_block/1]).
-export([delete_block/1, blocks_on_disk/0, block_exists/1]).
-export([write_tx/1, read_tx/1]).
-export([write_wallet_list/1, read_wallet_list/1]).
-export([write_block_hash_list/2, read_block_hash_list/1]).
-export([delete_tx/1, txs_on_disk/0, tx_exists/1]).
-export([enough_space/1, select_drive/2]).
-export([calculate_disk_space/0, calculate_used_space/0, start_update_used_space/0]).
-export([lookup_block_filename/1,lookup_tx_filename/1]).
-export([read_block_file/2, read_tx_file/1]).
-export([ensure_directories/0]).

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
	ar_block_index:start(),
	case ar_meta_db:get(disk_space) of
		undefined ->
			%% Add some margin for filesystem overhead.
			DiskSpaceWithMargin = round(calculate_disk_space() * 0.98),
			ar_meta_db:put(disk_space, DiskSpaceWithMargin),
			ok;
		_ ->
			ok
	end.

%% @doc Ensure that all of the relevant storage directories exist.
ensure_directories() ->
	DataDir = ar_meta_db:get(data_dir),
	%% Append "/" to every path so that filelib:ensure_dir/1 creates a directory if it does not exist.
	filelib:ensure_dir(filename:join(DataDir, ?TX_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?BLOCK_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?ENCRYPTED_BLOCK_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?WALLET_LIST_DIR) ++ "/"),
	filelib:ensure_dir(filename:join(DataDir, ?HASH_LIST_DIR) ++ "/").

%% @doc Clear the cache of saved blocks.
clear() ->
	lists:map(fun file:delete/1, filelib:wildcard(filename:join([ar_meta_db:get(data_dir), ?BLOCK_DIR, "*.json"]))),
	ar_block_index:clear().

%% @doc Removes a saved block.
delete_block(Hash) ->
	ar_block_index:remove(Hash),
	file:delete(block_filepath(Hash)).

%% @doc Returns the number of blocks stored on disk.
blocks_on_disk() ->
	ar_block_index:count().

block_exists(Hash) ->
	case filelib:find_file(block_filepath(Hash)) of
		{ok, _} -> true;
		{error, _} -> false
	end.

%% @doc Move a block into the 'invalid' block directory.
invalidate_block(B) ->
	ar_block_index:remove(B#block.indep_hash),
	TargetFile = invalid_block_filepath(B),
	filelib:ensure_dir(TargetFile),
	file:rename(block_filepath(B), TargetFile).

%% @doc Write a block (with the hash.json as the filename) to disk.
%% When debug is set, does not consider disk space. This is currently
%% necessary because of test timings
-ifdef(DEBUG).
write_block(Bs) when is_list(Bs) -> lists:foreach(fun write_block/1, Bs);
write_block(RawB) ->
	case ar_meta_db:get(disk_logging) of
		true ->
			ar:report([{writing_block_to_disk, ar_util:encode(RawB#block.indep_hash)}]);
		_ ->
			do_nothing
	end,
	WalletID = write_wallet_list(RawB#block.wallet_list),
	B = RawB#block { wallet_list = WalletID },
	BlockToWrite = ar_serialize:jsonify(ar_serialize:block_to_json_struct(B)),
	file:write_file(Name = block_filepath(B), BlockToWrite),
	ar_block_index:add(B, Name),
	Name.
-else.
write_block(Bs) when is_list(Bs) -> lists:foreach(fun write_block/1, Bs);
write_block(RawB) ->
	case ar_meta_db:get(disk_logging) of
		true ->
			ar:report([{writing_block_to_disk, ar_util:encode(RawB#block.indep_hash)}]);
		_ ->
			do_nothing
	end,
	WalletID = write_wallet_list(RawB#block.wallet_list),
	B = RawB#block { wallet_list = WalletID },
	BlockToWrite = ar_serialize:jsonify(ar_serialize:block_to_json_struct(B)),
	case enough_space(byte_size(BlockToWrite)) of
		true ->
			file:write_file(Name = block_filepath(B), BlockToWrite),
			ar_block_index:add(B, Name),
			spawn(
				ar_meta_db,
				increase,
				[used_space, byte_size(BlockToWrite)]
			),
			Name;
		false ->
			ar:err(
				[
					{not_enough_space_to_write_block},
					{block_not_written}
				]
			),
			{error, not_enough_space}
	end.
-endif.

write_full_block(B) ->
	BShadow = B#block { txs = [T#tx.id || T <- B#block.txs] },
	write_full_block(BShadow, B#block.txs).

write_full_block(BShadow, TXs) ->
	%% We only store data that passes the firewall configured by the miner.
	ScannedTXs = lists:filter(
		fun(TX) ->
			case ar_firewall:scan_tx(TX) of
				accept ->
					true;
				reject ->
					false
			end
		end,
		TXs
	),
	write_tx(ScannedTXs),
	write_block(BShadow),
	ar_tx_search:update_tag_table(BShadow#block{ txs = ScannedTXs }),
	app_ipfs:maybe_ipfs_add_txs(ScannedTXs).

%% @doc Write an encrypted	block (with the hash.json as the filename) to disk.
%% When debug is set, does not consider disk space. This is currently
%% necessary because of test timings
-ifdef(DEBUG).
write_encrypted_block(Hash, B) ->
	BlockToWrite = B,
	file:write_file(Name = encrypted_block_filepath(Hash), BlockToWrite),
	Name.
-else.
write_encrypted_block(Hash, B) ->
	BlockToWrite = B,
	case enough_space(byte_size(BlockToWrite)) of
		true ->
			file:write_file(Name = encrypted_block_filepath(Hash), BlockToWrite),
			spawn(
				ar_meta_db,
				increase,
				[used_space, byte_size(BlockToWrite)]
			),
			Name;
		false ->
			ar:report(
				[
					{not_enough_space_to_write_block},
					{block_not_written}
				]
			),
			{error, enospc}
	end.
-endif.

%% @doc Read a block from disk, given a hash.
read_block(unavailable, _BHL) -> unavailable;
read_block(B, _BHL) when is_record(B, block) -> B;
read_block(Bs, BHL) when is_list(Bs) ->
	lists:map(fun(B) -> read_block(B, BHL) end, Bs);
read_block(ID, BHL) ->
	case ar_block_index:get_block_filename(ID) of
		unavailable -> unavailable;
		Filename -> read_block_file(Filename, BHL)
	end.
read_block_file(Filename, BHL) ->
	{ok, Binary} = file:read_file(Filename),
	B = ar_serialize:json_struct_to_block(Binary),
	WL = B#block.wallet_list,
	FinalB =
		B#block {
			hash_list = ar_block:generate_hash_list_for_block(B, BHL),
			wallet_list =
				case WL of
					WL when is_list(WL) ->
						WL;
					WL when is_binary(WL) ->
						case read_wallet_list(WL) of
							{ok, ReadWL} ->
								ReadWL;
							{error, Type} ->
								ar:report(
									[
										{
											error_reading_wallet_list_from_disk,
											ar_util:encode(B#block.indep_hash)
										},
										{type, Type}
									]
								),
								not_found
						end
				end
		},
	case FinalB#block.wallet_list of
		not_found ->
			invalidate_block(B),
			unavailable;
		_ -> FinalB
	end.

%% @doc Read an encrypted block from disk, given a hash.
read_encrypted_block(unavailable) -> unavailable;
read_encrypted_block(ID) ->
	case file:read_file(encrypted_block_filepath(ID)) of
		{ok, Binary} ->
			Binary;
		{error, _} ->
			unavailable
	end.

%% @doc Accurately recalculate the current cumulative size of the Arweave directory
start_update_used_space() ->
	spawn(
		fun() ->
			catch ar_meta_db:put(used_space, calculate_used_space()),
			timer:apply_after(?DIRECTORY_SIZE_TIMER, ?MODULE, start_update_used_space, [])
		end
	).

lookup_block_filename(ID) ->
	ar_block_index:get_block_filename(ID).

%% @doc Delete the tx with the given hash from disk.
delete_tx(Hash) ->
	file:delete(tx_filepath(Hash)).

%% @doc Returns the number of transactions stored on disk.
txs_on_disk() ->
	{ok, Files} = file:list_dir(filename:join(ar_meta_db:get(data_dir), ?TX_DIR)),
	length(Files).

%% @doc Returns whether the TX with the given hash is stored on disk.
tx_exists(Hash) ->
	case filelib:find_file(tx_filepath(Hash)) of
		{ok, _} -> true;
		{error, _} -> false
	end.

%% @doc Write a tx (with the txid.json as the filename) to disk.
%% When debug is set, does not consider disk space. This is currently
%% necessary because of test timings
-ifdef(DEBUG).
write_tx(TXs) when is_list(TXs) -> lists:foreach(fun write_tx/1, TXs);
write_tx(TX) ->
	file:write_file(
		Name = tx_filepath(TX),
		ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
	),
	Name.
-else.
write_tx(TXs) when is_list(TXs) -> lists:foreach(fun write_tx/1, TXs);
write_tx(TX) ->
	TXToWrite = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
	case enough_space(byte_size(TXToWrite)) of
		true ->
			file:write_file(
				Name = tx_filepath(TX),
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
			),
			spawn(
				ar_meta_db,
				increase,
				[used_space, byte_size(TXToWrite)]
			),
			Name;
		false ->
			ar:report(
				[
					{not_enough_space_to_write_tx},
					{tx_not_written}
				]
			),
			{error, enospc}
	end.
-endif.

%% @doc Read a tx from disk, given a hash.
read_tx(unavailable) -> unavailable;
read_tx(Tx) when is_record(Tx, tx) -> Tx;
read_tx(Txs) when is_list(Txs) ->
	lists:map(fun read_tx/1, Txs);
read_tx(ID) ->
	case filelib:wildcard(tx_filepath(ID)) of
		[] -> unavailable;
		[Filename] -> read_tx_file(Filename);
		Filenames ->
			read_tx_file(hd(
				lists:sort(
					fun(Filename, Filename2) ->
						{ok, Info} = file:read_file_info(Filename, [{time, posix}]),
						{ok, Info2} = file:read_file_info(Filename2, [{time, posix}]),
						Info#file_info.mtime >= Info2#file_info.mtime
					end,
					Filenames
				)
			))
	end.

read_tx_file(Filename) ->
	{ok, Binary} = file:read_file(Filename),
	ar_serialize:json_struct_to_tx(Binary).

%% Write a block hash list to disk for retreival later (in emergencies).
write_block_hash_list(Hash, BHL) ->
	ar:report([{writing_block_hash_list_to_disk, ID = ar_util:encode(Hash)}]),
	JSON = ar_serialize:jsonify(ar_serialize:hash_list_to_json_struct(BHL)),
	file:write_file(hash_list_filepath(Hash), JSON),
	ID.

%% Write a block hash list to disk for retreival later (in emergencies).
write_wallet_list(WalletList) ->
	ID = ar_block:hash_wallet_list(WalletList),
	JSON = ar_serialize:jsonify(ar_serialize:wallet_list_to_json_struct(WalletList)),
	file:write_file(wallet_list_filepath(ID), JSON),
	ID.

%% @doc Read a list of block hashes from the disk.
read_block_hash_list(Hash) ->
	{ok, Binary} = file:read_file(hash_list_filepath(Hash)),
	ar_serialize:json_struct_to_hash_list(ar_serialize:dejsonify(Binary)).

%% @doc Read a given wallet list (by hash) from the disk.
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
	case filelib:wildcard(tx_filepath(ID)) of
		[] -> unavailable;
		[Filename] -> Filename;
		Filenames ->
			hd(lists:sort(
					fun(Filename, Filename2) ->
						{ok, Info} = file:read_file_info(Filename, [{time, posix}]),
						{ok, Info2} = file:read_file_info(Filename2, [{time, posix}]),
						Info#file_info.mtime >= Info2#file_info.mtime
					end,
					Filenames
				)
			)
	end.

% @doc Check that there is enough space to write Bytes bytes of data
enough_space(Bytes) ->
	(ar_meta_db:get(disk_space)) >= (Bytes + ar_meta_db:get(used_space)).

%% @doc Sum up the sizes of the files written by the Arweave client.
calculate_used_space() ->
	DataDir = ar_meta_db:get(data_dir),
	{ok, CWD} = file:get_cwd(),
	Dirs = [
		filename:join(DataDir, ?BLOCK_DIR),
		filename:join(DataDir, ?ENCRYPTED_BLOCK_DIR),
		filename:join(DataDir, ?HASH_LIST_DIR),
		filename:join(DataDir, ?TX_DIR),
		filename:join(DataDir, ?TX_INDEX_DIR),
		filename:join(DataDir, ?WALLET_DIR),
		filename:join(DataDir, ?WALLET_LIST_DIR),
		filename:join(CWD, ?LOG_DIR)
	],
	lists:foldl(
		fun(Dir, Acc) ->
			Acc + calculate_used_space(Dir)
		end,
		0,
		Dirs
	).

%% @doc Sum up the sizes of the files located directly (not in subdir) under the directory.
calculate_used_space(Directory) ->
	filelib:fold_files(
		Directory,
		"",
		false,
		fun(File, Acc) -> Acc + filelib:file_size(File) end,
		0
	).

%% @doc Calculate the total amount of disk space available
calculate_disk_space() ->
	application:start(sasl),
	application:start(os_mon),
	DataDir = filename:absname(ar_meta_db:get(data_dir)),
	[{_,Size,_}|_] = select_drive(disksup:get_disk_data(), DataDir),
	Size*1024.

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
	iolist_to_binary([
		integer_to_list(B#block.height), "_", ar_util:encode(B#block.indep_hash), ".json"
	]);
block_filename(Hash) when is_binary(Hash) ->
	iolist_to_binary(["*_", ar_util:encode(Hash), ".json"]);
block_filename(Height) when is_integer(Height) ->
	iolist_to_binary([integer_to_list(Height), "_*.json"]).

block_filepath(B) ->
	filepath([?BLOCK_DIR, block_filename(B)]).

invalid_block_filepath(B) ->
	filepath([?BLOCK_DIR, "invalid", block_filename(B)]).

encrypted_block_filepath(Hash) when is_binary(Hash) ->
	filepath([?ENCRYPTED_BLOCK_DIR, iolist_to_binary(["encrypted_", ar_util:encode(Hash), ".json"])]).

tx_filepath(TX) ->
	filepath([?TX_DIR, tx_filename(TX)]).

tx_filename(TX) when is_record(TX, tx) ->
	iolist_to_binary([ar_util:encode(TX#tx.id), ".json"]);
tx_filename(TXID) when is_binary(TXID) ->
	iolist_to_binary([ar_util:encode(TXID), ".json"]).

hash_list_filepath(Hash) when is_binary(Hash) ->
	filepath([?HASH_LIST_DIR, iolist_to_binary([ar_util:encode(Hash), ".json"])]).

wallet_list_filepath(Hash) when is_binary(Hash) ->
	filepath([?WALLET_LIST_DIR, iolist_to_binary([ar_util:encode(Hash), ".json"])]).

%% @doc Test block storage.
store_and_retrieve_block_test() ->
	ar_storage:clear(),
	?assertEqual(0, blocks_on_disk()),
	B0s = [B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	B0 = read_block(B0#block.indep_hash, B0#block.hash_list),
	B1s = [B1|_] = ar_weave:add(B0s, []),
	ar_storage:write_block(B1),
	[B2|_] = ar_weave:add(B1s, []),
	ar_storage:write_block(B2),
	write_block(B1),
	?assertEqual(3, blocks_on_disk()),
	B1 = read_block(B1#block.indep_hash, B2#block.hash_list),
	B1 = read_block(B1#block.height, B2#block.hash_list).

clear_blocks_test() ->
	ar_storage:clear(),
	?assertEqual(0, blocks_on_disk()).

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
	timer:sleep(500),
	unavailable = read_block(B#block.indep_hash, B#block.hash_list),
	TargetFile =
		lists:flatten(
			io_lib:format(
				"~s/invalid/~w_~s.json",
				[ar_meta_db:get(data_dir) ++ "/" ++ ?BLOCK_DIR, B#block.height, ar_util:encode(B#block.indep_hash)]
			)
		),
	?assertEqual(B, read_block_file(TargetFile, B#block.hash_list)).

store_and_retrieve_block_hash_list_test() ->
	ID = crypto:strong_rand_bytes(32),
	B0s = ar_weave:init([]),
	write_block(hd(B0s)),
	B1s = ar_weave:add(B0s, []),
	write_block(hd(B1s)),
	[B2|_] = ar_weave:add(B1s, []),
	write_block_hash_list(ID, B2#block.hash_list),
	receive after 500 -> ok end,
	BHL = read_block_hash_list(ID),
	BHL = B2#block.hash_list.

store_and_retrieve_wallet_list_test() ->
	[B0] = ar_weave:init(),
	write_wallet_list(WL = B0#block.wallet_list),
	receive after 500 -> ok end,
	?assertEqual({ok, WL}, read_wallet_list(ar_block:hash_wallet_list(WL))).

handle_corrupted_wallet_list_test() ->
	ar_storage:clear(),
	[B0] = ar_weave:init([]),
	ar_storage:write_block(B0),
	?assertEqual(B0, read_block(B0#block.indep_hash, B0#block.hash_list)),
	WalletListHash = ar_block:hash_wallet_list(B0#block.wallet_list),
	ok = file:write_file(wallet_list_filepath(WalletListHash), <<>>),
	?assertEqual(unavailable, read_block(B0#block.indep_hash, B0#block.hash_list)).
