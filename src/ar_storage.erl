-module(ar_storage).
-export([write_block/1, read_block/1, clear/0]).
-export([write_encrypted_block/2, read_encrypted_block/1]).
-export([delete_block/1, blocks_on_disk/0, block_exists/1]).
-export([write_tx/1, read_tx/1]).
-export([delete_tx/1, txs_on_disk/0, tx_exists/1]).
-export([enough_space/1, select_drive/2]).
-export([calculate_disk_space/0, calculate_used_space/0, update_directory_size/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%%% Reads and writes blocks from disk.

%% Where should the blocks be stored?
-define(BLOCK_DIR, "blocks").
-define(BLOCK_ENC_DIR, "blocks/enc").
-define(TX_DIR, "txs").
-define(DIRECTORY_SIZE_TIMER, 300000).
%% @doc Clear the cache of saved blocks.
clear() ->
	lists:map(fun file:delete/1, filelib:wildcard(?BLOCK_DIR ++ "/*.json")).

%% @doc Removes a saved block.
delete_block(Hash) ->
	file:delete(name_block(Hash)).

%% @doc Returns the number of blocks stored on disk.
blocks_on_disk() ->
	{ok, RawFiles} = file:list_dir(?BLOCK_DIR),
    Files = 
        lists:filter(
            fun(X) -> 
                case X of 
                    "enc" -> false;
                    _ -> true
                end    
            end,
            RawFiles
        ),	
	length(Files).

block_exists(Hash) ->
	case filelib:find_file(name_block(Hash)) of
		{ok, _} -> true;
		{error, _} -> false
	end.

%% @doc Write a block (with the hash.json as the filename) to disk.
%% When debug is set, does not consider disk space. This is currently
%% necessary because of test timings
-ifdef(DEBUG).
write_block(Bs) when is_list(Bs) -> lists:foreach(fun write_block/1, Bs);
write_block(B) ->
	BlockToWrite = ar_serialize:jsonify(ar_serialize:block_to_json_struct(B)),
	file:write_file(
		Name = lists:flatten(
			io_lib:format(
				"~s/~w_~s.json",
				[?BLOCK_DIR, B#block.height, ar_util:encode(B#block.indep_hash)]
			)
		),
		BlockToWrite
	),
	Name.
-else.
write_block(Bs) when is_list(Bs) -> lists:foreach(fun write_block/1, Bs);
write_block(B) ->
	BlockToWrite = ar_serialize:jsonify(ar_serialize:block_to_json_struct(B)),
	case enough_space(byte_size(BlockToWrite)) of
		true ->
			file:write_file(
				Name = lists:flatten(
					io_lib:format(
						"~s/~w_~s.json",
						[?BLOCK_DIR, B#block.height, ar_util:encode(B#block.indep_hash)]
					)
				),
				BlockToWrite
			),
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

%% @doc Write an encrypted  block (with the hash.json as the filename) to disk.
%% When debug is set, does not consider disk space. This is currently
%% necessary because of test timings
-ifdef(DEBUG).
write_encrypted_block(Hash, B) ->
	BlockToWrite = B,
	file:write_file(
		Name = lists:flatten(
			io_lib:format(
				"~s/~s_~s.json",
				[?BLOCK_ENC_DIR, "encrypted" , ar_util:encode(Hash)]
			)
		),
		BlockToWrite
	),
	Name.
-else.
write_encrypted_block(Hash, B) ->
	BlockToWrite = B,
	case enough_space(byte_size(BlockToWrite)) of
		true ->
			file:write_file(
				Name = lists:flatten(
					io_lib:format(
						"~s/~s_~s.json",
						[?BLOCK_ENC_DIR, "encrypted" , ar_util:encode(Hash)]
					)
				),
				BlockToWrite
			),
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
read_block(unavailable) -> unavailable;
read_block(B) when is_record(B, block) -> B;
read_block(Bs) when is_list(Bs) ->
	lists:map(fun read_block/1, Bs);
read_block(ID) ->
	case filelib:wildcard(name_block(ID)) of
		[] -> unavailable;
		[Filename] -> do_read_block(Filename);
		Filenames ->
			do_read_block(hd(
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
do_read_block(Filename) ->
	{ok, Binary} = file:read_file(Filename),
	ar_serialize:json_struct_to_block(Binary).

%% @doc Read an encrypted block from disk, given a hash.
read_encrypted_block(unavailable) -> unavailable;
read_encrypted_block(ID) ->
	case filelib:wildcard(name_enc_block(ID)) of
		[] -> unavailable;
		[Filename] -> do_read_encrypted_block(Filename);
		Filenames ->
			do_read_encrypted_block(hd(
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
do_read_encrypted_block(Filename) ->
	{ok, Binary} = file:read_file(Filename),
	Binary.


%% @doc Accurately recalculate the current cumulative size of the Arweave directory
update_directory_size() ->
	spawn(
		fun() ->
			ar_meta_db:put(used_space, calculate_used_space())
		end
	),
	timer:apply_after(?DIRECTORY_SIZE_TIMER, ar_storage, update_directory_size, []).

%% @doc Generate a wildcard search string for a block,
%% given a block, binary hash, or list.
name_block(Height) when is_integer(Height) ->
	?BLOCK_DIR ++ "/" ++ integer_to_list(Height) ++ "_*.json";
name_block(B) when is_record(B, block) ->
	?BLOCK_DIR
		++ "/"
		++ integer_to_list(B#block.height)
		++ "_"
		++ binary_to_list(ar_util:encode(B#block.indep_hash))
		++ ".json";
name_block(BinHash) when is_binary(BinHash) ->
	?BLOCK_DIR ++ "/*_" ++ binary_to_list(ar_util:encode(BinHash)) ++ ".json".

%% @doc Generate a wildcard search string for an encrypted block,
%% given a block, binary hash, or list.
name_enc_block(BinHash) when is_binary(BinHash) ->
	?BLOCK_ENC_DIR ++ "/*_" ++ binary_to_list(ar_util:encode(BinHash)) ++ ".json".

%% @doc Delete the tx with the given hash from disk.
delete_tx(Hash) ->
	file:delete(name_tx(Hash)).

%% @doc Returns the number of blocks stored on disk.
txs_on_disk() ->
	{ok, Files} = file:list_dir(?TX_DIR),
	length(Files).

%% @doc Returns whether the TX with the given hash is stored on disk.
tx_exists(Hash) ->
	case filelib:find_file(name_tx(Hash)) of
		{ok, _} -> true;
		{error, _} -> false
	end.

%% @doc Write a tx (with the txid.json as the filename) to disk.
%% When debug is set, does not consider disk space. This is currently
%% necessary because of test timings
-ifdef(DEBUG).
write_tx(Txs) when is_list(Txs) -> lists:foreach(fun write_tx/1, Txs);
write_tx(Tx) ->
	file:write_file(
		Name = lists:flatten(
			io_lib:format(
				"~s/~s.json",
				[?TX_DIR, ar_util:encode(Tx#tx.id)]
			)
		),
		ar_serialize:jsonify(ar_serialize:tx_to_json_struct(Tx))
	),
	Name.
-else.
write_tx(Txs) when is_list(Txs) -> lists:foreach(fun write_tx/1, Txs);
write_tx(Tx) ->
	TxToWrite = ar_serialize:jsonify(ar_serialize:tx_to_json_struct(Tx)),
	case enough_space(byte_size(TxToWrite)) of
		true ->
			file:write_file(
				Name = lists:flatten(
					io_lib:format(
						"~s/~s.json",
						[?TX_DIR, ar_util:encode(Tx#tx.id)]
					)
				),
				ar_serialize:jsonify(ar_serialize:tx_to_json_struct(Tx))
			),
			spawn(
				ar_meta_db,
				increase,
				[used_space, byte_size(TxToWrite)]
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
read_tx([]) ->
[];
read_tx(Tx) when is_record(Tx, tx) ->
Tx;
read_tx(Txs) when is_list(Txs) ->
	lists:map(fun read_tx/1, Txs);
read_tx(ID) ->
	case filelib:wildcard(name_tx(ID)) of
		[] -> unavailable;
		[Filename] -> do_read_tx(Filename);
		Filenames ->
			do_read_tx(hd(
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

do_read_tx(Filename) ->
	{ok, Binary} = file:read_file(Filename),
	ar_serialize:json_struct_to_tx(Binary).

%% @doc Returns the file name for a TX with the given hash
name_tx(Tx) when is_record(Tx, tx) ->
	?TX_DIR
		++ "/"
		++ binary_to_list(ar_util:encode(Tx#tx.id))
		++ ".json";
name_tx(BinHash) when is_binary(BinHash) ->
	?TX_DIR ++ "/" ++ binary_to_list(ar_util:encode(BinHash)) ++ ".json".

% @doc Check that there is enough space to write Bytes bytes of data
enough_space(Bytes) ->
	(ar_meta_db:get(disk_space)) >= (Bytes + ar_meta_db:get(used_space)).

%% @doc Calculate the amount of file space used by the Arweave client
calculate_used_space() ->
	{ok, CWD} = file:get_cwd(),
	(
		filelib:fold_files(
			CWD,
			"/*",
			true,
			fun(F, Acc) -> Acc + filelib:file_size(F) end,
			0
		)
	).

%% @doc Calculate the total amount of disk space available
calculate_disk_space() ->
	application:start(sasl),
	application:start(os_mon),
	{ok, CWD} = file:get_cwd(),
	[{_,Size,_}|_] = select_drive(disksup:get_disk_data(), CWD),
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

%% @doc Test block storage.
store_and_retrieve_block_test() ->
	[B0] = ar_weave:init(),
	write_block(B0),
	B0 = read_block(B0),
	file:delete(name_block(B0)).

store_and_retrieve_tx_test() ->
	Tx0 = ar_tx:new(<<"DATA1">>),
	write_tx(Tx0),
	Tx0 = read_tx(Tx0),
	Tx0 = read_tx(Tx0#tx.id),
	file:delete(name_tx(Tx0)).

% store_and_retrieve_encrypted_block_test() ->
%     B0 = ar_weave:init([]),
%     ar_storage:write_block(B0),
%     B1 = ar_weave:add(B0, []),
%     CipherText = ar_block:encrypt_block(hd(B0), hd(B1)),
%     write_encrypted_block((hd(B0))#block.hash, CipherText),
% 	read_encrypted_block((hd(B0))#block.hash),
% 	Block0 = hd(B0),
% 	Block0 = ar_block:decrypt_full_block(hd(B1), CipherText, Key).

% not_enough_space_test() ->
% 	Disk = ar_meta_db:get(disk_space),
% 	ar_meta_db:put(disk_space, 0),
% 	[B0] = ar_weave:init(),
% 	Tx0 = ar_tx:new(<<"DATA1">>),
% 	{error, enospc} = write_block(B0),
% 	{error, enospc} = write_tx(Tx0),
% 	ar_meta_db:put(disk_space, Disk).

	% Test that select_drive selects the correct drive on the unix architecture
select_drive_unix_test() ->
	CWD = "/home/usr/dev/arweave",
	Disks =
		[
			{"/dev",8126148,0},
			{"/run",1630656,1},
			{"/",300812640,8},
			{"/dev/shm",8153276,3},
			{"/boot/efi",98304,55}
		],
	[{"/",300812640,8}] = select_drive(Disks, CWD).

% Test that select_drive selects the correct drive on the unix architecture
select_drive_windows_test() ->
	CWD = "C:/dev/arweave",
	Disks = [{"C:\\",1000000000,10},{"D:\\",2000000000,20}],
	[{"C:\\",1000000000,10}] = select_drive(Disks, CWD).
