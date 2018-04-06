-module(ar_storage).
-export([write_block/1, read_block/1, clear/0]).
-export([delete_block/1, blocks_on_disk/0, block_exists/1]).
-export([write_tx/1, read_tx/1]).
-export([delete_tx/1, txs_on_disk/0, tx_exists/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%%% Reads and writes blocks from disk.

%% Where should the blocks be stored?
-define(BLOCK_DIR, "blocks").
-define(TX_DIR, "txs").

%% @doc Clear the cache of saved blocks.
clear() ->
	lists:map(fun file:delete/1, filelib:wildcard(?BLOCK_DIR ++ "/*.json")).

%% @doc Removes a saved block.
delete_block(Hash) ->
	file:delete(name_block(Hash)).

%% @doc Returns the number of blocks stored on disk.
blocks_on_disk() ->
	{ok, Files} = file:list_dir(?BLOCK_DIR),
	length(Files).

block_exists(Hash) ->
	case filelib:find_file(name_block(Hash)) of
		{ok, _} -> true;
		{error, _} -> false
	end.

%% @doc Write a block (with the hash.json as the filename) to disk.
write_block(Bs) when is_list(Bs) -> lists:foreach(fun write_block/1, Bs);
write_block(B) ->
	file:write_file(
		Name = lists:flatten(
			io_lib:format(
				"~s/~w_~s.json",
				[?BLOCK_DIR, B#block.height, ar_util:encode(B#block.indep_hash)]
			)
		),
		ar_serialize:jsonify(ar_serialize:block_to_json_struct(B))
	),
	Name.

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
			% TODO: There should never be multiple versions of a block on disk.
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
	ar_serialize:json_struct_to_block(binary_to_list(Binary)).

%% @doc Generate a wildcard search string for a block,
%% given a block, binary hash, or list.
name_block(Height) when is_integer(Height) ->
	?BLOCK_DIR ++ "/" ++ integer_to_list(Height) ++ "_*.json";
name_block(B) when is_record(B, block) ->
	?BLOCK_DIR
		++ "/"
		++ integer_to_list(B#block.height)
		++ "_"
		++ ar_util:encode(B#block.indep_hash)
		++ ".json";
name_block(BinHash) when is_binary(BinHash) ->
	?BLOCK_DIR ++ "/*_" ++ ar_util:encode(BinHash) ++ ".json".

delete_tx(Hash) ->
	file:delete(name_tx(Hash)).

%% @doc Returns the number of blocks stored on disk.
txs_on_disk() ->
	{ok, Files} = file:list_dir(?TX_DIR),
	length(Files).

tx_exists(Hash) ->
	case filelib:find_file(name_tx(Hash)) of
		{ok, _} -> true;
		{error, _} -> false
	end.

%% @doc Write a block (with the hash.json as the filename) to disk.
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
			% TODO: There should never be multiple versions of a tx on disk.
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
	ar_serialize:json_struct_to_tx(binary_to_list(Binary)).

name_tx(Tx) when is_record(Tx, tx) ->
	?TX_DIR
		++ "/"
		++ ar_util:encode(Tx#tx.id)
		++ ".json";
name_tx(BinHash) when is_binary(BinHash) ->
	?TX_DIR ++ "/" ++ ar_util:encode(BinHash) ++ ".json".


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