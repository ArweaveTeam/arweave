-module(ar_storage).
-export([write_block/1, read_block/1, clear/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%%% Reads and writes blocks from disk.

%% Where should the blocks be stored?
-define(BLOCK_DIR, "blocks").

%% @doc Clear the cache of saved blocks.
clear() ->
	lists:map(fun file:delete/1, filelib:wildcard(?BLOCK_DIR ++ "/*.json")).

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
	case filelib:wildcard(name(ID)) of
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
name(Height) when is_integer(Height) ->
	?BLOCK_DIR ++ "/" ++ integer_to_list(Height) ++ "_*.json";
name(B) when is_record(B, block) ->
	?BLOCK_DIR
		++ "/"
		++ integer_to_list(B#block.height)
		++ "_"
		++ ar_util:encode(B#block.indep_hash)
		++ ".json";
name(BinHash) when is_binary(BinHash) ->
	?BLOCK_DIR ++ "/*_" ++ ar_util:encode(BinHash) ++ ".json".

%% @doc Test block storage.
store_and_retrieve_block_test() ->
	[B0] = ar_weave:init(),
	write_block(B0),
	B0 = read_block(B0),
	file:delete(name(B0)).
