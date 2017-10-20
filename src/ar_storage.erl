-module(ar_storage).
-export([write_block/1, read_block/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Reads and writes blocks from disk.

%% Where should the blocks be stored?
-define(BLOCK_DIR, "blocks").

%% @doc Write a block (with the hash.json as the filename) to disk.
write_block(B) ->
	file:write_file(
		Name = name(B),
		ar_serialize:jsonify(ar_serialize:block_to_json_struct(B))
	),
	Name.

%% @doc Read a block from disk, given a hash.
read_block(ID) ->
	{ok, Binary} = file:read_file(name(ID)),
	ar_serialize:json_struct_to_block(binary_to_list(Binary)).

%% @doc Generate a name for a block, given a block, binary hash, or list.
name(B) when is_record(B, block) ->
	name(B#block.hash);
name(BinHash) when is_binary(BinHash) ->
	name(ar_util:hexify(BinHash));
name(Hash) ->
	?BLOCK_DIR ++ "/" ++ Hash ++ ".json".

%% @doc Test block storage.
store_and_retrieve_block_test() ->
	[B0] = ar_weave:init(),
	write_block(B0),
	B0 = read_block(B0),
	file:delete(name(B0)).
