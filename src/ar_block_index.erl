-module(ar_block_index).
-export([start/0, stop/1]).
-export([add/2, remove/1, clear/0]).
-export([get_block_filename/1, count/0]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").

%%% Creates and manges an index of relations between block JSOn files on disk
%%% and their respective hashes and block numbers.

%% @doc Generate the ETS table, populated with all the blocks found on disk.
start() ->
	% Spawn a new process to own the table.
	Parent = self(),
	PID = 
		spawn(
			fun() ->
				ar:report([starting_block_index]),
				case ets:info(?MODULE) of
					undefined ->
						ets:new(?MODULE, [bag, public, named_table]);
					_ -> do_nothing
				end,
				Parent ! table_ready,
				receive stop -> ok end
			end
		),
	receive table_ready -> ok end,
	lists:foreach(
		fun({Number, Hash, FN}) ->
			add(Number, Hash, FN)
		end,
		blocks_on_disk()
	),
	PID.

%% @doc Hal the ets table.
stop(ETSOwner) ->
	ETSOwner ! stop.

%% @doc Add an index for a given block to the database.
add(B, Filename) -> add(B#block.height, B#block.indep_hash, Filename).
add(Height, Hash, Filename) ->
	ets:insert(?MODULE, [{Height, Filename}, {Hash, Filename}]).

%% @doc Remove a reference to a block when given its hash or height.
remove(ID) ->
	FN = get_block_filename(ID),
	case ets:select_delete(?MODULE, [{{'_',FN},[],[true]}]) of
		0 -> not_found;
		_ -> ok
	end.

%% @doc Clear the entire index table.
clear() ->
	ets:delete_all_objects(?MODULE).

%% @doc Turn an identifier (block hash or height) into a corresponding file
%% name.
get_block_filename(ID) ->
	case ets:lookup(?MODULE, ID) of
		[] -> unavailable;
		[{_, Filename}] -> Filename;
		Filenames ->
			[{_, FN}|_] =
				lists:sort(
						fun({_, Filename}, {_, Filename2}) ->
							{ok, Info} = file:read_file_info(Filename, [{time, posix}]),
							{ok, Info2} = file:read_file_info(Filename2, [{time, posix}]),
							Info#file_info.mtime >= Info2#file_info.mtime
						end,
						Filenames
				),
			FN
	end.

%% @doc Return a list of block keys (hash and height) and their associated
%% file names.
blocks_on_disk() ->
	filelib:fold_files(
		"blocks",
		"\.json$",
		false,
		fun(FN, Acc) ->
			{ok, [Height, RawHash], ""} = io_lib:fread("blocks/~u_~64c.json", FN),
			[
				{Height, ar_util:decode(RawHash), FN}
			|
				Acc
			]
		end,
		[]
	).

%% @doc Return the number of blocks found on disk.
count() ->
	ets:info(?MODULE, size) div 2.

%%% TESTS

%% @doc Ensure that blocks referenced in the index can be found.
basic_index_test() ->
	start(),
	clear(),
	?assertEqual(0, count()),
	ID1 = crypto:strong_rand_bytes(32),
	ID2 = crypto:strong_rand_bytes(32),
	add(1, ID1, "test_file1"),
	?assertEqual(1, count()),
	add(2, ID2, "test_file2"),
	?assertEqual(2, count()),
	?assertEqual("test_file1", get_block_filename(ID1)),
	?assertEqual("test_file1", get_block_filename(1)),
	?assertEqual("test_file2", get_block_filename(ID2)),
	?assertEqual("test_file2", get_block_filename(2)),
	remove(1),
	?assertEqual(1, count()),
	remove(ID2),
	?assertEqual(0, count()).

%% @doc Test that new indexes are generated correctly on start.
index_generation_test() ->
	ar_storage:clear(),
	PID = start(),
	?assertEqual(0, count()),
    B0s = ar_weave:init([]),
	ar_storage:write_block(hd(B0s)),
    B1s = ar_weave:add(B0s, []),
	ar_storage:write_block(hd(B1s)),
	PID ! stop,
	receive after 100 -> ok end,
	start(),
	?assertEqual(2, count()).
