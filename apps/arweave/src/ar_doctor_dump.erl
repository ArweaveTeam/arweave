-module(ar_doctor_dump).

-export([main/1, help/0]).

-include_lib("kernel/include/file.hrl").
-include_lib("arweave/include/ar.hrl").

main(Args) ->
	dump(Args).

help() ->
	ar:console("data-doctor dump <data_dir> <output_dir>~n"),
	ar:console("  data_dir: Full path to your data_dir.~n"), 
	ar:console("  output_dir: Full path to a directory where the dumped data wil be written.~n"), 
	ar:console("~nExample:~n"), 
	ar:console("data-doctor bench 60 /mnt/arweave-data /mnt/output~n").

dump(Args) when length(Args) < 2 ->
	false;
dump(Args) ->
	[DataDir, OutputDir] = Args,

	DiskCachePath = filename:join(DataDir, ?DISK_CACHE_DIR),
	BlockPath = filename:join(DiskCachePath, ?DISK_CACHE_BLOCK_DIR),

	case file:list_dir(BlockPath) of
        {ok, Files} ->
            lists:foreach(fun(File) ->
                FullPath = filename:join(BlockPath, File),
				B = ar_storage:read_block_from_file(FullPath, binary),
				Height = B#block.height,
				io:format("Dumping block ~B~n", [Height]),
				Json = ar_serialize:block_to_json_struct(B),
				JsonString = ar_serialize:jsonify(Json),
				JsonFilename = io_lib:format("~B.json", [Height]),
				OutputFilePath = filename:join(OutputDir, JsonFilename),
                file:write_file(OutputFilePath, JsonString)
            end, Files);
        {error, Reason} ->
            io:format("Failed to list directory: ~p~n", [Reason])
    end,
	
	true.
