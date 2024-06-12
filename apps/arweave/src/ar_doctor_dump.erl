-module(ar_doctor_dump).

-export([main/1, help/0]).

-include_lib("kernel/include/file.hrl").
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

main(Args) ->
	dump(Args).

help() ->
	ar:console("data-doctor dump <min_height> <data_dir> <output_dir>~n"),
	ar:console("  min_height: The minimum height of the blocks to dump.~n"),
	ar:console("  data_dir: Full path to your data_dir.~n"), 
	ar:console("  output_dir: Full path to a directory where the dumped data will be written.~n"), 
	ar:console("~nExample:~n"), 
	ar:console("data-doctor dump /mnt/arweave-data /mnt/output~n").

dump(Args) when length(Args) < 3 ->
	false;
dump(Args) ->
	[MinHeight, DataDir, OutputDir] = Args,

	ok = filelib:ensure_dir(filename:join([OutputDir, "blocks", "dummy"])),
	ok = filelib:ensure_dir(filename:join([OutputDir, "txs", "dummy"])),

	Config = #config{data_dir = DataDir},
	application:set_env(arweave, config, Config),
	ar_kv_sup:start_link(),
	ar_storage_sup:start_link(),

	dump_blocks(<<>>, list_to_integer(MinHeight), OutputDir),
	true.

dump_blocks(Cursor, MinHeight, OutputDir) ->
	case ar_kv:get_next(block_db, Cursor) of
        {ok, BH, Bin} ->
            % Process the value here if needed
			H = ar_util:encode(BH),
			{ok, B} = ar_serialize:binary_to_block(Bin),

			case B#block.height >= MinHeight of
				true ->
					io:format("Block: ~p / ~p~n", [B#block.height, H]),
					dump_txs(B#block.txs, OutputDir),
					Json = ar_serialize:block_to_json_struct(B),
					JsonString = ar_serialize:jsonify(Json),
					JsonFilename = io_lib:format("~B.json", [B#block.height]),
					OutputFilePath = filename:join([OutputDir, "blocks", JsonFilename]),
					file:write_file(OutputFilePath, JsonString);
				false ->
					ok
			end,


			<< Start:384 >> = BH,
			NextCursor = << (Start + 1):384 >>,
            dump_blocks(NextCursor, MinHeight, OutputDir); % Recursive call with the new cursor
        none ->
            io:format("No more entries.~n")
    end.

dump_txs([], OutputDir) ->
	ok;
dump_txs([TXID | TXIDs], OutputDir) ->
	case ar_kv:get(tx_db, TXID) of
		{ok, Bin} ->
			{ok, TX} = ar_serialize:binary_to_tx(Bin),
			Json = ar_serialize:tx_to_json_struct(TX),
			JsonString = ar_serialize:jsonify(Json),
			JsonFilename = io_lib:format("~s.json", [ar_util:encode(TXID)]),
			OutputFilePath = filename:join([OutputDir, "txs", JsonFilename]),
			file:write_file(OutputFilePath, JsonString);
		_ ->
			ok
	end,
	dump_txs(TXIDs, OutputDir).
