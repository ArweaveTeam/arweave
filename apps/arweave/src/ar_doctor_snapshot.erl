-module(ar_doctor_snapshot).

-export([main/1, help/0]).

%%% data-doctor snapshot: export a join.start_from_state snapshot directory
%%% from the data directory of a stopped node.

main([DataDir, OutputDir]) ->
    snapshot(DataDir, OutputDir, latest);
main([DataDir, OutputDir, "height", HeightArg]) ->
    case string:to_integer(HeightArg) of
        {Height, ""} when Height >= 0 ->
            snapshot(DataDir, OutputDir, Height);
        _ ->
            false
    end;
main(_) ->
    false.

help() ->
    ar:console("data-doctor snapshot <data_dir> <output_dir> [height <N>]~n"),
    ar:console("  Export a snapshot a node can start from via "
            "join.start_from_state.~n"),
    ar:console("  data_dir: Full path to the data_dir of a node that is not "
            "running.~n"),
    ar:console("  output_dir: Full path to the snapshot directory to create "
            "(must not exist).~n"),
    ar:console("  height: Height of the snapshot tip (default: the highest "
            "block in the local block index).~n"),
    ar:console("~nExample:~n"),
    ar:console("data-doctor snapshot /mnt/arweave-data /mnt/snapshot_1999999 "
            "height 1999999~n").

snapshot(DataDir, OutputDir, Height) ->
    ar_kv_sup:start_link(),
    case ar_snapshot:export(DataDir, OutputDir, Height) of
        {ok, #{ height := TipHeight, hash := H, weave_size := WeaveSize }} ->
            ar:console("Snapshot written to ~s~n", [OutputDir]),
            ar:console("Tip height: ~B~n", [TipHeight]),
            ar:console("Tip hash: ~s~n", [arweave_util:encode(H)]),
            ar:console("Weave size: ~B~n", [WeaveSize]),
            warn_fork_height(TipHeight),
            true;
        {error, Reason} ->
            ar:console("Snapshot failed: ~s~n", [format_error(Reason)]),
            error
    end.

%% @doc When forking off a snapshot, tip height + 1 must be a multiple of
%% the retarget interval (10): the first block of the new network is then a
%% retarget, which lets the difficulty adjust when the new network has less
%% hashrate than the network the snapshot was taken from.
warn_fork_height(TipHeight) ->
    case (TipHeight + 1) rem 10 of
        0 ->
            ok;
        _ ->
            ar:console("Warning: the fork height ~B (tip height + 1) is not a "
                    "multiple of 10.~n", [TipHeight + 1])
    end.

format_error({data_dir_not_found, DataDir}) ->
    io_lib:format("~s has no rocksdb directory; is it a node data_dir?",
            [DataDir]);
format_error(block_index_not_found) ->
    "no block index found in the data_dir";
format_error({height_not_in_block_index, Height, TipHeight}) ->
    io_lib:format("height ~B is above the local block index tip ~B",
            [Height, TipHeight]);
format_error({block_headers_not_found_above_height, Height}) ->
    io_lib:format("the block headers of the tip are missing from the "
            "data_dir; the highest readable height is ~B", [Height]);
format_error({account_tree_not_found, Height, RootHash}) ->
    io_lib:format("the account tree ~s of the block at height ~B is missing "
            "from the data_dir; the requested tip cannot be served",
            [RootHash, Height]);
format_error({snapshot_dir_exists, Dir}) ->
    io_lib:format("the output directory ~s already exists", [Dir]);
format_error(Reason) ->
    io_lib:format("~p", [Reason]).
