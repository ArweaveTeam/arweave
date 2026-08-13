-module(ar_data_doctor).

-export([main/0, main/1, check_module_dir/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave/include/ar_consensus.hrl").

main() ->
    Args = init:get_plain_arguments(),
    main(Args).

main([]) ->
    help(),
    init:stop(1);
main(Args) ->
    logger:set_handler_config(default, level, error),
    Command = hd(Args),
    Success = case Command of
                  "merge" ->
                      ar_doctor_merge:main(tl(Args));
                  "bench" ->
                      ar_doctor_bench:main(tl(Args));
                  "dump" ->
                      ar_doctor_dump:main(tl(Args));
                  "inspect" ->
                      ar_doctor_inspect:main(tl(Args));
                  _ ->
                      false
              end,
    case Success of
        true ->
            init:stop(0);
        _ ->
            help(),
            init:stop(1)
    end.

%% @doc Verify the given storage module's on-disk directory exists
%% under DataDir. The doctor tools accept only current-notation
%% storage module arguments and resolve directories in the current
%% naming (they never run the config bootstrap that detects a legacy
%% launch), so a missing directory usually means a mistyped module or
%% a node whose directories still use the legacy naming. Erroring out
%% keeps the tools from silently working in empty folders.
check_module_dir(DataDir, StoreID) ->
    Path = ar_chunk_storage:storage_module_path(DataDir, StoreID),
    case filelib:is_dir(Path) of
        true ->
            true;
        false ->
            ar:console("Storage module directory not found: ~s~n", [Path]),
            ar:console(
                "The data doctor resolves directories in the current "
                "naming. If this node was launched with a legacy "
                "configuration, its storage module directories may use "
                "the legacy bucket naming - see the Legacy Configuration "
                "guide at docs.arweave.org.~n"),
            false
    end.

help() ->
    ar:console("~n"),
    ar_doctor_merge:help(),
    ar:console("~n"),
    ar_doctor_bench:help(),
    ar:console("~n"),
    ar_doctor_dump:help(),
    ar:console("~n"),
    ar_doctor_inspect:help(),
    ar:console("~n").
