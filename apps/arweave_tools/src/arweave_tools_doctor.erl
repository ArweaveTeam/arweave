%%% Dispatch offline doctor commands and report their exit status.
-module(arweave_tools_doctor).

-export([main/1, check_module_dir/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").

main([]) ->
    help(),
    1;
main(Args) ->
    logger:set_handler_config(default, level, error),
    Command = hd(Args),
    Success = case Command of
                  "merge" ->
                      arweave_tools_doctor_merge:main(tl(Args));
                  "bench" ->
                      arweave_tools_doctor_bench:main(tl(Args));
                  "dump" ->
                      arweave_tools_doctor_dump:main(tl(Args));
                  "inspect" ->
                      arweave_tools_doctor_inspect:main(tl(Args));
                  "snapshot" ->
                      arweave_tools_doctor_snapshot:main(tl(Args));
                  _ ->
                      false
              end,
    case Success of
        true ->
            0;
        error ->
            1;
        _ ->
            help(),
            1
    end.

%% @doc Verify the module's directory exists under the configured data_dir.
check_module_dir(ModuleOrID) ->
    #store_info{path = Path} = arweave_storage:store_info(ModuleOrID),
    case filelib:is_dir(Path) of
        true ->
            true;
        false ->
            %% Doctor arguments use current notation without legacy bootstrap.
            %% Refuse missing directories rather than working in empty folders.
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
    arweave_tools_doctor_merge:help(),
    ar:console("~n"),
    arweave_tools_doctor_bench:help(),
    ar:console("~n"),
    arweave_tools_doctor_dump:help(),
    ar:console("~n"),
    arweave_tools_doctor_inspect:help(),
    ar:console("~n"),
    arweave_tools_doctor_snapshot:help(),
    ar:console("~n").
