-module(arweave_storage_chunk_storage_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-include_lib("arweave/include/ar_sup.hrl").

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ets:new(
        chunk_storage_file_index,
        [set, public, named_table, {read_concurrency, true}]
    ),
    Workers =
        arweave_storage_chunk_storage:register_workers() ++
            entropy_workers(),
    {ok, {{one_for_one, 5, 10}, Workers}}.

entropy_workers() ->
    Configured = [
        {Module, arweave_storage_module:get_packing(Module)}
     || Module <- arweave_config:storage_modules()
    ],
    lists:filtermap(
        fun({Module, Packing}) ->
            case
                needs_entropy(arweave_storage_module:get_packing(Module)) orelse
                    needs_entropy(Packing)
            of
                false ->
                    false;
                true ->
                    StoreID = arweave_storage_module:id(Module),
                    Name = arweave_storage_entropy_storage:name(StoreID),
                    {true,
                        ?CHILD_WITH_ARGS(
                            arweave_storage_entropy_storage,
                            worker,
                            Name,
                            [Name, {StoreID, Packing}]
                        )}
            end
        end,
        Configured ++ arweave_config:repack_modules(full)
    ).

needs_entropy(unpacked_padded) -> true;
needs_entropy({replica_2_9, _}) -> true;
needs_entropy(_) -> false.
