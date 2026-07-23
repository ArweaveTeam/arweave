%%% @doc Manual benchmarks for the account-tree implementations: ar_patricia_tree,
%%% ar_patricia_tree_legacy, and ar_patricia_tree_ets. Not run by CI - invoke by hand from
%%% a node console.
-module(ar_bench_account_tree).

-export([test_account_tree_performance/1, test_account_tree_performance/2,
         bench_account_tree_matrix/0, bench_account_tree_matrix/1, bench_account_tree_matrix/4]).

%% Accounts updated before the batched hash recomputation. Models a full block: 1000 source
%% accounts, 1000 target accounts, the reward address, and the banned address.
-define(BENCH_NUM_UPDATES, 2002).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Benchmark account-tree build and hashing. Opts (all optional):
%%   hash            => ar_deep_hash (default) | sha256 - leaf and node hashing
%%   tree_repr       => in_memory (default) | legacy | ets - ar_patricia_tree,
%%                      ar_patricia_tree_legacy, or ar_patricia_tree_ets
%%   persist_updates => true (default) | false - in_memory and legacy return the UpdateMap,
%%                      ets streams node updates to the given PID
%%   num_updates     => 2002 (default) - accounts inserted before the batched hash
%%                      recomputation. The default models a full block: 1000 source
%%                      accounts, 1000 target accounts, the reward address, and the
%%                      banned address.
%% Accounts are always generated in a mix of the old and new formats, roughly half each.
test_account_tree_performance(NumAccounts) ->
    test_account_tree_performance(NumAccounts, #{}).

test_account_tree_performance(NumAccounts, Opts) ->
    Hash = maps:get(hash, Opts, ar_deep_hash),
    TreeRepr = maps:get(tree_repr, Opts, in_memory),
    PersistUpdates = maps:get(persist_updates, Opts, true),
    NumUpdates = maps:get(num_updates, Opts, ?BENCH_NUM_UPDATES),
    %% gc_before => true (default) runs erlang:garbage_collect/0 before every timed step.
    %% Set it to false to measure on a warm heap (no pre-step collection).
    GCBefore = maps:get(gc_before, Opts, true),
    case run_account_tree_bench(NumAccounts, NumUpdates, Hash, TreeRepr, PersistUpdates,
                                GCBefore) of
        {error, Msg} ->
            io:format("~s~n", [Msg]);
        {ok, Metrics} ->
            print_account_tree_metrics(Metrics)
    end.

%% @doc Run the account-tree benchmark for every combination of Sizes and Configs
%% ({Hash, TreeRepr}), averaging Reps runs per cell, and write one CSV row per cell to File.
%% persist_updates is always true. Serialization is skipped for the ets representation.
%% Rows are flushed as they are written, so partial results survive an early stop.
bench_account_tree_matrix() ->
    bench_account_tree_matrix("account_tree_bench.csv").

bench_account_tree_matrix(File) ->
    Sizes = lists:seq(200000, 3000000, 200000),
    Configs = [{Hash, Repr} || Hash <- [ar_deep_hash, sha256],
                               Repr <- [in_memory, legacy, ets]],
    bench_account_tree_matrix(File, Sizes, Configs, 2).

bench_account_tree_matrix(File, Sizes, Configs, Reps) ->
    {ok, Fd} = file:open(File, [write]),
    ok = file:write(Fd, bench_csv_header()),
    ok = file:datasync(Fd),
    lists:foreach(
      fun(Size) ->
              lists:foreach(
                fun({Hash, TreeRepr}) ->
                        lists:foreach(
                          fun(GCBefore) ->
                                  Row = bench_csv_cell(Size, Hash, TreeRepr, GCBefore, Reps),
                                  ok = file:write(Fd, Row),
                                  ok = file:datasync(Fd),
                                  io:format("wrote ~Bk ~p/~p gc_before=~p~n",
                                            [Size div 1000, Hash, TreeRepr, GCBefore])
                          end,
                          [true, false]
                         )
                end,
                Configs
               )
      end,
      Sizes
     ),
    ok = file:close(Fd),
    io:format("done; wrote ~s~n", [File]).

%%%===================================================================
%%% Internal functions.
%%%===================================================================

%% @doc Run one account-tree benchmark in a spawned process, so the benchmark heap is
%% isolated and freed on exit. Return its metrics map, or {error, Msg} on invalid options
%% or a crash. When GCBefore is true, erlang:garbage_collect/0 runs before every timed step.
run_account_tree_bench(NumAccounts, NumUpdates, Hash, TreeRepr, PersistUpdates, GCBefore) ->
    case validate_account_tree_bench_opts(Hash, TreeRepr, PersistUpdates) of
        {error, _} = Error ->
            Error;
        ok ->
            PersistOpts = account_tree_persist_opts(TreeRepr, PersistUpdates),
            Parent = self(),
            {Pid, Ref} = spawn_opt(
                           fun() ->
                                   Metrics = measure_account_tree(NumAccounts, NumUpdates, Hash, TreeRepr,
                                                                  PersistUpdates, PersistOpts, GCBefore),
                                   Parent ! {account_tree_metrics, self(), Metrics}
                           end,
                           [monitor]
                          ),
            receive
                {account_tree_metrics, Pid, Metrics} ->
                    erlang:demonitor(Ref, [flush]),
                    {ok, Metrics};
                {'DOWN', Ref, process, Pid, Reason} ->
                    {error, io_lib:format("benchmark crashed: ~p", [Reason])}
            end
    end.

validate_account_tree_bench_opts(Hash, TreeRepr, PersistUpdates) ->
    Hashes = [ar_deep_hash, sha256],
    Reprs = [in_memory, legacy, ets],
    Bools = [true, false],
    case {lists:member(Hash, Hashes), lists:member(TreeRepr, Reprs),
          lists:member(PersistUpdates, Bools)} of
        {false, _, _} -> {error, io_lib:format("Supported hash: ~p", [Hashes])};
        {_, false, _} -> {error, io_lib:format("Supported tree_repr: ~p", [Reprs])};
        {_, _, false} -> {error, io_lib:format("Supported persist_updates: ~p", [Bools])};
        _ -> ok
    end.

repr_module(in_memory) -> ar_patricia_tree;
repr_module(legacy) -> ar_patricia_tree_legacy;
repr_module(ets) -> ar_patricia_tree_ets.

%% @doc Map the persist_updates flag to the PersistOpts passed to the representation's
%% compute_hash/3. For ets, the #{ sink => Pid } entry is added by measure_account_tree/7.
account_tree_persist_opts(in_memory, true) -> #{ return_update_map => true };
account_tree_persist_opts(in_memory, false) -> #{};
account_tree_persist_opts(legacy, true) -> #{ return_update_map => true };
account_tree_persist_opts(legacy, false) -> #{};
account_tree_persist_opts(ets, _PersistUpdates) -> #{}.

%% @doc Run the build, hash, and rehash measurements and return a metrics map. Times are in
%% seconds, footprints in MB. Serialization is skipped for the ets representation. When
%% GCBefore is true, erlang:garbage_collect/0 runs before each timed step. Meant to run
%% inside the process spawned by run_account_tree_bench/6.
measure_account_tree(NumAccounts, NumUpdates, Hash, TreeRepr, PersistUpdates, PersistOpts,
                     GCBefore) ->
    Mod = repr_module(TreeRepr),
    HashFun = bench_hash_fun(Hash),
    %% ets persistence streams node-update batches to a sink process. The bench drains and
    %% discards them, so compute_hash measures hashing and batch shipping but not storage I/O.
    %% This keeps the comparison with the in-memory implementations fair - they only build
    %% the UpdateMap on the heap.
    {PersistOpts2, Sink} =
        case {TreeRepr, PersistUpdates} of
            {ets, true} -> S = spawn_discard_sink(), {PersistOpts#{ sink => S }, S};
            _ -> {PersistOpts, undefined}
        end,
    %% Stream-build: each account is generated and inserted immediately, to save memory.
    maybe_gc(GCBefore),
    {Time1, T1} = timer:tc(fun() -> bench_stream_build(Mod, NumAccounts) end),
    FootBuild = account_tree_footprint(TreeRepr, T1, GCBefore),
    %% Serialization applies only to the in-memory implementations:
    %% ar_serialize:wallet_list_to_json_struct/3 traverses the ar_patricia_tree map and does
    %% not support the ets store.
    {SerS, SerBytes} =
        case TreeRepr of
            ets ->
                {na, na};
            _ ->
                maybe_gc(GCBefore),
                {Time2, Binary} = timer:tc(fun() ->
                                                   ar_serialize:jsonify(
                                                     ar_serialize:wallet_list_to_json_struct(unclaimed, false, T1)) end),
                {Time2 / 1000000, byte_size(Binary)}
        end,
    maybe_gc(GCBefore),
    {Time3, {_, T2, _}} = timer:tc(fun() -> Mod:compute_hash(T1, HashFun, PersistOpts2) end),
    FootHash = account_tree_footprint(TreeRepr, T2, GCBefore),
    maybe_gc(GCBefore),
    {Time4, T3} = timer:tc(fun() -> bench_stream_insert(Mod, NumUpdates, T2) end),
    maybe_gc(GCBefore),
    {Time5, _} = timer:tc(fun() -> Mod:compute_hash(T3, HashFun, PersistOpts2) end),
    {A, B, LastTX} = random_wallet(),
    maybe_gc(GCBefore),
    {Time6, T4} = timer:tc(fun() ->
                                   Mod:insert(A, bench_wallet_value(mixed, B, LastTX), T2) end),
    maybe_gc(GCBefore),
    {Time7, _} = timer:tc(fun() -> Mod:compute_hash(T4, HashFun, PersistOpts2) end),
    stop_sink(Sink),
    case TreeRepr of
        ets -> ar_patricia_tree_ets:delete_table(T4);
        _ -> ok
    end,
    #{
      num_accounts => NumAccounts,
      num_updates => NumUpdates,
      hash => Hash,
      tree_repr => TreeRepr,
      persist_updates => PersistUpdates,
      gc_before => GCBefore,
      buildup_s => Time1 / 1000000,
      footprint_build_mb => FootBuild,
      serialization_s => SerS,
      serialization_bytes => SerBytes,
      scratch_hash_s => Time3 / 1000000,
      footprint_hash_mb => FootHash,
      inserts_batch_s => Time4 / 1000000,
      recompute_batch_s => Time5 / 1000000,
      insert_1_s => Time6 / 1000000,
      recompute_1_s => Time7 / 1000000
     }.

%% @doc Print one metrics map in the human-readable form.
print_account_tree_metrics(M) ->
    NumUpdates = maps:get(num_updates, M),
    io:format("# ~B accounts, ~B updates, hash: ~p, tree_repr: ~p, persist_updates: ~p, "
              "gc_before: ~p~n",
              [maps:get(num_accounts, M), NumUpdates, maps:get(hash, M), maps:get(tree_repr, M),
               maps:get(persist_updates, M), maps:get(gc_before, M)]),
    io:format("============~n"),
    io:format("tree buildup                    | ~f seconds~n", [maps:get(buildup_s, M)]),
    io:format("footprint after build           | ~B MB~n", [maps:get(footprint_build_mb, M)]),
    case maps:get(serialization_s, M) of
        na ->
            ok;
        SerS ->
            io:format("serialization                   | ~f seconds~n", [SerS]),
            io:format("                                | ~B bytes~n",
                      [maps:get(serialization_bytes, M)])
    end,
    io:format("root hash from scratch          | ~f seconds~n", [maps:get(scratch_hash_s, M)]),
    io:format("footprint after hash            | ~B MB~n", [maps:get(footprint_hash_mb, M)]),
    io:format("~B inserts                       | ~f seconds~n",
              [NumUpdates, maps:get(inserts_batch_s, M)]),
    io:format("recompute hash after inserts    | ~f seconds~n", [maps:get(recompute_batch_s, M)]),
    io:format("1 insert                        | ~f seconds~n", [maps:get(insert_1_s, M)]),
    io:format("recompute hash after 1 insert   | ~f seconds~n", [maps:get(recompute_1_s, M)]),
    ok.

%% @doc A test sink for the ets persistence stream: drains node-update batches and
%% discards them.
spawn_discard_sink() ->
    spawn_link(fun discard_sink_loop/0).

discard_sink_loop() ->
    receive
        {account_tree_node_batch, _Batch} ->
            discard_sink_loop();
        stop ->
            ok
    end.

stop_sink(undefined) ->
    ok;
stop_sink(Sink) ->
    unlink(Sink),
    exit(Sink, kill),
    ok.

maybe_gc(true) -> erlang:garbage_collect();
maybe_gc(false) -> ok.

%% @doc Measure the tree footprint without traversing it. The ets tree lives off the process
%% heap, so read ets:info/2. The in-memory tree (and its UpdateMap) live on the process
%% heap, so garbage-collect first (when GCBefore) and read the process memory. With
%% GCBefore=false the heap footprint includes uncollected garbage and is only an upper
%% bound, but skipping the collection is what keeps the next recompute warm.
account_tree_footprint(ets, Tree, _GCBefore) ->
    ets_table_mb(Tree);
account_tree_footprint(_Repr, _Tree, GCBefore) ->
    maybe_gc(GCBefore),
    {memory, Bytes} = erlang:process_info(self(), memory),
    Bytes div (1024 * 1024).

ets_table_mb(Tree) ->
    (ets:info(Tree, memory) * erlang:system_info(wordsize)) div (1024 * 1024).

random_wallet() ->
    {
     crypto:strong_rand_bytes(32),
     rand:uniform(1000000000000000000),
     crypto:strong_rand_bytes(32)
    }.

%% @doc Build the hash function for the benchmarks. Algo is used for both leaves and nodes:
%% HashFun(leaf, {Addr, Value}) hashes a leaf, HashFun(node, Hashes) combines sibling
%% hashes. The default ar_deep_hash algorithm is the production consensus hash, so delegate
%% to ar_block:wallet_list_hash_fun/0 - the two must never drift. The sha256 and sha384
%% funs exist only to benchmark alternative algorithms.
bench_hash_fun(ar_deep_hash) ->
    ar_block:wallet_list_hash_fun();
bench_hash_fun(Algo) ->
    fun (leaf, {Addr, {Balance, LastTX}}) ->
            Denomination = 0,
            MiningPermissionBin = <<1>>,
            Preimage = << (ar_serialize:encode_bin(Addr, 8))/binary,
                          (ar_serialize:encode_int(Balance, 8))/binary,
                          (ar_serialize:encode_bin(LastTX, 8))/binary,
                          (ar_serialize:encode_int(Denomination, 8))/binary,
                          MiningPermissionBin/binary >>,
            case Algo of
                no_ar_deep_hash_sha384 ->
                    crypto:hash(sha384, Preimage);
                sha256 ->
                    crypto:hash(sha256, Preimage)
            end;
        (leaf, {Addr, {Balance, LastTX, Denomination, MiningPermission}}) ->
            MiningPermissionBin =
                case MiningPermission of
                    true ->
                        <<1>>;
                    false ->
                        <<0>>
                end,
            Preimage = << (ar_serialize:encode_bin(Addr, 8))/binary,
                          (ar_serialize:encode_int(Balance, 8))/binary,
                          (ar_serialize:encode_bin(LastTX, 8))/binary,
                          (ar_serialize:encode_int(Denomination, 8))/binary,
                          MiningPermissionBin/binary >>,
            case Algo of
                sha256 ->
                    crypto:hash(sha256, Preimage);
                _ ->
                    crypto:hash(sha384, Preimage)
            end;
        (node, Hashes) ->
            crypto:hash(sha256, iolist_to_binary(Hashes))
    end.

bench_wallet_value(Denominations, Balance, LastTX) ->
    case Denominations of
        old ->
            {Balance, LastTX};
        new ->
            {Balance, LastTX, 1 + rand:uniform(10), true};
        mixed ->
            case rand:uniform(2) == 1 of
                true -> {Balance, LastTX};
                false -> {Balance, LastTX, 1 + rand:uniform(10), true}
            end
    end.

%% @doc Run Reps benchmarks for one cell, average the metrics across the runs that succeeded,
%% and format a CSV row. A cell whose every run crashed yields a row with empty metric columns.
bench_csv_cell(Size, Hash, TreeRepr, GCBefore, Reps) ->
    Runs = [run_account_tree_bench(Size, ?BENCH_NUM_UPDATES, Hash, TreeRepr, true, GCBefore)
            || _ <- lists:seq(1, Reps)],
    Metrics = average_metrics([M || {ok, M} <- Runs]),
    bench_csv_row(Size, Hash, TreeRepr, GCBefore, Metrics).

%% @doc Average each numeric metric over the given maps, ignoring na entries (serialization
%% on the ets representation). A metric with no numeric samples stays na.
average_metrics(Maps) ->
    Keys = [buildup_s, footprint_build_mb, serialization_s, serialization_bytes,
            scratch_hash_s, footprint_hash_mb, inserts_batch_s, recompute_batch_s,
            insert_1_s, recompute_1_s],
    lists:foldl(
      fun(Key, Acc) ->
              Present = [V || M <- Maps, V <- [maps:get(Key, M, na)], V =/= na],
              case Present of
                  [] -> Acc#{ Key => na };
                  _ -> Acc#{ Key => lists:sum(Present) / length(Present) }
              end
      end,
      #{},
      Keys
     ).

bench_csv_header() ->
    "accounts,num_updates,hash,tree_repr,persist_updates,gc_before,buildup_s,footprint_build_mb,"
        "serialization_s,serialization_bytes,scratch_hash_s,footprint_hash_mb,"
        "inserts_batch_s,recompute_batch_s,insert_1_s,recompute_1_s\n".

bench_csv_row(Size, Hash, TreeRepr, GCBefore, M) ->
    Cols = [integer_to_list(Size), integer_to_list(?BENCH_NUM_UPDATES), atom_to_list(Hash),
            atom_to_list(TreeRepr), "true", atom_to_list(GCBefore),
            fmt_num(maps:get(buildup_s, M, na)),
            fmt_num(maps:get(footprint_build_mb, M, na)),
            fmt_num(maps:get(serialization_s, M, na)),
            fmt_num(maps:get(serialization_bytes, M, na)),
            fmt_num(maps:get(scratch_hash_s, M, na)),
            fmt_num(maps:get(footprint_hash_mb, M, na)),
            fmt_num(maps:get(inserts_batch_s, M, na)),
            fmt_num(maps:get(recompute_batch_s, M, na)),
            fmt_num(maps:get(insert_1_s, M, na)),
            fmt_num(maps:get(recompute_1_s, M, na))],
    [lists:join(",", Cols), "\n"].

fmt_num(na) -> "";
fmt_num(N) when is_integer(N) -> integer_to_list(N);
fmt_num(N) when is_float(N) -> io_lib:format("~.6f", [N]).

bench_stream_build(Mod, Total) ->
    do_bench_stream_build(Mod, Total, Mod:new()).

do_bench_stream_build(_Mod, 0, Tree) ->
    Tree;
do_bench_stream_build(Mod, N, Tree) ->
    {A, B, LastTX} = random_wallet(),
    do_bench_stream_build(Mod, N - 1, Mod:insert(A, bench_wallet_value(mixed, B, LastTX), Tree)).

bench_stream_insert(_Mod, 0, Tree) ->
    Tree;
bench_stream_insert(Mod, N, Tree) ->
    {A, B, LastTX} = random_wallet(),
    bench_stream_insert(Mod, N - 1, Mod:insert(A, bench_wallet_value(mixed, B, LastTX), Tree)).
