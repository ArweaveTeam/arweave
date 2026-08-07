%%% @doc Tests asserting the ETS-based ar_account_tree and the map-based ar_wallets_legacy
%%% reply identically to the same sequences of requests. Every scenario runs against a
%%% matrix of account sets (varying counts, prefix-sharing keys, and denominated four-tuple
%%% values) so the same logic is exercised over a wide range of tree shapes.
-module(ar_account_tree_impl_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% Test generator
%%%===================================================================

ar_account_tree_impl_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [instantiator(Name, Scenario, Set)
      || {Name, Scenario} <- scenarios(), Set <- account_sets()]}.

scenarios() ->
    [
     {tip_and_fork_reads, fun scenario_tip_and_fork_reads/2},
     {reorg_and_uncle, fun scenario_reorg_and_uncle/2},
     {removals, fun scenario_removals/2},
     {denomination, fun scenario_denomination/2},
     {chunk_pagination, fun scenario_chunk_pagination/2},
     {unknown_and_pruned, fun scenario_unknown_and_pruned/2},
     {prune_drops_uncle, fun scenario_prune_drops_uncle/2},
     {noop_diff, fun scenario_noop_diff/2},
     {remove_then_readd, fun scenario_remove_then_readd/2},
     {step_equivalence, fun scenario_step_equivalence/2},
     {chunked_build, fun scenario_chunked_build/2},
     {excursions_leave_tip_clean, fun scenario_excursions_leave_tip_clean/2},
     {deep_excursions_leave_tip_clean, fun scenario_deep_excursions_leave_tip_clean/2}
    ].

%% @doc The account sets the matrix iterates over. Each is {Name, Accounts, Denomination},
%% where Accounts is a list of {Address, WalletValue} with distinct keys, and every
%% account's base denomination is at most Denomination.
%% The empty-address account exists on mainnet, so most sets include a {<<>>, _} account.
%% It is placed first, so the position-based diffs below update and remove it as well.
account_sets() ->
    [
     {tiny, [empty_account(short) | simple_accounts(1, 3)], 1},
     {many, [empty_account(short) | simple_accounts(1, 60)], 1},
     {large, simple_accounts(1, 200), 1},
     {shared_prefix, [empty_account(short) | prefix_accounts()], 1},
     {four_tuple, [empty_account(full) | four_tuple_accounts()], 3}
    ].

empty_account(short) ->
    {<<>>, {77, tx(0)}};
empty_account(full) ->
    {<<>>, {77, tx(0), 1, true}}.

instantiator(Name, Scenario, {SetName, _, _} = Set) ->
    fun(Ctx) ->
            Label = atom_to_list(Name) ++ " / " ++ atom_to_list(SetName),
            {timeout, 60, {Label, Scenario(Ctx, Set)}}
    end.

%%%===================================================================
%%% Scenarios
%%%===================================================================

%% @doc Build a tip, fork it two ways, and assert the two implementations agree on tip and
%% non-tip reads.
scenario_tip_and_fork_reads({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            Addrs = addrs(Accounts),
            {ok, R1} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R1, 10, 20}),
            %% Tip reads.
            cmp(New, Legacy, {get, [unknown_addr() | Addrs]}),
            cmp(New, Legacy, {get_balance, hd(Addrs)}),
            cmp(New, Legacy, {get_balance, unknown_addr()}),
            cmp(New, Legacy, {get_last_tx, hd(Addrs)}),
            cmp(New, Legacy, {get_last_tx, unknown_addr()}),
            cmp(New, Legacy, get_size),
            %% Two forks off R1.
            Fork2 = maps:merge(update_range(Accounts, 0, 2), add_fresh(2, Denom)),
            Fork3 = update_range(Accounts, 2, 3),
            {ok, R2} = cmp(New, Legacy, {add_wallets, R1, Fork2, 11, Denom}),
            {ok, R3} = cmp(New, Legacy, {add_wallets, R1, Fork3, 11, Denom}),
            Query = Addrs ++ fresh_addrs(2),
            reads_at_all(New, Legacy, [R1, R2, R3, <<>>], Query)
    end.

%% @doc Switch the tip across forks and assert reads of the tip, the previous tip (now an
%% uncle), and the fork base agree after each move.
scenario_reorg_and_uncle({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            Addrs = addrs(Accounts),
            {ok, R1} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R1, 10, 20}),
            {ok, R2} = cmp(New, Legacy, {add_wallets, R1, update_range(Accounts, 0, 1), 11, Denom}),
            {ok, R3} = cmp(New, Legacy, {add_wallets, R1,
                    maps:merge(update_range(Accounts, 1, 2), add_fresh(1, Denom)), 11, Denom}),
            Query = Addrs ++ fresh_addrs(1),
            ok = cmp(New, Legacy, {set_current, R2, 11, 20}),
            %% R2 bumped the LastTX (and balance) of the updated accounts. get_last_tx reflects it.
            [cmp(New, Legacy, {get_last_tx, A}) || A <- Addrs],
            reads_at_all(New, Legacy, [R1, R2, R3, <<>>], Query),
            %% Reorg to the sibling fork.
            ok = cmp(New, Legacy, {set_current, R3, 11, 20}),
            reads_at_all(New, Legacy, [R1, R2, R3, <<>>], Query),
            %% Extend R3 and reorg forward again.
            {ok, R4} = cmp(New, Legacy, {add_wallets, R3, update_range(Accounts, 0, 1), 12, Denom}),
            ok = cmp(New, Legacy, {set_current, R4, 12, 20}),
            reads_at_all(New, Legacy, [R1, R2, R3, R4], Query)
    end.

%% @doc A diff that removes existing accounts must reconstruct identically in both
%% implementations.
scenario_removals({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            Addrs = addrs(Accounts),
            {ok, R1} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R1, 10, 20}),
            {ok, R2} = cmp(New, Legacy, {add_wallets, R1, remove_range(Accounts, 0, 1), 11, Denom}),
            {ok, R3} = cmp(New, Legacy, {add_wallets, R1,
                    maps:merge(remove_range(Accounts, 1, 2), update_range(Accounts, 2, 3)), 11,
                    Denom}),
            reads_at_all(New, Legacy, [R1, R2, R3], Addrs),
            ok = cmp(New, Legacy, {set_current, R2, 11, 20}),
            reads_at_all(New, Legacy, [R1, R2, R3], Addrs),
            cmp(New, Legacy, get_size)
    end.

%% @doc Advance the denomination on a fork and assert balances redenominate identically.
scenario_denomination({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            Addrs = addrs(Accounts),
            {ok, R1} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R1, 10, 20}),
            [cmp(New, Legacy, {get_balance, A}) || A <- Addrs],
            %% A fork advancing the denomination by one, also touching one account.
            {ok, R2} = cmp(New, Legacy, {add_wallets, R1, update_range(Accounts, 0, 1), 11,
                    Denom + 1}),
            [cmp(New, Legacy, {get_balance, R2, A}) || A <- Addrs],
            cmp(New, Legacy, {get, R2, Addrs}),
            ok = cmp(New, Legacy, {set_current, R2, 11, 20}),
            [cmp(New, Legacy, {get_balance, A}) || A <- Addrs],
            [cmp(New, Legacy, {get_balance, R1, A}) || A <- Addrs]
    end.

%% @doc With ?WALLET_LIST_CHUNK_SIZE == 2 under test, more than two accounts paginate.
%% Walk the cursor over the tip and over a non-tip root.
scenario_chunk_pagination({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            {ok, R1} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R1, 10, 20}),
            walk_chunks(New, Legacy, R1),
            {ok, R2} = cmp(New, Legacy, {add_wallets, R1,
                    maps:merge(add_fresh(2, Denom), remove_range(Accounts, 0, 1)), 11, Denom}),
            walk_chunks(New, Legacy, R2),
            walk_chunks(New, Legacy, R1)
    end.

%% @doc Unknown roots return the same error in both implementations. A chain deeper than
%% the prune depth drops the base off both DAGs together.
scenario_unknown_and_pruned({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            Addrs = addrs(Accounts),
            Bogus = unknown_addr(),
            ?assertEqual(gen_server:call(Legacy, {get, Bogus, Addrs}),
                         gen_server:call(New, {get, Bogus, Addrs})),
            ?assertEqual(gen_server:call(Legacy, {get_chunk, Bogus, first}),
                         gen_server:call(New, {get_wallet_list_chunk, Bogus, first})),
            {ok, R1} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R1, 10, 1}),
            {ok, R2} = cmp(New, Legacy, {add_wallets, R1, update_range(Accounts, 0, 1), 11, Denom}),
            ok = cmp(New, Legacy, {set_current, R2, 11, 1}),
            {ok, R3} = cmp(New, Legacy, {add_wallets, R2, update_range(Accounts, 1, 2), 12, Denom}),
            ok = cmp(New, Legacy, {set_current, R3, 12, 1}),
            %% With prune depth 1 the empty base root is now beyond the window in both.
            ?assertEqual(gen_server:call(Legacy, {get, <<>>, Addrs}),
                         gen_server:call(New, {get, <<>>, Addrs})),
            reads_at_all(New, Legacy, [R3], Addrs)
    end.

%% @doc Pruning drops a losing fork, not only the linear base. Fork R1 into R2 and the
%% uncle R3, extend the winning chain to R4, then set_current(R4) with prune depth 1. The
%% fork base R1 falls out of the depth window and takes the uncle R3 with it: R3's only
%% path to the sink ran through R1, so it is dropped as part of R1's subtree even though
%% its own counter distance to the sink is within the depth. The winning chain (R2, R4)
%% must remain readable throughout.
scenario_prune_drops_uncle({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            Addrs = addrs(Accounts),
            {ok, R1} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R1, 10, 20}),
            {ok, R2} = cmp(New, Legacy, {add_wallets, R1, update_range(Accounts, 0, 1), 11, Denom}),
            {ok, R3} = cmp(New, Legacy, {add_wallets, R1, update_range(Accounts, 1, 2), 11, Denom}),
            ok = cmp(New, Legacy, {set_current, R2, 11, 20}),
            %% Before the prune the uncle is readable like any other root.
            reads_at_all(New, Legacy, [R1, R2, R3], Addrs),
            {ok, R4} = cmp(New, Legacy, {add_wallets, R2, update_range(Accounts, 2, 3), 12, Denom}),
            ok = cmp(New, Legacy, {set_current, R4, 12, 1}),
            %% The base and the uncle hanging off it are gone together.
            ?assertEqual({error, not_found}, gen_server:call(New, {get, R1, Addrs})),
            ?assertEqual({error, not_found}, gen_server:call(New, {get, R3, Addrs})),
            cmp(New, Legacy, {get, R1, Addrs}),
            cmp(New, Legacy, {get, R3, Addrs}),
            cmp(New, Legacy, {get_balance, R3, hd(Addrs)}),
            cmp(New, Legacy, {get_wallet_list_chunk, R3, first}),
            %% The winning chain still reads.
            reads_at_all(New, Legacy, [R2, R4], Addrs)
    end.

%% @doc An empty diff leaves the root unchanged: add_wallets must return the base root and
%% record nothing - no new DAG node, no ETS write. This exercises the
%% maybe_add_node(DAG, R, R, ...) clause.
scenario_noop_diff({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            {ok, R1} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R1, 10, 20}),
            TableBefore = table_dump(),
            StateBefore = sys:get_state(New),
            {ok, R1} = cmp(New, Legacy, {add_wallets, R1, #{}, 11, Denom}),
            ?assertEqual(StateBefore, sys:get_state(New)),
            ?assertEqual(TableBefore, table_dump()),
            %% The tip still advances normally after the no-op.
            ok = cmp(New, Legacy, {set_current, R1, 11, 20}),
            reads_at_all(New, Legacy, [R1], addrs(Accounts))
    end.

%% @doc Remove an account in the middle of a chain and re-add it at the tip with a different
%% value. A read at the middle root must see the account absent and a read at the base root
%% must see the original value - not the tip's. This checks the merge order in
%% merge_total_diff: the diff closer to the target must win over the one closer to the tip.
scenario_remove_then_readd({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            [{Addr, Value} | _] = Accounts,
            {ok, RA} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, RA, 10, 20}),
            {ok, RB} = cmp(New, Legacy, {add_wallets, RA, #{ Addr => remove }, 11, Denom}),
            ok = cmp(New, Legacy, {set_current, RB, 11, 20}),
            {ok, RC} = cmp(New, Legacy, {add_wallets, RB, #{ Addr => bump(Value) }, 12, Denom}),
            ok = cmp(New, Legacy, {set_current, RC, 12, 20}),
            %% The tip holds the re-added (bumped) value. It must not leak into the middle or
            %% base roots.
            ?assertEqual(#{ Addr => bump(Value) }, gen_server:call(New, {get, [Addr]})),
            ?assertEqual(#{}, gen_server:call(New, {get, RB, [Addr]})),
            ?assertEqual(0, gen_server:call(New, {get_balance, RB, Addr})),
            ?assertEqual(#{ Addr => Value }, gen_server:call(New, {get, RA, [Addr]})),
            reads_at_all(New, Legacy, [RA, RB, RC], addrs(Accounts))
    end.

%% @doc Reaching a state in one step, in several steps, and in several steps with a
%% rollback in the middle must all produce the same tip root, and therefore the same
%% balances, in both implementations. This exercises the move_sink and snapshot round
%% trips through the public API.
scenario_step_equivalence({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            %% Disjoint batches so the merged one-shot diff equals applying them in sequence.
            Batch1 = update_range(Accounts, 0, 2),
            Batch2 = maps:merge(update_range(Accounts, 2, 4), add_fresh(2, Denom)),
            Batch3 = remove_range(Accounts, 4, 6),
            Merged = maps:merge(maps:merge(Batch1, Batch2), Batch3),
            {ok, R0} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = cmp(New, Legacy, {set_current, R0, 10, 20}),
            %% One-shot.
            {ok, OneShot} = cmp(New, Legacy, {add_wallets, R0, Merged, 11, Denom}),
            %% Multi-step: apply the batches as a chain, set_current after each.
            MultiStep = apply_steps(New, Legacy, R0, [Batch1, Batch2, Batch3], Denom, 11),
            ?assertEqual(OneShot, MultiStep, multi_step),
            %% Rollback: apply two steps, reorg back to the first, then re-apply forward.
            {ok, S1} = cmp(New, Legacy, {add_wallets, R0, Batch1, 11, Denom}),
            ok = cmp(New, Legacy, {set_current, S1, 11, 20}),
            {ok, S2} = cmp(New, Legacy, {add_wallets, S1, Batch2, 12, Denom}),
            ok = cmp(New, Legacy, {set_current, S2, 12, 20}),
            ok = cmp(New, Legacy, {set_current, S1, 11, 20}),
            Rolled = apply_steps(New, Legacy, S1, [Batch2, Batch3], Denom, 12),
            ?assertEqual(OneShot, Rolled, rollback),
            %% Balances at the (shared) final root agree across impls.
            ok = cmp(New, Legacy, {set_current, OneShot, 13, 20}),
            [cmp(New, Legacy, {get_balance, A}) || A <- addrs(Accounts)]
    end.

%% @doc The same accounts must reach the same tip root no matter the order they are added
%% in. This matters across forks, where competing chains can apply the same updates in
%% different orders. Build the set in one step and as disjoint chunks added forward and
%% reversed, and assert all reach the same root in both implementations.
scenario_chunked_build({New, Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            {ok, OneShot} = cmp(New, Legacy, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            Chunks = chunkify(Accounts, 3),
            Forward = build_chunks(New, Legacy, Chunks, Denom),
            Reverse = build_chunks(New, Legacy, lists:reverse(Chunks), Denom),
            ?assertEqual(OneShot, Forward, forward),
            ?assertEqual(OneShot, Reverse, reverse)
    end.

%% @doc Hashing or traversing a non-tip representation moves the shared ETS tree off the
%% tip and back. That round trip must leave the tip's ETS table byte-identical, cached node
%% hashes included, so the next tip operation re-hashes nothing extra.
scenario_excursions_leave_tip_clean({New, _Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            Addrs = addrs(Accounts),
            {ok, R1} = gen_server:call(New, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = gen_server:call(New, {set_current, R1, 10, 20}),
            Before = table_dump(),
            {ok, R2} = gen_server:call(New, {add_wallets, R1, update_range(Accounts, 0, 1), 11,
                    Denom}),
            {ok, _R3} = gen_server:call(New, {add_wallets, R1,
                    maps:merge(remove_range(Accounts, 1, 2), add_fresh(2, Denom)), 11, Denom}),
            gen_server:call(New, {get_wallet_list_chunk, R2, first}),
            gen_server:call(New, {get, R2, Addrs}),
            [gen_server:call(New, {get_balance, R2, A}) || A <- Addrs],
            ?assertEqual(Before, table_dump())
    end.

%% @doc Like scenario_excursions_leave_tip_clean, but the tip sits at the end of a
%% two-block chain and the reads reach representations several hops away: the parent (one
%% hop), the grandparent (two hops), and an uncle (a fork of the grandparent, reached by
%% descending to the grandparent and climbing back up the other branch). Each
%% get_wallet_list_chunk call moves the shared ETS tree several nodes away before
%% snapshot_restore rolls it back, and must leave the tip byte-identical, cached node
%% hashes included, just like a one-hop read.
scenario_deep_excursions_leave_tip_clean({New, _Legacy, _Stubs}, {_SetName, Accounts, Denom}) ->
    fun() ->
            {ok, R0} = gen_server:call(New, {add_wallets, <<>>, base_map(Accounts), 10, Denom}),
            ok = gen_server:call(New, {set_current, R0, 10, 20}),
            %% An uncle: a fork of the grandparent R0.
            {ok, Uncle} = gen_server:call(New, {add_wallets, R0, update_range(Accounts, 0, 1), 11,
                    Denom}),
            %% The parent, then the tip - a two-block chain off R0.
            {ok, Parent} = gen_server:call(New, {add_wallets, R0,
                    maps:merge(update_range(Accounts, 1, 2), add_fresh(2, Denom)), 11, Denom}),
            ok = gen_server:call(New, {set_current, Parent, 11, 20}),
            {ok, Tip} = gen_server:call(New, {add_wallets, Parent, update_range(Accounts, 2, 3), 12,
                    Denom}),
            ok = gen_server:call(New, {set_current, Tip, 12, 20}),
            Before = table_dump(),
            %% Page each non-tip representation: every chunk call moves the ETS tree there and back.
            [walk_chunks_new(New, Root) || Root <- [Parent, R0, Uncle]],
            ?assertEqual(Before, table_dump())
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

%% @doc apply_block/2 whose previous block's wallet_list is not a node in the diff DAG
%% returns {error, root_hash_not_found} and leaves the gen_server running.
apply_block_unknown_prev_root_returns_error_test() ->
    assert_unknown_root_returns_error(
      fun(UnknownRoot) ->
              PrevB = #block{ height = 0, denomination = 1, redenomination_height = 0,
                              wallet_list = UnknownRoot },
              B = #block{ height = 1, denomination = 1, redenomination_height = 0 },
              {apply_block, B, PrevB}
      end).

%% @doc add_wallets/4 with a base root that is not a node in the diff DAG returns
%% {error, root_hash_not_found} and leaves the gen_server running.
add_wallets_unknown_root_returns_error_test() ->
    assert_unknown_root_returns_error(
      fun(UnknownRoot) -> {add_wallets, UnknownRoot, #{}, 10, 1} end).

%% @doc Paginating an empty tip (the initial <<>> tree, no accounts) yields
%% {ok, {last, []}}, the same as ar_wallets_legacy. The account-set matrix never leaves the
%% tip empty, so this is checked directly.
empty_tip_chunk_read_test() ->
    Stubs = [ensure_stub(N) || N <- [ar_node_worker, ar_storage]],
    {ok, New} = gen_server:start(ar_account_tree, [{blocks, []}], []),
    {ok, Legacy} = gen_server:start(ar_wallets_legacy, [{blocks, []}], []),
    NewReply = gen_server:call(New, {get_wallet_list_chunk, <<>>, first}),
    LegacyReply = gen_server:call(Legacy, {get_chunk, <<>>, first}),
    catch gen_server:stop(New),
    catch gen_server:stop(Legacy),
    [begin unregister_safe(Name), exit(Pid, kill) end || {stub, Name, Pid} <- Stubs],
    ?assertEqual({ok, {last, []}}, NewReply),
    ?assertEqual(LegacyReply, NewReply).

%% @doc Start ar_account_tree, issue MakeRequest(UnknownRoot) - where UnknownRoot is not a node
%% in the diff DAG - and assert the call returns {error, root_hash_not_found} without crashing
%% the gen_server. Started unlinked (gen_server:start) so a crash does not take the test process
%% with it.
assert_unknown_root_returns_error(MakeRequest) ->
    Stubs = [ensure_stub(N) || N <- [ar_node_worker, ar_storage]],
    {ok, New} = gen_server:start(ar_account_tree, [{blocks, []}], []),
    UnknownRoot = crypto:strong_rand_bytes(48),
    Result =
        try
            gen_server:call(New, MakeRequest(UnknownRoot), 5000)
        catch
            Class:Reason -> {Class, Reason}
        end,
    Alive = is_process_alive(New),
    catch gen_server:stop(New),
    [begin unregister_safe(Name), exit(Pid, kill) end || {stub, Name, Pid} <- Stubs],
    ?assertEqual({error, root_hash_not_found}, Result),
    ?assert(Alive).

setup() ->
    Stubs = [ensure_stub(N) || N <- [ar_node_worker, ar_storage]],
    {ok, New} = gen_server:start_link(ar_account_tree, [{blocks, []}], []),
    {ok, Legacy} = gen_server:start_link(ar_wallets_legacy, [{blocks, []}], []),
    {New, Legacy, [S || S <- Stubs, S /= existing]}.

cleanup({New, Legacy, Stubs}) ->
    gen_server:stop(New),
    gen_server:stop(Legacy),
    [begin unregister_safe(Name), exit(Pid, kill) end || {stub, Name, Pid} <- Stubs],
    ok.

ensure_stub(Name) ->
    case whereis(Name) of
        undefined ->
            Pid = spawn(fun drain/0),
            register(Name, Pid),
            {stub, Name, Pid};
        _ ->
            existing
    end.

unregister_safe(Name) ->
    catch unregister(Name).

drain() ->
    receive _ -> drain() end.

%% @doc Issue the same request to both gen_servers and assert identical replies. The
%% request uses the ar_account_tree protocol and is translated for ar_wallets_legacy via
%% legacy_request/1.
cmp(New, Legacy, Request) ->
    NewReply = gen_server:call(New, Request),
    LegacyReply = gen_server:call(Legacy, legacy_request(Request)),
    ?assertEqual(LegacyReply, NewReply, {request, Request}),
    NewReply.

%% @doc Translate a request to the ar_wallets_legacy protocol. ar_account_tree renamed the
%% chunk request to {get_wallet_list_chunk, ...} while ar_wallets_legacy still uses the
%% original {get_chunk, ...}. Every other request is identical across the two
%% implementations.
legacy_request({get_wallet_list_chunk, RootHash, Cursor}) ->
    {get_chunk, RootHash, Cursor};
legacy_request(Request) ->
    Request.

%% @doc Apply a list of diffs as a chain off Root (add_wallets then set_current for each) and
%% return the final root. Heights increase from StartHeight.
apply_steps(New, Legacy, Root, Diffs, Denom, StartHeight) ->
    {Final, _} = lists:foldl(
        fun(Diff, {Base, Height}) ->
            {ok, Next} = cmp(New, Legacy, {add_wallets, Base, Diff, Height, Denom}),
            ok = cmp(New, Legacy, {set_current, Next, Height, 20}),
            {Next, Height + 1}
        end,
        {Root, StartHeight},
        Diffs
    ),
    Final.

%% @doc Add each disjoint chunk on top of the previous tip and return the final root.
build_chunks(New, Legacy, Chunks, Denom) ->
    {Root, _} = lists:foldl(
        fun(Chunk, {Base, Height}) ->
            {ok, Next} = cmp(New, Legacy, {add_wallets, Base, maps:from_list(Chunk), Height,
                    Denom}),
            ok = cmp(New, Legacy, {set_current, Next, Height, 20}),
            {Next, Height + 1}
        end,
        {<<>>, 10},
        Chunks
    ),
    Root.

%% @doc Split a list into N (or fewer) disjoint contiguous chunks.
chunkify(List, N) ->
    Size = max(1, (length(List) + N - 1) div N),
    chunk_by(List, Size).

chunk_by([], _Size) ->
    [];
chunk_by(List, Size) ->
    {Head, Tail} = lists:split(min(Size, length(List)), List),
    [Head | chunk_by(Tail, Size)].

reads_at_all(New, Legacy, Roots, Addresses) ->
    cmp(New, Legacy, {get, Addresses}),
    cmp(New, Legacy, get_size),
    [cmp(New, Legacy, {get, Root, Addresses}) || Root <- Roots],
    [cmp(New, Legacy, {get_balance, Root, Addr}) || Root <- Roots, Addr <- Addresses],
    ok.

walk_chunks(New, Legacy, Root) ->
    walk_chunks(New, Legacy, Root, first).

walk_chunks(New, Legacy, Root, Cursor) ->
    {ok, {NextCursor, _Range}} = cmp(New, Legacy, {get_wallet_list_chunk, Root, Cursor}),
    case NextCursor of
        last ->
            ok;
        _ ->
            walk_chunks(New, Legacy, Root, NextCursor)
    end.

%% @doc Page every chunk of Root through New alone, without the legacy comparison. Used by
%% the scenarios asserting the tip stays clean, where each chunk call moves the shared ETS
%% tree to Root and back.
walk_chunks_new(New, Root) ->
    walk_chunks_new(New, Root, first).

walk_chunks_new(New, Root, Cursor) ->
    {ok, {NextCursor, _Range}} = gen_server:call(New, {get_wallet_list_chunk, Root, Cursor}),
    case NextCursor of
        last ->
            ok;
        _ ->
            walk_chunks_new(New, Root, NextCursor)
    end.

table_dump() ->
    lists:sort(ets:tab2list(ar_patricia_tree)).

%%%===================================================================
%%% Account set builders and diff derivation.
%%%===================================================================

addrs(Accounts) ->
    [Addr || {Addr, _} <- Accounts].

base_map(Accounts) ->
    maps:from_list(Accounts).

simple_accounts(From, To) ->
    [{addr(I), {I * 100, tx(I)}} || I <- lists:seq(From, To)].

%% @doc Distinct 32-byte keys sharing leading bytes to varying degrees, to exercise the
%% radix tree's splitting and merging.
prefix_accounts() ->
    Keys = lists:usort([pad32(<< (I rem 3):8, (I rem 5):8, I:8 >>) || I <- lists:seq(1, 40)]),
    [{Key, {erlang:phash2(Key, 1000000000), crypto:hash(sha256, Key)}} || Key <- Keys].

%% @doc Four-tuple accounts {Balance, LastTX, BaseDenomination, MiningPermission} with base
%% denominations 1..3, used with a tree denomination of 3.
four_tuple_accounts() ->
    [{addr(1000 + I), {I * 1000, tx(1000 + I), 1 + (I rem 3), I rem 2 == 0}}
     || I <- lists:seq(1, 20)].

%% @doc A diff updating the accounts at 0-indexed positions From..To-1 to bumped values.
update_range(Accounts, From, To) ->
    maps:from_list([{Addr, bump(Value)} || {Addr, Value} <- slice(Accounts, From, To)]).

%% @doc A diff removing the accounts at 0-indexed positions From..To-1.
remove_range(Accounts, From, To) ->
    maps:from_list([{Addr, remove} || {Addr, _} <- slice(Accounts, From, To)]).

slice(Accounts, From, To) ->
    lists:sublist(Accounts, From + 1, max(0, To - From)).

%% @doc A diff adding N fresh accounts (keys disjoint from every account set).
add_fresh(N, _Denom) ->
    maps:from_list([{Addr, {900000 + I, tx_bin(Addr)}} || {I, Addr} <- enum(fresh_addrs(N))]).

fresh_addrs(N) ->
    [addr(900000 + I) || I <- lists:seq(1, N)].

enum(List) ->
    lists:zip(lists:seq(1, length(List)), List).

bump({Balance, _LastTX}) ->
    {Balance + 1, crypto:hash(sha256, <<Balance:64>>)};
bump({Balance, _LastTX, BaseDenomination, MiningPermission}) ->
    {Balance + 1, crypto:hash(sha256, <<Balance:64>>), BaseDenomination, not MiningPermission}.

unknown_addr() ->
    addr(7777777).

addr(I) ->
    crypto:hash(sha256, <<I:32>>).

tx(I) ->
    crypto:hash(sha256, <<I:64>>).

tx_bin(Addr) ->
    crypto:hash(sha256, <<Addr/binary, 0>>).

pad32(Prefix) when byte_size(Prefix) =< 32 ->
    << Prefix/binary, 0:((32 - byte_size(Prefix)) * 8) >>.
