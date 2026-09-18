-module(arweave_sync_application_SUITE).
-compile([export_all, nowarn_export_all]).
-compile({no_auto_import, [get/1]}).
-include_lib("stdlib/include/assert.hrl").

suite() -> [{timetrap, {seconds, 30}}].

all() ->
    [
        standalone_application,
        app_owned_supervision,
        disabled_sync_keeps_store_registration,
        outbound_calls_use_boundary,
        selected_modules_implement_used_apis,
        mainnet_dependencies_select_external_modules,
        missing_ranges_preserve_sync_filters
    ].

init_per_suite(Config) ->
    {ok, Started} = application:ensure_all_started(arweave_sync),
    [{started_apps, Started} | Config].

end_per_suite(Config) ->
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ),
    ok.

init_per_testcase(_Case, Config) ->
    arweave_sync_deps:override_module(arweave_sync_test_deps),
    Config.

end_per_testcase(_Case, _Config) ->
    arweave_sync:deactivate(),
    arweave_sync_deps:reset_all_overrides().

%%====================================================================
%% Test cases
%%====================================================================

%% @doc The sync application starts without the host node or an active network
%% pipeline.
standalone_application(_Config) ->
    ?assertNot(lists:keymember(arweave, 1, application:which_applications())),
    ?assertEqual(undefined, whereis(ar_sup)),
    ?assert(is_pid(whereis(arweave_sync_sup))),
    ?assertEqual(undefined, whereis(arweave_sync_pipeline_sup)),
    ?assertEqual(arweave_sync, application_for(arweave_sync)),
    ?assertEqual(arweave_util, application_for(ar_intervals)).

%% @doc Sync owns its table and pipeline workers across activation and
%% deactivation.
app_owned_supervision(_Config) ->
    Root = whereis(arweave_sync_sup),
    Table = arweave_sync_state,
    ?assertEqual(Root, ets:info(Table, owner)),
    ok = arweave_sync:activate(),
    try
        ?assert(is_pid(whereis(arweave_sync_sup))),
        ?assert(is_pid(whereis(arweave_sync_scheduler))),
        ?assert(is_pid(whereis(arweave_sync_discovery))),
        ?assertEqual(pong, gen_server:call(arweave_sync_scheduler, ping)),
        ?assertEqual(0, arweave_sync_discovery:inflight_count()),
        ?assertEqual(
            [arweave_sync_discovery, arweave_sync_scheduler],
            lists:sort([
                ID
             || {ID, _, _, _} <-
                    supervisor:which_children(arweave_sync_pipeline_sup)
            ])
        )
    after
        arweave_sync:deactivate()
    end,
    ?assertEqual(Root, ets:info(Table, owner)),
    ?assertEqual(undefined, whereis(arweave_sync_scheduler)),
    ?assertEqual(undefined, whereis(arweave_sync_discovery)).

%% @doc Disabling downloads keeps the pipeline available for runtime resume.
disabled_sync_keeps_store_registration(_Config) ->
    arweave_config:internal_with_test_config(fun() ->
        ok = arweave_config:set([sync, max_download_rate], 0),
        ?assertNot(arweave_sync:enabled()),
        ?assertEqual(ok, arweave_sync:activate()),
        ?assert(is_pid(whereis(arweave_sync_pipeline_sup))),
        ?assert(is_pid(whereis(arweave_sync_runtime))),
        ?assertEqual(
            whereis(arweave_sync_sup),
            ets:info(arweave_sync_state, owner)
        )
    end).

%% @doc Sync modules have no direct imports outside their declared application
%% boundary.
outbound_calls_use_boundary(_Config) ->
    {ok, Modules} = application:get_key(arweave_sync, modules),
    {ok, Dependencies} = application:get_key(arweave_sync, applications),
    AllowedModules =
        %% The facade's AR_TEST-only operations delegate to an app-owned fixture.
        [arweave_sync_test_util | erlang:pre_loaded()] ++
            lists:flatmap(
                fun(App) ->
                    {ok, AppModules} = application:get_key(App, modules),
                    AppModules
                end,
                [arweave_sync | Dependencies]
            ),
    Calls = lists:flatmap(
        fun(Module) ->
            {ok, {Module, [{imports, Imports}]}} =
                beam_lib:chunks(code:which(Module), [imports]),
            [
                {Module, Dependency, Function, Arity}
             || {Dependency, Function, Arity} <- Imports,
                not lists:member(Dependency, AllowedModules)
            ]
        end,
        Modules
    ),
    ?assertEqual(arweave_sync, application_for(arweave_sync_deps_mainnet)),
    ?assertEqual([], Calls).

%% @doc Mainnet and simulator dependency selectors provide the APIs used by
%% sync.
selected_modules_implement_used_apis(_Config) ->
    {ok, Modules} = application:get_key(arweave_sync, modules),
    Calls = lists:usort(lists:flatmap(fun dependency_calls/1, Modules)),
    ?assertNotEqual([], Calls),
    ?assertEqual([], missing_dependency_functions(arweave_sync_deps_mainnet, Calls)),
    %% Simulated writes bypass ingestion, but use every other pipeline service.
    SimCalls = [
        Call
     || {Module, _, _, _} = Call <- Calls,
        Module =/= arweave_sync_ingest
    ],
    ?assertEqual([], missing_dependency_functions(arweave_sync_deps_sim, SimCalls)).

%% @doc Mainnet dependency functions only select external modules, without
%% embedded logic.
mainnet_dependencies_select_external_modules(_Config) ->
    {ok, SyncModules} = application:get_key(arweave_sync, modules),
    Functions = [
        Function
     || {function, _, _, _, _} = Function <-
            abstract_code(arweave_sync_deps_mainnet)
    ],
    ?assertNotEqual([], Functions),
    ?assertEqual([], [
        {Name, Arity}
     || {function, _, Name, Arity, _} = Function <- Functions,
        not external_module_selector(Function, SyncModules)
    ]).

%% @doc Missing-range queries exclude stored, blacklisted and footprint-limited
%% data.
missing_ranges_preserve_sync_filters(_Config) ->
    arweave_sync_deps:override_module(?MODULE),
    %% Six chunks span three two-chunk sectors. Keep only the first footprint,
    %% with its first chunk already stored and its second occurrence blacklisted.
    ChunkSize = 262144,
    End = 6 * ChunkSize,
    put(missing_byte_intervals, ar_intervals:from_list([{End, ChunkSize}])),
    put(
        blacklisted_intervals,
        ar_intervals:from_list([{3 * ChunkSize, 2 * ChunkSize}])
    ),
    put(
        kept_intervals,
        ar_intervals:from_list([
            {ChunkSize, 0},
            {3 * ChunkSize, 2 * ChunkSize},
            {5 * ChunkSize, 4 * ChunkSize}
        ])
    ),
    ?assertEqual(
        [{5 * ChunkSize, 4 * ChunkSize}],
        ar_intervals:to_list(
            arweave_sync_store_sweeper:unsynced_intervals(0, End, test_store)
        )
    ),
    ?assertEqual({0, End, 1}, get(kept_query)),
    {FootprintStart, FootprintEnd} = arweave_storage:get_footprint_range(0, 0),
    ?assertEqual(
        [{FootprintEnd, FootprintStart}],
        ar_intervals:to_list(
            arweave_sync_store_sweeper:unsynced_footprint_intervals(0, 0, test_store)
        )
    ),
    ?assertEqual({FootprintStart, FootprintEnd}, erase(footprint_query)),
    ?assertEqual(
        [],
        ar_intervals:to_list(
            arweave_sync_store_sweeper:unsynced_footprint_intervals(0, 1, test_store)
        )
    ),
    ?assertEqual(undefined, get(footprint_query)).

%%====================================================================
%% Helpers
%%====================================================================

abstract_code(Module) ->
    {ok, {Module, [{abstract_code, {raw_abstract_v1, Forms}}]}} =
        beam_lib:chunks(code:which(Module), [abstract_code]),
    Forms.

dependency_calls(Module) ->
    lists:flatmap(
        fun(Clause) ->
            Bindings = maps:from_list(
                collect_terms(
                    fun
                        ({match, _, {var, _, Var}, Expression}) ->
                            case selector(Expression) of
                                none -> [];
                                Name -> [{Var, Name}]
                            end;
                        (_) ->
                            []
                    end,
                    Clause
                )
            ),
            collect_terms(
                fun
                    ({call, _, {remote, _, Expression, {atom, _, Function}}, Args}) ->
                        Selector =
                            case Expression of
                                {var, _, Var} -> maps:get(Var, Bindings, none);
                                _ -> selector(Expression)
                            end,
                        case Selector of
                            none -> [];
                            _ -> [{Module, Selector, Function, length(Args)}]
                        end;
                    (_) ->
                        []
                end,
                Clause
            )
        end,
        [
            Clause
         || {function, _, _, _, Clauses} <- abstract_code(Module),
            Clause <- Clauses
        ]
    ).

selector({call, _, {remote, _, {atom, _, arweave_sync_deps}, {atom, _, Name}}, []}) -> Name;
selector(_) -> none.

collect_terms(Fun, Term) when is_tuple(Term) ->
    Fun(Term) ++ collect_terms(Fun, tuple_to_list(Term));
collect_terms(Fun, Terms) when is_list(Terms) ->
    lists:flatmap(fun(Term) -> collect_terms(Fun, Term) end, Terms);
collect_terms(_Fun, _Term) ->
    [].

missing_dependency_functions(Implementation, Calls) ->
    lists:filter(
        fun({_Caller, Selector, Function, Arity}) ->
            Module = Implementation:Selector(),
            code:ensure_loaded(Module),
            not erlang:function_exported(Module, Function, Arity)
        end,
        Calls
    ).

external_module_selector(
    {function, _, _, 0, [{clause, _, [], [], [{atom, _, Module}]}]},
    SyncModules
) ->
    not lists:member(Module, SyncModules);
external_module_selector(_, _) ->
    false.

storage() -> ?MODULE.
blacklist() -> ?MODULE.
footprint_limit() -> ?MODULE.

get_intervals(unsynced, _Start, _End, any_packing, {ar_data_sync, byte}, test_store) ->
    get(missing_byte_intervals);
get_intervals(unsynced, Start, End, any_packing, {ar_data_sync, footprint}, test_store) ->
    put(footprint_query, {Start, End}),
    ar_intervals:from_list([{End, Start}]).

get_blacklisted_intervals(_Start, _End) -> get(blacklisted_intervals).
get(test_store) -> 1;
get(Key) -> erlang:get(Key).
kept_intervals(Start, End, Limit) ->
    put(kept_query, {Start, End, Limit}),
    get(kept_intervals).

application_for(Module) ->
    {ok, App} = application:get_application(Module),
    App.
