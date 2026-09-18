%%% Shared admission and lifetime accounting for chunk-processing memory.
-module(ar_chunk_cache).
-behaviour(gen_server).
-export([
    create_ets/0,
    start_link/0,
    reserve/1, reserve/2,
    add_reference/2,
    add_references/3,
    transfer/2,
    transfer_many/2,
    release/1,
    mark_cached/1,
    reserved_size/0,
    cached_size/0, cached_size/1,
    limit/0,
    is_full/0,
    completed/1,
    record_completed/1,
    configure/2,
    reservation_bytes/0,
    limits/3,
    interval_limit/0,
    validate_config/0
]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).
-export_type([cache_ref/0]).
-include("ar.hrl").

-record(cache_ref, {
    % Shared capacity reservation.
    allocation_ref,
    % One independently releasable hold on the reservation.
    holder_ref
}).
-opaque cache_ref() :: #cache_ref{}.

%% Input, unpacked intermediate and packed output can coexist. This allowance
%% belongs to the whole lifecycle, so packing never needs new admission.
-define(CHUNK_BYTES, (3 * ?DATA_CHUNK_SIZE)).
%% Preserve the previous 2000-chunk default minus its 64 MiB metadata share.
-define(DEFAULT_CHUNKS, (2000 - 256)).
-define(INTERVAL_BYTES, (64 * ?MiB)).

create_ets() ->
    ets:new(?MODULE, [named_table, public, set, {read_concurrency, true}]),
    ok.

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Reserve lifecycle capacity before reading or fetching chunk payloads.
reserve(StoreID) -> reserve(StoreID, 1).
reserve(StoreID, Count) when is_integer(Count), Count > 0 ->
    CacheRef = #cache_ref{
        allocation_ref = make_ref(),
        holder_ref = make_ref()
    },
    call_with_cleanup(
        {reserve, CacheRef, StoreID, Count, self()}, [CacheRef], full
    ).

%% @doc Add an independently releasable reference without reserving more capacity.
add_reference(CacheRef, PID) when is_pid(PID) ->
    case add_references(CacheRef, PID, 1) of
        {ok, [NewCacheRef]} -> {ok, NewCacheRef};
        Error -> Error
    end.

%% @doc Add Count independent references to one reservation in a single call.
add_references(CacheRef, PID, Count) when
    is_pid(PID), is_integer(Count), Count >= 0
->
    do_add_references(add_reference, lists:duplicate(Count, CacheRef), PID).

%% @doc Transfer a holder's reservation without acquiring more capacity.
transfer(CacheRef, PID) when is_pid(PID) ->
    case transfer_many([CacheRef], PID) of
        {ok, [NewCacheRef]} -> {ok, NewCacheRef};
        Error -> Error
    end.

%% @doc Transfer distinct references together, leaving all unchanged if expired.
transfer_many(CacheRefs, PID) when is_list(CacheRefs), is_pid(PID) ->
    do_add_references(transfer, CacheRefs, PID).

do_add_references(_Operation, [], _PID) ->
    {ok, []};
do_add_references(Operation, CacheRefs, PID) ->
    NewCacheRefs = lists:map(
        fun(#cache_ref{} = CacheRef) ->
            CacheRef#cache_ref{holder_ref = make_ref()}
        end,
        CacheRefs
    ),
    call_with_cleanup(
        {references, Operation, lists:zip(CacheRefs, NewCacheRefs), PID},
        NewCacheRefs,
        {error, expired}
    ).

call_with_cleanup(Request, CacheRefs, Unavailable) ->
    %% The caller knows the new references even if the coordinator dies
    %% after recording ownership but before replying. Those holds must not leak.
    try
        call(Request, Unavailable)
    catch
        exit:Reason ->
            do_release(CacheRefs),
            exit(Reason)
    end.

%% @doc Release this holder once, leaving concurrent holders accounted for.
release(undefined) ->
    ok;
release(#cache_ref{} = CacheRef) ->
    do_release([CacheRef]).

do_release(CacheRefs) ->
    %% A completion can race the coordinator's restart. Keep an idempotent
    %% release in the supervisor-owned table until a coordinator consumes it.
    try
        ets:insert(?MODULE, [
            {{release, Ref, Hold}, true}
         || #cache_ref{allocation_ref = Ref, holder_ref = Hold} <- CacheRefs
        ])
    catch
        error:badarg -> ok
    end,
    try
        call({release, CacheRefs}, ok)
    catch
        exit:_ -> ok
    end.

%% @doc Mark a reserved chunk as fetched or read into memory.
mark_cached(CacheRef) -> call({cached, CacheRef}, ok).

%% @doc Count reserved chunks, including data not yet fetched or read.
reserved_size() -> counter(used).

%% @doc Count chunks that have been fetched or read into memory.
cached_size() ->
    try
        lists:sum(
            ets:select(
                ?MODULE,
                [{{{cached, '_'}, '$1'}, [], ['$1']}]
            )
        )
    catch
        error:badarg -> 0
    end.

%% @doc Count cached chunks for StoreID.
cached_size(StoreID) -> counter({cached, StoreID}).

limit() -> counter(limit).
is_full() -> reserved_size() >= limit().
completed(StoreID) -> counter({completed, StoreID}).
record_completed(StoreID) ->
    ets:update_counter(
        ?MODULE,
        {completed, StoreID},
        {2, 1},
        {{completed, StoreID}, 0}
    ),
    ok.
reservation_bytes() -> ?CHUNK_BYTES.
interval_limit() -> ?INTERVAL_BYTES.

%% @doc Reject combined cache limits that leave no memory for other services.
validate_config() ->
    validate_config(#{}).

validate_config(Updates) ->
    {Chunks, Intervals} = configured_limits(Updates),
    Entropy = config_value([packing, entropy, cache_size], Updates) * ?MiB,
    Total = arweave_util:system_memory(),
    case Total =:= undefined orelse Chunks + Intervals + Entropy < Total of
        true ->
            ok;
        false ->
            {error, <<
                "Chunk, entropy and peer-interval cache budgets "
                "exceed available system/container memory. Reduce "
                "packing.cache_size or packing.entropy.cache_size; "
                "leave memory for the rest of the node."
            >>}
    end.

%% @doc Recompute the shared budget after a configuration update.
configure(Key, Value) ->
    %% handle_set runs before the registry commits/validates a runtime set.
    %% Reject unsafe budgets before sending any live reconfiguration.
    Validation =
        case arweave_config:is_runtime() of
            true -> validate_config(#{Key => Value});
            false -> ok
        end,
    case Validation of
        ok -> gen_server:cast(?MODULE, {configure, Key, Value});
        Error -> Error
    end.

call(Request, Unavailable) ->
    case whereis(?MODULE) of
        undefined -> Unavailable;
        PID -> gen_server:call(PID, Request, infinity)
    end.

counter(Key) ->
    try
        ets:lookup_element(?MODULE, Key, 2)
    catch
        error:badarg -> 0
    end.

init([]) ->
    %% The supervisor owns the table: restarting this coordinator must not
    %% forget payloads still held by packing, storage or producer processes.
    Monitors = restore_allocations(),
    configure_limits(#{}),
    erlang:send_after(200, self(), metrics),
    {ok, release_pending(Monitors)}.

restore_allocations() ->
    ets:insert(?MODULE, {used, 0}),
    ets:match_delete(?MODULE, {{cached, '_'}, '_'}),
    lists:foldl(fun restore_allocation/2, #{}, ets:tab2list(?MODULE)).

restore_allocation({Ref, StoreID, Count, Cached, Holders}, Monitors) when
    is_reference(Ref)
->
    LiveHolders = maps:filter(
        fun(_, PID) -> is_process_alive(PID) end,
        Holders
    ),
    case map_size(LiveHolders) of
        0 ->
            ets:delete(?MODULE, Ref),
            Monitors;
        _ ->
            %% Count each surviving allocation once, regardless of its holders.
            ets:insert(?MODULE, {Ref, StoreID, Count, Cached, LiveHolders}),
            add_size(StoreID, Count, Cached),
            maps:fold(
                fun(_, PID, Acc) -> monitor_holder(PID, Acc) end,
                Monitors,
                LiveHolders
            )
    end;
restore_allocation(_, Monitors) ->
    Monitors.

handle_call(
    {reserve, #cache_ref{allocation_ref = Ref, holder_ref = Hold} = CacheRef,
        StoreID, Count, PID},
    _From,
    Monitors
) ->
    case reserved_size() + Count =< limit() of
        false ->
            {reply, full, Monitors};
        true ->
            ets:insert(?MODULE, {Ref, StoreID, Count, false, #{Hold => PID}}),
            add_size(StoreID, Count, false),
            {reply, {ok, CacheRef}, monitor_holder(PID, Monitors)}
    end;
handle_call({references, Operation, References, PID}, _From, Monitors) ->
    %% Validate the whole batch before transferring any of its holders.
    case valid_references(Operation, References) of
        true ->
            Monitors2 = lists:foldl(
                fun({CacheRef, NewCacheRef}, Acc) ->
                    do_add_reference(Operation, CacheRef, NewCacheRef, PID, Acc)
                end,
                Monitors,
                References
            ),
            {reply, {ok, [New || {_, New} <- References]}, Monitors2};
        false ->
            {reply, {error, expired}, Monitors}
    end;
handle_call({release, CacheRefs}, _From, Monitors) ->
    {reply, ok, lists:foldl(fun release_holder/2, Monitors, CacheRefs)};
handle_call(
    {cached, #cache_ref{allocation_ref = Ref, holder_ref = Hold}},
    _From,
    Monitors
) ->
    case ets:lookup(?MODULE, Ref) of
        [{Ref, StoreID, Count, false, Holders}] when
            is_map_key(Hold, Holders)
        ->
            add_counter({cached, StoreID}, Count),
            ets:insert(?MODULE, {Ref, StoreID, Count, true, Holders});
        _ ->
            ok
    end,
    {reply, ok, Monitors}.

handle_cast({configure, Key, Value}, Monitors) ->
    configure_limits(#{Key => Value}),
    {noreply, Monitors}.

handle_info({'DOWN', Monitor, process, PID, _}, Monitors) ->
    case maps:get(PID, Monitors, undefined) of
        {Monitor, _} ->
            CacheRefs = owned_references(PID),
            {noreply, lists:foldl(fun release_holder/2, Monitors, CacheRefs)};
        _ ->
            {noreply, Monitors}
    end;
handle_info(metrics, Monitors) ->
    Monitors2 = release_pending(Monitors),
    arweave_metrics:gauge_set(chunk_cache_size, reserved_size()),
    arweave_metrics:gauge_set(chunk_cache_size_limit, limit()),
    lists:foreach(
        fun
            ({{cached, StoreID}, Count}) ->
                arweave_metrics:gauge_set(
                    chunk_cache_size_by_store, [StoreID], Count
                );
            (_) ->
                ok
        end,
        ets:tab2list(?MODULE)
    ),
    erlang:send_after(200, self(), metrics),
    {noreply, Monitors2}.

owned_references(PID) ->
    Entries = ets:tab2list(?MODULE),
    lists:flatmap(fun(Entry) -> owned_references(Entry, PID) end, Entries).

owned_references({Ref, _, _, _, Holders}, PID) ->
    HolderRefs = [
        Hold
     || {Hold, Owner} <- maps:to_list(Holders),
        Owner =:= PID
    ],
    [
        #cache_ref{allocation_ref = Ref, holder_ref = Hold}
     || Hold <- HolderRefs
    ];
owned_references(_, _) ->
    [].

valid_references(Operation, References) ->
    CacheRefs = [CacheRef || {CacheRef, _} <- References],
    %% A transfer cannot consume the same holder twice.
    Distinct =
        Operation =:= add_reference orelse
            length(lists:usort(CacheRefs)) =:= length(CacheRefs),
    Distinct andalso lists:all(fun reference_exists/1, CacheRefs).

reference_exists(#cache_ref{allocation_ref = Ref, holder_ref = Hold}) ->
    case ets:lookup(?MODULE, Ref) of
        [{Ref, _, _, _, Holders}] -> maps:is_key(Hold, Holders);
        [] -> false
    end.

do_add_reference(
    Operation,
    #cache_ref{allocation_ref = Ref, holder_ref = Hold},
    #cache_ref{holder_ref = NewHold},
    PID,
    Monitors
) ->
    [{Ref, StoreID, Count, Cached, Holders}] = ets:lookup(?MODULE, Ref),
    Holders2 = Holders#{NewHold => PID},
    Monitors2 = monitor_holder(PID, Monitors),
    {Holders3, Monitors3} =
        case Operation of
            add_reference ->
                {Holders2, Monitors2};
            transfer ->
                {
                    maps:remove(Hold, Holders2),
                    unmonitor_holder(maps:get(Hold, Holders), Monitors2)
                }
        end,
    ets:insert(?MODULE, {Ref, StoreID, Count, Cached, Holders3}),
    Monitors3.

release_pending(Monitors) ->
    CacheRefs = ets:select(
        ?MODULE,
        [
            {{{release, '$1', '$2'}, true}, [], [
                {#cache_ref{allocation_ref = '$1', holder_ref = '$2'}}
            ]}
        ]
    ),
    lists:foldl(fun release_holder/2, Monitors, CacheRefs).

release_holder(#cache_ref{allocation_ref = Ref, holder_ref = Hold}, Monitors) ->
    Monitors2 =
        case ets:lookup(?MODULE, Ref) of
            [Allocation] ->
                do_release_holder(Allocation, Hold, Monitors);
            [] ->
                Monitors
        end,
    %% Repeated releases are harmless; consume the marker even if already freed.
    ets:delete(?MODULE, {release, Ref, Hold}),
    Monitors2.

do_release_holder({Ref, StoreID, Count, Cached, Holders}, Hold, Monitors) ->
    case maps:take(Hold, Holders) of
        error ->
            Monitors;
        {PID, RemainingHolders} ->
            update_allocation({Ref, StoreID, Count, Cached, RemainingHolders}),
            unmonitor_holder(PID, Monitors)
    end.

update_allocation({Ref, StoreID, Count, Cached, Holders}) when
    map_size(Holders) =:= 0
->
    %% Capacity remains reserved until the last holder releases the allocation.
    ets:delete(?MODULE, Ref),
    add_size(StoreID, -Count, Cached);
update_allocation(Allocation) ->
    ets:insert(?MODULE, Allocation).

monitor_holder(PID, Monitors) ->
    case maps:get(PID, Monitors, undefined) of
        undefined -> Monitors#{PID => {monitor(process, PID), 1}};
        {Ref, N} -> Monitors#{PID => {Ref, N + 1}}
    end.

unmonitor_holder(PID, Monitors) ->
    case maps:get(PID, Monitors) of
        {Ref, 1} ->
            demonitor(Ref, [flush]),
            maps:remove(PID, Monitors);
        {Ref, N} ->
            Monitors#{PID => {Ref, N - 1}}
    end.

add_size(StoreID, Count, Cached) ->
    add_counter(used, Count),
    case Cached of
        true -> add_counter({cached, StoreID}, Count);
        false -> ok
    end.

add_counter(Key, N) ->
    ets:update_counter(?MODULE, Key, {2, N}, {Key, 0}).

configure_limits(Updates) ->
    {ChunkBytes, _} = configured_limits(Updates),
    ets:insert(?MODULE, {limit, ChunkBytes div ?CHUNK_BYTES}),
    ok.

configured_limits(Updates) ->
    Get = fun(Key) -> config_value(Key, Updates) end,
    Requested = Get([packing, cache_size]),
    Entropy = Get([packing, entropy, cache_size]) * ?MiB,
    Total =
        case arweave_util:system_memory() of
            undefined ->
                %% No reliable RAM reading yet: retain the default chunk count,
                %% rather than reject configuration against invented hardware.
                2 *
                    (Entropy + ?INTERVAL_BYTES + ?DEFAULT_CHUNKS * ?CHUNK_BYTES);
            Bytes ->
                Bytes
        end,
    limits(Total, Entropy, Requested).

config_value(Key, Updates) ->
    maps:get(Key, Updates, arweave_config:get(Key)).

%% @doc Derive chunk capacity from half of RAM after entropy and metadata.
limits(Total, Entropy, RequestedMiB) ->
    Available = max(0, Total div 2 - Entropy - ?INTERVAL_BYTES),
    Requested =
        case RequestedMiB of
            undefined -> min(?DEFAULT_CHUNKS * ?CHUNK_BYTES, Available);
            _ -> RequestedMiB * ?MiB
        end,
    {max(?CHUNK_BYTES, Requested), ?INTERVAL_BYTES}.
