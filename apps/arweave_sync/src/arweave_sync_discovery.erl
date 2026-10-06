%%% @doc Peer discovery: scans all known peers, queries the byte ranges and
%%% footprints each one advertises, and caches them in two ETS tables for
%%% arweave_sync_sweeper to read:
%%% - the sync bucket cache (?SYNC_BUCKET_CACHE_TABLE) holds the coarse byte
%%%   and footprint ranges ("sync buckets") each peer advertises, with the
%%%   share of each bucket the peer holds;
%%% - the chunk interval cache (?CHUNK_INTERVAL_CACHE_TABLE) holds, for each
%%%   peer and location, the intervals the peer serves there. A location is
%%%   one QUERY_RANGE_STEP_SIZE step in byte mode or one footprint in footprint
%%%   mode.
%%%
%%% The process fills and drains the caches in these phases:
%%% 1. Peer collection (add_peer/2), every
%%%    DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS and once on join: track up to
%%%    MAX_DISCOVERY_PEERS current peers and queue a sync bucket job for each
%%%    new one. A peer `removed' event drops the peer's jobs and cache rows
%%%    (do_remove_peer/3).
%%% 2. Sync bucket refresh (enqueue_sync_bucket_jobs/1), every
%%%    SYNC_BUCKET_JOB_INTERVAL_MS with jitter: queue a sync bucket job for each
%%%    tracked peer. The job fetches the peer's byte and footprint sync buckets
%%%    into the sync bucket cache, and marks the peer's chunk interval rows in
%%%    a bucket stale when the bucket's share changed.
%%% 3. Warming (warm_peer_ranges/3): when the sweeper queues a sweep range,
%%%    and again when it processes the range, it asks discovery to warm the
%%%    range. Discovery queues a chunk interval job, in byte and footprint
%%%    mode, for each peer whose sync bucket covers the range and whose cached
%%%    intervals there are missing or stale (older than
%%%    CHUNK_INTERVAL_CACHE_TTL_MS). The job fetches the peer's intervals from
%%%    /data_sync_record or /footprints. Warming does not refresh sync buckets;
%%%    phase 2 does that.
%%% 4. Reads, in the caller's process: get_peers_for_offset/1 returns the
%%%    peers that advertise the sync bucket at an offset; cached_peer_ranges/5
%%%    returns the cached peer ranges for a sweep range, stale rows included,
%%%    and `cache_miss' when a peer and mode have no row.
%%% 5. Maintenance (run_maintenance/1), every MAINTENANCE_INTERVAL_MS: delete
%%%    sync bucket rows older than SYNC_BUCKET_CACHE_TTL_MS along with their
%%%    chunk interval rows, trim the oldest rows when the chunk interval cache
%%%    exceeds its byte limit, and publish metrics.
%%%
%%% Jobs run in monitored processes (start_jobs/2). Jobs for the peers and
%%% stores with the fewest running jobs start first. At most
%%% MAX_DISCOVERY_JOBS_PER_KIND jobs of each kind run at once, and at most
%%% MAX_BYTE_JOBS_PER_PEER_STORE or MAX_FOOTPRINT_JOBS_PER_PEER_STORE per peer,
%%% store and mode.
-module(arweave_sync_discovery).

-ifdef(AR_TEST).
-export([
    add_peer/2,
    chunk_interval_key/3,
    chunk_interval_lookup/3,
    compute_job_load/1,
    delete_expired_sync_buckets/0,
    do_remove_peer/3,
    enqueue_job/2,
    enqueue_sync_bucket_jobs/1,
    fetch_chunk_intervals/3,
    finish_job/2,
    get_chunk_intervals/5,
    inflight_peer_store_mode_counts/1,
    interval_location/2,
    job_exists/2,
    mark_chunk_intervals_stale_on_share_change/4,
    refresh_chunk_intervals/1,
    remove_jobs/2,
    start_jobs/1,
    store_row/5,
    sync_bucket/2,
    sync_bucket_job/1,
    sync_bucket_key/3,
    take_next_job/4
]).
-endif.

-behaviour(gen_server).

-export([
    start_link/0,
    get_peers_for_offset/1,
    warm_peer_ranges/3,
    cached_peer_ranges/5,
    sync_bucket_refresh_delay_ms/0
]).

-ifdef(AR_TEST).
-export([collect_peers/0, reset_all_caches/0, inflight_count/0]).
-endif.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_storage/include/arweave_storage.hrl").
-include_lib("arweave_sync/include/arweave_sync.hrl").

-include("arweave_sync_discovery.hrl").
-include_lib("arweave_sync/include/arweave_sync_deps.hrl").

%%%===================================================================
%%% State and data structures.
%%%===================================================================

%% Chunk interval cache rows are `{Key, Intervals, MonotonicMs}', and the
%% `{Mode, Peer, Location}' key keeps a peer's rows for one sync bucket
%% together. The timestamp drives the freshness checks and the trimming.

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%===================================================================
%%% Public interface: warming.
%%%===================================================================

%% @doc Queue a chunk interval job for each peer that advertises the sync
%% bucket at Offset but has no fresh cached intervals there.
warm_peer_ranges(StoreID, Peers, Offset) ->
    gen_server:call(
        ?MODULE, {warm_peer_ranges, StoreID, Peers, Offset}, infinity
    ).

%%%===================================================================
%%% Public interface: reads.
%%%===================================================================

%% @doc Return the peers that advertise the byte or footprint sync bucket
%% containing Offset; callers fetch chunks through /chunk2 in either case.
get_peers_for_offset(Offset) ->
    Peers = lists:foldl(
        fun(Mode, Acc) -> sets:union(Acc, get_peers_for_offset(Mode, Offset)) end,
        sets:new(),
        [byte, footprint]
    ),
    sets:to_list(Peers).

%% @doc Return the non-empty cached peer ranges, and `cache_miss' instead of
%% `ok' when a peer advertises the sync bucket in a mode but has no cached row
%% for that mode.
cached_peer_ranges(StoreID, Peers, Offset, RangeStart, RangeEnd) ->
    {PeerRanges, AnyCacheMiss} = lists:foldl(
        fun(Peer, Acc) ->
            cached_peer_ranges_for_peer(
                StoreID,
                Peer,
                Offset,
                RangeStart,
                RangeEnd,
                Acc
            )
        end,
        {[], false},
        Peers
    ),
    CacheStatus =
        case AnyCacheMiss of
            true -> cache_miss;
            false -> ok
        end,
    {lists:reverse(PeerRanges), CacheStatus}.

%%%===================================================================
%%% gen_server callbacks.
%%%===================================================================

init([]) ->
    %% arweave_sync_sup owns the discovery tables, so the caches survive a
    %% crash or restart of this server.
    ?DEP(clock):send_after(
        ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, self(), collect_peers
    ),
    schedule_sync_bucket_refresh(),
    %% The metrics read State, so maintenance runs in this server.
    ?DEP(clock):send_after(?MAINTENANCE_INTERVAL_MS, self(), maintenance),
    [ok, ok] = ?DEP(events):subscribe([peer, node_state]),
    {ok, #state{}}.

handle_call(inflight_count, _From, State) ->
    #state{jobs = JobsByKind} = State,
    NumInflight = maps:fold(
        fun(_Kind, Jobs, Acc) ->
            Acc + inflight_count(Jobs)
        end,
        0,
        JobsByKind
    ),
    {reply, NumInflight, State};
handle_call({warm_peer_ranges, StoreID, Peers, Offset}, _From, State) ->
    State2 = warm_peer_ranges(StoreID, Peers, Offset, State),
    {reply, ok, start_jobs(chunk_interval, State2)};
handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
    {reply, ok, State}.

handle_cast({add_peers, Peers}, State) ->
    State2 = lists:foldl(fun add_peer/2, State, Peers),
    {noreply, start_jobs(sync_bucket, State2)};
handle_cast({job_result, Peer, Result}, State) ->
    State2 =
        case is_peer_tracked(Peer, State) of
            true -> record_job_result(Peer, Result, State);
            false -> State
        end,
    {noreply, State2};
handle_cast({remove_peer, Peer, Reason}, State) ->
    do_remove_peer(Peer, Reason, State);
handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
    {noreply, State}.

handle_info({'DOWN', _, process, Pid, _Reason}, State) ->
    {noreply, finish_job(Pid, State)};
handle_info({event, peer, {removed, Peer}}, State) ->
    gen_server:cast(?MODULE, {remove_peer, Peer, peer_event_removed}),
    {noreply, State};
handle_info({event, peer, _}, State) ->
    {noreply, State};
handle_info({event, node_state, {initialized, _B}}, State) ->
    %% collect_peers/0 does nothing before the join, and the next periodic run
    %% could be up to ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS away, which
    %% would delay the first sync bucket jobs by minutes.
    collect_peers(),
    {noreply, State};
handle_info({event, node_state, _}, State) ->
    {noreply, State};
handle_info(collect_peers, State) ->
    ?DEP(clock):send_after(
        ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, self(), collect_peers
    ),
    collect_peers(),
    {noreply, State};
handle_info(refresh_sync_buckets, State) ->
    schedule_sync_bucket_refresh(),
    State2 = enqueue_sync_bucket_jobs(State),
    {noreply, start_jobs(sync_bucket, State2)};
handle_info(maintenance, State0) ->
    ?DEP(clock):send_after(?MAINTENANCE_INTERVAL_MS, self(), maintenance),
    State = start_jobs(chunk_interval, run_maintenance(State0)),
    {noreply, start_jobs(sync_bucket, State)};
handle_info(Message, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {message, Message}]),
    {noreply, State}.

terminate(Reason, State) ->
    #state{jobs = JobsByKind} = State,
    terminate_jobs(JobsByKind),
    ?LOG_INFO([{pid, self()}, {callback, terminate}, {reason, Reason}]),
    ok.

%%%===================================================================
%%% Peer collection.
%%%===================================================================

collect_peers() ->
    %% Wait for the join to finish so discovery traffic does not slow it down.
    case ?DEP(node):is_joined() of
        false ->
            ok;
        true ->
            LocalOnly = ?DEP(config):get([sync, local_peers_only]),
            Peers =
                case LocalOnly of
                    true ->
                        ?DEP(config):get([peers, local]);
                    false ->
                        %% Rank peers by their current rating, which reflects
                        %% recent throughput.
                        ?DEP(peers):get_peers(current)
                end,
            gen_server:cast(
                ?MODULE,
                {add_peers, lists:sublist(Peers, ?MAX_DISCOVERY_PEERS)}
            )
    end.

%% The caller starts the jobs after adding all current peers.
add_peer(Peer, State) ->
    #state{tracked_peers = TrackedPeers} = State,
    case sets:is_element(Peer, TrackedPeers) of
        true ->
            State;
        false ->
            State2 = State#state{
                tracked_peers = sets:add_element(Peer, TrackedPeers)
            },
            enqueue_job(sync_bucket_job(Peer), State2)
    end.

is_peer_tracked(Peer, State) ->
    #state{tracked_peers = TrackedPeers} = State,
    sets:is_element(Peer, TrackedPeers).

do_remove_peer(Peer, Reason, State) ->
    #state{
        tracked_peers = TrackedPeers,
        jobs = JobsByKind
    } = State,
    State2 = State#state{
        tracked_peers = sets:del_element(Peer, TrackedPeers),
        jobs = maps:map(
            fun(_Kind, Jobs) -> remove_jobs(Peer, Jobs) end,
            JobsByKind
        )
    },
    NumSyncBuckets = delete_rows(?SYNC_BUCKET_CACHE_TABLE, Peer),
    NumChunkIntervals = delete_rows(?CHUNK_INTERVAL_CACHE_TABLE, Peer),
    arweave_sync_metrics:count_chunk_interval_evictions(peer_removed,
        NumChunkIntervals),
    ?LOG_DEBUG([
        {event, peer_removed_from_discovery},
        {peer, arweave_lib_util:format_peer(Peer)},
        {reason, Reason},
        {had_sync_buckets, NumSyncBuckets > 0},
        {remaining_buckets_rows, ets:info(?SYNC_BUCKET_CACHE_TABLE, size)}
    ]),
    State3 = start_jobs(chunk_interval, State2),
    {noreply, start_jobs(sync_bucket, State3)}.

delete_rows(?SYNC_BUCKET_CACHE_TABLE = Table, Peer) ->
    ets:select_delete(
        Table,
        [{{{'_', '_', Peer}, '_', '_'}, [], [true]}]
    );
delete_rows(?CHUNK_INTERVAL_CACHE_TABLE = Table, Peer) ->
    ets:select_delete(
        Table,
        [{{{'_', Peer, '_'}, '_', '_'}, [], [true]}]
    ).

%%%===================================================================
%%% Sync bucket refresh.
%%%===================================================================

enqueue_sync_bucket_jobs(#state{tracked_peers = TrackedPeers} = State) ->
    sets:fold(
        fun(Peer, Acc) -> enqueue_job(sync_bucket_job(Peer), Acc) end,
        State,
        TrackedPeers
    ).

sync_bucket_job(Peer) ->
    #discovery_job{
        key = {sync_bucket, Peer},
        kind = sync_bucket,
        peer = Peer
    }.

%% @doc Schedule the next node-wide refresh with +/-25% jitter so nodes that
%% start together do not repeatedly query peers at the same time.
schedule_sync_bucket_refresh() ->
    {ok, _} = ?DEP(clock):send_after(
        arweave_sync_discovery:sync_bucket_refresh_delay_ms(),
        self(), refresh_sync_buckets
    ),
    ok.

%% @doc Return the sync bucket refresh delay with +/-25% jitter.
sync_bucket_refresh_delay_ms() ->
    Spread = ?SYNC_BUCKET_JOB_INTERVAL_MS div 4,
    ?SYNC_BUCKET_JOB_INTERVAL_MS - Spread + rand:uniform(2 * Spread + 1) - 1.

%%%===================================================================
%%% Sync bucket refresh: job.
%%%===================================================================

%% @doc Fetch the peer's byte sync buckets and then its footprint sync buckets,
%% so the job has one request open to the peer at a time.
fetch_sync_buckets(Peer) ->
    fetch_sync_buckets(byte, Peer),
    fetch_sync_buckets(footprint, Peer).

fetch_sync_buckets(Mode, Peer) ->
    try
        maybe
            true ?= peer_supports(Mode, Peer),
            {ok, SyncBuckets} ?= get_sync_buckets(Mode, Peer),
            gen_server:cast(
                ?MODULE,
                {job_result, Peer, {sync_buckets, Mode, SyncBuckets}}
            )
        else
            false ->
                ok;
            {error, request_type_not_found} ->
                ?LOG_DEBUG([
                    {event, sync_buckets_request_type_not_found},
                    {peer, arweave_lib_util:format_peer(Peer)},
                    {mode, Mode}
                ]),
                error;
            {error, RequestError} ->
                ?DEP(http):log_failed_request(
                    RequestError,
                    [
                        {event, failed_to_fetch_sync_buckets},
                        {peer, arweave_lib_util:format_peer(Peer)},
                        {mode, Mode},
                        {reason, io_lib:format("~p", [RequestError])}
                    ]
                ),
                error;
            Other ->
                ?LOG_DEBUG([
                    {event, failed_to_fetch_sync_buckets},
                    {peer, arweave_lib_util:format_peer(Peer)},
                    {mode, Mode},
                    {reason, io_lib:format("~p", [Other])}
                ]),
                error
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_WARNING([
                {event, refresh_sync_buckets_failed},
                {peer, arweave_lib_util:format_peer(Peer)},
                {mode, Mode},
                {class, Class},
                {reason, io_lib:format("~p", [Reason])},
                {stacktrace, Stacktrace}
            ])
    end,
    ok.

get_sync_buckets(byte, Peer) ->
    ?DEP(http):get_sync_buckets(Peer);
get_sync_buckets(footprint, Peer) ->
    ?DEP(http):get_footprint_buckets(Peer).

%%%===================================================================
%%% Sync bucket refresh: cache.
%%%===================================================================

store_sync_buckets(Mode, Peer, SyncBuckets) ->
    ?DEP(sync_buckets):foreach(
        fun(SyncBucket, Share) ->
            mark_chunk_intervals_stale_on_share_change(
                Mode, Peer, SyncBucket, Share
            ),
            store_row(sync_bucket, Mode, SyncBucket, Peer, Share)
        end,
        sync_bucket_size(Mode),
        sync_bucket_range_end(Mode, ?DEP(node):get_weave_size()),
        SyncBuckets
    ),
    ok.

sync_bucket_size(byte) ->
    ?DEP(sync_buckets):get_network_data_bucket_size();
sync_bucket_size(footprint) ->
    ?DEP(sync_buckets):get_network_footprint_bucket_size().

sync_bucket_range_end(byte, WeaveSize) ->
    WeaveSize;
sync_bucket_range_end(footprint, WeaveSize) ->
    arweave_lib_footprint:max_footprint_offset(WeaveSize).

mark_chunk_intervals_stale_on_share_change(
    Mode, Peer, SyncBucket, NewShare
) ->
    Key = sync_bucket_key(Mode, SyncBucket, Peer),
    case ets:lookup(?SYNC_BUCKET_CACHE_TABLE, Key) of
        [{_, NewShare, _}] ->
            ok;
        [] ->
            ok;
        [{_, _ChangedShare, _}] ->
            mark_chunk_intervals_stale(Mode, Peer, SyncBucket)
    end.

mark_chunk_intervals_stale(Mode, Peer, SyncBucket) ->
    StaleTimestamp =
        expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS) - 1,
    lists:foreach(
        fun(Key) ->
            ets:update_element(
                ?CHUNK_INTERVAL_CACHE_TABLE, Key, {3, StaleTimestamp}
            )
        end,
        chunk_interval_keys_in_bucket(Mode, Peer, SyncBucket)
    ).

%%%===================================================================
%%% Warming.
%%%===================================================================

warm_peer_ranges(StoreID, Peers, Offset, State) ->
    lists:foldl(
        fun(Peer, Acc) ->
            Acc2 = warm_peer_range(byte, StoreID, Peer, Offset, Acc),
            warm_peer_range(footprint, StoreID, Peer, Offset, Acc2)
        end,
        State,
        Peers
    ).

warm_peer_range(Mode, StoreID, Peer, Offset, State) ->
    maybe
        true ?= peer_supports(Mode, Peer),
        true ?= peer_has_sync_bucket(Mode, Peer, Offset),
        true ?= chunk_interval_refresh_needed(Mode, Peer, Offset),
        PeerRange = #peer_range{
            store_id = StoreID,
            offset = Offset,
            peer = Peer
        },
        enqueue_job(chunk_interval_job(Mode, PeerRange), State)
    else
        false ->
            State
    end.

chunk_interval_refresh_needed(Mode, Peer, Offset) ->
    Key = chunk_interval_key(Mode, Peer, Offset),
    %% Read only the timestamp, so the intervals are not copied out of ETS.
    case ets:lookup_element(?CHUNK_INTERVAL_CACHE_TABLE, Key, 3, undefined) of
        undefined -> true;
        MonotonicMs ->
            MonotonicMs < expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS)
    end.

chunk_interval_job(Mode, PeerRange) ->
    #peer_range{
        peer = Peer,
        store_id = StoreID,
        offset = Offset
    } = PeerRange,
    Start =
        case Mode of
            byte -> interval_location(byte, Offset);
            footprint -> Offset
        end,
    #discovery_job{
        key = {chunk_interval, Peer, StoreID, Mode, Start},
        kind = chunk_interval,
        peer = Peer,
        store_id = StoreID,
        mode = Mode,
        start = Start
    }.

%%%===================================================================
%%% Warming: job.
%%%===================================================================

%% @doc Fetch the peer's chunk intervals at the job's location, if the peer
%% still advertises the sync bucket and the cached row needs a refresh.
refresh_chunk_intervals(ChunkIntervalJob) ->
    #discovery_job{
        kind = chunk_interval,
        peer = Peer,
        store_id = StoreID,
        mode = Mode,
        start = Start
    } = ChunkIntervalJob,
    try
        maybe
            true ?= peer_has_sync_bucket(Mode, Peer, Start),
            true ?= chunk_interval_refresh_needed(Mode, Peer, Start),
            Location = interval_location(Mode, Start),
            Result = fetch_chunk_intervals(Mode, Peer, Location),
            gen_server:cast(
                ?MODULE,
                {job_result, Peer, {chunk_intervals, StoreID, Start, Mode, Result}}
            )
        else
            false ->
                ok
        end
    catch
        Class:Reason:Stacktrace ->
            ?LOG_WARNING([
                {event, chunk_interval_job_crashed},
                {class, Class},
                {peer, arweave_lib_util:format_peer(Peer)},
                {mode, Mode},
                {reason, io_lib:format("~p", [Reason])},
                {stacktrace, Stacktrace}
            ])
    end.

fetch_chunk_intervals(byte, Peer, Left) ->
    Right = Left + arweave_sync_cursor:query_range_step_size(),
    %% Peers older than GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE do not
    %% support a right bound and return an open-ended range, which
    %% collect_chunk_interval_page/4 cuts at Right once paging ends.
    Bound =
        case
            ?DEP(peers):get_peer_release(Peer) >=
                ?GET_SYNC_RECORD_RIGHT_BOUND_SUPPORT_RELEASE
        of
            true -> Right;
            false -> none
        end,
    fetch_chunk_interval_pages(
        Peer,
        Bound,
        Left,
        Right,
        ?MAX_CHUNK_INTERVAL_PAGES,
        arweave_lib_intervals:new()
    );
fetch_chunk_intervals(footprint, Peer, {Partition, Footprint}) ->
    case ?DEP(http):get_footprints(Peer, Partition, Footprint) of
        not_found ->
            {ok, arweave_lib_intervals:new()};
        Reply ->
            Reply
    end.

fetch_chunk_interval_pages(
    _Peer, _Bound, _Left, _Right, 0, _Intervals
) ->
    {error, interval_page_limit};
fetch_chunk_interval_pages(
    Peer, Bound, Left, Right, PagesLeft, Intervals
) ->
    case
        fetch_sync_record(
            Peer, Left + 1, Bound, ?QUERY_SYNC_INTERVALS_COUNT_LIMIT
        )
    of
        {ok, Page} ->
            case collect_chunk_interval_page(Left, Right, Intervals, Page) of
                {continue, PageEnd, Intervals2} ->
                    fetch_chunk_interval_pages(
                        Peer,
                        Bound,
                        PageEnd,
                        Right,
                        PagesLeft - 1,
                        Intervals2
                    );
                Result ->
                    Result
            end;
        Error ->
            Error
    end.

fetch_sync_record(Peer, Start, none, Limit) ->
    ?DEP(http):get_sync_record(Peer, Start, Limit);
fetch_sync_record(Peer, Start, Right, Limit) ->
    ?DEP(http):get_sync_record(Peer, Start, Right, Limit).

%% Paging stops once a page reaches Right or holds fewer than
%% QUERY_SYNC_INTERVALS_COUNT_LIMIT intervals, because the peer then has
%% nothing more to give. ar_http_iface_client rejects pages that start before
%% the requested cursor.
collect_chunk_interval_page(_Left, Right, Intervals, Page) ->
    case arweave_lib_intervals:count(Page) of
        0 ->
            {ok, arweave_lib_intervals:cut(Intervals, Right)};
        Count ->
            {PageEnd, _PageStart} = arweave_lib_intervals:largest(Page),
            Intervals2 = arweave_lib_intervals:union(Intervals, Page),
            case
                PageEnd >= Right orelse
                    Count < ?QUERY_SYNC_INTERVALS_COUNT_LIMIT
            of
                true ->
                    {ok, arweave_lib_intervals:cut(Intervals2, Right)};
                false ->
                    {continue, PageEnd, Intervals2}
            end
    end.

%%%===================================================================
%%% Reads: sync buckets.
%%%===================================================================

get_peers_for_offset(Mode, Offset) ->
    get_peers_for_sync_bucket(Mode, sync_bucket(Mode, Offset)).

get_peers_for_sync_bucket(Mode, SyncBucket) ->
    get_peers_for_sync_bucket(Mode, SyncBucket, sets:new()).

get_peers_for_sync_bucket(Mode, SyncBucket, Peers) ->
    %% no_peer sorts before every peer tuple, so the walk starts at the first
    %% row for SyncBucket.
    get_peers_for_sync_bucket(
        Mode,
        SyncBucket,
        Peers,
        sync_bucket_key(Mode, SyncBucket, no_peer)
    ).

get_peers_for_sync_bucket(Mode, SyncBucket, Peers, Cursor) ->
    case ets:next(?SYNC_BUCKET_CACHE_TABLE, Cursor) of
        {Mode, SyncBucket, Peer} = Key ->
            get_peers_for_sync_bucket(
                Mode, SyncBucket, sets:add_element(Peer, Peers), Key
            );
        _ ->
            Peers
    end.

%%%===================================================================
%%% Reads: chunk intervals.
%%%===================================================================

cached_peer_ranges_for_peer(
    StoreID,
    Peer,
    Offset,
    RangeStart,
    RangeEnd,
    Acc
) ->
    BasePeerRange = #peer_range{
        store_id = StoreID,
        offset = Offset,
        peer = Peer,
        intervals = arweave_lib_intervals:new()
    },
    Acc2 = cached_peer_range(byte, BasePeerRange, RangeStart, RangeEnd, Acc),
    cached_peer_range(
        footprint, BasePeerRange, RangeStart, RangeEnd, Acc2
    ).

cached_peer_range(
    Mode,
    BasePeerRange,
    RangeStart,
    RangeEnd,
    {PeerRanges, AnyCacheMiss}
) ->
    #peer_range{peer = Peer, offset = Offset, store_id = StoreID} =
        BasePeerRange,
    maybe
        true ?= peer_supports(Mode, Peer),
        true ?= peer_has_sync_bucket(Mode, Peer, Offset),
        {_Freshness, Intervals} ?=
            get_chunk_intervals(
                Mode, Peer, Offset, RangeStart, RangeEnd
            ),
        true ?= not arweave_lib_intervals:is_empty(Intervals),
        Location = interval_location(Mode, Offset),
        PeerRange = BasePeerRange#peer_range{
            intervals = Intervals,
            footprint = footprint_key(Mode, StoreID, Location)
        },
        {[PeerRange | PeerRanges], AnyCacheMiss}
    else
        false ->
            {PeerRanges, AnyCacheMiss};
        cache_miss ->
            {PeerRanges, true}
    end.

%% The key leaves out the peer: the scheduler assigns one peer per footprint
%% and tracks that peer separately (#peer_range.peer, #task.peer), so a peer
%% in the key would only let the same footprint appear under two keys.
footprint_key(byte, _StoreID, _Location) ->
    none;
footprint_key(footprint, StoreID, {Partition, Footprint}) ->
    #footprint{
        store_id = StoreID,
        partition = Partition,
        footprint = Footprint
    }.

get_chunk_intervals(_Mode, _Peer, _Offset, RangeStart, RangeEnd) when
    RangeStart >= RangeEnd
->
    {ok, arweave_lib_intervals:new()};
get_chunk_intervals(Mode, Peer, Offset, RangeStart, RangeEnd) ->
    case chunk_interval_lookup(Mode, Peer, Offset) of
        miss ->
            cache_miss;
        {Freshness, Intervals} ->
            Intervals2 = clip_chunk_intervals(
                Mode, Intervals, RangeStart, RangeEnd
            ),
            case Freshness of
                hit -> {ok, Intervals2};
                stale -> {stale, Intervals2}
            end
    end.

%% Footprint metadata stays in footprint-record space, where a run of chunks
%% is one interval; the chunk picker converts only the chunks a store needs.
clip_chunk_intervals(byte, Intervals, RangeStart, RangeEnd) ->
    ByteRange = arweave_lib_intervals:from_list([{RangeEnd, RangeStart}]),
    arweave_lib_intervals:intersection(Intervals, ByteRange);
clip_chunk_intervals(footprint, Intervals, _RangeStart, _RangeEnd) ->
    Intervals.

chunk_interval_lookup(Mode, Peer, Offset) ->
    Key = chunk_interval_key(Mode, Peer, Offset),
    case ets:lookup(?CHUNK_INTERVAL_CACHE_TABLE, Key) of
        [{_, Intervals, MonotonicMs}] ->
            Cutoff = expiration_cutoff(?CHUNK_INTERVAL_CACHE_TTL_MS),
            %% Callers still use stale intervals, since old data is better
            %% than stalling on a miss. Rows mostly go stale when a bucket's
            %% share changes; this TTL is a backstop.
            case MonotonicMs < Cutoff of
                true -> {stale, Intervals};
                false -> {hit, Intervals}
            end;
        [] ->
            miss
    end.

%%%===================================================================
%%% Maintenance.
%%%===================================================================

run_maintenance(State) ->
    delete_expired_sync_buckets(),
    trim_chunk_interval_cache(),
    emit_metrics(State),
    State.

delete_expired_sync_buckets() ->
    %% A row expires when the peer stops advertising the bucket or stops
    %% responding.
    Cutoff = expiration_cutoff(?SYNC_BUCKET_CACHE_TTL_MS),
    Expired = ets:select(
        ?SYNC_BUCKET_CACHE_TABLE,
        [{{'$1', '_', '$2'}, [{'<', '$2', Cutoff}], ['$1']}]
    ),
    lists:foreach(
        fun({Mode, SyncBucket, Peer}) ->
            delete_row(sync_bucket, Mode, SyncBucket, Peer),
            delete_chunk_intervals(Mode, Peer, SyncBucket)
        end,
        Expired
    ),
    case Expired of
        [] ->
            ok;
        _ ->
            ?LOG_DEBUG([
                {event, deleted_expired_sync_buckets},
                {count, length(Expired)}
            ])
    end.

delete_chunk_intervals(Mode, Peer, SyncBucket) ->
    Keys = chunk_interval_keys_in_bucket(Mode, Peer, SyncBucket),
    lists:foreach(
        fun(Key) -> ets:delete(?CHUNK_INTERVAL_CACHE_TABLE, Key) end, Keys
    ),
    arweave_sync_metrics:count_chunk_interval_evictions(withdrawn, length(Keys)).

%% This runs only from maintenance, because ETS memory figures lag behind
%% deletions: a check after every stored result could trim the cache several
%% times before the figures catch up.
trim_chunk_interval_cache() ->
    Bytes =
        ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory) *
            erlang:system_info(wordsize),
    MaxBytes = ?DEP(chunk_cache):interval_limit(),
    case Bytes > MaxBytes of
        false ->
            ok;
        true ->
            Size = ets:info(?CHUNK_INTERVAL_CACHE_TABLE, size),
            ToDelete = max(1, Size div ?CHUNK_INTERVAL_CACHE_TRIM_DIVISOR),
            Timestamps = ets:select(
                ?CHUNK_INTERVAL_CACHE_TABLE,
                [{{'_', '_', '$1'}, [], ['$1']}]
            ),
            Sorted = lists:sort(Timestamps),
            Threshold = lists:nth(min(ToDelete, length(Sorted)), Sorted),
            Deleted = ets:select_delete(
                ?CHUNK_INTERVAL_CACHE_TABLE,
                [{{'_', '_', '$1'}, [{'=<', '$1', Threshold}], [true]}]
            ),
            arweave_sync_metrics:count_chunk_interval_evictions(trim, Deleted),
            MbAfter =
                ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory) *
                    erlang:system_info(wordsize) div ?MiB,
            ?LOG_DEBUG([
                {event, chunk_interval_cache_trimmed},
                {rows_before, Size},
                {rows_deleted, Deleted},
                {rows_after, Size - Deleted},
                {mb_before, Bytes div ?MiB},
                {mb_after, MbAfter},
                {cap_mb, MaxBytes div ?MiB}
            ])
    end.

%%%===================================================================
%%% Maintenance: metrics.
%%%===================================================================

emit_metrics(State) ->
    #state{tracked_peers = TrackedPeers} = State,
    arweave_sync_metrics:publish_discovery(
        sets:size(TrackedPeers),
        ets:info(?CHUNK_INTERVAL_CACHE_TABLE, size),
        ets:info(?CHUNK_INTERVAL_CACHE_TABLE, memory) *
            erlang:system_info(wordsize),
        lists:flatmap(
            fun(Module) -> get_peers_for_store(Module, [byte, footprint]) end,
            ?DEP(config):storage_modules()
        )
    ).

get_peers_for_store(Module, Modes) ->
    #store_info{
        id = StoreID, effective_range = {RangeStart, RangeEnd}
    } = ?DEP(storage):store_info(Module),
    [
        {Mode, StoreID, get_peers_for_range(Mode, RangeStart, RangeEnd)}
     || Mode <- Modes
    ].

get_peers_for_range(Mode, RangeStart, RangeEnd) ->
    {StartSyncBucket, EndSyncBucket} =
        sync_bucket_range(Mode, RangeStart, RangeEnd),
    get_peers_for_sync_bucket_range(
        Mode, StartSyncBucket, EndSyncBucket, sets:new()
    ).

sync_bucket_range(byte, RangeStart, RangeEnd) ->
    {sync_bucket(byte, RangeStart), sync_bucket(byte, RangeEnd - 1)};
sync_bucket_range(footprint, RangeStart, RangeEnd) ->
    {sync_bucket(footprint, RangeStart), sync_bucket(footprint, RangeEnd - ?DATA_CHUNK_SIZE)}.

get_peers_for_sync_bucket_range(
    _Mode, StartSyncBucket, EndSyncBucket, Peers
) when
    StartSyncBucket > EndSyncBucket
->
    Peers;
get_peers_for_sync_bucket_range(
    Mode, StartSyncBucket, EndSyncBucket, Peers
) ->
    collect_sync_bucket_peers(
        Mode,
        EndSyncBucket,
        Peers,
        sync_bucket_key(Mode, StartSyncBucket, no_peer)
    ).

%% Walk the whole bucket range in one pass. Rows are keyed by
%% {Mode, SyncBucket, Peer}, so ets:next skips straight to the next bucket
%% with rows. Most buckets in a store's range have none, so the walk costs
%% far less than a lookup per bucket.
collect_sync_bucket_peers(Mode, EndSyncBucket, Peers, Cursor) ->
    case ets:next(?SYNC_BUCKET_CACHE_TABLE, Cursor) of
        {Mode, SyncBucket, Peer} = Key when SyncBucket =< EndSyncBucket ->
            collect_sync_bucket_peers(
                Mode, EndSyncBucket, sets:add_element(Peer, Peers), Key
            );
        _ ->
            Peers
    end.

%%%===================================================================
%%% Jobs: queueing.
%%%===================================================================

%% @doc Queue Job, within the pending limit, if its peer is tracked and no job
%% with the same key is pending or running.
enqueue_job(Job, State) ->
    #discovery_job{peer = Peer} = Job,
    case is_peer_tracked(Peer, State) of
        true -> enqueue_tracked_job(Job, State);
        false -> State
    end.

enqueue_tracked_job(#discovery_job{key = Key, kind = Kind} = Job, State) ->
    #state{jobs = JobsByKind} = State,
    Jobs = maps:get(Kind, JobsByKind),
    Jobs2 =
        case job_exists(Key, State) of
            true ->
                refresh_pending_job(Job, Jobs);
            false ->
                enqueue_new_job(Job, Jobs)
        end,
    State#state{jobs = JobsByKind#{Kind := Jobs2}}.

job_exists(Key, State) ->
    Jobs = jobs_for_key(Key, State),
    #discovery_jobs{
        pending = Pending,
        inflight = Inflight
    } = Jobs,
    PendingMatch =
        case maps:get(Key, Pending, undefined) of
            #discovery_job{key = Key} -> true;
            _ -> false
        end,
    PendingMatch orelse maps:is_key(Key, Inflight).

jobs_for_key(Key, #state{jobs = JobsByKind}) ->
    maps:get(element(1, Key), JobsByKind).

enqueue_new_job(Job, Jobs) ->
    #discovery_job{key = Key} = Job,
    #discovery_jobs{pending = Pending} = Jobs,
    case has_pending_capacity(Job, Jobs) of
        true ->
            Jobs#discovery_jobs{
                pending = maps:put(Key, Job, Pending)
            };
        false ->
            Jobs
    end.

refresh_pending_job(
    #discovery_job{key = Key} = Job,
    #discovery_jobs{pending = Pending} = Jobs
) ->
    case maps:is_key(Key, Pending) of
        true ->
            RefreshedJob = Job#discovery_job{
                requested_at = ?DEP(clock):monotonic_ms()
            },
            Jobs#discovery_jobs{
                pending = maps:put(Key, RefreshedJob, Pending)
            };
        false ->
            Jobs
    end.

has_pending_capacity(
    #discovery_job{kind = chunk_interval, store_id = StoreID},
    #discovery_jobs{
        pending = Pending,
        max_pending = MaxPending
    }
) ->
    map_size(Pending) < MaxPending orelse
        pending_job_count(StoreID, Pending) <
            ?MIN_PENDING_CHUNK_INTERVAL_JOBS_PER_STORE;
has_pending_capacity(_Job, #discovery_jobs{
    pending = Pending,
    max_pending = MaxPending
}) ->
    map_size(Pending) < MaxPending.

pending_job_count(StoreID, Pending) ->
    maps:fold(
        fun(_Key, #discovery_job{store_id = PendingStoreID}, Count)
                    when PendingStoreID =:= StoreID ->
                Count + 1;
           (_Key, _Job, Count) ->
                Count
        end,
        0,
        Pending
    ).

%%%===================================================================
%%% Jobs: starting.
%%%===================================================================

start_jobs(Kind, State) ->
    #state{jobs = JobsByKind} = State,
    Jobs = maps:get(Kind, JobsByKind),
    Jobs2 = start_jobs(Jobs),
    State#state{jobs = JobsByKind#{Kind := Jobs2}}.

start_jobs(Jobs) ->
    case has_capacity(Jobs) of
        false ->
            Jobs;
        true ->
            {PeerLoad, StoreLoad} = compute_job_load(Jobs),
            PeerStoreModeCounts = inflight_peer_store_mode_counts(Jobs),
            start_pending_jobs(
                Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts
            )
    end.

start_pending_jobs(Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts) ->
    case has_capacity(Jobs) of
        true ->
            case
                take_next_job(
                    Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts
                )
            of
                {ok,
                    #discovery_job{
                        peer = Peer,
                        store_id = StoreID
                    } = Job,
                    Jobs2} ->
                    Jobs3 = start_job(Job, Jobs2),
                    PeerStoreMode = peer_store_mode_key(
                        Job#discovery_job.key
                    ),
                    start_pending_jobs(
                        Jobs3,
                        arweave_lib_util:increment_map_value(Peer, PeerLoad),
                        arweave_lib_util:increment_map_value(
                            StoreID, StoreLoad
                        ),
                        arweave_lib_util:increment_map_value(
                            PeerStoreMode, PeerStoreModeCounts
                        )
                    );
                none ->
                    Jobs
            end;
        false ->
            Jobs
    end.

inflight_peer_store_mode_counts(#discovery_jobs{inflight = Inflight}) ->
    maps:fold(
        fun(Key, _Job, Acc) ->
            arweave_lib_util:increment_map_value(peer_store_mode_key(Key), Acc)
        end,
        #{},
        Inflight
    ).

peer_store_mode_key({chunk_interval, Peer, StoreID, Mode, _Start}) ->
    {Peer, StoreID, Mode};
peer_store_mode_key(Key) ->
    Key.

has_capacity(#discovery_jobs{max_inflight = MaxInflight} = Jobs) ->
    inflight_count(Jobs) < MaxInflight.

inflight_count(#discovery_jobs{inflight = Inflight}) ->
    map_size(Inflight).

compute_job_load(#discovery_jobs{inflight = Inflight}) ->
    maps:fold(
        fun(_Key, Job, {PeerLoad, StoreLoad}) ->
            #discovery_job{
                peer = Peer,
                store_id = StoreID
            } = Job,
            {
                arweave_lib_util:increment_map_value(Peer, PeerLoad),
                arweave_lib_util:increment_map_value(StoreID, StoreLoad)
            }
        end,
        {#{}, #{}},
        Inflight
    ).

take_next_job(Jobs, PeerLoad, StoreLoad, PeerStoreModeCounts) ->
    #discovery_jobs{pending = Pending} = Jobs,
    RunnableByPeerStoreMode = maps:fold(
        fun(PendingKey, Job, Acc) ->
            PeerStoreMode = peer_store_mode_key(PendingKey),
            case
                maps:get(PeerStoreMode, PeerStoreModeCounts, 0) <
                    max_jobs_per_peer_store_mode(PeerStoreMode)
            of
                true ->
                    Candidate = {PendingKey, Job},
                    maps:update_with(
                        PeerStoreMode,
                        fun(Existing) ->
                            preferred_pending_job(Candidate, Existing)
                        end,
                        Candidate,
                        Acc
                    );
                false ->
                    Acc
            end
        end,
        #{},
        Pending
    ),
    case maps:values(RunnableByPeerStoreMode) of
        [] ->
            none;
        PendingJobs ->
            %% Prefer the peer with the fewest running jobs, then the store
            %% with the fewest. The job key breaks ties, so the choice is
            %% deterministic.
            RankedJobs = lists:map(
                fun({Key, Job}) ->
                    {job_load(Job, PeerLoad, StoreLoad), Key, Job}
                end,
                PendingJobs
            ),
            {_Load, SelectedKey, Selected} = lists:min(RankedJobs),
            {ok, Selected, Jobs#discovery_jobs{
                pending = maps:remove(SelectedKey, Pending)
            }}
    end.

max_jobs_per_peer_store_mode({_Peer, _StoreID, footprint}) ->
    ?MAX_FOOTPRINT_JOBS_PER_PEER_STORE;
max_jobs_per_peer_store_mode(_PeerStoreMode) ->
    ?MAX_BYTE_JOBS_PER_PEER_STORE.

preferred_pending_job(
    {Key1, Job1} = Candidate1,
    {Key2, Job2} = Candidate2
) ->
    case
        pending_job_priority(Job1, Key1) >=
            pending_job_priority(Job2, Key2)
    of
        true -> Candidate1;
        false -> Candidate2
    end.

pending_job_priority(
    #discovery_job{
        kind = chunk_interval,
        requested_at = undefined,
        start = Start
    },
    _Key
) ->
    {0, 0, -Start};
pending_job_priority(
    #discovery_job{
        kind = chunk_interval,
        requested_at = RequestedAt,
        start = Start
    },
    _Key
) ->
    {1, RequestedAt, -Start};
pending_job_priority(_Job, Key) ->
    {0, 0, Key}.

job_load(Job, PeerLoad, StoreLoad) ->
    #discovery_job{peer = Peer, store_id = StoreID} = Job,
    {maps:get(Peer, PeerLoad, 0), maps:get(StoreID, StoreLoad, 0)}.

start_job(Job, Jobs) ->
    #discovery_job{key = Key} = Job,
    #discovery_jobs{inflight = Inflight} = Jobs,
    {Pid, _Ref} = spawn_monitor(fun() -> run_job(Job) end),
    Jobs#discovery_jobs{
        inflight = maps:put(Key, Job#discovery_job{pid = Pid}, Inflight)
    }.

run_job(#discovery_job{kind = sync_bucket, peer = Peer}) ->
    fetch_sync_buckets(Peer);
run_job(#discovery_job{kind = chunk_interval} = Job) ->
    refresh_chunk_intervals(Job).

%%%===================================================================
%%% Jobs: completion.
%%%===================================================================

record_job_result(Peer, {sync_buckets, Mode, SyncBuckets}, State) ->
    store_sync_buckets(Mode, Peer, SyncBuckets),
    ?LOG_DEBUG([
        {event, processed_sync_buckets},
        {peer, arweave_lib_util:format_peer(Peer)},
        {mode, Mode}
    ]),
    State;
record_job_result(
    Peer,
    {chunk_intervals, _StoreID, Offset, Mode, {ok, Intervals}},
    State
) ->
    store_row(chunk_interval, Mode, Offset, Peer, Intervals),
    State;
record_job_result(
    Peer,
    {chunk_intervals, _StoreID, Offset, Mode, _Error},
    State
) ->
    %% The fetch failed, so stop serving the old row. A later sweep will
    %% request the intervals again.
    delete_row(chunk_interval, Mode, Offset, Peer),
    State.

finish_job(Pid, State) ->
    #state{jobs = JobsByKind} = State,
    Result = maps:fold(
        fun(_Kind, _Jobs, {ok, _, _} = Acc) ->
                Acc;
           (Kind, Jobs, error) ->
                #discovery_jobs{inflight = Inflight} = Jobs,
                case pid_to_job(Pid, Inflight) of
                    {ok, Key} ->
                        Jobs2 = Jobs#discovery_jobs{
                            inflight = maps:remove(Key, Inflight)
                        },
                        {ok, Kind, Jobs2};
                    error ->
                        error
                end
        end,
        error,
        JobsByKind
    ),
    case Result of
        {ok, Kind, Jobs2} ->
            Jobs3 = start_jobs(Jobs2),
            State#state{jobs = JobsByKind#{Kind := Jobs3}};
        error ->
            State
    end.

pid_to_job(Pid, Inflight) ->
    case
        lists:search(
            fun({_Key, #discovery_job{pid = JobPid}}) ->
                JobPid =:= Pid
            end,
            maps:to_list(Inflight)
        )
    of
        {value, {Key, _Job}} -> {ok, Key};
        false -> error
    end.

%%%===================================================================
%%% Jobs: termination.
%%%===================================================================

remove_jobs(Peer, Jobs) ->
    #discovery_jobs{
        pending = Pending,
        inflight = Inflight
    } = Jobs,
    Jobs#discovery_jobs{
        pending = maps:filter(
            fun(_Key, #discovery_job{peer = JobPeer}) ->
                JobPeer =/= Peer
            end,
            Pending
        ),
        inflight = maps:filter(
            fun(_Key, #discovery_job{peer = JobPeer, pid = JobPID})
                        when JobPeer =:= Peer ->
                    terminate_job(JobPID),
                    false;
               (_Key, _Job) ->
                    true
            end,
            Inflight
        )
    }.

terminate_jobs(JobsByKind) ->
    maps:foreach(
        fun(_Kind, #discovery_jobs{inflight = Inflight}) ->
            maps:foreach(
                fun(_Key, #discovery_job{pid = JobPID}) ->
                    terminate_job(JobPID)
                end,
                Inflight
            )
        end,
        JobsByKind
    ).

terminate_job(JobPID) ->
    MonitorRef = erlang:monitor(process, JobPID),
    exit(JobPID, kill),
    receive
        {'DOWN', MonitorRef, process, JobPID, _Reason} -> ok
    end.

%%%===================================================================
%%% Helpers: peers.
%%%===================================================================

peer_supports(byte, _Peer) ->
    true;
peer_supports(footprint, Peer) ->
    ?DEP(peers):get_peer_release(Peer) >= ?GET_FOOTPRINT_SUPPORT_RELEASE.

peer_has_sync_bucket(Mode, Peer, Offset) ->
    ets:member(
        ?SYNC_BUCKET_CACHE_TABLE,
        sync_bucket_key(Mode, sync_bucket(Mode, Offset), Peer)
    ).

%%%===================================================================
%%% Helpers: cache keys.
%%%===================================================================

sync_bucket(byte, Offset) ->
    Offset div ?NETWORK_DATA_BUCKET_SIZE;
sync_bucket(footprint, Offset) ->
    arweave_lib_footprint:get_footprint_bucket(Offset + ?DATA_CHUNK_SIZE).

%% @doc Return a sync bucket cache key, with Mode first because byte and
%% footprint buckets are numbered separately, and Peer last so that lookups
%% can walk all peers' rows for one bucket in order.
sync_bucket_key(Mode, SyncBucket, Peer) ->
    {Mode, SyncBucket, Peer}.

interval_location(byte, Offset) ->
    Step = arweave_sync_cursor:query_range_step_size(),
    (Offset div Step) * Step;
interval_location(footprint, Offset) ->
    arweave_lib_footprint:get_footprint_location(Offset + ?DATA_CHUNK_SIZE).

%% @doc Return the key of the peer's chunk interval row covering Offset, with
%% Peer before the location so that one peer's rows for a bucket are adjacent.
chunk_interval_key(Mode, Peer, Offset) ->
    {Mode, Peer, interval_location(Mode, Offset)}.

chunk_interval_location_range(byte, SyncBucket) ->
    Lo = SyncBucket * ?NETWORK_DATA_BUCKET_SIZE,
    {Lo, Lo + ?NETWORK_DATA_BUCKET_SIZE - 1};
chunk_interval_location_range(footprint, SyncBucket) ->
    %% {Partition, Footprint} locations sort in the same order as the linear
    %% footprint layout (arweave_lib_footprint:get_footprint_offset/1), so a
    %% peer's rows for one bucket are adjacent. Bucket edges do not line up
    %% with footprint edges, so the range can include one extra footprint at
    %% each end; those rows are marked stale or deleted along with the
    %% bucket.
    FootprintSize =
        arweave_lib_constants:get_sub_chunks_per_replica_2_9_entropy(),
    FootprintsPerPartition =
        arweave_lib_constants:get_replica_2_9_footprints_per_partition(),
    LoGlobal = (SyncBucket * ?NETWORK_FOOTPRINT_BUCKET_SIZE) div FootprintSize,
    HiGlobal = ((SyncBucket + 1) * ?NETWORK_FOOTPRINT_BUCKET_SIZE) div FootprintSize,
    {{LoGlobal div FootprintsPerPartition, LoGlobal rem FootprintsPerPartition}, {
        HiGlobal div FootprintsPerPartition, HiGlobal rem FootprintsPerPartition
    }}.

chunk_interval_keys_in_bucket(Mode, Peer, SyncBucket) ->
    %% The keys form one contiguous range, so walk only that range. A peer's
    %% bucket refresh can change thousands of shares, and each change calls
    %% this function, so scanning every row of the mode would stall the
    %% server for minutes.
    {Lo, Hi} = chunk_interval_location_range(Mode, SyncBucket),
    First = {Mode, Peer, Lo},
    Start =
        case ets:member(?CHUNK_INTERVAL_CACHE_TABLE, First) of
            true -> First;
            false -> ets:next(?CHUNK_INTERVAL_CACHE_TABLE, First)
        end,
    collect_chunk_interval_keys(Mode, Peer, Hi, Start, []).

collect_chunk_interval_keys(
    Mode, Peer, Hi, {Mode, Peer, Location} = Key, Acc
) when Location =< Hi ->
    Next = ets:next(?CHUNK_INTERVAL_CACHE_TABLE, Key),
    collect_chunk_interval_keys(Mode, Peer, Hi, Next, [Key | Acc]);
collect_chunk_interval_keys(_Mode, _Peer, _Hi, _PastBucket, Acc) ->
    lists:reverse(Acc).

%%%===================================================================
%%% Helpers: cache rows.
%%%===================================================================

store_row(sync_bucket, Mode, SyncBucket, Peer, Share) ->
    Key = sync_bucket_key(Mode, SyncBucket, Peer),
    insert_row(?SYNC_BUCKET_CACHE_TABLE, Key, Share);
store_row(chunk_interval, Mode, Offset, Peer, Intervals) ->
    Key = chunk_interval_key(Mode, Peer, Offset),
    insert_row(?CHUNK_INTERVAL_CACHE_TABLE, Key, Intervals).

insert_row(Table, Key, Value) ->
    Now = ?DEP(clock):monotonic_ms(),
    ets:insert(Table, {Key, Value, Now}),
    ok.

delete_row(sync_bucket, Mode, SyncBucket, Peer) ->
    Key = sync_bucket_key(Mode, SyncBucket, Peer),
    ets:delete(?SYNC_BUCKET_CACHE_TABLE, Key);
delete_row(chunk_interval, Mode, Offset, Peer) ->
    Key = chunk_interval_key(Mode, Peer, Offset),
    ets:delete(?CHUNK_INTERVAL_CACHE_TABLE, Key).

expiration_cutoff(CacheTTLMs) ->
    ?DEP(clock):monotonic_ms() - CacheTTLMs.

%%%===================================================================
%%% Test support.
%%%===================================================================

-ifdef(AR_TEST).

%% @doc Return the number of running discovery jobs; their processes sleep on
%% the simulated clock, so the sim driver counts them when it checks that the
%% simulation has settled.
inflight_count() ->
    gen_server:call(?MODULE, inflight_count).

%% @doc Empty both discovery caches, so a test that reuses peer identifiers
%% does not inherit rows from an earlier test.
reset_all_caches() ->
    ensure_discovery_test_tables(),
    ets:delete_all_objects(?SYNC_BUCKET_CACHE_TABLE),
    ets:delete_all_objects(?CHUNK_INTERVAL_CACHE_TABLE),
    ok.

ensure_discovery_test_tables() ->
    ensure_named_table(?SYNC_BUCKET_CACHE_TABLE, ordered_set),
    ensure_named_table(?CHUNK_INTERVAL_CACHE_TABLE, ordered_set).

ensure_named_table(Name, Type) ->
    case ets:whereis(Name) of
        undefined ->
            ets:new(Name, [Type, public, named_table]),
            ok;
        _ ->
            ok
    end.

-endif.
