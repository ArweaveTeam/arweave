-module(ar_sync_progress_reporter).

-behaviour(gen_server).

-export([start_link/0,
	record_peer_progress/4,
	record_peer_download/2,
	record_peer_success/1,
	record_peer_not_found/1,
	record_peer_error/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include("ar.hrl").

-define(REPORT_INTERVAL_MS, 5000).
-define(RATE_WINDOW_MS, ?REPORT_INTERVAL_MS).
-define(MAX_ROWS, 5).
-define(MAX_ERRORS, 5).

-record(state, {
	joined = false,
	peer_stats = #{} :: #{ term() => map() },
	storage_recent = [] :: list(),
	recent_errors = [] :: list(),
	last_sizes = #{} :: map(),
	last_write_counters = #{} :: map(),
	last_packing_counters = #{} :: map(),
	window_start_ts = undefined :: term(),
	window_start_sizes = #{} :: map(),
	window_read_counters = #{} :: map()
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

record_peer_progress(Peer, AbsoluteEndOffset, StoreID, Packing) ->
	gen_server:cast(?MODULE, {peer_progress, Peer, AbsoluteEndOffset, StoreID, Packing}).

record_peer_download(Peer, Bytes) ->
	gen_server:cast(?MODULE, {peer_download, Peer, Bytes}).

record_peer_success(Peer) ->
	gen_server:cast(?MODULE, {peer_count, Peer, success}).

record_peer_not_found(Peer) ->
	gen_server:cast(?MODULE, {peer_count, Peer, not_found}).

record_peer_error(Peer, Reason) ->
	gen_server:cast(?MODULE, {peer_count, Peer, error}),
	gen_server:cast(?MODULE, {record_error, io_lib:format("~s: ~p", [ar_util:format_peer(Peer), Reason])}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ok = ar_events:subscribe(node_state),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast({peer_progress, Peer, AbsoluteEndOffset, StoreID, Packing}, State) ->
	Now = erlang:system_time(millisecond),
	PeerStats0 = State#state.peer_stats,
	Entry0 = maps:get(Peer, PeerStats0, #{}),
    Entry = maps:merge(Entry0, #{
		peer => Peer,
		offset => AbsoluteEndOffset,
		store_id => StoreID,
		packing => Packing,
		updated_at => Now,
		success => maps:get(success, Entry0, 0),
		not_found => maps:get(not_found, Entry0, 0),
        error => maps:get(error, Entry0, 0)
    }),
	{noreply, State#state{ peer_stats = maps:put(Peer, Entry, PeerStats0) }};

handle_cast({peer_count, Peer, Type}, State) ->
	PeerStats0 = State#state.peer_stats,
	Entry0 = maps:get(Peer, PeerStats0, #{}),
	NewCount = maps:get(Type, Entry0, 0) + 1,
	Entry = maps:put(Type, NewCount, Entry0),
	{noreply, State#state{ peer_stats = maps:put(Peer, Entry, PeerStats0) }};

handle_cast({peer_download, Peer, Bytes}, State) ->
	Now = erlang:system_time(millisecond),
	PeerStats0 = State#state.peer_stats,
	Entry0 = maps:get(Peer, PeerStats0, #{}),
	AccBytes = maps:get(dl_bytes, Entry0, 0) + Bytes,
	Entry = maps:merge(Entry0, #{ dl_bytes => AccBytes, updated_at => Now }),
	{noreply, State#state{ peer_stats = maps:put(Peer, Entry, PeerStats0) }};

handle_cast({record_error, MsgIOL}, State) ->
	Msg = lists:flatten(MsgIOL),
	Errors0 = State#state.recent_errors,
	Errors1 = [Msg | Errors0],
	Errors = case length(Errors1) > ?MAX_ERRORS of
		true -> lists:sublist(Errors1, 1, ?MAX_ERRORS);
		false -> Errors1
	end,
	{noreply, State#state{ recent_errors = Errors }};

handle_cast(report, #state{ joined = false } = State) ->
    ar_util:cast_after(?REPORT_INTERVAL_MS, self(), report),
    {noreply, State};

handle_cast(report, State) ->
    State2 = ensure_window(State),
    State3 = update_recent_storage_rows(State2),
    {PackingRate, State4} = compute_packing_rate(State3),
    {WriteRates, NewWriteCounters} = get_write_rates_map(State4#state.storage_recent, State4#state.last_write_counters),
    print_report(State4, PackingRate, WriteRates),
    ar_util:cast_after(?REPORT_INTERVAL_MS, self(), report),
    {noreply, State4#state{ last_write_counters = NewWriteCounters }};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({event, node_state, {initialized, _B}}, State) ->
    ar_util:cast_after(?REPORT_INTERVAL_MS, self(), report),
    {noreply, State#state{ joined = true }};

handle_info({event, node_state, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
    ?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
    {noreply, State}.

%%%===================================================================
%%% Internal helpers.
%%%===================================================================

update_recent_storage_rows(State) ->
	Modules = ar_storage_module:get_all_module_ranges(),
	NowSizes = lists:foldl(
		fun({{Start, _End}, Packing, StoreID}, Acc) ->
			Partition = ar_node:get_partition_number(Start),
			Size = ar_mining_stats:get_partition_data_size(Partition, Packing),
			maps:put(StoreID, {Size, Packing, Partition}, Acc)
		end,
		#{}, Modules),
	NowTs = erlang:system_time(millisecond),
	Rows = compute_recent_rows(State#state.last_sizes, NowSizes, State#state.storage_recent, NowTs),
	State#state{ storage_recent = Rows, last_sizes = NowSizes }.

compute_recent_rows(Prev, Now, OldRows, _NowTs) when map_size(Prev) =:= 0 ->
    %% Ignore the very first sampling (startup) to avoid reporting initial sizes as writes.
    refresh_old_rows(OldRows, Now);
	compute_recent_rows(Prev, Now, OldRows, NowTs) ->
    WindowedPrev = get_window_prev_sizes(Prev),
    NewRows = maps:fold(
		fun(StoreID, {Size, Packing, Partition}, Acc) ->
            case maps:get(StoreID, WindowedPrev, not_found) of
				{PS, _PP, _Part} ->
					Delta = Size - PS,
					case Delta > 0 of
						true -> [#{ module => StoreID, size => Size, added => Delta, partition => Partition, packing => Packing, updated_at => NowTs } | Acc];
						false -> Acc
					end;
				_ ->
					%% First time seeing this module after startup: don't treat existing size as Added
					Acc
			end
		end,
		[], Now),
    %% Refresh old rows and merge
    RefreshedOld = refresh_old_rows(OldRows, Now),
    All = NewRows ++ RefreshedOld,
    SortedAll = lists:sort(fun(A, B) -> maps:get(updated_at, A, 0) >= maps:get(updated_at, B, 0) end, All),
    Unique = unique_storage_modules(SortedAll),
    lists:sublist(Unique, 1, ?MAX_ROWS).

refresh_old_rows(OldRows, Now) ->
    lists:map(
        fun(Row) ->
            Module = maps:get(module, Row, undefined),
            case Module =/= undefined andalso maps:get(Module, Now, not_found) of
                {CurSize, CurPacking, CurPartition} ->
                    %% No new delta for this module in this tick; show 0 Added for this window
                    Row#{ size => CurSize, added => 0, packing => CurPacking, partition => CurPartition };
                _ ->
                    Row
            end
        end,
        OldRows).

unique_storage_modules(Rows) ->
	unique_storage_modules(Rows, #{}, []).

unique_storage_modules([Row | Rest], Seen, Acc) ->
	Module = maps:get(module, Row, undefined),
	case maps:is_key(Module, Seen) of
		true -> unique_storage_modules(Rest, Seen, Acc);
		false -> unique_storage_modules(Rest, maps:put(Module, true, Seen), [Row | Acc])
	end;
unique_storage_modules([], _Seen, Acc) ->
	lists:reverse(Acc).

ensure_window(State) ->
    Now = erlang:system_time(millisecond),
    case State#state.window_start_ts of
        undefined ->
            CountersW = get_write_counters_all(),
            CountersR = get_read_counters_all(),
            State#state{ window_start_ts = Now,
                         window_start_sizes = State#state.last_sizes,
                         last_write_counters = CountersW,
                         window_read_counters = CountersR };
        _ ->
            State
    end.

get_window_prev_sizes(DefaultPrev) ->
    %% This helper remains as a placeholder; update_recent_storage_rows chooses the baseline.
    DefaultPrev.

get_all_store_label_packings() ->
    Modules = ar_storage_module:get_all_module_ranges(),
    lists:usort([
        begin
            {_Range, Packing, StoreID} = M,
            {ar_storage_module:label(StoreID), ar_storage_module:packing_label(Packing)}
        end || M <- Modules
    ]).

get_write_counters_all() ->
    LabelPackings = get_all_store_label_packings(),
    LabelToPackings = lists:foldl(
        fun({Label, Pack}, Acc) ->
            Packs = maps:get(Label, Acc, []),
            maps:put(Label, lists:usort([Pack | Packs]), Acc)
        end, #{}, LabelPackings),
    maps:from_list([
        {Label, lists:sum([safe_counter_value(chunks_stored, [Pack, Label]) || Pack <- Packs])}
        || {Label, Packs} <- maps:to_list(LabelToPackings)
    ]).

get_read_counters_all() ->
    Labels = lists:usort([L || {L, _P} <- get_all_store_label_packings()]),
    maps:from_list([{L, safe_counter_value(chunks_read, [L])} || L <- Labels]).

print_report(State, PackingRate, WriteRates) ->
	%% Header
	Height = ar_node:get_height(),
	Hash = ar_node:get_current_block_hash(),
	HashStr = case Hash of
		not_joined -> "not_joined";
		_ -> binary_to_list(ar_util:encode(Hash))
	end,
	ar:console("\n=== Sync Progress ===\n", []),
	ar:console("Height: ~B  Hash: ~s\n", [Height, HashStr]),

    %% Rates summary
    WriteRate = get_write_rate_from_added(State#state.storage_recent),
    ar:console("Rates: write ~.2f MiB/s, packing ~.2f MiB/s\n", [WriteRate, PackingRate]),

    %% Peers table
    PeerStats = maps:values(State#state.peer_stats),
    SortedPeers = lists:sublist(sort_peers_by_updated(PeerStats), 1, min(?MAX_ROWS, length(PeerStats))),
    ar:console("+----------------------+----------------+--------+-----------+----------------+--------+----------+--------+----------------+\n", []),
    ar:console("|         Peer         | AbsEndOffset   | Mode   | Partition |     Packing    | Succ % | NotFnd % | Err %  | Download MiB/s |\n", []),
    ar:console("+----------------------+----------------+--------+-----------+----------------+--------+----------+--------+----------------+\n", []),
    lists:foreach(fun print_peer_row/1, SortedPeers),
    ar:console("+----------------------+----------------+--------+-----------+----------------+--------+----------+--------+----------------+\n", []),
    ar:console("Total peers queried: ~B\n", [map_size(State#state.peer_stats)]),

    %% Storage modules section
    ar:console("\nRecent storage writes (up to ~B):\n", [?MAX_ROWS]),
    ar:console("+-------------------------------+------------------+------------------+------------------+----------------------+-----------------+\n", []),
    ar:console("|        Storage Module         |     Size         |      Added       |     Partition    |          Packing     | WriteRate MiB/s |\n", []) ,
    ar:console("+-------------------------------+------------------+------------------+------------------+----------------------+-----------------+\n", []),
    lists:foreach(fun(Row) -> print_storage_row(Row, WriteRates) end, State#state.storage_recent),
    ar:console("+-------------------------------+------------------+------------------+------------------+----------------------+-----------------+\n", []),
    TotalModules = length(ar_storage_module:get_all_module_ranges()),
    ar:console("Total configured storage modules: ~B\n", [TotalModules]),

	%% Recent errors
	case State#state.recent_errors of
		[] -> ok;
		Errors ->
			ar:console("\nRecent errors:\n", []),
			lists:foreach(fun(E) -> ar:console("- ~s\n", [E]) end, lists:reverse(Errors))
	end.

sort_peers_by_updated(Peers) ->
	lists:sort(fun(A, B) -> maps:get(updated_at, A, 0) >= maps:get(updated_at, B, 0) end, Peers).

%% Compute write rate (MiB/s) by summing Added deltas over the report interval
get_write_rate_from_added(Rows) ->
    IntervalSec = (?REPORT_INTERVAL_MS) / 1000,
    Bytes = lists:sum([maps:get(added, R, 0) || R <- Rows]),
    (Bytes / IntervalSec) / ?MiB.

compute_peer_dl_rate(Entry) ->
    Bytes = maps:get(dl_bytes, Entry, 0),
    IntervalSec = (?REPORT_INTERVAL_MS) / 1000,
    case Bytes > 0 of
        true -> (Bytes / IntervalSec) / ?MiB;
        false -> 0.0
    end.

%% Approximate packing rate (requests per second) using packing_requests counter delta since last scrape is not accessible here.
%% We can only show the absolute counter per type; to keep it useful, display current total increment rate per second if the backend exposes it.
%% Fallback to 0 for now as we don't maintain previous counter in this process.

%% Compute packing rate (req/s) from prometheus packing_requests counter deltas
compute_packing_rate(State) ->
    Now = erlang:system_time(millisecond),
    Total = get_packing_requests_total(),
    PrevTotal = maps:get(total, State#state.last_packing_counters, undefined),
    PrevTs = State#state.window_start_ts,
    case PrevTotal of
        undefined -> {0.0, State#state{ last_packing_counters = #{ total => Total } }};
        PT when is_integer(PT), is_integer(PrevTs), Now > PrevTs ->
            Delta = case Total >= PT of true -> Total - PT; false -> 0 end,
            ReqPerSec = Delta / ((Now - PrevTs) / 1000),
            RateMiBs = (ReqPerSec * ?DATA_CHUNK_SIZE) / ?MiB,
            {RateMiBs, State#state{ last_packing_counters = #{ total => Total } }};
        _ -> {0.0, State#state{ last_packing_counters = #{ total => Total } }}
    end.

get_packing_requests_total() ->
    Types = [pack, unpack, unpack_sub_chunk],
    Packings = [unpacked, unpacked_padded, spora_2_5, spora_2_6, composite, replica_2_9],
    lists:sum([safe_counter_value(packing_requests, [T, P]) || T <- Types, P <- Packings]).

%% Removed per-packing rate map (no store_id in packing_requests), we only compute global packing rate.

safe_counter_value(Metric, Labels) ->
    try
        V = prometheus_counter:value(Metric, Labels),
        case is_number(V) of true -> V; false -> 0 end
    catch _:_ -> 0 end.

%% no longer used

%% removed histogram usage and per-store packing rates

%% Build per-store write rate map using chunks_stored[counter] deltas per store_id
get_write_rates_map(Rows, PrevCounters) ->
    Labels = [ar_storage_module:label(maps:get(module, R)) || R <- Rows],
    NowCounters = maps:from_list([{L, current_chunks_stored(L)} || L <- Labels]),
    IntervalSec = (?REPORT_INTERVAL_MS) / 1000,
    Rates = maps:from_list([
        begin
            StoreID = maps:get(module, R),
            Label = ar_storage_module:label(StoreID),
            Cur = maps:get(Label, NowCounters, 0),
            Prev = maps:get(Label, PrevCounters, Cur),
            DeltaReq = case Cur >= Prev of true -> Cur - Prev; false -> 0 end,
            MiBs = ((DeltaReq * ?DATA_CHUNK_SIZE) / IntervalSec) / ?MiB,
            {StoreID, MiBs}
        end || R <- Rows
    ]),
    {Rates, NowCounters}.

current_chunks_stored(Label) ->
    %% Sum chunks_stored over the actual packing labels configured for this store_id label
    LabelPackings = [P || {L, P} <- get_all_store_label_packings(), L =:= Label],
    lists:sum([safe_counter_value(chunks_stored, [P, Label]) || P <- LabelPackings]).

print_peer_row(Entry) ->
	Peer = maps:get(peer, Entry, undefined),
    PeerStr = case Peer of
        undefined -> "unknown";
        P when is_tuple(P) orelse is_list(P) -> ar_util:format_peer(P);
        _ -> lists:flatten(io_lib:format("~p", [Peer]))
    end,
	Offset = maps:get(offset, Entry, 0),
	Partition = ar_node:get_partition_number(Offset),
	Packing = maps:get(packing, Entry, unpacked),
	Mode = case Packing of {replica_2_9, _} -> "footprint"; _ -> "normal" end,
	PackingStr = ar_serialize:encode_packing(Packing, true),
	S = maps:get(success, Entry, 0),
	N = maps:get(not_found, Entry, 0),
	E = maps:get(error, Entry, 0),
	Tot = case S + N + E of 0 -> 1; X -> X end,
	SR = (S * 100) div Tot,
	NR = (N * 100) div Tot,
	ER = (E * 100) div Tot,
    DLRate = compute_peer_dl_rate(Entry),
    ar:console("| ~20s | ~14B | ~6s | ~9B | ~14s | ~5B% | ~7B% | ~5B% | ~9.2f |\n",
        [PeerStr, Offset, Mode, Partition, PackingStr, SR, NR, ER, DLRate]).

print_storage_row(Entry, WriteRates) ->
    StoreID = maps:get(module, Entry),
    Size = maps:get(size, Entry, 0),
    Added = maps:get(added, Entry, 0),
    Partition = maps:get(partition, Entry),
    Packing = maps:get(packing, Entry),
    PackingStr = ar_serialize:encode_packing(Packing, true),
    WriteRateMiBs = maps:get(StoreID, WriteRates, 0.0),
    AddedStr = lists:flatten(io_lib:format("+~B", [Added])),
    ar:console("| ~29s | ~16B | ~16s | ~16B | ~22s | ~9.2f |\n",
        [StoreID, Size, AddedStr, Partition, PackingStr, WriteRateMiBs]).