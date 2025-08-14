-module(ar_sync_progress_reporter).

-behaviour(gen_server).

-export([start_link/0,
	record_peer_progress/4,
	record_peer_success/1,
	record_peer_not_found/1,
	record_peer_error/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-include("ar.hrl").

-define(REPORT_INTERVAL_MS, 5000).
-define(MAX_ROWS, 5).
-define(MAX_ERRORS, 5).

-record(state, {
	joined = false,
	peer_stats = #{} :: #{ term() => map() },
	storage_recent = [] :: list(),
	recent_errors = [] :: list(),
	last_sizes = #{} :: map()
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

record_peer_progress(Peer, AbsoluteEndOffset, StoreID, Packing) ->
	gen_server:cast(?MODULE, {peer_progress, Peer, AbsoluteEndOffset, StoreID, Packing}).

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
	State2 = update_recent_storage_rows(State),
	print_report(State2),
	ar_util:cast_after(?REPORT_INTERVAL_MS, self(), report),
	{noreply, State2};

handle_cast(Cast, State) ->
    ?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
    {noreply, State}.

handle_info({event, node_state, {initialized, _B}}, State) ->
    ar_util:cast_after(?REPORT_INTERVAL_MS, self(), report),
    {noreply, State#state{ joined = true }};

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

compute_recent_rows(Prev, Now, OldRows, NowTs) ->
	NewRows = maps:fold(
		fun(StoreID, {Size, Packing, Partition}, Acc) ->
			PrevSize = case maps:get(StoreID, Prev, not_found) of
				{PS, _PP, _Part} -> PS;
				_ -> 0
			end,
			Delta = Size - PrevSize,
			case Delta > 0 of
				true -> [#{ module => StoreID, size => Delta, partition => Partition, packing => Packing, updated_at => NowTs } | Acc];
				false -> Acc
			end
		end,
		[], Now),
	%% Prepend new rows and keep most recent up to MAX_ROWS
	All = NewRows ++ OldRows,
	SortedAll = lists:sort(fun(A, B) -> maps:get(updated_at, A, 0) >= maps:get(updated_at, B, 0) end, All),
	Unique = unique_storage_modules(SortedAll),
	lists:sublist(Unique, 1, ?MAX_ROWS).

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

print_report(State) ->
	%% Header
	Height = ar_node:get_height(),
	Hash = ar_node:get_current_block_hash(),
	HashStr = case Hash of
		not_joined -> "not_joined";
		_ -> binary_to_list(ar_util:encode(Hash))
	end,
	ar:console("\n=== Sync Progress ===\n", []),
	ar:console("Height: ~B  Hash: ~s\n", [Height, HashStr]),

	%% Rates summary from Prometheus metrics
	WriteRate = get_metric_write_rate_mibs(),
	PackingRate = get_metric_packing_read_rate_mibs(),
	ar:console("Rates: write ~.2f MiB/s, packing ~.2f MiB/s\n", [WriteRate, PackingRate]),

	%% Peers table
	PeerStats = maps:values(State#state.peer_stats),
	SortedPeers = lists:sublist(sort_peers_by_updated(PeerStats), 1, min(?MAX_ROWS, length(PeerStats))),
    ar:console("Peers (up to ~B):\n", [?MAX_ROWS]),
	ar:console("[peer] [abs_end_offset] [mode] [partition] [packing] [success] [not_found] [error]\n", []),
	lists:foreach(fun print_peer_row/1, SortedPeers),
    ar:console("Total peers queried: ~B\n", [map_size(State#state.peer_stats)]),

	%% Storage modules section
	ar:console("\nRecent storage writes (up to ~B):\n", [?MAX_ROWS]),
	ar:console("[storage_module] [size] [partition] [packing]\n", []),
	lists:foreach(fun print_storage_row/1, State#state.storage_recent),
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

%% Read current write rate (MiB/s) from Prometheus histogram (sum of rates for store_id, any type)
get_metric_write_rate_mibs() ->
    Labels = get_all_store_labels(),
    RatesBytes = [safe_histogram_value(chunk_write_rate_bytes_per_second, [L, repack]) || L <- Labels],
    (lists:sum([case R of undefined -> 0; _ -> R end || R <- RatesBytes]) / ?MiB).

%% Read current packing read rate (MiB/s) from Prometheus histogram
get_metric_packing_read_rate_mibs() ->
    Labels = get_all_store_labels(),
    RatesBytes = [safe_histogram_value(chunk_read_rate_bytes_per_second, [L, repack]) || L <- Labels],
    (lists:sum([case R of undefined -> 0; _ -> R end || R <- RatesBytes]) / ?MiB).

get_all_store_labels() ->
    try
        ModuleRanges = ar_storage_module:get_all_module_ranges(),
        Labels = [ar_storage_module:label(StoreID) || {_Range, _Packing, StoreID} <- ModuleRanges],
        lists:usort(Labels)
    catch _:_ -> [] end.

safe_histogram_value(Metric, Labels) ->
    try
        V = prometheus_histogram:value(Metric, Labels),
        case is_number(V) of
            true -> V;
            false -> 0
        end
    catch _:_ -> 0 end.

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
	ar:console("~s ~B ~s ~p ~s ~B% ~B% ~B%\n",
		[PeerStr, Offset, Mode, Partition, PackingStr, SR, NR, ER]).

print_storage_row(Entry) ->
	StoreID = maps:get(module, Entry),
	Size = maps:get(size, Entry),
	Partition = maps:get(partition, Entry),
	Packing = maps:get(packing, Entry),
	PackingStr = ar_serialize:encode_packing(Packing, true),
	ar:console("~s ~B ~p ~s\n", [StoreID, Size, Partition, PackingStr]).