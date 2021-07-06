-module(ar_data_sync).

-behaviour(gen_server).

-export([start_link/2, join/3, add_tip_block/4, add_block/2, is_chunk_proof_ratio_attractive/3,
		add_chunk/1, add_data_root_to_disk_pool/3, maybe_drop_data_root_from_disk_pool/3,
		get_chunk/2, get_tx_data/1, get_tx_data/2, get_tx_offset/1, has_data_root/2,
		request_tx_data_removal/3, request_data_removal/4, record_disk_pool_chunks_count/0,
		record_sync_buffer_size_metric/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).
-export([remove_expired_disk_pool_data_roots/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name, StoreID) ->
	gen_server:start_link({local, Name}, ?MODULE, StoreID, []).

%% @doc Notify the server the node has joined the network on the given block index.
join(RecentBI, Packing_2_5_Threshold, Packing_2_6_Threshold) ->
	gen_server:cast(ar_data_sync_default, {join, RecentBI, Packing_2_5_Threshold,
			Packing_2_6_Threshold}).

%% @doc Notify the server about the new tip block.
add_tip_block(Packing_2_5_Threshold, Packing_2_6_Threshold, BlockTXPairs, RecentBI) ->
	gen_server:cast(ar_data_sync_default, {add_tip_block, Packing_2_5_Threshold,
			Packing_2_6_Threshold, BlockTXPairs, RecentBI}).

%% @doc The condition which is true if the chunk is too small compared to the proof.
%% Small chunks make syncing slower and increase space amplification. A small chunk
%% is accepted if it is the last chunk of the corresponding transaction - such chunks
%% may be produced by ar_tx:chunk_binary/1, the legacy splitting method used to split
%% v1 data or determine the data root of a v2 tx when data is uploaded via the data field.
%% Due to the block limit we can only get up to 1k such chunks per block.
is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) ->
	DataPathSize = byte_size(DataPath),
	case DataPathSize of
		0 ->
			false;
		_ ->
			case catch ar_merkle:extract_note(DataPath) of
				{'EXIT', _} ->
					false;
				Offset ->
					Offset == TXSize orelse DataPathSize =< ChunkSize
			end
	end.

%% @doc Store the given chunk if the proof is valid.
%% Called when a chunk is pushed to the node via POST /chunk.
%% The chunk is placed in the disk pool. The periodic process
%% scanning the disk pool will later record it as synced.
%% The item is removed from the disk pool when the chunk's offset
%% drops below the "disk pool threshold".
add_chunk(#{ data_root := DataRoot, offset := Offset, data_path := DataPath,
		chunk := Chunk, data_size := TXSize }) ->
	case ets:lookup(ar_data_sync_state, have_disk_space) of
		[{_, false}] ->
			{error, disk_full};
		_ ->
			add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize)
	end.

%% @doc Notify the server about the new pending data root (added to mempool).
%% The server may accept pending chunks and store them in the disk pool.
add_data_root_to_disk_pool(_, 0, _) ->
	ok;
add_data_root_to_disk_pool(DataRoot, _, _) when byte_size(DataRoot) < 32 ->
	ok;
add_data_root_to_disk_pool(DataRoot, TXSize, TXID) ->
	Key = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case ets:lookup(ar_disk_pool_data_roots, Key) of
		[] ->
			ets:insert(ar_disk_pool_data_roots, {Key,
					{0, os:system_time(microsecond), sets:from_list([TXID])}});
		[{_, {_, _, not_set}}] ->
			ok;
		[{_, {Size, Timestamp, TXIDSet}}] ->
			ets:insert(ar_disk_pool_data_roots,
					{Key, {Size, Timestamp, sets:add_element(TXID, TXIDSet)}})
	end,
	ok.

%% @doc Notify the server the given data root has been removed from the mempool.
maybe_drop_data_root_from_disk_pool(_, 0, _) ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, _, _) when byte_size(DataRoot) < 32 ->
	ok;
maybe_drop_data_root_from_disk_pool(DataRoot, TXSize, TXID) ->
	Key = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	case ets:lookup(ar_disk_pool_data_roots, Key) of
		[] ->
			ok;
		[{_, {_, _, not_set}}] ->
			ok;
		[{_, {Size, Timestamp, TXIDs}}] ->
			case sets:subtract(TXIDs, sets:from_list([TXID])) of
				TXIDs ->
					ok;
				TXIDs2 ->
					case sets:size(TXIDs2) of
						0 ->
							ets:delete(ar_disk_pool_data_roots, Key);
						_ ->
							ets:insert(ar_disk_pool_data_roots, {Key,
									{Size, Timestamp, TXIDs2}})
					end
			end
	end,
	ok.

%% @doc Fetch the chunk corresponding to Offset. When Offset is less than or equal to
%% the strict split data threshold, the chunk returned contains the byte with the given
%% Offset (the indexing is 1-based). Otherwise, the chunk returned ends in the same 256 KiB
%% bucket as Offset counting from the first 256 KiB after the strict split data threshold.
%% The strict split data threshold is weave_size of the block preceding the fork 2.5 block.
%%
%% Options:
%%	_________________________________________________________________________________________
%%	packing				| required; spora_2_5 or unpacked or {spora_2_6, <Mining Address>};
%%	_________________________________________________________________________________________
%%	pack				| if false and a packed chunk is requested but stored unpacked or
%%						| an unpacked chunk is requested but stored packed, return
%%						| {error, chunk_not_found} instead of packing/unpacking;
%%						| true by default;
%%	_________________________________________________________________________________________
%%	bucket_based_offset	| does not play a role for the offsets before
%%						| strict_data_split_threshold (weave_size of the block preceding
%%						| the fork 2.5 block); if true, return the chunk which ends in
%%						| the same 256 KiB bucket starting from
%%						| strict_data_split_threshold where borders belong to the
%%						| buckets on the left; true by default.
get_chunk(Offset, #{ packing := Packing } = Options) ->
	case ar_sync_record:is_recorded(Offset, ?MODULE) of
		{{true, StoredPacking}, StoreID} ->
			Pack = maps:get(pack, Options, true),
			case {Pack, Packing == StoredPacking} of
				{false, false} ->
					{error, chunk_not_found};
				_ ->
					get_chunk(Offset, Pack, Packing, StoredPacking, Options, StoreID)
			end;
		_ ->
			{error, chunk_not_found}
	end.

%% @doc Fetch the transaction data. Return {error, tx_data_too_big} if
%% the size is bigger than ?MAX_SERVED_TX_DATA_SIZE, unless the limitation
%% is disabled in the configuration.
get_tx_data(TXID) ->
	{ok, Config} = application:get_env(arweave, config),
	SizeLimit =
		case lists:member(serve_tx_data_without_limits, Config#config.enable) of
			true ->
				infinity;
			false ->
				?MAX_SERVED_TX_DATA_SIZE
		end,
	get_tx_data(TXID, SizeLimit).

%% @doc Fetch the transaction data. Return {error, tx_data_too_big} if
%% the size is bigger than SizeLimit.
get_tx_data(TXID, SizeLimit) ->
	case get_tx_offset(TXID) of
		{error, not_joined} ->
			{error, not_joined};
		{error, not_found} ->
			{error, not_found};
		{error, failed_to_read_tx_offset} ->
			{error, failed_to_read_tx_data};
		{ok, {Offset, Size}} ->
			case Size > SizeLimit of
				true ->
					{error, tx_data_too_big};
				false ->
					get_tx_data(Offset - Size, Offset, [])
			end
	end.

%% @doc Return the global end offset and size for the given transaction.
get_tx_offset(TXID) ->
	case ets:lookup(ar_data_sync_state, {tx_index, "default"}) of
		[] ->
			{error, not_joined};
		[{_, TXIndex}] ->
			get_tx_offset(TXIndex, TXID)
	end.

%% @doc Return true if the given {DataRoot, DataSize} is in the mempool
%% or in the index.
has_data_root(DataRoot, DataSize) ->
	DataRootKey = << DataRoot:32/binary, DataSize:256 >>,
	case ets:member(ar_disk_pool_data_roots, DataRootKey) of
		true ->
			true;
		false ->
			case get_data_root_offset(DataRootKey, "default") of
				{ok, _} ->
					true;
				_ ->
					false
			end
	end.

%% @doc Record the metadata of the given block.
add_block(B, SizeTaggedTXs) ->
	gen_server:cast(ar_data_sync_default, {add_block, B, SizeTaggedTXs}).

%% @doc Request the removal of the transaction data.
request_tx_data_removal(TXID, Ref, ReplyTo) ->
	[{_, TXIndex}] = ets:lookup(ar_data_sync_state, {tx_index, "default"}),
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{End, Size} = binary_to_term(Value),
			remove_range(End - Size, End, Ref, ReplyTo);
		not_found ->
			?LOG_WARNING([{event, tx_offset_not_found}, {tx, ar_util:encode(TXID)}]),
			ok;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_fetch_blacklisted_tx_offset},
					{tx, ar_util:encode(TXID)}, {reason, Reason}]),
			ok
	end.

%% @doc Request the removal of the given byte range.
request_data_removal(Start, End, Ref, ReplyTo) ->
	remove_range(Start, End, Ref, ReplyTo).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init("default" = StoreID) ->
	?LOG_INFO([{event, ar_data_sync_start}, {store_id, StoreID}]),
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	[ok, ok] = ar_events:subscribe([node_state, chunk]),
	State = init_kv(StoreID),
	move_disk_pool_index(State),
	move_data_root_index(State),
	timer:apply_interval(?RECORD_DISK_POOL_CHUNKS_COUNT_FREQUENCY_MS, ar_data_sync,
			record_disk_pool_chunks_count, []),
	StateMap = read_data_sync_state(),
	CurrentBI = maps:get(block_index, StateMap),
	DiskPoolDataRoots = maps:get(disk_pool_data_roots, StateMap),
	DiskPoolSize = maps:get(disk_pool_size, StateMap),
	%% Maintain a map of pending and orphaned data roots and those whose chunks were
	%% uploaded recently.
	%% << DataRoot:32/binary, TXSize:256 >> => {Size, Timestamp, TXIDSet}.
	%%
	%% Unconfirmed chunks can be accepted only after their data roots end up in this set.
	%% New chunks for these data roots are accepted until the corresponding size reaches
	%% #config.max_disk_pool_data_root_buffer_mb or the total size of added pending and
	%% seeded chunks reaches #config.max_disk_pool_buffer_mb. When a data root is orphaned,
	%% its timestamp is refreshed so that the chunks have chance to be reincluded later.
	%% After a data root expires, the corresponding chunks are removed from
	%% disk_pool_chunks_index and if they are not in data_root_index - from storage.
	%% TXIDSet keeps track of pending transaction identifiers - if all pending transactions
	%% with the << DataRoot:32/binary, TXSize:256 >> key are dropped from the mempool,
	%% the corresponding entry is removed from DiskPoolDataRoots. When a data root is
	%% confirmed, TXIDSet is set to not_set - from this point on, the key is only dropped
	%% after expiration.
	maps:map(fun(DataRootKey, V) -> ets:insert(ar_disk_pool_data_roots,
			{DataRootKey, V}) end, DiskPoolDataRoots),
	%% The sum of sizes of all pending and seeded chunks. When it reaches
	%% ?MAX_DISK_POOL_BUFFER_MB, new chunks with these data roots are rejected.
	ets:insert(ar_data_sync_state, {disk_pool_size, DiskPoolSize}),
	DiskPoolThreshold = maps:get(disk_pool_threshold, StateMap),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	SkipIntervalsTable = ets:new(sync_record_type, [ordered_set, public]),
	State2 = State#sync_data_state{
		block_index = CurrentBI,
		weave_size = maps:get(weave_size, StateMap),
		disk_pool_cursor = first,
		disk_pool_threshold = DiskPoolThreshold,
		packing_2_5_threshold = maps:get(packing_2_5_threshold, StateMap),
		repacking_cursor = 0,
		packing_disabled = lists:member(packing, Config#config.disable),
		packing_2_6_threshold = maps:get(packing_2_6_threshold, StateMap),
		mining_address = Config#config.mining_addr,
		store_id = StoreID,
		skip_intervals_table = SkipIntervalsTable
	},
	gen_server:cast(self(), check_space),
	gen_server:cast(self(), check_space_warning),
	timer:apply_interval(?REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS, ?MODULE,
			remove_expired_disk_pool_data_roots, []),
	lists:foreach(
		fun(_DiskPoolJobNumber) ->
			gen_server:cast(self(), process_disk_pool_item)
		end,
		lists:seq(1, Config#config.disk_pool_jobs)
	),
	gen_server:cast(self(), store_sync_state),
	gen_server:cast(self(), repack_stored_chunks),
	ets:insert(ar_data_sync_state, {sync_buffer_size, 0}),
	timer:apply_interval(200, ?MODULE, record_sync_buffer_size_metric, ["default"]),
	{ok, State2};
init(StoreID) ->
	?LOG_INFO([{event, ar_data_sync_start}, {store_id, StoreID}]),
	process_flag(trap_exit, true),
	[ok, ok] = ar_events:subscribe([node_state, chunk]),
	State = init_kv(StoreID),
	SkipIntervalsTable = ets:new(sync_record_type, [ordered_set, public]),
	{RangeStart, RangeEnd} = ar_storage_module:get_range(StoreID),
	State2 = State#sync_data_state{
		store_id = StoreID,
		skip_intervals_table = SkipIntervalsTable,
		range_start = RangeStart,
		range_end = RangeEnd
	},
	may_be_run_sync_jobs(),
	ets:insert(ar_data_sync_state, {sync_buffer_size, 0}),
	timer:apply_interval(200, ?MODULE, record_sync_buffer_size_metric, [StoreID]),
	{ok, State2}.

handle_cast({move_data_root_index, Cursor, N}, State) ->
	move_data_root_index(Cursor, N, State),
	{noreply, State};

handle_cast({join, RecentBI, Packing_2_5_Threshold, Packing_2_6_Threshold}, State) ->
	#sync_data_state{ block_index = CurrentBI, weave_size = CurrentWeaveSize,
			store_id = StoreID } = State,
	[{_, WeaveSize, _} | _] = RecentBI,
	case {CurrentBI, ar_block_index:get_intersection(CurrentBI)} of
		{[], _} ->
			ok;
		{_, {_, CurrentWeaveSize, _}} ->
			ok;
		{_, no_intersection} ->
			throw(last_stored_block_index_has_no_intersection_with_the_new_one);
		{_, {_H, Offset, _TXRoot}} ->
			PreviousWeaveSize = element(2, hd(CurrentBI)),
			{ok, OrphanedDataRoots} = remove_orphaned_data(State, Offset, PreviousWeaveSize),
			ar_chunk_storage:cut(Offset, StoreID),
			ar_sync_record:cut(Offset, ?MODULE, StoreID),
			ar_events:send(data_sync, {cut, Offset}),
			reset_orphaned_data_roots_disk_pool_timestamps(OrphanedDataRoots)
	end,
	BI = ar_block_index:get_list_by_hash(element(1, lists:last(RecentBI))),
	repair_data_root_offset_index(BI, State),
	DiskPoolThreshold = get_disk_pool_threshold(RecentBI),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	State2 =
		State#sync_data_state{
			weave_size = WeaveSize,
			block_index = lists:sublist(RecentBI, ?TRACK_CONFIRMATIONS),
			disk_pool_threshold = DiskPoolThreshold,
			packing_2_5_threshold = Packing_2_5_Threshold,
			packing_2_6_threshold = Packing_2_6_Threshold
		},
	store_sync_state(State2),
	{noreply, State2};

handle_cast({add_tip_block, Packing_2_5_Threshold, Packing_2_6_Threshold, BlockTXPairs, BI},
		State) ->
	#sync_data_state{ tx_index = TXIndex, tx_offset_index = TXOffsetIndex, store_id = StoreID,
			weave_size = CurrentWeaveSize, block_index = CurrentBI } = State,
	{BlockStartOffset, Blocks} = pick_missing_blocks(CurrentBI, BlockTXPairs),
	{ok, OrphanedDataRoots} = remove_orphaned_data(State, BlockStartOffset, CurrentWeaveSize),
	{WeaveSize, AddedDataRoots} = lists:foldl(
		fun ({_BH, []}, Acc) ->
				Acc;
			({_BH, SizeTaggedTXs}, {StartOffset, CurrentAddedDataRoots}) ->
				{ok, DataRoots} = add_block_data_roots(State, SizeTaggedTXs, StartOffset),
				ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, StartOffset),
				{StartOffset + element(2, lists:last(SizeTaggedTXs)),
					sets:union(CurrentAddedDataRoots, DataRoots)}
		end,
		{BlockStartOffset, sets:new()},
		Blocks
	),
	add_block_data_roots_to_disk_pool(AddedDataRoots),
	reset_orphaned_data_roots_disk_pool_timestamps(OrphanedDataRoots),
	ar_chunk_storage:cut(BlockStartOffset, StoreID),
	ar_sync_record:cut(BlockStartOffset, ?MODULE, StoreID),
	ar_events:send(data_sync, {cut, BlockStartOffset}),
	DiskPoolThreshold = get_disk_pool_threshold(BI),
	ets:insert(ar_data_sync_state, {disk_pool_threshold, DiskPoolThreshold}),
	State2 = State#sync_data_state{ weave_size = WeaveSize, block_index = BI,
			disk_pool_threshold = DiskPoolThreshold,
			packing_2_5_threshold = Packing_2_5_Threshold,
			packing_2_6_threshold = Packing_2_6_Threshold },
	store_sync_state(State2),
	{noreply, State2};

handle_cast({add_block, B, SizeTaggedTXs}, State) ->
	add_block(B, SizeTaggedTXs, State),
	{noreply, State};

handle_cast(check_space, State) ->
	ar_util:cast_after(ar_disksup:get_disk_space_check_frequency(), self(), check_space),
	HaveSpace = have_free_space(),
	ets:insert(ar_data_sync_state, {have_disk_space, HaveSpace}),
	{noreply, State#sync_data_state{ sync_disk_space = HaveSpace }};

handle_cast(check_space_warning, #sync_data_state{ sync_disk_space = false } = State) ->
	Msg =
		"The node has stopped syncing data - the available disk space is"
		" less than ~s. Add more disk space if you wish to store more data.~n",
	ar:console(Msg, [ar_util:bytes_to_mb_string(?DISK_DATA_BUFFER_SIZE)]),
	?LOG_INFO([{event, ar_data_sync_stopped_syncing}, {reason, little_disk_space_left}]),
	ar_util:cast_after(?DISK_SPACE_WARNING_FREQUENCY, self(), check_space_warning),
	{noreply, State};
handle_cast(check_space_warning, State) ->
	ar_util:cast_after(?DISK_SPACE_WARNING_FREQUENCY, self(), check_space_warning),
	{noreply, State};

%% Pick a leftmost unsynced interval from the range configured for this process (skip
%% the intervals we failed to find recently, if any), compute the intersection with
%% the intervals available by peers, and place the peer-interval pairs in the syncing queue.
handle_cast(collect_sync_intervals, #sync_data_state{ sync_disk_space = false } = State) ->
	Delay = ar_disksup:get_disk_space_check_frequency(),
	ar_util:cast_after(Delay, self(), collect_sync_intervals),
	{noreply, State};
handle_cast(collect_sync_intervals, State) ->
	#sync_data_state{ range_start = Start, range_end = End, sync_intervals_queue = Q,
			store_id = StoreID, skip_intervals_table = SkipIntervalsTable,
			disk_pool_threshold = DiskPoolThreshold } = State,
	case ar_node:is_joined() of
		false ->
			ar_util:cast_after(1000, self(), collect_sync_intervals);
		true ->
			case gb_sets:size(Q) > ?SYNC_INTERVALS_MAX_QUEUE_SIZE
					orelse Start >= DiskPoolThreshold of
				true ->
					ar_util:cast_after(500, self(), collect_sync_intervals);
				false ->
					Self = self(),
					monitor(process, spawn(
						fun() ->
							process_flag(trap_exit, true),
							find_peer_intervals(Start, min(End, DiskPoolThreshold),
									StoreID, SkipIntervalsTable, Self),
							ar_util:cast_after(500, Self, collect_sync_intervals)
						end
					))
			end
	end,
	{noreply, State};

handle_cast({enqueue_intervals, Peer, Intervals}, State) ->
	#sync_data_state{ sync_intervals_queue = Q, skip_intervals_table = TID,
			sync_intervals_queue_intervals = QIntervals } = State,
	%% Only keep unique intervals. We may get some duplicates for two
	%% reasons:
	%% 1) find_peer_intervals might choose the same interval several
	%%    times in a row even when there are other unsynced intervals
	%%    to pick because it is probabilistic.
	%% 2) We ask many peers simultaneously about the same interval
	%%    to make finding of the relatively rare intervals quicker.
	OuterJoin = ar_intervals:outerjoin(QIntervals, Intervals),
	{Q2, QIntervals2} =
		ar_intervals:fold(
			fun({End, Start}, {Acc, QIAcc}) ->
				?LOG_DEBUG([{event, add_interval_to_sync_queue}, {right, End}, {left, Start},
						{peer, ar_util:format_peer(Peer)}]),
				temporarily_exclude_interval_from_syncing(Start, End, TID, self()),
				{lists:foldl(
					fun(Start2, Acc2) ->
						End2 = min(Start2 + ?DATA_CHUNK_SIZE, End),
						gb_sets:add_element({Start2, End2, Peer}, Acc2)
					end,
					Acc,
					lists:seq(Start, End - 1, ?DATA_CHUNK_SIZE)
				), ar_intervals:add(QIAcc, End, Start)}
			end,
			{Q, QIntervals},
			OuterJoin
		),
	{noreply, State#sync_data_state{ sync_intervals_queue = Q2,
			sync_intervals_queue_intervals = QIntervals2 }};

handle_cast({exclude_interval_from_syncing, Left, Right}, State) ->
	#sync_data_state{ skip_intervals_table = TID } = State,
	temporarily_exclude_interval_from_syncing(Left, Right, TID, self()),
	{noreply, State};

handle_cast({re_include_interval_for_syncing, Left, Right}, State) ->
	#sync_data_state{ skip_intervals_table = TID } = State,
	ar_ets_intervals:delete(TID, Right, Left),
	{noreply, State};

handle_cast(sync_interval, State) ->
	#sync_data_state{ sync_intervals_queue = Q,
			sync_intervals_queue_intervals = QIntervals } = State,
	case gb_sets:is_empty(Q) of
		true ->
			ar_util:cast_after(500, self(), sync_interval),
			{noreply, State};
		false ->
			{{Start, End, _Peer} = Interval, Q2} = gb_sets:take_smallest(Q),
			gen_server:cast(self(), {sync_chunk, [Interval], loop}),
			{noreply, State#sync_data_state{ sync_intervals_queue = Q2,
					sync_intervals_queue_intervals = ar_intervals:delete(QIntervals, End,
							Start) }}
	end;

handle_cast({sync_chunk, [{Byte, RightBound, _} | SubIntervals], Loop}, State)
		when Byte >= RightBound ->
	gen_server:cast(self(), {sync_chunk, SubIntervals, Loop}),
	{noreply, State};
handle_cast({sync_chunk, [], loop}, State) ->
	gen_server:cast(self(), sync_interval),
	{noreply, State};
handle_cast({sync_chunk, [], do_not_loop}, State) ->
	{noreply, State};
handle_cast({sync_chunk, _, _} = Cast,
		#sync_data_state{ sync_disk_space = false } = State) ->
	ar_util:cast_after(ar_disksup:get_disk_space_check_frequency(), self(), Cast),
	{noreply, State};
handle_cast({sync_chunk, [{Byte, RightBound, Peer} | SubIntervals], Loop} = Cast, State) ->
	#sync_data_state{ skip_intervals_table = TID, store_id = StoreID } = State,
	case is_sync_buffer_full(StoreID) orelse ar_packing_server:is_buffer_full() of
		true ->
			ar_util:cast_after(1000, self(), Cast),
			{noreply, State};
		false ->
			Byte2 = ar_tx_blacklist:get_next_not_blacklisted_byte(Byte + 1),
			case Byte2 > Byte + 1 of
				true ->
					temporarily_exclude_interval_from_syncing(Byte, Byte2 - 1, TID, self());
				false ->
					ok
			end,
			case Byte2 - 1 >= RightBound of
				true ->
					gen_server:cast(self(), {sync_chunk, SubIntervals, Loop}),
					{noreply, State};
				false ->
					Self = self(),
					spawn_link(
						fun() ->
							process_flag(trap_exit, true),
							Fun = case ar_peers:get_peer_release(Peer) >= 42 of
									true -> fun ar_http_iface_client:get_chunk_binary/3;
									false -> fun ar_http_iface_client:get_chunk_json/3 end,
							case Fun(Peer, Byte2, any) of
								{ok, Proof, Time, TransferSize} ->
									gen_server:cast(Self, {store_fetched_chunk, Peer, Time,
											TransferSize, Byte2 - 1, RightBound, Proof,
											SubIntervals, Loop});
								{error, Reason} ->
									?LOG_DEBUG([{event, failed_to_fetch_chunk},
											{peer, ar_util:format_peer(Peer)},
											{byte, Byte2},
											{reason, io_lib:format("~p", [Reason])}]),
									ar_events:send(peer, {bad_response,
											{Peer, chunk, Reason}}),
									decrement_sync_buffer_size(StoreID),
									gen_server:cast(Self, {sync_chunk, SubIntervals, Loop})
							end
						end
					),
					increment_sync_buffer_size(StoreID),
					{noreply, State}
			end
	end;

handle_cast({store_fetched_chunk, Peer, Time, TransferSize, Byte, RightBound, Proof,
		SubIntervals, Loop}, State) ->
	#sync_data_state{ packing_map = PackingMap, store_id = StoreID } = State,
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,
	SeekByte = get_chunk_seek_offset(Byte + 1) - 1,
	{BlockStartOffset, BlockEndOffset, TXRoot} = ar_block_index:get_block_bounds(SeekByte),
	BlockSize = BlockEndOffset - BlockStartOffset,
	Offset = SeekByte - BlockStartOffset,
	{Strict, ValidateDataPathFun} =
		case BlockStartOffset >= ?STRICT_DATA_SPLIT_THRESHOLD of
			true ->
				{true, fun ar_merkle:validate_path_strict_data_split/4};
			false ->
				{false, fun ar_merkle:validate_path_strict_borders/4}
		end,
	case validate_proof(TXRoot, BlockStartOffset, Offset, BlockSize, Proof,
			ValidateDataPathFun) of
		{need_unpacking, AbsoluteOffset, ChunkArgs, VArgs} ->
			ar_events:send(peer, {served_chunk, Peer, Time, TransferSize}),
			gen_server:cast(self(), {sync_chunk, [{get_chunk_padded_offset(AbsoluteOffset) + 1,
					RightBound, Peer} | SubIntervals], Loop}),
			{Packing, DataRoot, TXStartOffset, ChunkEndOffset, TXSize, ChunkID} = VArgs,
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			Args = {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
					Chunk, ChunkID, ChunkEndOffset, Strict, Peer, Byte},
			case maps:is_key(AbsoluteOffset, PackingMap) of
				true ->
					?LOG_DEBUG([{event, fetched_chunk_already_being_packed},
							{offset, AbsoluteOffset}, {byte, Byte}]),
					Byte2 = get_chunk_padded_offset(AbsoluteOffset) + 1,
					decrement_sync_buffer_size(StoreID),
					gen_server:cast(self(), {sync_chunk, [{Byte2, RightBound, Peer}
							| SubIntervals], Loop}),
					{noreply, State};
				false ->
					?LOG_DEBUG([{event, schedule_fetched_chunk_unpacking},
							{offset, AbsoluteOffset}]),
					ar_events:send(chunk, {unpack_request, AbsoluteOffset, ChunkArgs}),
					ar_util:cast_after(600000, self(),
							{expire_unpack_fetched_chunk_request, AbsoluteOffset}),
					{noreply, State#sync_data_state{
							packing_map = PackingMap#{
								AbsoluteOffset => {unpack_fetched_chunk, Args} } }}
			end;
		false ->
			gen_server:cast(self(), {sync_chunk, SubIntervals, Loop}),
			process_invalid_fetched_chunk(Peer, Byte, State);
		{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize, ChunkSize, ChunkID} ->
			ar_events:send(peer, {served_chunk, Peer, Time, TransferSize}),
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			AbsoluteEndOffset = AbsoluteTXStartOffset + ChunkEndOffset,
			gen_server:cast(self(), {sync_chunk,
					[{get_chunk_padded_offset(AbsoluteEndOffset) + 1, RightBound, Peer}
					| SubIntervals], Loop}),
			ChunkArgs = {unpacked, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize},
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			Args = {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
					Chunk, ChunkID, ChunkEndOffset, Strict, Peer, Byte},
			process_valid_fetched_chunk(ChunkArgs, Args, State)
	end;

handle_cast(process_disk_pool_item, #sync_data_state{ sync_disk_space = false } = State) ->
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
	{noreply, State};
handle_cast(process_disk_pool_item, #sync_data_state{ disk_pool_scan_pause = true } = State) ->
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
	{noreply, State};
handle_cast(process_disk_pool_item, State) ->
	#sync_data_state{ disk_pool_cursor = Cursor, disk_pool_chunks_index = DiskPoolChunksIndex,
			disk_pool_full_scan_start_key = FullScanStartKey,
			disk_pool_full_scan_start_timestamp = Timestamp,
			currently_processed_disk_pool_keys = CurrentlyProcessedDiskPoolKeys } = State,
	NextKey =
		case ar_kv:get_next(DiskPoolChunksIndex, Cursor) of
			{ok, Key1, Value1} ->
				case sets:is_element(Key1, CurrentlyProcessedDiskPoolKeys) of
					true ->
						none;
					false ->
						{ok, Key1, Value1}
				end;
			none ->
				case ar_kv:get_next(DiskPoolChunksIndex, first) of
					none ->
						none;
					{ok, Key2, Value2} ->
						case sets:is_element(Key2, CurrentlyProcessedDiskPoolKeys) of
							true ->
								none;
							false ->
								{ok, Key2, Value2}
						end
				end
		end,
	case NextKey of
		none ->
			ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), resume_disk_pool_scan),
			ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(), process_disk_pool_item),
			{noreply, State#sync_data_state{ disk_pool_cursor = first,
					disk_pool_full_scan_start_key = none, disk_pool_scan_pause = true }};
		{ok, Key3, Value3} ->
			case FullScanStartKey of
				none ->
					process_disk_pool_item(State#sync_data_state{
							disk_pool_full_scan_start_key = Key3,
							disk_pool_full_scan_start_timestamp = erlang:timestamp() },
							Key3, Value3);
				Key3 ->
					TimePassed = timer:now_diff(erlang:timestamp(), Timestamp),
					case TimePassed < (?DISK_POOL_SCAN_DELAY_MS) * 1000 of
						true ->
							ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(),
									resume_disk_pool_scan),
							ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, self(),
									process_disk_pool_item),
							{noreply, State#sync_data_state{ disk_pool_cursor = first,
									disk_pool_full_scan_start_key = none,
									disk_pool_scan_pause = true }};
						false ->
							process_disk_pool_item(State, Key3, Value3)
					end;
				_ ->
					process_disk_pool_item(State, Key3, Value3)
			end
	end;

handle_cast(resume_disk_pool_scan, State) ->
	{noreply, State#sync_data_state{ disk_pool_scan_pause = false }};

handle_cast({process_disk_pool_chunk_offsets, Iterator, MayConclude, Args}, State) ->
	#sync_data_state{ disk_pool_chunks_index = DiskPoolChunksIndex } = State,
	{Offset, _, ChunkSize, DataRoot, _, ChunkDataKey, Key, _} = Args,
	%% Place the chunk under its last 10 offsets in the weave (the same data
	%% may be uploaded several times).
	case data_root_index_next_v2(Iterator, 10) of
		none ->
			State2 =
				case MayConclude of
					true ->
						?LOG_DEBUG([{event, removing_disk_pool_chunk},
								{key, ar_util:encode(Key)},
								{data_doot, ar_util:encode(DataRoot)}]),
						ok = ar_kv:delete(DiskPoolChunksIndex, Key),
						ok = delete_disk_pool_chunk(ChunkDataKey, State),
						DataRootKey = data_root_index_get_key(Iterator),
						decrease_occupied_disk_pool_size(ChunkSize, DataRootKey),
						may_be_reset_disk_pool_full_scan_key(Key, State);
					false ->
						State
				end,
			gen_server:cast(self(), process_disk_pool_item),
			{noreply, deregister_currently_processed_disk_pool_key(Key, State2)};
		{TXArgs, Iterator2} ->
			State2 = register_currently_processed_disk_pool_key(Key, State),
			{TXStartOffset, TXRoot, TXPath} = TXArgs,
			AbsoluteOffset = TXStartOffset + Offset,
			process_disk_pool_chunk_offset(Iterator2, TXRoot, TXPath, AbsoluteOffset,
					MayConclude, Args, State2)
	end;

handle_cast({remove_range, End, Cursor, Ref, PID}, State) when Cursor > End ->
	PID ! {removed_range, Ref},
	{noreply, State};
handle_cast({remove_range, End, Cursor, Ref, PID}, State) ->
	#sync_data_state{ chunks_index = ChunksIndex, store_id = StoreID,
			recently_processed_disk_pool_offsets = RecentlyProcessedOffsets } = State,
	case get_chunk_by_byte(ChunksIndex, Cursor) of
		{ok, Key, Chunk} ->
			<< AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >> = Key,
			case AbsoluteEndOffset > End of
				true ->
					PID ! {removed_range, Ref},
					{noreply, State};
				false ->
					{_, _, _, _, _, ChunkSize} = binary_to_term(Chunk),
					PaddedStartOffset = get_chunk_padded_offset(AbsoluteEndOffset - ChunkSize),
					PaddedOffset = get_chunk_padded_offset(AbsoluteEndOffset),
					%% 1) store updated sync record
					%% 2) remove chunk
					%% 3) update chunks_index
					%%
					%% The order is important - in case the VM crashes,
					%% we will not report false positives to peers,
					%% and the chunk can still be removed upon retry.
					ok = ar_sync_record:delete(PaddedOffset, PaddedStartOffset, ?MODULE,
							StoreID),
					ok = ar_chunk_storage:delete(PaddedOffset, StoreID),
					ok = ar_kv:delete(ChunksIndex, Key),
					NextCursor = AbsoluteEndOffset + 1,
					gen_server:cast(self(), {remove_range, End, NextCursor, Ref, PID}),
					{noreply, State#sync_data_state{
							recently_processed_disk_pool_offsets = maps:remove(
									AbsoluteEndOffset, RecentlyProcessedOffsets) }}
			end;
		{error, invalid_iterator} ->
			%% get_chunk_by_byte looks for a key with the same prefix or the next prefix.
			%% Therefore, if there is no such key, it does not make sense to look for any
			%% key smaller than the prefix + 2 in the next iteration.
			PrefixSpaceSize =
					trunc(math:pow(2, ?OFFSET_KEY_BITSIZE - ?OFFSET_KEY_PREFIX_BITSIZE)),
			NextCursor = ((Cursor div PrefixSpaceSize) + 2) * PrefixSpaceSize,
			gen_server:cast(self(), {remove_range, End, NextCursor, Ref, PID}),
			{noreply, State};
		{error, Reason} ->
			?LOG_ERROR([{event, data_removal_aborted_since_failed_to_query_chunk},
					{offset, Cursor}, {reason, Reason}]),
			{noreply, State}
	end;

handle_cast({expire_repack_chunk_request, Offset}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Offset, PackingMap, not_found) of
		{pack_fetched_chunk, _} ->
			{noreply, State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) }};
		{pack_disk_pool_chunk, _, _} ->
			{noreply, State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) }};
		{repack_stored_chunk, _, _} ->
			{noreply, State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) }};
		_ ->
			{noreply, State}
	end;

handle_cast({expire_unpack_fetched_chunk_request, Offset}, State) ->
	#sync_data_state{ packing_map = PackingMap, store_id = StoreID } = State,
	case maps:get(Offset, PackingMap, not_found) of
		{unpack_fetched_chunk, _Args} ->
			decrement_sync_buffer_size(StoreID),
			{noreply, State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) }};
		_ ->
			{noreply, State}
	end;

handle_cast(store_sync_state, State) ->
	store_sync_state(State),
	ar_util:cast_after(?STORE_STATE_FREQUENCY_MS, self(), store_sync_state),
	{noreply, State};

handle_cast(repack_stored_chunks, #sync_data_state{ packing_disabled = true } = State) ->
	{noreply, State};
handle_cast(repack_stored_chunks,
		#sync_data_state{ packing_2_6_threshold = infinity } = State) ->
	ar_util:cast_after(30000, self(), repack_stored_chunks),
	{noreply, State};
handle_cast(repack_stored_chunks, State) ->
	case ar_packing_server:is_buffer_full() of
		true ->
			ar_util:cast_after(1000, self(), repack_stored_chunks),
			{noreply, State};
		false ->
			#sync_data_state{ packing_2_6_threshold = PackingThreshold,
					repacking_cursor = Cursor, disk_pool_threshold = DiskPoolThreshold,
					mining_address = MiningAddress, store_id = StoreID } = State,
			SearchStart = max(Cursor, PackingThreshold),
			RightBound = DiskPoolThreshold,
			Range =
				case ar_sync_record:get_next_synced_interval(SearchStart, RightBound,
						spora_2_5, ?MODULE, StoreID) of
					not_found ->
						not_found;
					{End2, Start2} ->
						{End2, max(Start2, PackingThreshold), {spora_2_6, MiningAddress}}
				end,
			case Range of
				not_found ->
					ar_util:cast_after(10000, self(), repack_stored_chunks),
					{noreply, State#sync_data_state{ repacking_cursor = 0 }};
				{End3, Start3, Packing} ->
					?LOG_DEBUG([{event, picked_interval_for_repacking}, {left, Start3},
							{right, End3}, {packing, format_packing(Packing)}]),
					gen_server:cast(self(), {repack_stored_chunks, Start3, End3, Packing}),
					{noreply, State}
		end
	end;

handle_cast({repack_stored_chunks, Offset, End, _RequiredPacking}, State) when Offset >= End ->
	ar_util:cast_after(200, self(), repack_stored_chunks),
	{noreply, State#sync_data_state{ repacking_cursor = End }};
handle_cast({repack_stored_chunks, Offset, End, RequiredPacking}, State) ->
	case ar_packing_server:is_buffer_full() of
		true ->
			ar_util:cast_after(1000, self(), {repack_stored_chunks, Offset, End,
					RequiredPacking}),
			{noreply, State};
		false ->
			#sync_data_state{ chunks_index = ChunksIndex, packing_map = PackingMap,
					store_id = StoreID } = State,
			CheckPacking =
				case ar_sync_record:is_recorded(Offset + 1, ?MODULE, StoreID) of
					{true, RequiredPacking} ->
						skip;
					{true, Packing} ->
						{ok, Packing};
					Reply ->
						?LOG_DEBUG([{event, unexpected_sync_record_check},
								{offset, Offset}, {reply, io_lib:format("~p", [Reply])}]),
						skip
				end,
			ReadChunk =
				case CheckPacking of
					skip ->
						skip;
					{ok, Packing2} ->
						case ar_chunk_storage:get(Offset, StoreID) of
							not_found ->
								%% This chunk should have only recently fallen below the disk
								%% pool threshold and a disk pool job did not pick it up just
								%% yet.
								skip;
							{Offset2, Chunk} ->
								{ok, Offset2, Chunk, Packing2}
					end
			end,
			ReadChunkMetadata =
				case ReadChunk of
					skip ->
						skip;
					{ok, MaybePaddedOffset, Chunk2, Packing3} ->
						SeekOffset = get_chunk_seek_offset(MaybePaddedOffset),
						case get_chunk_by_byte(ChunksIndex, SeekOffset) of
							{error, Reason} ->
								?LOG_WARNING([{event, failed_to_read_chunk_metadata},
										{error, io_lib:format("~p", [Reason])},
										{seek_offset, SeekOffset}]),
								skip;
							{ok, Key, Metadata} ->
								{ok, Key, Metadata, Chunk2, Packing3}
						end
				end,
			case ReadChunkMetadata of
				skip ->
					gen_server:cast(self(), {repack_stored_chunks, Offset + ?DATA_CHUNK_SIZE,
							End, RequiredPacking}),
					{noreply, State};
				{ok, Key2, Metadata2, Chunk3, Packing4} ->
					 << AbsoluteOffset:?OFFSET_KEY_BITSIZE >> = Key2,
					 case maps:is_key(AbsoluteOffset, PackingMap) of
						true ->
							gen_server:cast(self(), {repack_stored_chunks,
									Offset + ?DATA_CHUNK_SIZE, End, RequiredPacking}),
							{noreply, State};
						false ->
							{ChunkDataKey, TXRoot, DataRoot, TXPath, RelativeOffset,
									ChunkSize} = binary_to_term(Metadata2),
							case read_data_path(ChunkDataKey, State) of
								not_found ->
									?LOG_WARNING([{event, chunk_data_path_not_found},
											{offset, Offset},
											{data_key, ar_util:encode(ChunkDataKey)}]),
									gen_server:cast(self(), {repack_stored_chunks,
											Offset + ?DATA_CHUNK_SIZE, End, RequiredPacking}),
									{noreply, State};
								DataPath ->
									gen_server:cast(self(), {repack_stored_chunks,
											Offset + ChunkSize, End, RequiredPacking}),
									ar_events:send(chunk, {repack_request, AbsoluteOffset,
											{RequiredPacking, Packing4, Chunk3, AbsoluteOffset,
											TXRoot, ChunkSize}}),
									ar_util:cast_after(600000, self(),
											{expire_repack_chunk_request, AbsoluteOffset}),
									Args = {DataRoot, DataPath, TXPath, RelativeOffset},
									{noreply, State#sync_data_state{
										packing_map = maps:put(AbsoluteOffset,
												{repack_stored_chunk, RequiredPacking,
														Packing4, Args}, PackingMap) }}
							end
					end
			end
	end;

handle_cast({remove_recently_processed_disk_pool_offset, Offset, ChunkDataKey}, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(Offset, Map, sets:new()),
	Map2 = maps:put(Offset, sets:del_element(ChunkDataKey, Set), Map),
	{noreply, State#sync_data_state{ recently_processed_disk_pool_offsets = Map2 }};

handle_cast({store_chunk, ChunkArgs, Args}, State) ->
	store_chunk2(ChunkArgs, Args, State),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_info({event, node_state, {initialized, _B}},
		#sync_data_state{ store_id = "default" } = State) ->
	{noreply, State};
handle_info({event, node_state, {initialized, _B}}, State) ->
	run_sync_jobs(),
	{noreply, State};

handle_info({event, node_state, {search_space_upper_bound, Bound}}, State) ->
	{noreply, State#sync_data_state{ disk_pool_threshold = Bound }};

handle_info({event, node_state, _}, State) ->
	{noreply, State};

handle_info({event, chunk, {unpacked, Offset, ChunkArgs}}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Offset, PackingMap, not_found) of
		{unpack_fetched_chunk, Args} ->
			State2 = State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) },
			process_unpacked_chunk(ChunkArgs, Args, State2);
		_ ->
			{noreply, State}
	end;

handle_info({event, chunk, {packed, Offset, ChunkArgs}}, State) ->
	#sync_data_state{ packing_map = PackingMap, store_id = StoreID } = State,
	Packing = element(1, ChunkArgs),
	case maps:get(Offset, PackingMap, not_found) of
		{pack_fetched_chunk, Args} when element(1, Args) == Packing ->
			?LOG_DEBUG([{event, storing_packed_fetched_chunk}, {offset, Offset}]),
			State2 = State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) },
			store_chunk(ChunkArgs, Args, State2),
			{noreply, State2};
		{pack_disk_pool_chunk, ChunkDataKey, Args} when element(1, Args) == Packing ->
			?LOG_DEBUG([{event, storing_packed_disk_pool_chunk}, {offset, Offset}]),
			State2 = State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) },
			case store_chunk(ChunkArgs, Args, State2) of
				ok ->
					{_, _, AbsoluteOffset, _, _} = ChunkArgs,
					{noreply, cache_recently_processed_offset(AbsoluteOffset, ChunkDataKey,
							State2)};
				_Error ->
					{noreply, State2}
			end;
		{repack_stored_chunk, ExpectedPacking, PreviousPacking, Args}
				when ExpectedPacking == Packing ->
			?LOG_DEBUG([{event, storing_repacked_stored_chunk}, {offset, Offset},
					{packing, format_packing(Packing)},
					{store_id, StoreID}]),
			store_repacked_chunk(ChunkArgs, PreviousPacking, Args,
					State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) });
		_ ->
			{noreply, State}
	end;

handle_info({event, chunk, _}, State) ->
	{noreply, State};

handle_info({'EXIT', _PID, normal}, State) ->
	{noreply, State};

handle_info({'DOWN', _,  process, _, normal}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, noproc}, State) ->
	{noreply, State};
handle_info({'DOWN', _,  process, _, Reason}, State) ->
	?LOG_WARNING([{event, collect_intervals_job_failed},
			{reason, io_lib:format("~p", [Reason])}, {action, spawning_another_one}]),
	gen_server:cast(self(), collect_sync_intervals),
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(Reason, State) ->
	#sync_data_state{ chunks_index = {DB, _}, chunk_data_db = ChunkDataDB,
			disk_pool_chunks_index = DiskPoolIndexDB } = State,
	?LOG_INFO([{event, terminate}, {reason, io_lib:format("~p", [Reason])}]),
	store_sync_state(State),
	ar_kv:close(DB),
	ar_kv:close(DiskPoolIndexDB),
	ar_kv:close(ChunkDataDB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

remove_expired_disk_pool_data_roots() ->
	Now = os:system_time(microsecond),
	{ok, Config} = application:get_env(arweave, config),
	ExpirationTime = Config#config.disk_pool_data_root_expiration_time * 1000000,
	ets:foldl(
		fun({Key, {_Size, Timestamp, _TXIDSet}}, _Acc) ->
			case Timestamp + ExpirationTime > Now of
				true ->
					ok;
				false ->
					ets:delete(ar_disk_pool_data_roots, Key),
					ok
			end
		end,
		ok,
		ar_disk_pool_data_roots
	).

get_chunk(Offset, Pack, Packing, StoredPacking, Options, StoreID) ->
	case maps:get(search_fast_storage_only, Options, false) of
		true ->
			case maps:get(bucket_based_offset, Options, true) of
				false ->
					{error, option_set_not_supported};
				true ->
					get_chunk_from_fast_storage(Offset, Pack, Packing, StoredPacking, StoreID)
			end;
		false ->
			case ets:lookup(ar_data_sync_state, {chunks_index, StoreID}) of
				[] ->
					{error, not_joined};
				[{_, ChunksIndex}] ->
					[{_, ChunkDataDB}] = ets:lookup(ar_data_sync_state,
							{chunk_data_db, StoreID}),
					get_chunk(Offset, Pack, Packing, StoredPacking, ChunksIndex, ChunkDataDB,
							maps:get(bucket_based_offset, Options, true), StoreID)
			end
	end.

get_chunk_from_fast_storage(Offset, Pack, Packing, StoredPacking, StoreID) ->
	case ar_chunk_storage:get(Offset - 1, StoreID) of
		not_found ->
			{error, chunk_not_found};
		{_Offset, Chunk} ->
			case ar_sync_record:is_recorded(Offset, StoredPacking, ?MODULE, StoreID) of
				false ->
					%% The chunk should have been re-packed
					%% in the meantime - very unlucky timing.
					{error, chunk_not_found};
				true ->
					case {Pack, Packing == StoredPacking} of
						{false, true} ->
							{ok, Chunk};
						{false, false} ->
							{error, chunk_not_found};
						{true, true} ->
							{ok, Chunk};
						{true, false} ->
							{error, option_set_not_supported}
					end
			end
	end.

get_chunk(Offset, Pack, Packing, StoredPacking, ChunksIndex, ChunkDataDB,
		IsBucketBasedOffset, StoreID) ->
	SeekOffset =
		case IsBucketBasedOffset of
			true ->
				get_chunk_seek_offset(Offset);
			false ->
				Offset
		end,
	ReadChunkKeyResult =
		case get_chunk_by_byte(ChunksIndex, SeekOffset) of
			{error, _} ->
				{error, chunk_not_found};
			Reply ->
				Reply
		end,
	CheckOffsetResult =
		case ReadChunkKeyResult of
			{error, Reason} ->
				{error, Reason};
			{ok, Key, Value} ->
				<< C:?OFFSET_KEY_BITSIZE >> = Key,
				{K, Root, _, P, _, S} = binary_to_term(Value),
				case C - SeekOffset >= S of
					true ->
						invalidate_bad_data_record(SeekOffset - 1, C - S, ChunksIndex,
								StoreID, 1),
						{error, chunk_not_found};
					false ->
						{ok, C, K, Root, P, S}
				end
		end,
	ReadChunkResult =
		case CheckOffsetResult of
			{error, Reason2} ->
				{error, Reason2};
			{ok, O, ChunkDataKey, Root2, P2, S2} ->
				case read_chunk(O, ChunkDataDB, ChunkDataKey, StoreID) of
					not_found ->
						invalidate_bad_data_record(SeekOffset - 1, O, ChunksIndex,
								StoreID, 2),
						{error, chunk_not_found};
					{error, reason} ->
						?LOG_ERROR([{event, failed_to_read_chunk}, {reason, reason}]),
						{error, failed_to_read_chunk};
					{ok, ChunkData} ->
						case ar_sync_record:is_recorded(Offset, StoredPacking, ?MODULE,
								StoreID) of
							false ->
								%% The chunk should have been re-packed
								%% in the meantime - very unlucky timing.
								{error, chunk_not_found};
							true ->
								{ok, ChunkData, O, Root2, S2, P2}
						end
				end
		end,
	ValidateChunk =
		case ReadChunkResult of
			{error, Reason3} ->
				{error, Reason3};
			{ok, {Chunk, DataPath}, ChunkOffset, TXRoot, ChunkSize, TXPath} ->
				case validate_served_chunk({ChunkOffset, DataPath, TXPath, TXRoot, ChunkSize,
						ChunksIndex, StoreID}) of
					{true, ChunkID} ->
						{ok, {Chunk, DataPath}, ChunkOffset, TXRoot, ChunkSize, TXPath,
								ChunkID};
					false ->
						{error, chunk_not_found}
				end
		end,
	case ValidateChunk of
		{error, Reason4} ->
			{error, Reason4};
		{ok, {Chunk2, DataPath2}, ChunkOffset2, TXRoot2, ChunkSize2, TXPath2, ChunkID2} ->
			PackResult =
				case {Pack, Packing == StoredPacking} of
					{false, true} ->
						U = case StoredPacking of unpacked -> Chunk2; _ -> none end,
						{ok, {Chunk2, U, ChunkID2}};
					{false, false} ->
						{error, chunk_not_found};
					{true, true} ->
						U = case StoredPacking of unpacked -> Chunk2; _ -> none end,
						{ok, {Chunk2, U, ChunkID2}};
					{true, false} ->
						{ok, Unpacked} = ar_packing_server:unpack(StoredPacking, ChunkOffset2,
								TXRoot2, Chunk2, ChunkSize2),
						{ok, Packed} = ar_packing_server:pack(Packing, ChunkOffset2, TXRoot2,
								Unpacked),
						{ok, {Packed, Unpacked, ChunkID2}}
				end,
			case PackResult of
				{ok, {PackedChunk, none, _ChunkID3}} ->
					Proof = #{ tx_root => TXRoot2, chunk => PackedChunk,
							data_path => DataPath2, tx_path => TXPath2 },
					{ok, Proof};
				{ok, {PackedChunk, _MaybeUnpackedChunk, none}} ->
					Proof = #{ tx_root => TXRoot2, chunk => PackedChunk,
							data_path => DataPath2, tx_path => TXPath2 },
					{ok, Proof};
				{ok, {PackedChunk, MaybeUnpackedChunk, ChunkID3}} ->
					case ar_tx:generate_chunk_id(MaybeUnpackedChunk) == ChunkID3 of
						true ->
							Proof = #{ tx_root => TXRoot2, chunk => PackedChunk,
									data_path => DataPath2, tx_path => TXPath2 },
							{ok, Proof};
						false ->
							invalidate_bad_data_record(ChunkOffset2 - ChunkSize2, ChunkOffset2,
									ChunksIndex, StoreID, 3),
							{error, chunk_not_found}
					end;
				Error ->
					Error
			end
	end.

invalidate_bad_data_record(Start, End, ChunksIndex, StoreID, Case) ->
	[{_, T}] = ets:lookup(ar_data_sync_state, disk_pool_threshold),
	case End > T of
		true ->
			%% Do not invalidate fresh records - a reorg may be in progress.
			ok;
		false ->
			PaddedEnd = get_chunk_padded_offset(End),
			?LOG_WARNING([{event, invalidating_bad_data_record}, {type, Case},
					{range_start, Start}, {range_end, PaddedEnd}]),
			ok = ar_sync_record:delete(PaddedEnd, Start, ?MODULE, StoreID),
			ok = ar_kv:delete(ChunksIndex, << End:?OFFSET_KEY_BITSIZE >>)
	end.

validate_served_chunk(Args) ->
	{Offset, DataPath, TXPath, TXRoot, ChunkSize, ChunksIndex, StoreID} = Args,
	[{_, T}] = ets:lookup(ar_data_sync_state, disk_pool_threshold),
	case Offset > T orelse not ar_node:is_joined() of
		true ->
			{true, none};
		false ->
			case ar_block_index:get_block_bounds(Offset - 1) of
				{BlockStart, BlockEnd, TXRoot} ->
					{_Strict, ValidateDataPathFun} =
						case BlockStart >= ?STRICT_DATA_SPLIT_THRESHOLD of
							true ->
								{true, fun ar_merkle:validate_path_strict_data_split/4};
							false ->
								{false, fun ar_merkle:validate_path_strict_borders/4}
						end,
					BlockSize = BlockEnd - BlockStart,
					ChunkOffset = Offset - BlockStart - 1,
					case validate_proof2({TXRoot, ChunkOffset, BlockSize, DataPath, TXPath,
							ChunkSize, ValidateDataPathFun}) of
						{true, ChunkID} ->
							{true, ChunkID};
						false ->
							StartOffset = Offset - ChunkSize,
							invalidate_bad_data_record(StartOffset, Offset, ChunksIndex,
									StoreID, 3),
							false
					end;
				{_BlockStart, _BlockEnd, TXRoot2} ->
					?LOG_WARNING([{event, stored_chunk_invalid_tx_root},
							{byte, Offset - 1}, {tx_root, ar_util:encode(TXRoot2)},
							{stored_tx_root, ar_util:encode(TXRoot)}]),
					invalidate_bad_data_record(Offset - ChunkSize, Offset, ChunksIndex,
							StoreID, 4),
					false
			end
	end.

%% @doc Return Offset if it is smaller than or equal to ?STRICT_DATA_SPLIT_THRESHOLD.
%% Otherwise, return the offset of the first byte of the chunk + 1. The function
%% returns an offset the chunk can be found under even if Offset is inside padding.
get_chunk_seek_offset(Offset) ->
	case Offset > ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			ar_poa:get_padded_offset(Offset, ?STRICT_DATA_SPLIT_THRESHOLD)
					- (?DATA_CHUNK_SIZE)
					+ 1;
		false ->
			Offset
	end.

%% @doc Return Offset if it is smaller than or equal to ?STRICT_DATA_SPLIT_THRESHOLD.
%% Otherwise, return the offset of the last byte of the chunk + the size of the padding.
get_chunk_padded_offset(Offset) ->
	case Offset > ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			ar_poa:get_padded_offset(Offset, ?STRICT_DATA_SPLIT_THRESHOLD);
		false ->
			Offset
	end.

get_chunk_by_byte(ChunksIndex, Byte) ->
	ar_kv:get_next_by_prefix(ChunksIndex, ?OFFSET_KEY_PREFIX_BITSIZE, ?OFFSET_KEY_BITSIZE,
			<< Byte:?OFFSET_KEY_BITSIZE >>).

read_chunk(Offset, ChunkDataDB, ChunkDataKey, StoreID) ->
	case ar_kv:get(ChunkDataDB, ChunkDataKey) of
		not_found ->
			not_found;
		{ok, Value} ->
			case binary_to_term(Value) of
				{Chunk, DataPath} ->
					{ok, {Chunk, DataPath}};
				DataPath ->
					case ar_chunk_storage:get(Offset - 1, StoreID) of
						not_found ->
							not_found;
						{_EndOffset, Chunk} ->
							{ok, {Chunk, DataPath}}
					end
			end;
		Error ->
			Error
	end.

get_tx_offset(TXIndex, TXID) ->
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{ok, binary_to_term(Value)};
		not_found ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_read_tx_offset}, {reason, Reason}]),
			{error, failed_to_read_offset}
	end.

get_tx_data(Start, End, Chunks) when Start >= End ->
	{ok, iolist_to_binary(Chunks)};
get_tx_data(Start, End, Chunks) ->
	case get_chunk(Start + 1, #{ pack => true, packing => unpacked,
			bucket_based_offset => false }) of
		{ok, #{ chunk := Chunk }} ->
			get_tx_data(Start + byte_size(Chunk), End, [Chunks | Chunk]);
		{error, chunk_not_found} ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_get_tx_data},
					{reason, io_lib:format("~p", [Reason])}]),
			{error, failed_to_get_tx_data}
	end.

get_data_root_offset(DataRootKey, StoreID) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	[{_, DataRootIndex}] = ets:lookup(ar_data_sync_state, {data_root_index, StoreID}),
	case ar_kv:get_prev(DataRootIndex, << DataRoot:32/binary,
			(ar_serialize:encode_int(TXSize, 8))/binary, <<"a">>/binary >>) of
		none ->
			not_found;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				OffsetSize:8, Offset:(OffsetSize * 8) >>, TXPath} ->
			{ok, {Offset, TXPath}};
		{ok, _, _} ->
			not_found;
		{error, _} = Error ->
			Error
	end.

remove_range(Start, End, Ref, ReplyTo) ->
	ReplyFun =
		fun(Fun, StorageRefs) ->
			case sets:is_empty(StorageRefs) of
				true ->
					ReplyTo ! {removed_range, Ref},
					ar_events:send(data_sync, {remove_range, Start, End});
				false ->
					receive
						{removed_range, StorageRef} ->
							Fun(Fun, sets:del_element(StorageRef, StorageRefs))
					after 10000 ->
						ok
					end
			end
		end,
	StorageModules = ar_storage_module:get_all(Start, End),
	StoreIDs = ["default" | [ar_storage_module:id(M) || M <- StorageModules]],
	RefL = [make_ref() || _ <- StoreIDs],
	PID = spawn(fun() -> ReplyFun(ReplyFun, sets:from_list(RefL)) end),
	lists:foreach(
		fun({StoreID, R}) ->
			GenServerID = list_to_atom("ar_data_sync_" ++ StoreID),
			gen_server:cast(GenServerID, {remove_range, End, Start + 1, R, PID})
		end,
		lists:zip(StoreIDs, RefL)
	).

init_kv(StoreID) ->
	BasicOpts = [{max_open_files, 10000}],
	BloomFilterOpts = [
		{block_based_table_options, [
			{cache_index_and_filter_blocks, true}, % Keep bloom filters in memory.
			{bloom_filter_policy, 10} % ~1% false positive probability.
		]},
		{optimize_filters_for_hits, true}
	],
	PrefixBloomFilterOpts =
		BloomFilterOpts ++ [
			{prefix_extractor, {capped_prefix_transform, ?OFFSET_KEY_PREFIX_BITSIZE div 8}}],
	ColumnFamilyDescriptors = [
		{"default", BasicOpts},
		{"chunks_index", BasicOpts ++ PrefixBloomFilterOpts},
		{"data_root_index", BasicOpts ++ BloomFilterOpts},
		{"data_root_offset_index", BasicOpts},
		{"tx_index", BasicOpts ++ BloomFilterOpts},
		{"tx_offset_index", BasicOpts},
		{"disk_pool_chunks_index", BasicOpts ++ BloomFilterOpts},
		{"migrations_index", BasicOpts}
	],
	Dir =
		case StoreID of
			"default" ->
				?ROCKS_DB_DIR;
			_ ->
				filename:join(StoreID, ?ROCKS_DB_DIR)
		end,
	{ok, DB, [_, CF1, CF2, CF3, CF4, CF5, CF6, CF7]} =
		ar_kv:open(filename:join(Dir, "ar_data_sync_db"), ColumnFamilyDescriptors),
	{ok, ChunkDataDB} =
		ar_kv:open_without_column_families(
			filename:join(Dir, "ar_data_sync_chunk_db"), [
				{max_open_files, 10000},
				{max_background_compactions, 8},
				{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
				{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
				%% 10 files in L1 to make L1 == L0 as recommended by the
				%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
				{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}
			]
		),
	{ok, DiskPoolIndexDB} =
		ar_kv:open_without_column_families(
			filename:join(Dir, "ar_data_sync_disk_pool_chunks_index_db"), [
				{max_open_files, 1000},
				{max_background_compactions, 8},
				{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
				{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
				%% 10 files in L1 to make L1 == L0 as recommended by the
				%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
				{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}
			] ++ BloomFilterOpts
		),
	{ok, DataRootIndexDB} =
		ar_kv:open_without_column_families(
			filename:join(Dir, "ar_data_sync_data_root_index_db"), [
				{max_open_files, 100},
				{max_background_compactions, 8},
				{write_buffer_size, 256 * 1024 * 1024}, % 256 MiB per memtable.
				{target_file_size_base, 256 * 1024 * 1024}, % 256 MiB per SST file.
				%% 10 files in L1 to make L1 == L0 as recommended by the
				%% RocksDB guide https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
				{max_bytes_for_level_base, 10 * 256 * 1024 * 1024}
			] ++ BloomFilterOpts
		),
	State = #sync_data_state{
		chunks_index = {DB, CF1},
		data_root_index = DataRootIndexDB,
		data_root_index_old = {DB, CF2},
		data_root_offset_index = {DB, CF3},
		tx_index = {DB, CF4},
		tx_offset_index = {DB, CF5},
		disk_pool_chunks_index_old = {DB, CF6},
		disk_pool_chunks_index = DiskPoolIndexDB,
		migrations_index = {DB, CF7},
		chunk_data_db = ChunkDataDB
	},
	ets:insert(ar_data_sync_state, [
		{{chunks_index, StoreID}, {DB, CF1}},
		{{data_root_index, StoreID}, DataRootIndexDB},
		{{data_root_offset_index, StoreID}, {DB, CF3}},
		{{tx_index, StoreID}, {DB, CF4}},
		{{tx_offset_index, StoreID}, {DB, CF5}},
		{{disk_pool_chunks_index, StoreID}, DiskPoolIndexDB},
		{{chunk_data_db, StoreID}, ChunkDataDB}
	]),
	State.

may_be_run_sync_jobs() ->
	case ar_node:is_joined() of
		false ->
			ok;
		true ->
			run_sync_jobs()
	end.

run_sync_jobs() ->
	gen_server:cast(self(), collect_sync_intervals),
	{ok, Config} = application:get_env(arweave, config),
	lists:foreach(
		fun(_SyncingJobNumber) ->
			gen_server:cast(self(), sync_interval)
		end,
		lists:seq(1, Config#config.sync_jobs)
	).

move_disk_pool_index(State) ->
	move_disk_pool_index(first, State).

move_disk_pool_index(Cursor, State) ->
	#sync_data_state{ disk_pool_chunks_index_old = Old,
			disk_pool_chunks_index = New } = State,
	case ar_kv:get_next(Old, Cursor) of
		none ->
			ok;
		{ok, Key, Value} ->
			ok = ar_kv:put(New, Key, Value),
			ok = ar_kv:delete(Old, Key),
			move_disk_pool_index(Key, State)
	end.

move_data_root_index(#sync_data_state{ migrations_index = MI,
		data_root_index_old = DI } = State) ->
	case ar_kv:get(MI, <<"move_data_root_index">>) of
		{ok, <<"complete">>} ->
			ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
			ok;
		{ok, Cursor} ->
			move_data_root_index(Cursor, 1, State);
		not_found ->
			case ar_kv:get_next(DI, last) of
				none ->
					ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
					ok;
				{ok, Key, _} ->
					move_data_root_index(Key, 1, State)
			end
	end.

move_data_root_index(Cursor, N, State) ->
	#sync_data_state{ migrations_index = MI, data_root_index_old = Old,
			data_root_index = New } = State,
	case N rem 50000 of
		0 ->
			?LOG_DEBUG([{event, moving_data_root_index}, {moved_keys, N}]),
			ok = ar_kv:put(MI, <<"move_data_root_index">>, Cursor),
			gen_server:cast(self(), {move_data_root_index, Cursor, N + 1});
		_ ->
			case ar_kv:get_prev(Old, Cursor) of
				none ->
					ok = ar_kv:put(MI, <<"move_data_root_index">>, <<"complete">>),
					ets:insert(ar_data_sync_state, {move_data_root_index_migration_complete}),
					ok;
				{ok, << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>, Value} ->
					M = binary_to_term(Value),
					move_data_root_index(DataRoot, TXSize, data_root_index_iterator(M), New),
					PrevKey = << DataRoot:32/binary, (TXSize - 1):?OFFSET_KEY_BITSIZE >>,
					move_data_root_index(PrevKey, N + 1, State);
				{ok, Key, _} ->
					%% The empty data root key (from transactions without data) was
					%% unnecessarily recorded in the index.
					PrevKey = binary:part(Key, 0, byte_size(Key) - 1),
					move_data_root_index(PrevKey, N + 1, State)
			end
	end.

move_data_root_index(DataRoot, TXSize, Iterator, DB) ->
	case data_root_index_next(Iterator, infinity) of
		none ->
			ok;
		{{Offset, _TXRoot, TXPath}, Iterator2} ->
			Key = data_root_key_v2(DataRoot, TXSize, Offset),
			ok = ar_kv:put(DB, Key, TXPath),
			move_data_root_index(DataRoot, TXSize, Iterator2, DB)
	end.

data_root_key_v2(DataRoot, TXSize, Offset) ->
	<< DataRoot:32/binary, (ar_serialize:encode_int(TXSize, 8))/binary,
			(ar_serialize:encode_int(Offset, 8))/binary >>.

record_disk_pool_chunks_count() ->
	[{_, DB}] = ets:lookup(ar_data_sync_state, {disk_pool_chunks_index, "default"}),
	case ar_kv:count(DB) of
		Count when is_integer(Count) ->
			prometheus_gauge:set(disk_pool_chunks_count, Count);
		Error ->
			?LOG_WARNING([{event, failed_to_read_disk_pool_chunks_count},
					{error, io_lib:format("~p", [Error])}])
	end.

read_data_sync_state() ->
	case ar_storage:read_term(data_sync_state) of
		{ok, #{ block_index := RecentBI, disk_pool_data_roots := DiskPoolDataRoots } = M} ->
			maps:merge(M, #{
				packing_2_6_threshold => maps:get(packing_2_6_threshold, M, infinity),
				weave_size => case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
				disk_pool_size => maps:get(disk_pool_size, M,
						calculate_disk_pool_size(DiskPoolDataRoots)),
				disk_pool_threshold => maps:get(disk_pool_threshold, M,
						get_disk_pool_threshold(RecentBI)) });
		not_found ->
			#{ block_index => [], disk_pool_data_roots => #{}, disk_pool_size => 0,
					weave_size => 0, packing_2_5_threshold => infinity,
					packing_2_6_threshold => infinity, disk_pool_threshold => 0 }
	end.

calculate_disk_pool_size(DiskPoolDataRoots) ->
	maps:fold(fun(_, {Size, _, _}, Acc) -> Acc + Size end, 0, DiskPoolDataRoots).

get_disk_pool_threshold([]) ->
	0;
get_disk_pool_threshold(BI) ->
	ar_node:get_partition_upper_bound(BI).

remove_orphaned_data(State, BlockStartOffset, WeaveSize) ->
	ok = remove_tx_index_range(BlockStartOffset, WeaveSize, State),
	{ok, OrphanedDataRoots} = remove_data_root_index_range(BlockStartOffset, WeaveSize, State),
	ok = remove_data_root_offset_index_range(BlockStartOffset, WeaveSize, State),
	ok = remove_chunks_index_range(BlockStartOffset, WeaveSize, State),
	{ok, OrphanedDataRoots}.

remove_tx_index_range(Start, End, State) ->
	#sync_data_state{ tx_offset_index = TXOffsetIndex, tx_index = TXIndex } = State,
	ok = case ar_kv:get_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			ok;
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, TXID, ok) ->
						ar_kv:delete(TXIndex, TXID),
						ar_tx_blacklist:norify_about_orphaned_tx(TXID)
				end,
				ok,
				Map
			);
		Error ->
			Error
	end,
	ar_kv:delete_range(TXOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< End:?OFFSET_KEY_BITSIZE >>).

remove_chunks_index_range(Start, End, State) ->
	#sync_data_state{ chunks_index = ChunksIndex } = State,
	ar_kv:delete_range(ChunksIndex, << (Start + 1):?OFFSET_KEY_BITSIZE >>,
			<< (End + 1):?OFFSET_KEY_BITSIZE >>).

remove_data_root_index_range(Start, End, State) ->
	#sync_data_state{ data_root_offset_index = DataRootOffsetIndex,
			data_root_index = DataRootIndex } = State,
	case ar_kv:get_range(DataRootOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< (End - 1):?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			{ok, sets:new()};
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, Value, {ok, RemovedDataRoots}) ->
						{_TXRoot, _BlockSize, DataRootIndexKeySet} = binary_to_term(Value),
						sets:fold(
							fun (_Key, {error, _} = Error) ->
									Error;
								(<< _DataRoot:32/binary, _TXSize:?OFFSET_KEY_BITSIZE >> = Key,
										{ok, Removed}) ->
									case remove_data_root(DataRootIndex, Key, Start, End) of
										removed ->
											{ok, sets:add_element(Key, Removed)};
										ok ->
											{ok, Removed};
										Error ->
											Error
									end;
								(_, Acc) ->
									Acc
							end,
							{ok, RemovedDataRoots},
							DataRootIndexKeySet
						)
				end,
				{ok, sets:new()},
				Map
			);
		Error ->
			Error
	end.

remove_data_root(DataRootIndex, DataRootKey, Start, End) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	StartKey = data_root_key_v2(DataRoot, TXSize, Start),
	EndKey = data_root_key_v2(DataRoot, TXSize, End),
	case ar_kv:delete_range(DataRootIndex, StartKey, EndKey) of
		ok ->
			case ar_kv:get_prev(DataRootIndex, StartKey) of
				{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
						_Rest/binary >>, _} ->
					ok;
				{ok, _, _} ->
					removed;
				none ->
					removed;
				{error, _} = Error ->
					Error
			end;
		Error ->
			Error
	end.

remove_data_root_offset_index_range(Start, End, State) ->
	#sync_data_state{ data_root_offset_index = DataRootOffsetIndex } = State,
	ar_kv:delete_range(DataRootOffsetIndex, << Start:?OFFSET_KEY_BITSIZE >>,
			<< End:?OFFSET_KEY_BITSIZE >>).

repair_data_root_offset_index(BI, State) ->
	#sync_data_state{ migrations_index = DB } = State,
	case ar_kv:get(DB, <<"repair_data_root_offset_index">>) of
		not_found ->
			?LOG_INFO([{event, starting_data_root_offset_index_scan}]),
			ReverseBI = lists:reverse(BI),
			ResyncBlocks = repair_data_root_offset_index(ReverseBI, <<>>, 0, [], State),
			[ar_header_sync:remove_block(Height) || Height <- ResyncBlocks],
			ok = ar_kv:put(DB, <<"repair_data_root_offset_index">>, <<>>),
			?LOG_INFO([{event, data_root_offset_index_scan_complete}]);
		_ ->
			ok
	end.

repair_data_root_offset_index(BI, Cursor, Height, ResyncBlocks, State) ->
	#sync_data_state{ data_root_offset_index = DRI } = State,
	case ar_kv:get_next(DRI, Cursor) of
		none ->
			ResyncBlocks;
		{ok, Key, Value} ->
			<< BlockStart:?OFFSET_KEY_BITSIZE >> = Key,
			{TXRoot, BlockSize, _DataRootKeys} = binary_to_term(Value),
			BlockEnd = BlockStart + BlockSize,
			case shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks, BI) of
				{ok, {Height2, BI2}} ->
					Cursor2 = << (BlockStart + 1):?OFFSET_KEY_BITSIZE >>,
					repair_data_root_offset_index(BI2, Cursor2, Height2, ResyncBlocks, State);
				{bad_key, []} ->
					ResyncBlocks;
				{bad_key, ResyncBlocks2} ->
					?LOG_INFO([{event, removing_data_root_index_range},
							{range_start, BlockStart}, {range_end, BlockEnd}]),
					ok = remove_tx_index_range(BlockStart, BlockEnd, State),
					{ok, _} = remove_data_root_index_range(BlockStart, BlockEnd, State),
					ok = remove_data_root_offset_index_range(BlockStart, BlockEnd, State),
					repair_data_root_offset_index(BI, Cursor, Height, ResyncBlocks2, State)
			end
	end.

shift_block_index(_TXRoot, _BlockStart, _BlockEnd, _Height, ResyncBlocks, []) ->
	{bad_key, ResyncBlocks};
shift_block_index(TXRoot, BlockStart, BlockEnd, Height, ResyncBlocks,
		[{_H, WeaveSize, _TXRoot} | BI]) when BlockEnd > WeaveSize ->
	ResyncBlocks2 = case BlockStart < WeaveSize of true -> [Height | ResyncBlocks];
			_ -> ResyncBlocks end,
	shift_block_index(TXRoot, BlockStart, BlockEnd, Height + 1, ResyncBlocks2, BI);
shift_block_index(TXRoot, _BlockStart, WeaveSize, Height, _ResyncBlocks,
		[{_H, WeaveSize, TXRoot} | BI]) ->
	{ok, {Height + 1, BI}};
shift_block_index(_TXRoot, _BlockStart, _WeaveSize, Height, ResyncBlocks, _BI) ->
	{bad_key, [Height | ResyncBlocks]}.

have_free_space() ->
	ar_storage:get_free_space(?ROCKS_DB_DIR) > ?DISK_DATA_BUFFER_SIZE
		andalso ar_storage:get_free_space(?CHUNK_DIR) > ?DISK_DATA_BUFFER_SIZE.

add_block(#block{ indep_hash = H, weave_size = WeaveSize, tx_root = TXRoot } = B,
		SizeTaggedTXs, State) ->
	#sync_data_state{ tx_index = TXIndex, tx_offset_index = TXOffsetIndex,
			data_root_offset_index = DRI } = State,
	case ar_block_index:get_element_by_height(B#block.height) of
		{H, WeaveSize, TXRoot} ->
			BlockStart = B#block.weave_size - B#block.block_size,
			case ar_kv:get(DRI, << BlockStart:?OFFSET_KEY_BITSIZE >>) of
				not_found ->
					{ok, _} = add_block_data_roots(State, SizeTaggedTXs, BlockStart),
					ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStart);
				_ ->
					ok
			end;
		_ ->
			ok
	end.

update_tx_index(_TXIndex, _TXOffsetIndex, [], _BlockStartOffset) ->
	ok;
update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset) ->
	lists:foldl(
		fun ({_, Offset}, Offset) ->
				Offset;
			({{padding, _}, Offset}, _) ->
				Offset;
			({{TXID, _}, TXEndOffset}, PreviousOffset) ->
				AbsoluteEndOffset = BlockStartOffset + TXEndOffset,
				TXSize = TXEndOffset - PreviousOffset,
				AbsoluteStartOffset = AbsoluteEndOffset - TXSize,
				case ar_kv:put(TXOffsetIndex, << AbsoluteStartOffset:?OFFSET_KEY_BITSIZE >>,
						TXID) of
					ok ->
						case ar_kv:put(TXIndex, TXID,
								term_to_binary({AbsoluteEndOffset, TXSize})) of
							ok ->
								ar_tx_blacklist:notify_about_added_tx(TXID, AbsoluteEndOffset,
										AbsoluteStartOffset),
								TXEndOffset;
							{error, Reason} ->
								?LOG_ERROR([{event, failed_to_update_tx_index},
										{reason, Reason}]),
								TXEndOffset
						end;
					{error, Reason} ->
						?LOG_ERROR([{event, failed_to_update_tx_offset_index},
								{reason, Reason}]),
						TXEndOffset
				end
		end,
		0,
		SizeTaggedTXs
	),
	ok.

add_block_data_roots(_State, [], _CurrentWeaveSize) ->
	{ok, sets:new()};
add_block_data_roots(State, SizeTaggedTXs, CurrentWeaveSize) ->
	#sync_data_state{ data_root_offset_index = DataRootOffsetIndex } = State,
	SizeTaggedDataRoots = [{Root, Offset} || {{_, Root}, Offset} <- SizeTaggedTXs],
	{TXRoot, TXTree} = ar_merkle:generate_tree(SizeTaggedDataRoots),
	{BlockSize, DataRootIndexKeySet, Args} = lists:foldl(
		fun ({_, Offset}, {Offset, _, _} = Acc) ->
				Acc;
			({{padding, _}, Offset}, {_, Acc1, Acc2}) ->
				{Offset, Acc1, Acc2};
			({{_, DataRoot}, Offset}, {_, Acc1, Acc2}) when byte_size(DataRoot) < 32 ->
				{Offset, Acc1, Acc2};
			({{_, DataRoot}, TXEndOffset}, {PrevOffset, CurrentDataRootSet, CurrentArgs}) ->
				TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
				TXOffset = CurrentWeaveSize + PrevOffset,
				TXSize = TXEndOffset - PrevOffset,
				DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
				{TXEndOffset, sets:add_element(DataRootKey, CurrentDataRootSet),
						[{DataRoot, TXSize, TXOffset, TXPath} | CurrentArgs]}
		end,
		{0, sets:new(), []},
		SizeTaggedTXs
	),
	case BlockSize > 0 of
		true ->
			ok = ar_kv:put(DataRootOffsetIndex, << CurrentWeaveSize:?OFFSET_KEY_BITSIZE >>,
					term_to_binary({TXRoot, BlockSize, DataRootIndexKeySet})),
			lists:foreach(
				fun({DataRoot, TXSize, TXOffset, TXPath}) ->
					ok = update_data_root_index(State, DataRoot, TXSize, TXOffset, TXPath)
				end,
				Args
			);
		false ->
			do_not_update_data_root_offset_index
	end,
	{ok, DataRootIndexKeySet}.

update_data_root_index(State, DataRoot, TXSize, AbsoluteTXStartOffset, TXPath) ->
	#sync_data_state{ data_root_index = DataRootIndex } = State,
	ar_kv:put(DataRootIndex, data_root_key_v2(DataRoot, TXSize, AbsoluteTXStartOffset),
			TXPath).

add_block_data_roots_to_disk_pool(DataRootKeySet) ->
	sets:fold(
		fun(R, T) ->
			case ets:lookup(ar_disk_pool_data_roots, R) of
				[] ->
					ets:insert(ar_disk_pool_data_roots, {R, {0, T, not_set}});
				[{_, {Size, Timeout, _}}] ->
					ets:insert(ar_disk_pool_data_roots, {R, {Size, Timeout, not_set}})
			end,
			T + 1
		end,
		os:system_time(microsecond),
		DataRootKeySet
	).

reset_orphaned_data_roots_disk_pool_timestamps(DataRootKeySet) ->
	sets:fold(
		fun(R, T) ->
			case ets:lookup(ar_disk_pool_data_roots, R) of
				[] ->
					ets:insert(ar_disk_pool_data_roots, {R, {0, T, not_set}});
				[{_, {Size, _, TXIDSet}}] ->
					ets:insert(ar_disk_pool_data_roots, {R, {Size, T, TXIDSet}})
			end,
			T + 1
		end,
		os:system_time(microsecond),
		DataRootKeySet
	).

store_sync_state(#sync_data_state{ store_id = "default" } = State) ->
	#sync_data_state{ block_index = BI, packing_2_5_threshold = Packing_2_5_Threshold,
			packing_2_6_threshold = Packing_2_6_Threshold } = State,
	DiskPoolDataRoots = ets:foldl(
			fun({DataRootKey, V}, Acc) -> maps:put(DataRootKey, V, Acc) end, #{},
			ar_disk_pool_data_roots),
	[{_, DiskPoolSize}] = ets:lookup(ar_data_sync_state, disk_pool_size),
	StoredState = #{ block_index => BI, disk_pool_data_roots => DiskPoolDataRoots,
			disk_pool_size => DiskPoolSize, packing_2_5_threshold => Packing_2_5_Threshold,
			packing_2_6_threshold => Packing_2_6_Threshold,
			%% Storing it for backwards-compatibility.
			strict_data_split_threshold => ?STRICT_DATA_SPLIT_THRESHOLD },
	case ar_storage:write_term(data_sync_state, StoredState) of
		{error, enospc} ->
			?LOG_WARNING([{event, failed_to_dump_state}, {reason, disk_full},
					{store_id, "default"}]),
			ok;
		ok ->
			ok
	end;
store_sync_state(_State) ->
	ok.

find_peer_intervals(Start, End, StoreID, SkipIntervalsTable, Self) ->
	case get_next_unsynced_interval(Start, End, StoreID, SkipIntervalsTable) of
		not_found ->
			[];
		{Right, Left} ->
			Left2 = ar_tx_blacklist:get_next_not_blacklisted_byte(Left),
			case Left2 >= Right of
				true ->
					find_peer_intervals(Right, End, StoreID, SkipIntervalsTable, Self);
				false ->
					find_subintervals(Left2, End, StoreID, SkipIntervalsTable, Self)
			end
	end.

temporarily_exclude_interval_from_syncing(Left, Right, TID, Self) ->
	AlreadyExcluded =
		case ets:next(TID, Left) of
			'$end_of_table' ->
				false;
			Offset ->
				case ets:lookup(TID, Offset) of
					[{Offset, StartOffset}] when Offset >= Right, StartOffset =< Left ->
						true;
					_ ->
						false
				end
		end,
	case AlreadyExcluded of
		true ->
			ok;
		false ->
			?LOG_DEBUG([{event, exclude_interval_from_syncing}, {left, Left}, {right, Right}]),
			ar_ets_intervals:add(TID, Right, Left),
			ar_util:cast_after(?EXCLUDE_MISSING_INTERVAL_TIMEOUT_MS, Self,
					{re_include_interval_for_syncing, Left, Right}),
			ok
	end.

%% @doc Return the lowest unsynced interval strictly above the given Offset.
%% The right bound of the interval is at most RightBound.
%% Return not_found if there are no such intervals.
%% Skip the intervals we have recently failed to find.
get_next_unsynced_interval(Offset, RightBound, _StoreID, _SkipIntervalsTable)
		when Offset >= RightBound ->
	not_found;
get_next_unsynced_interval(Offset, RightBound, StoreID, SkipIntervalsTable) ->
	case ar_sync_record:get_next_synced_interval(Offset, RightBound, ?MODULE, StoreID) of
		not_found ->
			{RightBound, Offset};
		{NextOffset, Start} ->
			case Start > Offset of
				true ->
					End = min(RightBound, Start),
					case ets:next(SkipIntervalsTable, Offset) of
						'$end_of_table' ->
							{End, Offset};
						SkipEnd ->
							case ets:lookup(SkipIntervalsTable, SkipEnd) of
								[{SkipEnd, SkipStart}] when SkipStart >= End ->
									{End, Offset};
								[{SkipEnd, SkipStart}] when SkipStart > Offset ->
									{SkipStart, Offset};
								[{SkipEnd, _SkipStart}] when SkipEnd < End ->
									{End, SkipEnd};
								_ ->
									get_next_unsynced_interval(SkipEnd, RightBound, StoreID,
											SkipIntervalsTable)
							end
					end;
				_ ->
					get_next_unsynced_interval(NextOffset, RightBound, StoreID,
							SkipIntervalsTable)
			end
	end.

%% @doc Collect the unsynced intervals between Start and RightBound.
get_next_unsynced_intervals(Start, RightBound, StoreID) ->
	get_next_unsynced_intervals(Start, RightBound, ar_intervals:new(), StoreID).

get_next_unsynced_intervals(Start, RightBound, Intervals, _StoreID) when Start >= RightBound ->
	Intervals;
get_next_unsynced_intervals(Start, RightBound, Intervals, StoreID) ->
	case ar_sync_record:get_next_synced_interval(Start, RightBound, ?MODULE, StoreID) of
		not_found ->
			ar_intervals:add(Intervals, RightBound, Start);
		{NextOffset, NextStart} ->
			case NextStart > Start of
				true ->
					End = min(NextStart, RightBound),
					get_next_unsynced_intervals(NextOffset, RightBound,
							ar_intervals:add(Intervals, End, Start), StoreID);
				_ ->
					get_next_unsynced_intervals(NextOffset, RightBound, Intervals, StoreID)
			end
	end.

find_subintervals(Left, Right, _StoreID, _TID, _Self) when Left >= Right ->
	ok;
find_subintervals(Left, Right, StoreID, TID, Self) ->
	Bucket = Left div ?NETWORK_DATA_BUCKET_SIZE,
	LeftBound = Left - Left rem ?NETWORK_DATA_BUCKET_SIZE,
	Peers = ar_data_discovery:get_bucket_peers(Bucket),
	RightBound = min(LeftBound + ?NETWORK_DATA_BUCKET_SIZE, Right),
	Results =
		ar_util:pmap(
			fun(Peer) ->
				UnsyncedIntervals = get_next_unsynced_intervals(Left, RightBound, StoreID),
				case ar_intervals:is_empty(UnsyncedIntervals) of
					true ->
						found;
					false ->
						case get_peer_intervals(Peer, Left, UnsyncedIntervals) of
							{ok, Intervals} ->
								gen_server:cast(Self, {enqueue_intervals, Peer, Intervals}),
								case ar_intervals:is_empty(Intervals) of
									true ->
										not_found;
									false ->
										found
								end;
							{error, Reason} ->
								?LOG_DEBUG([{event, failed_to_fetch_peer_intervals},
										{peer, ar_util:format_peer(Peer)},
										{reason, io_lib:format("~p", [Reason])}]),
								not_found
						end
				end
			end,
			Peers
		),
	case lists:any(fun(Result) -> Result == found end, Results) of
		false ->
			temporarily_exclude_interval_from_syncing(Left, Right, TID, Self);
		true ->
			ok
	end,
	find_subintervals(LeftBound + ?NETWORK_DATA_BUCKET_SIZE, Right, StoreID, TID, Self).

get_peer_intervals(Peer, Left, SoughtIntervals) ->
	%% A guess. A bigger limit may result in unnecesarily large response. A smaller
	%% limit may be too small to fetch all the synced intervals from the requested range,
	%% in case the peer's sync record is too choppy.
	Limit = min(2000, ?MAX_SHARED_SYNCED_INTERVALS_COUNT),
	case ar_http_iface_client:get_sync_record(Peer, Left + 1, Limit) of
		{ok, PeerIntervals} ->
			{ok, ar_intervals:intersection(PeerIntervals, SoughtIntervals)};
		Error ->
			Error
	end.

validate_proof(TXRoot, BlockStartOffset, Offset, BlockSize, Proof, ValidateDataPathFun) ->
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,
	case ar_merkle:validate_path(TXRoot, Offset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			TXSize = TXEndOffset - TXStartOffset,
			ChunkOffset = Offset - TXStartOffset,
			case ValidateDataPathFun(DataRoot, ChunkOffset, TXSize, DataPath) of
				false ->
					false;
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					AbsoluteEndOffset = BlockStartOffset + TXStartOffset + ChunkEndOffset,
					ChunkSize = ChunkEndOffset - ChunkStartOffset,
					case Packing of
						unpacked ->
							case ar_tx:generate_chunk_id(Chunk) == ChunkID of
								false ->
									false;
								true ->
									case ChunkSize == byte_size(Chunk) of
										true ->
											{true, DataRoot, TXStartOffset, ChunkEndOffset,
												TXSize, ChunkSize, ChunkID};
										false ->
											false
									end
							end;
						_ ->
							ChunkArgs = {Packing, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize},
							Args = {Packing, DataRoot, TXStartOffset, ChunkEndOffset, TXSize,
									ChunkID},
							{need_unpacking, AbsoluteEndOffset, ChunkArgs, Args}
					end
			end
	end.

validate_proof2(Args) ->
	{TXRoot, Offset, BlockSize, DataPath, TXPath, ChunkSize, ValidateDataPathFun} = Args,
	case ar_merkle:validate_path(TXRoot, Offset, BlockSize, TXPath) of
		false ->
			false;
		{DataRoot, TXStartOffset, TXEndOffset} ->
			TXSize = TXEndOffset - TXStartOffset,
			ChunkOffset = Offset - TXStartOffset,
			case ValidateDataPathFun(DataRoot, ChunkOffset, TXSize, DataPath) of
				{ChunkID, ChunkStartOffset, ChunkEndOffset} ->
					case ChunkEndOffset - ChunkStartOffset == ChunkSize of
						false ->
							false;
						true ->
							{true, ChunkID}
					end;
				_ ->
					false
			end
	end.

validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	ValidatePathResult =
		case ar_merkle:validate_path_strict_data_split(DataRoot, Offset, TXSize, DataPath) of
			false ->
				case ar_merkle:validate_path_strict_borders(DataRoot, Offset, TXSize,
						DataPath) of
					false ->
						false;
					R ->
						{false, R}
				end;
			R ->
				{true, R}
		end,
	case ValidatePathResult of
		false ->
			false;
		{Strict, {ChunkID, StartOffset, EndOffset}} ->
			case ar_tx:generate_chunk_id(Chunk) == ChunkID of
				false ->
					false;
				true ->
					case EndOffset - StartOffset == byte_size(Chunk) of
						true ->
							{true, Strict, EndOffset};
						false ->
							false
					end
			end
	end.

add_chunk(DataRoot, DataPath, Chunk, Offset, TXSize) ->
	[{_, DataRootIndex}] = ets:lookup(ar_data_sync_state, {data_root_index, "default"}),
	[{_, DiskPoolSize}] = ets:lookup(ar_data_sync_state, disk_pool_size),
	[{_, DiskPoolChunksIndex}] = ets:lookup(ar_data_sync_state,
			{disk_pool_chunks_index, "default"}),
	[{_, ChunkDataDB}] = ets:lookup(ar_data_sync_state, {chunk_data_db, "default"}),
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	DataRootOffsetReply = get_data_root_offset(DataRootKey, "default"),
	DataRootInDiskPool = ets:member(ar_disk_pool_data_roots, DataRootKey),
	ChunkSize = byte_size(Chunk),
	{ok, Config} = application:get_env(arweave, config),
	DataRootLimit = Config#config.max_disk_pool_data_root_buffer_mb * 1024 * 1024,
	DiskPoolLimit = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
	CheckDiskPool =
		case {DataRootOffsetReply, DataRootInDiskPool} of
			{not_found, false} ->
				{error, data_root_not_found};
			{not_found, true} ->
				[{_, {Size, Timestamp, TXIDSet}}] = ets:lookup(ar_disk_pool_data_roots,
						DataRootKey),
				case Size + ChunkSize > DataRootLimit
						orelse DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						{error, exceeds_disk_pool_size_limit};
					false ->
						{ok, {Size + ChunkSize, Timestamp, TXIDSet}}
				end;
			_ ->
				case DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						{error, exceeds_disk_pool_size_limit};
					false ->
						{ok, {ChunkSize, os:system_time(microsecond), not_set}}
				end
		end,
	ValidateProof =
		case CheckDiskPool of
			{error, _} = Error ->
				Error;
			{ok, DiskPoolDataRootValue} ->
				case validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) of
					false ->
						{error, invalid_proof};
					{true, PassesStrict, EndOffset} ->
						{ok, {EndOffset, PassesStrict, DiskPoolDataRootValue}}
				end
		end,
	CheckSynced =
		case ValidateProof of
			{error, _} = Error2 ->
				Error2;
			{ok, {EndOffset2, _PassesStrict2, {_, Timestamp2, _}} = PassedState2} ->
				DataPathHash = crypto:hash(sha256, DataPath),
				DiskPoolChunkKey = << Timestamp2:256, DataPathHash/binary >>,
				case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
					{ok, _DiskPoolChunk} ->
						%% The chunk is already in disk pool.
						synced;
					not_found ->
						case DataRootOffsetReply of
							not_found ->
								{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}};
							{ok, {TXStartOffset, _}} ->
								case chunk_offsets_synced(DataRootIndex, DataRootKey,
										%% The same data may be uploaded several times.
										%% Here we only accept the chunk if any of the
										%% last 5 instances of this data is not filled in
										%% yet.
										EndOffset2, TXStartOffset, 5) of
									true ->
										synced;
									false ->
										{ok, {DataPathHash, DiskPoolChunkKey, PassedState2}}
								end
						end;
					{error, Reason} ->
						?LOG_WARNING([{event, failed_to_read_chunk_from_disk_pool},
								{reason, io_lib:format("~p", [Reason])}]),
						{error, failed_to_store_chunk}
				end
		end,
	case CheckSynced of
		synced ->
			?LOG_DEBUG([{event, chunk_already_synced}, {data_root, ar_util:encode(DataRoot)}]),
			ok;
		{error, _} = Error4 ->
			Error4;
		{ok, {DataPathHash2, DiskPoolChunkKey2, {EndOffset3, PassesStrict3,
				DiskPoolDataRootValue2}}} ->
			ChunkDataKey = get_chunk_data_key(DataPathHash2),
			case ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary({Chunk, DataPath})) of
				{error, Reason2} ->
					?LOG_WARNING([{event, failed_to_store_chunk_in_disk_pool},
						{reason, io_lib:format("~p", [Reason2])}]),
					{error, failed_to_store_chunk};
				ok ->
					DiskPoolChunkValue = term_to_binary({EndOffset3, ChunkSize, DataRoot,
							TXSize, ChunkDataKey, PassesStrict3}),
					case ar_kv:put(DiskPoolChunksIndex, DiskPoolChunkKey2,
							DiskPoolChunkValue) of
						{error, Reason3} ->
							?LOG_WARNING([{event, failed_to_record_chunk_in_disk_pool},
								{reason, io_lib:format("~p", [Reason3])}]),
							{error, failed_to_store_chunk};
						ok ->
							ets:insert(ar_disk_pool_data_roots,
									{DataRootKey, DiskPoolDataRootValue2}),
							ets:update_counter(ar_data_sync_state, disk_pool_size,
									{2, ChunkSize}),
							prometheus_gauge:inc(pending_chunks_size, ChunkSize),
							ok
					end
			end
	end.

chunk_offsets_synced(_, _, _, _, N) when N == 0 ->
	true;
chunk_offsets_synced(DataRootIndex, DataRootKey, ChunkOffset, TXStartOffset, N) ->
	case ar_sync_record:is_recorded(TXStartOffset + ChunkOffset, ?MODULE) of
		{{true, _}, _StoreID} ->
			case TXStartOffset of
				0 ->
					true;
				_ ->
					<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
					Key = data_root_key_v2(DataRoot, TXSize, TXStartOffset - 1),
					case ar_kv:get_prev(DataRootIndex, Key) of
						none ->
							true;
						{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
								TXStartOffset2Size:8,
								TXStartOffset2:(TXStartOffset2Size * 8) >>, _} ->
							chunk_offsets_synced(DataRootIndex, DataRootKey, ChunkOffset,
									TXStartOffset2, N - 1);
						{ok, _, _} ->
							true;
						_ ->
							false
					end
			end;
		false ->
			false
	end.

%% @doc Return a storage reference to the chunk proof (and possibly the chunk itself).
get_chunk_data_key(DataPathHash) ->
	Timestamp = os:system_time(microsecond),
	<< Timestamp:256, DataPathHash/binary >>.

write_disk_pool_chunk(ChunkDataKey, Chunk, DataPath, State) ->
	#sync_data_state{ chunk_data_db = ChunkDataDB } = State,
	ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary({Chunk, DataPath})).

write_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing, State) ->
	case ar_tx_blacklist:is_byte_blacklisted(Offset) of
		true ->
			ok;
		false ->
			write_not_blacklisted_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath,
					Packing, State)
	end.

write_not_blacklisted_chunk(Offset, ChunkDataKey, Chunk, ChunkSize, DataPath, Packing,
		State) ->
	#sync_data_state{ chunk_data_db = ChunkDataDB, store_id = StoreID } = State,
	ShouldStoreInChunkStorage = should_store_in_chunk_storage(Offset, ChunkSize, Packing),
	Result =
		case ShouldStoreInChunkStorage of
			true ->
				PaddedOffset = get_chunk_padded_offset(Offset),
				ar_chunk_storage:put(PaddedOffset, Chunk, StoreID);
			false ->
				ok
		end,
	case Result of
		ok ->
			case ShouldStoreInChunkStorage of
				false ->
					ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary({Chunk, DataPath}));
				true ->
					ar_kv:put(ChunkDataDB, ChunkDataKey, term_to_binary(DataPath))
			end;
		_ ->
			Result
	end.

%% @doc 256 KiB chunks are stored in the blob storage optimized for read speed.
%% Return true if we want to place the chunk there.
should_store_in_chunk_storage(Offset, ChunkSize, Packing) ->
	case Offset > ?STRICT_DATA_SPLIT_THRESHOLD of
		true ->
			%% All chunks above ?STRICT_DATA_SPLIT_THRESHOLD are placed in 256 KiB buckets
			%% so technically can be stored in ar_chunk_storage. However, to avoid
			%% managing padding in ar_chunk_storage for unpacked chunks smaller than 256 KiB
			%% (we do not need fast random access to unpacked chunks after
			%% ?STRICT_DATA_SPLIT_THRESHOLD anyways), we put them to RocksDB.
			Packing /= unpacked orelse ChunkSize == (?DATA_CHUNK_SIZE);
		false ->
			ChunkSize == (?DATA_CHUNK_SIZE)
	end.

update_chunks_index(Args, State) ->
	AbsoluteChunkOffset = element(1, Args),
	case ar_tx_blacklist:is_byte_blacklisted(AbsoluteChunkOffset) of
		true ->
			ok;
		false ->
			update_chunks_index2(Args, State)
	end.

update_chunks_index2(Args, State) ->
	{AbsoluteOffset, Offset, ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkSize,
			Packing} = Args,
	#sync_data_state{ chunks_index = ChunksIndex, store_id = StoreID } = State,
	Key = << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>,
	Value = {ChunkDataKey, TXRoot, DataRoot, TXPath, Offset, ChunkSize},
	case ar_kv:put(ChunksIndex, Key, term_to_binary(Value)) of
		ok ->
			StartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
			PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
			case ar_sync_record:add(PaddedOffset, StartOffset, Packing, ?MODULE, StoreID) of
				ok ->
					ar_events:send(data_sync, {add_range, StartOffset, PaddedOffset, StoreID}),
					ok;
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_update_sync_record}, {reason, Reason},
							{chunk, ar_util:encode(ChunkDataKey)}, {offset, AbsoluteOffset},
							{store_id, StoreID}]),
					{error, Reason}
			end;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_update_chunk_index}, {reason, Reason},
					{chunk, ar_util:encode(ChunkDataKey)}, {offset, AbsoluteOffset},
					{store_id, StoreID}]),
			{error, Reason}
	end.

add_chunk_to_disk_pool(Args, State) ->
	{DataRoot, TXSize, DataPathHash, ChunkDataKey, ChunkOffset, ChunkSize,
			PassedStrictValidation} = Args,
	#sync_data_state{ disk_pool_chunks_index = DiskPoolChunksIndex } = State,
	DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	Timestamp =
		case ets:lookup(ar_disk_pool_data_roots, DataRootKey) of
			[] ->
				os:system_time(microsecond);
			[{_, {_, DataRootTimestamp, _}}] ->
				DataRootTimestamp
		end,
	DiskPoolChunkKey = << Timestamp:256, DataPathHash/binary >>,
	case ar_kv:get(DiskPoolChunksIndex, DiskPoolChunkKey) of
		not_found ->
			Value = {ChunkOffset, ChunkSize, DataRoot, TXSize, ChunkDataKey,
					PassedStrictValidation},
			case ar_kv:put(DiskPoolChunksIndex, DiskPoolChunkKey, term_to_binary(Value)) of
				ok ->
					ok;
				Error ->
					Error
			end;
		_ ->
			ok
	end.

pick_missing_blocks([{H, WeaveSize, _} | CurrentBI], BlockTXPairs) ->
	{After, Before} = lists:splitwith(fun({BH, _}) -> BH /= H end, BlockTXPairs),
	case Before of
		[] ->
			pick_missing_blocks(CurrentBI, BlockTXPairs);
		_ ->
			{WeaveSize, lists:reverse(After)}
	end.

process_invalid_fetched_chunk(Peer, Byte, State) ->
	#sync_data_state{ weave_size = WeaveSize, store_id = StoreID } = State,
	?LOG_WARNING([{event, got_invalid_proof_from_peer}, {peer, ar_util:format_peer(Peer)},
			{byte, Byte}, {weave_size, WeaveSize}]),
	decrement_sync_buffer_size(StoreID),
	%% Not necessarily a malicious peer, it might happen
	%% if the chunk is recent and from a different fork.
	{noreply, State}.

process_valid_fetched_chunk(ChunkArgs, Args, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	{Packing, UnpackedChunk, AbsoluteEndOffset, TXRoot, ChunkSize} = ChunkArgs,
	{AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot, Chunk, _ChunkID,
			ChunkEndOffset, Strict, Peer, Byte} = Args,
	decrement_sync_buffer_size(StoreID),
	case is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) of
		false ->
			?LOG_WARNING([{event, got_too_big_proof_from_peer},
					{peer, ar_util:format_peer(Peer)}]),
			{noreply, State};
		true ->
			#sync_data_state{ store_id = StoreID } = State,
			case ar_sync_record:is_recorded(Byte + 1, ?MODULE, StoreID) of
				{true, _} ->
					?LOG_DEBUG([{event, unpacked_fetched_chunk_already_synced},
							{offset, AbsoluteEndOffset}]),
					%% The chunk has been synced by another job already.
					{noreply, State};
				false ->
					pack_and_store_fetched_chunk({DataRoot, AbsoluteTXStartOffset,
							TXPath, TXRoot, TXSize, DataPath, Packing, ChunkEndOffset,
							ChunkSize, Chunk, UnpackedChunk, Strict}, State)
			end
	end.

pack_and_store_fetched_chunk(Args, State) ->
	{DataRoot, AbsoluteTXStartOffset, TXPath, TXRoot, TXSize, DataPath, Packing, Offset,
			ChunkSize, Chunk, UnpackedChunk, PassedStrictValidation} = Args,
	#sync_data_state{ packing_map = PackingMap, store_id = StoreID,
			disk_pool_threshold = DiskPoolThreshold } = State,
	AbsoluteOffset = AbsoluteTXStartOffset + Offset,
	DataPathHash = crypto:hash(sha256, DataPath),
	case AbsoluteOffset > DiskPoolThreshold of
		true ->
			?LOG_DEBUG([{event, storing_fetched_chunk_in_disk_pool},
					{offset, AbsoluteOffset}]),
			ChunkDataKey = get_chunk_data_key(DataPathHash),
			Write = write_disk_pool_chunk(ChunkDataKey, UnpackedChunk, DataPath, State),
			case Write of
				ok ->
					Args2 = {DataRoot, TXSize, DataPathHash, ChunkDataKey, Offset, ChunkSize,
							PassedStrictValidation},
					case add_chunk_to_disk_pool(Args2, State) of
						ok ->
							case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey,
										TXRoot, DataRoot, TXPath, ChunkSize, unpacked},
										State) of
								ok ->
									{noreply, State};
								{error, Reason} ->
									log_failed_to_store_chunk(Reason, Offset, DataRoot,
											StoreID),
									{noreply, State}
							end;
						{error, Reason} ->
							log_failed_to_store_chunk(Reason, Offset, DataRoot, StoreID),
							{noreply, State}
					end;
				{error, Reason} ->
					log_failed_to_store_chunk(Reason, Offset, DataRoot, StoreID),
					{noreply, State}
			end;
		false ->
			RequiredPacking = get_required_chunk_packing(AbsoluteOffset, ChunkSize, State),
			PackingStatus =
				case {RequiredPacking, Packing} of
					{unpacked, _} ->
						{ready, {unpacked, UnpackedChunk}};
					{Packing, Packing} ->
						{ready, {Packing, Chunk}};
					{DifferentPacking, _} ->
						{need_packing, DifferentPacking}
				end,
			case PackingStatus of
				{ready, {StoredPacking, StoredChunk}} ->
					?LOG_DEBUG([{event, storing_fetched_chunk}, {offset, AbsoluteOffset},
							{required_packing, format_packing(RequiredPacking)},
							{packing, format_packing(Packing)}]),
					ChunkArgs = {StoredPacking, StoredChunk, AbsoluteOffset, TXRoot,
							ChunkSize},
					store_chunk(ChunkArgs, {StoredPacking, DataPath, Offset, DataRoot, TXPath},
							State),
					{noreply, State};
				{need_packing, RequiredPacking} ->
					case maps:is_key(AbsoluteOffset, PackingMap) of
						true ->
							{noreply, State};
						false ->
							?LOG_DEBUG([{event, schedule_fetched_chunk_repacking},
									{offset, AbsoluteOffset}, {got_packing,
											format_packing(Packing)},
									{need_packing, format_packing(RequiredPacking)}]),
							ar_events:send(chunk, {repack_request, AbsoluteOffset,
									{RequiredPacking, unpacked, UnpackedChunk, AbsoluteOffset,
									TXRoot, ChunkSize}}),
							ar_util:cast_after(600000, self(),
									{expire_repack_chunk_request, AbsoluteOffset}),
							PackingArgs = {pack_fetched_chunk, {RequiredPacking, DataPath,
									Offset, DataRoot, TXPath}},
							{noreply, State#sync_data_state{
								packing_map = PackingMap#{ AbsoluteOffset => PackingArgs }}}
					end
			end
	end.

format_packing({spora_2_6, Addr}) ->
	iolist_to_binary([<<"spora_2_6_">>, ar_util:encode(Addr)]);
format_packing(Atom) ->
	Atom.

store_chunk(ChunkArgs, Args, State) ->
	#sync_data_state{ store_id = StoreID, chunks_index = ChunksIndex } = State,
	{Packing, _Chunk, AbsoluteOffset, _TXRoot, ChunkSize} = ChunkArgs,
	{_, _DataPath, _Offset, DataRoot, _TXPath} = Args,
	PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
	StartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
	TargetStoreIDs = get_target_store_ids(PaddedOffset, Packing, ChunkSize),
	?LOG_DEBUG([{event, storing_chunk}, {offset, AbsoluteOffset},
			{packing, format_packing(element(1, ChunkArgs))}, {store_ids, TargetStoreIDs}]),
	case lists:member(StoreID, TargetStoreIDs) of
		true ->
			store_chunk2(ChunkArgs, Args, State);
		false ->
			[gen_server:cast(list_to_atom("ar_data_sync_" ++ TargetStoreID),
					{store_chunk, ChunkArgs, Args}) || TargetStoreID <- TargetStoreIDs],
			case ar_sync_record:delete(PaddedOffset, StartOffset, ?MODULE, StoreID) of
				{error, Reason} ->
					log_failed_to_remove_moved_chunk(Reason, AbsoluteOffset, DataRoot,
							StoreID, TargetStoreIDs),
					{error, Reason};
				ok ->
					case ar_kv:delete(ChunksIndex, << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>) of
						{error, Reason} ->
							log_failed_to_remove_moved_chunk(Reason, AbsoluteOffset, DataRoot,
									StoreID, TargetStoreIDs),
							{error, Reason};
						ok ->
							ok
					end
			end
	end.

get_target_store_ids(Offset, unpacked, ChunkSize)
		when ChunkSize < ?DATA_CHUNK_SIZE, Offset =< ?STRICT_DATA_SPLIT_THRESHOLD ->
	case ar_storage_module:get_all(Offset) of
		[] ->
			["default"];
		Modules ->
			[ar_storage_module:id(Module) || Module <- Modules]
	end;
get_target_store_ids(Offset, Packing, _ChunkSize) ->
	Modules = [{BucketSize, Bucket, ModulePacking} ||
			{BucketSize, Bucket, ModulePacking} <- ar_storage_module:get_all(Offset),
			ModulePacking == Packing],
	case Modules of
		[] ->
			["default"];
		_ ->
			[ar_storage_module:id(Module) || Module <- Modules]
	end.

store_chunk2(ChunkArgs, Args, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = ChunkArgs,
	{_Packing, DataPath, Offset, DataRoot, TXPath} = Args,
	PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
	StartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize),
	case ar_sync_record:delete(PaddedOffset, StartOffset, ?MODULE, StoreID) of
		{error, Reason} ->
			log_failed_to_store_chunk(Reason, AbsoluteOffset, DataRoot, StoreID),
			{error, Reason};
		ok ->
			DataPathHash = crypto:hash(sha256, DataPath),
			ChunkDataKey = get_chunk_data_key(DataPathHash),
			case write_chunk(AbsoluteOffset, ChunkDataKey, Chunk, ChunkSize, DataPath,
					Packing, State) of
				ok ->
					case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey, TXRoot,
							DataRoot, TXPath, ChunkSize, Packing}, State) of
						ok ->
							ok;
						{error, Reason} ->
							log_failed_to_store_chunk(Reason, AbsoluteOffset, DataRoot,
									StoreID),
							{error, Reason}
					end;
				{error, Reason} ->
					log_failed_to_store_chunk(Reason, AbsoluteOffset, DataRoot, StoreID),
					{error, Reason}
			end
	end.

log_failed_to_remove_moved_chunk(Reason, Offset, DataRoot, StoreID, TargetStoreIDs) ->
	?LOG_ERROR([{event, failed_to_remove_moved_chunk}, {reason, io_lib:format("~p", [Reason])},
			{origin_store_id, StoreID}, {target_store_ids, TargetStoreIDs},
			{absolute_chunk_offset, Offset}, {data_root, ar_util:encode(DataRoot)}]).

log_failed_to_store_chunk(Reason, Offset, DataRoot, StoreID) ->
	?LOG_ERROR([{event, failed_to_store_chunk}, {reason, io_lib:format("~p", [Reason])},
			{store_id, StoreID}, {absolute_chunk_offset, Offset},
			{data_root, ar_util:encode(DataRoot)}]).

cache_recently_processed_offset(Offset, ChunkDataKey, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(Offset, Map, sets:new()),
	Map2 =
		case sets:is_element(ChunkDataKey, Set) of
			false ->
				ar_util:cast_after(?CACHE_RECENTLY_PROCESSED_DISK_POOL_OFFSET_LIFETIME_MS,
						self(), {remove_recently_processed_disk_pool_offset, Offset,
						ChunkDataKey}),
				maps:put(Offset, sets:add_element(ChunkDataKey, Set), Map);
			true ->
				Map
		end,
	State#sync_data_state{ recently_processed_disk_pool_offsets = Map2 }.

get_required_chunk_packing(_Offset, _ChunkSize,
		#sync_data_state{ packing_disabled = true, store_id = "default" }) ->
	unpacked;
get_required_chunk_packing(Offset, ChunkSize,
		#sync_data_state{ store_id = "default",
				disk_pool_threshold = DiskPoolThreshold } = State) ->
	case Offset =< ?STRICT_DATA_SPLIT_THRESHOLD andalso ChunkSize < ?DATA_CHUNK_SIZE of
		true ->
			unpacked;
		false ->
			case Offset > DiskPoolThreshold of
				true ->
					unpacked;
				false ->
					#sync_data_state{ packing_2_5_threshold = PackingThreshold,
							packing_2_6_threshold = Packing_2_6_Threshold,
							mining_address = MiningAddress } = State,
					case Offset > Packing_2_6_Threshold of
						true ->
							{spora_2_6, MiningAddress};
						false ->
							case Offset > PackingThreshold of
								true ->
									spora_2_5;
								false ->
									unpacked
							end
					end
			end
	end;
get_required_chunk_packing(Offset, ChunkSize, #sync_data_state{ store_id = StoreID }) ->
	case Offset =< ?STRICT_DATA_SPLIT_THRESHOLD andalso ChunkSize < ?DATA_CHUNK_SIZE of
		true ->
			unpacked;
		false ->
			ar_storage_module:get_packing(StoreID)
	end.

process_disk_pool_item(State, Key, Value) ->
	#sync_data_state{ disk_pool_chunks_index = DiskPoolChunksIndex,
			data_root_index = DataRootIndex, store_id = StoreID } = State,
	prometheus_counter:inc(disk_pool_processed_chunks),
	<< Timestamp:256, DataPathHash/binary >> = Key,
	DiskPoolChunk = parse_disk_pool_chunk(Value),
	{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey,
			PassedStrictValidation} = DiskPoolChunk,
	DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	InDataRootIndex = get_data_root_offset(DataRootKey, StoreID),
	InDiskPool = ets:member(ar_disk_pool_data_roots, DataRootKey),
	case {InDataRootIndex, InDiskPool} of
		{not_found, true} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% all chunks belonging to the same data root because the data root is not
			%% yet on chain.
			NextCursor = {seek, << (Timestamp + 1):256 >>},
			gen_server:cast(self(), process_disk_pool_item),
			{noreply, State#sync_data_state{ disk_pool_cursor = NextCursor }};
		{not_found, false} ->
			%% The chunk never made it to the chain, the data root has expired in disk pool.
			case ets:member(ar_data_sync_state, move_data_root_index_migration_complete) of
				true ->
					?LOG_DEBUG([{event, disk_pool_chunk_data_root_expired}]),
					ok = ar_kv:delete(DiskPoolChunksIndex, Key),
					ok = delete_disk_pool_chunk(ChunkDataKey, State);
				false ->
					%% Do not remove the chunk from the disk pool until the data root index
					%% migration is complete, because the data root might still exist in the
					%% old index.
					ok
			end,
			NextCursor = << Key/binary, <<"a">>/binary >>,
			gen_server:cast(self(), process_disk_pool_item),
			State2 = may_be_reset_disk_pool_full_scan_key(Key, State),
			{noreply, State2#sync_data_state{ disk_pool_cursor = NextCursor }};
		{{ok, {TXStartOffset, _TXPath}}, _} ->
			DataRootIndexIterator = data_root_index_iterator_v2(DataRootKey, TXStartOffset + 1,
					DataRootIndex),
			NextCursor = << Key/binary, <<"a">>/binary >>,
			State2 = State#sync_data_state{ disk_pool_cursor = NextCursor },
			Args = {Offset, InDiskPool, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key,
					PassedStrictValidation},
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, DataRootIndexIterator,
					true, Args}),
			{noreply, State2}
	end.

decrease_occupied_disk_pool_size(Size, DataRootKey) ->
	ets:update_counter(ar_data_sync_state, disk_pool_size, {2, -Size}),
	prometheus_gauge:dec(pending_chunks_size, Size),
	case ets:lookup(ar_disk_pool_data_roots, DataRootKey) of
		[] ->
			ok;
		[{_, {Size2, Timestamp, TXIDSet}}] ->
			ets:insert(ar_disk_pool_data_roots, {DataRootKey,
					{Size2 - Size, Timestamp, TXIDSet}}),
			ok
	end.

may_be_reset_disk_pool_full_scan_key(Key,
		#sync_data_state{ disk_pool_full_scan_start_key = Key } = State) ->
	State#sync_data_state{ disk_pool_full_scan_start_key = none };
may_be_reset_disk_pool_full_scan_key(_Key, State) ->
	State.

parse_disk_pool_chunk(Bin) ->
	case binary_to_term(Bin) of
		{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey} ->
			{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey, false};
		R ->
			R
	end.

delete_disk_pool_chunk(ChunkDataKey, State) ->
	#sync_data_state{
		chunk_data_db = ChunkDataDB
	} = State,
	ar_kv:delete(ChunkDataDB, ChunkDataKey).

register_currently_processed_disk_pool_key(Key, State) ->
	#sync_data_state{ currently_processed_disk_pool_keys = Keys } = State,
	Keys2 = sets:add_element(Key, Keys),
	State#sync_data_state{ currently_processed_disk_pool_keys = Keys2 }.

deregister_currently_processed_disk_pool_key(Key, State) ->
	#sync_data_state{ currently_processed_disk_pool_keys = Keys } = State,
	Keys2 = sets:del_element(Key, Keys),
	State#sync_data_state{ currently_processed_disk_pool_keys = Keys2 }.

process_disk_pool_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, MayConclude,
		Args, State) ->
	#sync_data_state{ disk_pool_threshold = DiskPoolThreshold } = State,
	{_, _, _, DataRoot, _, _, _, PassedStrictValidation} = Args,
	case AbsoluteOffset =< ?STRICT_DATA_SPLIT_THRESHOLD orelse PassedStrictValidation of
		false ->
			%% When we accept chunks into the disk pool, we do not know where they will
			%% end up on the weave. Therefore, we cannot require all Merkle proofs pass
			%% the strict validation rules taking effect only after
			%% ?STRICT_DATA_SPLIT_THRESHOLD.
			%% Instead we note down whether the chunk passes the strict validation and take it
			%% into account here where the chunk is associated with a global weave offset.
			?LOG_INFO([{event, disk_pool_chunk_from_bad_split}, {offset, AbsoluteOffset},
					{data_root, ar_util:encode(DataRoot)}]),
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
					MayConclude, Args}),
			{noreply, State};
		true ->
			case AbsoluteOffset > DiskPoolThreshold of
				true ->
					process_disk_pool_immature_chunk_offset(Iterator, TXRoot, TXPath,
							AbsoluteOffset, Args, State);
				false ->
					process_disk_pool_matured_chunk_offset(Iterator, TXRoot, TXPath,
							AbsoluteOffset, MayConclude, Args, State)
			end
	end.

process_disk_pool_immature_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, Args,
		State) ->
	#sync_data_state{ store_id = StoreID } = State,
	case ar_sync_record:is_recorded(AbsoluteOffset, ?MODULE, StoreID) of
		{true, unpacked} ->
			%% Pass MayConclude as false because we have encountered an offset
			%% above the disk pool threshold => we need to keep the chunk in the
			%% disk pool for now and not pack and move to the offset-based storage.
			%% The motivation is to keep chain reorganisations cheap.
			gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false, Args}),
			{noreply, State};
		false ->
			?LOG_DEBUG([{event, record_disk_pool_chunk}, {offset, AbsoluteOffset},
					{chunk_data_key, ar_util:encode(element(5, Args))}]),
			{Offset, _, ChunkSize, DataRoot, _, ChunkDataKey, Key, _} = Args,
			case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey,
					TXRoot, DataRoot, TXPath, ChunkSize, unpacked}, State) of
				ok ->
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false,
							Args}),
					{noreply, State};
				{error, Reason} ->
					?LOG_WARNING([{event, failed_to_update_chunks_index},
							{reason, io_lib:format("~p", [Reason])}]),
					gen_server:cast(self(), process_disk_pool_item),
					{noreply, deregister_currently_processed_disk_pool_key(Key, State)}
			end
	end.

process_disk_pool_matured_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, MayConclude,
		Args, State) ->
	#sync_data_state{ chunk_data_db = ChunkDataDB, packing_map = PackingMap,
			store_id = StoreID } = State,
	{Offset, _, ChunkSize, DataRoot, _, ChunkDataKey, Key, _} = Args,
	PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
	CheckHaveStorageModules =
		case ar_storage_module:get_all(PaddedOffset) of
			[] ->
				gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
						MayConclude, Args}),
				{noreply, State};
			_ ->
				true
		end,
	CheckPackingInProgress =
		case CheckHaveStorageModules of
			{noreply, State2} ->
				{noreply, State2};
			true ->
				case maps:is_key(AbsoluteOffset, PackingMap) of
					true ->
						?LOG_DEBUG([{event, disk_pool_chunk_already_being_packed},
								{offset, AbsoluteOffset},
								{chunk_data_key, ar_util:encode(element(5, Args))}]),
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								false, Args}),
						{noreply, State};
					false ->
						false
				end
		end,
	CheckPackingBufferFull =
		case CheckPackingInProgress of
			{noreply, State3} ->
				{noreply, State3};
			false ->
				case ar_packing_server:is_buffer_full() of
					true ->
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								false, Args}),
						{noreply, State};
					false ->
						false
				end
		end,
	CheckIsDiskPoolChunk =
		case CheckPackingBufferFull of
			{noreply, State4} ->
				{noreply, State4};
			false ->
				case is_disk_pool_chunk(AbsoluteOffset, ChunkDataKey, State) of
					false ->
						State5 = cache_recently_processed_offset(AbsoluteOffset, ChunkDataKey,
								State),
						?LOG_DEBUG([{event, chunk_already_moved_from_disk_pool},
								{offset, AbsoluteOffset},
								{chunk_data_key, ar_util:encode(element(5, Args))},
								{data_root, ar_util:encode(DataRoot)}]),
						gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
								MayConclude, Args}),
						{noreply, State5};
					{error, Reason} ->
						?LOG_WARNING([{event, failed_to_read_chunks_index},
								{reason, io_lib:format("~p", [Reason])}]),
						gen_server:cast(self(), process_disk_pool_item),
						{noreply, deregister_currently_processed_disk_pool_key(Key, State)};
					true ->
						true
				end
		end,
	case CheckIsDiskPoolChunk of
		{noreply, State6} ->
			{noreply, State6};
		true ->
			case read_chunk(AbsoluteOffset, ChunkDataDB, ChunkDataKey, StoreID) of
				not_found ->
					?LOG_WARNING([{event, disk_pool_chunk_not_found}, {offset, AbsoluteOffset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator,
							MayConclude, Args}),
					{noreply, State};
				{error, Reason2} ->
					?LOG_ERROR([{event, failed_to_read_disk_pool_chunk},
							{reason, io_lib:format("~p", [Reason2])}]),
					gen_server:cast(self(), process_disk_pool_item),
					{noreply, deregister_currently_processed_disk_pool_key(Key, State)};
				{ok, {Chunk, DataPath}} ->
					?LOG_DEBUG([{event, request_disk_pool_chunk_packing},
							{offset, AbsoluteOffset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					RequiredPacking = get_required_chunk_packing(AbsoluteOffset, ChunkSize,
							State),
					ar_events:send(chunk, {repack_request, AbsoluteOffset,
							{RequiredPacking, unpacked, Chunk, AbsoluteOffset, TXRoot,
									ChunkSize}}),
					ar_util:cast_after(600000, self(),
							{expire_repack_chunk_request, AbsoluteOffset}),
					PackingArgs = {pack_disk_pool_chunk, ChunkDataKey,
							{RequiredPacking, DataPath, Offset, DataRoot, TXPath}},
					gen_server:cast(self(), {process_disk_pool_chunk_offsets, Iterator, false,
							Args}),
					{noreply, State#sync_data_state{
							packing_map = PackingMap#{ AbsoluteOffset => PackingArgs } }}
			end
	end.

is_disk_pool_chunk(AbsoluteOffset, ChunkDataKey, State) ->
	case ar_sync_record:is_recorded(AbsoluteOffset, ?MODULE) of
		{{true, spora_2_5}, _StoreID} ->
			false;
		{{true, {spora_2_6, _}}, _StoreID} ->
			false;
		{{true, unpacked}, StoreID} when StoreID /= "default" ->
			case ar_storage_module:get_packing(StoreID) == unpacked of
				true ->
					false;
				false ->
					is_disk_pool_chunk2(AbsoluteOffset, ChunkDataKey, State)
			end;
		_ ->
			is_disk_pool_chunk2(AbsoluteOffset, ChunkDataKey, State)
	end.

is_disk_pool_chunk2(AbsoluteOffset, ChunkDataKey, State) ->
	#sync_data_state{ chunks_index = ChunksIndex,
			recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(AbsoluteOffset, Map, sets:new()),
	case sets:is_element(ChunkDataKey, Set) of
		true ->
			%% Search the cache to avoid an extra disk read.
			false;
		false ->
			case ar_kv:get(ChunksIndex, << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>) of
				not_found ->
					true;
				{ok, V} ->
					{Key, _, _, _, _, _} = binary_to_term(V),
					Key == ChunkDataKey;
				Error ->
					Error
			end
	end.

process_unpacked_chunk(ChunkArgs, Args, State) ->
	{_AbsoluteTXStartOffset, _TXSize, _DataPath, _TXPath, _DataRoot, _Chunk, ChunkID,
			_ChunkEndOffset, _Strict, Peer, Byte} = Args,
	{_Packing, Chunk, AbsoluteEndOffset, _TXRoot, ChunkSize} = ChunkArgs,
	?LOG_DEBUG([{event, validating_unpacked_fetched_chunk}, {offset, AbsoluteEndOffset}]),
	case validate_chunk_id_size(Chunk, ChunkID, ChunkSize) of
		false ->
			?LOG_DEBUG([{event, invalid_unpacked_fetched_chunk}, {offset, AbsoluteEndOffset}]),
			process_invalid_fetched_chunk(Peer, Byte, State);
		true ->
			?LOG_DEBUG([{event, valid_unpacked_fetched_chunk}, {offset, AbsoluteEndOffset}]),
			process_valid_fetched_chunk(ChunkArgs, Args, State)
	end.

validate_chunk_id_size(Chunk, ChunkID, ChunkSize) ->
	case ar_tx:generate_chunk_id(Chunk) == ChunkID of
		false ->
			false;
		true ->
			ChunkSize == byte_size(Chunk)
	end.

read_data_path(ChunkDataKey, State) ->
	#sync_data_state{ chunk_data_db = ChunkDataDB } = State,
	case ar_kv:get(ChunkDataDB, ChunkDataKey) of
		{ok, V} ->
			case binary_to_term(V) of
				{_Chunk, DataPath} ->
					DataPath;
				DataPath ->
					DataPath
			end;
		not_found ->
			not_found
	end.

store_repacked_chunk(ChunkArgs, PreviousPacking, Args, State) ->
	#sync_data_state{ store_id = StoreID } = State,
	{Packing, Chunk, AbsoluteOffset, _, ChunkSize} = ChunkArgs,
	case ar_sync_record:is_recorded(AbsoluteOffset, PreviousPacking, ?MODULE, StoreID) of
		false ->
			%% The chunk should have been removed or packed in the meantime.
			ok;
		true ->
			PaddedOffset = get_chunk_padded_offset(AbsoluteOffset),
			StartOffset = AbsoluteOffset - ChunkSize,
			case ar_sync_record:delete(PaddedOffset, StartOffset, ?MODULE, StoreID) of
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_reset_sync_record_before_updating},
							{reason, io_lib:format("~p", [Reason])}, {store_id, StoreID}]);
				ok ->
					TargetStoreIDs = get_target_store_ids(PaddedOffset, Packing, ChunkSize),
					{DataRoot, DataPath, TXPath, RelativeOffset} = Args,
					Args2 = {Packing, DataPath, RelativeOffset, DataRoot, TXPath},
					[gen_server:cast(list_to_atom("ar_data_sync_" ++ TargetStoreID),
							{store_chunk, ChunkArgs, Args2})
							|| TargetStoreID <- TargetStoreIDs, TargetStoreID /= "default"],
					case ar_chunk_storage:put(PaddedOffset, Chunk, StoreID) of
						ok ->
							case ar_sync_record:add(PaddedOffset, StartOffset, Packing,
									?MODULE, StoreID) of
								{error, Reason} ->
									?LOG_ERROR([{event, failed_to_record_repacked_chunk},
											{reason, io_lib:format("~p", [Reason])},
											{store_id, StoreID}]);
								ok ->
									ar_events:send(data_sync, {add_range, StartOffset,
											PaddedOffset, StoreID}),
									ok
							end;
						{error, Reason} ->
							?LOG_ERROR([{event, failed_to_write_repacked_chunk},
									{reason, io_lib:format("~p", [Reason])},
									{store_id, StoreID}])
					end
			end
	end,
	{noreply, State}.

data_root_index_iterator_v2(DataRootKey, TXStartOffset, DataRootIndex) ->
	{DataRootKey, TXStartOffset, DataRootIndex, 1}.

data_root_index_next_v2({_, _, _, Count}, Limit) when Count > Limit ->
	none;
data_root_index_next_v2({_, 0, _, _}, _Limit) ->
	none;
data_root_index_next_v2({DataRootKey, TXStartOffset, DataRootIndex, Count}, _Limit) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	Key = data_root_key_v2(DataRoot, TXSize, TXStartOffset - 1),
	case ar_kv:get_prev(DataRootIndex, Key) of
		none ->
			none;
		{ok, << DataRoot:32/binary, TXSizeSize:8, TXSize:(TXSizeSize * 8),
				TXStartOffset2Size:8, TXStartOffset2:(TXStartOffset2Size * 8) >>, TXPath} ->
			{ok, TXRoot} = ar_merkle:extract_root(TXPath),
			{{TXStartOffset2, TXRoot, TXPath},
					{DataRootKey, TXStartOffset2, DataRootIndex, Count + 1}};
		{ok, _, _} ->
			none
	end.

data_root_index_get_key(Iterator) ->
	element(1, Iterator).

data_root_index_iterator(TXRootMap) ->
	{maps:fold(
		fun(TXRoot, Map, Acc) ->
			maps:fold(
				fun(Offset, TXPath, Acc2) ->
					gb_sets:insert({Offset, TXRoot, TXPath}, Acc2)
				end,
				Acc,
				Map
			)
		end,
		gb_sets:new(),
		TXRootMap
	), 0}.

data_root_index_next({_Index, Count}, Limit) when Count >= Limit ->
	none;
data_root_index_next({Index, Count}, _Limit) ->
	case gb_sets:is_empty(Index) of
		true ->
			none;
		false ->
			{Element, Index2} = gb_sets:take_largest(Index),
			{Element, {Index2, Count + 1}}
	end.

is_sync_buffer_full(StoreID) ->
	case ets:lookup(ar_data_sync_state, {sync_buffer_size, StoreID}) of
		[{_, Size}] when Size > ?SYNC_BUFFER_SIZE ->
			true;
		_ ->
			false
	end.

decrement_sync_buffer_size(StoreID) ->
	ets:update_counter(ar_data_sync_state, {sync_buffer_size, StoreID}, {2, -1},
			{{sync_buffer_size, StoreID}, 0}).

increment_sync_buffer_size(StoreID) ->
	ets:update_counter(ar_data_sync_state, {sync_buffer_size, StoreID}, {2, 1},
			{{sync_buffer_size, StoreID}, 1}).

record_sync_buffer_size_metric(StoreID) ->
	case ets:lookup(ar_data_sync_state, {sync_buffer_size, StoreID}) of
		[{_, Size}] ->
			prometheus_gauge:set(sync_buffer_size, [StoreID], Size);
		_ ->
			ok
	end.
