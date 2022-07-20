-module(ar_data_sync).

-behaviour(gen_server).

-export([start_link/0, join/3, add_tip_block/4, add_block/2, is_chunk_proof_ratio_attractive/3,
		add_chunk/1, add_data_root_to_disk_pool/3, maybe_drop_data_root_from_disk_pool/3,
		get_chunk/2, get_tx_data/1, get_tx_data/2, get_tx_offset/1, has_data_root/2,
		request_tx_data_removal/3, request_data_removal/4, sync_interval/2,
		record_disk_pool_chunks_count/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).
-export([update_disk_pool_data_roots/0]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_chunk_storage.hrl").

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Notify the server the node has joined the network on the given block index.
join(RecentBI, Packing_2_5_Threshold, StrictDataSplitThreshold) ->
	gen_server:cast(?MODULE, {join, RecentBI, Packing_2_5_Threshold, StrictDataSplitThreshold}).

%% @doc Notify the server about the new tip block.
add_tip_block(Packing_2_5_Threshold, StrictDataSplitThreshold, BlockTXPairs, RecentBI) ->
	gen_server:cast(?MODULE, {add_tip_block, Packing_2_5_Threshold, StrictDataSplitThreshold,
			BlockTXPairs, RecentBI}).

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
		[{_, {S, T, TXIDSet}}] ->
				ets:insert(ar_disk_pool_data_roots,
						{Key, {S, T, sets:add_element(TXID, TXIDSet)}})
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
		[{_, {Size, T, TXIDs}}] ->
			case sets:subtract(TXIDs, sets:from_list([TXID])) of
				TXIDs ->
					ok;
				UpdatedSet ->
					case sets:size(UpdatedSet) of
						0 ->
							ets:delete(ar_disk_pool_data_roots, Key),
							ets:update_counter(ar_data_sync_state, disk_pool_size, {2, -Size});
						_ ->
							ets:insert(ar_disk_pool_data_roots, {Key, {Size, T, UpdatedSet}})
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
%%
%%	packing	spora_2_5 or unpacked
%%	pack	if false and a packed chunk is requested but stored unpacked or
%%			an unpacked chunk is requested but stored packed, return
%%			{error, chunk_not_found} instead of packing/unpacking; true by default
%%	search_fast_storage_only	if true, do not look for the chunk in RocksDB and return
%%								only the chunk, without the proof; false by default
%%	bucket_based_offset			does not play a role for the offsets before
%%								strict_data_split_threshold (weave_size of the block preceding
%%								the fork 2.5 block); if true, return the chunk which ends in
%%								the same 256 KiB bucket starting from
%%								strict_data_split_threshold where borders belong to the
%%								buckets on the left; true by default
get_chunk(Offset, #{ packing := Packing } = Options) ->
	case ar_sync_record:is_recorded(Offset, ?MODULE) of
		false ->
			{error, chunk_not_found};
		{true, StoredPacking} ->
			Pack = maps:get(pack, Options, true),
			case {Pack, Packing == StoredPacking} of
				{false, false} ->
					{error, chunk_not_found};
				{_, true} ->
					get_chunk(Offset, Pack, Packing, Packing, Options);
				{true, false} ->
					get_chunk(Offset, Pack, Packing, StoredPacking, Options)
			end
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
	case ets:lookup(ar_data_sync_state, tx_index) of
		[] ->
			{error, not_joined};
		[{_, TXIndex}] ->
			[{_, ChunksIndex}] = ets:lookup(ar_data_sync_state, chunks_index),
			[{_, ChunkDataDB}] = ets:lookup(ar_data_sync_state, chunk_data_db),
			get_tx_data(TXIndex, ChunksIndex, ChunkDataDB, TXID, SizeLimit)
	end.

%% @doc Return the global end offset and size for the given transaction.
get_tx_offset(TXID) ->
	case ets:lookup(ar_data_sync_state, tx_index) of
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
			case get_data_root_offset(DataRootKey) of
				{ok, _} ->
					true;
				_ ->
					false
			end
	end.

%% @doc Record the metadata of the given block.
add_block(B, SizeTaggedTXs) ->
	gen_server:cast(?MODULE, {add_block, B, SizeTaggedTXs}).

%% @doc Request the removal of the transaction data.
request_tx_data_removal(TXID, Ref, ReplyTo) ->
	gen_server:cast(?MODULE, {remove_tx_data, TXID, Ref, ReplyTo}).

%% @doc Request the removal of the given byte range.
request_data_removal(Start, End, Ref, ReplyTo) ->
	gen_server:cast(?MODULE, {remove_range, End, Start + 1, Ref, ReplyTo}).

%% @doc Make our best attempt to sync the given interval. There is no
%% guarantee the interval will be eventually synced.
sync_interval(Left, Right) ->
	gen_server:cast(?MODULE, {sync_interval, Left, Right}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	?LOG_INFO([{event, ar_data_sync_start}]),
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	ok = ar_events:subscribe(chunk),
	State = init_kv(),
	move_disk_pool_index(State),
	move_data_root_index(State),
	timer:apply_interval(?RECORD_DISK_POOL_CHUNKS_COUNT_FREQUENCY_MS, ar_data_sync,
			record_disk_pool_chunks_count, []),
	{CurrentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize,
			Packing_2_5_Threshold, StrictDataSplitThreshold} = read_data_sync_state(),
	%% Maintain a map of pending, orphaned, and recent data roots
	%% << DataRoot:32/binary, TXSize:256 >> => {Size, Timestamp, TXIDSet}.
	%%
	%% Unconfirmed chunks can be accepted only after their data roots end up in this set.
	%% Each time a pending data root is added to the map the size is set to 0. New chunks
	%% for these data roots are accepted until the corresponding size reaches
	%% #config.max_disk_pool_data_root_buffer_mb or the total size of added pending chunks
	%% reaches #config.max_disk_pool_buffer_mb. When a data root is orphaned, its timestamp
	%% is refreshed so that the chunks have chance to be reincluded later.
	%% After a data root expires, the corresponding chunks are removed from
	%% disk_pool_chunks_index and if they are not in data_root_index - from storage.
	%% TXIDSet keeps track of pending transaction identifiers - if all pending transactions
	%% with the << DataRoot:32/binary, TXSize:256 >> key are dropped from the mempool,
	%% the corresponding entry is removed from DiskPoolDataRoots. When a data root is
	%% confirmed, TXIDSet is set to not_set - from this point on, the key cannot be dropped.
	maps:map(fun(DataRootKey, V) -> ets:insert(ar_disk_pool_data_roots,
			{DataRootKey, V}) end, DiskPoolDataRoots),
	%% The sum of sizes of all pending chunks. When it reaches
	%% ?MAX_DISK_POOL_BUFFER_MB, new chunks with these data roots are rejected.
	ets:insert(ar_data_sync_state, {disk_pool_size, DiskPoolSize}),
	{ok, SyncRecord} = ar_sync_record:get_record(#{ format => raw }, ?MODULE),
	%% A set of intervals temporarily excluded from syncing. The intervals
	%% are recorded here after we failed to find them. The motivation
	%% is to prevent missing intervals from slowing down the syncing process.
	ar_ets_intervals:init_from_gb_set(ar_data_sync_skip_intervals, SyncRecord),
	State2 = State#sync_data_state{
		block_index = CurrentBI,
		weave_size = WeaveSize,
		disk_pool_cursor = first,
		disk_pool_threshold = get_disk_pool_threshold(CurrentBI),
		packing_2_5_threshold = Packing_2_5_Threshold,
		repacking_cursor = 0,
		strict_data_split_threshold = StrictDataSplitThreshold,
		packing_disabled = lists:member(packing, Config#config.disable)
	},
	ets:insert(ar_data_sync_state, {strict_data_split_threshold, StrictDataSplitThreshold}),
	gen_server:cast(?MODULE, check_space),
	gen_server:cast(?MODULE, check_space_warning),
	timer:apply_interval(?REMOVE_EXPIRED_DATA_ROOTS_FREQUENCY_MS, ?MODULE,
			update_disk_pool_data_roots, []),
	lists:foreach(
		fun(_DiskPoolJobNumber) ->
			gen_server:cast(?MODULE, process_disk_pool_item)
		end,
		lists:seq(1, Config#config.disk_pool_jobs)
	),
	gen_server:cast(?MODULE, store_sync_state),
	gen_server:cast(?MODULE, repack_stored_chunks),
	may_be_run_sync_jobs(),
	{ok, State2}.

handle_cast({move_data_root_index, Cursor, N}, State) ->
	move_data_root_index(Cursor, N, State),
	{noreply, State};

handle_cast({join, RecentBI, Packing_2_5_Threshold, StrictDataSplitThreshold}, State) ->
	#sync_data_state{ block_index = CurrentBI, weave_size = CurrentWeaveSize } = State,
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
			ar_chunk_storage:cut(Offset),
			ar_sync_record:cut(Offset, ?MODULE),
			reset_orphaned_data_roots_disk_pool_timestamps(OrphanedDataRoots)
	end,
	State2 =
		State#sync_data_state{
			weave_size = WeaveSize,
			block_index = lists:sublist(RecentBI, ?TRACK_CONFIRMATIONS),
			disk_pool_threshold = get_disk_pool_threshold(RecentBI),
			packing_2_5_threshold = Packing_2_5_Threshold,
			strict_data_split_threshold = StrictDataSplitThreshold
		},
	ets:insert(ar_data_sync_state, {strict_data_split_threshold, StrictDataSplitThreshold}),
	gen_server:cast(?MODULE, collect_sync_intervals),
	run_sync_jobs(),
	store_sync_state(State2),
	{noreply, State2};

handle_cast({add_tip_block, Packing_2_5_Threshold, StrictDataSplitThreshold, BlockTXPairs, BI},
		State) ->
	#sync_data_state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex,
		weave_size = CurrentWeaveSize,
		block_index = CurrentBI
	} = State,
	{BlockStartOffset, Blocks} = pick_missing_blocks(CurrentBI, BlockTXPairs),
	{ok, OrphanedDataRoots} = remove_orphaned_data(State, BlockStartOffset, CurrentWeaveSize),
	[{_, DiskPoolSize}] = ets:lookup(ar_data_sync_state, disk_pool_size),
	{WeaveSize, AddedDataRoots, DiskPoolSize2} = lists:foldl(
		fun ({_BH, []}, Acc) ->
				Acc;
			({_BH, SizeTaggedTXs}, {StartOffset, CurrentAddedDataRoots, CurrentDiskPoolSize}) ->
				{ok, DataRoots} = add_block_data_roots(State, SizeTaggedTXs, StartOffset),
				ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, StartOffset),
				{StartOffset + element(2, lists:last(SizeTaggedTXs)),
					sets:union(CurrentAddedDataRoots, DataRoots),
					sets:fold(
						fun(Key, Acc) ->
							Size = case ets:lookup(ar_disk_pool_data_roots, Key) of
									[] -> 0; [{_, {N, _, _}}] -> N end,
							Acc - Size
						end,
						CurrentDiskPoolSize,
						DataRoots
					)}
		end,
		{BlockStartOffset, sets:new(), DiskPoolSize},
		Blocks
	),
	ets:insert(ar_data_sync_state, {disk_pool_size, DiskPoolSize2}),
	add_block_data_roots_to_disk_pool(AddedDataRoots),
	reset_orphaned_data_roots_disk_pool_timestamps(OrphanedDataRoots),
	ar_chunk_storage:cut(BlockStartOffset),
	ar_sync_record:cut(BlockStartOffset, ?MODULE),
	State2 = State#sync_data_state{
		weave_size = WeaveSize,
		block_index = BI,
		disk_pool_threshold = get_disk_pool_threshold(BI),
		packing_2_5_threshold = Packing_2_5_Threshold,
		strict_data_split_threshold = StrictDataSplitThreshold
	},
	ets:insert(ar_data_sync_state, {strict_data_split_threshold, StrictDataSplitThreshold}),
	store_sync_state(State2),
	{noreply, State2};

handle_cast({add_block, B, SizeTaggedTXs}, State) ->
	add_block(B, SizeTaggedTXs, State),
	{noreply, State};

handle_cast(check_space, State) ->
	ar_util:cast_after(ar_disksup:get_disk_space_check_frequency(), ?MODULE, check_space),
	HaveSpace = have_free_space(),
	ets:insert(ar_data_sync_state, {have_disk_space, HaveSpace}),
	{noreply, State#sync_data_state{ sync_disk_space = HaveSpace }};

handle_cast(check_space_warning, #sync_data_state{ sync_disk_space = false } = State) ->
	Msg =
		"The node has stopped syncing data - the available disk space is"
		" less than ~s. Add more disk space if you wish to store more data.~n",
	ar:console(Msg, [ar_util:bytes_to_mb_string(?DISK_DATA_BUFFER_SIZE)]),
	?LOG_INFO([{event, ar_data_sync_stopped_syncing}, {reason, little_disk_space_left}]),
	ar_util:cast_after(?DISK_SPACE_WARNING_FREQUENCY, ?MODULE, check_space_warning),
	{noreply, State};
handle_cast(check_space_warning, State) ->
	ar_util:cast_after(?DISK_SPACE_WARNING_FREQUENCY, ?MODULE, check_space_warning),
	{noreply, State};

%% Pick a random not synced interval and place it in the syncing queue.
handle_cast(collect_sync_intervals, #sync_data_state{ sync_disk_space = false } = State) ->
	Delay = ar_disksup:get_disk_space_check_frequency(),
	ar_util:cast_after(Delay, ?MODULE, collect_sync_intervals),
	{noreply, State};
handle_cast(collect_sync_intervals, State) ->
	#sync_data_state{ weave_size = WeaveSize, sync_intervals_queue = Q } = State,
	case queue:len(Q) > ?SYNC_INTERVALS_MAX_QUEUE_SIZE of
		true ->
			ar_util:cast_after(500, ?MODULE, collect_sync_intervals);
		false ->
			spawn_link(
				fun() ->
					process_flag(trap_exit, true),
					catch register(ar_sync_intervals_collector, self()),
					find_random_interval(WeaveSize),
					ar_util:cast_after(500, ?MODULE, collect_sync_intervals)
				end
			)
	end,
	{noreply, State};

handle_cast({enqueue_intervals, Peer, Intervals}, State) ->
	#sync_data_state{ sync_intervals_queue = Q,
			sync_intervals_queue_intervals = QIntervals } = State,
	%% Only keep unique intervals. We may get some duplicates for two
	%% reasons:
	%% 1) find_random_interval might choose the same interval several
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
				{lists:foldl(
					fun(Start2, Acc2) ->
						End2 = min(Start2 + ?DATA_CHUNK_SIZE, End),
						queue:in({Start2, End2, Peer}, Acc2)
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
	ar_ets_intervals:add(ar_data_sync_skip_intervals, Right, Left),
	ar_util:cast_after(?EXCLUDE_MISSING_INTERVAL_TIMEOUT_MS, ?MODULE,
			{re_include_interval_for_syncing, Left, Right}),
	{noreply, State};

handle_cast({re_include_interval_for_syncing, Left, Right}, State) ->
	ar_ets_intervals:delete(ar_data_sync_skip_intervals, Right, Left),
	{noreply, State};

handle_cast({sync_interval, _, _},
		#sync_data_state{ sync_disk_space = false } = State) ->
	{noreply, State};
handle_cast({sync_interval, Left, Right}, State) ->
	spawn(fun() -> process_flag(trap_exit, true), find_subintervals(Left, Right) end),
	{noreply, State};

handle_cast(sync_random_interval, State) ->
	#sync_data_state{ sync_intervals_queue = Q,
			sync_intervals_queue_intervals = QIntervals } = State,
	case queue:out(Q) of
		{empty, _} ->
			ar_util:cast_after(500, ?MODULE, sync_random_interval),
			{noreply, State};
		{{value, {Start, End, _Peer} = Interval}, Q2} ->
			gen_server:cast(?MODULE, {sync_chunk, [Interval], loop}),
			{noreply, State#sync_data_state{ sync_intervals_queue = Q2,
					sync_intervals_queue_intervals = ar_intervals:delete(QIntervals, End, Start) }}
	end;

handle_cast({sync_chunk, [{Byte, RightBound, _} | SubIntervals], Loop}, State)
		when Byte >= RightBound ->
	gen_server:cast(?MODULE, {sync_chunk, SubIntervals, Loop}),
	{noreply, State};
handle_cast({sync_chunk, [], loop}, State) ->
	gen_server:cast(?MODULE, sync_random_interval),
	{noreply, State};
handle_cast({sync_chunk, [], do_not_loop}, State) ->
	{noreply, State};
handle_cast({sync_chunk, _, _} = Cast,
		#sync_data_state{ sync_disk_space = false } = State) ->
	ar_util:cast_after(ar_disksup:get_disk_space_check_frequency(), ?MODULE, Cast),
	{noreply, State};
handle_cast({sync_chunk, _, _} = Cast,
		#sync_data_state{ sync_buffer_size = Size } = State)
			when Size >= ?SYNC_BUFFER_SIZE ->
	ar_util:cast_after(200, ?MODULE, Cast),
	{noreply, State};
handle_cast({sync_chunk, [{Byte, RightBound, Peer} | SubIntervals], Loop}, State) ->
	#sync_data_state{ sync_buffer_size = Size } = State,
	Byte2 = ar_tx_blacklist:get_next_not_blacklisted_byte(Byte + 1),
	case Byte2 > Byte + 1 of
		true ->
			gen_server:cast(?MODULE, {exclude_interval_from_syncing, Byte, Byte2 - 1});
		false ->
			ok
	end,
	case Byte2 >= RightBound of
		true ->
			gen_server:cast(?MODULE, {sync_chunk, SubIntervals, Loop}),
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
									{reason, io_lib:format("~p", [Reason])}]),
							ar_events:send(peer, {bad_response, {Peer, chunk, Reason}}),
							gen_server:cast(Self, dec_sync_buffer_size),
							gen_server:cast(Self, {sync_chunk, SubIntervals, Loop})
					end
				end
			),
			{noreply, State#sync_data_state{ sync_buffer_size = Size + 1 }}
	end;

handle_cast({store_fetched_chunk, Peer, Time, TransferSize, Byte, RightBound, Proof,
		SubIntervals, Loop}, State) ->
	#sync_data_state{ packing_map = PackingMap, block_index = RecentBI,
			strict_data_split_threshold = StrictDataSplitThreshold } = State,
	#{ data_path := DataPath, tx_path := TXPath, chunk := Chunk, packing := Packing } = Proof,
	SeekByte = get_chunk_seek_offset(Byte + 1, StrictDataSplitThreshold) - 1,
	{BlockStartOffset, BlockEndOffset, TXRoot} = ar_block_index:get_block_bounds(SeekByte,
			RecentBI),
	BlockSize = BlockEndOffset - BlockStartOffset,
	Offset = SeekByte - BlockStartOffset,
	{Strict, ValidateDataPathFun} =
		case BlockStartOffset >= StrictDataSplitThreshold of
			true ->
				{true, fun ar_merkle:validate_path_strict_data_split/4};
			false ->
				{false, fun ar_merkle:validate_path_strict_borders/4}
		end,
	case validate_proof(TXRoot, BlockStartOffset, Offset, BlockSize, Proof,
			ValidateDataPathFun) of
		{need_unpacking, AbsoluteOffset, ChunkArgs, VArgs} ->
			ar_events:send(peer, {served_chunk, Peer, Time, TransferSize}),
			gen_server:cast(?MODULE, {sync_chunk, [{get_chunk_padded_offset(AbsoluteOffset,
					StrictDataSplitThreshold) + 1, RightBound, Peer} | SubIntervals], Loop}),
			{Packing, DataRoot, TXStartOffset, ChunkEndOffset, TXSize, ChunkID} = VArgs,
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			Args = {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
					Chunk, ChunkID, ChunkEndOffset, Strict, Peer, Byte},
			case maps:is_key(AbsoluteOffset, PackingMap) of
				true ->
					?LOG_DEBUG([{event, fetched_chunk_already_being_packed},
							{offset, AbsoluteOffset}, {byte, Byte}]),
					Byte2 = get_chunk_padded_offset(AbsoluteOffset, StrictDataSplitThreshold) + 1,
					gen_server:cast(?MODULE, dec_sync_buffer_size),
					gen_server:cast(?MODULE, {sync_chunk, [{Byte2, RightBound, Peer}
							| SubIntervals], Loop}),
					{noreply, State};
				false ->
					?LOG_DEBUG([{event, schedule_fetched_chunk_unpacking},
							{offset, AbsoluteOffset}]),
					ar_events:send(chunk, {unpack_request, AbsoluteOffset, ChunkArgs}),
					ar_util:cast_after(600000, ?MODULE,
							{expire_unpack_fetched_chunk_request, AbsoluteOffset}),
					{noreply, State#sync_data_state{
							packing_map = PackingMap#{
								AbsoluteOffset => {unpack_fetched_chunk, Args} } }}
			end;
		false ->
			gen_server:cast(?MODULE, {sync_chunk, SubIntervals, Loop}),
			process_invalid_fetched_chunk(Peer, Byte, State);
		{true, DataRoot, TXStartOffset, ChunkEndOffset, TXSize, ChunkSize, ChunkID} ->
			ar_events:send(peer, {served_chunk, Peer, Time, TransferSize}),
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			AbsoluteEndOffset = AbsoluteTXStartOffset + ChunkEndOffset,
			gen_server:cast(?MODULE, {sync_chunk, [{get_chunk_padded_offset(AbsoluteEndOffset,
					StrictDataSplitThreshold) + 1, RightBound, Peer} | SubIntervals], Loop}),
			ChunkArgs = {unpacked, Chunk, AbsoluteEndOffset, TXRoot, ChunkSize},
			AbsoluteTXStartOffset = BlockStartOffset + TXStartOffset,
			Args = {AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot,
					Chunk, ChunkID, ChunkEndOffset, Strict, Peer, Byte},
			process_valid_fetched_chunk(ChunkArgs, Args, State)
	end;

handle_cast(dec_sync_buffer_size, #sync_data_state{ sync_buffer_size = Size } = State) ->
	{noreply, State#sync_data_state{ sync_buffer_size = Size - 1 }};

handle_cast(process_disk_pool_item, #sync_data_state{ sync_disk_space = false } = State) ->
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, ?MODULE, process_disk_pool_item),
	{noreply, State};
handle_cast(process_disk_pool_item, #sync_data_state{ disk_pool_scan_pause = true } = State) ->
	ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, ?MODULE, process_disk_pool_item),
	{noreply, State};
handle_cast(process_disk_pool_item, State) ->
	#sync_data_state{
		disk_pool_cursor = Cursor,
		disk_pool_chunks_index = DiskPoolChunksIndex,
		disk_pool_full_scan_start_key = FullScanStartKey,
		disk_pool_full_scan_start_timestamp = Timestamp,
		currently_processed_disk_pool_keys = CurrentlyProcessedDiskPoolKeys
	} = State,
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
			ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, ?MODULE, resume_disk_pool_scan),
			ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, ?MODULE, process_disk_pool_item),
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
							ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, ?MODULE,
									resume_disk_pool_scan),
							ar_util:cast_after(?DISK_POOL_SCAN_DELAY_MS, ?MODULE,
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

handle_cast({process_disk_pool_chunk_offset, MayConclude, TXArgs, Args, Iterator}, State) ->
	{TXStartOffset, TXRoot, TXPath} = TXArgs,
	{Offset, _, _, _, _, _, _, _} = Args,
	AbsoluteOffset = TXStartOffset + Offset,
	process_disk_pool_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, MayConclude,
			Args, State);

handle_cast({remove_tx_data, TXID, Ref, From}, State) ->
	#sync_data_state{ tx_index = TXIndex } = State,
	case ar_kv:get(TXIndex, TXID) of
		{ok, Value} ->
			{End, Size} = binary_to_term(Value),
			Start = End - Size,
			gen_server:cast(?MODULE, {remove_range, End, Start + 1, Ref, From}),
			{noreply, State};
		not_found ->
			?LOG_WARNING([
				{event, tx_offset_not_found},
				{tx, ar_util:encode(TXID)}
			]),
			{noreply, State};
		{error, Reason} ->
			?LOG_ERROR([
				{event, failed_to_fetch_blacklisted_tx_offset},
				{tx, ar_util:encode(TXID)},
				{reason, Reason}
			]),
			{noreply, State}
	end;

handle_cast({remove_range, End, Cursor, Ref, From}, State) when Cursor > End ->
	From ! {removed_range, Ref},
	{noreply, State};
handle_cast({remove_range, End, Cursor, Ref, From}, State) ->
	#sync_data_state{ chunks_index = ChunksIndex,
			strict_data_split_threshold = StrictDataSplitThreshold,
			recently_processed_disk_pool_offsets = RecentlyProcessedOffsets } = State,
	case get_chunk_by_byte(ChunksIndex, Cursor) of
		{ok, Key, Chunk} ->
			<< AbsoluteEndOffset:?OFFSET_KEY_BITSIZE >> = Key,
			case AbsoluteEndOffset > End of
				true ->
					From ! {removed_range, Ref},
					{noreply, State};
				false ->
					{_, _, _, _, _, ChunkSize} = binary_to_term(Chunk),
					PaddedStartOffset = get_chunk_padded_offset(AbsoluteEndOffset - ChunkSize,
							StrictDataSplitThreshold),
					%% 1) store updated sync record
					%% 2) remove chunk
					%% 3) update chunks_index
					%%
					%% The order is important - in case the VM crashes,
					%% we will not report false positives to peers,
					%% and the chunk can still be removed upon retry.
					PaddedOffset = get_chunk_padded_offset(AbsoluteEndOffset,
							StrictDataSplitThreshold),
					ok = ar_sync_record:delete(PaddedOffset, PaddedStartOffset, ?MODULE),
					ok = ar_chunk_storage:delete(PaddedOffset),
					ok = ar_kv:delete(ChunksIndex, Key),
					gen_server:cast(?MODULE,
							{remove_range, End, AbsoluteEndOffset + 1, Ref, From}),
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
			gen_server:cast(?MODULE, {remove_range, End, NextCursor, Ref, From}),
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
		repack_stored_chunk ->
			{noreply, State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) }};
		_ ->
			{noreply, State}
	end;

handle_cast({expire_unpack_fetched_chunk_request, Offset}, State) ->
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Offset, PackingMap, not_found) of
		{unpack_fetched_chunk, _Args} ->
			gen_server:cast(?MODULE, dec_sync_buffer_size),
			{noreply, State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) }};
		_ ->
			{noreply, State}
	end;

handle_cast(store_sync_state, State) ->
	store_sync_state(State),
	ar_util:cast_after(?STORE_STATE_FREQUENCY_MS, ?MODULE, store_sync_state),
	{noreply, State};

handle_cast(repack_stored_chunks, #sync_data_state{ packing_disabled = true } = State) ->
	{noreply, State};
handle_cast(repack_stored_chunks,
		#sync_data_state{ packing_2_5_threshold = infinity } = State) ->
	ar_util:cast_after(30000, ?MODULE, repack_stored_chunks),
	{noreply, State};
handle_cast(repack_stored_chunks, #sync_data_state{ packing_map = Map } = State)
		when map_size(Map) >= ?PACKING_BUFFER_SIZE ->
	ar_util:cast_after(200, ?MODULE, repack_stored_chunks),
	{noreply, State};
handle_cast(repack_stored_chunks, State) ->
	#sync_data_state{ packing_2_5_threshold = PackingThreshold,
			disk_pool_threshold = DiskPoolThreshold, repacking_cursor = Cursor } = State,
	SearchStart = max(Cursor, PackingThreshold),
	case ar_sync_record:get_next_synced_interval(SearchStart, DiskPoolThreshold, unpacked,
			?MODULE) of
		not_found ->
			ar_util:cast_after(10000, ?MODULE, repack_stored_chunks),
			{noreply, State#sync_data_state{ repacking_cursor = 0 }};
		{End, Start} ->
			Start2 = max(Start, PackingThreshold),
			?LOG_DEBUG([{event, picked_interval_for_repacking}, {left, Start2}, {right, End}]),
			gen_server:cast(?MODULE, {repack_stored_chunks, Start2, End}),
			{noreply, State}
	end;

handle_cast({repack_stored_chunks, Offset, End}, State) when Offset >= End ->
	ar_util:cast_after(200, ?MODULE, repack_stored_chunks),
	{noreply, State#sync_data_state{ repacking_cursor = End }};
handle_cast({repack_stored_chunks, Offset, End},
		#sync_data_state{ packing_map = Map } = State)
			when map_size(Map) >= ?PACKING_BUFFER_SIZE ->
	ar_util:cast_after(200, ?MODULE, {repack_stored_chunks, Offset, End}),
	{noreply, State};
handle_cast({repack_stored_chunks, Offset, End}, State) ->
	#sync_data_state{ chunks_index = ChunksIndex, packing_map = PackingMap } = State,
	gen_server:cast(?MODULE, {repack_stored_chunks, Offset + ?DATA_CHUNK_SIZE, End}),
	CheckRecorded = ar_sync_record:is_recorded(Offset + 1, ar_chunk_storage)
			andalso ar_sync_record:is_recorded(Offset + 1, unpacked, ?MODULE),
	CheckPacking = case CheckRecorded of
		false ->
			skip;
		true ->
			case ar_chunk_storage:get(Offset) of
				not_found ->
					?LOG_WARNING([{event, chunk_in_sync_record_but_not_chunk_storage},
							{byte, Offset}]),
					skip;
				{O, C} ->
					%% Note that although for packed chunks the offset returned from
					%% ar_chunk_storage:get/1 might not equal their actual end offset
					%% because of padding, here the offset should be correct because we
					%% only store 256 KiB unpacked chunks in ar_chunk_storage.
					case maps:is_key(O, PackingMap) of
						true ->
							skip;
						false ->
							{ok, O, C}
					end
			end
	end,
	case CheckPacking of
		skip ->
			{noreply, State};
		{ok, AbsoluteOffset, Chunk} ->
			case ar_kv:get(ChunksIndex, << AbsoluteOffset:(?OFFSET_KEY_BITSIZE) >>) of
				not_found ->
					?LOG_WARNING([{event, chunk_in_chunk_storage_but_not_chunks_index},
							{offset, AbsoluteOffset}]),
					{noreply, State};
				{error, Reason} ->
					?LOG_WARNING([{event, failed_to_read_chunk_metadata},
							{error, io_lib:format("~p", [Reason])}]),
					{noreply, State};
				{ok, V} ->
					{_, TXRoot, _, _, _, ChunkSize} = binary_to_term(V),
					ChunkSize = (?DATA_CHUNK_SIZE),
					ar_events:send(chunk, {repack_request, AbsoluteOffset,
							{spora_2_5, unpacked, Chunk, AbsoluteOffset, TXRoot, ChunkSize}}),
					ar_util:cast_after(600000, ?MODULE,
							{expire_repack_chunk_request, AbsoluteOffset}),
					{noreply, State#sync_data_state{
						packing_map = maps:put(AbsoluteOffset, repack_stored_chunk,
							PackingMap) }}
			end
	end;

handle_cast({remove_recently_processed_disk_pool_offset, Offset, ChunkDataKey}, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(Offset, Map, sets:new()),
	Map2 = maps:put(Offset, sets:del_element(ChunkDataKey, Set), Map),
	{noreply, State#sync_data_state{ recently_processed_disk_pool_offsets = Map2 }};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

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
	#sync_data_state{ packing_map = PackingMap } = State,
	case maps:get(Offset, PackingMap, not_found) of
		{pack_fetched_chunk, Args} ->
			?LOG_DEBUG([{event, storing_packed_fetched_chunk}, {offset, Offset}]),
			State2 = State#sync_data_state{ packing_map = maps:remove(Offset, PackingMap) },
			store_chunk(ChunkArgs, Args, State2),
			{noreply, State2};
		{pack_disk_pool_chunk, ChunkDataKey, Args} ->
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
		repack_stored_chunk ->
			?LOG_DEBUG([{event, storing_repacked_stored_chunk}, {offset, Offset}]),
			store_repacked_chunk(ChunkArgs, State#sync_data_state{
					packing_map = maps:remove(Offset, PackingMap) });
		_ ->
			{noreply, State}
	end;

handle_info({event, chunk, _}, State) ->
	{noreply, State};

handle_info({'EXIT', _PID, normal}, State) ->
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

update_disk_pool_data_roots() ->
	Now = os:system_time(microsecond),
	{ok, Config} = application:get_env(arweave, config),
	DiskPoolSize = ets:foldl(
		fun({Key, {Size, Timestamp, _}}, Acc) ->
			case Timestamp +
					Config#config.disk_pool_data_root_expiration_time * 1000000 > Now of
				true ->
					Acc + Size;
				false ->
					ets:delete(ar_disk_pool_data_roots, Key),
					Acc
			end
		end,
		0,
		ar_disk_pool_data_roots
	),
	ets:insert(ar_data_sync_state, {disk_pool_size, DiskPoolSize}),
	prometheus_gauge:set(pending_chunks_size, DiskPoolSize).

get_chunk(Offset, Pack, Packing, StoredPacking, Options) ->
	case maps:get(search_fast_storage_only, Options, false) of
		true ->
			case maps:get(bucket_based_offset, Options, true) of
				false ->
					{error, option_set_not_supported};
				true ->
					get_chunk_from_fast_storage(Offset, Pack, Packing, StoredPacking)
			end;
		false ->
			case ets:lookup(ar_data_sync_state, chunks_index) of
				[] ->
					{error, not_joined};
				[{_, ChunksIndex}] ->
					[{_, ChunkDataDB}] = ets:lookup(ar_data_sync_state, chunk_data_db),
					get_chunk(Offset, Pack, Packing, StoredPacking, ChunksIndex, ChunkDataDB,
							maps:get(bucket_based_offset, Options, true))
			end
	end.

get_chunk_from_fast_storage(Offset, Pack, Packing, StoredPacking) ->
	case ar_chunk_storage:get(Offset - 1) of
		not_found ->
			{error, chunk_not_found};
		{_EndOffset, Chunk} ->
			case ar_sync_record:is_recorded(Offset, StoredPacking, ?MODULE) of
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

get_chunk(Offset, Pack, Packing, StoredPacking, ChunksIndex, ChunkDataDB, IsBucketBasedOffset) ->
	[{_, StrictDataSplitThreshold}] = ets:lookup(ar_data_sync_state,
			strict_data_split_threshold),
	SeekOffset =
		case IsBucketBasedOffset of
			true ->
				get_chunk_seek_offset(Offset, StrictDataSplitThreshold);
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
				case read_chunk(O, ChunkDataDB, ChunkDataKey) of
					not_found ->
						{error, chunk_not_found};
					{error, reason} ->
						?LOG_ERROR([{event, failed_to_read_chunk}, {reason, reason}]),
						{error, failed_to_read_chunk};
					{ok, ChunkData} ->
						case ar_sync_record:is_recorded(Offset, StoredPacking, ?MODULE) of
							false ->
								%% The chunk should have been re-packed
								%% in the meantime - very unlucky timing.
								{error, chunk_not_found};
							true ->
								{ok, ChunkData, O, Root2, S2, P2}
						end
				end
		end,
	case ReadChunkResult of
		{error, Reason3} ->
			{error, Reason3};
		{ok, {Chunk, DataPath}, ChunkOffset, TXRoot, ChunkSize, TXPath} ->
			PackResult =
				case {Pack, Packing == StoredPacking} of
					{false, true} ->
						{ok, Chunk};
					{false, false} ->
						{error, chunk_not_found};
					{true, true} ->
						{ok, Chunk};
					{true, false} ->
						{ok, Unpacked} = ar_packing_server:unpack(StoredPacking, ChunkOffset,
								TXRoot, Chunk, ChunkSize),
						ar_packing_server:pack(Packing, ChunkOffset, TXRoot, Unpacked)
				end,
			case PackResult of
				{ok, PackedChunk} ->
					Proof = #{ tx_root => TXRoot, chunk => PackedChunk, data_path => DataPath,
							tx_path => TXPath },
					{ok, Proof};
				Error ->
					Error
			end
	end.

%% @doc Return Offset if it is smaller than or equal to StrictDataSplitThreshold.
%% Otherwise, return the offset of the first byte of the chunk + 1. The function
%% returns an offset the chunk can be found under even if Offset is inside padding.
get_chunk_seek_offset(Offset, StrictDataSplitThreshold) ->
	case Offset > StrictDataSplitThreshold of
		true ->
			ar_poa:get_padded_offset(Offset, StrictDataSplitThreshold)
					- (?DATA_CHUNK_SIZE)
					+ 1;
		false ->
			Offset
	end.

%% @doc Return Offset if it is smaller than or equal to StrictDataSplitThreshold.
%% Otherwise, return the offset of the last byte of the chunk + the size of the padding.
get_chunk_padded_offset(Offset, StrictDataSplitThreshold) ->
	case Offset > StrictDataSplitThreshold of
		true ->
			ar_poa:get_padded_offset(Offset, StrictDataSplitThreshold);
		false ->
			Offset
	end.

get_chunk_by_byte(ChunksIndex, Byte) ->
	ar_kv:get_next_by_prefix(ChunksIndex, ?OFFSET_KEY_PREFIX_BITSIZE, ?OFFSET_KEY_BITSIZE,
			<< Byte:?OFFSET_KEY_BITSIZE >>).

read_chunk(Offset, ChunkDataDB, ChunkDataKey) ->
	case ar_kv:get(ChunkDataDB, ChunkDataKey) of
		not_found ->
			not_found;
		{ok, Value} ->
			case binary_to_term(Value) of
				{Chunk, DataPath} ->
					{ok, {Chunk, DataPath}};
				DataPath ->
					case ar_chunk_storage:get(Offset - 1) of
						not_found ->
							not_found;
						{_EndOffset, Chunk} ->
							{ok, {Chunk, DataPath}}
					end
			end;
		Error ->
			Error
	end.

get_tx_data(TXIndex, ChunksIndex, ChunkDataDB, TXID, SizeLimit) ->
	case ar_kv:get(TXIndex, TXID) of
		not_found ->
			{error, not_found};
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_get_tx_data}, {reason, Reason}]),
			{error, failed_to_get_tx_data};
		{ok, Value} ->
			{Offset, Size} = binary_to_term(Value),
			case Size > SizeLimit of
				true ->
					{error, tx_data_too_big};
				false ->
					StartKey = << (Offset - Size):?OFFSET_KEY_BITSIZE >>,
					EndKey = << Offset:?OFFSET_KEY_BITSIZE >>,
					case ar_kv:get_range(ChunksIndex, StartKey, EndKey) of
						{error, Reason} ->
							?LOG_ERROR([{event, failed_to_get_chunks_for_tx_data},
									{reason, Reason}]),
							{error, not_found};
						{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
							{error, not_found};
						{ok, Map} ->
							get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map)
					end
			end
	end.

get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map) ->
	get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map, <<>>).

get_tx_data_from_chunks(_ChunkDataDB, _Offset, 0, _Map, Data) ->
	{ok, iolist_to_binary(Data)};
get_tx_data_from_chunks(ChunkDataDB, Offset, Size, Map, Data) ->
	case maps:get(<< Offset:?OFFSET_KEY_BITSIZE >>, Map, not_found) of
		not_found ->
			{error, not_found};
		Value ->
			{ChunkDataKey, TXRoot, _, _, _, ChunkSize} = binary_to_term(Value),
			case ar_sync_record:is_recorded(Offset, ?MODULE) of
				false ->
					?LOG_WARNING([{event, found_offset_in_chunks_index_not_in_sync_record},
							{offset, Offset}, {chunk_data_key, ar_util:encode(ChunkDataKey)}]),
					{error, not_found};
				{true, Packing} ->
					case read_chunk(Offset, ChunkDataDB, ChunkDataKey) of
						not_found ->
							{error, not_found};
						{error, Reason} ->
							?LOG_ERROR([{event, failed_to_read_chunk_for_tx_data},
									{reason, Reason}]),
							{error, not_found};
						{ok, {Chunk, _}} ->
							case ar_sync_record:is_recorded(Offset, Packing, ?MODULE) of
								false ->
									%% The chunk should have been repacked
									%% in the meantime - very unlucky timing.
									{error, not_found};
								true ->
									{ok, Unpacked} = ar_packing_server:unpack(Packing, Offset,
											TXRoot, Chunk, ChunkSize),
									get_tx_data_from_chunks(
										ChunkDataDB,
										Offset - ChunkSize,
										Size - ChunkSize,
										Map,
										[Unpacked | Data]
									)
							end
					end
			end
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

get_data_root_offset(DataRootKey) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	[{_, DataRootIndex}] = ets:lookup(ar_data_sync_state, data_root_index),
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

init_kv() ->
	BasicOpts = [
		{max_open_files, 1000000}
	],
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
	{ok, DB, [_, CF1, CF2, CF3, CF4, CF5, CF6, CF7]} =
		ar_kv:open("ar_data_sync_db", ColumnFamilyDescriptors),
	{ok, ChunkDataDB} =
		ar_kv:open_without_column_families(
			"ar_data_sync_chunk_db", [
				{max_open_files, 1000000},
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
			"ar_data_sync_disk_pool_chunks_index_db", [
				{max_open_files, 1000000},
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
			"ar_data_sync_data_root_index_db", [
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
		{chunks_index, {DB, CF1}},
		{data_root_index, DataRootIndexDB},
		{data_root_offset_index, {DB, CF3}},
		{tx_index, {DB, CF4}},
		{tx_offset_index, {DB, CF5}},
		{disk_pool_chunks_index, DiskPoolIndexDB},
		{chunk_data_db, ChunkDataDB}
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
	{ok, Config} = application:get_env(arweave, config),
	lists:foreach(
		fun(_SyncingJobNumber) ->
			gen_server:cast(?MODULE, sync_random_interval)
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
			gen_server:cast(?MODULE, {move_data_root_index, Cursor, N + 1});
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
	[{_, DB}] = ets:lookup(ar_data_sync_state, disk_pool_chunks_index),
	case ar_kv:count(DB) of
		Count when is_integer(Count) ->
			prometheus_gauge:set(disk_pool_chunks_count, Count);
		Error ->
			?LOG_WARNING([{event, failed_to_read_disk_pool_chunks_count},
					{error, io_lib:format("~p", [Error])}])
	end.

read_data_sync_state() ->
	case ar_storage:read_term(data_sync_state) of
		{ok, {SyncRecord, RecentBI, DiskPoolDataRoots, _DiskPoolSize}} ->
			DiskPoolSize = calculate_disk_pool_size(DiskPoolDataRoots),
			ok = ar_sync_record:set(SyncRecord, unpacked, ?MODULE),
			WeaveSize = case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
			{RecentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize, infinity, infinity};
		{ok, {RecentBI, DiskPoolDataRoots, _DiskPoolSize}} ->
			DiskPoolSize = calculate_disk_pool_size(DiskPoolDataRoots),
			WeaveSize = case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
			{RecentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize, infinity, infinity};
		{ok, #{ block_index := RecentBI, disk_pool_data_roots := DiskPoolDataRoots,
				packing_2_5_threshold := PackingThreshold,
				strict_data_split_threshold := StrictDataSplitThreshold }} ->
			WeaveSize = case RecentBI of [] -> 0; _ -> element(2, hd(RecentBI)) end,
			DiskPoolSize = calculate_disk_pool_size(DiskPoolDataRoots),
			{RecentBI, DiskPoolDataRoots, DiskPoolSize, WeaveSize, PackingThreshold,
					StrictDataSplitThreshold};
		not_found ->
			{[], #{}, 0, 0, infinity, infinity}
	end.

calculate_disk_pool_size(DiskPoolDataRoots) ->
	maps:fold(fun(_, {Size, _, _}, Acc) -> Acc + Size end, 0, DiskPoolDataRoots).

get_disk_pool_threshold([]) ->
	0;
get_disk_pool_threshold(BI) ->
	ar_mine:get_search_space_upper_bound(BI).

remove_orphaned_data(State, BlockStartOffset, WeaveSize) ->
	ok = remove_orphaned_txs(State, BlockStartOffset, WeaveSize),
	{ok, OrphanedDataRoots} = remove_orphaned_data_roots(State, BlockStartOffset),
	ok = remove_orphaned_data_root_offsets(State, BlockStartOffset, WeaveSize),
	ok = remove_orphaned_chunks(State, BlockStartOffset, WeaveSize),
	{ok, OrphanedDataRoots}.

remove_orphaned_txs(State, BlockStartOffset, WeaveSize) ->
	#sync_data_state{
		tx_offset_index = TXOffsetIndex,
		tx_index = TXIndex
	} = State,
	ok = case ar_kv:get_range(TXOffsetIndex, << BlockStartOffset:?OFFSET_KEY_BITSIZE >>) of
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
	ar_kv:delete_range(
		TXOffsetIndex,
		<< BlockStartOffset:?OFFSET_KEY_BITSIZE >>,
		<< (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>
	).

remove_orphaned_chunks(State, BlockStartOffset, WeaveSize) ->
	#sync_data_state{ chunks_index = ChunksIndex } = State,
	EndKey = << (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>,
	ar_kv:delete_range(ChunksIndex, << (BlockStartOffset + 1):?OFFSET_KEY_BITSIZE >>, EndKey).

remove_orphaned_data_roots(State, BlockStartOffset) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex,
		data_root_index = DataRootIndex,
		weave_size = WeaveSize
	} = State,
	case ar_kv:get_range(DataRootOffsetIndex, << BlockStartOffset:?OFFSET_KEY_BITSIZE >>) of
		{ok, EmptyMap} when map_size(EmptyMap) == 0 ->
			{ok, sets:new()};
		{ok, Map} ->
			maps:fold(
				fun
					(_, _Value, {error, _} = Error) ->
						Error;
					(_, Value, {ok, OrphanedDataRoots}) ->
						{_TXRoot, _BlockSize, DataRootIndexKeySet} = binary_to_term(Value),
						sets:fold(
							fun (_Key, {error, _} = Error) ->
									Error;
								(<< _DataRoot:32/binary, _TXSize:?OFFSET_KEY_BITSIZE >> = Key,
										{ok, Orphaned}) ->
									case remove_orphaned_data_root(DataRootIndex, Key,
											BlockStartOffset, WeaveSize) of
										removed ->
											{ok, sets:add_element(Key, Orphaned)};
										ok ->
											{ok, Orphaned};
										Error ->
											Error
									end;
								(_, Acc) ->
									Acc
							end,
							{ok, OrphanedDataRoots},
							DataRootIndexKeySet
						)
				end,
				{ok, sets:new()},
				Map
			);
		Error ->
			Error
	end.

remove_orphaned_data_root(DataRootIndex, DataRootKey, StartOffset, WeaveSize) ->
	<< DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >> = DataRootKey,
	StartKey = data_root_key_v2(DataRoot, TXSize, StartOffset),
	EndKey = data_root_key_v2(DataRoot, TXSize, WeaveSize),
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

remove_orphaned_data_root_offsets(State, BlockStartOffset, WeaveSize) ->
	#sync_data_state{
		data_root_offset_index = DataRootOffsetIndex
	} = State,
	ar_kv:delete_range(
		DataRootOffsetIndex,
		<< BlockStartOffset:?OFFSET_KEY_BITSIZE >>,
		<< (WeaveSize + 1):?OFFSET_KEY_BITSIZE >>
	).

have_free_space() ->
	ar_storage:get_free_space(?ROCKS_DB_DIR) > ?DISK_DATA_BUFFER_SIZE
		andalso ar_storage:get_free_space(?CHUNK_DIR) > ?DISK_DATA_BUFFER_SIZE.

add_block(B, SizeTaggedTXs, State) ->
	#sync_data_state{
		tx_index = TXIndex,
		tx_offset_index = TXOffsetIndex
	} = State,
	BlockStartOffset = B#block.weave_size - B#block.block_size,
	{ok, _} = add_block_data_roots(State, SizeTaggedTXs, BlockStartOffset),
	ok = update_tx_index(TXIndex, TXOffsetIndex, SizeTaggedTXs, BlockStartOffset).

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
	{BlockSize, DataRootIndexKeySet} = lists:foldl(
		fun ({_, Offset}, {Offset, _} = Acc) ->
				Acc;
			({{padding, _}, Offset}, {_, Acc}) ->
				{Offset, Acc};
			({{_, DataRoot}, Offset}, {_, Acc}) when byte_size(DataRoot) < 32 ->
				{Offset, Acc};
			({{_, DataRoot}, TXEndOffset}, {PrevOffset, CurrentDataRootSet}) ->
				TXPath = ar_merkle:generate_path(TXRoot, TXEndOffset - 1, TXTree),
				TXOffset = CurrentWeaveSize + PrevOffset,
				TXSize = TXEndOffset - PrevOffset,
				DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
				ok = update_data_root_index(State, DataRoot, TXSize, TXOffset, TXPath),
				{TXEndOffset, sets:add_element(DataRootKey, CurrentDataRootSet)}
		end,
		{0, sets:new()},
		SizeTaggedTXs
	),
	case BlockSize > 0 of
		true ->
			ok = ar_kv:put(DataRootOffsetIndex, << CurrentWeaveSize:?OFFSET_KEY_BITSIZE >>,
					term_to_binary({TXRoot, BlockSize, DataRootIndexKeySet}));
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
				[{_, {_, Timeout, _}}] ->
					ets:insert(ar_disk_pool_data_roots, {R, {0, Timeout, not_set}})
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

store_sync_state(State) ->
	#sync_data_state{ block_index = BI, packing_2_5_threshold = Packing_2_5_Threshold,
			strict_data_split_threshold = StrictDataSplitThreshold } = State,
	DiskPoolDataRoots = ets:foldl(
			fun({DataRootKey, V}, Acc) -> maps:put(DataRootKey, V, Acc) end, #{},
			ar_disk_pool_data_roots),
	DiskPoolSize = calculate_disk_pool_size(DiskPoolDataRoots),
	StoredState = #{ block_index => BI, disk_pool_data_roots => DiskPoolDataRoots,
			disk_pool_size => DiskPoolSize, packing_2_5_threshold => Packing_2_5_Threshold,
			strict_data_split_threshold => StrictDataSplitThreshold },
	case ar_storage:write_term(data_sync_state, StoredState) of
		{error, enospc} ->
			?LOG_WARNING([{event, failed_to_dump_state}, {reason, disk_full}]),
			ok;
		ok ->
			ok
	end.

find_random_interval(0) ->
	[];
find_random_interval(WeaveSize) ->
	%% Try keeping no more than ?SYNCED_INTERVALS_TARGET intervals
	%% in the sync record by choosing the appropriate size of continuous
	%% intervals to sync. The motivation is to have a syncing heuristic
	%% that syncs data quickly, keeps the record size small, and has
	%% synced data reasonably spread out. Attempt syncing entire buckets
	%% to improve discoverability of the data. Choose at least one bucket.
	SyncSize = WeaveSize div ?SYNCED_INTERVALS_TARGET,
	SyncBucketCount = max(1, 1 + SyncSize div ?NETWORK_DATA_BUCKET_SIZE),
	BucketCount = max(1, 1 + WeaveSize div ?NETWORK_DATA_BUCKET_SIZE),
	RandomBucket = rand:uniform(BucketCount) - 1,
	BucketStart = RandomBucket * ?NETWORK_DATA_BUCKET_SIZE,
	find_random_interval(BucketStart, SyncBucketCount, WeaveSize).

find_random_interval(Start, SyncBucketCount, WeaveSize) ->
	case get_next_unsynced_interval(Start, WeaveSize) of
		not_found ->
			case Start of
				0 ->
					[];
				_ ->
					find_random_interval(0, SyncBucketCount, WeaveSize)
			end;
		{_Right, Left} ->
			BucketSize = ?NETWORK_DATA_BUCKET_SIZE,
			SyncSize = SyncBucketCount * BucketSize,
			NextBucketBorder = Left - Left rem SyncSize + SyncSize,
			Right = min(WeaveSize, NextBucketBorder),
			find_subintervals(Left, Right)
	end.

%% @doc Return the lowest unsynced interval strictly above the given Offset.
%% The right bound of the interval is at most RightBound.
%% Return not_found if there are no such intervals.
%% Skip the intervals we have recently failed to find.
get_next_unsynced_interval(Offset, RightBound) when Offset >= RightBound ->
	not_found;
get_next_unsynced_interval(Offset, RightBound) ->
	case ets:next(?MODULE, Offset) of
		'$end_of_table' ->
			{RightBound, Offset};
		NextOffset ->
			case ets:lookup(?MODULE, NextOffset) of
				[{NextOffset, Start}] when Start > Offset ->
					End = min(RightBound, Start),
					case ets:next(ar_data_sync_skip_intervals, Offset) of
						'$end_of_table' ->
							{End, Offset};
						SkipEnd ->
							case ets:lookup(ar_data_sync_skip_intervals,
									SkipEnd) of
								[{SkipEnd, SkipStart}]
										when SkipStart >= End ->
									{End, Offset};
								[{SkipEnd, SkipStart}]
										when SkipStart > Offset ->
									{SkipStart, Offset};
								[{SkipEnd, _SkipStart}]
										when SkipEnd < End ->
									{End, SkipEnd};
								_ ->
									get_next_unsynced_interval(SkipEnd, RightBound)
							end
					end;
				_ ->
					get_next_unsynced_interval(NextOffset, RightBound)
			end
	end.

%% @doc Collect the unsynced intervals between Start and RightBound.
get_next_unsynced_intervals(Start, RightBound) ->
	get_next_unsynced_intervals(Start, RightBound, ar_intervals:new()).

get_next_unsynced_intervals(Start, RightBound, Intervals) when Start >= RightBound ->
	Intervals;
get_next_unsynced_intervals(Start, RightBound, Intervals) ->
	case ets:next(?MODULE, Start) of
		'$end_of_table' ->
			ar_intervals:add(Intervals, RightBound, Start);
		NextOffset ->
			case ets:lookup(?MODULE, NextOffset) of
				[{NextOffset, NextStart}] when NextStart > Start ->
					End = min(NextStart, RightBound),
					get_next_unsynced_intervals(NextOffset, RightBound,
							ar_intervals:add(Intervals, End, Start));
				_ ->
					get_next_unsynced_intervals(NextOffset, RightBound, Intervals)
			end
	end.

find_subintervals(Left, Right) when Left >= Right ->
	ok;
find_subintervals(Left, Right) ->
	Bucket = Left div ?NETWORK_DATA_BUCKET_SIZE,
	LeftBound = Left - Left rem ?NETWORK_DATA_BUCKET_SIZE,
	Peers = ar_data_discovery:get_bucket_peers(Bucket),
	RightBound = min(LeftBound + ?NETWORK_DATA_BUCKET_SIZE, Right),
	Results =
		ar_util:pmap(
			fun(Peer) ->
				UnsyncedIntervals = get_next_unsynced_intervals(Left, RightBound),
				case get_peer_intervals(Peer, Left, UnsyncedIntervals) of
					{ok, Intervals} ->
						gen_server:cast(?MODULE, {enqueue_intervals, Peer, Intervals}),
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
			end,
			Peers
		),
	case lists:any(fun(Result) -> Result == found end, Results) of
		false ->
			?LOG_DEBUG([{event, exclude_interval_from_syncing}, {left, Left}, {right, Right}]),
			gen_server:cast(?MODULE, {exclude_interval_from_syncing, Left, Right});
		true ->
			ok
	end,
	find_subintervals(LeftBound + ?NETWORK_DATA_BUCKET_SIZE, Right).

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

validate_data_path(DataRoot, Offset, TXSize, DataPath, Chunk) ->
	ValidatePathResult =
		case ar_merkle:validate_path_strict_data_split(DataRoot, Offset, TXSize, DataPath) of
			false ->
				case ar_merkle:validate_path_strict_borders(DataRoot, Offset, TXSize, DataPath) of
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
	[{_, DataRootIndex}] = ets:lookup(ar_data_sync_state, data_root_index),
	[{_, DiskPoolSize}] = ets:lookup(ar_data_sync_state, disk_pool_size),
	[{_, DiskPoolChunksIndex}] = ets:lookup(ar_data_sync_state, disk_pool_chunks_index),
	[{_, ChunkDataDB}] = ets:lookup(ar_data_sync_state, chunk_data_db),
	DataRootKey = << DataRoot/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	DataRootOffsetReply = get_data_root_offset(DataRootKey),
	DataRootInDiskPool = ets:member(ar_disk_pool_data_roots, DataRootKey),
	ChunkSize = byte_size(Chunk),
	CheckDiskPool =
		case {DataRootOffsetReply, DataRootInDiskPool} of
			{not_found, false} ->
				{error, data_root_not_found};
			{not_found, true} ->
				{ok, Config} = application:get_env(arweave, config),
				DataRootLimit = Config#config.max_disk_pool_data_root_buffer_mb * 1024 * 1024,
				DiskPoolLimit = Config#config.max_disk_pool_buffer_mb * 1024 * 1024,
				[{_, {Size, Timestamp,
						TXIDSet}}] = ets:lookup(ar_disk_pool_data_roots, DataRootKey),
				case Size + ChunkSize > DataRootLimit
						orelse DiskPoolSize + ChunkSize > DiskPoolLimit of
					true ->
						{error, exceeds_disk_pool_size_limit};
					false ->
						{ok, {Size + ChunkSize, Timestamp, TXIDSet}}
				end;
			_ ->
				{ok, {0, os:system_time(microsecond), not_set}}
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
					case ar_kv:put(DiskPoolChunksIndex, DiskPoolChunkKey2, DiskPoolChunkValue) of
						{error, Reason3} ->
							?LOG_WARNING([{event, failed_to_record_chunk_in_disk_pool},
								{reason, io_lib:format("~p", [Reason3])}]),
							{error, failed_to_store_chunk};
						ok ->
							ets:insert(ar_disk_pool_data_roots,
									{DataRootKey, DiskPoolDataRootValue2}),
							ets:update_counter(ar_data_sync_state, disk_pool_size,
									{2, ChunkSize}),
							ok
					end
			end
	end.

chunk_offsets_synced(_, _, _, _, N) when N == 0 ->
	true;
chunk_offsets_synced(DataRootIndex, DataRootKey, ChunkOffset, TXStartOffset, N) ->
	case ar_sync_record:is_recorded(TXStartOffset + ChunkOffset, ?MODULE) of
		{true, _} ->
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

write_chunk(Offset, ChunkDataKey, ChunkSize, Chunk, DataPath, Packing, State) ->
	case ar_tx_blacklist:is_byte_blacklisted(Offset) of
		true ->
			ok;
		false ->
			write_not_blacklisted_chunk(Offset, ChunkDataKey, ChunkSize, Chunk, DataPath,
					Packing, State)
	end.

write_not_blacklisted_chunk(Offset, ChunkDataKey, ChunkSize, Chunk, DataPath, Packing, State) ->
	#sync_data_state{ chunk_data_db = ChunkDataDB,
			strict_data_split_threshold = StrictDataSplitThreshold } = State,
	ShouldStoreInChunkStorage = should_store_in_chunk_storage(Offset, ChunkSize, Packing, State),
	Result =
		case ShouldStoreInChunkStorage of
			true ->
				PaddedOffset = get_chunk_padded_offset(Offset, StrictDataSplitThreshold),
				ar_chunk_storage:put(PaddedOffset, Chunk);
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
should_store_in_chunk_storage(Offset, ChunkSize, Packing, State) ->
	#sync_data_state{ strict_data_split_threshold = StrictDataSplitThreshold } = State,
	case Offset > StrictDataSplitThreshold of
		true ->
			%% All chunks above StrictDataSplitThreshold are placed in 256 KiB buckets
			%% so technically can be stored in ar_chunk_storage. However, to avoid
			%% managing padding in ar_chunk_storage for unpacked chunks smaller than 256 KiB
			%% (we do not need fast random access to unpacked chunks after
			%% StrictDataSplitThreshold anyways), we put them to RocksDB.
			Packing /= unpacked orelse ChunkSize == (?DATA_CHUNK_SIZE);
		false ->
			ChunkSize == (?DATA_CHUNK_SIZE)
	end.

update_chunks_index(Args, State) ->
	{AbsoluteChunkOffset, ChunkOffset, ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkSize,
			Packing} = Args,
	case ar_tx_blacklist:is_byte_blacklisted(AbsoluteChunkOffset) of
		true ->
			ok;
		false ->
			update_chunks_index2({AbsoluteChunkOffset, ChunkOffset, ChunkDataKey, TXRoot,
					DataRoot, TXPath, ChunkSize, Packing}, State)
	end.

update_chunks_index2(Args, State) ->
	{AbsoluteOffset, Offset, ChunkDataKey, TXRoot, DataRoot, TXPath, ChunkSize,
			Packing} = Args,
	#sync_data_state{ chunks_index = ChunksIndex,
			strict_data_split_threshold = StrictDataSplitThreshold } = State,
	Key = << AbsoluteOffset:?OFFSET_KEY_BITSIZE >>,
	Value = {ChunkDataKey, TXRoot, DataRoot, TXPath, Offset, ChunkSize},
	case ar_kv:put(ChunksIndex, Key, term_to_binary(Value)) of
		ok ->
			StartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize,
					StrictDataSplitThreshold),
			PaddedOffset = get_chunk_padded_offset(AbsoluteOffset, StrictDataSplitThreshold),
			case ar_sync_record:add(PaddedOffset, StartOffset, Packing, ?MODULE) of
				ok ->
					ok;
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_update_sync_record}, {reason, Reason},
							{chunk, ar_util:encode(ChunkDataKey)}, {offset, AbsoluteOffset}]),
					{error, Reason}
			end;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_update_chunk_index}, {reason, Reason},
					{chunk, ar_util:encode(ChunkDataKey)}, {offset, AbsoluteOffset}]),
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
	#sync_data_state{ weave_size = WeaveSize } = State,
	?LOG_WARNING([{event, got_invalid_proof_from_peer}, {peer, ar_util:format_peer(Peer)},
			{byte, Byte}, {weave_size, WeaveSize}]),
	gen_server:cast(?MODULE, dec_sync_buffer_size),
	%% Not necessarily a malicious peer, it might happen
	%% if the chunk is recent and from a different fork.
	{noreply, State}.

process_valid_fetched_chunk(ChunkArgs, Args, State) ->
	{Packing, UnpackedChunk, AbsoluteEndOffset, TXRoot, ChunkSize} = ChunkArgs,
	{AbsoluteTXStartOffset, TXSize, DataPath, TXPath, DataRoot, Chunk, _ChunkID,
			ChunkEndOffset, Strict, Peer, Byte} = Args,
	gen_server:cast(?MODULE, dec_sync_buffer_size),
	case is_chunk_proof_ratio_attractive(ChunkSize, TXSize, DataPath) of
		false ->
			?LOG_WARNING([{event, got_too_big_proof_from_peer},
					{peer, ar_util:format_peer(Peer)}]),
			{noreply, State};
		true ->
			case ar_sync_record:is_recorded(Byte + 1, ?MODULE) of
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
	#sync_data_state{ disk_pool_threshold = DiskPoolThreshold,
			packing_map = PackingMap } = State,
	AbsoluteOffset = AbsoluteTXStartOffset + Offset,
	DataPathHash = crypto:hash(sha256, DataPath),
	case AbsoluteOffset > DiskPoolThreshold of
		true ->
			?LOG_DEBUG([{event, storing_fetched_chunk_in_disk_pool}, {offset, AbsoluteOffset}]),
			ChunkDataKey = get_chunk_data_key(DataPathHash),
			Write = write_disk_pool_chunk(ChunkDataKey, UnpackedChunk, DataPath, State),
			case Write of
				ok ->
					Args2 = {DataRoot, TXSize, DataPathHash, ChunkDataKey, Offset, ChunkSize,
							PassedStrictValidation},
					case add_chunk_to_disk_pool(Args2, State) of
						ok ->
							case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey,
										TXRoot, DataRoot, TXPath, ChunkSize, unpacked}, State) of
								ok ->
									{noreply, State};
								{error, Reason} ->
									log_failed_to_store_chunk(Reason, Offset, DataRoot),
									{noreply, State}
							end;
						{error, Reason} ->
							log_failed_to_store_chunk(Reason, Offset, DataRoot),
							{noreply, State}
					end;
				{error, Reason} ->
					log_failed_to_store_chunk(Reason, Offset, DataRoot),
					{noreply, State}
			end;
		false ->
			PackingStatus =
				case {get_required_chunk_packing(AbsoluteOffset, ChunkSize, State), Packing} of
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
							{packing, StoredPacking}]),
					ChunkArgs = {StoredPacking, StoredChunk, AbsoluteOffset, TXRoot, ChunkSize},
					store_chunk(ChunkArgs, {DataPath, Offset, DataRoot, TXPath}, State),
					{noreply, State};
				{need_packing, RequiredPacking} ->
					case maps:is_key(AbsoluteOffset, PackingMap) of
						true ->
							{noreply, State};
						false ->
							?LOG_DEBUG([{event, schedule_fetched_chunk_repacking},
									{offset, AbsoluteOffset}, {got_packing, Packing},
									{need_packing, RequiredPacking}]),
							ar_events:send(chunk, {repack_request, AbsoluteOffset,
									{RequiredPacking, unpacked, Chunk, AbsoluteOffset, TXRoot,
									ChunkSize}}),
							ar_util:cast_after(600000, ?MODULE,
									{expire_repack_chunk_request, AbsoluteOffset}),
							PackingArgs = {pack_fetched_chunk, {DataPath, Offset, DataRoot,
									TXPath}},
							{noreply, State#sync_data_state{
								packing_map = PackingMap#{ AbsoluteOffset => PackingArgs }}}
					end
			end
	end.

store_chunk(ChunkArgs, Args, State) ->
	#sync_data_state{ strict_data_split_threshold = StrictDataSplitThreshold } = State,
	{Packing, Chunk, AbsoluteOffset, TXRoot, ChunkSize} = ChunkArgs,
	{DataPath, Offset, DataRoot, TXPath} = Args,
	PaddedOffset = get_chunk_padded_offset(AbsoluteOffset, StrictDataSplitThreshold),
	StartOffset = get_chunk_padded_offset(AbsoluteOffset - ChunkSize, StrictDataSplitThreshold),
	case ar_sync_record:delete(PaddedOffset, StartOffset, ?MODULE) of
		{error, Reason} ->
			log_failed_to_store_chunk(Reason, AbsoluteOffset, DataRoot),
			{error, Reason};
		ok ->
			DataPathHash = crypto:hash(sha256, DataPath),
			ChunkDataKey = get_chunk_data_key(DataPathHash),
			case write_chunk(AbsoluteOffset, ChunkDataKey, ChunkSize, Chunk, DataPath, Packing,
					State) of
				ok ->
					case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey, TXRoot,
							DataRoot, TXPath, ChunkSize, Packing}, State) of
						ok ->
							ok;
						{error, Reason} ->
							log_failed_to_store_chunk(Reason, AbsoluteOffset, DataRoot),
							{error, Reason}
					end;
				{error, Reason} ->
					log_failed_to_store_chunk(Reason, AbsoluteOffset, DataRoot),
					{error, Reason}
			end
	end.

log_failed_to_store_chunk(Reason, Offset, DataRoot) ->
	?LOG_ERROR([{event, failed_to_store_chunk}, {reason, io_lib:format("~p", [Reason])},
			{absolute_chunk_offset, Offset}, {data_root, ar_util:encode(DataRoot)}]).

cache_recently_processed_offset(Offset, ChunkDataKey, State) ->
	#sync_data_state{ recently_processed_disk_pool_offsets = Map } = State,
	Set = maps:get(Offset, Map, sets:new()),
	Map2 =
		case sets:is_element(ChunkDataKey, Set) of
			false ->
				ar_util:cast_after(?CACHE_RECENTLY_PROCESSED_DISK_POOL_OFFSET_LIFETIME_MS,
						?MODULE, {remove_recently_processed_disk_pool_offset, Offset,
						ChunkDataKey}),
				maps:put(Offset, sets:add_element(ChunkDataKey, Set), Map);
			true ->
				Map
		end,
	State#sync_data_state{ recently_processed_disk_pool_offsets = Map2 }.

get_required_chunk_packing(_Offset, _ChunkSize, #sync_data_state{ packing_disabled = true }) ->
	unpacked;
get_required_chunk_packing(AbsoluteOffset, _ChunkSize,
		#sync_data_state{ disk_pool_threshold = Threshold }) when AbsoluteOffset > Threshold ->
	unpacked;
get_required_chunk_packing(AbsoluteOffset, _ChunkSize,
		#sync_data_state{ strict_data_split_threshold = Threshold })
				when AbsoluteOffset > Threshold ->
	spora_2_5;
get_required_chunk_packing(_AbsoluteOffset, ChunkSize, _State)
		when ChunkSize < ?DATA_CHUNK_SIZE ->
	unpacked;
get_required_chunk_packing(AbsoluteOffset, _ChunkSize, State) ->
	#sync_data_state{ packing_2_5_threshold = PackingThreshold } = State,
	case AbsoluteOffset > PackingThreshold of
		true ->
			spora_2_5;
		false ->
			unpacked
	end.

process_disk_pool_item(State, Key, Value) ->
	#sync_data_state{ disk_pool_chunks_index = DiskPoolChunksIndex,
			data_root_index = DataRootIndex } = State,
	prometheus_counter:inc(disk_pool_processed_chunks),
	<< Timestamp:256, DataPathHash/binary >> = Key,
	DiskPoolChunk = parse_disk_pool_chunk(Value),
	{Offset, ChunkSize, DataRoot, TXSize, ChunkDataKey,
			PassedStrictValidation} = DiskPoolChunk,
	DataRootKey = << DataRoot:32/binary, TXSize:?OFFSET_KEY_BITSIZE >>,
	InDataRootIndex = get_data_root_offset(DataRootKey),
	InDiskPool = ets:member(ar_disk_pool_data_roots, DataRootKey),
	case {InDataRootIndex, InDiskPool} of
		{not_found, true} ->
			%% Increment the timestamp by one (microsecond), so that the new cursor is
			%% a prefix of the first key of the next data root. We want to quickly skip
			%% all chunks belonging to the same data root because the data root is not
			%% yet on chain.
			NextCursor = {seek, << (Timestamp + 1):256 >>},
			gen_server:cast(?MODULE, process_disk_pool_item),
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
			gen_server:cast(?MODULE, process_disk_pool_item),
			State2 = may_be_reset_disk_pool_full_scan_key(Key, State),
			{noreply, State2#sync_data_state{ disk_pool_cursor = NextCursor }};
		{{ok, {TXStartOffset, TXPath}}, _} ->
			DataRootIndexIterator = data_root_index_iterator_v2(DataRootKey, TXStartOffset,
					TXPath, DataRootIndex),
			NextCursor = << Key/binary, <<"a">>/binary >>,
			State2 = State#sync_data_state{ disk_pool_cursor = NextCursor },
			Args = {Offset, InDiskPool, ChunkSize, DataRoot, DataPathHash, ChunkDataKey, Key,
					PassedStrictValidation},
			process_disk_pool_chunk_offsets(DataRootIndexIterator, true, Args, State2)
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

process_disk_pool_chunk_offsets(Iterator, MayConclude, Args, State) ->
	#sync_data_state{ disk_pool_chunks_index = DiskPoolChunksIndex } = State,
	{_, _, _, DataRoot, _, ChunkDataKey, Key, _} = Args,
	%% Place the chunk under its last 10 offsets in the weave (the same data
	%% may be uploaded several times).
	case data_root_index_next_v2(Iterator, 10) of
		none ->
			State2 =
				case MayConclude of
					true ->
						?LOG_DEBUG([{event, removing_disk_pool_chunk}, {key, ar_util:encode(Key)},
								{data_doot, ar_util:encode(DataRoot)}]),
						ok = ar_kv:delete(DiskPoolChunksIndex, Key),
						ok = delete_disk_pool_chunk(ChunkDataKey, State),
						may_be_reset_disk_pool_full_scan_key(Key, State);
					false ->
						State
				end,
			gen_server:cast(?MODULE, process_disk_pool_item),
			{noreply, deregister_currently_processed_disk_pool_key(Key, State2)};
		{TXArgs, Iterator2} ->
			gen_server:cast(?MODULE, {process_disk_pool_chunk_offset, MayConclude, TXArgs, Args,
					Iterator2}),
			{noreply, register_currently_processed_disk_pool_key(Key, State)}
	end.

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
	#sync_data_state{ disk_pool_threshold = DiskPoolThreshold,
			strict_data_split_threshold = StrictDataSplitThreshold } = State,
	{_, _, _, DataRoot, _, _, _, PassedStrictValidation} = Args,
	case AbsoluteOffset =< StrictDataSplitThreshold orelse PassedStrictValidation of
		false ->
			%% When we accept chunks into the disk pool, we do not know where they will
			%% end up on the weave. Therefore, we cannot require all Merkle proofs pass
			%% the strict validation rules taking effect only after StrictDataSplitThreshold.
			%% Instead we note down whether the chunk passes the strict validation and take it
			%% into account here where the chunk is associated with a global weave offset.
			?LOG_INFO([{event, disk_pool_chunk_from_bad_split}, {offset, AbsoluteOffset},
					{data_root, ar_util:encode(DataRoot)}]),
			process_disk_pool_chunk_offsets(Iterator, MayConclude, Args, State);
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
	case ar_sync_record:is_recorded(AbsoluteOffset, ?MODULE) of
		{true, unpacked} ->
			%% Pass MayConclude as false because we have encountered an offset
			%% above the disk pool threshold => we need to keep the chunk in the
			%% disk pool for now and not pack and move to the offset-based storage.
			%% The motivation is to keep chain reorganisations cheap.
			process_disk_pool_chunk_offsets(Iterator, false, Args, State);
		false ->
			?LOG_DEBUG([{event, record_disk_pool_chunk}, {offset, AbsoluteOffset},
					{chunk_data_key, ar_util:encode(element(5, Args))}]),
			{Offset, _, ChunkSize, DataRoot, _, ChunkDataKey, Key, _} = Args,
			case update_chunks_index({AbsoluteOffset, Offset, ChunkDataKey,
					TXRoot, DataRoot, TXPath, ChunkSize, unpacked}, State) of
				ok ->
					process_disk_pool_chunk_offsets(Iterator, false, Args, State);
				{error, Reason} ->
					?LOG_WARNING([{event, failed_to_update_chunks_index},
							{reason, io_lib:format("~p", [Reason])}]),
					gen_server:cast(?MODULE, process_disk_pool_item),
					{noreply, deregister_currently_processed_disk_pool_key(Key, State)}
			end
	end.

process_disk_pool_matured_chunk_offset(Iterator, TXRoot, TXPath, AbsoluteOffset, MayConclude,
		Args, State) ->
	#sync_data_state{ chunk_data_db = ChunkDataDB, packing_map = PackingMap } = State,
	{Offset, _, ChunkSize, DataRoot, _, ChunkDataKey, Key, _} = Args,
	case is_disk_pool_chunk(AbsoluteOffset, ChunkDataKey, State) of
		false ->
			State2 = cache_recently_processed_offset(AbsoluteOffset, ChunkDataKey, State),
			?LOG_DEBUG([{event, chunk_already_moved_from_disk_pool}, {offset, AbsoluteOffset},
					{chunk_data_key, ar_util:encode(element(5, Args))},
					{data_root, ar_util:encode(DataRoot)}]),
			process_disk_pool_chunk_offsets(Iterator, MayConclude, Args, State2);
		{error, Reason} ->
			?LOG_WARNING([{event, failed_to_read_chunks_index},
					{reason, io_lib:format("~p", [Reason])}]),
			gen_server:cast(?MODULE, process_disk_pool_item),
			{noreply, deregister_currently_processed_disk_pool_key(Key, State)};
		true ->
			case maps:is_key(AbsoluteOffset, PackingMap) of
				true ->
					?LOG_DEBUG([{event, disk_pool_chunk_already_being_packed},
							{offset, AbsoluteOffset},
							{chunk_data_key, ar_util:encode(element(5, Args))}]),
					process_disk_pool_chunk_offsets(Iterator, false, Args, State);
				false ->
					case map_size(PackingMap) >= ?PACKING_BUFFER_SIZE of
						true ->
							process_disk_pool_chunk_offsets(Iterator, false, Args, State);
						false ->
							case read_chunk(AbsoluteOffset, ChunkDataDB, ChunkDataKey) of
								not_found ->
									?LOG_WARNING([{event, disk_pool_chunk_not_found},
											{offset, AbsoluteOffset},
											{chunk_data_key, ar_util:encode(element(5, Args))}]),
									process_disk_pool_chunk_offsets(Iterator, MayConclude,
											Args, State);
								{error, Reason} ->
									?LOG_ERROR([{event, failed_to_read_disk_pool_chunk},
											{reason, io_lib:format("~p", [Reason])}]),
									gen_server:cast(?MODULE, process_disk_pool_item),
									{noreply, deregister_currently_processed_disk_pool_key(Key,
											State)};
								{ok, {Chunk, DataPath}} ->
									?LOG_DEBUG([{event, request_disk_pool_chunk_packing},
											{offset, AbsoluteOffset},
											{chunk_data_key, ar_util:encode(element(5, Args))}]),
									RequiredPacking = get_required_chunk_packing(AbsoluteOffset,
											ChunkSize, State),
									ar_events:send(chunk, {repack_request, AbsoluteOffset,
											{RequiredPacking, unpacked, Chunk, AbsoluteOffset,
													TXRoot, ChunkSize}}),
									ar_util:cast_after(600000, ?MODULE,
											{expire_repack_chunk_request, AbsoluteOffset}),
									PackingArgs = {pack_disk_pool_chunk, ChunkDataKey, {DataPath,
											Offset, DataRoot, TXPath}},
									process_disk_pool_chunk_offsets(Iterator, false, Args,
											State#sync_data_state{
												packing_map = PackingMap#{
														AbsoluteOffset => PackingArgs }})
							end
					end
			end
	end.

is_disk_pool_chunk(AbsoluteOffset, ChunkDataKey, State) ->
	#sync_data_state{ chunks_index = ChunksIndex,
			recently_processed_disk_pool_offsets = Map } = State,
	case ar_sync_record:is_recorded(AbsoluteOffset, ?MODULE) of
		{true, spora_2_5} ->
			%% We only pack "matured" chunks.
			false;
		_ ->
			Set = maps:get(AbsoluteOffset, Map, sets:new()),
			case sets:is_element(ChunkDataKey, Set) of
				true ->
					%% The chunk has "matured" but was not packed (e.g., packing is disabled).
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

store_repacked_chunk(ChunkArgs, State) ->
	#sync_data_state{ strict_data_split_threshold = StrictDataSplitThreshold } = State,
	{Packing, Chunk, AbsoluteOffset, _, _} = ChunkArgs,
	case ar_sync_record:is_recorded(AbsoluteOffset, unpacked, ?MODULE) of
		false ->
			%% The chunk should have been removed or packed in the meantime.
			ok;
		true ->
			PaddedOffset = get_chunk_padded_offset(AbsoluteOffset, StrictDataSplitThreshold),
			StartOffset = PaddedOffset - ?DATA_CHUNK_SIZE,
			case ar_sync_record:delete(PaddedOffset, StartOffset, ?MODULE) of
				{error, Reason} ->
					?LOG_ERROR([{event, failed_to_reset_sync_record_before_updating},
							{reason, io_lib:format("~p", [Reason])}]);
				ok ->
					case ar_chunk_storage:put(PaddedOffset, Chunk) of
						ok ->
							case ar_sync_record:add(PaddedOffset, StartOffset, Packing,
									?MODULE) of
								{error, Reason} ->
									?LOG_ERROR([{event, failed_to_record_repacked_chunk},
											{reason, io_lib:format("~p", [Reason])}]);
								ok ->
									ok
							end;
						{error, Reason} ->
							?LOG_ERROR([{event, failed_to_write_repacked_chunk},
									{reason, io_lib:format("~p", [Reason])}])
					end
			end
	end,
	{noreply, State}.

data_root_index_iterator_v2(DataRootKey, TXStartOffset, TXPath, DataRootIndex) ->
	{DataRootKey, TXStartOffset, TXPath, DataRootIndex, 1}.

data_root_index_next_v2({_, _, _, _, Count}, Limit)
		when Count > Limit ->
	none;
data_root_index_next_v2({DataRootKey, TXStartOffset, TXPath, DataRootIndex, Count}, _Limit) ->
	{ok, TXRoot} = ar_merkle:extract_root(TXPath),
	{{TXStartOffset, TXRoot, TXPath},
			{DataRootKey, TXStartOffset, DataRootIndex, Count + 1}};
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
