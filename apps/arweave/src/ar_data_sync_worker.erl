%%% @doc A process fetching the weave data from the network and from the local
%%% storage modules, one chunk (or a range of chunks) at a time. The workers
%%% are coordinated by ar_data_sync_worker_master. The workers do not update the
%%% storage - updates are handled by ar_data_sync_* processes.
-module(ar_data_sync_worker).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, [], []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(data_sync),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({read_range, Args}, State) ->
	case read_range(Args) of
		recast ->
			ok;
		_ ->
			gen_server:cast(ar_data_sync_worker_master, task_completed)
	end,
	{noreply, State};

handle_cast({sync_range, Args}, State) ->
	case sync_range(Args) of
		recast ->
			ok;
		_ ->
			gen_server:cast(ar_data_sync_worker_master, task_completed)
	end,
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, data_sync, {timeout, {sync_range, Peer}}}, State) ->
	?LOG_ERROR("*** worker ~p handling sync_range timeout for peer ~p", [self(), ar_util:format_peer(Peer)]),
	purge_messages({sync_range, Peer}),
	{noreply, State};

handle_info({event, data_sync, _}, State) ->
	{noreply, State};

handle_info(_Message, State) ->
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {reason, io_lib:format("~p", [Reason])}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

read_range({Start, End, _OriginStoreID, _TargetStoreID, _SkipSmall}) when Start >= End ->
	ok;
read_range({_Start, _End, _OriginStoreID, TargetStoreID, _SkipSmall} = Args) ->
	case ar_data_sync:is_chunk_cache_full() of
		false ->
			case ar_data_sync:is_disk_space_sufficient(TargetStoreID) of
				true ->
					read_range2(Args);
				_ ->
					ar_util:cast_after(30000, self(), {read_range, Args}),
					recast
			end;
		_ ->
			ar_util:cast_after(200, self(), {read_range, Args}),
			recast
	end.

read_range2({Start, End, _OriginStoreID, _TargetStoreID, _SkipSmall}) when Start >= End ->
	ok;
read_range2({Start, End, OriginStoreID, TargetStoreID, SkipSmall}) ->
	ChunksIndex = {chunks_index, OriginStoreID},
	ChunkDataDB = {chunk_data_db, OriginStoreID},
	case ar_data_sync:get_chunk_by_byte(ChunksIndex, Start + 1) of
		{error, invalid_iterator} ->
			%% get_chunk_by_byte looks for a key with the same prefix or the next
			%% prefix. Therefore, if there is no such key, it does not make sense to
			%% look for any key smaller than the prefix + 2 in the next iteration.
			PrefixSpaceSize = trunc(math:pow(2,
					?OFFSET_KEY_BITSIZE - ?OFFSET_KEY_PREFIX_BITSIZE)),
			Start2 = ((Start div PrefixSpaceSize) + 2) * PrefixSpaceSize,
			read_range2({Start2, End, OriginStoreID, TargetStoreID, SkipSmall});
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_query_chunk_metadata}, {offset, Start + 1},
					{reason, io_lib:format("~p", [Reason])}]);
		{ok, _Key, {AbsoluteOffset, _, _, _, _, _, _}} when AbsoluteOffset > End ->
			ok;
		{ok, _Key, {AbsoluteOffset, ChunkDataKey, TXRoot, DataRoot, TXPath,
					RelativeOffset, ChunkSize}} ->
			Skip = SkipSmall andalso AbsoluteOffset =< ?STRICT_DATA_SPLIT_THRESHOLD
					andalso ChunkSize < ?DATA_CHUNK_SIZE,
			ReadChunk =
				case Skip of
					true ->
						skip;
					false ->
						ar_data_sync:read_chunk(AbsoluteOffset, ChunkDataDB, ChunkDataKey,
								OriginStoreID)
				end,
			case ReadChunk of
				skip ->
					read_range2({Start + ChunkSize, End, OriginStoreID, TargetStoreID,
							SkipSmall});
				not_found ->
					gen_server:cast(list_to_atom("ar_data_sync_" ++ OriginStoreID),
							{invalidate_bad_data_record, {Start, AbsoluteOffset, ChunksIndex,
							OriginStoreID, 1}}),
					read_range2({Start + ChunkSize, End, OriginStoreID, TargetStoreID,
							SkipSmall});
				{error, Error} ->
					?LOG_ERROR([{event, failed_to_read_chunk},
							{absolute_end_offset, AbsoluteOffset},
							{chunk_data_key, ar_util:encode(ChunkDataKey)},
							{reason, io_lib:format("~p", [Error])}]),
					read_range2({Start + ChunkSize, End, OriginStoreID, TargetStoreID,
							SkipSmall});
				{ok, {Chunk, DataPath}} ->
					case ar_sync_record:is_recorded(AbsoluteOffset, ar_data_sync,
							OriginStoreID) of
						{true, Packing} ->
							ar_data_sync:increment_chunk_cache_size(),
							?LOG_DEBUG([{event, copying_chunk},
									{origin_store_id, OriginStoreID},
									{target_store_id, TargetStoreID},
									{absolute_end_offset, AbsoluteOffset}]),
							UnpackedChunk =
								case Packing of
									unpacked ->
										Chunk;
									_ ->
										none
								end,
							Args = {DataRoot, AbsoluteOffset, TXPath, TXRoot, DataPath,
									Packing, RelativeOffset, ChunkSize, Chunk,
									UnpackedChunk, TargetStoreID, ChunkDataKey},
							gen_server:cast(list_to_atom("ar_data_sync_"
									++ TargetStoreID), {pack_and_store_chunk, Args}),
							read_range2({Start + ChunkSize, End, OriginStoreID,
									TargetStoreID, SkipSmall});
						Reply ->
							?LOG_ERROR([{event, chunk_record_not_found},
									{absolute_end_offset, AbsoluteOffset},
									{ar_sync_record_reply, io_lib:format("~p", [Reply])}]),
							read_range2({Start + ChunkSize, End, OriginStoreID,
									TargetStoreID, SkipSmall})
					end
			end
	end.

sync_range({Start, End, _Peer, _TargetStoreID, _RetryCount}) when Start >= End ->
	ok;
sync_range({_Start, _End, Peer, _TargetStoreID, 0}) ->
	?LOG_ERROR("*** sync_range peer timeout: ~p", [ar_util:format_peer(Peer)]),
	ar_events:send(data_sync, {timeout, {sync_range, Peer}}),
	purge_messages({sync_range, Peer}),
	ok;
sync_range({Start, End, Peer, TargetStoreID, RetryCount} = Args) ->
	IsChunkCacheFull =
		case ar_data_sync:is_chunk_cache_full() of
			true ->
				ar_util:cast_after(500, self(), {sync_range, Args}),
				true;
			false ->
				false
		end,
	IsDiskSpaceSufficient =
		case IsChunkCacheFull of
			false ->
				case ar_data_sync:is_disk_space_sufficient(TargetStoreID) of
					true ->
						true;
					_ ->
						ar_util:cast_after(30000, self(), {sync_range, Args}),
						false
				end;
			true ->
				false
		end,
	case IsDiskSpaceSufficient of
		false ->
			recast;
		true ->
			Start2 = ar_tx_blacklist:get_next_not_blacklisted_byte(Start + 1),
			case Start2 - 1 >= End of
				true ->
					ok;
				false ->
					case ar_http_iface_client:get_chunk_binary(Peer, Start2, any) of
						{ok, #{ chunk := Chunk } = Proof, Time, TransferSize} ->
							%% In case we fetched a packed small chunk,
							%% we may potentially skip some chunks by
							%% continuing with Start2 + byte_size(Chunk) - the skip
							%% chunks will be then requested later.
							Start3 = ar_data_sync:get_chunk_padded_offset(
									Start2 + byte_size(Chunk)) + 1,
							gen_server:cast(list_to_atom("ar_data_sync_" ++ TargetStoreID),
									{store_fetched_chunk, Peer, Time, TransferSize, Start2 - 1,
									Proof}),
							ar_data_sync:increment_chunk_cache_size(),
							sync_range({Start3, End, Peer, TargetStoreID, RetryCount});
						{error, timeout} ->
							?LOG_ERROR([{event, timeout_fetching_chunk},
									{peer, ar_util:format_peer(Peer)},
									{start_offset, Start2}, {end_offset, End}]),
							% Args2 = {Start, End, Peer, TargetStoreID, RetryCount - 1},
							% ar_util:cast_after(1000, self(), {sync_range, Args2}),
							% recast;
							ar_events:send(data_sync, {timeout, {sync_range, Peer}}),
							purge_messages({sync_range, Peer}),
							ok;
						{error, Reason} ->
							?LOG_ERROR([{event, failed_to_fetch_chunk},
									{peer, ar_util:format_peer(Peer)},
									{start_offset, Start2}, {end_offset, End},
									{reason, io_lib:format("~p", [Reason])}])
					end
			end
	end.

purge_messages({sync_range, Peer}) ->
	receive
		{cast, {sync_range, {_Start, _End, Peer, _TargetStoreID, _RetryCount}}} ->
			?LOG_ERROR("*** worker ~p purged sync_range message for peer ~p",
					[self(), ar_util:format_peer(Peer)]),
			purge_messages({sync_range, Peer})
	after 0 ->
		ok
	end.
