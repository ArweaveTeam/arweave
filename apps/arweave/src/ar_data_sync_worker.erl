%%% @doc Network sync worker for ar_data_sync.
%%%
%%% Each worker repeatedly shuffles the active peer-worker roster and pulls
%%% one task via ar_peer_worker:take_one/1. It performs the chunk HTTP
%%% request, then reports completion back through ar_data_sync so the owning
%%% storage module can store the chunk and release queue dedup state.
%%%
%%% Local-disk chunk-copy work runs in ar_chunk_copy_worker, not here.
%%% Storage writes are deliberately left to ar_data_sync processes.
-module(ar_data_sync_worker).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

-record(state, {
	name = undefined,
	request_packed_chunks = false,
	sync_in_progress = false
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(Name) ->
	gen_server:start_link({local, Name}, ?MODULE, Name, []).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(Name) ->
	?LOG_INFO([{event, init}, {module, ?MODULE}, {name, Name}]),
	{ok, Config} = arweave_config:get_env(),
	gen_server:cast(self(), pull),
	{ok, #state{
		name = Name,
		request_packed_chunks = Config#config.data_sync_request_packed_chunks
	}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.


handle_cast(pull, #state{ sync_in_progress = true } = State) ->
	{noreply, State};
handle_cast(pull, State) ->
	%% Shuffle list to distribute peer load across workers.
	Peers = ar_util:shuffle_list(ar_peer_worker:get_all_peers()),
	case try_take_one(Peers) of
		{ok, SyncTask} ->
			case run_sync_range(SyncTask, State) of
				done ->
					%% Loop: immediately try to pull again.
					gen_server:cast(self(), pull),
					{noreply, State#state{ sync_in_progress = false }};
				recast ->
					{noreply, State#state{ sync_in_progress = true }}
			end;
		none ->
			%% No peer has work right now. Retry after a short delay.
			ar_util:cast_after(500 + rand:uniform(1000), self(), pull),
			{noreply, State}
	end;

handle_cast({sync_range, SyncTask}, State) ->
	{ElapsedUs, SyncResult} = timer:tc(fun() -> sync_range(SyncTask, State) end),
	case SyncResult of
		recast ->
			{noreply, State#state{ sync_in_progress = true }};
		_ ->
			ar_peer_sync:task_completed(SyncTask, self(), SyncResult, ElapsedUs),
			gen_server:cast(self(), pull),
			{noreply, State#state{ sync_in_progress = false }}
	end;

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

try_take_one([]) ->
	none;
try_take_one([{_Peer, PeerPid} | Rest]) ->
	case ar_peer_worker:take_one(PeerPid) of
		{task, SyncTask} ->
			{ok, SyncTask};
		none ->
			try_take_one(Rest)
	end.

%% @doc Execute one sync_range task and report completion through
%% ar_peer_sync (which fans out to the per-peer accounting and releases
%% the StoreID's queue dedup overlay). On recast (cache full / disk full
%% / retryable HTTP error) sync_range internally schedules a
%% self-cast_after; we leave the slot claimed and in_flight_count
%% incremented at the peer worker. The scheduled retry lands in the
%% `{sync_range, _}` cast handler, which will report completion when
%% the retry succeeds (or definitively fails after exhausting retries —
%% see sync_range/2 retry-zero clause which returns {error, timeout}
%% directly, not recast).
run_sync_range(SyncTask, State) ->
	{ElapsedUs, SyncResult} = timer:tc(fun() -> sync_range(SyncTask, State) end),
	case SyncResult of
		recast ->
			%% Slot stays claimed; eventual retry will report completion.
			recast;
		_ ->
			ar_peer_sync:task_completed(SyncTask, self(), SyncResult, ElapsedUs),
			done
	end.

handle_info(_Message, State) ->
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{event, terminate}, {module, ?MODULE}, {reason, io_lib:format("~p", [Reason])}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

sync_range(#sync_task{ start_offset = Start, end_offset = End }, _State) when Start >= End ->
	ok;
sync_range(#sync_task{ start_offset = Start, end_offset = End, peer = Peer,
		retry_count = 0 }, _State) ->
	?LOG_INFO([{event, sync_range_retries_exhausted},
				{peer, ar_util:format_peer(Peer)},
				{start_offset, Start}, {end_offset, End}]),
	{error, timeout};
sync_range(#sync_task{ start_offset = Start, end_offset = End, peer = Peer,
		store_id = TargetStoreID, retry_count = RetryCount } = SyncTask, State) ->
	IsChunkCacheFull =
		case ar_data_sync:is_chunk_cache_full() of
			true ->
				ar_util:cast_after(500, self(), {sync_range, SyncTask}),
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
						ar_util:cast_after(30000, self(), {sync_range, SyncTask}),
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
			Byte = Start2 - 1,
			IsRecorded = ar_sync_record:is_recorded(Byte + 1, ar_data_sync, TargetStoreID),
			case {Byte >= End, IsRecorded} of
				{true, _} ->
					ok;
				{_, {true, _}} ->
					ok;
				_ ->
					Packing = get_target_packing(TargetStoreID, State#state.request_packed_chunks),
					case ar_http_iface_client:get_chunk_binary(Peer, Start2, Packing) of
						{ok, #{ chunk := Chunk } = Proof, _Time, _TransferSize} ->
							%% In case we fetched a packed small chunk,
							%% we may potentially skip some chunks by
							%% continuing with Start2 + byte_size(Chunk) - the skip
							%% chunks will be then requested later.
							Start3 = ar_block:get_chunk_padded_offset(
									Start2 + byte_size(Chunk)) + 1,
							ar_data_sync:store_fetched_chunk(
									TargetStoreID, Peer, Byte, Proof),
							ar_data_sync:increment_chunk_cache_size(),
							sync_range(
								SyncTask#sync_task{ start_offset = Start3 },
								State);
						{error, timeout} ->
							?LOG_DEBUG([{event, timeout_fetching_chunk},
									{peer, ar_util:format_peer(Peer)},
									{start_offset, Start2}, {end_offset, End}]),
							SyncTask2 = SyncTask#sync_task{
								retry_count = RetryCount - 1 },
							ar_util:cast_after(1000, self(), {sync_range, SyncTask2}),
							recast;
						{error, {ok, {{<<"404">>, _}, _, _, _, _}} = Reason} ->
							{error, Reason};
						{error, Reason} ->
							ar_http_iface_client:log_failed_request({error, Reason}, [
								{event, failed_to_fetch_chunk},
								{peer, ar_util:format_peer(Peer)},
								{start_offset, Start2}, {end_offset, End},
								{reason, io_lib:format("~p", [Reason])}]),
							{error, Reason}
					end
			end
	end.

get_target_packing(StoreID, true) ->
	ar_storage_module:get_packing(StoreID);
get_target_packing(_StoreID, false) ->
	any.
