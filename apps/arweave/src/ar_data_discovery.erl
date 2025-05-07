-module(ar_data_discovery).

-behaviour(gen_server).

-export([start_link/0, get_bucket_peers/1, collect_peers/0, pick_peers/2,
		 get_entropy_footprint_peers/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include("ar.hrl").
-include("ar_data_discovery.hrl").
-include("ar_sync_buckets.hrl").

-record(state, {
	peer_queue,
	peers_pending,
	network_map,
	expiration_map
}).

-define(MODULE_ENTROPY_FOOTPRINTS, ar_data_discovery_entropy_footprints).

%% The frequency of asking peers about their data.
-ifdef(AR_TEST).
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 2 * 1000).
-else.
-define(DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, 4 * 60 * 1000).
-endif.

%% The expiration time of peer's buckets. If a peer is found in the list of
%% the first best ?DATA_DISCOVERY_COLLECT_PEERS_COUNT peers (checked every
%% ?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS milliseconds), the timer is refreshed.
-define(PEER_EXPIRATION_TIME_MS, 60 * 60 * 1000).

%% The maximum number of requests running at any time.
-define(DATA_DISCOVERY_PARALLEL_PEER_REQUESTS, 10).

%% The number of peers from the top of the rating to schedule for inclusion
%% into the peer map every DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS milliseconds.
-define(DATA_DISCOVERY_COLLECT_PEERS_COUNT, 1000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return the list of ?QUERY_BEST_PEERS_COUNT peers who have at least one byte of
%% data synced in the given Bucket of size ?NETWORK_DATA_BUCKET_SIZE. 80% of the peers
%% are chosen from the 20% of peers with the biggest share in the given bucket.
get_bucket_peers(Bucket) ->
	case ets:member(ar_peers, block_connections) of
		true ->
			[];
		false ->
			get_bucket_peers(Bucket, {Bucket, 0, no_peer}, [])
	end.

get_bucket_peers(Bucket, Cursor, Peers) ->
	case ets:next(?MODULE, Cursor) of
		'$end_of_table' ->
			UniquePeers = sets:to_list(sets:from_list(Peers)),
			PickedPeers = pick_peers(UniquePeers, ?QUERY_BEST_PEERS_COUNT),
			PickedPeers;
		{Bucket, _Share, Peer} = Key ->
			get_bucket_peers(Bucket, Key, [Peer | Peers]);
		_ ->
			UniquePeers = sets:to_list(sets:from_list(Peers)),
			PickedPeers = pick_peers(UniquePeers, ?QUERY_BEST_PEERS_COUNT),
			PickedPeers
	end.

%% @doc Return a list of peers where 80% of the peers are randomly chosen
%% from the first 20% of Peers and the other 20% of the peers are randomly
%% chosen from the other 80% of Peers.
pick_peers(Peers, N) ->
	pick_peers(Peers, length(Peers), N).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, _} = timer:apply_interval(
		?DATA_DISCOVERY_COLLECT_PEERS_FREQUENCY_MS, ?MODULE, collect_peers, []),
	gen_server:cast(?MODULE, update_network_data_map),
	ok = ar_events:subscribe(peer),
	ets:new(?MODULE_ENTROPY_FOOTPRINTS, [set, public, named_table, {keypos, 1}, {read_concurrency, true}]),
	{ok, #state{
		peer_queue = queue:new(),
		peers_pending = 0,
		network_map = #{},
		expiration_map = #{}
	}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({add_peer, Peer}, #state{ peer_queue = Queue } = State) ->
	{noreply, State#state{ peer_queue = queue:in(Peer, Queue) }};

handle_cast(update_network_data_map, #state{ peers_pending = N } = State)
		when N < ?DATA_DISCOVERY_PARALLEL_PEER_REQUESTS ->
	case queue:out(State#state.peer_queue) of
		{empty, _} ->
			ar_util:cast_after(200, ?MODULE, update_network_data_map),
			{noreply, State};
		{{value, Peer}, Queue} ->
			monitor(process, spawn_link(
				fun() ->
					case ar_http_iface_client:get_sync_buckets(Peer) of
						{ok, SyncBuckets} ->
							gen_server:cast(?MODULE, {add_peer_sync_buckets, Peer,
									SyncBuckets});
						{error, request_type_not_found} ->
							get_sync_buckets(Peer);
						Error ->
							?LOG_DEBUG([{event, failed_to_fetch_sync_buckets},
								{peer, ar_util:format_peer(Peer)},
								{reason, io_lib:format("~p", [Error])}])
					end
				end
			)),
			gen_server:cast(?MODULE, update_network_data_map),
			{noreply, State#state{ peers_pending = N + 1, peer_queue = Queue }}
	end;
handle_cast(update_network_data_map, State) ->
	ar_util:cast_after(200, ?MODULE, update_network_data_map),
	{noreply, State};

handle_cast({add_peer_sync_buckets, Peer, SyncBuckets}, State) ->
	#state{ network_map = Map } = State,
	State2 = refresh_expiration_timer(Peer, State),
	Map2 = maps:put(Peer, SyncBuckets, Map),
	ar_sync_buckets:foreach(
		fun(Bucket, Share) ->
			ets:insert(?MODULE, {{Bucket, Share, Peer}})
		end,
		?NETWORK_DATA_BUCKET_SIZE,
		SyncBuckets
	),

	ets:match_delete(?MODULE_ENTROPY_FOOTPRINTS, {{ '_', Peer}, '_'}) ,

	WeaveEndForFootprintCalc = ar_node:get_current_weave_size(),

	PopulateFootprintsFun = fun(BucketGlobalOffset, NumBytesInBucket) ->
		ChunkStartOffset = BucketGlobalOffset,
		MaxChunkStartOffsetInSegment = BucketGlobalOffset + NumBytesInBucket - ?DATA_CHUNK_SIZE,

		IterFun = fun(CurrentCSO, InnerFun) ->
			if CurrentCSO =< MaxChunkStartOffsetInSegment ->
				ChunkPaddedEndOffset = ar_block:get_chunk_padded_offset(CurrentCSO + ?DATA_CHUNK_SIZE),
				
				AllOffsetsInFootprint = ar_entropy_gen:entropy_offsets(ChunkPaddedEndOffset, WeaveEndForFootprintCalc),
				if AllOffsetsInFootprint =/= [] ->
					FootprintDefiningOffset = hd(AllOffsetsInFootprint),
					ets:update_counter(?MODULE_ENTROPY_FOOTPRINTS, {FootprintDefiningOffset, Peer}, {2, 1, 0, 0});
				true -> ok
				end,
				InnerFun(CurrentCSO + ?DATA_CHUNK_SIZE, InnerFun);
			true -> ok
			end
		end,
		IterFun(ChunkStartOffset, IterFun)
	end,

	ar_sync_buckets:foreach(
		PopulateFootprintsFun,
		?NETWORK_DATA_BUCKET_SIZE,
		SyncBuckets
	),

	{noreply, State2#state{ network_map = Map2 }};

handle_cast({remove_peer, Peer}, State) ->
	#state{ network_map = Map, expiration_map = E } = State,
	Map2 =
		case maps:take(Peer, Map) of
			error ->
				Map;
			{SyncBucketsValue, Map3} ->
				ar_sync_buckets:foreach(
					fun(Bucket, Share) ->
						ets:delete(?MODULE, {Bucket, Share, Peer})
					end,
					?NETWORK_DATA_BUCKET_SIZE,
					SyncBucketsValue
				),
				Map3
		end,
	E2 = maps:remove(Peer, E),
	ets:match_delete(?MODULE_ENTROPY_FOOTPRINTS, {{ '_', Peer}, '_'}) ,
	{noreply, State#state{ network_map = Map2, expiration_map = E2 }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({'DOWN', _,  process, _, _}, #state{ peers_pending = N } = State) ->
	{noreply, State#state{ peers_pending = N - 1 }};

handle_info({event, peer, {removed, Peer}}, State) ->
	gen_server:cast(?MODULE, {remove_peer, Peer}),
	{noreply, State};

handle_info({event, peer, _}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

pick_peers(Peers, PeerLen, N) when N >= PeerLen ->
	Peers;
pick_peers([], _PeerLen, _N) ->
	[];
pick_peers(_Peers, _PeerLen, N) when N =< 0 ->
	[];
pick_peers(Peers, PeerLen, N) ->
	%% N: the target number of peers to pick
	%% Best: top 20% of the Peers list
	%% Other: the rest of the Peers list
	{Best, Other} = lists:split(max(PeerLen div 5, 1), Peers),
	%% TakeBest: Select 80% of N worth of Best - or all of Best if Best is short.
	TakeBest = max((8 * N) div 10, 1),
	Part1 = ar_util:pick_random(Best, min(length(Best), TakeBest)),
	%% TakeOther: rather than strictly take 20% of N, take enough to ensure we're
	%% getting the full N of picked peers.
	TakeOther = N - length(Part1),
	Part2 = ar_util:pick_random(Other, min(length(Other), TakeOther)),
	Part1 ++ Part2.

collect_peers() ->
	N = ?DATA_DISCOVERY_COLLECT_PEERS_COUNT,
	%% rank peers by current rating since we care about their recent throughput performance
	collect_peers(lists:sublist(ar_peers:get_peers(current), N)).

collect_peers([Peer | Peers]) ->
	gen_server:cast(?MODULE, {add_peer, Peer}),
	collect_peers(Peers);
collect_peers([]) ->
	ok.

get_sync_buckets(Peer) ->
	case ar_http_iface_client:get_sync_record(Peer) of
		{ok, SyncRecord} ->
			SyncBuckets = ar_sync_buckets:from_intervals(SyncRecord),
			{SyncBuckets2, _} = ar_sync_buckets:serialize(SyncBuckets, ?MAX_SYNC_BUCKETS_SIZE),
			gen_server:cast(?MODULE, {add_peer_sync_buckets, Peer, SyncBuckets2});
		Error ->
			?LOG_DEBUG([{event, failed_to_fetch_sync_record_from_peer},
					{peer, ar_util:format_peer(Peer)}, {reason, io_lib:format("~p", [Error])}])
	end.

refresh_expiration_timer(Peer, State) ->
	#state{ expiration_map = Map } = State,
	case maps:get(Peer, Map, not_found) of
		not_found ->
			ok;
		Timer ->
			timer:cancel(Timer)
	end,
	Timer2 = ar_util:cast_after(?PEER_EXPIRATION_TIME_MS, ?MODULE, {remove_peer, Peer}),
	State#state{ expiration_map = maps:put(Peer, Timer2, Map) }.

%% @doc Get peers who have chunks in a specific entropy footprint.
%% FootprintDefiningOffset is the canonical starting offset of an entropy footprint.
%% N is the desired number of peers.
-spec get_entropy_footprint_peers(FootprintDefiningOffset :: non_neg_integer(), N :: pos_integer()) -> [ar_peer:peer()].
get_entropy_footprint_peers(FootprintDefiningOffset, N) ->
	Matches = ets:match_object(?MODULE_ENTROPY_FOOTPRINTS, {{{FootprintDefiningOffset, '$1'}, '$2'}, '$3'}),
	
	PeerCounts = lists:filtermap(
		fun({{{FOD, PeerInMatch}, CountInMatch}, _}) when FOD == FootprintDefiningOffset -> 
			{true, {PeerInMatch, CountInMatch}};
		   (_) -> false
		end, Matches),
	
	SortedPeerCounts = lists:sort(
		fun({_P1, C1}, {_P2, C2}) -> C1 >= C2 end,
		PeerCounts
	),
	BestPeers = [P || {P, _Count} <- SortedPeerCounts],
	pick_peers(BestPeers, N).
