-module(ar_coordination).

-behaviour(gen_server).

-export([
	start_link/0, computed_h1/2, compute_h2_for_peer/2, computed_h2_for_peer/1,
	get_public_state/0, send_h1_batch_to_peer/0, stat_loop/0, get_peers/1, get_peer/1,
	update_peer/2, remove_peer/1, garbage_collect/0, is_exit_peer/0,
	get_unique_partitions_list/0, get_self_plus_external_partitions_list/0,
	get_cluster_partitions_list/0
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").

-record(state, {
	last_peer_response = #{},
	peers_by_partition = #{},
	out_batches = #{},
	out_batch_timeout = ?DEFAULT_CM_BATCH_TIMEOUT_MS
}).

-define(START_DELAY, 1000).

-ifdef(DEBUG).
-define(BATCH_SIZE_LIMIT, 2).
-else.
-define(BATCH_SIZE_LIMIT, 400).
-endif.

-define(BATCH_POLL_INTERVAL_MS, 20).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Helper function to see state while testing and later for monitoring API
get_public_state() ->
	gen_server:call(?MODULE, get_public_state).

%% @doc An H1 has been generated. Store it to send it later to a
%% coordinated mining peer
computed_h1(Candidate, DiffPair) ->
	#mining_candidate{
		h1 = H1,
		nonce = Nonce
	} = Candidate,
	%% Prepare Candidate to be shared with a remote miner.
	%% 1. Add the current difficulty (the remote peer will use this instead of
	%%    its local difficulty).
	%% 2. Remove any data that's not needed by the peer. This cuts down on the volume of data
	%%    shared.
	%% 3. The peer field will be set to this peer's address by the remote miner.
	ShareableCandidate = Candidate#mining_candidate{
		chunk1 = not_set,
		chunk2 = not_set,
		cm_diff = DiffPair,
		cm_lead_peer = not_set,
		h1 = not_set,
		h2 = not_set,
		nonce = not_set,
		poa2 = not_set,
		preimage = not_set
	},
	gen_server:cast(?MODULE, {computed_h1, ShareableCandidate, H1, Nonce}).

send_h1_batch_to_peer() ->
	gen_server:cast(?MODULE, send_h1_batch_to_peer).

%% @doc Compute h2 for a remote peer
compute_h2_for_peer(Peer, Candidate) ->
	gen_server:cast(?MODULE, {compute_h2_for_peer, 
		Candidate#mining_candidate{ cm_lead_peer = Peer }}).

computed_h2_for_peer(Candidate) ->
	gen_server:cast(?MODULE, {computed_h2_for_peer, Candidate}).

stat_loop() ->
	gen_server:call(?MODULE, stat_loop).

get_peer(PartitionNumber) ->
	gen_server:call(?MODULE, {get_peer, PartitionNumber}).

get_peers(PartitionNumber) ->
	gen_server:call(?MODULE, {get_peers, PartitionNumber}).

update_peer(Peer, PartitionList) ->
	gen_server:cast(?MODULE, {update_peer, {Peer, PartitionList}}).

remove_peer(Peer) ->
	gen_server:cast(?MODULE, {remove_peer, Peer}).

garbage_collect() ->
	gen_server:cast(?MODULE, garbage_collect).

%% Return true if we are an exit peer in the coordinated mining setup.
is_exit_peer() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.coordinated_mining == true andalso
			Config#config.cm_exit_peer == not_set.

%% @doc Return a list of unique partitions including local partitions and all of
%% external (relevant pool peers') partitions.
%%
%% A single partition in the following format:
%% {[
%%   {bucket, PartitionID},
%%   {bucketsize, ?PARTITION_SIZE},
%%   {addr, EncodedMiningAddress}
%% ]}
%%
%% A single partition with the composite packing is in the following format:
%% {[
%%   {bucket, PartitionID},
%%   {bucketsize, ?PARTITION_SIZE},
%%   {addr, EncodedMiningAddress},
%%   {pdiff, PackingDifficulty}
%% ]}
get_self_plus_external_partitions_list() ->
	PoolPeer = ar_pool:pool_peer(),
	PoolPartitions = get_peer_partitions(PoolPeer),
	LocalPartitions = get_unique_partitions_set(),
	lists:sort(sets:to_list(get_unique_partitions_set(PoolPartitions, LocalPartitions))).

%% @doc Return a list of unique partitions including local partitions and all of
%% CM peers' partitions.
%%
%% A single partition in the following format:
%% {[
%%   {bucket, PartitionID},
%%   {bucketsize, ?PARTITION_SIZE},
%%   {addr, EncodedMiningAddress}
%% ]}
%%
%% A single partition with the composite packing is in the following format:
%% {[
%%   {bucket, PartitionID},
%%   {bucketsize, ?PARTITION_SIZE},
%%   {addr, EncodedMiningAddress},
%%   {pdiff, PackingDifficulty}
%% ]}
get_cluster_partitions_list() ->
	gen_server:call(?MODULE, get_cluster_partitions_list, infinity).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, Config} = application:get_env(arweave, config),
	
	ar_util:cast_after(?BATCH_POLL_INTERVAL_MS, ?MODULE, check_batches),
	State = #state{
		last_peer_response = #{}
	},
	State2 = case Config#config.coordinated_mining of
		false ->
			State;
		true ->
			case Config#config.cm_exit_peer of
				not_set ->
					ar:console(
						"This node is configured as a Coordinated Mining Exit Node. If this is "
						"not correct, set 'cm_exit_peer' and relaunch.~n");
				_ ->
					ok
			end,
			ar_util:cast_after(?START_DELAY, ?MODULE, refetch_peer_partitions),
			State#state{
				last_peer_response = #{}
			}
	end,
	{ok, State2#state{
		out_batch_timeout = Config#config.cm_out_batch_timeout }}.

%% Helper function to see state while testing and later for monitoring API
handle_call(get_public_state, _From, State) ->
	PublicState = {State#state.last_peer_response},
	{reply, {ok, PublicState}, State};

handle_call({get_peer, PartitionNumber}, _From, State) ->
	{reply, get_peer(PartitionNumber, State), State};

handle_call({get_peers, PartitionNumber}, _From, State) ->
	{reply, get_peers(PartitionNumber, State), State};

handle_call({get_peer_partitions, Peer}, _From, State) ->
	#state{ last_peer_response = Map } = State,
	case maps:get(Peer, Map, []) of
		[] ->
			{reply, [], State};
		{true, Partitions} ->
			{reply, Partitions, State}
	end;

handle_call(get_cluster_partitions_list, _From, State) ->
	PeerPartitions =
		maps:fold(
			fun(PartitionID, Items, Acc) ->
				lists:foldl(
					fun	({{pool, _}, _, _}, Acc2) ->
							Acc2;
						({_Peer, PackingAddr, PackingDifficulty}, Acc2) ->
							sets:add_element(ar_serialize:partition_to_json_struct(
									PartitionID, ?PARTITION_SIZE, PackingAddr,
									PackingDifficulty), Acc2)
					end,
					Acc,
					Items
				)
			end,
			sets:new(),
			State#state.peers_by_partition
		),
	Set = get_unique_partitions_set(ar_mining_io:get_partitions(), PeerPartitions),
	{reply, lists:sort(sets:to_list(Set)), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast(check_batches, State) ->
	ar_util:cast_after(?BATCH_POLL_INTERVAL_MS, ?MODULE, check_batches),
	OutBatches = check_out_batches(State),
	{noreply, State#state{ out_batches = OutBatches }};

handle_cast({computed_h1, ShareableCandidate, H1, Nonce}, State) ->
	#state{ out_batches = OutBatches } = State,
	#mining_candidate{
		cache_ref = CacheRef
	} = ShareableCandidate,
	Now = os:system_time(millisecond),
	{Start, ShareableCandidate2} = maps:get(CacheRef, OutBatches, {Now, ShareableCandidate}),
	H1List = [{H1, Nonce} | ShareableCandidate2#mining_candidate.cm_h1_list],
	ShareableCandidate3 = ShareableCandidate2#mining_candidate{ cm_h1_list = H1List },
	OutBatches2 = case length(H1List) >= ?BATCH_SIZE_LIMIT of
		true ->
			send_h1(ShareableCandidate3, State),
			maps:remove(CacheRef, OutBatches);
		false ->
			maps:put(CacheRef, {Start, ShareableCandidate3}, OutBatches)
	end,
	{noreply, State#state{ out_batches = OutBatches2 }};

handle_cast({compute_h2_for_peer, Candidate}, State) ->
	%% No don't need to batch inbound batches since ar_mining_io will cache the recall
	%% range for a short period greatly lowering the cost of processing the same
	%% multiple times across several batches.
	ar_mining_server:compute_h2_for_peer(Candidate),
	{noreply, State};

handle_cast({computed_h2_for_peer, Candidate}, State) ->
	#mining_candidate{ cm_lead_peer = Peer } = Candidate,
	send_h2(Peer, Candidate),
	{noreply, State};

handle_cast(refetch_peer_partitions, State) ->
	{ok, Config} = application:get_env(arweave, config),
	Peers = Config#config.cm_peers,
	Peers2 =
		case Config#config.cm_exit_peer == not_set
				orelse lists:member(Config#config.cm_exit_peer, Peers) of
			true ->
				%% Either we are the exit node or the exit node
				%% is already configured as yet another mining peer.
				Peers;
			false ->
				[Config#config.cm_exit_peer | Peers]
		end,
	ar_util:cast_after(Config#config.cm_poll_interval, ?MODULE, refetch_peer_partitions),
	refetch_peer_partitions(Peers2),
	{noreply, State};

handle_cast({update_peer, {Peer, PartitionList}}, State) ->
	SetValue = {true, PartitionList},
	State2 = State#state{
		last_peer_response = maps:put(Peer, SetValue, State#state.last_peer_response)
	},
	State3 = remove_mining_peer(Peer, State2),
	State4 = add_mining_peer({Peer, PartitionList}, State3),
	{noreply, State4};

handle_cast({remove_peer, Peer}, State) ->
	State3 = case maps:get(Peer, State#state.last_peer_response, none) of
		none ->
			State;
		{_, OldPartitionList} ->
			SetValue = {false, OldPartitionList},
			% NOTE. We keep OldPartitionList because we don't want blinky stat
			State2 = State#state{
				last_peer_response = maps:put(Peer, SetValue, State#state.last_peer_response)
			},
			?LOG_INFO([{event, cm_peer_removed}, {peer, ar_util:format_peer(Peer)}]),
			remove_mining_peer(Peer, State2)
	end,
	{noreply, State3};

handle_cast(refetch_pool_peer_partitions, State) ->
	%% Casted when we are a CM exit peer and a pool client. We collect our local peer
	%% partitions and push them to the pool getting the pool's complementary partitions
	%% in response.
	UniquePeerPartitions =
		maps:fold(
			fun	({pool, _}, _Value, Acc) ->
					Acc;
				(_Peer, {_, Partitions}, Acc) ->
					get_unique_partitions_set(Partitions, Acc)
			end,
			sets:new(),
			State#state.last_peer_response
		),
	refetch_pool_peer_partitions(UniquePeerPartitions),
	{noreply, State};

handle_cast(garbage_collect, State) ->
	erlang:garbage_collect(self(),
		[{async, {ar_coordination, self(), erlang:monotonic_time()}}]),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({garbage_collect, {Name, Pid, StartTime}, GCResult}, State) ->
	EndTime = erlang:monotonic_time(),
	ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, millisecond),
	case GCResult == false orelse ElapsedTime > ?GC_LOG_THRESHOLD of
		true ->
			?LOG_DEBUG([
				{event, mining_debug_garbage_collect}, {process, Name}, {pid, Pid},
				{gc_time, ElapsedTime}, {gc_result, GCResult}]);
		false ->
			ok
	end,
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%% @doc Return the list of the partitions of the given Peer, to the best
%% of our knowledge.
get_peer_partitions(Peer) ->
	gen_server:call(?MODULE, {get_peer_partitions, Peer}, infinity).

check_out_batches(#state{out_batches = OutBatches}) when map_size(OutBatches) == 0 ->
	OutBatches;
check_out_batches(State) ->
	#state{ out_batches = OutBatches, out_batch_timeout = BatchTimeout } = State,
	Now = os:system_time(millisecond),
	maps:filter(
		fun	(_CacheRef, {Start, Candidate}) ->
			case Now - Start >= BatchTimeout of
				true ->
					%% send this batch, and remove it from the map
					send_h1(Candidate, State),
					false;
				false ->
					%% not yet time to send this batch, keep it in the map
					true
			end
		end,
		OutBatches
	).

get_peers(PartitionNumber, State) ->
	[element(1, El) || El <- maps:get(PartitionNumber, State#state.peers_by_partition, [])].

get_peer(PartitionNumber, State) ->
	case get_peers(PartitionNumber, State) of
		[] ->
			none;
		Peers ->
			lists:last(Peers)
	end.

send_h1(Candidate, State) ->
	#mining_candidate{
		partition_number2 = PartitionNumber2, cm_h1_list = H1List } = Candidate,
	case get_peer(PartitionNumber2, State) of
		none ->
			ok;
		Peer ->
			Candidate2 = Candidate#mining_candidate{ label = <<"cm">> },
			spawn(fun() ->
				ar_http_iface_client:cm_h1_send(Peer, Candidate2)
			end),
			case Peer of
				{pool, _} ->
					ar_mining_stats:h1_sent_to_peer(pool, length(H1List));
				_ ->
					ar_mining_stats:h1_sent_to_peer(Peer, length(H1List))
			end
	end.

send_h2(Peer, Candidate) ->
	spawn(fun() ->
		ar_http_iface_client:cm_h2_send(Peer, Candidate)
	end),
	case Peer of
		{pool, _} ->
			ar_mining_stats:h2_sent_to_peer(pool);
		_ ->
			ar_mining_stats:h2_sent_to_peer(Peer)
	end.

add_mining_peer({Peer, StorageModules}, State) ->
	Partitions = lists:map(
		fun({PartitionID, _PartitionSize, PackingAddr, PackingDifficulty}) ->
			{PartitionID, PackingAddr, PackingDifficulty} end, StorageModules),
	?LOG_INFO([{event, cm_peer_updated},
		{peer, ar_util:format_peer(Peer)},
		{partitions, io_lib:format("~p",
			[[{ID, ar_util:encode(Addr), PackingDifficulty}
				|| {ID, Addr, PackingDifficulty} <- Partitions]])}]),
	PeersByPartition =
		lists:foldl(
			fun({PartitionID, PackingAddr, PackingDifficulty}, Acc) ->
				Items = maps:get(PartitionID, Acc, []),
				maps:put(PartitionID, [{Peer, PackingAddr, PackingDifficulty} | Items], Acc)
			end,
			State#state.peers_by_partition,
			Partitions
		),
	State#state{ peers_by_partition = PeersByPartition }.

remove_mining_peer(Peer, State) ->
	PeersByPartition = maps:fold(
		fun(PartitionID, Peers, Acc) ->
			Peers2 = [{Peer2, Addr, PackingDifficulty}
					|| {Peer2, Addr, PackingDifficulty} <- Peers, Peer2 /= Peer],
			maps:put(PartitionID, Peers2, Acc)
		end,
		#{},
		State#state.peers_by_partition
	),
	State#state{ peers_by_partition = PeersByPartition }.

refetch_peer_partitions(Peers) ->
	spawn(fun() ->
		
		ar_util:pmap(
			fun(Peer) ->
				case ar_http_iface_client:get_cm_partition_table(Peer) of
					{ok, PartitionList} ->
						ar_coordination:update_peer(Peer, PartitionList);
					_ ->
						ok
				end end,
				Peers
			),
		%% ar_util:pmap ensures we fetch all the local up-to-date CM peer partitions first,
		%% then share them with the Pool to fetch the complementary pool CM peer partitions.
		case {ar_pool:is_client(), ar_coordination:is_exit_peer()} of
			{true, true} ->
				refetch_pool_peer_partitions();
			_ ->
				ok
		end
	end).

refetch_pool_peer_partitions() ->
	gen_server:cast(?MODULE, refetch_pool_peer_partitions).

get_unique_partitions_list() ->
	Set = get_unique_partitions_set(ar_mining_io:get_partitions(), sets:new()),
	lists:sort(sets:to_list(Set)).

get_unique_partitions_set() ->
	get_unique_partitions_set(ar_mining_io:get_partitions(), sets:new()).

get_unique_partitions_set([], UniquePartitions) ->
	UniquePartitions;
get_unique_partitions_set([{PartitionID, MiningAddress, PackingDifficulty} | Partitions],
		UniquePartitions) ->
	get_unique_partitions_set(
		Partitions,
		sets:add_element(ar_serialize:partition_to_json_struct(PartitionID, ?PARTITION_SIZE,
				MiningAddress, PackingDifficulty), UniquePartitions)
	);
get_unique_partitions_set([{PartitionID, BucketSize, MiningAddress, PackingDifficulty}
		| Partitions], UniquePartitions) ->
	get_unique_partitions_set(
		Partitions,
		sets:add_element(
			ar_serialize:partition_to_json_struct(PartitionID, BucketSize,
					MiningAddress, PackingDifficulty), UniquePartitions)
	).

refetch_pool_peer_partitions(UniquePeerPartitions) ->
	spawn(fun() ->
		JSON = ar_serialize:jsonify(lists:sort(sets:to_list(UniquePeerPartitions))),
		PoolPeer = ar_pool:pool_peer(),
		case ar_http_iface_client:post_cm_partition_table_to_pool(PoolPeer, JSON) of
			{ok, PartitionList} ->
				ar_coordination:update_peer(PoolPeer, PartitionList);
			_ ->
				ok
		end
	end).
