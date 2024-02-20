-module(ar_coordination).

-behaviour(gen_server).

-export([
	start_link/0, computed_h1/2, compute_h2_for_peer/2, computed_h2_for_peer/1,
	get_public_state/0, send_h1_batch_to_peer/0, stat_loop/0, get_peers/1, get_peer/1,
	update_peer/2, remove_peer/1, is_exit_peer/0
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").

-record(state, {
	last_peer_response = #{},
	peers_by_partition = #{},
	peer_requests = #{},
	h1_batch_timer,
	batch_timeout = ?DEFAULT_CM_BATCH_TIMEOUT_MS
}).

-define(START_DELAY, 1000).

-ifdef(DEBUG).
-define(BATCH_SIZE_LIMIT, 2).
-else.
-define(BATCH_SIZE_LIMIT, 400).
-endif.

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
computed_h1(Candidate, Diff) ->
	gen_server:cast(?MODULE, {computed_h1, Candidate, Diff}).

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

%% Return true if we are an exit peer in the coordinated mining setup.
is_exit_peer() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.coordinated_mining == true andalso
			Config#config.cm_exit_peer == not_set.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	
	%% using ar_util:cast_after so we can cancel pending timers. This allows us to send the
	%% h1 batch as soon as it's full instead of waiting for the timeout to expire.
	H1BatchTimerRef = ar_util:cast_after(
		Config#config.cm_batch_timeout, ?MODULE, send_h1_batch_to_peer),
	State = #state{
		last_peer_response = #{},
		h1_batch_timer = H1BatchTimerRef
	},
	State2 = case Config#config.coordinated_mining of
		false ->
			State;
		true ->
			case Config#config.cm_exit_peer of
				not_set ->
					ar:console(
						"CRITICAL WARNING. cm_exit_peer is not set. Coordinated mining will "
						"not produce final solution.~n");
				_ ->
					ok
			end,
			ar_util:cast_after(?START_DELAY, ?MODULE, refresh_peers),
			State#state{
				last_peer_response = #{}
			}
	end,
	{ok, State2#state{ batch_timeout = Config#config.cm_batch_timeout }}.

%% Helper function to see state while testing and later for monitoring API
handle_call(get_public_state, _From, State) ->
	PublicState = {State#state.last_peer_response},
	{reply, {ok, PublicState}, State};

handle_call({get_peer, PartitionNumber}, _From, State) ->
	{reply, get_peer(PartitionNumber, State), State};

handle_call({get_peers, PartitionNumber}, _From, State) ->
	{reply, get_peers(PartitionNumber, State), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({computed_h1, Candidate, DiffPair}, State) ->
	#mining_candidate{
		cache_ref = CacheRef,
		h1 = H1,
		nonce = Nonce
	} = Candidate,
	#state{peer_requests = PeerRequests} = State,
	%% prepare Candidate to be shared with a remote miner.
	%% 1. Add the current difficulty (the remote peer will use this instead of its local difficulty)
	%% 2. Remove any data that's not needed by the peer. This cuts down on the volume of data
	%%    shared.
	%% 3. The peer field will be set to this peer's address by the remote miner
	DefaultCandidate = Candidate#mining_candidate{
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
	ShareableCandidate = maps:get(CacheRef, PeerRequests, DefaultCandidate),
	H1List = [{H1, Nonce} | ShareableCandidate#mining_candidate.cm_h1_list],
	PeerRequests2 = maps:put(
		CacheRef,
		ShareableCandidate#mining_candidate{ cm_h1_list = H1List },
		PeerRequests),
	case length(H1List) >= ?BATCH_SIZE_LIMIT of
		true ->
			send_h1_batch_to_peer();
		false ->
			ok
	end,
	{noreply, State#state{ peer_requests = PeerRequests2 }};

handle_cast(send_h1_batch_to_peer, #state{peer_requests = PeerRequests} = State)
  		when map_size(PeerRequests) == 0 ->
	#state{ h1_batch_timer = TimerRef, batch_timeout = BatchTimeout } = State,
	erlang:cancel_timer(TimerRef),
	NewTimerRef = ar_util:cast_after(BatchTimeout, ?MODULE, send_h1_batch_to_peer),
	{noreply, State#state{h1_batch_timer = NewTimerRef}};

handle_cast(send_h1_batch_to_peer, State) ->
	#state{ peer_requests = PeerRequests, h1_batch_timer = TimerRef,
		batch_timeout = BatchTimeout } = State,
	erlang:cancel_timer(TimerRef),
	NewTimerRef = ar_util:cast_after(BatchTimeout, ?MODULE, send_h1_batch_to_peer),
	maps:fold(
		fun	(_CacheRef, Candidate, _) ->
			#mining_candidate{ partition_number2 = PartitionNumber2 } = Candidate,
			case get_peer(PartitionNumber2, State) of
				none ->
					ok;
				Peer ->
					send_h1(Peer, Candidate)
			end
		end,
		ok,
		PeerRequests
	),
	NewState = State#state{
		peer_requests = #{},
		h1_batch_timer = NewTimerRef
	},
	{noreply, NewState};

handle_cast({compute_h2_for_peer, Candidate}, State) ->
	#mining_candidate{ cm_lead_peer = Peer, cm_h1_list = H1List } = Candidate,
	ar_mining_server:compute_h2_for_peer(Candidate),
	ar_mining_stats:h1_received_from_peer(Peer, length(H1List)),
	{noreply, State};

handle_cast({computed_h2_for_peer, Candidate}, State) ->
	#mining_candidate{ cm_lead_peer = Peer } = Candidate,
	send_h2(Peer, Candidate),
	{noreply, State};

handle_cast(refresh_peers, State) ->
	{ok, Config} = application:get_env(arweave, config),
	ar_util:cast_after(Config#config.cm_poll_interval, ?MODULE, refresh_peers),
	refresh(Config#config.cm_peers),
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

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================
get_peers(PartitionNumber, State) ->
	maps:get(PartitionNumber, State#state.peers_by_partition, []).

get_peer(PartitionNumber, State) ->
	case get_peers(PartitionNumber, State) of
		[] ->
			none;
		Peers ->
			lists:last(Peers)
	end.

send_h1(Peer, Candidate) ->
	#mining_candidate{ cm_h1_list = H1List } = Candidate,
	spawn(fun() -> 
        ar_http_iface_client:cm_h1_send(Peer, Candidate)
    end),
	ar_mining_stats:h1_sent_to_peer(Peer, length(H1List)).

send_h2(Peer, Candidate) ->
	spawn(fun() -> 
        ar_http_iface_client:cm_h2_send(Peer, Candidate)
    end),
	ar_mining_stats:h2_sent_to_peer(Peer).

add_mining_peer({Peer, StorageModules}, State) ->
	Partitions = lists:map(
		fun({PartitionId, _PartitionSize, _PackingAddr}) -> PartitionId end, StorageModules),
	?LOG_INFO([{event, cm_peer_updated},
		{peer, ar_util:format_peer(Peer)}, {partitions, Partitions}]),
	PeersByPartition =
		lists:foldl(
			fun(PartitionId, Acc) ->
				Peers = maps:get(PartitionId, Acc, []),
				maps:put(PartitionId, [Peer | Peers], Acc)
			end,
			State#state.peers_by_partition,
			Partitions
		),
	State#state{peers_by_partition = PeersByPartition}.

remove_mining_peer(Peer, State) ->
	PeersByPartition = maps:fold(
		fun(PartitionId, Peers, Acc) ->
			maps:put(PartitionId, lists:delete(Peer, Peers), Acc)
		end,
		#{},
		State#state.peers_by_partition
	),
	State#state{peers_by_partition = PeersByPartition}.

refresh([]) ->
	ok;
refresh([Peer | Peers]) ->
	spawn(fun() -> 
        case ar_http_iface_client:get_cm_partition_table(Peer) of
			{ok, PartitionList} ->
				ar_coordination:update_peer(Peer, PartitionList);
			_ ->
				ar_coordination:remove_peer(Peer)
		end
    end),
	refresh(Peers).
