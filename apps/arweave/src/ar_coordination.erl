-module(ar_coordination).

-behaviour(gen_server).

-export([
	start_link/0, computed_h1/2, compute_h2_for_peer/2, computed_h2_for_peer/1,
	get_public_state/0, send_h1_batch_to_peer/0, stat_loop/0, get_peer/1,
	is_exit_peer/0
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
	h1_batch_timer
}).

-define(START_DELAY, 1000).

-ifdef(DEBUG).
-define(BATCH_SIZE_LIMIT, 2).
-else.
-define(BATCH_SIZE_LIMIT, 400).
-endif.

-define(BATCH_TIMEOUT_MS, 20).

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
	H1BatchTimerRef = ar_util:cast_after(?BATCH_TIMEOUT_MS, ?MODULE, send_h1_batch_to_peer),
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
	{ok, State2}.

%% Helper function to see state while testing and later for monitoring API
handle_call(get_public_state, _From, State) ->
	PublicState = {State#state.last_peer_response},
	{reply, {ok, PublicState}, State};

handle_call({get_peer, PartitionNumber}, _From, State) ->
	{reply, get_peer(PartitionNumber, State), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({computed_h1, Candidate, Diff}, State) ->
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
		cm_diff = Diff,
		cm_lead_peer = not_set,
		h1 = not_set,
		h2 = not_set,
		nonce = not_set,
		poa2 = not_set,		
		preimage = not_set
	},
	ShareableCandidate = maps:get(CacheRef, PeerRequests, DefaultCandidate),
	H1List = [{H1, Nonce} | ShareableCandidate#mining_candidate.cm_h1_list],
	?LOG_DEBUG([{event, cm_computed_h1},
		{mining_address, ar_util:encode(ShareableCandidate#mining_candidate.mining_address)},
		{h1, ar_util:encode(H1)}, {nonce, Nonce}]),
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
	TimerRef = ar_util:cast_after(?BATCH_TIMEOUT_MS, ?MODULE, send_h1_batch_to_peer),
	{noreply, State#state{h1_batch_timer = TimerRef}};

handle_cast(send_h1_batch_to_peer, #state{peer_requests = PeerRequests, h1_batch_timer = TimerRef} = State) ->
	erlang:cancel_timer(TimerRef),
	maps:fold(
		fun	(_CacheRef, Candidate, _) ->
			#mining_candidate{
				partition_number2 = PartitionNumber2, cm_h1_list = H1List } = Candidate,
			case get_peer(PartitionNumber2, State) of
				none ->
					ok;
				Peer ->
					ar_http_iface_client:cm_h1_send(Peer, Candidate),
					ar_mining_stats:h1_sent_to_peer(Peer, length(H1List))
			end
		end,
		ok,
		PeerRequests
	),
	NewTimerRef = ar_util:cast_after(?BATCH_TIMEOUT_MS, ?MODULE, send_h1_batch_to_peer),
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
	ar_http_iface_client:cm_h2_send(Peer, Candidate),
	ar_mining_stats:h2_sent_to_peer(Peer),
	{noreply, State};

handle_cast(refresh_peers, State) ->
	{ok, Config} = application:get_env(arweave, config),
	State2 = refresh(Config#config.cm_peers, State),
	ar_util:cast_after(Config#config.cm_poll_interval, ?MODULE, refresh_peers),
	{noreply, State2};

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
get_peer(PartitionNumber, State) ->
	case maps:find(PartitionNumber, State#state.peers_by_partition) of
		{ok, Peers} ->
			lists:last(Peers);
		_ ->
			none
	end.

add_mining_peer({Peer, StorageModules}, State) ->
	PeersByPartition =
		lists:foldl(
			fun({PartitionId, _PartitionSize, _PackingAddr}, Acc) ->
				Peers = maps:get(PartitionId, Acc, []),
				maps:put(PartitionId, [Peer | Peers], Acc)
			end,
			State#state.peers_by_partition,
			StorageModules
		),
	State#state{peers_by_partition = PeersByPartition}.

remove_mining_peer(Peer, State) ->
	PeersByPartition = maps:fold(
		fun(PartitionId, Peers, Acc) ->
			Peers2 = lists:delete(Peer, Peers),
			case Peers2 of
				[] ->
					Acc;
				_ ->
					maps:put(PartitionId, Peers2, Acc)
			end
		end,
		#{},
		State#state.peers_by_partition
	),
	State#state{peers_by_partition = PeersByPartition}.

refresh([], State) ->
	State;
refresh([Peer | Peers], State) ->
	NewState = case ar_http_iface_client:get_cm_partition_table(Peer) of
		{ok, PartitionList} ->
			SetValue = {true, PartitionList},
			State2 = State#state{
				last_peer_response = maps:put(Peer, SetValue, State#state.last_peer_response)
			},
			State3 = remove_mining_peer(Peer, State2),
			add_mining_peer({Peer, PartitionList}, State3);
		_ ->
			case maps:get(Peer, State#state.last_peer_response, none) of
				none ->
					State;
				{_, OldPartitionList} ->
					SetValue = {false, OldPartitionList},
					% NOTE. We keep OldPartitionList because we don't want blinky stat
					State2 = State#state{
						last_peer_response = maps:put(Peer, SetValue, State#state.last_peer_response)
					},
					remove_mining_peer(Peer, State2)
			end
	end,
	refresh(Peers, NewState).
