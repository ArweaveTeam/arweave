-module(ar_coordination).

-behaviour(gen_server).

-export([
	start_link/0, computed_h1/2, compute_h2/3, computed_h2/1, post_solution/1,
	reset_mining_session/0, get_public_state/0,
	compute_h2_on_peer/0, stat_loop/0, get_peer/1
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").

-record(state, {
	last_peer_response = #{},
	timer_stat,
	peers_by_partition = #{},
	peer_requests = #{},
	peer_io_stat = #{},
	timer
}).
-record(peer_io_stat, {
	h1_out_counter = 0,
	h1_in_counter = 0,
	% per second resetable
	h1_out_counter_ps = 0,
	h1_in_counter_ps = 0,
	% solutions
	h2_out_counter = 0,
	h2_in_counter = 0
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
% TODO call/cast -> call

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

compute_h2_on_peer() ->
	gen_server:cast(?MODULE, compute_h2_on_peer).

%% @doc Mining session has changed. Reset it and discard any intermediate value
reset_mining_session() ->
	gen_server:call(?MODULE, reset_mining_session).

%% @doc Compute h2 for a remote peer
compute_h2(Peer, Candidate, H1List) ->
	gen_server:cast(?MODULE, {compute_h2,
		Candidate#mining_candidate{ cm_lead_peer = Peer }, H1List}).

computed_h2(Candidate) ->
	gen_server:cast(?MODULE, {computed_h2, Candidate}).

post_solution(Candidate) ->
	ar_mining_server:prepare_and_post_solution(Candidate).

stat_loop() ->
	gen_server:call(?MODULE, stat_loop).

get_peer(PartitionNumber) ->
	gen_server:call(?MODULE, {get_peer, PartitionNumber}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	
	{ok, TRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, compute_h2_on_peer, []),
	State = #state{
		last_peer_response = #{},
		timer = TRef
	},
	State2 = case Config#config.coordinated_mining of
		false ->
			State;
		true ->
			case Config#config.cm_exit_peer of
				not_set ->
					ar:console("CRITICAL WARNING. cm_exit_peer is not set. Coordinated mining will not produce final solution.~n");
				_ ->
					ok
			end,
			ar_util:cast_after(?START_DELAY, ?MODULE, refresh_peers),
			{ok, TimerStat} = timer:apply_after(?START_DELAY, ?MODULE, stat_loop, []),
			State#state{
				last_peer_response = #{},
				timer_stat = TimerStat
			}
	end,
	{ok, State2}.

%% Helper function to see state while testing and later for monitoring API
handle_call(get_public_state, _From, State) ->
	PublicState = {State#state.last_peer_response},
	{reply, {ok, PublicState}, State};

handle_call(reset_mining_session, _From, State) ->
	{reply, ok, State#state{ peer_requests = #{} }};

handle_call(stat_loop, _From, State) ->
	ar:console("Coordinated mining stat~n", []),
	% TODO print IO stat in header
	ar:console("~21s | ~5s | ~13s | ~13s | ~13s ~n", ["peer", "alive",
		"h1 I/O total", "h1 per second", "h2 I/O total"
	]),
	ar:console("~21s | ~5s | ~s~n", ["", "", "partition list"]),
	{ok, Config} = application:get_env(arweave, config),
	% TODO print self
	% self alive == is_joined
	maps:foreach(fun(Peer, Value) ->
		{AliveStatus, PartitionList} = Value,
		IOStat = maps:get(Peer, State#state.peer_io_stat, #peer_io_stat{}),
		% 21 enough for IPv4 111.111.111.111:11111
		ar:console("~21s | ~5s | ~6B/~6B | ~6B/~6B | ~6B/~6B ~n", [
			list_to_binary(ar_util:format_peer(Peer)), AliveStatus,
			IOStat#peer_io_stat.h1_in_counter, IOStat#peer_io_stat.h1_out_counter,
			% TODO make float
			(1000*IOStat#peer_io_stat.h1_in_counter_ps) div Config#config.cm_stat_interval,
			(1000*IOStat#peer_io_stat.h1_out_counter_ps) div Config#config.cm_stat_interval,
			IOStat#peer_io_stat.h2_in_counter, IOStat#peer_io_stat.h2_out_counter
		]),
		lists:foreach(
			fun	(ListValue) ->
				{Bucket, BucketSize, Addr} = ListValue,
				ar:console("~21s | ~5s | ~5B ~20B ~s~n", ["", "", Bucket, BucketSize, ar_util:encode(Addr)]),
				ok
			end,
			PartitionList
		),
		ok
		end,
		State#state.last_peer_response
	),
	{ok, TimerStat} = timer:apply_after(Config#config.cm_stat_interval, ?MODULE, stat_loop, []),
	NewPeerIOStat = maps:fold(
		fun (Peer, OldStat, Acc) ->
			NewStat = OldStat#peer_io_stat{
				h1_out_counter_ps = 0,
				h1_in_counter_ps = 0
			},
			maps:put(Peer, NewStat, Acc)
		end,
		#{},
		State#state.peer_io_stat
	),
	NewState = State#state{
		timer_stat = TimerStat,
		peer_io_stat = NewPeerIOStat
	},
	{reply, {ok}, NewState};

handle_call({get_peer, PartitionNumber}, _From, State) ->
	{reply, get_peer(PartitionNumber, State), State};

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
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
		preimage = not_set,
		session_ref = not_set
	},
	{ShareableCandidate, H1List} = maps:get(
			CacheRef, PeerRequests,
			{DefaultCandidate, []}),
	H1List2 = [{H1, Nonce} | H1List],
	?LOG_DEBUG([{event, cm_computed_h1},
		{mining_address, ar_util:encode(ShareableCandidate#mining_candidate.mining_address)},
		{h1, ar_util:encode(H1)}, {nonce, Nonce}]),
	PeerRequests2 = maps:put(CacheRef, {ShareableCandidate, H1List2}, PeerRequests),
	case length(H1List2) >= ?BATCH_SIZE_LIMIT of
		true ->
			compute_h2_on_peer();
		false ->
			ok
	end,
	{noreply, State#state{peer_requests = PeerRequests2}};

handle_cast(compute_h2_on_peer, #state{peer_requests = PeerRequests} = State)
  		when map_size(PeerRequests) == 0 ->
	{ok, NewTRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, compute_h2_on_peer, []),
	{noreply, State#state{timer = NewTRef}};

handle_cast(compute_h2_on_peer, #state{peer_requests = PeerRequests, timer = TRef} = State) ->
	timer:cancel(TRef),
	NewPeerIOStat = maps:fold(
		fun	(_CacheRef, {Candidate, H1List}, Acc) ->
			#mining_candidate{ partition_number2 = PartitionNumber2 } = Candidate,
			case get_peer(PartitionNumber2, State) of
				none ->
					Acc;
				Peer ->
					ar_http_iface_client:cm_h1_send(Peer, Candidate, H1List),
					H1Count = length(H1List),
					OldStat = maps:get(Peer, Acc, #peer_io_stat{}),
					NewStat = OldStat#peer_io_stat{
						h1_out_counter = OldStat#peer_io_stat.h1_out_counter + H1Count,
						h1_out_counter_ps = OldStat#peer_io_stat.h1_out_counter_ps + H1Count
					},
					maps:put(Peer, NewStat, Acc)
			end
		end,
		State#state.peer_io_stat,
		PeerRequests
	),
	{ok, NewTRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, compute_h2_on_peer, []),
	NewState = State#state{
		peer_requests = #{},
		timer = NewTRef,
		peer_io_stat = NewPeerIOStat
	},
	{noreply, NewState};

handle_cast({compute_h2, Candidate, H1List}, State) ->
	#mining_candidate{ cm_lead_peer = Peer } = Candidate,
	ar_mining_server:compute_h2_for_peer(Candidate, H1List),
	OldStat = maps:get(Peer, State#state.peer_io_stat, #peer_io_stat{}),
	H1Count = length(H1List),
	NewStat = OldStat#peer_io_stat{
		h1_in_counter = OldStat#peer_io_stat.h1_in_counter + H1Count,
		h1_in_counter_ps = OldStat#peer_io_stat.h1_in_counter_ps + H1Count
	},
	NewPeerIOStat = maps:put(Peer, NewStat, State#state.peer_io_stat),
	NewState = State#state{
		peer_io_stat = NewPeerIOStat
	},
	{noreply, NewState};

handle_cast({computed_h2, Candidate}, State) ->
	#mining_candidate{ cm_lead_peer = Peer } = Candidate,
	ar_http_iface_client:cm_h2_send(Peer, Candidate),
	OldStat = maps:get(Peer, State#state.peer_io_stat, #peer_io_stat{}),
	NewStat = OldStat#peer_io_stat{
		h2_out_counter = OldStat#peer_io_stat.h2_out_counter + 1
	},
	NewPeerIOStat = maps:put(Peer, NewStat, State#state.peer_io_stat),
	NewState = State#state{
		peer_io_stat = NewPeerIOStat
	},
	{noreply, NewState};

handle_cast(refresh_peers, State) ->
	{ok, Config} = application:get_env(arweave, config),
	State2 = refresh(Config#config.cm_peers, State),
	ar_util:cast_after(Config#config.cm_poll_interval, ?MODULE, refresh_peers),
	{noreply, State2};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
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
