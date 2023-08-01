-module(ar_coordination).

-behaviour(gen_server).

-export([
	start_link/0, computed_h1/2, compute_h2/3, computed_h2/1, 
	reset_mining_session/0, get_public_state/0, compute_h2_on_peer/0, poll_loop/0, stat_loop/0
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_mining.hrl").

-record(state, {
	last_peer_response = #{},
	timer_poll,
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
	gen_server:call(?MODULE, {reset_mining_session}).

%% @doc Compute h2 for a remote peer
compute_h2(Peer, Candidate, H1List) ->
	gen_server:cast(?MODULE, {compute_h2,
		Candidate#mining_candidate{ cm_lead_peer = Peer }, H1List}).

computed_h2(Candidate) ->
	gen_server:cast(?MODULE, {computed_h2, Candidate}).

poll_loop() ->
	gen_server:call(?MODULE, poll_loop).

stat_loop() ->
	gen_server:call(?MODULE, stat_loop).

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
			{ok, TimerPoll} = timer:apply_after(?START_DELAY, ?MODULE, poll_loop, []),
			{ok, TimerStat} = timer:apply_after(?START_DELAY, ?MODULE, stat_loop, []),
			State#state{
				last_peer_response = #{},
				timer_poll = TimerPoll,
				timer_stat = TimerStat
			}
	end,
	{ok, State2}.

%% Helper function to see state while testing and later for monitoring API
handle_call(get_public_state, _From, State) ->
	PublicState = {State#state.last_peer_response},
	{reply, {ok, PublicState}, State};

handle_call({reset_mining_session}, _From, State) ->
	{reply, ok, State#state{
		peer_requests = #{}
	}};

handle_call(poll_loop, _From, State) ->
	{ok, Config} = application:get_env(arweave, config),
	NewState = refresh(Config#config.cm_peers, State),
	{ok, TimerPoll} = timer:apply_after(Config#config.cm_poll_interval, ?MODULE, poll_loop, []),
	NewState2 = NewState#state{
		timer_poll = TimerPoll
	},
	{reply, {ok}, NewState2};

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
		cm_diff = Diff,
		cm_lead_peer = not_set,
		nonce = not_set,
		h1 = not_set,
		h2 = not_set,
		poa1 = not_set,
		poa2 = not_set,
		chunk1 = not_set,
		chunk2 = not_set,
		preimage = not_set
	},
	{ShareableCandidate, H1List} = maps:get(
			CacheRef, PeerRequests,
			{DefaultCandidate, []}),
	H1List2 = [{H1, Nonce} | H1List],
	PeerRequests2 = maps:put(CacheRef, {ShareableCandidate, H1List2}, PeerRequests),
	case length(H1List2) >= ?BATCH_SIZE_LIMIT of
		true ->
			?LOG_INFO([{event, batch_size_limit}]),
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
	?LOG_INFO([{event, compute_h2_on_peer}]),
	timer:cancel(TRef),
	NewPeerIOStat = maps:fold(
		fun	(_CacheRef, {Candidate, H1List}, Acc) ->
			#mining_candidate{ 
				partition_number2 = PartitionNumber2,
				mining_address = MiningAddress } = Candidate,
			case maps:find(PartitionNumber2, State#state.peers_by_partition) of
				{ok, List} ->
					{Peer, _PartitionStart, _PartitionEnd, MiningAddress} = lists:last(List),
					ar_http_iface_client:cm_h1_send(Peer, Candidate, H1List),
					H1Count = length(H1List),
					OldStat = maps:get(Peer, Acc, #peer_io_stat{}),
					NewStat = OldStat#peer_io_stat{
						h1_out_counter = OldStat#peer_io_stat.h1_out_counter + H1Count,
						h1_out_counter_ps = OldStat#peer_io_stat.h1_out_counter_ps + H1Count
					},
					maps:put(Peer, NewStat, Acc);
				_ ->
					Acc
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

handle_cast({reset_mining_session, _MiningSession}, State) ->
	{noreply, State#state{
		peer_requests = #{}
	}};

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

add_mining_peer({Peer, StorageModules}, State) ->
	MiningPeers =
		lists:foldl(
			fun({PartitionId, PartitionSize, PackingAddr}, Acc) ->
				%% Allowing the case of same partition handled by different peers
				% TODO range start, range end
				NewElement = {Peer, 0, PartitionSize, PackingAddr},
				Acc2 = case maps:get(PartitionId, Acc, none) of
					L when is_list(L) ->
						case lists:member(NewElement, L) of
							true ->
								Acc;
							false ->
								maps:put(PartitionId, [NewElement | L], Acc)
						end;
					none ->
						maps:put(PartitionId, [NewElement], Acc)
				end,
				Acc2
			end,
			State#state.peers_by_partition,
			StorageModules
		),
	State#state{peers_by_partition = MiningPeers}.

remove_mining_peer(Peer, State) ->
	PeersByPartition2 = maps:fold(
		fun(PartitionId, PeerPartitionList, Acc) ->
			PeerPartitionList2 = lists:foldl(
				fun(Value, ListAcc) ->
					case Value of
						{Peer, _PartitionSize, _PackingAddr} ->
							ListAcc;
						_ ->
							[ Value | ListAcc ]
					end
				end,
				[],
				PeerPartitionList
			),
			case PeerPartitionList2 of
				[] ->
					Acc;
				_ ->
					maps:put(PartitionId, PeerPartitionList2, Acc)
			end
		end,
		#{},
		State#state.peers_by_partition
	),
	State#state{peers_by_partition = PeersByPartition2}.

refresh([], State) ->
	State;
refresh([Peer | Peers], State) ->
	NewState = case ar_http_iface_client:get_cm_partition_table(Peer) of
		{ok, PartitionList} ->
			SetValue = {true, PartitionList},
			State2 = State#state{
				last_peer_response = maps:put(Peer, SetValue, State#state.last_peer_response)
			},
			% TODO maybe there is better variant how to refresh partitions without remove+add shuffling
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
