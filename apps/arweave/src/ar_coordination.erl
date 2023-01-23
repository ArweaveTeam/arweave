-module(ar_coordination).

-behaviour(gen_server).

% TODO reset_mining_session/1 -> reset_mining_session/0
-export([
	start_link/0, computed_h1/1, check_partition/2, reset_mining_session/1, get_state/0, get_public_state/0, call_remote_peer/0,
	compute_h2/2, computed_h2/1, poll_loop/0, stat_loop/0
]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-record(state, {
	last_peer_response = #{},
	timer_poll,
	timer_stat,
	peers_by_partition = #{},
	diff_addr_h0_pn_pub_to_req_list_map = #{},
	% key = {Diff, Addr, H0, PartitionNumber, PartitionUpperBound, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput}
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
-define(BATCH_SIZE_LIMIT, 400).
-define(BATCH_TIMEOUT_MS, 20).

%%%===================================================================
%%% Public interface.
%%%===================================================================
% TODO call/cast -> call


%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% Helper function to see state while testing
%% TODO Remove it
get_state() ->
	gen_server:call(?MODULE, get_state).

%% Helper function to see state while testing and later for monitoring API
get_public_state() ->
	gen_server:call(?MODULE, get_public_state).

%% @doc An H1 has been generated. Store it to send it later to a
%% coordinated mining peer
computed_h1({CorrelationRef, Diff, Addr, H0, H1, Nonce, PartitionNumber, PartitionUpperBound, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput, MiningSession}) ->
	gen_server:cast(?MODULE, {computed_h1, CorrelationRef, Diff, Addr, H0, H1, Nonce, PartitionNumber, PartitionUpperBound, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput, MiningSession}).

call_remote_peer() ->
	gen_server:cast(?MODULE, call_remote_peer).

%% @doc Check if there is a peer with PartitionNumber2 and prepare the
%% coordination to send requests
check_partition(PartitionNumber2, Addr) ->
	gen_server:call(
		?MODULE,
		{check_partition, PartitionNumber2, Addr}
	).

%% @doc Mining session has changed. Reset it and discard any intermediate value
reset_mining_session(Ref) ->
	gen_server:call(?MODULE, {reset_mining_session, Ref}).

%% @doc Compute h2 from a remote peer
compute_h2(Peer, H2Materials) ->
	gen_server:cast(?MODULE, {compute_h2, Peer, H2Materials}).

computed_h2(Args) ->
	gen_server:cast(?MODULE, {computed_h2, Args}).

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
	
	{ok, TRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, call_remote_peer, []),
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
					io:format("CRITICAL WARNING. cm_exit_peer is not set. Coordinated mining will not produce final solution.~n");
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

%% Helper callback to see state while testing
%% TODO Remove it
handle_call(get_state, _From, State) ->
	{reply, {ok, State}, State};

% TODO remove
handle_call(
	{check_partition, PartitionNumber2, Addr},
	_From,
	State
) ->
	% BUG. Argument should be not partition number, but offset
	case maps:find(PartitionNumber2, State#state.peers_by_partition) of
		{ok, PeerList} ->
			lists:foldl(
				fun (Val, Res) ->
					case Val of
						{_Peer, _PartitionStart, _PartitionEnd, Addr} ->
							true;
						_ ->
							Res
					end
				end,
				false,
				PeerList
			);
		_ ->
			{reply, false, State}
	end;

%% Helper function to see state while testing and later for monitoring API
handle_call(get_public_state, _From, State) ->
	PublicState = {State#state.last_peer_response},
	{reply, {ok, PublicState}, State};

handle_call({reset_mining_session, _MiningSession}, _From, State) ->
	{reply, ok, State#state{
		diff_addr_h0_pn_pub_to_req_list_map = #{}
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
	io:format("Coordinated mining stat~n", []),
	% TODO print IO stat in header
	io:format("~21s | ~5s | ~13s | ~13s | ~13s ~n", ["peer", "alive",
		"h1 I/O total", "h1 per second", "h2 I/O total"
	]),
	io:format("~21s | ~5s | ~s~n", ["", "", "partition list"]),
	{ok, Config} = application:get_env(arweave, config),
	% TODO print self
	% self alive == is_joined
	maps:foreach(fun(Peer, Value) ->
		{AliveStatus, PartitionList} = Value,
		IOStat = maps:get(Peer, State#state.peer_io_stat, #peer_io_stat{}),
		% 21 enough for IPv4 111.111.111.111:11111
		io:format("~21s | ~5s | ~6B/~6B | ~6B/~6B | ~6B/~6B ~n", [
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
				io:format("~21s | ~5s | ~5B ~20B ~s~n", ["", "", Bucket, BucketSize, ar_util:encode(Addr)]),
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


handle_cast({computed_h1, _CorrelationRef, Diff, Addr, H0, H1, Nonce, PartitionNumber, PartitionUpperBound, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput, _MiningSession}, State) ->
	#state{diff_addr_h0_pn_pub_to_req_list_map = DiffAddrH0PNPUBToReqListMap} = State,
	OldList = maps:get({Diff, Addr, H0, PartitionNumber, PartitionUpperBound, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput}, DiffAddrH0PNPUBToReqListMap, []),
	NewList = OldList ++ [{H1, Nonce}],
	NewAH0ReqListMap = maps:put({Diff, Addr, H0, PartitionNumber, PartitionUpperBound, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput}, NewList, DiffAddrH0PNPUBToReqListMap),
	case length(NewList) >= ?BATCH_SIZE_LIMIT of
		true ->
			% NOTE should save first, then call applied
			call_remote_peer();
		false ->
			ok
	end,
	{noreply, State#state{diff_addr_h0_pn_pub_to_req_list_map = NewAH0ReqListMap}};

handle_cast(call_remote_peer, #state{diff_addr_h0_pn_pub_to_req_list_map = DiffAddrH0PNPUBToReqListMap} = State) when map_size(DiffAddrH0PNPUBToReqListMap) == 0 ->
	{ok, NewTRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, call_remote_peer, []),
	{noreply, State#state{timer = NewTRef}};

handle_cast(call_remote_peer, #state{diff_addr_h0_pn_pub_to_req_list_map = DiffAddrH0PNPUBToReqListMap, timer = TRef} = State) ->
	timer:cancel(TRef),
	NewPeerIOStat = maps:fold(
		fun	({Diff, Addr, H0, PartitionNumber, PartitionUpperBound, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput}, ReqList, Acc) ->
			case maps:find(PartitionNumber, State#state.peers_by_partition) of
				{ok, List} ->
					{Peer, _PartitionStart, _PartitionEnd, Addr} = lists:last(List),
					ar_http_iface_client:cm_h1_send(Peer, {Diff, Addr, H0, PartitionNumber, PartitionUpperBound, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput, ReqList}),
					H1Count = length(ReqList),
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
		DiffAddrH0PNPUBToReqListMap
	),
	{ok, NewTRef} = timer:apply_after(?BATCH_TIMEOUT_MS, ?MODULE, call_remote_peer, []),
	NewState = State#state{
		diff_addr_h0_pn_pub_to_req_list_map = #{},
		timer = NewTRef,
		peer_io_stat = NewPeerIOStat
	},
	{noreply, NewState};

handle_cast({reset_mining_session, _MiningSession}, State) ->
	{noreply, State#state{
		diff_addr_h0_pn_pub_to_req_list_map = #{}
	}};

handle_cast({compute_h2, Peer, H2Materials}, State) ->
	ar_mining_server:remote_compute_h2(Peer, H2Materials),
	{_Diff, _Addr, _H0, _PartitionNumber, _PartitionUpperBound, ReqList} = H2Materials,
	OldStat = maps:get(Peer, State#state.peer_io_stat, #peer_io_stat{}),
	H1Count = length(ReqList),
	NewStat = OldStat#peer_io_stat{
		h1_in_counter = OldStat#peer_io_stat.h1_in_counter + H1Count,
		h1_in_counter_ps = OldStat#peer_io_stat.h1_in_counter_ps + H1Count
	},
	NewPeerIOStat = maps:put(Peer, NewStat, State#state.peer_io_stat),
	NewState = State#state{
		peer_io_stat = NewPeerIOStat
	},
	{noreply, NewState};

handle_cast({computed_h2, {Diff, Addr, H0, H1, Nonce, PartitionNumber, PartitionUpperBound, PoA2, H2, Preimage, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput, Peer}}, State) ->
	io:format("DEBUG computed_h2~n"),
	ar_http_iface_client:cm_h2_send(Peer, {Diff, Addr, H0, H1, Nonce, PartitionNumber, PartitionUpperBound, PoA2, H2, Preimage, Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput}),
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
			fun({PartitionSize, PartitionId, PackingAddr}, Acc) ->
				%% Allowing the case of same partition handled by different peers
				% TODO range start, range end
				NewElement = {Peer, 0, PartitionSize, PackingAddr},
				Acc2 = case maps:get(PartitionId, Acc, none) of
					% BUG. Partition numbers in hash are for partitions with 4TB size
					% PartitionId in request if size != 4TB can be other id
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


