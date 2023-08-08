%%% @doc The 2.6 mining server.
-module(ar_mining_server).
% TODO Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput -> #vdf

-behaviour(gen_server).

-export([start_link/0, pause/0, start_mining/1, recall_chunk/4, computed_hash/4, set_difficulty/1,
		pause_performance_reports/1, compute_h2_for_peer/2,
		prepare_and_post_solution/1, post_solution/1,
		get_recall_bytes/4, is_session_valid/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(mining_session, {
	ref,
	seed,
	next_seed,
	start_interval_number,
	partition_upper_bound,
	chunk_cache = #{},
	chunk_cache_size_limit = infinity
}).

-record(state, {
	hashing_threads				= queue:new(),
	hashing_thread_monitor_refs = #{},
	session						= #mining_session{},
	diff						= infinity,
	task_queue					= gb_sets:new(),
	pause_performance_reports	= false,
	pause_performance_reports_timeout
}).

-define(TASK_CHECK_FREQUENCY_MS, 200).
-define(PERFORMANCE_REPORT_FREQUENCY_MS, 10000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Pause the mining server.
pause() ->
	gen_server:cast(?MODULE, pause).

%% @doc Start mining.
start_mining(Args) ->
	gen_server:cast(?MODULE, {start_mining, Args}).

%% @doc Callback from ar_mining_io when a chunk is read
recall_chunk(chunk1, Chunk, Nonce, Candidate) ->
	add_task(chunk1, Candidate#mining_candidate{ chunk1 = Chunk, nonce = Nonce });
recall_chunk(chunk2, Chunk, Nonce, Candidate) ->
	add_task(chunk2, Candidate#mining_candidate{ chunk2 = Chunk, nonce = Nonce });
recall_chunk(skipped, undefined, Nonce, Candidate) ->
	signal_cache_cleanup(Nonce, Candidate).

%% @doc Callback from the hashing threads when a hash is computed
computed_hash(computed_h0, H0, undefined, Candidate) ->
	add_task(computed_h0, Candidate#mining_candidate{ h0 = H0 });
computed_hash(computed_h1, H1, Preimage, Candidate) ->
	add_task(computed_h1, Candidate#mining_candidate{ h1 = H1, preimage = Preimage });
computed_hash(computed_h2, H2, Preimage, Candidate) ->
	add_task(computed_h2, Candidate#mining_candidate{ h2 = H2, preimage = Preimage }).

%% @doc Compute H2 for a remote peer (used in coordinated mining).
compute_h2_for_peer(Candidate, H1List) ->
	gen_server:cast(?MODULE, {compute_h2_for_peer, generate_cache_ref(Candidate), H1List}).

%% @doc Set the new mining difficulty. We do not recalculate it inside the mining
%% server because we want to completely detach the mining server from the block
%% ordering. The previous block is chosen only after the mining solution is found (if
%% we choose it in advance we may miss a better option arriving in the process).
%% Also, a mining session may (in practice, almost always will) span several blocks.
set_difficulty(Diff) ->
	gen_server:cast(?MODULE, {set_difficulty, Diff}).

set_merkle_rebase_threshold(Threshold) ->
	gen_server:cast(?MODULE, {set_merkle_rebase_threshold, Threshold}).

%% @doc Stop logging performance reports for the given number of milliseconds.
pause_performance_reports(Time) ->
	gen_server:cast(?MODULE, {pause_performance_reports, Time}).

prepare_and_post_solution(Candidate) ->
	gen_server:cast(?MODULE, {prepare_and_post_solution, Candidate}).

post_solution(Solution) ->
	gen_server:cast(?MODULE, {post_solution, Solution}).

%% @doc Returns true if the mining candidate belongs to a valid mining session. Always assume
%% that a coordinated mining candidate is valid (its session_ref is not_set)
is_session_valid(_SessionRef, #mining_candidate{ session_ref = not_set }) ->
	true;
is_session_valid(undefined, _Candidate) ->
	false; 
is_session_valid(SessionRef, #mining_candidate{ session_ref = SessionRef }) ->
	true;
is_session_valid(_SessionRef, _Candidate) ->
	false.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(nonce_limiter),
	{ok, Config} = application:get_env(arweave, config),
	ar_chunk_storage:open_files("default"),
	gen_server:cast(?MODULE, handle_task),
	gen_server:cast(?MODULE, report_performance),
	ets:insert(?MODULE, {chunk_cache_size, 0}),
	prometheus_gauge:set(mining_server_chunk_cache_size, 0),
	State = lists:foldl(
		fun(_, Acc) -> start_hashing_thread(Acc) end,
		#state{},
		lists:seq(1, Config#config.hashing_threads)
	),
	{ok, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast(pause, #state{ hashing_threads = HashingThreads } = State) ->
	[Thread ! stop || Thread <- queue:to_list(HashingThreads)],
	ar_mining_io:stop(),
	prometheus_gauge:set(mining_rate, 0),
	{noreply, State#state{ diff = infinity, session = #mining_session{} }};

handle_cast({start_mining, Args}, State) ->
	{Diff, RebaseThreshold} = Args,
	ar:console("Starting mining.~n"),
	Session = reset_mining_session(State),
	ar_mining_io:reset_performance_counters(),
	{noreply, State#state{ diff = Diff, session = Session }};

handle_cast({set_difficulty, _Diff},
		#state{ session = #mining_session{ ref = undefined } } = State) ->
	{noreply, State};
handle_cast({set_difficulty, Diff}, State) ->
	{noreply, State#state{ diff = Diff }};

handle_cast({add_task, {TaskType, Candidate} = Task}, State) ->
	#state{ session = #mining_session{ ref = SessionRef }, task_queue = Q } = State,
	case is_session_valid(SessionRef, Candidate) of
		true ->
			StepNumber = Candidate#mining_candidate.step_number,
			Q2 = gb_sets:insert({priority(TaskType, StepNumber), make_ref(), Task}, Q),
			prometheus_gauge:inc(mining_server_task_queue_len),
			{noreply, State#state{ task_queue = Q2 }};
		false ->
			{noreply, State}
	end;
	
handle_cast(handle_task, #state{ task_queue = Q } = State) ->
	case gb_sets:is_empty(Q) of
		true ->
			ar_util:cast_after(?TASK_CHECK_FREQUENCY_MS, ?MODULE, handle_task),
			{noreply, State};
		_ ->
			{{_Priority, _ID, Task}, Q2} = gb_sets:take_smallest(Q),
			prometheus_gauge:dec(mining_server_task_queue_len),
			may_be_warn_about_lag(Task, Q2),
			gen_server:cast(?MODULE, handle_task),
			handle_task(Task, State#state{ task_queue = Q2 })
	end;

handle_cast({compute_h2_for_peer, Candidate, H1List}, State) ->
	#state{ session = Session } = State,
	#mining_session{ chunk_cache = Map } = Session,
	#mining_candidate{
		h0 = H0,
		partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound
	} = Candidate,
	
	{_RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	Range2Exists = ar_mining_io:read_recall_range(chunk2, Candidate, RecallRange2Start),
	case Range2Exists of
		true ->
			reserve_cache_space(),
			Map2 = cache_h1_list(Candidate, H1List, Map),
			Session2 = Session#mining_session{ chunk_cache = Map2 },
			{noreply, State#state{ session = Session2 }};
		false ->
			%% This can happen if the remote peer has an outdated partition table
			{noreply, State}
	end;

handle_cast({prepare_and_post_solution, Candidate}, State) ->
	prepare_and_post_solution(Candidate, State),
	{noreply, State};

handle_cast({post_solution, Solution}, State) ->
	post_solution(Solution, State),
	{noreply, State};

handle_cast({may_be_remove_chunk_from_cache, _Args},
		#state{ session = #mining_session{ ref = undefined } } = State) ->
	{noreply, State};
handle_cast({may_be_remove_chunk_from_cache, _Args} = Task,
		#state{ task_queue = Q } = State) ->
	Q2 = gb_sets:insert({priority(may_be_remove_chunk_from_cache), make_ref(), Task}, Q),
	prometheus_gauge:inc(mining_server_task_queue_len),
	{noreply, State#state{ task_queue = Q2 }};

handle_cast(report_performance,
		#state{ session = #mining_session{ partition_upper_bound = undefined } } = State) ->
	ar_util:cast_after(?PERFORMANCE_REPORT_FREQUENCY_MS, ?MODULE, report_performance),
	{noreply, State};
handle_cast(report_performance, #state{ pause_performance_reports = true,
			pause_performance_reports_timeout = Timeout } = State) ->
	Now = os:system_time(millisecond),
	case Now > Timeout of
		true ->
			gen_server:cast(?MODULE, report_performance),
			{noreply, State#state{ pause_performance_reports = false }};
		false ->
			ar_util:cast_after(?PERFORMANCE_REPORT_FREQUENCY_MS, ?MODULE, report_performance),
			{noreply, State}
	end;
handle_cast(report_performance, #state{ session = Session } = State) ->
	#mining_session{ partition_upper_bound = PartitionUpperBound } = Session,	
	Partitions = ar_mining_io:get_partitions(PartitionUpperBound),
	Now = erlang:monotonic_time(millisecond),
	VdfSpeed = vdf_speed(Now),
	{IOList, MaxPartitionTime, PartitionsSum, MaxCurrentTime, CurrentsSum} =
		lists:foldr(
			fun({Partition, _ReplicaID, StoreID}, {Acc1, Acc2, Acc3, Acc4, Acc5} = Acc) ->
				case ets:lookup(?MODULE, {performance, Partition}) of
					[] ->
						Acc;
					[{_, PartitionStart, _, CurrentStart, _}]
							when Now - PartitionStart =:= 0
								orelse Now - CurrentStart =:= 0 ->
						Acc;
					[{_, PartitionStart, PartitionTotal, CurrentStart, CurrentTotal}] ->
						ets:update_counter(?MODULE,
										{performance, Partition},
										[{4, -1, Now, Now}, {5, 0, -1, 0}]),
						PartitionTimeLapse = (Now - PartitionStart) / 1000,
						PartitionAvg = PartitionTotal / PartitionTimeLapse / 4,
						CurrentTimeLapse = (Now - CurrentStart) / 1000,
						CurrentAvg = CurrentTotal / CurrentTimeLapse / 4,
						Optimal = optimal_performance(StoreID, VdfSpeed),
						?LOG_INFO([{event, mining_partition_performance_report},
								{partition, Partition}, {avg, PartitionAvg},
								{current, CurrentAvg}]),
						case Optimal of
							undefined ->
								{[io_lib:format("Partition ~B avg: ~.2f MiB/s, "
										"current: ~.2f MiB/s.~n",
									[Partition, PartitionAvg, CurrentAvg]) | Acc1],
									max(Acc2, PartitionTimeLapse), Acc3 + PartitionTotal,
									max(Acc4, CurrentTimeLapse), Acc5 + CurrentTotal};
							_ ->
								{[io_lib:format("Partition ~B avg: ~.2f MiB/s, "
										"current: ~.2f MiB/s, "
										"optimum: ~.2f MiB/s, ~.2f MiB/s (full weave).~n",
									[Partition, PartitionAvg, CurrentAvg, Optimal / 2,
											Optimal]) | Acc1],
									max(Acc2, PartitionTimeLapse), Acc3 + PartitionTotal,
									max(Acc4, CurrentTimeLapse), Acc5 + CurrentTotal}
						end
				end
			end,
			{[], 0, 0, 0, 0},
			Partitions
		),
	case MaxPartitionTime > 0 of
		true ->
			TotalAvg = PartitionsSum / MaxPartitionTime / 4,
			TotalCurrent = CurrentsSum / MaxCurrentTime / 4,
			?LOG_INFO([{event, mining_performance_report}, {total_avg_mibps, TotalAvg},
					{total_avg_hps, TotalAvg * 4}, {total_current_mibps, TotalCurrent},
					{total_current_hps, TotalCurrent * 4}]),
			Str =
				case VdfSpeed of
					undefined ->
						io_lib:format("~nMining performance report:~nTotal avg: ~.2f MiB/s, "
								" ~.2f h/s; current: ~.2f MiB/s, ~.2f h/s.~n",
						[TotalAvg, TotalAvg * 4, TotalCurrent, TotalCurrent * 4]);
					_ ->
						io_lib:format("~nMining performance report:~nTotal avg: ~.2f MiB/s, "
								" ~.2f h/s; current: ~.2f MiB/s, ~.2f h/s; VDF: ~.2f s.~n",
						[TotalAvg, TotalAvg * 4, TotalCurrent, TotalCurrent * 4, VdfSpeed])
				end,
			prometheus_gauge:set(mining_rate, TotalCurrent * 4),
			IOList2 = [Str | [IOList | ["~n"]]],
			ar:console(iolist_to_binary(IOList2));
		false ->
			ok
	end,
	ar_util:cast_after(?PERFORMANCE_REPORT_FREQUENCY_MS, ?MODULE, report_performance),
	{noreply, State};

handle_cast({pause_performance_reports, Time}, State) ->
	Now = os:system_time(millisecond),
	Timeout = Now + Time,
	{noreply, State#state{ pause_performance_reports = true,
			pause_performance_reports_timeout = Timeout }};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({'DOWN', Ref, process, _, Reason},
		#state{ hashing_thread_monitor_refs = HashingThreadRefs } = State) ->
	case maps:is_key(Ref, HashingThreadRefs) of
		true ->
			{noreply, handle_hashing_thread_down(Ref, Reason, State)};
		_ ->
			{noreply, State}
	end;

handle_info({event, nonce_limiter, {computed_output, _}},
		#state{ session = #mining_session{ ref = undefined } } = State) ->
	?LOG_DEBUG([{event, mining_debug_nonce_limiter_computed_output_session_undefined}]),
	{noreply, State};
handle_info({event, nonce_limiter, {computed_output, Args}},
		#state{ task_queue = Q } = State) ->
	{SessionKey, Session, _PrevSessionKey, _PrevSession, Output, PartitionUpperBound} = Args,
	StepNumber = Session#vdf_session.step_number,
	true = is_integer(StepNumber),
	ets:update_counter(?MODULE,
			{performance, nonce_limiter},
			[{3, 1}],
			{{performance, nonce_limiter}, erlang:monotonic_time(millisecond), 0}),
	#vdf_session{ seed = Seed, step_number = StepNumber } = Session,
	Task = {computed_output, {SessionKey, Seed, StepNumber, Output, PartitionUpperBound}},
	Q2 = gb_sets:insert({priority(nonce_limiter_computed_output, StepNumber), make_ref(),
			Task}, Q),
	prometheus_gauge:inc(mining_server_task_queue_len),
	{noreply, State#state{ task_queue = Q2 }};
handle_info({event, nonce_limiter, _}, State) ->
	{noreply, State};


handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, #state{ hashing_threads = HashingThreads } = State) ->
	[Thread ! stop || Thread <- queue:to_list(HashingThreads)],
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

start_hashing_thread(State) ->
	#state{ hashing_threads = Threads, hashing_thread_monitor_refs = Refs,
			session = #mining_session{ ref = SessionRef } } = State,
	Thread = spawn(fun hashing_thread/0),
	Ref = monitor(process, Thread),
	Threads2 = queue:in(Thread, Threads),
	Refs2 = maps:put(Ref, Thread, Refs),
	case SessionRef of
		undefined ->
			ok;
		_ ->
			Thread ! {new_mining_session, SessionRef}
	end,
	State#state{ hashing_threads = Threads2, hashing_thread_monitor_refs = Refs2 }.

handle_hashing_thread_down(Ref, Reason,
		#state{ hashing_threads = Threads, hashing_thread_monitor_refs = Refs } = State) ->
	?LOG_WARNING([{event, mining_hashing_thread_down},
			{reason, io_lib:format("~p", [Reason])}]),
	Thread = maps:get(Ref, Refs),
	Refs2 = maps:remove(Ref, Refs),
	Threads2 = queue:delete(Thread, Threads),
	start_hashing_thread(State#state{ hashing_threads = Threads2,
			hashing_thread_monitor_refs = Refs2 }).

get_chunk_cache_size_limit() ->
	ThreadCount = ar_mining_io:get_thread_count(),
	% Two ranges per output.
	OptimalLimit = ar_util:ceil_int(
		(?RECALL_RANGE_SIZE * 2 * ThreadCount) div ?DATA_CHUNK_SIZE,
		100),

	{ok, Config} = application:get_env(arweave, config),
	Limit = case Config#config.mining_server_chunk_cache_size_limit of
		undefined ->
			Total = proplists:get_value(total_memory,
					memsup:get_system_memory_data(), 2000000000),
			Bytes = Total * 0.7 / 3,
			CalculatedLimit = erlang:ceil(Bytes / ?DATA_CHUNK_SIZE),
			min(ar_util:ceil_int(CalculatedLimit, 100), OptimalLimit);
		N ->
			N
	end,

	ar:console("~nSetting the chunk cache size limit to ~B chunks.~n", [Limit]),
	?LOG_INFO([{event, setting_chunk_cache_size_limit}, {limit, Limit}]),
	case Limit < OptimalLimit of
		true ->
			ar:console("~nChunk cache size limit is below optimal limit of ~p. "
				"Mining performance may be impacted.~n"
				"Consider adding more RAM or changing the "
				"'mining_server_chunk_cache_size_limit' option.", [OptimalLimit]);
		false -> ok
	end,
	Limit.

may_be_warn_about_lag({computed_output, _Args}, Q) ->
	case gb_sets:is_empty(Q) of
		true ->
			ok;
		false ->
			case gb_sets:take_smallest(Q) of
				{{_Priority, _ID, {computed_output, _}}, Q3} ->
					N = count_nonce_limiter_tasks(Q3) + 1,
					?LOG_WARNING([{event, mining_server_lags_behind_the_nonce_limiter},
						{step_count, N}]);
				_ ->
					ok
			end
	end;
may_be_warn_about_lag(_Task, _Q) ->
	ok.

count_nonce_limiter_tasks(Q) ->
	case gb_sets:is_empty(Q) of
		true ->
			0;
		false ->
			case gb_sets:take_smallest(Q) of
				{{_Priority, _ID, {computed_output, _Args}}, Q2} ->
					1 + count_nonce_limiter_tasks(Q2);
				_ ->
					0
			end
	end.

hashing_thread() ->
	hashing_thread(not_set).

hashing_thread(SessionRef) ->
	receive
		stop ->
			hashing_thread();
		{compute_h0, Candidate} ->
			case ar_mining_server:is_session_valid(SessionRef, Candidate) of
				true ->
					#mining_candidate{
						mining_address = MiningAddress, nonce_limiter_output = Output,
						partition_number = PartitionNumber, seed = Seed } = Candidate,
					H0 = ar_block:compute_h0(Output, PartitionNumber, Seed, MiningAddress),
					ar_mining_server:computed_hash(computed_h0, H0, undefined, Candidate);
				false ->
					ok %% Clear the message queue of requests from outdated mining sessions
			end,
			hashing_thread(SessionRef);
		{compute_h1, Candidate} ->
			case ar_mining_server:is_session_valid(SessionRef, Candidate) of
				true ->
					#mining_candidate{ h0 = H0, nonce = Nonce, chunk1 = Chunk1 } = Candidate,
					{H1, Preimage} = ar_block:compute_h1(H0, Nonce, Chunk1),
					ar_mining_server:computed_hash(computed_h1, H1, Preimage, Candidate);
				false ->
					ok %% Clear the message queue of requests from outdated mining sessions
			end,
			hashing_thread(SessionRef);
		{compute_h2, Candidate} ->
			case ar_mining_server:is_session_valid(SessionRef, Candidate) of
				true ->
					#mining_candidate{ h0 = H0, h1 = H1, chunk2 = Chunk2 } = Candidate,
					{H2, Preimage} = ar_block:compute_h2(H1, Chunk2, H0),
					ar_mining_server:computed_hash(computed_h2, H2, Preimage, Candidate);
				false ->
					ok %% Clear the message queue of requests from outdated mining sessions
			end,
			hashing_thread(SessionRef);
		{new_mining_session, Ref} ->
			hashing_thread(Ref)
	end.

distribute_output(Partitions, Candidate, Distributed, State) ->
	distribute_output(Partitions, Candidate, Distributed, State, 0).

distribute_output([], _Candidate, _Distributed, State, N) ->
	{N, State};
distribute_output([{PartitionNumber, MiningAddress, _StoreID} | Partitions],
		Candidate, Distributed, State, N) ->
	case maps:is_key({PartitionNumber, MiningAddress}, Distributed) of
		true ->
			distribute_output(Partitions, Candidate, Distributed, State, N);
		false ->
			#state{ session = Session } = State,
			#mining_session{ chunk_cache = Map } = Session,
			?LOG_ERROR([{event, distribute_output},
				{partition, PartitionNumber}, {keys, maps:size(Map)}]),
			#state{ hashing_threads = Threads } = State,
			{Thread, Threads2} = pick_hashing_thread(Threads),
			Thread ! {compute_h0,
				Candidate#mining_candidate{
					partition_number = PartitionNumber,
					mining_address = MiningAddress
				}},
			State2 = State#state{ hashing_threads = Threads2 },
			Distributed2 = maps:put({PartitionNumber, MiningAddress}, sent, Distributed),
			distribute_output(Partitions, Candidate, Distributed2, State2, N + 1)
	end.

reserve_cache_space() ->
	NonceMax = max(0, ((?RECALL_RANGE_SIZE) div ?DATA_CHUNK_SIZE - 1)),
	ets:update_counter(?MODULE, chunk_cache_size, {2, NonceMax + 1}),
	prometheus_gauge:inc(mining_server_chunk_cache_size, NonceMax + 1).

priority(may_be_remove_chunk_from_cache) ->
	0.

priority(computed_h2, StepNumber) ->
	{1, -StepNumber};
priority(computed_h1, StepNumber) ->
	{2, -StepNumber};
priority(chunk2, StepNumber) ->
	{3, -StepNumber};
priority(chunk1, StepNumber) ->
	{4, -StepNumber};
priority(computed_h0, StepNumber) ->
	{5, -StepNumber};
priority(nonce_limiter_computed_output, StepNumber) ->
	{6, -StepNumber}.

handle_task({computed_output, _},
		#state{ session = #mining_session{ ref = undefined } } = State) ->
	?LOG_DEBUG([{event, mining_debug_handle_task_computed_output_session_undefined}]),
	{noreply, State};
handle_task({computed_output, Args}, State) ->
	#state{ session = Session } = State,
	{{NextSeed, StartIntervalNumber}, Seed, StepNumber, Output, PartitionUpperBound} = Args,
	#mining_session{ next_seed = CurrentNextSeed,
			start_interval_number = CurrentStartIntervalNumber,
			partition_upper_bound = CurrentPartitionUpperBound } = Session,
	Session2 =
		case {CurrentStartIntervalNumber, CurrentNextSeed, CurrentPartitionUpperBound}
				== {StartIntervalNumber, NextSeed, PartitionUpperBound} of
			true ->
				Session;
			false ->
				ar:console("Starting new mining session. Upper bound: ~B, entropy nonce: ~s, "
						"next entropy nonce: ~s, interval number: ~B.~n",
						[PartitionUpperBound, ar_util:encode(Seed), ar_util:encode(NextSeed),
						StartIntervalNumber]),
				NewSession = reset_mining_session(State),
				NewSession#mining_session{ seed = Seed, next_seed = NextSeed,
						start_interval_number = StartIntervalNumber,
						partition_upper_bound = PartitionUpperBound }
		end,
	Ref = Session2#mining_session.ref,
	Partitions = ar_mining_io:get_partitions(PartitionUpperBound),
	Candidate = #mining_candidate{
		session_ref = Ref,
		seed = Seed,
		next_seed = NextSeed,
		start_interval_number = StartIntervalNumber,
		step_number = StepNumber,
		nonce_limiter_output = Output,
		partition_upper_bound = PartitionUpperBound
	},
	{N, State2} = distribute_output(Partitions, Candidate, #{}, State),
	?LOG_DEBUG([{event, mining_debug_processing_vdf_output}, {found_io_threads, N}]),
	{noreply, State2#state{ session = Session2 }};

handle_task({chunk1, Candidate}, State) ->
	case is_session_valid(State#state.session#mining_session.ref, Candidate) of
		true ->
			#state{ hashing_threads = Threads } = State,
			{Thread, Threads2} = pick_hashing_thread(Threads),
			Thread ! {compute_h1, Candidate},
			{noreply, State#state{ hashing_threads = Threads2 }};
		false ->
			{noreply, State}
	end;

handle_task({chunk2, Candidate}, State) ->
	case is_session_valid(State#state.session#mining_session.ref, Candidate) of
		true ->
			#state{ session = Session, hashing_threads = Threads } = State,
			#mining_session{ chunk_cache = Map } = Session,
			#mining_candidate{ chunk2 = Chunk2 } = Candidate,
			case cycle_chunk_cache(Candidate, {chunk2, Chunk2}, Map) of
				{{chunk1, Chunk1, H1}, Map2} ->
					{Thread, Threads2} = pick_hashing_thread(Threads),
					Thread ! {compute_h2, Candidate#mining_candidate{ chunk1 = Chunk1, h1 = H1 } },
					Session2 = Session#mining_session{ chunk_cache = Map2 },
					{noreply, State#state{ session = Session2, hashing_threads = Threads2 }};
				{{chunk1, H1}, Map2} ->
					{Thread, Threads2} = pick_hashing_thread(Threads),
					Thread ! {compute_h2, Candidate#mining_candidate{ h1 = H1 } },
					Session2 = Session#mining_session{ chunk_cache = Map2 },
					{noreply, State#state{ session = Session2, hashing_threads = Threads2 }};
				{_, Map2} ->
					Session2 = Session#mining_session{ chunk_cache = Map2 },
					{noreply, State#state{ session = Session2 }}
			end;
		false ->
			{noreply, State}
	end;

handle_task({computed_h0, Candidate}, State) ->
	case is_session_valid(State#state.session#mining_session.ref, Candidate) of
		true ->
			#state{ session = #mining_session{ chunk_cache_size_limit = Limit } } = State,
			[{_, Size}] = ets:lookup(?MODULE, chunk_cache_size),
			case Size > Limit of
				true ->
					%% Re-add the task so that it can bd executed later once some cache space frees up.
					add_task(computed_h0, Candidate);
				false ->
					#mining_candidate{ h0 = H0, partition_number = PartitionNumber,
						partition_upper_bound = PartitionUpperBound } = Candidate,
					{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
							PartitionNumber, PartitionUpperBound),
					PartitionNumber2 = ?PARTITION_NUMBER(RecallRange2Start),
					Candidate2 = Candidate#mining_candidate{ partition_number2 = PartitionNumber2 },
					Candidate3 = generate_cache_ref(Candidate2),
					?LOG_ERROR([{event, computed_h0},
							{partition1, PartitionNumber},
							{partition2, PartitionNumber2},
							{recall_range1, RecallRange1Start},
							{recall_range2, RecallRange2Start}]),

					Range1Exists = ar_mining_io:read_recall_range(
							chunk1, Candidate3, RecallRange1Start),
					case Range1Exists of
						true ->
							reserve_cache_space(),
							Range2Exists = ar_mining_io:read_recall_range(
									chunk2, Candidate3, RecallRange2Start),
							case Range2Exists of
								true -> reserve_cache_space();
								false -> signal_cache_cleanup(Candidate3)
							end;
						false ->
							?LOG_DEBUG([{event, mining_debug_no_io_thread_found_for_range},
								{range_start, RecallRange1Start},
								{range_end, RecallRange1Start + ?RECALL_RANGE_SIZE}]),
							ok
					end
			end;
		false ->
			ok
	end,
	{noreply, State};

handle_task({computed_h1, Candidate}, State) ->
	case is_session_valid(State#state.session#mining_session.ref, Candidate) of
		true ->
			#state{ session = Session, diff = Diff, hashing_threads = Threads } = State,
			#mining_session{ chunk_cache = Map } = Session,
			#mining_candidate{ h1 = H1, chunk1 = Chunk1 } = Candidate,
			case binary:decode_unsigned(H1, big) > Diff of
				true ->
					?LOG_ERROR([{event, h1_solution}, {partition, Candidate#mining_candidate.partition_number}]),
					#state{ session = Session } = State,
					Map2 = evict_chunk_cache(Candidate, Map),
					Session2 = Session#mining_session{ chunk_cache = Map2 },
					State2 = State#state{ session = Session2 },
					prepare_and_post_solution(Candidate, State2),
					{noreply, State2};
				false ->
					{ok, Config} = application:get_env(arweave, config),
					case cycle_chunk_cache(Candidate, {chunk1, Chunk1, H1}, Map) of
						{cached, Map2} ->
							%% Chunk2 hasn't been read yet, so we cache Chunk1 and wait for
							%% Chunk2 to be read.
							Session2 = Session#mining_session{ chunk_cache = Map2 },
							{noreply, State#state{ session = Session2 }};
						{do_not_cache, Map2} ->
							%% This node does not store Chunk2. If we're part of a coordinated
							%% mining set, we can try one of our peers, otherwise we're done.
							case Config#config.coordinated_mining of
								false ->
									ok;
								true ->
									ar_coordination:computed_h1(Candidate, Diff)
							end,
							Session2 = Session#mining_session{ chunk_cache = Map2 },
							{noreply, State#state{ session = Session2 }};
						{{chunk2, Chunk2}, Map2} ->
							%% Chunk2 has already been read, so we can compute H2 now.
							{Thread, Threads2} = pick_hashing_thread(Threads),
							Thread ! {compute_h2, Candidate#mining_candidate{ chunk2 = Chunk2 }},
							Session2 = Session#mining_session{ chunk_cache = Map2 },
							{noreply, State#state{ session = Session2, hashing_threads = Threads2 }}
					end
			end;
		false ->
			{noreply, State}
	end;

handle_task({computed_h2, Candidate}, State) ->
	case is_session_valid(State#state.session#mining_session.ref, Candidate) of
		true ->
			#mining_candidate{
				chunk2 = Chunk2, h0 = H0, h2 = H2, mining_address = MiningAddress,
				nonce = Nonce, partition_number = PartitionNumber, 
				partition_upper_bound = PartitionUpperBound, cm_lead_peer = Peer
			} = Candidate,
			case binary:decode_unsigned(H2, big) > get_difficulty(State, Candidate) of
				true ->
					?LOG_ERROR([
						{event, h2_solution},
						{nonce, Nonce},
						{h0, ar_util:encode(H0)},
						{partition, Candidate#mining_candidate.partition_number2}]),
					case Peer of
						not_set ->
							prepare_and_post_solution(Candidate, State);
						_ ->
							{_RecallByte1, RecallByte2} = get_recall_bytes(
									H0, PartitionNumber, Nonce, PartitionUpperBound),
							PoA2 = read_poa(RecallByte2, Chunk2, MiningAddress),
							case PoA2 of
								error ->
									?LOG_WARNING([{event,
											mined_block_but_failed_to_read_second_chunk_proof},
											{recall_byte2, RecallByte2},
											{mining_address, ar_util:encode(MiningAddress)}]),
									ar:console("WARNING: we found a solution but failed to read "
											"the proof for the second chunk. See logs for more "
											"details.~n");
								_ ->
									ar_coordination:computed_h2(
										Candidate#mining_candidate{ poa2 = PoA2 })
							end
					end;
				false ->
					ok
			end;
		false ->
			ok
	end,
	{noreply, State};

handle_task({may_be_remove_chunk_from_cache, {Nonce, Candidate}}, State) ->
	case is_session_valid(State#state.session#mining_session.ref, Candidate) of
		true ->
			#state{ session = Session } = State,
			#mining_session{ chunk_cache = Map } = Session,
			Map2 = evict_chunk_cache(Candidate#mining_candidate{ nonce = Nonce }, Map),
			Session2 = Session#mining_session{ chunk_cache = Map2 },
			{noreply, State#state{ session = Session2 }};
		false ->
			{noreply, State}
	end.

add_task(TaskType, Candidate) ->
	gen_server:cast(?MODULE, {add_task, {TaskType, Candidate}}).

signal_cache_cleanup(Candidate) ->
	NonceMax = max(0, ((?RECALL_RANGE_SIZE) div ?DATA_CHUNK_SIZE - 1)),
	signal_cache_cleanup(0, NonceMax, Candidate).

signal_cache_cleanup(Nonce, NonceMax, _Candidate)
		when Nonce > NonceMax ->
	ok;
signal_cache_cleanup(Nonce, NonceMax, Candidate) ->
	signal_cache_cleanup(Nonce, Candidate),
	signal_cache_cleanup(Nonce + 1, NonceMax, Candidate).

signal_cache_cleanup(Nonce, Candidate) ->
	gen_server:cast(?MODULE, {may_be_remove_chunk_from_cache, {Nonce, Candidate}}).

%% @doc The chunk_cache stores either the first or second chunk for a given nonce. This is because
%% we process both the first and second recall ranges in parallel and don't know
%% which data will be available first. The function manages that shared cache slot by either
%% caching data if its the first to arrive, or "popping" data that was previously cached. The
%% caller is responsible for taking the appropriate action based on the return value.
%%
%% do_not_cache is a special value used to prevent unnecessary data from being cached once a
%% solution has been found for a given nonce.
cycle_chunk_cache(#mining_candidate{ cache_ref = CacheRef, nonce = Nonce }, Data, Cache)
		when CacheRef /= not_set ->
	case maps:take({CacheRef, Nonce}, Cache) of
		{do_not_cache, Cache2} ->
			ets:update_counter(?MODULE, chunk_cache_size, {2, -1}),
			prometheus_gauge:dec(mining_server_chunk_cache_size),
			{do_not_cache, Cache2};
		error ->
			Cache2 = maps:put({CacheRef, Nonce}, Data, Cache),
			{cached, Cache2};
		{CachedData, Cache2} ->
			ets:update_counter(?MODULE, chunk_cache_size, {2, -2}),
			prometheus_gauge:dec(mining_server_chunk_cache_size, 2),
			{CachedData, Cache2}
	end.

evict_chunk_cache(#mining_candidate{ cache_ref = CacheRef, nonce = Nonce }, Cache)
		when CacheRef /= not_set ->
	case maps:take({CacheRef, Nonce}, Cache) of
		{do_not_cache, Cache2} ->
			ets:update_counter(?MODULE, chunk_cache_size, {2, -1}),
			prometheus_gauge:dec(mining_server_chunk_cache_size),
			Cache2;
		{_, Cache2} ->
			ets:update_counter(?MODULE, chunk_cache_size, {2, -2}),
			prometheus_gauge:dec(mining_server_chunk_cache_size, 2),
			Cache2;
		error ->
			ets:update_counter(?MODULE, chunk_cache_size, {2, -1}),
			prometheus_gauge:dec(mining_server_chunk_cache_size),
			maps:put({CacheRef, Nonce}, do_not_cache, Cache)
	end.

cache_h1_list(_Candidate, [], Cache) ->
	Cache;
cache_h1_list(
		#mining_candidate{ cache_ref = CacheRef } = Candidate,
		[ {H1, Nonce} | H1List ], Cache) when CacheRef /= not_set ->
	Cache2 = maps:put({CacheRef, Nonce}, {chunk1, H1}, Cache),
	cache_h1_list(Candidate, H1List, Cache2).

generate_cache_ref(Candidate) ->
	#mining_candidate{
		partition_number = PartitionNumber, partition_number2 = PartitionNumber2,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	CacheRef = {PartitionNumber, PartitionNumber2, PartitionUpperBound, make_ref()},
	Candidate#mining_candidate{ cache_ref = CacheRef }.

prepare_and_post_solution(Candidate, State) ->
	Solution = prepare_solution(Candidate, State),
	post_solution(Solution, State).

prepare_solution(Candidate, State) ->
	case is_session_valid(State#state.session#mining_session.ref, Candidate) of
		true ->
			#mining_candidate{
				mining_address = MiningAddress, next_seed = NextSeed, nonce = Nonce,
				nonce_limiter_output = NonceLimiterOutput, partition_number = PartitionNumber,
				partition_upper_bound = PartitionUpperBound, poa2 = PoA2, preimage = Preimage,
				seed = Seed, start_interval_number = StartIntervalNumber, step_number = StepNumber
			} = Candidate,
			
			Solution = #mining_solution{
				mining_address = MiningAddress,
				next_seed = NextSeed,
				nonce = Nonce,
				nonce_limiter_output = NonceLimiterOutput,
				partition_number = PartitionNumber,
				partition_upper_bound = PartitionUpperBound,
				poa2 = PoA2,
				preimage = Preimage,
				seed = Seed,
				start_interval_number = StartIntervalNumber,
				step_number = StepNumber
			},
			prepare_solution(last_step_checkpoints, Candidate, Solution);
		false ->
			error
	end.
	
prepare_solution(last_step_checkpoints, Candidate, Solution) ->
	#mining_candidate{
		next_seed = NextSeed, start_interval_number = StartIntervalNumber,
		step_number = StepNumber } = Candidate,
	LastStepCheckpoints = ar_nonce_limiter:get_step_checkpoints(
			StepNumber, NextSeed, StartIntervalNumber),
	case LastStepCheckpoints of
		not_found ->
			error;
		_ ->
			prepare_solution(steps, Candidate, Solution#mining_solution{
				last_step_checkpoints = LastStepCheckpoints })
	end;

prepare_solution(steps, Candidate, Solution) ->
	#mining_candidate{ step_number = StepNumber } = Candidate,
	[{_, TipNonceLimiterInfo}] = ets:lookup(node_state, nonce_limiter_info),
	#nonce_limiter_info{ global_step_number = PrevStepNumber,
		next_seed = PrevNextSeed } = TipNonceLimiterInfo,
	case StepNumber > PrevStepNumber of
		true ->
			Steps = ar_nonce_limiter:get_steps(PrevStepNumber, StepNumber, PrevNextSeed),
			case Steps of
				not_found ->
					?LOG_WARNING([{event, found_solution_but_failed_to_find_checkpoints},
							{start_step_number, PrevStepNumber},
							{next_step_number, StepNumber},
							{next_seed, ar_util:encode(PrevNextSeed)}]),
					ar:console("WARNING: found a solution but failed to find checkpoints, "
							"start step number: ~B, end step number: ~B, next_seed: ~s.",
							[PrevStepNumber, StepNumber, PrevNextSeed]),
					error;
				_ ->
					prepare_solution(proofs, Candidate, Solution#mining_solution{ steps = Steps })
			end;
		false ->
			?LOG_WARNING([{event, found_solution_but_stale_step_number},
							{start_step_number, PrevStepNumber},
							{next_step_number, StepNumber},
							{next_seed, ar_util:encode(PrevNextSeed)}]),
			error
	end;

prepare_solution(proofs, Candidate, Solution) ->
	#mining_candidate{
		h0 = H0, h1 = H1, h2 = H2, nonce = Nonce, partition_number = PartitionNumber,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	{RecallByte1, RecallByte2} = get_recall_bytes(H0, PartitionNumber, Nonce,
			PartitionUpperBound),
	case { H1, H2 } of
		{not_set, not_set} ->
			error;
		{H1, not_set} ->
			prepare_solution(poa1, Candidate, Solution#mining_solution{
				solution_hash = H1, recall_byte1 = RecallByte1, poa2 = #poa{} });
		{_, H2} ->
			prepare_solution(poa2, Candidate, Solution#mining_solution{
				solution_hash = H2, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2 })
	end;

prepare_solution(poa1, Candidate, #mining_solution{ poa1 = not_set } = Solution ) ->
	#mining_solution{
		mining_address = MiningAddress, partition_number = PartitionNumber,
		recall_byte1 = RecallByte1 } = Solution,
	#mining_candidate{
		chunk1 = Chunk1, h0 = H0, nonce = Nonce,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	case read_poa(RecallByte1, Chunk1, MiningAddress) of
		error ->
			{RecallRange1Start, _RecallRange2Start} = ar_block:get_recall_range(H0,
					PartitionNumber, PartitionUpperBound),
			?LOG_WARNING([{event, mined_block_but_failed_to_read_chunk_proofs},
					{recall_byte1, RecallByte1},
					{recall_range_start1, RecallRange1Start},
					{nonce, Nonce},
					{partition, PartitionNumber},
					{mining_address, ar_util:encode(MiningAddress)}]),
			ar:console("WARNING: we have mined a block but failed to fetch "
					"the chunk proofs required for publishing it. "
					"Check logs for more details~n"),
			error;
		PoA1 ->
			Solution#mining_solution{ poa1 = PoA1 }
	end;
prepare_solution(poa2, Candidate, #mining_solution{ poa2 = not_set } = Solution) ->
	#mining_solution{ mining_address = MiningAddress, partition_number = PartitionNumber,
		recall_byte2 = RecallByte2 } = Solution,
	#mining_candidate{
		chunk2 = Chunk2, h0 = H0, nonce = Nonce,
		partition_upper_bound = PartitionUpperBound } = Candidate,
	case read_poa(RecallByte2, Chunk2, MiningAddress) of
		error ->
			{_RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
					PartitionNumber, PartitionUpperBound),
			?LOG_WARNING([{event, mined_block_but_failed_to_read_chunk_proofs},
					{recall_byte2, RecallByte2},
					{recall_range_start2, RecallRange2Start},
					{nonce, Nonce},
					{partition, PartitionNumber},
					{mining_address, ar_util:encode(MiningAddress)}]),
			ar:console("WARNING: we have mined a block but failed to fetch "
					"the chunk proofs required for publishing it. "
					"Check logs for more details~n"),
			error;
		PoA2 ->
			prepare_solution(poa1, Candidate, Solution#mining_solution{ poa2 = PoA2 })
	end;
prepare_solution(poa2, Candidate, #mining_solution{ poa1 = not_set } = Solution) ->
	prepare_solution(poa1, Candidate, Solution);
prepare_solution(_, _Candidate, Solution) ->
	Solution.

post_solution(error, _State) ->
	error;
post_solution(Solution, State) ->
	{ok, Config} = application:get_env(arweave, config),
	post_solution(Config#config.cm_exit_peer, Solution, State).
post_solution(not_set, Solution, State) ->
	#state{ diff = Diff } = State,
	#mining_solution{
		mining_address = MiningAddress, nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2,
		solution_hash = H, step_number = StepNumber } = Solution,
	case validate_solution(Solution, Diff) of
		error ->
			?LOG_WARNING([{event, failed_to_validate_solution},
					{partition, PartitionNumber},
					{step_number, StepNumber},
					{mining_address, ar_util:encode(MiningAddress)},
					{recall_byte1, RecallByte1},
					{recall_byte2, RecallByte2},
					{solution_h, ar_util:encode(H)},
					{nonce_limiter_output, ar_util:encode(NonceLimiterOutput)}]),
			ar:console("WARNING: we failed to validate our solution. Check logs for more "
					"details~n");
		false ->
			?LOG_WARNING([{event, found_invalid_solution},
					{partition, PartitionNumber},
					{step_number, StepNumber},
					{mining_address, ar_util:encode(MiningAddress)},
					{recall_byte1, RecallByte1},
					{recall_byte2, RecallByte2},
					{solution_h, ar_util:encode(H)},
					{nonce_limiter_output, ar_util:encode(NonceLimiterOutput)}]),
			ar:console("WARNING: the solution we found is invalid. Check logs for more "
					"details~n");
		true ->
			ar_events:send(miner, {found_solution, Solution})
	end;
post_solution(ExitPeer, Solution, _State) ->
	case ar_http_iface_client:cm_publish_send(ExitPeer, Solution) of
		{ok, _} ->
			ok;
		{error, Reason} ->
			?LOG_WARNING([{event, found_solution_but_failed_to_reach_exit_node},
					{reason, io_lib:format("~p", [Reason])}]),
			ar:console("We found a solution but failed to reach the exit node, "
					"error: ~p.", [io_lib:format("~p", [Reason])])
	end.


read_poa(RecallByte, Chunk, MiningAddress) ->
	PoA = read_poa(RecallByte, MiningAddress),
	case {Chunk, PoA} of
		{_, error} -> error;
		{not_set, _} -> PoA;
		{Chunk, #poa{ chunk = Chunk }} -> PoA;
		_ -> error
	end.
read_poa(RecallByte, MiningAddress) ->
	Options = #{ pack => true, packing => {spora_2_6, MiningAddress} },
	case ar_data_sync:get_chunk(RecallByte + 1, Options) of
		{ok, #{ chunk := Chunk, tx_path := TXPath, data_path := DataPath }} ->
			#poa{ option = 1, chunk = Chunk, tx_path = TXPath, data_path = DataPath };
		_ ->
			error
	end.

validate_solution(Solution, Diff) ->
	#mining_solution{
		mining_address = MiningAddress, nonce = Nonce, nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber, partition_upper_bound = PartitionUpperBound,
		poa1 = PoA1, poa2 = PoA2, recall_byte1 = RecallByte1, recall_byte2 = RecallByte2,
		seed = Seed } = Solution,
	H0 = ar_block:compute_h0(NonceLimiterOutput, PartitionNumber, Seed, MiningAddress),
	{H1, _Preimage1} = ar_block:compute_h1(H0, Nonce, PoA1#poa.chunk),
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	case binary:decode_unsigned(H1, big) > Diff of
		true ->
			%% validates RecallByte1
			RecallByte1 = RecallRange1Start + Nonce * ?DATA_CHUNK_SIZE, 
			{BlockStart1, BlockEnd1, TXRoot1} = ar_block_index:get_block_bounds(RecallByte1),
			BlockSize1 = BlockEnd1 - BlockStart1,
			ar_poa:validate({BlockStart1, RecallByte1, TXRoot1, BlockSize1, PoA1,
					?STRICT_DATA_SPLIT_THRESHOLD, {spora_2_6, MiningAddress}});
		false ->
			{H2, _Preimage2} = ar_block:compute_h2(H1, PoA2#poa.chunk, H0),
			case binary:decode_unsigned(H2, big) > Diff of
				false ->
					false;
				true ->
					%% validates RecallByte2
					RecallByte2 = RecallRange2Start + Nonce * ?DATA_CHUNK_SIZE, 
					{BlockStart2, BlockEnd2, TXRoot2} = ar_block_index:get_block_bounds(
							RecallByte2),
					BlockSize2 = BlockEnd2 - BlockStart2,
					ar_poa:validate({BlockStart2, RecallByte2, TXRoot2, BlockSize2, PoA2,
							?STRICT_DATA_SPLIT_THRESHOLD, {spora_2_6, MiningAddress}})
			end
	end.

get_difficulty(State, #mining_candidate{ cm_diff = not_set }) ->
	State#state.diff;
get_difficulty(_State, #mining_candidate{ cm_diff = Diff }) ->
	Diff.

get_recall_bytes(H0, PartitionNumber, Nonce, PartitionUpperBound) ->
	{RecallRange1Start, RecallRange2Start} = ar_block:get_recall_range(H0,
			PartitionNumber, PartitionUpperBound),
	RelativeOffset = Nonce * (?DATA_CHUNK_SIZE),
	{RecallRange1Start + RelativeOffset, RecallRange2Start + RelativeOffset}.

pick_hashing_thread(Threads) ->
	{{value, Thread}, Threads2} = queue:out(Threads),
	{Thread, queue:in(Thread, Threads2)}.

optimal_performance(_StoreID, undefined) ->
	undefined;
optimal_performance("default", _VdfSpeed) ->
	undefined;
optimal_performance(StoreID, VdfSpeed) ->
	{PartitionSize, PartitionIndex, _Packing} = ar_storage_module:get_by_id(StoreID),
	case prometheus_gauge:value(v2_index_data_size_by_packing, [StoreID, spora_2_6,
			PartitionSize, PartitionIndex]) of
		undefined -> 0.0;
		StorageSize -> (200 / VdfSpeed) * (StorageSize / PartitionSize)
	end.

vdf_speed(Now) ->
	case ets:lookup(?MODULE, {performance, nonce_limiter}) of
		[] ->
			undefined;
		[{_, Now, _}] ->
			undefined;
		[{_, _Now, 0}] ->
			undefined;
		[{_, VdfStart, VdfCount}] ->
			ets:update_counter(?MODULE,
							{performance, nonce_limiter},
							[{2, -1, Now, Now}, {3, 0, -1, 0}]),
			VdfLapse = (Now - VdfStart) / 1000,
			VdfLapse / VdfCount
	end.

reset_mining_session(State) ->
	#state{ hashing_threads = HashingThreads } = State,
	Ref = make_ref(),
	[Thread ! {new_mining_session, Ref} || Thread <- queue:to_list(HashingThreads)],
	ar_mining_io:reset(Ref),
	CacheSizeLimit = get_chunk_cache_size_limit(),
	ets:insert(?MODULE, {chunk_cache_size, 0}),
	prometheus_gauge:set(mining_server_chunk_cache_size, 0),
	ar_coordination:reset_mining_session(),
	#mining_session{ ref = Ref, chunk_cache_size_limit = CacheSizeLimit }.

%%%===================================================================
%%% Tests.
%%%===================================================================
