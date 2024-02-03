-module(ar_mining_hash).

-behaviour(gen_server).

-export([start_link/0, compute_h0/2, compute_h1/2, compute_h2/2, set_cache_limit/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SAMPLE_PROCESS_INTERVAL, 1000).

-record(state, {
	hashing_threads				= queue:new(),
  	hashing_thread_monitor_refs = #{},
	chunks_seen = 0,
	chunk_cache_limit = infinity
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

compute_h0(Worker, Candidate) ->
	gen_server:cast(?MODULE, {compute, h0, Worker, Candidate}).

compute_h1(Worker, Candidate) ->
	gen_server:cast(?MODULE, {compute, h1, Worker, Candidate}).

compute_h2(Worker, Candidate) ->
	gen_server:cast(?MODULE, {compute, h2, Worker, Candidate}).

set_cache_limit(CacheLimit) ->
	gen_server:cast(?MODULE, {set_cache_limit, CacheLimit}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, Config} = application:get_env(arweave, config),
	State = lists:foldl(
		fun(_, Acc) -> start_hashing_thread(Acc) end,
		#state{},
		lists:seq(1, Config#config.hashing_threads)
	),
	% ar_util:cast_after(?SAMPLE_PROCESS_INTERVAL, ?MODULE, sample_process),
	{ok, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({set_cache_limit, CacheLimit}, State) ->
	{noreply, State#state{ chunk_cache_limit = CacheLimit }};

handle_cast(sample_process, State) ->
	[{binary, BinInfoBefore}] = process_info(self(), [binary]),
	?LOG_DEBUG([{event, mining_hash_process_sample},{pid, self()}, {b, length(BinInfoBefore)},
		{binary_before, BinInfoBefore}]),
	queue:fold(
		fun(Thread, _) ->
			[{binary, BinInfoBefore2}] = process_info(Thread, [binary]),
			?LOG_DEBUG([{event, mining_hash_thread_sample}, {thread, Thread}, {b, length(BinInfoBefore2)},
				{binary_before, BinInfoBefore2}])
		end,
		ok,
		State#state.hashing_threads
	),
	ar_util:cast_after(?SAMPLE_PROCESS_INTERVAL, ?MODULE, sample_process),
	{noreply, State};

handle_cast({compute, HashType, Worker, Candidate},
		#state{ hashing_threads = Threads } = State) ->
	{Thread, Threads2} = pick_hashing_thread(Threads),
	Thread ! {compute, HashType, Worker, Candidate},
	State2 = check_garbage_collection(State),
	{noreply, State2#state{ hashing_threads = Threads2 }};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({'DOWN', Ref, process, _, Reason},
		#state{ hashing_thread_monitor_refs = HashingThreadRefs } = State) ->
	case maps:is_key(Ref, HashingThreadRefs) of
		true ->
			{noreply, handle_hashing_thread_down(Ref, Reason, State)};
		_ ->
			{noreply, State}
	end;

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

start_hashing_thread(State) ->
	#state{ hashing_threads = Threads, hashing_thread_monitor_refs = Refs } = State,
	Thread = spawn_link(fun hashing_thread/0),
	Ref = monitor(process, Thread),
	Threads2 = queue:in(Thread, Threads),
	Refs2 = maps:put(Ref, Thread, Refs),
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


hashing_thread() ->
	receive
		{compute, h0, Worker, Candidate} ->
			#mining_candidate{
				mining_address = MiningAddress, nonce_limiter_output = Output,
				partition_number = PartitionNumber, seed = Seed } = Candidate,
			H0 = ar_block:compute_h0(Output, PartitionNumber, Seed, MiningAddress),
			ar_mining_worker:computed_hash(Worker, computed_h0, H0, undefined, Candidate),
			hashing_thread();
		{compute, h1, Worker, Candidate} ->
			#mining_candidate{ h0 = H0, nonce = Nonce, chunk1 = Chunk1 } = Candidate,
			{H1, Preimage} = ar_block:compute_h1(H0, Nonce, Chunk1),
			ar_mining_worker:computed_hash(Worker, computed_h1, H1, Preimage, Candidate),
			hashing_thread();
		{compute, h2, Worker, Candidate} ->
			#mining_candidate{ h0 = H0, h1 = H1, chunk2 = Chunk2 } = Candidate,
			{H2, Preimage} = ar_block:compute_h2(H1, Chunk2, H0),
			ar_mining_worker:computed_hash(Worker, computed_h2, H2, Preimage, Candidate),
			hashing_thread()
	end.

pick_hashing_thread(Threads) ->
	{{value, Thread}, Threads2} = queue:out(Threads),
	{Thread, queue:in(Thread, Threads2)}.

check_garbage_collection(State) ->
	%% Every time ar_mining_hash routes a hash task to one of the hashing threads, it retains
	%% a reference to the chunk - which, as a sub-binary of the full ?RECALL_RANGE_SIZE binary -
	%% is actually a a reference to the full binary. Furthermore, since ar_mining_hash only
	%% does lightweight routing, its heap does't grow fast nor does it rack up many reductions.
	%% This means that the automatic garbage collection will not be triggered often. 
	%% Instead we'll count each chunk that it routes and manually trigger the garbage collector
	%% once its routed mining_server_chunk_cache_limit worth of chunks.
	ChunksSeen = State#state.chunks_seen + 1,
	ChunksSeen2 = case ChunksSeen > State#state.chunk_cache_limit of
		true   ->
			StartTime = erlang:monotonic_time(),
			garbage_collect(self()),
			queue:fold(
				fun(Thread, _) ->
					garbage_collect(Thread)
				end,
				ok,
				State#state.hashing_threads
			),
			EndTime = erlang:monotonic_time(),
			ElapsedTime = erlang:convert_time_unit(EndTime-StartTime, native, millisecond),
			?LOG_DEBUG([
				{event, mining_debug_hash_gc_limit_reached}, {chunks_seen, ChunksSeen},
				{gc_time, ElapsedTime}]),
			0;
		false ->
			ChunksSeen
	end,
	State#state{ chunks_seen = ChunksSeen2 }.