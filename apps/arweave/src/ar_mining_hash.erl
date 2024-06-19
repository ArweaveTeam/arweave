-module(ar_mining_hash).

-behaviour(gen_server).

-export([start_link/0, compute_h0/2, compute_h1/2, compute_h2/2,
		garbage_collect/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	hashing_threads				= queue:new(),
  	hashing_thread_monitor_refs = #{}
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

garbage_collect() ->
	gen_server:cast(?MODULE, garbage_collect).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, Config} = application:get_env(arweave, config),
	State = lists:foldl(
		fun(_, Acc) -> start_hashing_thread(Acc) end,
		#state{},
		lists:seq(1, Config#config.hashing_threads)
	),
	{ok, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({compute, HashType, Worker, Candidate},
		#state{ hashing_threads = Threads } = State) ->
	{Thread, Threads2} = pick_hashing_thread(Threads),
	Thread ! {compute, HashType, Worker, Candidate},
	{noreply, State#state{ hashing_threads = Threads2 }};

handle_cast(garbage_collect, State) ->
	erlang:garbage_collect(self(),
		[{async, {ar_mining_hash, self(), erlang:monotonic_time()}}]),
	queue:fold(
		fun(Thread, _) ->
			erlang:garbage_collect(Thread,
				[{async, {ar_mining_hash_worker, Thread, erlang:monotonic_time()}}])
		end,
		ok,
		State#state.hashing_threads
	),
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
	[{_, RandomXStateRef}] = ets:lookup(ar_packing_server, randomx_packing_state),
	Thread = spawn_link(
		fun() ->
			hashing_thread(RandomXStateRef)
		end
	),
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

hashing_thread(RandomXStateRef) ->
	receive
		{compute, h0, Worker, Candidate} ->
			#mining_candidate{
				mining_address = MiningAddress, nonce_limiter_output = Output,
				partition_number = PartitionNumber, seed = Seed,
				packing_difficulty = PackingDifficulty } = Candidate,
			H0 = ar_block:compute_h0(Output, PartitionNumber, Seed, MiningAddress,
					PackingDifficulty, RandomXStateRef),
			ar_mining_worker:computed_hash(Worker, computed_h0, H0, undefined, Candidate),
			hashing_thread(RandomXStateRef);
		{compute, h1, Worker, Candidate} ->
			#mining_candidate{ h0 = H0, nonce = Nonce, chunk1 = Chunk1 } = Candidate,
			{H1, Preimage} = ar_block:compute_h1(H0, Nonce, Chunk1),
			ar_mining_worker:computed_hash(Worker, computed_h1, H1, Preimage, Candidate),
			hashing_thread(RandomXStateRef);
		{compute, h2, Worker, Candidate} ->
			#mining_candidate{ h0 = H0, h1 = H1, chunk2 = Chunk2 } = Candidate,
			{H2, Preimage} = ar_block:compute_h2(H1, Chunk2, H0),
			ar_mining_worker:computed_hash(Worker, computed_h2, H2, Preimage, Candidate),
			hashing_thread(RandomXStateRef)
	end.

pick_hashing_thread(Threads) ->
	{{value, Thread}, Threads2} = queue:out(Threads),
	{Thread, queue:in(Thread, Threads2)}.
