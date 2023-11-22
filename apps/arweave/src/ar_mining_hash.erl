-module(ar_mining_hash).

-behaviour(gen_server).

-export([start_link/0, compute_h0/1, compute_h1/1, compute_h2/1]).

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

compute_h0(Candidate) ->
	gen_server:cast(?MODULE, {compute, h0, Candidate}).

compute_h1(Candidate) ->
	gen_server:cast(?MODULE, {compute, h1, Candidate}).

compute_h2(Candidate) ->
	gen_server:cast(?MODULE, {compute, h2, Candidate}).

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
	{ok, State}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({compute, HashType, Candidate},
		#state{ hashing_threads = Threads } = State) ->
	{Thread, Threads2} = pick_hashing_thread(Threads),
	Thread ! {compute, HashType, Candidate},
	{noreply, State#state{ hashing_threads = Threads2 }};

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

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
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
		{compute, h0, Candidate} ->
			#mining_candidate{
				mining_address = MiningAddress, nonce_limiter_output = Output,
				partition_number = PartitionNumber, seed = Seed } = Candidate,
			H0 = ar_block:compute_h0(Output, PartitionNumber, Seed, MiningAddress),
			ar_mining_server:computed_hash(computed_h0, H0, undefined, Candidate),
			hashing_thread();
		{compute, h1, Candidate} ->
			#mining_candidate{ h0 = H0, nonce = Nonce, chunk1 = Chunk1 } = Candidate,
			{H1, Preimage} = ar_block:compute_h1(H0, Nonce, Chunk1),
			ar_mining_server:computed_hash(computed_h1, H1, Preimage, Candidate),
			hashing_thread();
		{compute, h2, Candidate} ->
			#mining_candidate{ h0 = H0, h1 = H1, chunk2 = Chunk2 } = Candidate,
			{H2, Preimage} = ar_block:compute_h2(H1, Chunk2, H0),
			ar_mining_server:computed_hash(computed_h2, H2, Preimage, Candidate),
			hashing_thread()
	end.


pick_hashing_thread(Threads) ->
	{{value, Thread}, Threads2} = queue:out(Threads),
	{Thread, queue:in(Thread, Threads2)}.
