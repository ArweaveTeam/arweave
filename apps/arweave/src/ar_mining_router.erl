-module(ar_mining_router).

-behaviour(gen_server).

-export([start_link/0,
	prepare_solution/1, route_solution/1,
	found_solution/2, received_solution/2,
	reject_solution/3, reject_solution/4,
	accept_solution/1, accept_solution/2,
	accept_block_solution/2, accept_block_solution/3,
	route_h1/2, route_h2/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-record(state, {
	request_pid_by_ref = maps:new()
}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
	
prepare_solution(#mining_candidate{} = Candidate) ->
	%% A pool client does not validate VDF before sharing a solution.
	{ok, Config} = application:get_env(arweave, config),
	ar_mining_server:prepare_solution(Candidate, Config#config.is_pool_client).

route_solution(#mining_solution{} = Solution) ->
	{ok, Config} = application:get_env(arweave, config),
	%% XXX: handle timeout
	gen_server:call(?MODULE, {route_solution, Config, Solution}).

found_solution(#mining_candidate{} = Candidate, ExtraLogs) ->
	#mining_candidate{
		mining_address = MiningAddress,
		nonce_limiter_output = NonceLimiterOutput,
		seed = Seed, next_seed = NextSeed, 
		start_interval_number = StartIntervalNumber, step_number = StepNumber } = Candidate,

	{Hash, PartitionNumber} = select_hash(Candidate),
	?LOG_INFO([
		{event, solution_lifecycle},
		{status, found},
		{hash, ar_util:safe_encode(Hash)},
		{mining_address, ar_util:safe_encode(MiningAddress)},
		{partition, PartitionNumber},
		{seed, Seed},
		{next_seed, NextSeed},
		{start_interval_number, StartIntervalNumber},
		{step_number, StepNumber},
		{nonce_limiter_output, ar_util:safe_encode(NonceLimiterOutput)}] ++
		ExtraLogs),		
	ar_mining_stats:solution(found).

received_solution(#mining_candidate{} = Candidate, ExtraLogs) ->
	#mining_candidate{
		mining_address = MiningAddress,
		nonce_limiter_output = NonceLimiterOutput,
		seed = Seed, next_seed = NextSeed, 
		start_interval_number = StartIntervalNumber, step_number = StepNumber } = Candidate,

	{Hash, PartitionNumber} = select_hash(Candidate),
	received_solution(Hash, MiningAddress, PartitionNumber, Seed, NextSeed,
		StartIntervalNumber, StepNumber, NonceLimiterOutput, ExtraLogs);

received_solution(#mining_solution{} = Solution, ExtraLogs) ->
	#mining_solution{
		mining_address = MiningAddress,
		nonce_limiter_output = NonceLimiterOutput, partition_number = PartitionNumber,
		solution_hash = H, seed = Seed, next_seed = NextSeed, 
		start_interval_number = StartIntervalNumber, step_number = StepNumber } = Solution,
	received_solution(H, MiningAddress, PartitionNumber, Seed, NextSeed, StartIntervalNumber,
		StepNumber, NonceLimiterOutput, ExtraLogs).
	
reject_solution(#mining_candidate{} = Candidate, Reason, ExtraLogs) ->
	#mining_candidate{
		mining_address = MiningAddress,
		nonce_limiter_output = NonceLimiterOutput,
		seed = Seed, next_seed = NextSeed, 
		start_interval_number = StartIntervalNumber, step_number = StepNumber } = Candidate,

	{Hash, PartitionNumber} = select_hash(Candidate),
	reject_solution(Reason, Hash, MiningAddress, PartitionNumber, Seed, NextSeed,
		StartIntervalNumber, StepNumber, NonceLimiterOutput, ExtraLogs);

reject_solution(#mining_solution{} = Solution, Reason, ExtraLogs) ->
	#mining_solution{
		mining_address = MiningAddress,
		nonce_limiter_output = NonceLimiterOutput, partition_number = PartitionNumber,
		solution_hash = H, seed = Seed, next_seed = NextSeed, 
		start_interval_number = StartIntervalNumber, step_number = StepNumber } = Solution,
	reject_solution(Reason, H, MiningAddress, PartitionNumber,
		Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput, ExtraLogs).

reject_solution(#mining_solution{} = Solution, Reason, ExtraLogs, Ref) ->
	reject_solution(Solution, Reason, ExtraLogs),
	Status = iolist_to_binary([<<"rejected_">>, atom_to_binary(Reason)]),
	gen_server:cast(?MODULE, {solution_response, Status, <<>>, Ref}).

accept_solution(#mining_solution{} = Solution) ->
	#mining_solution{ mining_address = MiningAddress, solution_hash = H } = Solution,
	?LOG_WARNING([
		{event, solution_lifecycle},
		{status, accepted},
		{hash, ar_util:safe_encode(H)},
		{mining_address, ar_util:safe_encode(MiningAddress)}]),		
	ar_mining_stats:solution(accepted).

accept_solution(#mining_solution{} = Solution, Ref) ->
	accept_solution(Solution),
	gen_server:cast(?MODULE, {solution_response, <<"accepted">>, <<>>, Ref}).

accept_block_solution(#mining_solution{} = Solution, BlockH) ->
	#mining_solution{ mining_address = MiningAddress, solution_hash = H } = Solution,
	?LOG_WARNING([
		{event, solution_lifecycle},
		{status, accepted},
		{hash, ar_util:safe_encode(H)},
		{mining_address, ar_util:safe_encode(MiningAddress)},
		{block_hash, ar_util:safe_encode(BlockH)}]),		
	ar_mining_stats:solution(accepted).

accept_block_solution(#mining_solution{} = Solution, BlockH, Ref) ->
	accept_block_solution(Solution, BlockH),
	gen_server:cast(?MODULE, {solution_response, <<"accepted_block">>, BlockH, Ref}).

route_h1(#mining_candidate{} = Candidate, DiffPair) ->
	{ok, Config} = application:get_env(arweave, config),
	case Config#config.coordinated_mining of
		false ->
			ok;
		true ->
			ar_coordination:computed_h1(Candidate, DiffPair)
	end.

route_h2(#mining_candidate{ cm_lead_peer = not_set } = Candidate) ->
	prepare_solution(Candidate);
route_h2(#mining_candidate{} = Candidate) ->
	ar_coordination:computed_h2_for_peer(Candidate).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, #state{}}.

handle_call({route_solution, Config, Solution}, From, State) ->
	#state{ request_pid_by_ref = Map } = State,
	Ref = make_ref(),
	case route_solution(Config, Solution, Ref) of
		noreply ->
			{noreply, State#state{ request_pid_by_ref = maps:put(Ref, From, Map) }};
		Reply ->
			{reply, Reply, State}
	end;

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({solution_response, Status, BlockH, {pool, Ref}}, State) ->
	#state{ request_pid_by_ref = Map } = State,
	PID = maps:get(Ref, Map),
	gen_server:reply(PID,
			#solution_response{ indep_hash = BlockH, status = Status }),
	{noreply, State#state{ request_pid_by_ref = maps:remove(Ref, Map) }};
	
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

received_solution(Hash, MiningAddress, PartitionNumber, Seed, NextSeed,
		StartIntervalNumber, StepNumber, NonceLimiterOutput, ExtraLogs) ->
	?LOG_INFO([
		{event, solution_lifecycle},
		{status, received},
		{hash, ar_util:safe_encode(Hash)},
		{mining_address, ar_util:safe_encode(MiningAddress)},
		{partition, PartitionNumber},
		{seed, Seed},
		{next_seed, NextSeed},
		{start_interval_number, StartIntervalNumber},
		{step_number, StepNumber},
		{nonce_limiter_output, ar_util:safe_encode(NonceLimiterOutput)}] ++
		ExtraLogs),
	ar_mining_stats:solution(received).

reject_solution(Reason, H, MiningAddress, PartitionNumber,
		Seed, NextSeed, StartIntervalNumber, StepNumber, NonceLimiterOutput, ExtraLogs) ->
	ar:console("WARNING: solution was rejected. Check logs for more details~n"),
	?LOG_WARNING([
		{event, solution_lifecycle},
		{status, rejected},
		{reason, Reason},
		{hash, ar_util:safe_encode(H)},
		{mining_address, ar_util:safe_encode(MiningAddress)},
		{partition, PartitionNumber},
		{seed, Seed},
		{next_seed, NextSeed},
		{start_interval_number, StartIntervalNumber},
		{step_number, StepNumber},
		{nonce_limiter_output, ar_util:safe_encode(NonceLimiterOutput)}] ++
		ExtraLogs),		
	ar_mining_stats:solution(rejected).

route_solution(#config{ is_pool_server = true }, Solution, Ref) ->
	ar_pool:process_partial_solution(Solution, Ref);
route_solution(#config{ cm_exit_peer = not_set, is_pool_client = true }, Solution, Ref) ->
	%% When posting a partial solution the pool client will skip many of the validation steps
	%% that are normally performed before sharing a solution.
	ar_pool:post_partial_solution(Solution);
route_solution(#config{ cm_exit_peer = not_set, is_pool_client = false }, Solution, Ref) ->
	ar_mining_server:validate_solution(Solution);
route_solution(#config{ cm_exit_peer = ExitPeer, is_pool_client = true }, Solution, Ref) ->
	case ar_http_iface_client:post_partial_solution(ExitPeer, Solution) of
		{ok, _} ->
			ok;
		{error, Reason} ->
			?LOG_WARNING([{event, found_partial_solution_but_failed_to_reach_exit_node},
					{reason, io_lib:format("~p", [Reason])}]),
			ar:console("We found a partial solution but failed to reach the exit node, "
					"error: ~p.", [io_lib:format("~p", [Reason])])
	end;
route_solution(#config{ cm_exit_peer = ExitPeer, is_pool_client = false }, Solution, Ref) ->
	case ar_http_iface_client:cm_publish_send(ExitPeer, Solution) of
		{ok, _} ->
			ok;
		{error, Reason} ->
			?LOG_WARNING([{event, solution_rejected},
					{reason, failed_to_reach_exit_node},
					{message, io_lib:format("~p", [Reason])}]),
			ar:console("We found a solution but failed to reach the exit node, "
					"error: ~p.", [io_lib:format("~p", [Reason])]),
			ar_mining_stats:solution(rejected)
	end.

select_hash(#mining_candidate{ h2 = not_set } = Candidate) ->
	{Candidate#mining_candidate.h1, Candidate#mining_candidate.partition_number};
select_hash(#mining_candidate{} = Candidate) ->
	{Candidate#mining_candidate.h2, Candidate#mining_candidate.partition_number2}.
