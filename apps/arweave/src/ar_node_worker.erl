%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%

%%% @doc The server responsible for processing blocks and transactions and
%%% maintaining the node state. Blocks are prioritized over transactions.
-module(ar_node_worker).

-export([start_link/0, calculate_delay/1, is_mempool_or_block_cache_tx/1,
		tx_id_prefix/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-export([set_reward_addr/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-ifdef(DEBUG).
-define(PROCESS_TASK_QUEUE_FREQUENCY_MS, 10).
-else.
-define(PROCESS_TASK_QUEUE_FREQUENCY_MS, 200).
-endif.

-define(FILTER_MEMPOOL_CHUNK_SIZE, 100).

-ifdef(DEBUG).
-define(BLOCK_INDEX_HEAD_LEN, (?STORE_BLOCKS_BEHIND_CURRENT * 2)).
-else.
-define(BLOCK_INDEX_HEAD_LEN, 10000).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return the prefix used to inform block receivers about the block's transactions
%% via POST /block_announcement.
tx_id_prefix(TXID) ->
	binary:part(TXID, 0, 8).

%% @doc Return true if the given transaction identifier is found in the mempool or
%% block cache (the last ?STORE_BLOCKS_BEHIND_CURRENT blocks).
is_mempool_or_block_cache_tx(TXID) ->
	ets:match_object(tx_prefixes, {tx_id_prefix(TXID), TXID}) /= [].

set_reward_addr(Addr) ->
	gen_server:call(?MODULE, {set_reward_addr, Addr}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	[ok, ok, ok, ok] = ar_events:subscribe([tx, block, nonce_limiter, miner]),
	%% Initialize RandomX.
	ar_randomx_state:start(),
	ar_randomx_state:start_block_polling(),
	%% Read persisted mempool.
	ar_mempool:load_from_disk(),
	%% Join the network.
	{ok, Config} = application:get_env(arweave, config),
	case Config#config.start_from_block_index of
		false ->
			validate_trusted_peers(Config);
		_ ->
			ok
	end,
	StateLookup =
		case {Config#config.start_from_block_index, Config#config.init} of
			{false, false} ->
				not_joined;
			{true, _} ->
				case ar_storage:read_block_index_and_reward_history() of
					{error, enoent} ->
						io:format(
							"~n~n\tBlock index file is not found. "
							"If you want to start from a block index copied "
							"from another node, place it in "
							"<data_dir>/hash_lists/last_block_index_and_reward_history.bin~n~n"
						),
						timer:sleep(1000),
						erlang:halt();
					Result ->
						Result
				end;
			{false, true} ->
				Config2 = Config#config{ init = false },
				application:set_env(arweave, config, Config2),
				ar_weave:init([], ar_retarget:switch_to_linear_diff(Config#config.diff))
		end,
	case {StateLookup, Config#config.auto_join} of
		{not_joined, true} ->
			ar_join:start(ar_peers:get_trusted_peers());
		{{BI, RewardHistory}, true} ->
			start_from_block_index(BI, RewardHistory);
		{_, false} ->
			do_nothing
	end,
	%% Add pending transactions from the persisted mempool to the propagation queue.
	gb_sets:filter(
		fun ({_Utility, _TXID, ready_for_mining}) ->
				false;
			({_Utility, TXID, waiting}) ->
				start_tx_mining_timer(ar_mempool:get_tx(TXID)),
				true
		end,
		ar_mempool:get_priority_set()
	),
	%% May be start mining.
	case Config#config.mine of
		true ->
			gen_server:cast(?MODULE, automine);
		_ ->
			ok
	end,
	gen_server:cast(?MODULE, process_task_queue),
	ets:insert(node_state, [
		{is_joined,						false},
		{hash_list_2_0_for_1_0_blocks,	read_hash_list_2_0_for_1_0_blocks()}
	]),
	%% Start the HTTP server.
	ok = ar_http_iface_server:start(),
	gen_server:cast(?MODULE, refresh_timestamp),
	{ok, #{
		miner_2_5 => undefined,
		miner_2_6 => undefined,
		io_threads => [],
		automine => false,
		tags => [],
		blocks_missing_txs => sets:new(),
		missing_txs_lookup_processes => #{},
		task_queue => gb_sets:new()
	}}.

validate_trusted_peers(#config{ peers = [] }) ->
	ok;
validate_trusted_peers(Config) ->
	Peers = Config#config.peers,
	ValidPeers = filter_valid_peers(Peers),
	case ValidPeers of
		[] ->
			erlang:halt();
		_ ->
			application:set_env(arweave, config, Config#config{ peers = ValidPeers }),
			case lists:member(time_syncing, Config#config.disable) of
				false ->
					validate_clock_sync(ValidPeers);
				true ->
					ok
			end
	end.

%% @doc Verify peers are on the same network as us.
filter_valid_peers(Peers) ->
	lists:filter(
		fun(Peer) ->
			case ar_http_iface_client:get_info(Peer, name) of
				info_unavailable ->
					io:format("~n\tPeer ~s is not available.~n~n",
							[ar_util:format_peer(Peer)]),
					false;
				<<?NETWORK_NAME>> ->
					true;
				_ ->
					io:format(
						"~n\tPeer ~s does not belong to the network ~s.~n~n",
						[ar_util:format_peer(Peer), ?NETWORK_NAME]
					),
					false
			end
		end,
		Peers
	).

%% @doc Validate our clocks are in sync with the trusted peers' clocks.
validate_clock_sync(Peers) ->
	ValidatePeerClock = fun(Peer) ->
		case ar_http_iface_client:get_time(Peer, 5 * 1000) of
			{ok, {RemoteTMin, RemoteTMax}} ->
				LocalT = os:system_time(second),
				Tolerance = ?JOIN_CLOCK_TOLERANCE,
				case LocalT of
					T when T < RemoteTMin - Tolerance ->
						log_peer_clock_diff(Peer, RemoteTMin - Tolerance - T),
						false;
					T when T < RemoteTMin - Tolerance div 2 ->
						log_peer_clock_diff(Peer, RemoteTMin - T),
						true;
					T when T > RemoteTMax + Tolerance ->
						log_peer_clock_diff(Peer, T - RemoteTMax - Tolerance),
						false;
					T when T > RemoteTMax + Tolerance div 2 ->
						log_peer_clock_diff(Peer, T - RemoteTMax),
						true;
					_ ->
						true
				end;
			{error, Err} ->
				ar:console(
					"Failed to get time from peer ~s: ~p.",
					[ar_util:format_peer(Peer), Err]
				),
				false
		end
	end,
	Responses = ar_util:pmap(ValidatePeerClock, [P || P <- Peers, not is_pid(P)]),
	case lists:all(fun(R) -> R end, Responses) of
		true ->
			ok;
		false ->
			io:format(
				"~n\tInvalid peers. A valid peer must be part of the"
				" network ~s and its clock must deviate from ours by no"
				" more than ~B seconds.~n", [?NETWORK_NAME, ?JOIN_CLOCK_TOLERANCE]
			),
			timer:sleep(1000),
			erlang:halt()
	end.

log_peer_clock_diff(Peer, Diff) ->
	Warning = "Your local clock deviates from peer ~s by ~B seconds or more.",
	WarningArgs = [ar_util:format_peer(Peer), Diff],
	io:format(Warning, WarningArgs),
	?LOG_WARNING(Warning, WarningArgs).

start_tx_mining_timer(TX) ->
	%% Calling with ar_node_worker: allows to mock calculate_delay/1 in tests.
	erlang:send_after(ar_node_worker:calculate_delay(tx_propagated_size(TX)), ?MODULE,
			{tx_ready_for_mining, TX}).

tx_propagated_size(#tx{ format = 2 }) ->
	?TX_SIZE_BASE;
tx_propagated_size(#tx{ format = 1, data = Data }) ->
	?TX_SIZE_BASE + byte_size(Data).

%% @doc Return a delay in milliseconds to wait before including a transaction
%% into a block. The delay is computed as base delay + a function of data size with
%% a conservative estimation of the network speed.
calculate_delay(Bytes) ->
	BaseDelay = (?BASE_TX_PROPAGATION_DELAY) * 1000,
	NetworkDelay = Bytes * 8 div (?TX_PROPAGATION_BITS_PER_SECOND) * 1000,
	BaseDelay + NetworkDelay.

handle_call({set_reward_addr, Addr}, _From, State) ->
	{reply, ok, State#{ reward_addr => Addr }}.

handle_cast(process_task_queue, #{ task_queue := TaskQueue } = State) ->
	RunTask =
		case gb_sets:is_empty(TaskQueue) of
			true ->
				false;
			false ->
				case ets:lookup(node_state, is_joined) of
					[{_, true}] ->
						true;
					_ ->
						false
				end
		end,
	case RunTask of
		true ->
			record_metrics(),
			{{_Priority, Task}, TaskQueue2} = gb_sets:take_smallest(TaskQueue),
			gen_server:cast(self(), process_task_queue),
			handle_task(Task, State#{ task_queue => TaskQueue2 });
		false ->
			ar_util:cast_after(?PROCESS_TASK_QUEUE_FREQUENCY_MS, ?MODULE, process_task_queue),
			{noreply, State}
	end;

handle_cast(Message, #{ task_queue := TaskQueue } = State) ->
	Task = {priority(Message), Message},
	case gb_sets:is_element(Task, TaskQueue) of
		true ->
			{noreply, State};
		false ->
			{noreply, State#{ task_queue => gb_sets:insert(Task, TaskQueue) }}
	end.

handle_info({join, BI, Blocks}, State) ->
	{ok, _} = ar_wallets:start_link([{blocks, Blocks},
			{peers, ar_peers:get_trusted_peers()}]),
	ar_block_index:init(BI),
	Blocks2 = lists:sublist(Blocks, ?SEARCH_SPACE_UPPER_BOUND_DEPTH),
	Blocks3 = may_be_initialize_nonce_limiter(Blocks2, BI),
	Blocks4 =
		case length(Blocks) >= ?SEARCH_SPACE_UPPER_BOUND_DEPTH of
			true ->
				Blocks3 ++ lists:nthtail(?SEARCH_SPACE_UPPER_BOUND_DEPTH, Blocks);
			false ->
				Blocks3
		end,
	ets:insert(node_state, [
		{recent_block_index,	lists:sublist(BI, ?BLOCK_INDEX_HEAD_LEN)},
		{joined_blocks,			Blocks4}
	]),
	ar_events:send(node_state, initializing),
	{noreply, State};

handle_info(wallets_ready, State) ->
	[{_, Joined}] = ets:lookup(node_state, is_joined),
	ets:insert(node_state, {account_tree_initialized}),
	case ets:member(node_state, nonce_limiter_initialized) andalso not Joined of
		true ->
			handle_initialized(State);
		false ->
			ok
	end,
	{noreply, State};

handle_info({event, nonce_limiter, initialized}, State) ->
	[{_, Joined}] = ets:lookup(node_state, is_joined),
	ets:insert(node_state, {nonce_limiter_initialized}),
	case ets:member(node_state, account_tree_initialized) andalso not Joined of
		true ->
			handle_initialized(State);
		false ->
			ok
	end,
	{noreply, State};

handle_info({event, nonce_limiter, {invalid, H, Code}}, State) ->
	?LOG_WARNING([{event, received_block_with_invalid_nonce_limiter_chain},
			{block, ar_util:encode(H)}, {code, Code}]),
	ar_block_cache:remove(block_cache, H),
	ar_ignore_registry:add(H),
	gen_server:cast(?MODULE, apply_block),
	{noreply, State#{ nonce_limiter_validation_scheduled => false }};

handle_info({event, nonce_limiter, {valid, H}}, State) ->
	?LOG_INFO([{event, vdf_validation_successful}, {block, ar_util:encode(H)}]),
	ar_block_cache:mark_nonce_limiter_validated(block_cache, H),
	gen_server:cast(?MODULE, apply_block),
	{noreply, State#{ nonce_limiter_validation_scheduled => false }};

handle_info({event, nonce_limiter, {validation_error, H}}, State) ->
	?LOG_WARNING([{event, vdf_validation_error}, {block, ar_util:encode(H)}]),
	ar_block_cache:remove(block_cache, H),
	gen_server:cast(?MODULE, apply_block),
	{noreply, State#{ nonce_limiter_validation_scheduled => false }};

handle_info({event, nonce_limiter, _}, State) ->
	{noreply, State};

handle_info({event, miner, {found_solution, _Solution}},
		#{ automine := false, miner_2_6 := undefined } = State) ->
	{noreply, State};
handle_info({event, miner, {found_solution, Solution}}, State) ->
	#mining_solution{ 
		last_step_checkpoints = LastStepCheckpoints,
		mining_address = MiningAddress,
		next_seed = NonceLimiterNextSeed,
		nonce = Nonce,
		nonce_limiter_output = NonceLimiterOutput,
		partition_number = PartitionNumber,
		poa1 = PoA1,
		poa2 = PoA2,
		preimage = SolutionPreimage,
		recall_byte1 = RecallByte1,
		recall_byte2 = RecallByte2,
		solution_hash = SolutionH,
		start_interval_number = IntervalNumber,
		step_number = StepNumber,
		steps = SuppliedSteps
	} = Solution,

	[{_, PrevH}] = ets:lookup(node_state, current),
	[{_, PrevWeaveSize}] = ets:lookup(node_state, weave_size),
	[{_, PrevTimestamp}] = ets:lookup(node_state, timestamp),
	Now = os:system_time(second),
	MaxDeviation = ar_block:get_max_timestamp_deviation(),
	Timestamp =
		case Now < PrevTimestamp - MaxDeviation of
			true ->
				?LOG_WARNING([{event, clock_out_of_sync},
						{previous_block, ar_util:encode(PrevH)},
						{previous_block_timestamp, PrevTimestamp},
						{our_time, Now},
						{max_allowed_deviation, MaxDeviation}]),
				PrevTimestamp - MaxDeviation;
			false ->
				Now
		end,

	%% Load key
	RewardKey = case ar_wallet:load_key(MiningAddress) of
		not_found ->
			?LOG_WARNING([{event, mined_block_but_no_mining_key_found}, {node, node()},
					{mining_address, ar_util:encode(MiningAddress)}]),
			ar:console("WARNING. Can't find key ~w~n", [ar_util:encode(MiningAddress)]),
			not_found;
		Key ->
			Key
	end,
	PassesKeyCheck = RewardKey =/= not_found,

	%% Check solution difficulty
	Diff = get_current_diff(Timestamp),
	PassesDiffCheck = PassesKeyCheck andalso binary:decode_unsigned(SolutionH, big) > Diff,

	%% Check that solution is laster than the previous solution on the timeline
	[{_, TipNonceLimiterInfo}] = ets:lookup(node_state, nonce_limiter_info),
	NonceLimiterInfo = #nonce_limiter_info{ global_step_number = StepNumber,
			output = NonceLimiterOutput,
			prev_output = TipNonceLimiterInfo#nonce_limiter_info.output },
	PassesTimelineCheck = PassesDiffCheck andalso
			ar_nonce_limiter:is_ahead_on_the_timeline(NonceLimiterInfo, TipNonceLimiterInfo),

	%% Check solution seed
	#nonce_limiter_info{ next_seed = PrevNextSeed,
			global_step_number = PrevStepNumber } = TipNonceLimiterInfo,
	PrevIntervalNumber = PrevStepNumber div ?NONCE_LIMITER_RESET_FREQUENCY,
	PassesSeedCheck = PassesTimelineCheck andalso
			{IntervalNumber, NonceLimiterNextSeed} == {PrevIntervalNumber, PrevNextSeed},

	%% Check steps and step checkpoints
	HaveSteps =
		case PassesSeedCheck of
			false ->
				?LOG_INFO([{event, ignore_mining_solution}, {reason, accepted_another_block}]),
				false;
			true ->
				ar_nonce_limiter:get_steps(PrevStepNumber, StepNumber, PrevNextSeed)
		end,
	HaveSteps2 =
		case HaveSteps of
			not_found ->
				% TODO verify
				SuppliedSteps;
			_ ->
				HaveSteps
		end,

	%% Pack, build, and sign block
	case HaveSteps2 of
		false ->
			{noreply, State};
		not_found ->
			?LOG_WARNING([{event, did_not_find_steps_for_mined_block},
					{seed, ar_util:encode(PrevNextSeed)}, {prev_step_number, PrevStepNumber},
					{step_number, StepNumber}]),
			{noreply, State};
		[NonceLimiterOutput | _] = Steps ->
			{Seed, NextSeed, PartitionUpperBound, NextPartitionUpperBound}
					= ar_nonce_limiter:get_seed_data(StepNumber, TipNonceLimiterInfo,
							PrevH, PrevWeaveSize),
			LastStepCheckpoints2 =
				case LastStepCheckpoints of
					not_found ->
						PrevOutput =
							case Steps of
								[_, PrevStepOutput | _] ->
									PrevStepOutput;
								_ ->
									TipNonceLimiterInfo#nonce_limiter_info.output
							end,
						PrevOutput2 = ar_nonce_limiter:maybe_add_entropy(
								PrevOutput, PrevStepNumber, StepNumber, PrevNextSeed),
						{ok, NonceLimiterOutput, Checkpoints} = ar_nonce_limiter:compute(
								StepNumber, PrevOutput2),
						Checkpoints;
					_ ->
						LastStepCheckpoints
				end,
			NonceLimiterInfo2 = NonceLimiterInfo#nonce_limiter_info{ seed = Seed,
					next_seed = NextSeed, partition_upper_bound = PartitionUpperBound,
					next_partition_upper_bound = NextPartitionUpperBound,
					last_step_checkpoints = LastStepCheckpoints2,
					steps = Steps },
			PrevB = ar_block_cache:get(block_cache, PrevH),
			Height = PrevB#block.height + 1,
			{Rate, ScheduledRate} = ar_pricing:recalculate_usd_to_ar_rate(PrevB),
			{PricePerGiBMinute, ScheduledPricePerGiBMinute} =
					ar_pricing:recalculate_price_per_gib_minute(PrevB),
			Denomination = PrevB#block.denomination,
			{Denomination2, RedenominationHeight2} = ar_pricing:may_be_redenominate(PrevB),
			PricePerGiBMinute2 = ar_pricing:redenominate(PricePerGiBMinute, Denomination,
					Denomination2),
			ScheduledPricePerGiBMinute2 = ar_pricing:redenominate(ScheduledPricePerGiBMinute,
					Denomination, Denomination2),
			CDiff = ar_difficulty:next_cumulative_diff(PrevB#block.cumulative_diff, Diff,
					Height),
			UnsignedB = pack_block_with_transactions(#block{
				nonce = Nonce,
				previous_block = PrevH,
				timestamp = Timestamp,
				last_retarget =
					case ar_retarget:is_retarget_height(Height) of
						true -> Timestamp;
						false -> PrevB#block.last_retarget
					end,
				diff = Diff,
				height = Height,
				hash = SolutionH,
				hash_list_merkle = ar_block:compute_hash_list_merkle(PrevB),
				reward_addr = ar_wallet:to_address(RewardKey),
				tags = [],
				cumulative_diff = CDiff,
				previous_cumulative_diff = PrevB#block.cumulative_diff,
				poa = PoA1,
				usd_to_ar_rate = Rate,
				scheduled_usd_to_ar_rate = ScheduledRate,
				packing_2_5_threshold = 0,
				strict_data_split_threshold = PrevB#block.strict_data_split_threshold,
				hash_preimage = SolutionPreimage,
				recall_byte = RecallByte1,
				previous_solution_hash = PrevB#block.hash,
				partition_number = PartitionNumber,
				nonce_limiter_info = NonceLimiterInfo2,
				poa2 = PoA2,
				recall_byte2 = RecallByte2,
				reward_key = element(2, RewardKey),
				price_per_gib_minute = PricePerGiBMinute2,
				scheduled_price_per_gib_minute = ScheduledPricePerGiBMinute2,
				denomination = Denomination2,
				redenomination_height = RedenominationHeight2,
				double_signing_proof = may_be_get_double_signing_proof(PrevB, State)
			}, PrevB),
			SignedH = ar_block:generate_signed_hash(UnsignedB),
			PrevCDiff = PrevB#block.cumulative_diff,
			SignaturePreimage = << (ar_serialize:encode_int(CDiff, 16))/binary,
					(ar_serialize:encode_int(PrevCDiff, 16))/binary, (PrevB#block.hash)/binary,
					SignedH/binary >>,
			Signature = ar_wallet:sign(element(1, RewardKey), SignaturePreimage),
			H = ar_block:indep_hash2(SignedH, Signature),
			B = UnsignedB#block{ indep_hash = H, signature = Signature },
			ar_watchdog:mined_block(H, Height, PrevH),
			?LOG_INFO([{event, mined_block}, {indep_hash, ar_util:encode(H)},
					{solution, ar_util:encode(SolutionH)}, {txs, length(B#block.txs)}]),
			PrevBlocks = [PrevB],
			[{_, RecentBI}] = ets:lookup(node_state, recent_block_index),
			RecentBI2 = [block_index_entry(B) | RecentBI],
			[{_, BlockTXPairs}] = ets:lookup(node_state, block_txs_pairs),
			BlockTXPairs2 = [block_txs_pair(B) | BlockTXPairs],
			ar_block_cache:add(block_cache, B),
			State2 = apply_validated_block(State, B, PrevBlocks, [], RecentBI2, BlockTXPairs2),
			%% Won't be received by itself, but we should let know all "block" subscribers.
			ar_events:send(block, {new, B, #{ source => miner }}),
			{noreply, State2};
		_Steps ->
			?LOG_ERROR([{event, bad_steps},
					{prev_block, ar_util:encode(PrevH)},
					{step_number, StepNumber},
					{prev_step_number, PrevStepNumber},
					{prev_next_seed, ar_util:encode(PrevNextSeed)},
					{output, ar_util:encode(NonceLimiterOutput)}]),
			{noreply, State}
	end;

handle_info({event, miner, _}, State) ->
	{noreply, State};

handle_info({tx_ready_for_mining, TX}, State) ->
	ar_mempool:add_tx(TX, ready_for_mining),
	ar_events:send(tx, {ready_for_mining, TX}),
	{noreply, State};

handle_info({event, block, {double_signing, Proof}}, State) ->
	Map = maps:get(double_signing_proofs, State, #{}),
	Key = element(1, Proof),
	Addr = ar_wallet:to_address({?DEFAULT_KEY_TYPE, Key}),
	case is_map_key(Addr, Map) of
		true ->
			{noreply, State};
		false ->
			Map2 = maps:put(Addr, {os:system_time(second), Proof}, Map),
			{noreply, State#{ double_signing_proofs => Map2 }}
	end;

handle_info({event, block, {new, Block, _Source}}, State)
		when length(Block#block.txs) > ?BLOCK_TX_COUNT_LIMIT ->
	?LOG_WARNING([{event, received_block_with_too_many_txs},
			{block, ar_util:encode(Block#block.indep_hash)}, {txs, length(Block#block.txs)}]),
	{noreply, State};

handle_info({event, block, {new, B, _Source}}, State) ->
	H = B#block.indep_hash,
	%% Record the block in the block cache. Schedule an application of the
	%% earliest not validated block from the longest chain, if any.
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			case ar_block_cache:get(block_cache, B#block.previous_block) of
				not_found ->
					%% The cache should have been just pruned and this block is old.
					?LOG_WARNING([{event, block_cache_missing_block},
							{previous_block, ar_util:encode(B#block.previous_block)},
							{previous_height, B#block.height - 1},
							{block, ar_util:encode(H)}]),
					{noreply, State};
				_PrevB ->
					ar_block_cache:add(block_cache, B),
					gen_server:cast(?MODULE, apply_block),
					{noreply, State}
			end;
		_ ->
			%% The block's already received from a different peer or
			%% fetched by ar_poller.
			{noreply, State}
	end;

handle_info({event, block, {mined, Block, TXs, CurrentBH}}, State) ->
	case ets:lookup(node_state, recent_block_index) of
		[{recent_block_index, [{CurrentBH, _, _} | _] = RecentBI}] ->
			[{block_txs_pairs, BlockTXPairs}] = ets:lookup(node_state, block_txs_pairs),
			[{current, Current}] = ets:lookup(node_state, current),
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs,
					Block#block.height),
			B = Block#block{ txs = TXs, size_tagged_txs = SizeTaggedTXs },
			ar_watchdog:mined_block(B#block.indep_hash, B#block.height,
					B#block.previous_block),
			?LOG_INFO([{event, mined_block}, {indep_hash, ar_util:encode(B#block.indep_hash)},
				{solution, ar_util:encode(B#block.hash)}, {txs, length(TXs)}]),
			PrevBlocks = [ar_block_cache:get(block_cache, Current)],
			RecentBI2 = [block_index_entry(B) | RecentBI],
			BlockTXPairs2 = [block_txs_pair(B) | BlockTXPairs],
			ar_block_cache:add(block_cache, B),
			State2 = apply_validated_block(State, B, PrevBlocks, [], RecentBI2, BlockTXPairs2),
			%% Won't be received by itself, but we should let know all "block" subscribers.
			ar_events:send(block, {new, Block#block{ txs = TXs }, #{ source => miner }}),
			{noreply, State2};
		_ ->
			?LOG_INFO([{event, ignore_mined_block}, {reason, accepted_foreign_block}]),
			{noreply, State}
	end;

handle_info({event, block, _}, State) ->
	{noreply, State};

%% Add the new waiting transaction to the server state.
handle_info({event, tx, {new, TX, _Source}}, State) ->
	TXID = TX#tx.id,
	case ar_mempool:has_tx(TXID) of
		false ->
			ar_mempool:add_tx(TX, waiting),
			case ar_mempool:has_tx(TXID) of
				true ->
					start_tx_mining_timer(TX);
				false ->
					%% The transaction has been dropped because more valuable transactions
					%% exceed the mempool limit.
					ok
			end,
			{noreply, State};
		true ->
			{noreply, State}
	end;

handle_info({event, tx, {emitting_scheduled, Utility, TXID}}, State) ->
	ar_mempool:del_from_propagation_queue(Utility, TXID),
	{noreply, State};

%% Add the transaction to the mining pool, to be included in the mined block.
handle_info({event, tx, {ready_for_mining, TX}}, State) ->
	ar_mempool:add_tx(TX, ready_for_mining),
	{noreply, State};

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info({'DOWN', _Ref, process, PID, _Info}, State) ->
	#{
		blocks_missing_txs := Set,
		missing_txs_lookup_processes := Map
	} = State,
	BH = maps:get(PID, Map),
	{noreply, State#{
		missing_txs_lookup_processes => maps:remove(PID, Map),
		blocks_missing_txs => sets:del_element(BH, Set)
	}};

handle_info({'EXIT', _PID, normal}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {message, Info}]),
	{noreply, State}.

terminate(Reason, #{ miner_2_5 := Miner_2_5 }) ->
	ar_http_iface_server:stop(),
	case ets:lookup(node_state, is_joined) of
		[{_, true}] ->
			case Miner_2_5 of
				undefined -> do_nothing;
				PID -> ar_mine:stop(PID)
			end,
			[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
			Mempool =
				gb_sets:fold(
					fun({_Utility, TXID, Status}, Acc) ->
						maps:put(TXID, {ar_mempool:get_tx(TXID), Status}, Acc)
					end,
					#{},
					ar_mempool:get_priority_set()	
				),
			dump_mempool(Mempool, MempoolSize);
		_ ->
			ok
	end,
	?LOG_INFO([{event, ar_node_worker_terminated}, {reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

record_metrics() ->
	[{mempool_size, MempoolSize}] = ets:lookup(node_state, mempool_size),
	[{weave_size, WeaveSize}] = ets:lookup(node_state, weave_size),
	[{height, Height}] = ets:lookup(node_state, height),
	prometheus_gauge:set(arweave_block_height, Height),
	record_mempool_size_metrics(MempoolSize),
	prometheus_gauge:set(weave_size, WeaveSize).

record_mempool_size_metrics({HeaderSize, DataSize}) ->
	prometheus_gauge:set(mempool_header_size_bytes, HeaderSize),
	prometheus_gauge:set(mempool_data_size_bytes, DataSize).

may_be_initialize_nonce_limiter([#block{ height = Height } = B | Blocks], BI) ->
	case Height + 1 == ar_fork:height_2_6() of
		true ->
			{Seed, PartitionUpperBound, _TXRoot} = ar_node:get_nth_or_last(
					?SEARCH_SPACE_UPPER_BOUND_DEPTH, BI),
			Output = crypto:hash(sha256, Seed),
			NextSeed = B#block.indep_hash,
			NextPartitionUpperBound = B#block.weave_size,
			Info = #nonce_limiter_info{ output = Output, seed = Seed, next_seed = NextSeed,
					partition_upper_bound = PartitionUpperBound,
					next_partition_upper_bound = NextPartitionUpperBound },
			[B#block{ nonce_limiter_info = Info } | Blocks];
		false ->
			[B | may_be_initialize_nonce_limiter(Blocks, tl(BI))]
	end;
may_be_initialize_nonce_limiter([], _BI) ->
	[].

handle_initialized(State) ->
	[{recent_block_index, RecentBI}] = ets:lookup(node_state, recent_block_index),
	[{joined_blocks, Blocks}] = ets:lookup(node_state, joined_blocks),
	B = hd(Blocks),
	Height = B#block.height,
	ar_disk_cache:write_block(B),
	ar_data_sync:join(RecentBI),
	ar_header_sync:join(Height, RecentBI, Blocks),
	ar_tx_blacklist:start_taking_down(),
	Current = element(1, hd(RecentBI)),
	ar_block_cache:initialize_from_list(block_cache,
			lists:sublist(Blocks, ?STORE_BLOCKS_BEHIND_CURRENT)),
	BlockTXPairs = [block_txs_pair(Block) || Block <- Blocks],
	{BlockAnchors, RecentTXMap} = get_block_anchors_and_recent_txs_map(BlockTXPairs),
	{Rate, ScheduledRate} =
		case Height >= ar_fork:height_2_5() of
			true ->
				{B#block.usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
			false ->
				{?INITIAL_USD_TO_AR((Height + 1))(), ?INITIAL_USD_TO_AR((Height + 1))()}
		end,
	ets:insert(node_state, [
		{is_joined,				true},
		{current,				Current},
		{timestamp,				B#block.timestamp},
		{nonce_limiter_info,	B#block.nonce_limiter_info},
		{wallet_list,			B#block.wallet_list},
		{height,				Height},
		{hash,					B#block.hash},
		{reward_pool,			B#block.reward_pool},
		{diff,					B#block.diff},
		{cumulative_diff,		B#block.cumulative_diff},
		{last_retarget,			B#block.last_retarget},
		{weave_size,			B#block.weave_size},
		{block_txs_pairs,		BlockTXPairs},
		{block_anchors,			BlockAnchors},
		{recent_txs_map,		RecentTXMap},
		{usd_to_ar_rate,		Rate},
		{scheduled_usd_to_ar_rate, ScheduledRate},
		{price_per_gib_minute, B#block.price_per_gib_minute},
		{kryder_plus_rate_multiplier, B#block.kryder_plus_rate_multiplier},
		{denomination, B#block.denomination},
		{redenomination_height, B#block.redenomination_height},
		{scheduled_price_per_gib_minute, B#block.scheduled_price_per_gib_minute}
	]),
	SearchSpaceUpperBound = ar_node:get_partition_upper_bound(RecentBI),
	ar_events:send(node_state, {search_space_upper_bound, SearchSpaceUpperBound}),
	ar_events:send(node_state, {initialized, B}),
	ar_events:send(node_state, {checkpoint_block, get_checkpoint_block(RecentBI)}),
	ar:console("Joined the Arweave network successfully.~n"),
	?LOG_INFO([{event, joined_the_network}]),
	{noreply, may_be_reset_miner(State)}.

handle_task(apply_block, State) ->
	apply_block(State);

handle_task({cache_missing_txs, BH, TXs}, State) ->
	case ar_block_cache:get_block_and_status(block_cache, BH) of
		not_found ->
			%% The block should have been pruned while we were fetching the missing txs.
			{noreply, State};
		{B, {not_validated, _}} ->
			case ar_block_cache:get(block_cache, B#block.previous_block) of
				not_found ->
					ok;
				_ ->
					ar_block_cache:add(block_cache, B#block{ txs = TXs })
			end,
			gen_server:cast(?MODULE, apply_block),
			{noreply, State};
		{_B, _AnotherStatus} ->
			%% The transactions should have been received and the block validated while
			%% we were looking for previously missing transactions.
			{noreply, State}
	end;

handle_task(mine, State) ->
	{noreply, start_mining(State)};

handle_task(automine, State) ->
	{noreply, start_mining(State#{ automine => true })};

handle_task({filter_mempool, Mempool}, State) ->
	{ok, List, RemainingMempool} = ar_mempool:take_chunk(Mempool, ?FILTER_MEMPOOL_CHUNK_SIZE),
	case List of
		[] ->
			{noreply, State};
		_ ->
			[{wallet_list, WalletList}] = ets:lookup(node_state, wallet_list),
			[{height, Height}] = ets:lookup(node_state, height),
			[{usd_to_ar_rate, Rate}] = ets:lookup(node_state, usd_to_ar_rate),
			[{price_per_gib_minute, Price}] = ets:lookup(node_state, price_per_gib_minute),
			[{kryder_plus_rate_multiplier, KryderPlusRateMultiplier}] = ets:lookup(node_state,
					kryder_plus_rate_multiplier),
			[{denomination, Denomination}] = ets:lookup(node_state, denomination),
			[{redenomination_height, RedenominationHeight}] = ets:lookup(node_state,
					redenomination_height),
			[{block_anchors, BlockAnchors}] = ets:lookup(node_state, block_anchors),
			[{recent_txs_map, RecentTXMap}] = ets:lookup(node_state, recent_txs_map),
			Wallets = ar_wallets:get(WalletList, ar_tx:get_addresses(List)),
			InvalidTXs =
				lists:foldl(
					fun(TX, Acc) ->
						case ar_tx_replay_pool:verify_tx({TX, Rate, Price,
								KryderPlusRateMultiplier, Denomination, Height,
								RedenominationHeight, BlockAnchors, RecentTXMap, #{}, Wallets},
								do_not_verify_signature) of
							valid ->
								Acc;
							{invalid, _Reason} ->
								[TX | Acc]
						end
					end,
					[],
					List
				),
			ar_mempool:drop_txs(InvalidTXs),
			case RemainingMempool of
				[] ->
					scan_complete;
				_ ->
					gen_server:cast(self(), {filter_mempool, RemainingMempool})
			end,
			{noreply, State}
	end;

handle_task(refresh_timestamp, #{ miner_2_6 := undefined } = State) ->
	{noreply, State};
handle_task(refresh_timestamp, State) ->
	Diff = get_current_diff(),
	ar_mining_server:set_difficulty(Diff),
	ar_util:cast_after((?MINING_TIMESTAMP_REFRESH_INTERVAL) * 1000, ?MODULE,
			refresh_timestamp),
	{noreply, State};

handle_task(Msg, State) ->
	?LOG_ERROR([
		{event, ar_node_worker_received_unknown_message},
		{message, Msg}
	]),
	{noreply, State}.

get_block_anchors_and_recent_txs_map(BlockTXPairs) ->
	lists:foldr(
		fun({BH, L}, {Acc1, Acc2}) ->
			Acc3 =
				lists:foldl(
					fun({{TXID, _}, _}, Acc4) ->
						%% We use a map instead of a set here because it is faster.
						maps:put(TXID, ok, Acc4)
					end,
					Acc2,
					L
				),
			{[BH | Acc1], Acc3}
		end,
		{[], #{}},
		lists:sublist(BlockTXPairs, ?MAX_TX_ANCHOR_DEPTH)
	).

apply_block(State) ->
	case ar_block_cache:get_earliest_not_validated_from_longest_chain(block_cache) of
		not_found ->
			%% Nothing to do - we are at the longest known chain already.
			{noreply, State};
		{B, [PrevB | _PrevBlocks], {{not_validated, awaiting_nonce_limiter_validation},
				_Timestamp}} ->
			case maps:get(nonce_limiter_validation_scheduled, State, false) of
				true ->
					{noreply, State};
				false ->
					request_nonce_limiter_validation(B, PrevB),
					{noreply, State#{ nonce_limiter_validation_scheduled => true }}
			end;
		{_B, _PrevBlocks, {{not_validated, nonce_limiter_validation_scheduled}, _Timestamp}} ->
			%% Waiting until the nonce limiter chain is validated.
			{noreply, State};
		{B, PrevBlocks, {{not_validated, nonce_limiter_validated}, Timestamp}} ->
			apply_block(B, PrevBlocks, Timestamp, State);
		{B, PrevBlocks, {{not_validated, awaiting_validation}, Timestamp}} ->
			%% Pre-2.6 blocks.
			apply_block(B, PrevBlocks, Timestamp, State)
	end.

apply_block(B, PrevBlocks, Timestamp, State) ->
	#{ blocks_missing_txs := BlocksMissingTXs } = State,
	case sets:is_element(B#block.indep_hash, BlocksMissingTXs) of
		true ->
			%% We do not have some of the transactions from this block,
			%% searching for them at the moment.
			{noreply, State};
		false ->
			apply_block2(B, PrevBlocks, Timestamp, State)
	end.

apply_block2(BShadow, PrevBlocks, Timestamp, State) ->
	#{ blocks_missing_txs := BlocksMissingTXs,
			missing_txs_lookup_processes := MissingTXsLookupProcesses } = State,
	{TXs, MissingTXIDs} = pick_txs(BShadow#block.txs),
	case MissingTXIDs of
		[] ->
			Height = BShadow#block.height,
			SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs, Height),
			B = BShadow#block{ txs = TXs, size_tagged_txs = SizeTaggedTXs },
			apply_block3(B, PrevBlocks, Timestamp, State);
		_ ->
			?LOG_INFO([{event, missing_txs_for_block}, {count, length(MissingTXIDs)}]),
			Self = self(),
			monitor(
				process,
				PID = spawn(fun() -> process_flag(trap_exit, true),
						get_missing_txs_and_retry(BShadow, Self) end)
			),
			BH = BShadow#block.indep_hash,
			{noreply, State#{
				blocks_missing_txs => sets:add_element(BH, BlocksMissingTXs),
				missing_txs_lookup_processes => maps:put(PID, BH, MissingTXsLookupProcesses)
			}}
	end.

apply_block3(B, [PrevB | _] = PrevBlocks, Timestamp, State) ->
	[{block_txs_pairs, BlockTXPairs}] = ets:lookup(node_state, block_txs_pairs),
	[{recent_block_index, RecentBI}] = ets:lookup(node_state, recent_block_index),
	RootHash = PrevB#block.wallet_list,
	TXs = B#block.txs,
	Accounts = ar_wallets:get(RootHash, [B#block.reward_addr | ar_tx:get_addresses(TXs)]),
	{Orphans, RecentBI2} = update_block_index(B, PrevBlocks, RecentBI),
	BlockTXPairs2 = update_block_txs_pairs(B, PrevBlocks, BlockTXPairs),
	BlockTXPairs3 = tl(BlockTXPairs2),
	{BlockAnchors, RecentTXMap} = get_block_anchors_and_recent_txs_map(BlockTXPairs3),
	RecentBI3 = tl(RecentBI2),
	PartitionUpperBound = ar_node:get_partition_upper_bound(RecentBI3),
	case ar_node_utils:validate(B, PrevB, Accounts, BlockAnchors, RecentTXMap,
			PartitionUpperBound) of
		error ->
			?LOG_WARNING([{event, failed_to_validate_block},
					{h, ar_util:encode(B#block.indep_hash)}]),
			gen_server:cast(?MODULE, apply_block),
			{noreply, State};
		{invalid, Reason} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, Reason},
					{h, ar_util:encode(B#block.indep_hash)}]),
			ar_events:send(block, {rejected, Reason, B#block.indep_hash, no_peer}),
			BH = B#block.indep_hash,
			ar_block_cache:remove(block_cache, BH),
			ar_ignore_registry:add(BH),
			gen_server:cast(?MODULE, apply_block),
			{noreply, State};
		valid ->
			case validate_wallet_list(B, PrevB) of
				error ->
					BH = B#block.indep_hash,
					ar_block_cache:remove(block_cache, BH),
					ar_ignore_registry:add(BH),
					gen_server:cast(?MODULE, apply_block),
					{noreply, State};
				ok ->
					B2 =
						case B#block.height >= ar_fork:height_2_6() of
							true ->
								RewardHistory = PrevB#block.reward_history,
								Reward = B#block.reward,
								HashRate = ar_difficulty:get_hash_rate(B#block.diff),
								Denomination2 = B#block.denomination,
								Addr = B#block.reward_addr,
								RewardHistory2 = [{Addr, HashRate, Reward, Denomination2}
										| RewardHistory],
								Len = ?REWARD_HISTORY_BLOCKS + ?STORE_BLOCKS_BEHIND_CURRENT,
								RewardHistory3 = lists:sublist(RewardHistory2, Len),
								B#block{ reward_history = RewardHistory3 };
							false ->
								B
						end,
					State2 = apply_validated_block(State, B2, PrevBlocks, Orphans, RecentBI2,
							BlockTXPairs2),
					record_processing_time(Timestamp),
					{noreply, State2}
			end
	end.

request_nonce_limiter_validation(#block{ indep_hash = H } = B, PrevB) ->
	Info = B#block.nonce_limiter_info,
	PrevInfo = ar_nonce_limiter:get_or_init_nonce_limiter_info(PrevB),
	ar_nonce_limiter:request_validation(H, Info, PrevInfo),
	ar_block_cache:mark_nonce_limiter_validation_scheduled(block_cache, H).

pick_txs(TXIDs) ->
	Mempool = ar_mempool:get_map(),
	lists:foldr(
		fun (TX, {Found, Missing}) when is_record(TX, tx) ->
				{[TX | Found], Missing};
			(TXID, {Found, Missing}) ->
				case maps:get(TXID, Mempool, tx_not_in_mempool) of
					tx_not_in_mempool ->
						%% This disk read should almost never be useful. Presumably,
						%% the only reason to find some of these transactions on disk
						%% is they had been written prior to the call, what means they are
						%% from an orphaned fork, more than one block behind.
						case ar_storage:read_tx(TXID) of
							unavailable ->
								{Found, [TXID | Missing]};
							TX ->
								{[TX | Found], Missing}
						end;
					_Status ->
						{[ar_mempool:get_tx(TXID) | Found], Missing}
				end
		end,
		{[], []},
		TXIDs
	).

may_be_get_double_signing_proof(PrevB, State) ->
	RewardHistory = lists:sublist(PrevB#block.reward_history, ?REWARD_HISTORY_BLOCKS),
	Proofs = maps:get(double_signing_proofs, State, #{}),
	RootHash = PrevB#block.wallet_list,
	may_be_get_double_signing_proof2(maps:iterator(Proofs), RootHash, RewardHistory).

may_be_get_double_signing_proof2(Iterator, RootHash, RewardHistory) ->
	case maps:next(Iterator) of
		none ->
			undefined;
		{Addr, {_Timestamp, Proof2}, Iterator2} ->
			case is_in_reward_history(Addr, RewardHistory) of
				false ->
					may_be_get_double_signing_proof2(Iterator2, RootHash, RewardHistory);
				true ->
					Accounts = ar_wallets:get(RootHash, [Addr]),
					case is_account_banned(Addr, Accounts) of
						true ->
							may_be_get_double_signing_proof2(Iterator2, RootHash,
									RewardHistory);
						false ->
							Proof2
					end
			end
	end.

is_in_reward_history(_Addr, []) ->
	false;
is_in_reward_history(Addr, [{Addr, _, _, _} | _]) ->
	true;
is_in_reward_history(Addr, [_ | RewardHistory]) ->
	is_in_reward_history(Addr, RewardHistory).

is_account_banned(Addr, Map) ->
	case maps:get(Addr, Map, not_found) of
		not_found ->
			false;
		{_, _} ->
			false;
		{_, _, _, MiningPermission} ->
			not MiningPermission
	end.

pack_block_with_transactions(#block{ height = Height, diff = Diff } = B, PrevB) ->
	#block{ reward_history = RewardHistory } = PrevB,
	TXs = collect_mining_transactions(?BLOCK_TX_COUNT_LIMIT),
	Rate = ar_pricing:usd_to_ar_rate(PrevB),
	PricePerGiBMinute = PrevB#block.price_per_gib_minute,
	Denomination = PrevB#block.denomination,
	Denomination2 = B#block.denomination,
	KryderPlusRateMultiplier = PrevB#block.kryder_plus_rate_multiplier,
	RedenominationHeight = PrevB#block.redenomination_height,
	Addresses = [B#block.reward_addr | ar_tx:get_addresses(TXs)],
	Addresses2 =
		case length(RewardHistory) >= ?REWARD_HISTORY_BLOCKS of
			true ->
				[element(1, lists:nth(?REWARD_HISTORY_BLOCKS, RewardHistory)) | Addresses];
			false ->
				Addresses
		end,
	Addresses3 =
		case B#block.double_signing_proof of
			undefined ->
				Addresses2;
			Proof ->
				[ar_wallet:to_address({?DEFAULT_KEY_TYPE, element(1, Proof)}) | Addresses2]
		end,
	Accounts = ar_wallets:get(PrevB#block.wallet_list, Addresses3),
	[{_, BlockAnchors}] = ets:lookup(node_state, block_anchors),
	[{_, RecentTXMap}] = ets:lookup(node_state, recent_txs_map),
	ValidTXs = ar_tx_replay_pool:pick_txs_to_mine({BlockAnchors, RecentTXMap, Height - 1,
			RedenominationHeight, Rate, PricePerGiBMinute, KryderPlusRateMultiplier,
			Denomination, B#block.timestamp, Accounts, TXs}),
	BlockSize =
		lists:foldl(
			fun(TX, Acc) ->
				Acc + ar_tx:get_weave_size_increase(TX, Height)
			end,
			0,
			ValidTXs
		),
	WeaveSize = PrevB#block.weave_size + BlockSize,
	B2 = B#block{ txs = ValidTXs, block_size = BlockSize, weave_size = WeaveSize,
			tx_root = ar_block:generate_tx_root_for_block(ValidTXs, Height),
			size_tagged_txs = ar_block:generate_size_tagged_list_from_txs(ValidTXs, Height) },
	{ok, {EndowmentPool, Reward, DebtSupply, KryderPlusRateMultiplierLatch,
			KryderPlusRateMultiplier2, Accounts2}} = ar_node_utils:update_accounts(B2, PrevB,
					Accounts),
	Reward2 = ar_pricing:redenominate(Reward, Denomination, Denomination2),
	EndowmentPool2 = ar_pricing:redenominate(EndowmentPool, Denomination, Denomination2),
	DebtSupply2 = ar_pricing:redenominate(DebtSupply, Denomination, Denomination2),
	RewardAddr = B2#block.reward_addr,
	{ok, RootHash} = ar_wallets:add_wallets(PrevB#block.wallet_list, Accounts2, Height,
			Denomination2),
	HashRate = ar_difficulty:get_hash_rate(Diff),
	RewardHistory2 = lists:sublist([{RewardAddr, HashRate, Reward2, Denomination2}
			| RewardHistory], ?REWARD_HISTORY_BLOCKS + ?STORE_BLOCKS_BEHIND_CURRENT),
	RewardHistory3 = lists:sublist([{RewardAddr, HashRate, Reward2, Denomination2}
			| RewardHistory], ?REWARD_HISTORY_BLOCKS),
	B2#block{
		wallet_list = RootHash,
		reward_pool = EndowmentPool2,
		reward = Reward2,
		reward_history = RewardHistory2,
		reward_history_hash = ar_block:reward_history_hash(RewardHistory3),
		debt_supply = DebtSupply2,
		kryder_plus_rate_multiplier_latch = KryderPlusRateMultiplierLatch,
		kryder_plus_rate_multiplier = KryderPlusRateMultiplier2
	}.

update_block_index(B, PrevBlocks, BI) ->
	#block{ indep_hash = H } = lists:last(PrevBlocks),
	{Orphans, Base} = get_orphans(BI, H),
	{Orphans, [block_index_entry(B) |
		[block_index_entry(PrevB) || PrevB <- PrevBlocks] ++ Base]}.

get_orphans(BI, H) ->
	get_orphans(BI, H, []).

get_orphans([{H, _, _} | BI], H, Orphans) ->
	{Orphans, BI};
get_orphans([{OrphanH, _, _} | BI], H, Orphans) ->
	get_orphans(BI, H, [OrphanH | Orphans]).

block_index_entry(B) ->
	{B#block.indep_hash, B#block.weave_size, B#block.tx_root}.

update_block_txs_pairs(B, PrevBlocks, BlockTXPairs) ->
	lists:sublist(update_block_txs_pairs2(B, PrevBlocks, BlockTXPairs),
			2 * ?MAX_TX_ANCHOR_DEPTH).

update_block_txs_pairs2(B, [PrevB, PrevPrevB | PrevBlocks], BP) ->
	[block_txs_pair(B) | update_block_txs_pairs2(PrevB, [PrevPrevB | PrevBlocks], BP)];
update_block_txs_pairs2(B, [#block{ indep_hash = H }], BP) ->
	[block_txs_pair(B) | lists:dropwhile(fun({Hash, _}) -> Hash /= H end, BP)].

block_txs_pair(B) ->
	{B#block.indep_hash, B#block.size_tagged_txs}.

validate_wallet_list(#block{ indep_hash = H } = B, PrevB) ->
	case ar_wallets:apply_block(B, PrevB) of
		{error, invalid_denomination} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_denomination}, {h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_denomination, H, no_peer}),
			error;
		{error, mining_address_banned} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, mining_address_banned}, {h, ar_util:encode(H)},
					{mining_address, ar_util:encode(B#block.reward_addr)}]),
			ar_events:send(block, {rejected, mining_address_banned, H, no_peer}),
			error;
		{error, invalid_double_signing_proof_same_signature} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_double_signing_proof_same_signature},
					{h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_double_signing_proof_same_signature, H,
					no_peer}),
			error;
		{error, invalid_double_signing_proof_cdiff} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_double_signing_proof_cdiff},
					{h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_double_signing_proof_cdiff, H, no_peer}),
			error;
		{error, invalid_double_signing_proof_same_address} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_double_signing_proof_same_address},
					{h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_double_signing_proof_same_address, H,
					no_peer}),
			error;
		{error, invalid_double_signing_proof_not_in_reward_history} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_double_signing_proof_not_in_reward_history},
					{h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected,
					invalid_double_signing_proof_not_in_reward_history, H, no_peer}),
			error;
		{error, invalid_double_signing_proof_already_banned} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_double_signing_proof_already_banned},
					{h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected,
					invalid_double_signing_proof_already_banned, H, no_peer}),
			error;
		{error, invalid_double_signing_proof_invalid_signature} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_double_signing_proof_invalid_signature},
					{h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected,
					invalid_double_signing_proof_invalid_signature, H, no_peer}),
			error;
		{error, invalid_account_anchors} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_account_anchors}, {h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_account_anchors, H, no_peer}),
			error;
		{error, invalid_reward_pool} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_reward_pool}, {h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_reward_pool, H, no_peer}),
			error;
		{error, invalid_miner_reward} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_miner_reward}, {h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_miner_reward, H, no_peer}),
			error;
		{error, invalid_debt_supply} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_debt_supply}, {h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_debt_supply, H, no_peer}),
			error;
		{error, invalid_kryder_plus_rate_multiplier_latch} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_kryder_plus_rate_multiplier_latch},
					{h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_kryder_plus_rate_multiplier_latch, H,
					no_peer}),
			error;
		{error, invalid_kryder_plus_rate_multiplier} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_kryder_plus_rate_multiplier},
					{h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_kryder_plus_rate_multiplier, H, no_peer}),
			error;
		{error, invalid_wallet_list} ->
			?LOG_WARNING([{event, received_invalid_block},
					{validation_error, invalid_wallet_list}, {h, ar_util:encode(H)}]),
			ar_events:send(block, {rejected, invalid_wallet_list, H, no_peer}),
			error;
		{ok, _RootHash2} ->
			ok
	end.

get_missing_txs_and_retry(#block{ txs = TXIDs }, _Worker)
		when length(TXIDs) > 1000 ->
	?LOG_WARNING([{event, ar_node_worker_downloaded_txs_count_exceeds_limit}]),
	ok;
get_missing_txs_and_retry(BShadow, Worker) ->
	get_missing_txs_and_retry(BShadow#block.indep_hash, BShadow#block.txs,
			Worker, ar_peers:get_peers(), [], 0).

get_missing_txs_and_retry(_H, _TXIDs, _Worker, _Peers, _TXs, TotalSize)
		when TotalSize > ?BLOCK_TX_DATA_SIZE_LIMIT ->
	?LOG_WARNING([{event, ar_node_worker_downloaded_txs_exceed_block_size_limit}]),
	ok;
get_missing_txs_and_retry(H, [], Worker, _Peers, TXs, _TotalSize) ->
	gen_server:cast(Worker, {cache_missing_txs, H, lists:reverse(TXs)});
get_missing_txs_and_retry(H, TXIDs, Worker, Peers, TXs, TotalSize) ->
	Split = min(5, length(TXIDs)),
	{Bulk, Rest} = lists:split(Split, TXIDs),
	Fetch =
		lists:foldl(
			fun	(TX = #tx{ format = 1, data_size = DataSize }, {Acc1, Acc2}) ->
					{[TX | Acc1], Acc2 + DataSize};
				(TX = #tx{}, {Acc1, Acc2}) ->
					{[TX | Acc1], Acc2};
				(_, failed_to_fetch_tx) ->
					failed_to_fetch_tx;
				(_, _) ->
					failed_to_fetch_tx
			end,
			{TXs, TotalSize},
			ar_util:pmap(
				fun(TXID) ->
					ar_http_iface_client:get_tx(Peers, TXID)
				end,
				Bulk
			)
		),
	case Fetch of
		failed_to_fetch_tx ->
			?LOG_WARNING([{event, ar_node_worker_failed_to_fetch_missing_tx}]),
			ok;
		{TXs2, TotalSize2} ->
			get_missing_txs_and_retry(H, Rest, Worker, Peers, TXs2, TotalSize2)
	end.

apply_validated_block(State, B, PrevBlocks, Orphans, RecentBI, BlockTXPairs) ->
	[{_, CDiff}] = ets:lookup(node_state, cumulative_diff),
	B2 =
		case B#block.height + 1 == ar_fork:height_2_6() of
			true ->
				Info = ar_nonce_limiter:get_or_init_nonce_limiter_info(B, RecentBI),
				B3 = B#block{ nonce_limiter_info = Info },
				ar_events:send(node_state, {validated_pre_fork_2_6_block, B3}),
				B3;
			_ ->
				B
		end,
	case B2#block.cumulative_diff =< CDiff of
		true ->
			%% The block is from the longest fork, but not the latest known block from there.
			ar_block_cache:add_validated(block_cache, B2),
			gen_server:cast(?MODULE, apply_block),
			log_applied_block(B2),
			State;
		false ->
			apply_validated_block2(State, B2, PrevBlocks, Orphans, RecentBI, BlockTXPairs)
	end.

apply_validated_block2(State, B, PrevBlocks, Orphans, RecentBI, BlockTXPairs) ->
	[{current, CurrentH}] = ets:lookup(node_state, current),
	PruneDepth = ?STORE_BLOCKS_BEHIND_CURRENT,
	BH = B#block.indep_hash,
	%% Overwrite the block to store computed size tagged txs - they
	%% may be needed for reconstructing block_txs_pairs if there is a reorg
	%% off and then back on this fork.
	ar_block_cache:add(block_cache, B),
	ar_block_cache:mark_tip(block_cache, BH),
	ar_block_cache:prune(block_cache, PruneDepth),
	%% We could have missed a few blocks due to networking issues, which would then
	%% be picked by ar_poller and end up waiting for missing transactions to be fetched.
	%% Thefore, it is possible (although not likely) that there are blocks above the new tip,
	%% for which we trigger a block application here, in order not to wait for the next
	%% arrived or fetched block to trigger it.
	gen_server:cast(?MODULE, apply_block),
	log_applied_block(B),
	log_tip(B),
	maybe_report_n_confirmations(B, RecentBI),
	PrevB = hd(PrevBlocks),
	prometheus_gauge:set(block_time, B#block.timestamp - PrevB#block.timestamp),
	record_economic_metrics(B, PrevB),
	record_fork_depth(Orphans),
	record_vdf_metrics(B, PrevB),
	return_orphaned_txs_to_mempool(CurrentH, (lists:last(PrevBlocks))#block.indep_hash),
	lists:foldl(
		fun (CurrentB, start) ->
				CurrentB;
			(CurrentB, _CurrentPrevB) ->
				Wallets = CurrentB#block.wallet_list,
				Height = CurrentB#block.height,
				%% Use a twice bigger depth than the depth requested on join to serve
				%% the wallet trees to the joining nodes.
				PruneDepth2 = PruneDepth * 2,
				ok = ar_wallets:set_current(Wallets, Height, PruneDepth2),
				CurrentB
		end,
		start,
		lists:reverse([B | PrevBlocks])
	),
	ar_disk_cache:write_block(B),
	BlockTXs = B#block.txs,
	ar_mempool:drop_txs(BlockTXs, false, false),
	gen_server:cast(self(), {filter_mempool, ar_mempool:get_all_txids()}),
	{BlockAnchors, RecentTXMap} = get_block_anchors_and_recent_txs_map(BlockTXPairs),
	Height = B#block.height,
	{Rate, ScheduledRate} =
		case Height >= ar_fork:height_2_5() of
			true ->
				{B#block.usd_to_ar_rate, B#block.scheduled_usd_to_ar_rate};
			false ->
				{?INITIAL_USD_TO_AR((Height + 1))(), ?INITIAL_USD_TO_AR((Height + 1))()}
		end,
	AddedBIElements = tl(lists:reverse([block_index_entry(B)
			| [block_index_entry(PrevB2) || PrevB2 <- PrevBlocks]])),
	ar_block_index:update(AddedBIElements, length(Orphans)),
	RecentBI2 = lists:sublist(RecentBI, ?BLOCK_INDEX_HEAD_LEN),
	ar_data_sync:add_tip_block(BlockTXPairs, RecentBI2),
	ar_header_sync:add_tip_block(B, RecentBI2),
	lists:foreach(
		fun(PrevB3) ->
			ar_header_sync:add_block(PrevB3),
			ar_disk_cache:write_block(PrevB3)
		end,
		tl(lists:reverse(PrevBlocks))
	),
	maybe_store_block_index(B),
	ets:insert(node_state, [
		{recent_block_index,	RecentBI2},
		{current,				B#block.indep_hash},
		{timestamp,				B#block.timestamp},
		{wallet_list,			B#block.wallet_list},
		{height,				B#block.height},
		{hash,					B#block.hash},
		{reward_pool,			B#block.reward_pool},
		{diff,					B#block.diff},
		{cumulative_diff,		B#block.cumulative_diff},
		{last_retarget,			B#block.last_retarget},
		{weave_size,			B#block.weave_size},
		{nonce_limiter_info,	B#block.nonce_limiter_info},
		{block_txs_pairs,		BlockTXPairs},
		{block_anchors,			BlockAnchors},
		{recent_txs_map,		RecentTXMap},
		{usd_to_ar_rate,		Rate},
		{scheduled_usd_to_ar_rate, ScheduledRate},
		{price_per_gib_minute, B#block.price_per_gib_minute},
		{kryder_plus_rate_multiplier, B#block.kryder_plus_rate_multiplier},
		{denomination, B#block.denomination},
		{redenomination_height, B#block.redenomination_height},
		{scheduled_price_per_gib_minute, B#block.scheduled_price_per_gib_minute}
	]),
	SearchSpaceUpperBound = ar_node:get_partition_upper_bound(RecentBI),
	ar_events:send(node_state, {search_space_upper_bound, SearchSpaceUpperBound}),
	ar_events:send(node_state, {new_tip, B, PrevB}),
	ar_events:send(node_state, {checkpoint_block, get_checkpoint_block(RecentBI)}),
	may_be_reset_miner(State).

get_checkpoint_block(RecentBI) ->
	get_checkpoint_block(RecentBI, 1).

get_checkpoint_block([{H, _, _}], _N) ->
	%% The genesis block.
	ar_block_cache:get(block_cache, H);
get_checkpoint_block([{H, _, _} | BI], N) ->
	 B = ar_block_cache:get(block_cache, H),
	 get_checkpoint_block(BI, N + 1, B).

get_checkpoint_block([{H, _, _}], _N, B) ->
	%% The genesis block.
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			B;
		B2 ->
			B2
	end;
get_checkpoint_block([{H, _, _} | _], N, B) when N == ?STORE_BLOCKS_BEHIND_CURRENT ->
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			B;
		B2 ->
			B2
	end;
get_checkpoint_block([{H, _, _} | BI], N, B) ->
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			B;
		B2 ->
			get_checkpoint_block(BI, N + 1, B2)
	end.

log_applied_block(B) ->
	?LOG_INFO([
		{event, applied_block},
		{indep_hash, ar_util:encode(B#block.indep_hash)},
		{height, B#block.height}
	]).

log_tip(B) ->
	?LOG_INFO([{event, new_tip_block}, {indep_hash, ar_util:encode(B#block.indep_hash)},
			{height, B#block.height}]).

maybe_report_n_confirmations(B, BI) ->
	N = 10,
	LastNBlocks = lists:sublist(BI, N),
	case length(LastNBlocks) == N of
		true ->
			{H, _, _} = lists:last(LastNBlocks),
			ar_watchdog:block_received_n_confirmations(H, B#block.height - N + 1);
		false ->
			do_nothing
	end.

maybe_store_block_index(B) ->
	case B#block.height rem ?STORE_BLOCKS_BEHIND_CURRENT of
		0 ->
			BI = ar_node:get_block_index(),
			spawn(fun() -> ar_storage:write_block_index_and_reward_history(BI,
					B#block.reward_history) end);
		_ ->
			ok
	end.

record_fork_depth(Orphans) ->
	record_fork_depth(Orphans, 0).

record_fork_depth([], 0) ->
	ok;
record_fork_depth([], N) ->
	prometheus_histogram:observe(fork_recovery_depth, N),
	ok;
record_fork_depth([H | Orphans], N) ->
	?LOG_INFO([{event, orphaning_block}, {block, ar_util:encode(H)}]),
	record_fork_depth(Orphans, N + 1).

record_economic_metrics(B, PrevB) ->
	case B#block.height >= ar_fork:height_2_5() of
		false ->
			ok;
		true ->
			record_economic_metrics2(B, PrevB)
	end.

record_economic_metrics2(B, PrevB) ->
	prometheus_gauge:set(network_hashrate, ar_difficulty:get_hash_rate(B#block.diff)),
	prometheus_gauge:set(endowment_pool, B#block.reward_pool),
	Period_200_Years = 200 * 365 * 24 * 60 * 60,
	Burden = ar_pricing:get_storage_cost(B#block.weave_size, B#block.timestamp,
			B#block.usd_to_ar_rate, B#block.height),
	case B#block.height >= ar_fork:height_2_6() of
		true ->
			#block{ reward_history = RewardHistory } = B,
			RewardHistorySize = length(RewardHistory),
			AverageHashRate = ar_util:safe_divide(lists:sum([HR
					|| {_, HR, _, _} <- RewardHistory]), RewardHistorySize),
			prometheus_gauge:set(average_network_hash_rate, AverageHashRate),
			AverageBlockReward = ar_util:safe_divide(lists:sum([R
					|| {_, _, R, _} <- RewardHistory]), RewardHistorySize),
			prometheus_gauge:set(average_block_reward, AverageBlockReward),
			prometheus_gauge:set(price_per_gibibyte_minute, B#block.price_per_gib_minute),
			Args = {PrevB#block.reward_pool, PrevB#block.debt_supply, B#block.txs,
					B#block.weave_size, B#block.height, PrevB#block.price_per_gib_minute,
					PrevB#block.kryder_plus_rate_multiplier_latch,
					PrevB#block.kryder_plus_rate_multiplier, PrevB#block.denomination},
			{ExpectedBlockReward,
					_, _, _, _} = ar_pricing:get_miner_reward_endowment_pool_debt_supply(Args),
			prometheus_gauge:set(expected_block_reward, ExpectedBlockReward),
			LegacyPricePerGibibyte = ar_pricing:get_storage_cost(1024 * 1024 * 1024,
					os:system_time(second), PrevB#block.usd_to_ar_rate, B#block.height),
			prometheus_gauge:set(legacy_price_per_gibibyte_minute, LegacyPricePerGibibyte),
			prometheus_gauge:set(available_supply,
					?TOTAL_SUPPLY - B#block.reward_pool + B#block.debt_supply),
			prometheus_gauge:set(debt_supply, B#block.debt_supply);
		false ->
			ok
	end,
	%% 2.5 metrics:
	prometheus_gauge:set(network_burden, Burden),
	Burden_10_USD_AR = ar_pricing:get_storage_cost(B#block.weave_size, B#block.timestamp,
			{1, 10}, B#block.height),
	prometheus_gauge:set(network_burden_10_usd_ar, Burden_10_USD_AR),
	Burden_200_Years = Burden - ar_pricing:get_storage_cost(B#block.weave_size,
			B#block.timestamp + Period_200_Years, B#block.usd_to_ar_rate, B#block.height),
	prometheus_gauge:set(network_burden_200_years, Burden_200_Years),
	Burden_200_Years_10_USD_AR = Burden_10_USD_AR - ar_pricing:get_storage_cost(
			B#block.weave_size, B#block.timestamp + Period_200_Years, {1, 10}, B#block.height),
	prometheus_gauge:set(network_burden_200_years_10_usd_ar, Burden_200_Years_10_USD_AR),
	case catch ar_pricing:get_expected_min_decline_rate(B#block.timestamp,
			Period_200_Years, B#block.reward_pool, B#block.weave_size, B#block.usd_to_ar_rate,
			B#block.height) of
		{'EXIT', _} ->
			?LOG_ERROR([{event, failed_to_compute_expected_min_decline_rate}]);
		{RateDivisor, RateDividend} ->
			prometheus_gauge:set(expected_minimum_200_years_storage_costs_decline_rate,
					ar_util:safe_divide(RateDivisor, RateDividend))
	end,
	case catch ar_pricing:get_expected_min_decline_rate(B#block.timestamp,
			Period_200_Years, B#block.reward_pool, B#block.weave_size, {1, 10},
			B#block.height) of
		{'EXIT', _} ->
			?LOG_ERROR([{event, failed_to_compute_expected_min_decline_rate2}]);
		{RateDivisor2, RateDividend2} ->
			prometheus_gauge:set(
					expected_minimum_200_years_storage_costs_decline_rate_10_usd_ar,
					ar_util:safe_divide(RateDivisor2, RateDividend2))
	end.

record_vdf_metrics(#block{ height = Height } = B, PrevB) ->
	case Height >= ar_fork:height_2_6() of
		true ->
			#nonce_limiter_info{ global_step_number = StepNumber }
					= B#block.nonce_limiter_info,
			#nonce_limiter_info{ global_step_number = PrevBStepNumber }
					= PrevB#block.nonce_limiter_info,
			prometheus_gauge:set(block_vdf_time, StepNumber - PrevBStepNumber);
		false ->
			ok
	end.

return_orphaned_txs_to_mempool(H, H) ->
	ok;
return_orphaned_txs_to_mempool(H, BaseH) ->
	#block{ txs = TXs, previous_block = PrevH } = ar_block_cache:get(block_cache, H),
	lists:foreach(fun(TX) ->
		ar_events:send(tx, {ready_for_mining, TX}),
		%% Add it to the mempool here even though have triggered an event - processes
		%% do not handle their own events.
		ar_mempool:add_tx(TX, ready_for_mining)
	end, TXs),
	return_orphaned_txs_to_mempool(PrevH, BaseH).

%% @doc Stop the current mining session and optionally start a new one,
%% depending on the automine setting.
may_be_reset_miner(#{ miner_2_5 := Miner_2_5, miner_2_6 := Miner_2_6,
		automine := false } = State) ->
	case Miner_2_5 of
		undefined ->
			ok;
		_ ->
			ar_mine:stop(Miner_2_5)
	end,
	case Miner_2_6 of
		undefined ->
			ok;
		_ ->
			ar_mining_server:pause()
	end,
	State#{ miner_2_5 => undefined, miner_2_6 => undefined };
may_be_reset_miner(#{ miner_2_5 := undefined } = State) ->
	start_mining(State);
may_be_reset_miner(#{ miner_2_5 := Pid } = State) ->
	ar_mine:stop(Pid),
	start_mining(State#{ miner_2_5 => undefined }).

start_mining(State) ->
	[{height, Height}] = ets:lookup(node_state, height),
	case Height + 1 >= ar_fork:height_2_6() of
		true ->
			Diff = get_current_diff(),
			case maps:get(miner_2_6, State) of
				undefined ->
					ar_mining_server:start_mining({Diff}),
					State#{ miner_2_6 => running };
				_ ->
					ar_mining_server:set_difficulty(Diff),
					State
			end;
		false ->
			start_2_5_mining(State)
	end.

get_current_diff() ->
	get_current_diff(os:system_time(second)).

get_current_diff(TS) ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', diff},
					{'==', '$1', last_retarget},
					{'==', '$1', timestamp}}], ['$_']}]
		),
	Height = proplists:get_value(height, Props),
	Diff = proplists:get_value(diff, Props),
	LastRetarget = proplists:get_value(last_retarget, Props),
	PrevTS = proplists:get_value(timestamp, Props),
	ar_retarget:maybe_retarget(Height + 1, Diff, TS, LastRetarget, PrevTS).

start_2_5_mining(State) ->
	State2 = may_be_start_2_5_io_threads(State),
	#{ tags := Tags, io_threads := IOThreads } = State2,
	[{recent_block_index, RecentBI}] = ets:lookup(node_state, recent_block_index),
	[{block_anchors, BlockAnchors}] = ets:lookup(node_state, block_anchors),
	[{recent_txs_map, RecentTXMap}] = ets:lookup(node_state, recent_txs_map),
	[{current, Current}] = ets:lookup(node_state, current),
	ar_watchdog:started_hashing(),
	B = ar_block_cache:get(block_cache, Current),
	PartitionUpperBound = ar_node:get_partition_upper_bound(RecentBI),
	{ok, Config} = application:get_env(arweave, config),
	RewardAddr = maps:get(reward_addr, State2, Config#config.mining_addr),
	Miner = ar_mine:start({B, collect_mining_transactions(?BLOCK_TX_COUNT_LIMIT),
			RewardAddr, Tags, BlockAnchors, RecentTXMap, PartitionUpperBound, IOThreads}),
	?LOG_INFO([{event, started_mining}]),
	State2#{ miner_2_5 => Miner }.

may_be_start_2_5_io_threads(#{ io_threads := IOThreads } = State) ->
	IOThreads2 =
		case IOThreads of
			[] ->
				start_2_5_io_threads();
			_ ->
				IOThreads
		end,
	State#{ io_threads => IOThreads2 }.

start_2_5_io_threads() ->
	ar_mine:start_io_threads().

collect_mining_transactions(Limit) ->
	collect_mining_transactions(Limit, ar_mempool:get_priority_set(), []).

collect_mining_transactions(0, _Set, TXs) ->
	TXs;
collect_mining_transactions(Limit, Set, TXs) ->
	case gb_sets:is_empty(Set) of
		true ->
			TXs;
		false ->
			{{_Utility, TXID, Status}, Set2} = gb_sets:take_largest(Set),
			case Status of
				ready_for_mining ->
					TX = ar_mempool:get_tx(TXID),
					collect_mining_transactions(Limit - 1, Set2, [TX | TXs]);
				_ ->
					collect_mining_transactions(Limit, Set2, TXs)
			end
	end.

record_processing_time(StartTimestamp) ->
	ProcessingTime = timer:now_diff(erlang:timestamp(), StartTimestamp) / 1000000,
	prometheus_histogram:observe(block_processing_time, ProcessingTime).

priority(apply_block) ->
	{1, 1};
priority({work_complete, _, _, _, _, _}) ->
	{2, 1};
priority({cache_missing_txs, _, _}) ->
	{3, 1};
priority(_) ->
	{os:system_time(second), 1}.

read_hash_list_2_0_for_1_0_blocks() ->
	Fork_2_0 = ar_fork:height_2_0(),
	case Fork_2_0 > 0 of
		true ->
			File = filename:join(["data", "hash_list_1_0"]),
			{ok, Binary} = file:read_file(File),
			HL = lists:map(fun ar_util:decode/1, jiffy:decode(Binary)),
			Fork_2_0 = length(HL),
			HL;
		false ->
			[]
	end.

start_from_block_index([#block{} = GenesisB], RewardHistory) ->
	BI = [ar_util:block_index_entry_from_block(GenesisB)],
	case ar_fork:height_2_6() of
		0 ->
			ok;
		_ ->
			ar_randomx_state:init(BI, [])
	end,
	self() ! {join, BI, [GenesisB#block{ reward_history = RewardHistory }]};
start_from_block_index(BI, RewardHistory) ->
	case length(BI) - 1 - ?STORE_BLOCKS_BEHIND_CURRENT > ar_fork:height_2_6() of
		true ->
			ok;
		_ ->
			ar_randomx_state:init(BI, [])
	end,
	Blocks = read_recent_blocks(BI),
	Blocks2 = ar_join:set_reward_history(Blocks, RewardHistory),
	self() ! {join, BI, Blocks2}.

read_recent_blocks(not_joined) ->
	[];
read_recent_blocks(BI) ->
	read_recent_blocks2(lists:sublist(BI, 2 * ?MAX_TX_ANCHOR_DEPTH)).

read_recent_blocks2([]) ->
	[];
read_recent_blocks2([{BH, _, _} | BI]) ->
	B = ar_storage:read_block(BH),
	TXs = ar_storage:read_tx(B#block.txs),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(TXs, B#block.height),
	[B#block{ size_tagged_txs = SizeTaggedTXs, txs = TXs } | read_recent_blocks2(BI)].

dump_mempool(TXs, MempoolSize) ->
	SerializedTXs = maps:map(fun(_, {TX, St}) -> {ar_serialize:tx_to_binary(TX), St} end, TXs),
	case ar_storage:write_term(mempool, {SerializedTXs, MempoolSize}) of
		ok ->
			ok;
		{error, Reason} ->
			?LOG_ERROR([{event, failed_to_dump_mempool}, {reason, Reason}])
	end.

