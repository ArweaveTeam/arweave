-module(ar_mine).
-export([start/6, start/7, change_txs/2, stop/1, start_miner/2, schedule_hash/1]).
-export([validate/3, validate_by_hash/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A module for managing mining of blocks on the weave,

%% State record for miners
-record(state,{
	parent, % miners parent process (initiator)
	current_block, % current block held by node
	recall_block, % recall block related to current
	txs, % the set of txs to be mined
	timestamp, % the current timestamp of the miner
	data_segment = <<>>, % the data segment generated for mining
	reward_addr, % the nodes reward address
	tags, % the nodes block tags
	diff, % the current network difficulty
	delay = 0, % hashing delay used for testing
	max_miners = ?NUM_MINING_PROCESSES, % max mining process to start (ar.hrl)
	miners = [], % miner worker processes
	nonces % nonce builder to ensure entropy
}).

%% @doc Spawns a new mining process and returns its PID.
start(CurrentB, RecallB, RawTXs, RewardAddr, Tags, Parent) ->
	start(CurrentB, RecallB, RawTXs, RewardAddr, Tags, next_diff(CurrentB), Parent).

start(CurrentB, RecallB, RawTXs, unclaimed, Tags, Diff, Parent) ->
	start(CurrentB, RecallB, RawTXs, <<>>, Tags, Diff, Parent);
start(CurrentB, RecallB, RawTXs, RewardAddr, Tags, Diff, Parent) ->
	crypto:rand_seed(),
	Timestamp = os:system_time(seconds),
	%% Filter out invalid TXs. A TX could be valid by itself, but still invalid
	%% in the context of the other TXs and the block it would be mined to.
	TXs =
		lists:filter(
			fun(T) ->
				ar_tx:verify(T, Diff, CurrentB#block.wallet_list)
			end,
			ar_node_utils:filter_all_out_of_order_txs(
				CurrentB#block.wallet_list, RawTXs
			)
		),
	start_server(
		#state {
			parent = Parent,
			current_block = CurrentB,
			recall_block = RecallB,
			txs = TXs,
			timestamp = Timestamp,
			data_segment =
				ar_block:generate_block_data_segment(
					CurrentB,
					RecallB,
					TXs,
					RewardAddr,
					Timestamp,
					Tags
				),
			reward_addr = RewardAddr,
			tags = Tags,
			diff = Diff,
			max_miners = ar_meta_db:get(max_miners),
			nonces = []
		}
	).

%% @doc Stop a running mining server.
stop(PID) ->
	PID ! stop.

%% @doc Update the set of TXs that the miner is mining on.
change_txs(PID, NewTXs) ->
	PID ! {new_data, NewTXs}.

%% @doc Start the main mining server.
start_server(S) ->
	spawn(fun() ->
		server(start_miners(S))
	end).

%% @doc The main mining server.
server(
	S = #state {
		parent = Parent,
		timestamp = Timestamp,
		miners = Miners
	}
) ->
	receive
		% Stop the mining process killing all the workers.
		stop ->
			stop_miners(Miners),
			ok;
		% Update the miner to mine on a new set of data.
		{new_data, TXs} ->
			server(restart_miners(update_txs(S, TXs)));
		%% The block timestamp must be reasonable fresh since it's going to be
		%% validated on the remote nodes when it's propagated to them. Only blocks
		%% with a timestamp close to current time will be accepted in the propagation.
		refresh_timestamp ->
			case os:system_time(seconds) of
				CurrentTime when Timestamp > CurrentTime - ?MINING_TIMESTAMP_REFRESH_INTERVAL ->
					%% Something else (i.e. new TXs received) triggered an update
					%% recently, so this refresh is unessesary. A new refresh
					%% should already be scheduled by whatever triggered the
					%% recent refresh.
					server(S);
				CurrentTime ->
					server(restart_miners(update_timestamp(S, CurrentTime)))
			end;
		% Handle a potential solution for the mining puzzle.
		% Returns the solution back to the node to verify and ends the process.
		{solution, Hash, Nonces, MinedTXs, MinedDiff, MinedTimestamp} ->
			Parent ! {work_complete, MinedTXs, Hash, MinedDiff, Nonces, MinedTimestamp},
			stop_miners(Miners)
	end.

%% @doc Start the workers and return the new state.
start_miners(S = #state {max_miners = MaxMiners}) ->
	Miners =
		lists:map(
			fun(_) -> spawn(?MODULE, start_miner, [S, self()]) end,
			lists:seq(1, MaxMiners)
		),
	lists:foreach(
		fun(Pid) -> Pid ! hash end,
		Miners
	),
	schedule_refresh_timestamp(S#state.timestamp),
	S#state {miners = Miners}.

%% @doc Creates a timer for refreshing the block timestamp in the mining.
schedule_refresh_timestamp(Timestamp) ->
	case ?MINING_TIMESTAMP_REFRESH_INTERVAL - (os:system_time(seconds) - Timestamp) of
		NextRefreshSeconds when NextRefreshSeconds =< 0 ->
			ar:warn(
				"ar_mine: Cannot keep up refreshing the timestamp fast enough. Time margin (seconds): ~B",
				[NextRefreshSeconds]
			),
			self() ! refresh_timestamp;
		NextRefreshSeconds ->
			erlang:send_after(
				NextRefreshSeconds * 1000,
				self(),
				refresh_timestamp
			)
	end.

%% @doc Stop all workers.
stop_miners(Miners) ->
	lists:foreach(
		fun(Pid) -> Pid ! stop end,
		Miners
	).

%% @doc Stop and then start the workers again and return the new state.
restart_miners(S) ->
	stop_miners(S#state.miners),
	start_miners(S).

%% @doc Takes a state and a timestamp and returns an updated state with the new
%% timestamp.
update_timestamp(
	S = #state {
		current_block = CurrentB,
		recall_block = RecallB,
		txs = TXs,
		reward_addr = RewardAddr,
		tags = Tags
	},
	Timestamp
) ->
	BDS = ar_block:generate_block_data_segment(
		CurrentB,
		RecallB,
		TXs,
		RewardAddr,
		Timestamp,
		Tags
	),
	S#state {
		timestamp = Timestamp,
		data_segment = BDS
	}.

%% @doc Takes a state and a set of transactions and return a new state with the
%% new set of transactions.
update_txs(
	S = #state {
		current_block = CurrentB,
		recall_block = RecallB,
		reward_addr = RewardAddr,
		tags = Tags,
		diff = Diff
	},
	TXs
) ->
	CurrentTimestamp = os:system_time(seconds),
	ValidTXs =
		lists:filter(
			fun(TX) ->
				ar_tx:verify(TX, Diff, CurrentB#block.wallet_list)
			end,
			ar_node_utils:filter_all_out_of_order_txs(
				CurrentB#block.wallet_list,
				TXs
			)
		),
	S#state {
		txs = ValidTXs,
		timestamp = CurrentTimestamp,
		data_segment =
			ar_block:generate_block_data_segment(
				CurrentB,
				RecallB,
				ValidTXs,
				RewardAddr,
				CurrentTimestamp,
				Tags
			),
		diff = Diff
	}.

%% @doc A worker process to hash the data segment searching for a solution
%% for the given diff.
%% TODO: Change byte string for nonces to bitstring
start_miner(S, Supervisor) ->
	process_flag(priority, low),
	miner(S, Supervisor).

miner(
	S = #state {
		data_segment = BDS,
		diff = Diff,
		nonces = Nonces,
		txs = TXs,
		timestamp = Timestamp
	},
	Supervisor
) ->
	receive
		stop -> ok;
		hash ->
			schedule_hash(S),
			case validate(BDS, iolist_to_binary(Nonces), Diff) of
				false ->
					case(length(Nonces) > 512) and coinflip() of
						false ->
							miner(
								S#state {
									nonces =
										[bool_to_binary(coinflip()) | Nonces]
								},
								Supervisor
							);
						true ->
							miner(
								S#state {
									nonces = []
								},
								Supervisor
							)
					end;
				Hash ->
					Supervisor ! {solution, Hash, iolist_to_binary(Nonces), TXs, Diff, Timestamp}
			end
	end.

%% @doc Converts a boolean value to a binary of 0 or 1.
bool_to_binary(true) -> <<1>>;
bool_to_binary(false) -> <<0>>.

%% @doc A simple boolean coinflip.
coinflip() ->
	case rand:uniform(2) of
		1 -> true;
		2 -> false
	end.

%% @doc Schedule a hashing attempt.
%% Hashing attempts can be delayed for testing purposes.
schedule_hash(S = #state { delay = 0 }) ->
	self() ! hash,
	S;
schedule_hash(S = #state { delay = Delay }) ->
	Parent = self(),
	spawn(fun() -> receive after ar:scale_time(Delay) -> Parent ! hash end end),
	S.

%% @doc Given a block calculate the difficulty to mine on for the next block.
%% Difficulty is retargeted each ?RETARGET_BlOCKS blocks, specified in ar.hrl
%% This is done in attempt to maintain on average a fixed block time.
next_diff(CurrentB) ->
	ar_retarget:maybe_retarget(
		CurrentB#block.height + 1,
		CurrentB#block.diff,
		os:system_time(seconds),
		CurrentB#block.last_retarget
	).

%% @doc Validate that a given hash/nonce satisfy the difficulty requirement.
validate(BDS, Nonce, Diff) ->
	case NewHash = ar_weave:hash(BDS, Nonce) of
		<< 0:Diff, _/bitstring >> -> NewHash;
		_ -> false
	end.

%% @doc Validate that a given block data segment hash satisfies the difficulty requirement.
validate_by_hash(BDSHash, Diff) ->
	case BDSHash of
		<< 0:Diff, _/bitstring >> ->
			true;
		_ ->
			false
	end.

%%% Tests: ar_mine

%% @doc Test that found nonces abide by the difficulty criteria.
basic_test() ->
	B0 = ar_weave:init(),
	ar_node:start([], B0),
	B1 = ar_weave:add(B0, []),
	B = hd(B1),
	RecallB = hd(B0),
	start(B, RecallB, [], unclaimed, [], self()),
	receive
		{work_complete, MinedTXs, _Hash, Diff, Nonce, Timestamp} ->
			?assertEqual(MinedTXs, []),
			BDS = ar_block:generate_block_data_segment(
				B,
				RecallB,
				[],
				<<>>,
				Timestamp,
				[]
			),
			Res = crypto:hash(
				?MINING_HASH_ALG,
				<< Nonce/binary, BDS/binary >>
			),
			?assertMatch(
				<< 0:Diff, _/bitstring >>,
				Res
			)
	end.

%% @doc Ensure that we can change the transactions while mining is in progress.
change_txs_test() ->
	B0 = ar_weave:init(),
	ar_node:start([], B0),
	B1 = ar_weave:add(B0, []),
	B = hd(B1),
	RecallB = hd(B0),
	FirstTXSet = [ar_tx:new()],
	SecondTXSet = FirstTXSet ++ [ar_tx:new(), ar_tx:new()],
	%% Start mining with a very high difficulty, so that the mining won't finish
	%% before adding more TXs.
	VeryHighDiff = 100,
	PID = start(B, RecallB, FirstTXSet, unclaimed, [], VeryHighDiff, self()),
	%% Add more TXs. This will also re-calculate a new difficulty.
	change_txs(PID, SecondTXSet),
	receive
		{work_complete, SecondTXSet, Hash, Diff, Nonce, Timestamp} ->
			BDS = ar_block:generate_block_data_segment(
				B,
				RecallB,
				SecondTXSet,
				<<>>,
				Timestamp,
				[]
			),
			?assertEqual(
				Hash,
				crypto:hash(
					?MINING_HASH_ALG,
					<< Nonce/binary, BDS/binary >>
				)
			),
			?assertMatch(
				<< 0:Diff, _/bitstring >>,
				Hash
			)
	end.

%% @doc Ensure that an active miner process can be killed.
kill_miner_test() ->
	B0 = ar_weave:init(),
	ar_node:start([], B0),
	B1 = ar_weave:add(B0, []),
	B = hd(B1),
	RecallB = hd(B0),
	PID = start(B, RecallB, [], unclaimed, [], self()),
	erlang:monitor(process, PID),
	stop_miners([PID]),
	receive
		{'DOWN', _Ref, process, PID, normal} ->
			ok
	after 1000 ->
		erlang:error(no_match)
	end.
