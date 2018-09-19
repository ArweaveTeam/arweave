-module(ar_mine).
-export([start/6, start/7, change_data/2, stop/1, miner/2, schedule_hash/1]).
-export([validate/3, next_diff/1]).
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
start(CurrentB, RecallB, TXs, unclaimed, Tags, Parent) ->
	start(CurrentB, RecallB, TXs, <<>>, Tags, Parent);
start(CurrentB, RecallB, RawTXs, RewardAddr, Tags, Parent) ->
	start(CurrentB, RecallB, RawTXs, RewardAddr, Tags, next_diff(CurrentB), Parent).
start(CurrentB, RecallB, RawTXs, RewardAddr, Tags, Diff, Parent) ->
	crypto:rand_seed(),
	Timestamp = os:system_time(seconds),
	% Ensure that the txs in which the mining process is passed
	% validate and can be serialized.
	TXs =
		lists:filter(
			fun(T) ->
				ar_tx:verify(T, Diff, CurrentB#block.wallet_list)
			end,
			ar_node_utils:filter_all_out_of_order_txs(
				CurrentB#block.wallet_list, RawTXs
			)
		),
	PID = spawn(
		fun() ->
			server(
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
			)
		end
	),
	PID ! mine,
	PID.

%% @doc Stop a running mining server.
stop(PID) ->
	PID ! stop.

%% @doc Update the set of TXs that the miner is mining on.
change_data(PID, NewTXs) ->
	PID ! {new_data, NewTXs}.

%% @doc Schedule a timer to refresh data segment.
refresh_data_timer(PID) ->
	erlang:send_after(?REFRESH_MINE_DATA_TIMER, PID, {refresh_data, PID}).

%% @doc The main mining server.
server(
	S = #state {
		parent = Parent,
		current_block = CurrentB,
		recall_block = RecallB,
		txs = TXs,
		timestamp = Timestamp,
		reward_addr = RewardAddr,
		tags = Tags,
		diff = Diff,
		miners = Miners,
		max_miners = MaxMiners
	}) ->
	receive
		% Stop the mining process killing all the workers.
		stop ->
			lists:foreach(
				fun(Miner) -> Miner ! stop end,
				Miners
			),
			ok;
		% Update the miner to mine on a new set of data.
		{new_data, RawTXs} ->
			lists:foreach(
				fun(Miner) -> Miner ! stop end,
				Miners
			),
			self() ! mine,
			% Update mine loop to mine on the newly provided data.
			NewTimestamp = os:system_time(seconds),
			NewDiff = next_diff(CurrentB),
			NewTXs =
				lists:filter(
					fun(T) ->
						ar_tx:verify(T, Diff, CurrentB#block.wallet_list)
					end,
					ar_node_utils:filter_all_out_of_order_txs(
						CurrentB#block.wallet_list, RawTXs
					)
				),
			server(
				S#state {
					parent = Parent,
					current_block = CurrentB,
					recall_block = RecallB,
					txs = NewTXs,
					timestamp = NewTimestamp,
					reward_addr = RewardAddr,
					data_segment =
						ar_block:generate_block_data_segment(
							CurrentB,
							RecallB,
							NewTXs,
							RewardAddr,
							NewTimestamp,
							Tags
						),
					tags = Tags,
					diff = NewDiff
				}
			);
		% Refresh the mining data in case of diff change.
		{refresh_data, PID} ->
			ar:d({miner_data_refreshed}),
			spawn(
				fun() ->
					PID ! {new_data, TXs},
					refresh_data_timer(PID)
				end
			),
			server(S);
		% Spawn the hashing worker processes and begin to mine.
		mine ->
			Workers =
				lists:map(
					fun(_) -> spawn(?MODULE, miner, [S, self()]) end,
					lists:seq(1, MaxMiners)
				),
			lists:foreach(
				fun(Worker) -> Worker ! hash end,
				Workers
			),
			server(
				S#state {
					miners = Workers
				}
			);
		% Handle a potential solution for the mining puzzle.
		% Returns the solution back to the node to verify and ends the process.
		{solution, Hash, Nonce} ->
			lists:foreach(
				fun(Miner) -> Miner ! stop end,
				Miners
			),
			Parent ! {work_complete, TXs, Hash, Diff, Nonce, Timestamp}
	end.

%% @doc A worker process to hash the data segment searching for a solution
%% for the given diff.
%% TODO: Change byte string for nonces to bitstring
miner(
	S = #state {
		data_segment = DataSegment,
		diff = Diff,
		nonces = Nonces
	},
	Supervisor) ->
	receive
		stop -> ok;
		hash ->
			schedule_hash(S),
			case validate(DataSegment, iolist_to_binary(Nonces), Diff) of
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
					Supervisor ! {solution, Hash, iolist_to_binary(Nonces)}
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
	Timestamp = os:system_time(seconds),
	IsRetargetHeight = ar_retarget:is_retarget_height(CurrentB#block.height + 1),
	case IsRetargetHeight of
		true -> ar_retarget:maybe_retarget(
				CurrentB#block.height + 1,
				CurrentB#block.diff,
				Timestamp,
				CurrentB#block.last_retarget
			);
		false -> CurrentB#block.diff
	end.

%% @doc Validate that a given hash/nonce satisfy the difficulty requirement.
-ifdef(DEBUG).
validate(DataSegment, Nonce, Diff) ->
	NewDiff = Diff,
	case NewHash = ar_weave:hash(DataSegment, Nonce) of
		<< 0:NewDiff, _/bitstring >> -> NewHash;
		_ -> false
	end.
-else.
validate(DataSegment, Nonce, Diff) ->
	NewDiff = erlang:max(Diff, ?MIN_DIFF),
	case NewHash = ar_weave:hash(DataSegment, Nonce) of
		<< 0:NewDiff, _/bitstring >> -> NewHash;
		_ -> false
	end.
-endif.

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
		{work_complete, _MinedTXs, _Hash, Diff, Nonce, Timestamp} ->
			DataSegment = ar_block:generate_block_data_segment(
				B,
				RecallB,
				[],
				<<>>,
				Timestamp,
				[]
			),
			Res = crypto:hash(
				?MINING_HASH_ALG,
				<< Nonce/binary, DataSegment/binary >>
			),
			<< 0:Diff, _/bitstring >> = Res
	end.

%% @doc Ensure that we can change the data while mining is in progress.
change_data_test() ->
	B0 = ar_weave:init(),
	ar_node:start([], B0),
	B1 = ar_weave:add(B0, []),
	B = hd(B1),
	RecallB = hd(B0),
	TXs = [ar_tx:new()],
	NewTXs = TXs ++ [ar_tx:new(), ar_tx:new()],
	PID = start(B, RecallB, TXs, unclaimed, [], self()),
	change_data(PID, NewTXs),
	timer:sleep(500),
	receive
		{work_complete, MinedTXs, _Hash, Diff, Nonce, Timestamp} ->
			DataSegment = ar_block:generate_block_data_segment(
				B,
				RecallB,
				MinedTXs,
				<<>>,
				Timestamp,
				[]
			),
			Res = crypto:hash(
				?MINING_HASH_ALG,
				<< Nonce/binary, DataSegment/binary >>
			),
			<< 0:Diff, _/bitstring >> = Res,
			MinedTXs == NewTXs
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
	PID ! stop,
	receive
		{'DOWN', _Ref, process, PID, normal} -> ok
		after 1000 -> erlang:error(no_match)
	end.
