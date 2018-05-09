-module(ar_mine).
-export([start/5]).
-export([change_data/2, stop/1, validate/3, schedule_hash/1, miner/2]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% A module for managing mining of blocks on the weave.

%% State record for miners
-record(state,{
    parent, % parent process
    current_block,
    recall_block,
    txs,
    timestamp,
    data_segment = <<>>,
    reward_addr,
    tags,
    diff,
    delay = 0,
    max_miners = 4,
    miners = []
}).

start(CurrentB, RecallB, TXs, unclaimed, Tags) ->
    start(CurrentB, RecallB, TXs, <<>>, Tags);
%% @doc Returns the PID of a new mining worker process.
start(CurrentB, RecallB, RawTXs, RewardAddr, Tags) ->
    Parent = self(),
    Timestamp = os:system_time(seconds),
    Diff = next_diff(CurrentB),
    TXs = ar_node:filter_all_out_of_order_txs(CurrentB#block.wallet_list, RawTXs),
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
                    diff = Diff
                }
            )
        end
    ),
    PID ! mine,
    refresh_data_timer(PID),
    PID.


%% @doc Stop a running mining server.
stop(PID) ->
	PID ! stop.

%% @doc Change the data attachment that the miner is using.
change_data(PID, NewTXs) ->
    PID ! {new_data, NewTXs}.

%% Schedule a timer to refresh data segment.
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
        max_miners = Max
	}) ->
	receive
		stop ->
            % Kill all active workers
            lists:foreach(
                fun(Miner) -> Miner ! stop end,
                Miners
            ),
            ok;
		{new_data, RawTXs} ->
            % Kill all active workers
            lists:foreach(
                fun(Miner) -> Miner ! stop end,
                Miners
            ),
            %ar:d({updating_txs, RawTXs}),
            % Send mine message to self
            self() ! mine,
            % Continue server loop with new block_data_segment
            NewTimestamp = os:system_time(seconds),
            Diff = next_diff(CurrentB),
            WalletList = CurrentB#block.wallet_list,
            NewTXs = ar_node:filter_all_out_of_order_txs(WalletList, RawTXs),
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
                    diff = Diff
                }
            );
        {refresh_data, PID} ->
            ar:d({miner_data_refreshed}),
            spawn(
                fun() ->
                    PID ! {new_data, TXs},
                    refresh_data_timer(PID)
                end
            ),
            server(S);
		mine ->
            % Spawn the list of worker processes
            Workers =
                lists:map(
                    fun(_) -> spawn(?MODULE, miner, [S, self()]) end,
                    lists:seq(1, Max)
                ),
            % Tell each worker to start hashing
            lists:foreach(
                fun(Worker) -> Worker ! hash end,
                Workers
            ),
            % Continue server loop
            server(
                S#state {
                    miners = Workers
                }
            );
        {solution, Hash, Nonce} ->
            % Kill all active workers
            lists:foreach(
                fun(Miner) -> Miner ! stop end,
                Miners
            ),
            %ar:d({miner_txs, TXs}),
            % Send work complete data back to parent for verification
            %ar:d({miner, Hash, Nonce, Diff}),
            Parent ! {work_complete, TXs, Hash, Diff, Nonce, Timestamp}
    end.

miner(S = #state { data_segment = DataSegment, diff = Diff }, Supervisor) ->
    receive
        stop -> ok;
        hash ->
            schedule_hash(S),
            case validate(DataSegment, Nonce = generate_nonce(), Diff) of
                false -> miner(S, Supervisor);
                Hash -> Supervisor ! {solution, Hash, Nonce}
            end
    end.

schedule_hash(S = #state { delay = 0 }) ->
    self() ! hash,
    S;
schedule_hash(S = #state { delay = Delay }) ->
    Parent = self(),
    spawn(fun() -> receive after ar:scale_time(Delay) -> Parent ! hash end end),
    S.
next_diff(CurrentB) ->
    Timestamp = os:system_time(seconds),
    case ar_retarget:is_retarget_height(CurrentB#block.height + 1) of
        true -> ar_retarget:maybe_retarget(
                CurrentB#block.height + 1,
                CurrentB#block.diff,
                Timestamp,
                CurrentB#block.last_retarget
            );
        false -> CurrentB#block.diff
    end.

%% @doc Validate that a hash and a nonce satisfy the difficulty.
validate(DataSegment, Nonce, NewDiff) ->
    % ar:d({mine_validate, {data, DataSegment}, {nonce, Nonce}, {diff, NewDiff}, {ts, Timestamp}}),
    case NewHash = ar_weave:hash(DataSegment, Nonce) of
        << 0:NewDiff, _/bitstring >> -> NewHash;
        _ -> false
    end.

%% @doc Generate a random nonce, to be added to the previous hash.
generate_nonce() -> crypto:strong_rand_bytes(8).


%%% Tests

%% @doc Test that found nonces abide by the difficulty criteria.
basic_test() ->
    B = ar_block:new(),
    RecallB = ar_block:new(),
    start(B, RecallB, [], unclaimed, []),
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
            << 0:Diff, _/bitstring >>
                = crypto:hash(?HASH_ALG, << Nonce/binary, DataSegment/binary >>)
	end.

%% @doc Ensure that we can change the data while mining is in progress.
change_data_test() ->
    B = ar_block:new(),
    RecallB = ar_block:new(),
    TXs = [ar_tx:new()],
    NewTXs = TXs ++ [ar_tx:new(), ar_tx:new()],
    PID = start(B, RecallB, TXs, unclaimed, []),
    change_data(PID, NewTXs),
    receive after 500 -> ok end,
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
			<< 0:Diff, _/bitstring >>
                = crypto:hash(?HASH_ALG, << Nonce/binary, DataSegment/binary >>),
            MinedTXs == NewTXs

    end.

%% @doc Ensure that an active miner process can be killed.
kill_miner_test() ->
    B = ar_block:new(),
    RecallB = ar_block:new(),
    PID = start(B, RecallB, [], unclaimed, []),
    erlang:monitor(process, PID),
    PID ! stop,
    receive
        {'DOWN', _Ref, process, PID, normal} -> ok
        after 1000 -> erlang:error(no_match)
    end.
