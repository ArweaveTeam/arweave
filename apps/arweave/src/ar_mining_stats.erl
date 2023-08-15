-module(ar_mining_stats).

-behaviour(gen_server).

-export([start_link/0, pause_performance_reports/1, increment_partition_stats/1,
		 increment_vdf_stats/0, reset_all_stats/0]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	pause_performance_reports	= false,
	pause_performance_reports_timeout
}).

-define(PERFORMANCE_REPORT_FREQUENCY_MS, 10000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Stop logging performance reports for the given number of milliseconds.
pause_performance_reports(Time) ->
	gen_server:cast(?MODULE, {pause_performance_reports, Time}).

increment_vdf_stats() ->
	ets:update_counter(?MODULE, vdf,
		[{3, 1}], 								     %% increment vdf count by 1
		{vdf, erlang:monotonic_time(millisecond), 0} %% initialize timestamp and count
	).

reset_vdf_stats(Now) ->
	ets:insert(?MODULE, [{vdf, Now, 0}]).

get_vdf_stats() ->
	ets:lookup(?MODULE, vdf).

increment_partition_stats(PartitionNumber) ->
	ets:update_counter(?MODULE, {partition, PartitionNumber},
		[
			{3, 1}, %% increment total count chunks read for partition PartitionNumber
			{5, 1}  %% increment current count chunks read for partition PartitionNumber
		],
		{
			{partition, PartitionNumber},
			erlang:monotonic_time(millisecond), 1, %% initialize total count and timestamp
			erlang:monotonic_time(millisecond), 1  %% initialize current count and timestamp
		}).

reset_current_partition_stats(PartitionNumber, Now) ->
	ets:update_counter(?MODULE, {partition, PartitionNumber},
		[
			{4, -1, Now, Now}, %% atomically reset the current count timestamp to Now
			{5, 0, -1, 0}      %% atomically reset the current count to 0
		]).

reset_partition_stats(PartitionNumber, Now) ->
	ets:insert(?MODULE, [{partition, PartitionNumber, Now, 0, Now, 0}]).

get_partition_stats(PartitionNumber) ->
	ets:lookup(?MODULE, {partition, PartitionNumber}).

reset_all_stats() ->
	Now = erlang:monotonic_time(millisecond),

	%% Reset partition stats
	PartitionNumbers = ets:match(?MODULE, {{partition, '$1'}, '_', '_', '_', '_'}),
    lists:foreach(
		fun([PartitionNumber]) ->
            reset_partition_stats(PartitionNumber, Now)
        end,
		PartitionNumbers),

	%% Reset vdf stats
	reset_vdf_stats(Now).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	gen_server:cast(?MODULE, report_performance),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

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
handle_cast(report_performance, State) ->
	report_performance(ar_mining_io:get_partitions()),
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

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

report_performance([]) ->
	ok;
report_performance(Partitions) ->
	Now = erlang:monotonic_time(millisecond),
	VdfSpeed = vdf_speed(Now),
	{IOList, MaxPartitionTime, PartitionsSum, MaxCurrentTime, CurrentsSum} =
		lists:foldr(
			fun({Partition, _ReplicaID, StoreID}, {Acc1, Acc2, Acc3, Acc4, Acc5} = Acc) ->
				case get_partition_stats(Partition) of
					[] ->
						Acc;
					[{_, PartitionStart, _, CurrentStart, _}]
							when Now - PartitionStart =:= 0
								orelse Now - CurrentStart =:= 0 ->
						Acc;
					[{_, PartitionStart, PartitionTotal, CurrentStart, CurrentTotal}] ->
						reset_current_partition_stats(Partition, Now),
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
	end.
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
	case get_vdf_stats() of
		[] ->
			undefined;
		[{_, Now, _}] ->
			undefined;
		[{_, _Now, 0}] ->
			undefined;
		[{_, VdfStart, VdfCount}] ->
			reset_vdf_stats(Now),
			VdfLapse = (Now - VdfStart) / 1000,
			VdfLapse / VdfCount
	end.
