-module(ar_mining_stats).
-behaviour(gen_server).

-export([start_link/0, start_performance_reports/0, pause_performance_reports/1, mining_paused/0,
		set_total_data_size/1, set_storage_module_data_size/6,
		vdf_computed/0, raw_read_rate/2, chunks_read/2, h1_computed/2, h2_computed/2,
		h1_solution/0, h2_solution/0, block_found/0, block_mined_but_orphaned/0,
		h1_sent_to_peer/2, h1_received_from_peer/2, h2_sent_to_peer/1, h2_received_from_peer/1,
		get_partition_data_size/2]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	pause_performance_reports = false,
	pause_performance_reports_timeout
}).

-record(report, {
	now,
	vdf_speed = undefined,
	h1_solution = 0,
	h2_solution = 0,
	confirmed_block = 0,
	total_data_size = 0,
	optimal_overall_read_mibps = 0.0,
	optimal_overall_hash_hps = 0.0,
	average_read_mibps = 0.0,
	current_read_mibps = 0.0,
	average_hash_hps = 0.0,
	current_hash_hps = 0.0,
	average_h1_to_peer_hps = 0.0,
	current_h1_to_peer_hps = 0.0,
	average_h1_from_peer_hps = 0.0,
	current_h1_from_peer_hps = 0.0,
	total_h2_to_peer = 0,
	total_h2_from_peer = 0,
	
	partitions = [],
	peers = []
}).

-record(partition_report, {
	partition_number,
	data_size,
	optimal_read_mibps,
	optimal_hash_hps,
	average_read_mibps,
	current_read_mibps,
	average_hash_hps,
	current_hash_hps
}).

-record(peer_report, {
	peer,
	average_h1_to_peer_hps,
	current_h1_to_peer_hps,
	average_h1_from_peer_hps,
	current_h1_from_peer_hps,
	total_h2_to_peer,
	total_h2_from_peer
}).

%% ETS table structure:
%%
%% {vdf, 													StartTime, Samples, VDFStepCount}
%% {h1_solution, 											StartTime, Samples, TotalH1SolutionsFound}
%% {h2_solution, 											StartTime, Samples, TotalH2SolutionsFound}
%% {confirmed_block, 										StartTime, Samples, TotalConfirmedBlocksMined}
%% {{partition, PartitionNumber, read, total}, 				StartTime, Samples, TotalChunksRead}
%% {{partition, PartitionNumber, read, current}, 			StartTime, Samples, CurrentChunksRead}
%% {{partition, PartitionNumber, h1, total}, 				StartTime, Samples, TotalH1}
%% {{partition, PartitionNumber, h1, current}, 				StartTime, Samples, CurrentH1}
%% {{partition, PartitionNumber, h2, total}, 				StartTime, Samples, TotalH2}
%% {{partition, PartitionNumber, h2, current}, 				StartTime, Samples, CurrentH2}
%% {total_data_size, 										TotalBytesPacked}
%% {{partition, PartitionNumber, storage_module, StoreID}, 	BytesPacked}
%% {{peer, Peer, h1_to_peer, total}, 						StartTime, Samples, TotalH1sSentToPeer}
%% {{peer, Peer, h1_to_peer, current}, 						StartTime, Samples, CurrentH1sSentToPeer}
%% {{peer, Peer, h1_from_peer, total}, 						StartTime, Samples, TotalH1sReceivedFromPeer}
%% {{peer, Peer, h1_from_peer, current}, 					StartTime, Samples, CurrentH1sReceivedFromPeer}
%% {{peer, Peer, h2_to_peer, total}, 						StartTime, Samples, TotalH2sSentToPeer}
%% {{peer, Peer, h2_from_peer, total}, 						StartTime, Samples, TotalH2sReceivedFromPeer}

-define(PERFORMANCE_REPORT_FREQUENCY_MS, 10000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the gen_server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_performance_reports() ->
	reset_all_stats(),
	ar_util:cast_after(?PERFORMANCE_REPORT_FREQUENCY_MS, ?MODULE, report_performance).

%% @doc Stop logging performance reports for the given number of milliseconds.
pause_performance_reports(Time) ->
	gen_server:call(?MODULE, {pause_performance_reports, Time}).

vdf_computed() ->
	vdf_computed(erlang:monotonic_time(millisecond)).

vdf_computed(Now) ->
	increment_count(vdf, 1, Now).

raw_read_rate(PartitionNumber, ReadRate) ->
	prometheus_gauge:set(mining_rate, [raw_read, PartitionNumber], ReadRate).

chunks_read(PartitionNumber, Count) ->
	chunks_read(PartitionNumber, Count, erlang:monotonic_time(millisecond)).

chunks_read(PartitionNumber, Count, Now) ->
	increment_count({partition, PartitionNumber, read, total}, Count, Now),
	increment_count({partition, PartitionNumber, read, current}, Count, Now).

h1_computed(PartitionNumber, Count) ->
	h1_computed(PartitionNumber, Count, erlang:monotonic_time(millisecond)).

h1_computed(PartitionNumber, Count, Now) ->
	increment_count({partition, PartitionNumber, h1, total}, Count, Now),
	increment_count({partition, PartitionNumber, h1, current}, Count, Now).

h2_computed(PartitionNumber, Count) ->
	h2_computed(PartitionNumber, Count, erlang:monotonic_time(millisecond)).

h2_computed(PartitionNumber, Count, Now) ->
	increment_count({partition, PartitionNumber, h2, total}, Count, Now),
	increment_count({partition, PartitionNumber, h2, current}, Count, Now).

h1_sent_to_peer(Peer, H1Count) ->
	h1_sent_to_peer(Peer, H1Count, erlang:monotonic_time(millisecond)).

h1_sent_to_peer(Peer, H1Count, Now) ->
	increment_count({peer, Peer, h1_to_peer, total}, H1Count, Now),
	increment_count({peer, Peer, h1_to_peer, current}, H1Count, Now).

h1_received_from_peer(Peer, H1Count) ->
	h1_received_from_peer(Peer, H1Count, erlang:monotonic_time(millisecond)).

h1_received_from_peer(Peer, H1Count, Now) ->
	increment_count({peer, Peer, h1_from_peer, total}, H1Count, Now),
	increment_count({peer, Peer, h1_from_peer, current}, H1Count, Now).

h2_sent_to_peer(Peer) ->
	h2_sent_to_peer(Peer, erlang:monotonic_time(millisecond)).

h2_sent_to_peer(Peer, Now) ->
	increment_count({peer, Peer, h2_to_peer, total}, 1, Now).

h2_received_from_peer(Peer) ->
	h2_received_from_peer(Peer, erlang:monotonic_time(millisecond)).

h2_received_from_peer(Peer, Now) ->
	increment_count({peer, Peer, h2_from_peer, total}, 1, Now).

h1_solution() ->
	h1_solution(erlang:monotonic_time(millisecond)).

h1_solution(Now) ->
	increment_count(h1_solution, 1, Now).

h2_solution() ->
	h2_solution(erlang:monotonic_time(millisecond)).

h2_solution(Now) ->
	increment_count(h2_solution, 1, Now).

block_found() ->
	block_found(erlang:monotonic_time(millisecond)).

block_found(Now) ->
	increment_count(confirmed_block, 1, Now).

block_mined_but_orphaned() ->
	block_mined_but_orphaned(erlang:monotonic_time(millisecond)).

block_mined_but_orphaned(Now) ->
	increment_count(block_mined_but_orphaned, 1, Now).

set_total_data_size(DataSize) ->
	try
		prometheus_gauge:set(v2_index_data_size, DataSize),
		ets:insert(?MODULE, {total_data_size, DataSize})
	catch
		error:badarg ->
			?LOG_WARNING([{event, set_total_data_size_failed},
				{reason, prometheus_not_started}, {data_size, DataSize}]);
		Type:Reason ->
			?LOG_ERROR([{event, set_total_data_size_failed},
				{type, Type}, {reason, Reason}, {data_size, DataSize}])
	end.

set_storage_module_data_size(
		StoreID, Packing, PartitionNumber, StorageModuleSize, StorageModuleIndex, DataSize) ->
	StoreIDLabel = ar_storage_module:label(StoreID),
	PackingLabel = ar_storage_module:packing_label(Packing),
	try	
		PackingDifficulty = ar_mining_server:get_packing_difficulty(Packing),
		prometheus_gauge:set(v2_index_data_size_by_packing,
			[StoreIDLabel, PackingLabel, PartitionNumber,
			 StorageModuleSize, StorageModuleIndex,
			 PackingDifficulty],
			DataSize),
		ets:insert(?MODULE, {
			{partition, PartitionNumber, storage_module, StoreID, packing, Packing}, DataSize})
	catch
		error:badarg ->
			?LOG_WARNING([{event, set_storage_module_data_size_failed},
				{reason, prometheus_not_started},
				{store_id, StoreID}, {store_id_label, StoreIDLabel},
				{packing, ar_serialize:encode_packing(Packing, true)},
				{packing_label, PackingLabel},
				{partition_number, PartitionNumber}, {storage_module_size, StorageModuleSize},
				{storage_module_index, StorageModuleIndex}, {data_size, DataSize}]);
		error:{unknown_metric,default,v2_index_data_size_by_packing} ->
			?LOG_WARNING([{event, set_storage_module_data_size_failed},
				{reason, prometheus_not_started},
				{store_id, StoreID}, {store_id_label, StoreIDLabel},
				{packing, ar_serialize:encode_packing(Packing, true)},
				{packing_label, PackingLabel},
				{partition_number, PartitionNumber}, {storage_module_size, StorageModuleSize},
				{storage_module_index, StorageModuleIndex}, {data_size, DataSize}]);
		Type:Reason ->
			?LOG_ERROR([{event, set_storage_module_data_size_failed},
				{type, Type}, {reason, Reason},
				{store_id, StoreID}, {store_id_label, StoreIDLabel},
				{packing, ar_serialize:encode_packing(Packing, true)},
				{packing_label, PackingLabel},
				{partition_number, PartitionNumber}, {storage_module_size, StorageModuleSize},
				{storage_module_index, StorageModuleIndex}, {data_size, DataSize} ])
	end.

mining_paused() ->
	clear_metrics().

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	{ok, #state{}}.

handle_call({pause_performance_reports, Time}, _From, State) ->
	Now = os:system_time(millisecond),
	Timeout = Now + Time,
	{reply, ok, State#state{ pause_performance_reports = true,
			pause_performance_reports_timeout = Timeout }};

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
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
	report_performance(),
	ar_util:cast_after(?PERFORMANCE_REPORT_FREQUENCY_MS, ?MODULE, report_performance),
	{noreply, State};



handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

reset_all_stats() ->
	ets:delete_all_objects(?MODULE).

%% @doc Atomically increments the count for ETS records stored in the format:
%% {Key, StartTimestamp, Count}
%% If the Key doesn't exist, it is initialized with the current timestamp and a count of Amount
increment_count(_Key, 0, _Now) ->
	ok;
increment_count(Key, Amount, Now) ->
	ets:update_counter(?MODULE, Key,
		[{3, 1}, {4, Amount}], 						%% increment samples by 1, count by Amount
		{Key, Now, 0, 0} %% initialize timestamp, samples, count
	).

reset_count(Key, Now) ->
	ets:insert(?MODULE, [{Key, Now, 0, 0}]).

get_average_count_by_time(Key, Now) ->
	{_AvgSamples, AvgCount} = get_average_by_time(Key, Now),
	AvgCount.

get_average_samples_by_time(Key, Now) ->
	{AvgSamples, _AvgCount} = get_average_by_time(Key, Now),
	AvgSamples.

get_average_by_time(Key, Now) ->
	case ets:lookup(?MODULE, Key) of 
		[] ->
			{0.0, 0.0};
		[{_, Start, _Samples, _Count}] when Now - Start =:= 0 ->
			{0.0, 0.0};
		[{_, Start, Samples, Count}] ->
			Elapsed = (Now - Start) / 1000,
			{Samples / Elapsed, Count / Elapsed}
	end.

get_average_by_samples(Key) ->
	case ets:lookup(?MODULE, Key) of 
		[] ->
			0.0;
		[{_, _Start, Samples, _Count}] when Samples == 0 ->
			0.0;
		[{_, _Start, Samples, Count}] ->
			Count / Samples
	end.


get_count(Key) ->
	case ets:lookup(?MODULE, Key) of 
		[] ->
			0;
		[{_, _Start, _Samples, Count}] ->
			Count
	end.

get_start(Key) ->
	case ets:lookup(?MODULE, Key) of 
		[] ->
			undefined;
		[{_, Start, _Samples, _Count}] ->
			Start
	end.

get_hashrate_divisor(PackingDifficulty) ->
	%% Raw hashrate varies based on packing difficulty. Assuming a spora_2_6 base hashrate
	%% of 404, the raw hashrate at different packing difficulties is:
	%% spora_2_6: 404
	%% composite, 1: 404 * 32 / 4 / 1 = 3232
	%% composite, 2: 404 * 32 / 4 / 2 = 1616
	%% composite, 32: 404 * 32 / 4 / 32 = 101
	%%
	%% Basically:
	%% - composite packing generate 32x the number of hashes, but they are compared against
	%%   a higher solution difficulty
	%% - composite uses a 4x lower read recall range which *reduces* the number of hashes
	%%   4-fold, and increases the solution difficulty
	%% - finally as the difficulty increases, the number of hashes generated decreases as does
	%%   the solution difficulty
	%%
	%% This function returns a divisor we can use to normalize the hahsrate to 404.
	case PackingDifficulty of
		0 ->
			1.0;
		_ ->
			(32.0 / 4.0) / PackingDifficulty
	end.

get_total_minable_data_size(Packing) ->
	Pattern = {
		{partition, '_', storage_module, '_', packing, Packing}, '$1'
	},
	Sizes = [Size || [Size] <- ets:match(?MODULE, Pattern)],
	TotalDataSize = lists:sum(Sizes),

	WeaveSize = ar_node:get_weave_size(),
	TipPartition = ar_node:get_max_partition_number(WeaveSize) + 1,
	TipPartitionSize = get_partition_data_size(TipPartition, Packing),
	?LOG_DEBUG([{event, get_total_minable_data_size},
		{total_data_size, TotalDataSize}, {weave_size, WeaveSize},
		{tip_partition, TipPartition}, {tip_partition_size, TipPartitionSize},
		{total_minable_data_size, TotalDataSize - TipPartitionSize}	]),
	TotalDataSize - TipPartitionSize.

get_overall_total(PartitionPeer, Stat, TotalCurrent) ->
	Pattern = {{PartitionPeer, '_', Stat, TotalCurrent}, '_', '_', '$1'},
    Matches = ets:match(?MODULE, Pattern),
	Counts = [Count || [Count] <- Matches],
	lists:sum(Counts).

get_partition_data_size(PartitionNumber, Packing) ->
	Pattern = {{partition, PartitionNumber, storage_module, '_', packing, Packing }, '$1'},
	Sizes = [Size || [Size] <- ets:match(?MODULE, Pattern)],
    lists:sum(Sizes).

vdf_speed(Now) ->
	case get_average_count_by_time(vdf, Now) of
		0.0 ->
			undefined;
		StepsPerSecond ->
			reset_count(vdf, Now),
			1.0 / StepsPerSecond
	end.

get_hash_hps(PoA1Multiplier, Packing, PartitionNumber, TotalCurrent, Now) ->
	H1 = get_average_count_by_time({partition, PartitionNumber, h1, TotalCurrent}, Now),
	H2 = get_average_count_by_time({partition, PartitionNumber, h2, TotalCurrent}, Now),
	PackingDifficulty = ar_mining_server:get_packing_difficulty(Packing),
	((H1 / PoA1Multiplier) + H2) / get_hashrate_divisor(PackingDifficulty).

%% @doc calculate the maximum hash rate (in MiB per second read from disk) for the given VDF
%% speed at the current weave size.
optimal_partition_read_mibps(_Packing, undefined, _PartitionDataSize, _TotalDataSize, _WeaveSize) ->
	0.0;	
optimal_partition_read_mibps(Packing, VDFSpeed, PartitionDataSize, TotalDataSize, WeaveSize) ->
	PackingDifficulty = ar_mining_server:get_packing_difficulty(Packing),
	RecallRangeSize = ar_block:get_recall_range_size(PackingDifficulty) / ?MiB,
	(RecallRangeSize / VDFSpeed) *
	min(1.0, (PartitionDataSize / ar_block:partition_size())) *
	(1 + min(1.0, (TotalDataSize / WeaveSize))).

%% @doc calculate the maximum hash rate (in hashes per second) for the given VDF speed
%% at the current weave size.
optimal_partition_hash_hps(_PoA1Multiplier, undefined, _PartitionDataSize, _TotalDataSize, _WeaveSize) ->
	0.0;	
optimal_partition_hash_hps(PoA1Multiplier, VDFSpeed, PartitionDataSize, TotalDataSize, WeaveSize) ->
	BasePartitionHashes = (400.0 / VDFSpeed) * min(1.0, (PartitionDataSize / ar_block:partition_size())),
	H1Optimal = BasePartitionHashes / PoA1Multiplier,
	H2Optimal = BasePartitionHashes * min(1.0, (TotalDataSize / WeaveSize)),
	H1Optimal + H2Optimal.

generate_report() ->
	Height = ar_node:get_height(),
	Packing = ar_mining_io:get_packing(),
	Partitions = ar_mining_io:get_partitions(),
	generate_report(
		Height,
		Packing,
		Partitions,
		arweave_config:get(cm_peers),
		ar_node:get_weave_size(),
		erlang:monotonic_time(millisecond)
	).

generate_report(_Height, _Packing, [], _Peers, _WeaveSize, Now) ->
	#report{
		now = Now
	};
generate_report(Height, Packing, Partitions, Peers, WeaveSize, Now) ->
	PoA1Multiplier = ar_difficulty:poa1_diff_multiplier(Height),
	VDFSpeed = vdf_speed(Now),
	TotalDataSize = get_total_minable_data_size(Packing),
	Report = #report{
		now = Now,
		vdf_speed = VDFSpeed,
		h1_solution = get_count(h1_solution),
		h2_solution = get_count(h2_solution),
		confirmed_block = get_count(confirmed_block),
		total_data_size = TotalDataSize,
		total_h2_to_peer = get_overall_total(peer, h2_to_peer, total),
		total_h2_from_peer = get_overall_total(peer, h2_from_peer, total)
	},

	Report2 = generate_partition_reports(
		PoA1Multiplier, Partitions, Packing, Report, WeaveSize),
	Report3 = generate_peer_reports(Peers, Report2),
	Report3.

generate_partition_reports(PoA1Multiplier, Partitions, Packing, Report, WeaveSize) ->
	lists:foldr(
		fun({PartitionNumber, _MiningAddr, _PackingDifficulty}, Acc) ->
			generate_partition_report(
				PoA1Multiplier, PartitionNumber, Packing, Acc, WeaveSize)
		end,
		Report,
		Partitions
	).

generate_partition_report(
		PoA1Multiplier, PartitionNumber, Packing, Report, WeaveSize) ->
	#report{
		now = Now,
		vdf_speed = VDFSpeed,
		total_data_size = TotalDataSize,
		partitions = Partitions,
		optimal_overall_read_mibps = OptimalOverallRead,
		optimal_overall_hash_hps = OptimalOverallHash,
		average_read_mibps = AverageRead,
		current_read_mibps = CurrentRead,
		average_hash_hps = AverageHash,
		current_hash_hps = CurrentHash } = Report,
	DataSize = get_partition_data_size(PartitionNumber, Packing),
	PartitionReport = #partition_report{
		partition_number = PartitionNumber,
		data_size = DataSize,
		average_read_mibps = get_average_count_by_time(
				{partition, PartitionNumber, read, total}, Now) / 4,
		current_read_mibps = get_average_count_by_time(
				{partition, PartitionNumber, read, current}, Now) / 4,
		average_hash_hps = get_hash_hps(
			PoA1Multiplier, Packing, PartitionNumber, total, Now),
		current_hash_hps = get_hash_hps(
			PoA1Multiplier, Packing, PartitionNumber, current, Now),
		optimal_read_mibps = optimal_partition_read_mibps(
			Packing, VDFSpeed, DataSize, TotalDataSize, WeaveSize),
		optimal_hash_hps = optimal_partition_hash_hps(
			PoA1Multiplier, VDFSpeed, DataSize, TotalDataSize, WeaveSize)
	},

	reset_count({partition, PartitionNumber, read, current}, Now),
	reset_count({partition, PartitionNumber, h1, current}, Now),
	reset_count({partition, PartitionNumber, h2, current}, Now),

	Report#report{ 
		optimal_overall_read_mibps = 
			OptimalOverallRead + PartitionReport#partition_report.optimal_read_mibps,
		optimal_overall_hash_hps = 
			OptimalOverallHash + PartitionReport#partition_report.optimal_hash_hps,
		average_read_mibps = AverageRead + PartitionReport#partition_report.average_read_mibps,
		current_read_mibps = CurrentRead + PartitionReport#partition_report.current_read_mibps,
		average_hash_hps = AverageHash + PartitionReport#partition_report.average_hash_hps,
		current_hash_hps = CurrentHash + PartitionReport#partition_report.current_hash_hps,
		partitions = Partitions ++ [PartitionReport] }.

generate_peer_reports(Peers, Report) ->
	lists:foldr(
		fun(Peer, Acc) ->
			generate_peer_report(Peer, Acc)
		end,
		Report,
		Peers
	).

generate_peer_report(Peer, Report) ->
	#report{
		now = Now,
		peers = Peers,
		average_h1_to_peer_hps = AverageH1ToPeer,
		current_h1_to_peer_hps = CurrentH1ToPeer,
		average_h1_from_peer_hps = AverageH1FromPeer,
		current_h1_from_peer_hps = CurrentH1FromPeer } = Report,
	PeerReport = #peer_report{
		peer = Peer,
		average_h1_to_peer_hps =
			get_average_count_by_time({peer, Peer, h1_to_peer, total}, Now),
		current_h1_to_peer_hps =
			get_average_count_by_time({peer, Peer, h1_to_peer, current}, Now),
		average_h1_from_peer_hps =
			get_average_count_by_time({peer, Peer, h1_from_peer, total}, Now),
		current_h1_from_peer_hps =
			get_average_count_by_time({peer, Peer, h1_from_peer, current}, Now),
		total_h2_to_peer = get_count({peer, Peer, h2_to_peer, total}),
		total_h2_from_peer = get_count({peer, Peer, h2_from_peer, total})
	},

	reset_count({peer, Peer, h1_to_peer, current}, Now),
	reset_count({peer, Peer, h1_from_peer, current}, Now),

	Report#report{
		peers = Peers ++ [PeerReport],
		average_h1_to_peer_hps =
			AverageH1ToPeer + PeerReport#peer_report.average_h1_to_peer_hps,
		current_h1_to_peer_hps =
			CurrentH1ToPeer + PeerReport#peer_report.current_h1_to_peer_hps,
		average_h1_from_peer_hps =
			AverageH1FromPeer + PeerReport#peer_report.average_h1_from_peer_hps,
		current_h1_from_peer_hps =
			CurrentH1FromPeer + PeerReport#peer_report.current_h1_from_peer_hps
	}.

report_performance() ->
	Report = generate_report(),
	case Report#report.partitions of
		[] ->
			ok;
		_ ->
			set_metrics(Report),
			ReportString = format_report(Report),
			ar:console("~s", [ReportString]),
			log_report(ReportString)
	end.

log_report(ReportString) ->
	Lines = string:tokens(lists:flatten(ReportString), "\n"),
	log_report_lines(Lines).

log_report_lines([]) ->
	ok;
log_report_lines([Line | Lines]) ->
	?LOG_INFO(Line),
	log_report_lines(Lines).

set_metrics(Report) ->
	prometheus_gauge:set(mining_rate, [read, total], Report#report.current_read_mibps),
	prometheus_gauge:set(mining_rate, [hash, total],  Report#report.current_hash_hps),
	prometheus_gauge:set(mining_rate, [ideal_read, total],  Report#report.optimal_overall_read_mibps),
	prometheus_gauge:set(mining_rate, [ideal_hash, total],  Report#report.optimal_overall_hash_hps),
	prometheus_gauge:set(cm_h1_rate, [total, to], Report#report.current_h1_to_peer_hps),
	prometheus_gauge:set(cm_h1_rate, [total, from], Report#report.current_h1_from_peer_hps),
	prometheus_gauge:set(cm_h2_count, [total, to], Report#report.total_h2_to_peer),
	prometheus_gauge:set(cm_h2_count, [total, from], Report#report.total_h2_from_peer),
	set_partition_metrics(Report#report.partitions),
	set_peer_metrics(Report#report.peers).

set_partition_metrics([]) ->
	ok;
set_partition_metrics([PartitionReport | PartitionReports]) ->
	PartitionNumber = PartitionReport#partition_report.partition_number,
	prometheus_gauge:set(mining_rate, [read, PartitionNumber],
		PartitionReport#partition_report.current_read_mibps),
	prometheus_gauge:set(mining_rate, [hash, PartitionNumber],
		PartitionReport#partition_report.current_hash_hps),
	prometheus_gauge:set(mining_rate, [ideal_read, PartitionNumber],
		PartitionReport#partition_report.optimal_read_mibps),
	prometheus_gauge:set(mining_rate, [ideal_hash, PartitionNumber],
		PartitionReport#partition_report.optimal_hash_hps),
	set_partition_metrics(PartitionReports).

set_peer_metrics([]) ->
	ok;
set_peer_metrics([PeerReport | PeerReports]) ->
	Peer = ar_util:format_peer(PeerReport#peer_report.peer),
	prometheus_gauge:set(cm_h1_rate, [Peer, to],
		PeerReport#peer_report.current_h1_to_peer_hps),
	prometheus_gauge:set(cm_h1_rate, [Peer, from],
		PeerReport#peer_report.current_h1_from_peer_hps),
	prometheus_gauge:set(cm_h2_count, [Peer, to],
		PeerReport#peer_report.total_h2_to_peer),
	prometheus_gauge:set(cm_h2_count, [Peer, from],
		PeerReport#peer_report.total_h2_from_peer),
	set_peer_metrics(PeerReports).

clear_metrics() ->
	Report = generate_report(),
	prometheus_gauge:set(mining_rate, [read, total], 0),
	prometheus_gauge:set(mining_rate, [hash, total],  0),
	prometheus_gauge:set(mining_rate, [ideal, total],  0),
	prometheus_gauge:set(cm_h1_rate, [total, to], 0),
	prometheus_gauge:set(cm_h1_rate, [total, from], 0),
	prometheus_gauge:set(cm_h2_count, [total, to], 0),
	prometheus_gauge:set(cm_h2_count, [total, from], 0),
	clear_partition_metrics(Report#report.partitions),
	clear_peer_metrics(Report#report.peers).

clear_partition_metrics([]) ->
	ok;
clear_partition_metrics([PartitionReport | PartitionReports]) ->
	PartitionNumber = PartitionReport#partition_report.partition_number,
	prometheus_gauge:set(mining_rate, [read, PartitionNumber], 0),
	prometheus_gauge:set(mining_rate, [hash, PartitionNumber], 0),
	prometheus_gauge:set(mining_rate, [ideal, PartitionNumber], 0),
	clear_partition_metrics(PartitionReports).

clear_peer_metrics([]) ->
	ok;
clear_peer_metrics([PeerReport | PeerReports]) ->
	Peer = ar_util:format_peer(PeerReport#peer_report.peer),
	prometheus_gauge:set(cm_h1_rate, [Peer, to], 0),
	prometheus_gauge:set(cm_h1_rate, [Peer, from], 0),
	prometheus_gauge:set(cm_h2_count, [Peer, to], 0),
	prometheus_gauge:set(cm_h2_count, [Peer, from], 0),
	clear_peer_metrics(PeerReports).

format_report(Report) ->
	format_report(Report, ar_node:get_weave_size()).
format_report(Report, WeaveSize) ->
	Preamble = io_lib:format(
		"================================================= Mining Performance Report =================================================\n"
		"\n"
		"VDF Speed:       ~s\n"
		"H1 Solutions:     ~B\n"
		"H2 Solutions:     ~B\n"
		"Confirmed Blocks: ~B\n"
		"\n",
		[format_vdf_speed(Report#report.vdf_speed), Report#report.h1_solution,
			Report#report.h2_solution, Report#report.confirmed_block]
	),
	PartitionTable = format_partition_report(Report, WeaveSize),
	PeerTable = format_peer_report(Report),
    
    io_lib:format("\n~s~s~s", [Preamble, PartitionTable, PeerTable]).

format_partition_report(Report, WeaveSize) ->
	Header = 
		"Local mining stats:\n"
		"+-----------+-----------+----------+---------------+---------------+---------------+------------+------------+--------------+\n"
        "| Partition | Data Size | % of Max |   Read (Cur)  |   Read (Avg)  |  Read (Ideal) | Hash (Cur) | Hash (Avg) | Hash (Ideal) |\n"
		"+-----------+-----------+----------+---------------+---------------+---------------+------------+------------+--------------+\n",
	TotalRow = format_partition_total_row(Report, WeaveSize),
	PartitionRows = format_partition_rows(Report#report.partitions),
    Footer =
		"+-----------+-----------+----------+---------------+---------------+---------------+------------+------------+--------------+\n",
	io_lib:format("~s~s~s~s", [Header, TotalRow, PartitionRows, Footer]).

format_partition_total_row(Report, WeaveSize) ->
	#report{
		total_data_size = TotalDataSize,
		optimal_overall_read_mibps = OptimalOverallRead,
		optimal_overall_hash_hps = OptimalOverallHash,
		average_read_mibps = AverageRead,
		current_read_mibps = CurrentRead,
		average_hash_hps = AverageHash,
		current_hash_hps = CurrentHash } = Report,
	TotalTiB = TotalDataSize / ?TiB,
	PctOfWeave = floor((TotalDataSize / WeaveSize) * 100),
    io_lib:format(
		"|     Total | ~5.1f TiB | ~6.B % "
		"| ~7.1f MiB/s | ~7.1f MiB/s | ~7.1f MiB/s "
		"| ~6B h/s | ~6B h/s | ~8B h/s |\n",
		[
			TotalTiB, PctOfWeave,
			CurrentRead, AverageRead, OptimalOverallRead,
			floor(CurrentHash), floor(AverageHash), floor(OptimalOverallHash)]).

format_partition_rows([]) ->
	"";
format_partition_rows([PartitionReport | PartitionReports]) ->
	format_partition_rows(PartitionReports) ++
	[format_partition_row(PartitionReport)].

format_partition_row(PartitionReport) ->
	#partition_report{
		partition_number = PartitionNumber,
		data_size = DataSize,
		optimal_read_mibps = OptimalRead,
		average_read_mibps = AverageRead,
		current_read_mibps = CurrentRead,
		optimal_hash_hps = OptimalHash,
		average_hash_hps = AverageHash,
		current_hash_hps = CurrentHash } = PartitionReport,
	TiB = DataSize / ?TiB,
	PctOfPartition = floor((DataSize / ar_block:partition_size()) * 100),
    io_lib:format(
		"| ~9.B | ~5.1f TiB | ~6.B % "
		"| ~7.1f MiB/s | ~7.1f MiB/s | ~7.1f MiB/s "
		"| ~6B h/s | ~6B h/s | ~8B h/s |\n",
		[
			PartitionNumber, TiB, PctOfPartition,
			CurrentRead, AverageRead, OptimalRead,
			floor(CurrentHash), floor(AverageHash), floor(OptimalHash)]).

format_peer_report(#report{ peers = [] }) ->
	"";
format_peer_report(Report) ->
	Header = 
		"\n"
		"Coordinated mining cluster stats:\n"
		"+----------------------+--------------+--------------+-------------+-------------+--------+--------+\n"
        "|                 Peer | H1 Out (Cur) | H1 Out (Avg) | H1 In (Cur) | H1 In (Avg) | H2 Out |  H2 In |\n"
		"+----------------------+--------------+--------------+-------------+-------------+--------+--------+\n",
	TotalRow = format_peer_total_row(Report),
	PartitionRows = format_peer_rows(Report#report.peers),
    Footer =
		"+----------------------+--------------+--------------+-------------+-------------+--------+--------+\n",
	io_lib:format("~s~s~s~s", [Header, TotalRow, PartitionRows, Footer]).

format_peer_total_row(Report) ->
	#report{
		average_h1_to_peer_hps = AverageH1To,
		current_h1_to_peer_hps = CurrentH1To,
		average_h1_from_peer_hps = AverageH1From,
		current_h1_from_peer_hps = CurrentH1From,
		total_h2_to_peer = TotalH2To,
		total_h2_from_peer = TotalH2From } = Report,
    io_lib:format(
		"|                  All | ~8B h/s | ~8B h/s | ~7B h/s | ~7B h/s | ~6B | ~6B |\n",
		[
			floor(CurrentH1To), floor(AverageH1To),
			floor(CurrentH1From), floor(AverageH1From),
			TotalH2To, TotalH2From
		]).

format_peer_rows([]) ->
	"";
format_peer_rows([PeerReport | PeerReports]) ->
	format_peer_rows(PeerReports) ++
	[format_peer_row(PeerReport)].

format_peer_row(PeerReport) ->
	#peer_report{
		peer = Peer,
		average_h1_to_peer_hps = AverageH1To,
		current_h1_to_peer_hps = CurrentH1To,
		average_h1_from_peer_hps = AverageH1From,
		current_h1_from_peer_hps = CurrentH1From,
		total_h2_to_peer = TotalH2To,
		total_h2_from_peer = TotalH2From } = PeerReport,
    io_lib:format(
		"| ~20s | ~8B h/s | ~8B h/s | ~7B h/s | ~7B h/s | ~6B | ~6B |\n",
		[
			ar_util:format_peer(Peer),
			floor(CurrentH1To), floor(AverageH1To), 
			floor(CurrentH1From), floor(AverageH1From), 
			TotalH2To, TotalH2From
		]).

format_vdf_speed(undefined) ->
	" undefined";
format_vdf_speed(VDFSpeed) ->
	io_lib:format("~5.2f s", [VDFSpeed]).

%%%===================================================================
%%% Tests
%%%===================================================================

mining_stats_test_() ->
    [
		{timeout, 30, fun test_read_stats/0},
		{timeout, 30, fun test_h1_stats/0},
		{timeout, 30, fun test_h2_stats/0},
		{timeout, 30, fun test_vdf_stats/0},
		{timeout, 30, fun test_data_size_stats/0},
		{timeout, 30, fun test_h1_sent_to_peer_stats/0},
		{timeout, 30, fun test_h1_received_from_peer_stats/0},
		{timeout, 30, fun test_h2_peer_stats/0},
		{timeout, 30, fun test_optimal_stats_poa1_multiple_1/0},
		{timeout, 30, fun test_optimal_stats_poa1_multiple_2/0},
		{timeout, 30, fun test_report_poa1_multiple_1/0},
		ar_test_node:test_with_mocked_functions(
			[
				{ar_difficulty, poa1_diff_multiplier, fun(_) -> 2 end}
			],
			fun test_report_poa1_multiple_2/0
		)
	].

test_read_stats() ->
	test_local_stats(fun chunks_read/2, read).

test_h1_stats() ->
	test_local_stats(fun h1_computed/2, h1).

test_h2_stats() ->
	test_local_stats(fun h2_computed/2, h2).

test_local_stats(Fun, Stat) ->
	ar_mining_stats:pause_performance_reports(120000),
	reset_all_stats(),
	Fun(1, 1),
	TotalStart1 = get_start({partition, 1, Stat, total}),
	CurrentStart1 = get_start({partition, 1, Stat, current}),
	timer:sleep(1000),
	Fun(1, 1),
	Fun(1, 1),
	
	Fun(2, 1),
	TotalStart2 = get_start({partition, 2, Stat, total}),
	CurrentStart2 = get_start({partition, 2, Stat, current}),
	Fun(2, 1),
	
	?assert(TotalStart1 /= TotalStart2),
	?assert(CurrentStart1 /= CurrentStart2),
	?assertEqual(0.0, get_average_count_by_time({partition, 1, Stat, total}, TotalStart1)),
	?assertEqual(0.0, get_average_count_by_time({partition, 1, Stat, current}, CurrentStart1)),
	?assertEqual(0.0, get_average_count_by_time({partition, 2, Stat, total}, TotalStart2)),
	?assertEqual(0.0, get_average_count_by_time({partition, 2, Stat, current}, CurrentStart2)),

	?assertEqual(6.0, get_average_count_by_time({partition, 1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.25, get_average_count_by_time({partition, 1, Stat, current}, CurrentStart1 + 12000)),
	?assertEqual(0.5, get_average_count_by_time({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(8.0, get_average_count_by_time({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_count_by_time({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_count_by_time({partition, 3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(6.0, get_average_samples_by_time({partition, 1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.25, get_average_samples_by_time({partition, 1, Stat, current}, CurrentStart1 + 12000)),
	?assertEqual(0.5, get_average_samples_by_time({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(8.0, get_average_samples_by_time({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(1.0, get_average_by_samples({partition, 1, Stat, total})),
	?assertEqual(1.0, get_average_by_samples({partition, 1, Stat, current})),
	?assertEqual(1.0, get_average_by_samples({partition, 2, Stat, total})),
	?assertEqual(1.0, get_average_by_samples({partition, 2, Stat, current})),
	?assertEqual(0.0, get_average_by_samples({partition, 3, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({partition, 3, Stat, current})),

	Now = CurrentStart2 + 1000,
	reset_count({partition, 1, Stat, current}, Now),
	?assertEqual(Now, get_start({partition, 1, Stat, current})),
	?assertEqual(6.0, get_average_count_by_time({partition, 1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average_count_by_time({partition, 1, Stat, current}, Now + 12000)),
	?assertEqual(0.5, get_average_count_by_time({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(8.0, get_average_count_by_time({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_count_by_time({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_count_by_time({partition, 3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(6.0, get_average_samples_by_time({partition, 1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 1, Stat, current}, Now + 12000)),
	?assertEqual(0.5, get_average_samples_by_time({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(8.0, get_average_samples_by_time({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(1.0, get_average_by_samples({partition, 1, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({partition, 1, Stat, current})),
	?assertEqual(1.0, get_average_by_samples({partition, 2, Stat, total})),
	?assertEqual(1.0, get_average_by_samples({partition, 2, Stat, current})),
	?assertEqual(0.0, get_average_by_samples({partition, 3, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({partition, 3, Stat, current})),

	reset_all_stats(),
	?assertEqual(0.0, get_average_count_by_time({partition, 1, Stat, total}, Now + 500)),
	?assertEqual(0.0, get_average_count_by_time({partition, 1, Stat, current}, Now + 12000)),
	?assertEqual(0.0, get_average_count_by_time({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(0.0, get_average_count_by_time({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_count_by_time({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_count_by_time({partition, 3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(0.0, get_average_samples_by_time({partition, 1, Stat, total}, Now + 500)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 1, Stat, current}, Now + 12000)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_samples_by_time({partition, 3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(0.0, get_average_by_samples({partition, 1, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({partition, 1, Stat, current})),
	?assertEqual(0.0, get_average_by_samples({partition, 2, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({partition, 2, Stat, current})),
	?assertEqual(0.0, get_average_by_samples({partition, 3, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({partition, 3, Stat, current})).

test_vdf_stats() ->
	ar_mining_stats:pause_performance_reports(120000),
	reset_all_stats(),
	ar_mining_stats:vdf_computed(),
	Start = get_start(vdf),
	ar_mining_stats:vdf_computed(),
	ar_mining_stats:vdf_computed(),

	?assertEqual(0.0, get_average_count_by_time(vdf, Start)),
	?assertEqual(6.0, get_average_count_by_time(vdf, Start + 500)),
	?assertEqual(0.0, get_average_samples_by_time(vdf, Start)),
	?assertEqual(6.0, get_average_samples_by_time(vdf, Start + 500)),
	?assertEqual(1.0, get_average_by_samples(vdf)),

	Now = Start + 1000,
	?assertEqual(1.0/3.0, vdf_speed(Now)),
	?assertEqual(Now, get_start(vdf)),
	?assertEqual(undefined, vdf_speed(Now)),
	?assertEqual(0.0, get_average_count_by_time(vdf, Now + 500)),
	?assertEqual(0.0, get_average_samples_by_time(vdf, Now + 500)),
	?assertEqual(0.0, get_average_by_samples(vdf)),
	?assertEqual(undefined, vdf_speed(Now + 500)),

	ar_mining_stats:vdf_computed(),
	Start2 = get_start(vdf),
	?assertEqual(0.5, vdf_speed(Start2 + 500)),

	ar_mining_stats:vdf_computed(),
	reset_all_stats(),
	?assertEqual(undefined, get_start(vdf)),
	?assertEqual(0.0, get_average_count_by_time(vdf, 1000)),
	?assertEqual(0.0, get_average_samples_by_time(vdf, 1000)),
	?assertEqual(0.0, get_average_by_samples(vdf)),
	?assertEqual(undefined, vdf_speed(1000)).

test_data_size_stats() ->
	{ok, Config} = application:get_env(arweave, config),
	try
		application:set_env(
		  	arweave,
			config,
			Config#config{
				mining_addr = <<"MINING">>
			}
		),

		WeaveSize = floor(2 * ar_block:partition_size()),
		ets:insert(node_state, [{weave_size, WeaveSize}]),

		ar_mining_stats:pause_performance_reports(120000),
		do_test_data_size_stats(Config, {spora_2_6, <<"MINING">>}, {spora_2_6, <<"PACKING">>}),
		do_test_data_size_stats(Config, {composite, <<"MINING">>, 1}, {composite, <<"PACKING">>, 1}),
		do_test_data_size_stats(Config, {composite, <<"MINING">>, 2}, {composite, <<"PACKING">>, 2})
	after
		application:set_env(arweave, config, Config)
	end.

do_test_data_size_stats(Config, Mining, Packing) ->
	application:set_env(arweave, config, Config#config{ 
		storage_modules = [
			{floor(0.1 * ar_block:partition_size()), 10, unpacked},
			{floor(0.1 * ar_block:partition_size()), 10, Mining},
			{floor(0.1 * ar_block:partition_size()), 10, Packing},
			{floor(0.3 * ar_block:partition_size()), 4, unpacked},
			{floor(0.3 * ar_block:partition_size()), 4, Mining},
			{floor(0.3 * ar_block:partition_size()), 4, Packing},
			{floor(0.2 * ar_block:partition_size()), 8, unpacked},
			{floor(0.2 * ar_block:partition_size()), 8, Mining},
			{floor(0.2 * ar_block:partition_size()), 8, Packing},
			{ar_block:partition_size(), 2, unpacked},
			{ar_block:partition_size(), 2, Mining},
			{ar_block:partition_size(), 2, Packing}
		]
	}),

	reset_all_stats(),
	?assertEqual(0, get_total_minable_data_size(Mining)),
	?assertEqual(0, get_partition_data_size(1, Mining)),
	?assertEqual(0, get_partition_data_size(2, Mining)),

	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.1 * ar_block:partition_size()), 10, unpacked}),
		unpacked, 1, floor(0.1 * ar_block:partition_size()), 10, 101),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.1 * ar_block:partition_size()), 10, Mining}),
		Mining, 1, floor(0.1 * ar_block:partition_size()), 10, 102),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.1 * ar_block:partition_size()), 10, Packing}),
		Packing, 1, floor(0.1 * ar_block:partition_size()), 10, 103),

	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.3 * ar_block:partition_size()), 4, unpacked}),
		unpacked, 1, floor(0.3 * ar_block:partition_size()), 4, 111),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.3 * ar_block:partition_size()), 4, Mining}),
		Mining, 1, floor(0.3 * ar_block:partition_size()), 4, 112),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.3 * ar_block:partition_size()), 4, Packing}),
		Packing, 1, floor(0.3 * ar_block:partition_size()), 4, 113),

	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({ar_block:partition_size(), 2, unpacked}),
		unpacked, 2, ar_block:partition_size(), 2, 201),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({ar_block:partition_size(), 2, Mining}),
		Mining, 2, ar_block:partition_size(), 2, 202),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({ar_block:partition_size(), 2, Packing}),
		Packing, 2, ar_block:partition_size(), 2, 203),

	?assertEqual(214, get_partition_data_size(1, Mining)),
	?assertEqual(202, get_partition_data_size(2, Mining)),
	?assertEqual(214, get_total_minable_data_size(Mining)),

	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.2 * ar_block:partition_size()), 8, unpacked}),
		unpacked, 1, floor(0.2 * ar_block:partition_size()), 8, 121),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.2 * ar_block:partition_size()), 8, Mining}),
		Mining, 1, floor(0.2 * ar_block:partition_size()), 8, 122),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({floor(0.2 * ar_block:partition_size()), 8, Packing}),
		Packing, 1, floor(0.2 * ar_block:partition_size()), 8, 123),

	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({ar_block:partition_size(), 2, unpacked}),
		unpacked, 2, ar_block:partition_size(), 2, 51),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({ar_block:partition_size(), 2, Mining}),
		Mining, 2, ar_block:partition_size(), 2, 52),
	ar_mining_stats:set_storage_module_data_size(
		ar_storage_module:id({ar_block:partition_size(), 2, Packing}),
		Packing, 2, ar_block:partition_size(), 2, 53),
	
	?assertEqual(336, get_partition_data_size(1, Mining)),
	?assertEqual(52, get_partition_data_size(2, Mining)),
	?assertEqual(336, get_total_minable_data_size(Mining)),

	reset_all_stats(),
	?assertEqual(0, get_total_minable_data_size(Mining)),
	?assertEqual(0, get_partition_data_size(1, Mining)),
	?assertEqual(0, get_partition_data_size(2, Mining)).

test_h1_sent_to_peer_stats() ->
	test_peer_stats(fun h1_sent_to_peer/2, h1_to_peer).

test_h1_received_from_peer_stats() ->
	test_peer_stats(fun h1_received_from_peer/2, h1_from_peer).

test_peer_stats(Fun, Stat) ->
	ar_mining_stats:pause_performance_reports(120000),
	reset_all_stats(),

	Peer1 = ar_test_node:peer_ip(peer1),
	Peer2 = ar_test_node:peer_ip(peer2),
	Peer3 = ar_test_node:peer_ip(peer3),

	Fun(Peer1, 10),
	TotalStart1 = get_start({peer, Peer1, Stat, total}),
	CurrentStart1 = get_start({peer, Peer1, Stat, current}),
	timer:sleep(1000),
	Fun(Peer1, 5),
	Fun(Peer1, 15),
	
	Fun(Peer2, 1),
	TotalStart2 = get_start({peer, Peer2, Stat, total}),
	CurrentStart2 = get_start({peer, Peer2, Stat, current}),
	Fun(Peer2, 19),
	
	?assert(TotalStart1 /= TotalStart2),
	?assert(CurrentStart1 /= CurrentStart2),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer1, Stat, total}, TotalStart1)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer1, Stat, current}, CurrentStart1)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer2, Stat, total}, TotalStart2)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer2, Stat, current}, CurrentStart2)),
	?assertEqual(60.0, get_average_count_by_time({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(2.5, get_average_count_by_time({peer, Peer1, Stat, current}, CurrentStart1 + 12000)),
	?assertEqual(5.0, get_average_count_by_time({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(80.0, get_average_count_by_time({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(0.0, get_average_samples_by_time({peer, Peer1, Stat, total}, TotalStart1)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer1, Stat, current}, CurrentStart1)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer2, Stat, total}, TotalStart2)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer2, Stat, current}, CurrentStart2)),
	?assertEqual(6.0, get_average_samples_by_time({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.25, get_average_samples_by_time({peer, Peer1, Stat, current}, CurrentStart1 + 12000)),
	?assertEqual(0.5, get_average_samples_by_time({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(8.0, get_average_samples_by_time({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(10.0, get_average_by_samples({peer, Peer1, Stat, total})),
	?assertEqual(10.0, get_average_by_samples({peer, Peer1, Stat, current})),
	?assertEqual(10.0, get_average_by_samples({peer, Peer2, Stat, total})),
	?assertEqual(10.0, get_average_by_samples({peer, Peer2, Stat, current})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer3, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer3, Stat, current})),

	Now = CurrentStart2 + 1000,
	reset_count({peer, Peer1, Stat, current}, Now),
	?assertEqual(Now, get_start({peer, Peer1, Stat, current})),
	?assertEqual(60.0, get_average_count_by_time({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer1, Stat, current}, Now + 12000)),
	?assertEqual(5.0, get_average_count_by_time({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(80.0, get_average_count_by_time({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(6.0, get_average_samples_by_time({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer1, Stat, current}, Now + 12000)),
	?assertEqual(0.5, get_average_samples_by_time({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(8.0, get_average_samples_by_time({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(10.0, get_average_by_samples({peer, Peer1, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer1, Stat, current})),
	?assertEqual(10.0, get_average_by_samples({peer, Peer2, Stat, total})),
	?assertEqual(10.0, get_average_by_samples({peer, Peer2, Stat, current})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer3, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer3, Stat, current})),

	reset_all_stats(),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer1, Stat, current}, Now + 12000)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_count_by_time({peer, Peer3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(0.0, get_average_samples_by_time({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer1, Stat, current}, Now + 12000)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average_samples_by_time({peer, Peer3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(0.0, get_average_by_samples({peer, Peer1, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer1, Stat, current})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer2, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer2, Stat, current})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer3, Stat, total})),
	?assertEqual(0.0, get_average_by_samples({peer, Peer3, Stat, current})).

test_h2_peer_stats() ->
	ar_mining_stats:pause_performance_reports(120000),
	reset_all_stats(),

	Peer1 = ar_test_node:peer_ip(peer1),
	Peer2 = ar_test_node:peer_ip(peer2),
	Peer3 = ar_test_node:peer_ip(peer3),

	ar_mining_stats:h2_sent_to_peer(Peer1),
	ar_mining_stats:h2_sent_to_peer(Peer1),
	ar_mining_stats:h2_sent_to_peer(Peer1),
	ar_mining_stats:h2_sent_to_peer(Peer2),
	ar_mining_stats:h2_sent_to_peer(Peer2),

	?assertEqual(3, get_count({peer, Peer1, h2_to_peer, total})),
	?assertEqual(2, get_count({peer, Peer2, h2_to_peer, total})),
	?assertEqual(0, get_count({peer, Peer3, h2_to_peer, total})),

	?assertEqual(5, get_overall_total(peer, h2_to_peer, total)),

	ar_mining_stats:h2_received_from_peer(Peer1),
	ar_mining_stats:h2_received_from_peer(Peer1),
	ar_mining_stats:h2_received_from_peer(Peer1),
	ar_mining_stats:h2_received_from_peer(Peer2),
	ar_mining_stats:h2_received_from_peer(Peer2),

	?assertEqual(3, get_count({peer, Peer1, h2_from_peer, total})),
	?assertEqual(2, get_count({peer, Peer2, h2_from_peer, total})),
	?assertEqual(0, get_count({peer, Peer3, h2_from_peer, total})),

	?assertEqual(5, get_overall_total(peer, h2_from_peer, total)),

	reset_count({peer, Peer1, h2_to_peer, total}, 1000),
	reset_count({peer, Peer2, h2_from_peer, total}, 1000),

	?assertEqual(0, get_count({peer, Peer1, h2_to_peer, total})),
	?assertEqual(2, get_count({peer, Peer2, h2_to_peer, total})),
	?assertEqual(0, get_count({peer, Peer3, h2_to_peer, total})),
	?assertEqual(3, get_count({peer, Peer1, h2_from_peer, total})),
	?assertEqual(0, get_count({peer, Peer2, h2_from_peer, total})),
	?assertEqual(0, get_count({peer, Peer3, h2_from_peer, total})),

	?assertEqual(2, get_overall_total(peer, h2_to_peer, total)),
	?assertEqual(3, get_overall_total(peer, h2_from_peer, total)),

	reset_all_stats(),

	?assertEqual(0, get_count({peer, Peer1, h2_to_peer, total})),
	?assertEqual(0, get_count({peer, Peer2, h2_to_peer, total})),
	?assertEqual(0, get_count({peer, Peer3, h2_to_peer, total})),
	?assertEqual(0, get_count({peer, Peer1, h2_from_peer, total})),
	?assertEqual(0, get_count({peer, Peer2, h2_from_peer, total})),
	?assertEqual(0, get_count({peer, Peer3, h2_from_peer, total})),

	?assertEqual(0, get_overall_total(peer, h2_to_peer, total)),
	?assertEqual(0, get_overall_total(peer, h2_from_peer, total)).

test_optimal_stats_poa1_multiple_1() ->
	test_optimal_stats({spora_2_6, <<"MINING">>}, 1),
	test_optimal_stats({composite, <<"MINING">>, 1}, 1),
	test_optimal_stats({composite, <<"MINING">>, 2}, 1).

test_optimal_stats_poa1_multiple_2() ->
	test_optimal_stats({spora_2_6, <<"MINING">>}, 2),
	test_optimal_stats({composite, <<"MINING">>, 1}, 2),
	test_optimal_stats({composite, <<"MINING">>, 2}, 2).

test_optimal_stats(Packing, PoA1Multiplier) ->
	PackingDifficulty = ar_mining_server:get_packing_difficulty(Packing),
	RecallRangeSize = case PackingDifficulty of
		0 ->
			0.5;
		1 ->
			0.125;
		2 ->
			0.0625
	end,
	?assertEqual(0.0, 
		optimal_partition_read_mibps(
			Packing, undefined, ar_block:partition_size(),
			floor(10 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),
	?assertEqual(RecallRangeSize * 2, 
		optimal_partition_read_mibps(
			Packing, 1.0, ar_block:partition_size(),
			floor(10 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),
	?assertEqual(RecallRangeSize, 
		optimal_partition_read_mibps(
			Packing, 2.0, ar_block:partition_size(),
			floor(10 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),
	?assertEqual(RecallRangeSize / 2, 
		optimal_partition_read_mibps(
			Packing, 1.0, floor(0.25 * ar_block:partition_size()),
			floor(10 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),
	?assertEqual(RecallRangeSize * 1.6, 
		optimal_partition_read_mibps(
			Packing, 1.0, ar_block:partition_size(),
			floor(6 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),

	{FullWeave, SlowVDF, SmallPartition, SmallWeave} = case PoA1Multiplier of
		1 -> {800.0, 400.0, 200.0, 640.0};
		2 -> {600.0, 300.0, 150.0, 440.0}
	end,

	?assertEqual(0.0, 
		optimal_partition_hash_hps(
			PoA1Multiplier, undefined, ar_block:partition_size(),
			floor(10 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),
	?assertEqual(FullWeave, 
		optimal_partition_hash_hps(
			PoA1Multiplier, 1.0, ar_block:partition_size(),
			floor(10 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),
	?assertEqual(SlowVDF, 
		optimal_partition_hash_hps(
			PoA1Multiplier, 2.0, ar_block:partition_size(),
			floor(10 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),
	?assertEqual(SmallPartition, 
		optimal_partition_hash_hps(
			PoA1Multiplier, 1.0, floor(0.25 * ar_block:partition_size()),
			floor(10 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))),
	?assertEqual(SmallWeave, 
		optimal_partition_hash_hps(
			PoA1Multiplier, 1.0, ar_block:partition_size(),
			floor(6 * ar_block:partition_size()), floor(10 * ar_block:partition_size()))).

test_report_poa1_multiple_1() ->
	test_report({spora_2_6, <<"MINING">>}, {spora_2_6, <<"PACKING">>}, 1),
	test_report({composite, <<"MINING">>, 1}, {composite, <<"PACKING">>, 1}, 1),
	test_report({composite, <<"MINING">>, 2}, {composite, <<"PACKING">>, 2}, 1).

test_report_poa1_multiple_2() ->
	test_report({spora_2_6, <<"MINING">>}, {spora_2_6, <<"PACKING">>}, 2),
	test_report({composite, <<"MINING">>, 1}, {composite, <<"PACKING">>, 1}, 2),
	test_report({composite, <<"MINING">>, 2}, {composite, <<"PACKING">>, 2}, 2).

test_report(Mining, Packing, PoA1Multiplier) ->
	{ok, Config} = application:get_env(arweave, config),
	MiningAddress = case Mining of
		{spora_2_6, Addr} ->
			Addr;
		{composite, Addr, _} ->
			Addr
	end,
	PackingDifficulty = ar_mining_server:get_packing_difficulty(Mining),
	DifficultyDivisor = case PackingDifficulty of
		0 ->
			1.0;
		1 ->
			8.0;
		2 ->
			4.0
	end,
	RecallRangeSize = case PackingDifficulty of
		0 ->
			0.5;
		1 ->
			0.125;
		2 ->
			0.0625
	end,
	StorageModules = [
		%% partition 1
		{floor(0.1 * ar_block:partition_size()), 10, unpacked},
		{floor(0.1 * ar_block:partition_size()), 10, Mining},
		{floor(0.1 * ar_block:partition_size()), 10, Packing},
		{floor(0.3 * ar_block:partition_size()), 4, unpacked},
		{floor(0.3 * ar_block:partition_size()), 4, Mining},
		{floor(0.3 * ar_block:partition_size()), 4, Packing},
		{floor(0.2 * ar_block:partition_size()), 8, unpacked},
		{floor(0.2 * ar_block:partition_size()), 8, Mining},
		{floor(0.2 * ar_block:partition_size()), 8, Packing},
		%% partition 2
		{ar_block:partition_size(), 2, unpacked},
		{ar_block:partition_size(), 2, Mining},
		{ar_block:partition_size(), 2, Packing}
	],
	
	try	
		application:set_env(arweave, config,
			Config#config{
				storage_modules = StorageModules,
				mining_addr = MiningAddress
			}),
		ar_mining_stats:pause_performance_reports(120000),
		reset_all_stats(),
		Partitions = [
			{1, MiningAddress, 0},
			{2, MiningAddress, 0},
			{3, MiningAddress, 0}
		],
		Peer1 = ar_test_node:peer_ip(peer1),
		Peer2 = ar_test_node:peer_ip(peer2),
		Peer3 = ar_test_node:peer_ip(peer3),
		Peers = [Peer1, Peer2, Peer3],

		Now = erlang:monotonic_time(millisecond),
		WeaveSize = floor(10 * ar_block:partition_size()),
		ets:insert(node_state, [{weave_size, WeaveSize}]),
		ar_mining_stats:set_total_data_size(floor(0.6 * ar_block:partition_size())),
		ar_mining_stats:set_storage_module_data_size(
			ar_storage_module:id({floor(0.1 * ar_block:partition_size()), 10, Mining}),
			Mining, 1, floor(0.1 * ar_block:partition_size()), 10,
			floor(0.1 * ar_block:partition_size())),
		ar_mining_stats:set_storage_module_data_size(
			ar_storage_module:id({floor(0.3 * ar_block:partition_size()), 4, Mining}),
			Mining, 1, floor(0.3 * ar_block:partition_size()), 4,
			floor(0.2 * ar_block:partition_size())),
		ar_mining_stats:set_storage_module_data_size(
			ar_storage_module:id({floor(0.2 * ar_block:partition_size()), 8, Mining}),
			Mining, 1, floor(0.2 * ar_block:partition_size()), 8,
			floor(0.05 * ar_block:partition_size())),	
		ar_mining_stats:set_storage_module_data_size(
			ar_storage_module:id({ar_block:partition_size(), 2, Mining}),
			Mining, 2, ar_block:partition_size(), 2, floor(0.25 * ar_block:partition_size())),
		vdf_computed(Now),
		vdf_computed(Now),
		vdf_computed(Now),
		h1_solution(Now),
		h2_solution(Now),
		h2_solution(Now),
		block_found(Now),
		chunks_read(1, 1, Now),
		chunks_read(1, 2, Now),
		chunks_read(2, 2, Now),
		h1_computed(1, 1, Now),
		h1_computed(1, 2, Now),
		h2_computed(1, 2, Now),
		h1_computed(2, 4, Now),
		h1_sent_to_peer(Peer1, 10, Now),
		h1_sent_to_peer(Peer1, 5, Now),
		h1_sent_to_peer(Peer1, 15, Now),
		h1_sent_to_peer(Peer2, 1, Now),
		h1_sent_to_peer(Peer2, 19, Now),
		h1_received_from_peer(Peer2, 10, Now),
		h1_received_from_peer(Peer2, 5, Now),
		h1_received_from_peer(Peer2, 15, Now),
		h1_received_from_peer(Peer1, 1, Now),
		h1_received_from_peer(Peer1, 19, Now),
		h2_sent_to_peer(Peer1, Now),
		h2_sent_to_peer(Peer1, Now),
		h2_sent_to_peer(Peer1, Now),
		h2_sent_to_peer(Peer2, Now),
		h2_sent_to_peer(Peer2, Now),
		h2_received_from_peer(Peer1, Now),
		h2_received_from_peer(Peer1, Now),
		h2_received_from_peer(Peer2, Now),
		h2_received_from_peer(Peer2, Now),
		h2_received_from_peer(Peer2, Now),
		
		Report1 = generate_report(0, Mining, [], [], WeaveSize, Now+1000),
		?assertEqual(#report{ now = Now+1000 }, Report1),
		log_report(format_report(Report1, WeaveSize)),

		Report2 = generate_report(0, Mining, Partitions, Peers, WeaveSize, Now+1000),
		ReportString = format_report(Report2, WeaveSize),
		log_report(ReportString),

		{
			TotalHash, Partition1Hash, Partition2Hash,
			TotalOptimal, Partition1Optimal, Partition2Optimal
		} = case PoA1Multiplier of
			1 -> {9.0, 5.0, 4.0, 763.1992309570705, 445.19924812320824, 317.9999828338623};
			2 -> {5.5, 3.5, 2.0, 403.19957427982445, 235.19959144596214, 167.9999828338623}
		end,

		?assertEqual(#report{ 
			now = Now+1000,
			vdf_speed = 1.0 / 3.0,
			h1_solution = 1,
			h2_solution = 2,
			confirmed_block = 1,
			total_data_size = 
				floor(0.1 * ar_block:partition_size()) + floor(0.2 * ar_block:partition_size()) +
				floor(0.05 * ar_block:partition_size()) + floor(0.25 * ar_block:partition_size()),
			optimal_overall_read_mibps = 0.9539990386963382 * 2 * RecallRangeSize,
			optimal_overall_hash_hps = TotalOptimal,
			average_read_mibps = 1.25,
			current_read_mibps = 1.25,
			average_hash_hps = TotalHash / DifficultyDivisor,
			current_hash_hps = TotalHash / DifficultyDivisor,
			average_h1_to_peer_hps = 50.0,
			current_h1_to_peer_hps = 50.0,
			average_h1_from_peer_hps = 50.0,
			current_h1_from_peer_hps = 50.0,
			total_h2_to_peer = 5,
			total_h2_from_peer = 5,
			partitions = [
				#partition_report{
					partition_number = 3,
					data_size = 0,
					optimal_read_mibps = 0.0,
					average_read_mibps = 0.0,
					current_read_mibps = 0.0,
					optimal_hash_hps = 0.0,
					average_hash_hps = 0.0,
					current_hash_hps = 0.0
				},
				#partition_report{
					partition_number = 2,
					data_size = floor(0.25 * ar_block:partition_size()),
					optimal_read_mibps = 0.3974999785423279 * 2 * RecallRangeSize,
					average_read_mibps = 0.5,
					current_read_mibps = 0.5,
					optimal_hash_hps = Partition2Optimal,
					average_hash_hps = Partition2Hash / DifficultyDivisor,
					current_hash_hps = Partition2Hash / DifficultyDivisor
				},
				#partition_report{
					partition_number = 1,
					data_size = 734002,
					optimal_read_mibps = 0.5564990601540103 * 2 * RecallRangeSize,
					average_read_mibps = 0.75,
					current_read_mibps = 0.75,
					optimal_hash_hps = Partition1Optimal,
					average_hash_hps = Partition1Hash / DifficultyDivisor,
					current_hash_hps = Partition1Hash / DifficultyDivisor
				}
			],
			peers = [
				#peer_report{
					peer = Peer3,
					average_h1_to_peer_hps = 0.0,
					current_h1_to_peer_hps = 0.0,
					average_h1_from_peer_hps = 0.0,
					current_h1_from_peer_hps = 0.0,
					total_h2_to_peer = 0,
					total_h2_from_peer = 0
				},
				#peer_report{
					peer = Peer2,
					average_h1_to_peer_hps = 20.0,
					current_h1_to_peer_hps = 20.0,
					average_h1_from_peer_hps = 30.0,
					current_h1_from_peer_hps = 30.0,
					total_h2_to_peer = 2,
					total_h2_from_peer = 3
				},
				#peer_report{
					peer = Peer1,
					average_h1_to_peer_hps = 30.0,
					current_h1_to_peer_hps = 30.0,
					average_h1_from_peer_hps = 20.0,
					current_h1_from_peer_hps = 20.0,
					total_h2_to_peer = 3,
					total_h2_from_peer = 2
				}
			]
		},
		Report2)
	after
		application:set_env(arweave, config, Config)
	end.
