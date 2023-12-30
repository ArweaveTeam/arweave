-module(ar_mining_stats).
-behaviour(gen_server).

-export([start_link/0, start_performance_reports/0, pause_performance_reports/1, mining_paused/0,
		set_total_data_size/1, set_storage_module_data_size/6,
		vdf_computed/0, chunk_read/1, hash_computed/1,
		h1_sent_to_peer/2, h1_received_from_peer/2, h2_sent_to_peer/1, h2_received_from_peer/1]).

-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	pause_performance_reports = false,
	pause_performance_reports_timeout
}).

-record(report, {
	now,
	vdf_speed = undefined,
	total_data_size = 0,
	optimal_overall_read_mibps = 0.0,
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
%% {vdf, 													StartTime, VDFStepCount}
%% {{partition, PartitionNumber, read, total}, 				StartTime, TotalChunksRead}
%% {{partition, PartitionNumber, read, current}, 			StartTime, CurrentChunksRead}
%% {{partition, PartitionNumber, hash, total}, 				StartTime, TotalHashes}
%% {{partition, PartitionNumber, hash, current}, 			StartTime, CurrentHashes}
%% {total_data_size, 										TotalBytesPacked}
%% {{partition, PartitionNumber, storage_module, StoreID}, 	BytesPacked}
%% {{peer, Peer, h1_to_peer, total}, 						StartTime, TotalH1sSentToPeer}
%% {{peer, Peer, h1_to_peer, current}, 						StartTime, CurrentH1sSentToPeer}
%% {{peer, Peer, h1_from_peer, total}, 						StartTime, TotalH1sReceivedFromPeer}
%% {{peer, Peer, h1_from_peer, current}, 					StartTime, CurrentH1sReceivedFromPeer}
%% {{peer, Peer, h2_to_peer, total}, 						StartTime, TotalH2sSentToPeer}
%% {{peer, Peer, h2_from_peer, total}, 						StartTime, TotalH2sReceivedFromPeer}

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
	gen_server:cast(?MODULE, {pause_performance_reports, Time}).

vdf_computed() ->
	increment_count(vdf).

chunk_read(PartitionNumber) ->
	increment_count({partition, PartitionNumber, read, total}),
	increment_count({partition, PartitionNumber, read, current}).

hash_computed(PartitionNumber) ->
	increment_count({partition, PartitionNumber, hash, total}),
	increment_count({partition, PartitionNumber, hash, current}).

h1_sent_to_peer(Peer, H1Count) ->
	increment_count({peer, Peer, h1_to_peer, total}, H1Count),
	increment_count({peer, Peer, h1_to_peer, current}, H1Count).

h1_received_from_peer(Peer, H1Count) ->
	increment_count({peer, Peer, h1_from_peer, total}, H1Count),
	increment_count({peer, Peer, h1_from_peer, current}, H1Count).

h2_sent_to_peer(Peer) ->
	increment_count({peer, Peer, h2_to_peer, total}).

h2_received_from_peer(Peer) ->
	increment_count({peer, Peer, h2_from_peer, total}).

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
	try
		prometheus_gauge:set(v2_index_data_size_by_packing,
			[StoreID, Packing, PartitionNumber, StorageModuleSize, StorageModuleIndex],
			DataSize),
		ets:insert(?MODULE, {{partition, PartitionNumber, storage_module, StoreID}, DataSize})
	catch
		error:badarg ->
			?LOG_WARNING([{event, set_storage_module_data_size_failed},
				{reason, prometheus_not_started},
				{store_id, StoreID}, {packing, Packing}, {partition_number, PartitionNumber},
				{storage_module_size, StorageModuleSize}, {storage_module_index, StorageModuleIndex},
				{data_size, DataSize}]);
		Type:Reason ->
			?LOG_ERROR([{event, set_storage_module_data_size_failed},
				{type, Type}, {reason, Reason},
				{store_id, StoreID}, {packing, Packing}, {partition_number, PartitionNumber},
				{storage_module_size, StorageModuleSize}, {storage_module_index, StorageModuleIndex},
				{data_size, DataSize} ])
	end.

mining_paused() ->
	clear_metrics().

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	{ok, #state{}}.

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

handle_cast({pause_performance_reports, Time}, State) ->
	Now = os:system_time(millisecond),
	Timeout = Now + Time,
	{noreply, State#state{ pause_performance_reports = true,
			pause_performance_reports_timeout = Timeout }};

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

reset_all_stats() ->
	ets:delete_all_objects(?MODULE).

%% @doc Atomically increments the count for ETS records stored in the format:
%% {Key, StartTimestamp, Count}
%% If the Key doesn't exist, it is initialized with the current timestamp and a count of Amount
increment_count(Key) ->
	increment_count(Key, 1).
increment_count(Key, Amount) ->
	ets:update_counter(?MODULE, Key,
		[{3, Amount}], 									%% increment count by Amount
		{Key, erlang:monotonic_time(millisecond), 0} 	%% initialize timestamp and count
	).

reset_count(Key, Now) ->
	ets:insert(?MODULE, [{Key, Now, 0}]).

get_average(Key, Now) ->
	case ets:lookup(?MODULE, Key) of 
		[] ->
			0.0;
		[{_, Start, _Count}] when Now - Start =:= 0 ->
			0.0;
		[{_, Start, Count}] ->
			Elapsed = (Now - Start) / 1000,
			Count / Elapsed
	end.

get_count(Key) ->
	case ets:lookup(?MODULE, Key) of 
		[] ->
			0;
		[{_, _Start, Count}] ->
			Count
	end.

get_start(Key) ->
	case ets:lookup(?MODULE, Key) of 
		[] ->
			undefined;
		[{_, Start, _Count}] ->
			Start
	end.

get_total_data_size() ->
	case ets:lookup(?MODULE, total_data_size) of 
		[] ->
			0;
		[{_, TotalDataSize}] ->
			TotalDataSize
	end.

get_overall_average(PartitionPeer, Stat, TotalCurrent, Now) ->
	Pattern = {{PartitionPeer, '_', Stat, TotalCurrent}, '$1', '$2'},
    Matches = ets:match(?MODULE, Pattern),
    Starts = [Start || [Start, _] <- Matches],
	Counts = [Count || [_, Count] <- Matches],

	case Starts of
		[] ->
			0.0;
		_ ->
			TotalCount = lists:sum(Counts),
			MinStart = lists:min(Starts),

			case Now > MinStart of
				true ->
					Elapsed = (Now - MinStart) / 1000,
					TotalCount / Elapsed;
				false ->
					0.0
			end
	end.

get_overall_total(PartitionPeer, Stat, TotalCurrent) ->
	Pattern = {{PartitionPeer, '_', Stat, TotalCurrent}, '$1', '$2'},
    Matches = ets:match(?MODULE, Pattern),
	Counts = [Count || [_, Count] <- Matches],
	lists:sum(Counts).

get_partition_data_size(PartitionNumber) ->
    Pattern = {{partition, PartitionNumber, storage_module, '_'}, '$1'},
	Sizes = [Size || [Size] <- ets:match(?MODULE, Pattern)],
    lists:sum(Sizes).

vdf_speed(Now) ->
	case get_average(vdf, Now) of
		0.0 ->
			undefined;
		StepsPerSecond ->
			reset_count(vdf, Now),
			1.0 / StepsPerSecond
	end.

%% @doc calculate the maximum hash rate (in MiB per second read from disk) for the given VDF speed
%% at the current weave size.
optimal_partition_read_mibps(undefined, _PartitionDataSize, _TotalDataSize, _WeaveSize) ->
	0.0;	
optimal_partition_read_mibps(VDFSpeed, PartitionDataSize, TotalDataSize, WeaveSize) ->
	(100.0 / VDFSpeed) * (PartitionDataSize / ?PARTITION_SIZE) * (1 + (TotalDataSize / WeaveSize)).

generate_report() ->
	{ok, Config} = application:get_env(arweave, config),
	generate_report(
		ar_mining_io:get_partitions(),
		Config#config.cm_peers,
		ar_node:get_weave_size(),
		erlang:monotonic_time(millisecond)
	).

generate_report([], _Peers, _WeaveSize, Now) ->
	#report{
		now = Now
	};
generate_report(Partitions, Peers, WeaveSize, Now) ->
	VDFSpeed = vdf_speed(Now),
	TotalDataSize = get_total_data_size(),
	Report = #report{
		now = Now,
		vdf_speed = VDFSpeed,
		total_data_size = TotalDataSize,
		average_h1_to_peer_hps = get_overall_average(peer, h1_to_peer, total, Now),
		current_h1_to_peer_hps = get_overall_average(peer, h1_to_peer, current, Now),
		average_h1_from_peer_hps = get_overall_average(peer, h1_from_peer, total, Now),
		current_h1_from_peer_hps = get_overall_average(peer, h1_from_peer, current, Now),
		total_h2_to_peer = get_overall_total(peer, h2_to_peer, total),
		total_h2_from_peer = get_overall_total(peer, h2_from_peer, total)
	},

	Report2 = generate_partition_reports(Partitions, Report, WeaveSize),
	Report3 = generate_peer_reports(Peers, Report2),
	Report3.

generate_partition_reports(Partitions, Report, WeaveSize) ->
	lists:foldr(
		fun({PartitionNumber, _ReplicaID}, Acc) ->
			generate_partition_report(PartitionNumber, Acc, WeaveSize)
		end,
		Report,
		Partitions
	).

generate_partition_report(PartitionNumber, Report, WeaveSize) ->
	#report{
		now = Now,
		vdf_speed = VDFSpeed,
		total_data_size = TotalDataSize,
		partitions = Partitions,
		optimal_overall_read_mibps = OptimalOverallRead,
		average_read_mibps = AverageRead,
		current_read_mibps = CurrentRead,
		average_hash_hps = AverageHash,
		current_hash_hps = CurrentHash } = Report,
	DataSize = get_partition_data_size(PartitionNumber),
	PartitionReport = #partition_report{
		partition_number = PartitionNumber,
		data_size = DataSize,
		average_read_mibps = get_average({partition, PartitionNumber, read, total}, Now) / 4,
		current_read_mibps = get_average({partition, PartitionNumber, read, current}, Now) / 4,
		average_hash_hps = get_average({partition, PartitionNumber, hash, total}, Now),
		current_hash_hps = get_average({partition, PartitionNumber, hash, current}, Now),
		optimal_read_mibps = optimal_partition_read_mibps(
			VDFSpeed, DataSize, TotalDataSize, WeaveSize)
	},

	reset_count({partition, PartitionNumber, read, current}, Now),
	reset_count({partition, PartitionNumber, hash, current}, Now),

	Report#report{ 
		optimal_overall_read_mibps = 
			OptimalOverallRead + PartitionReport#partition_report.optimal_read_mibps,
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
		peers = Peers } = Report,
	PeerReport = #peer_report{
		peer = Peer,
		average_h1_to_peer_hps = get_average({peer, Peer, h1_to_peer, total}, Now),
		current_h1_to_peer_hps = get_average({peer, Peer, h1_to_peer, current}, Now),
		average_h1_from_peer_hps = get_average({peer, Peer, h1_from_peer, total}, Now),
		current_h1_from_peer_hps = get_average({peer, Peer, h1_from_peer, current}, Now),
		total_h2_to_peer = get_count({peer, Peer, h2_to_peer, total}),
		total_h2_from_peer = get_count({peer, Peer, h2_from_peer, total})
	},

	reset_count({peer, Peer, h1_to_peer, current}, Now),
	reset_count({peer, Peer, h1_from_peer, current}, Now),

	Report#report{ peers = Peers ++ [PeerReport] }.

report_performance() ->
	Report = generate_report(),
	set_metrics(Report),
	ReportString = format_report(Report),
	ar:console("~s", [ReportString]),
	log_report(ReportString).

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
	prometheus_gauge:set(mining_rate, [ideal, total],  Report#report.optimal_overall_read_mibps),
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
	prometheus_gauge:set(mining_rate, [ideal, PartitionNumber],
		PartitionReport#partition_report.optimal_read_mibps),
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
		"VDF Speed: ~s\n"
		"\n",
		[format_vdf_speed(Report#report.vdf_speed)]
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
			floor(CurrentHash), floor(AverageHash), floor(OptimalOverallRead * 4)]).

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
		average_hash_hps = AverageHash,
		current_hash_hps = CurrentHash } = PartitionReport,
	TiB = DataSize / ?TiB,
	PctOfPartition = floor((DataSize / ?PARTITION_SIZE) * 100),
    io_lib:format(
		"| ~9.B | ~5.1f TiB | ~6.B % "
		"| ~7.1f MiB/s | ~7.1f MiB/s | ~7.1f MiB/s "
		"| ~6B h/s | ~6B h/s | ~8B h/s |\n",
		[
			PartitionNumber, TiB, PctOfPartition,
			CurrentRead, AverageRead, OptimalRead,
			floor(CurrentHash), floor(AverageHash), floor(OptimalRead * 4)]).

format_peer_report(#report{ peers = [] }) ->
	"";
format_peer_report(Report) ->
	Header = 
		"\n"
		"Coordinated mining cluster stats:\n"
		"+----------------------+-------------+-------------+---------------+---------------+---------+---------+\n"
        "|                 Peer | H1 To (Cur) | H1 To (Avg) | H1 From (Cur) | H1 From (Avg) |   H2 To | H2 From |\n"
		"+----------------------+-------------+-------------+---------------+---------------+---------+---------+\n",
	TotalRow = format_peer_total_row(Report),
	PartitionRows = format_peer_rows(Report#report.peers),
    Footer =
		"+----------------------+-------------+-------------+---------------+---------------+---------+---------+\n",
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
		"|                  All | ~7B h/s | ~7B h/s | ~9B h/s | ~9B h/s | ~7B | ~7B |\n",
		[
			floor(AverageH1To), floor(CurrentH1To),
			floor(AverageH1From), floor(CurrentH1From),
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
		"| ~20s | ~7B h/s | ~7B h/s | ~9B h/s | ~9B h/s | ~7B | ~7B |\n",
		[
			ar_util:format_peer(Peer),
			floor(AverageH1To), floor(CurrentH1To),
			floor(AverageH1From), floor(CurrentH1From),
			TotalH2To, TotalH2From
		]).

format_vdf_speed(undefined) ->
	"undefined";
format_vdf_speed(VDFSpeed) ->
	io_lib:format("~5.2f s", [VDFSpeed]).

%%%===================================================================
%%% Tests
%%%===================================================================

mining_stats_test_() ->
	[
		{timeout, 30, fun test_read_stats/0},
		{timeout, 30, fun test_hash_stats/0},
		{timeout, 30, fun test_vdf_stats/0},
		{timeout, 30, fun test_data_size_stats/0},
		{timeout, 30, fun test_h1_sent_to_peer_stats/0},
		{timeout, 30, fun test_h1_received_from_peer_stats/0},
		{timeout, 30, fun test_h2_peer_stats/0},
		{timeout, 30, fun test_optimal_stats/0},
		{timeout, 30, fun test_report/0}
	].

test_read_stats() ->
	test_local_stats(fun chunk_read/1, read).

test_hash_stats() ->
	test_local_stats(fun hash_computed/1, hash).

test_local_stats(Fun, Stat) ->
	ar_mining_stats:pause_performance_reports(120000),
	reset_all_stats(),
	Fun(1),
	TotalStart1 = get_start({partition, 1, Stat, total}),
	CurrentStart1 = get_start({partition, 1, Stat, current}),
	timer:sleep(1000),
	Fun(1),
	Fun(1),
	
	Fun(2),
	TotalStart2 = get_start({partition, 2, Stat, total}),
	CurrentStart2 = get_start({partition, 2, Stat, current}),
	Fun(2),
	
	?assert(TotalStart1 /= TotalStart2),
	?assert(CurrentStart1 /= CurrentStart2),
	?assertEqual(0.0, get_average({partition, 1, Stat, total}, TotalStart1)),
	?assertEqual(0.0, get_average({partition, 1, Stat, current}, CurrentStart1)),
	?assertEqual(0.0, get_average({partition, 2, Stat, total}, TotalStart2)),
	?assertEqual(0.0, get_average({partition, 2, Stat, current}, CurrentStart2)),

	?assertEqual(6.0, get_average({partition, 1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.25, get_average({partition, 1, Stat, current}, CurrentStart1 + 12000)),
	?assertEqual(0.5, get_average({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(8.0, get_average({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average({partition, 3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(0.5, get_overall_average(partition, Stat, total, TotalStart1 + 10000)),
	?assertEqual(0.5, get_overall_average(partition, Stat, current, TotalStart1 + 10000)),

	Now = CurrentStart2 + 1000,
	reset_count({partition, 1, Stat, current}, Now),
	?assertEqual(Now, get_start({partition, 1, Stat, current})),
	?assertEqual(6.0, get_average({partition, 1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average({partition, 1, Stat, current}, Now + 12000)),
	?assertEqual(0.5, get_average({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(8.0, get_average({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average({partition, 3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(0.5, get_overall_average(partition, Stat, total, TotalStart1 + 10000)),
	?assertEqual(0.2, get_overall_average(partition, Stat, current, CurrentStart2 + 10000)),

	reset_all_stats(),
	?assertEqual(0.0, get_average({partition, 1, Stat, total}, Now + 500)),
	?assertEqual(0.0, get_average({partition, 1, Stat, current}, Now + 12000)),
	?assertEqual(0.0, get_average({partition, 2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(0.0, get_average({partition, 2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average({partition, 3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average({partition, 3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(0.0, get_overall_average(partition, Stat, total, TotalStart1 + 10000)),
	?assertEqual(0.0, get_overall_average(partition, Stat, current, CurrentStart2 + 10000)).

test_vdf_stats() ->
	ar_mining_stats:pause_performance_reports(120000),
	reset_all_stats(),
	ar_mining_stats:vdf_computed(),
	Start = get_start(vdf),
	ar_mining_stats:vdf_computed(),
	ar_mining_stats:vdf_computed(),

	?assertEqual(0.0, get_average(vdf, Start)),
	?assertEqual(6.0, get_average(vdf, Start + 500)),

	Now = Start + 1000,
	?assertEqual(1.0/3.0, vdf_speed(Now)),
	?assertEqual(Now, get_start(vdf)),
	?assertEqual(undefined, vdf_speed(Now)),
	?assertEqual(0.0, get_average(vdf, Now + 500)),
	?assertEqual(undefined, vdf_speed(Now + 500)),

	ar_mining_stats:vdf_computed(),
	Start2 = get_start(vdf),
	?assertEqual(0.5, vdf_speed(Start2 + 500)),

	ar_mining_stats:vdf_computed(),
	reset_all_stats(),
	?assertEqual(undefined, get_start(vdf)),
	?assertEqual(0.0, get_average(vdf, 1000)),
	?assertEqual(undefined, vdf_speed(1000)).

test_data_size_stats() ->
	ar_mining_stats:pause_performance_reports(120000),
	reset_all_stats(),
	?assertEqual(0, get_total_data_size()),
	?assertEqual(0, get_partition_data_size(1)),
	?assertEqual(0, get_partition_data_size(2)),

	ar_mining_stats:set_total_data_size(1000),
	?assertEqual(1000, get_total_data_size()),
	ar_mining_stats:set_total_data_size(500),
	?assertEqual(500, get_total_data_size()),

	ar_mining_stats:set_storage_module_data_size(store_id1, unpacked, 1, 100, 1, 100),
	ar_mining_stats:set_storage_module_data_size(store_id2, unpacked, 1, 300, 2, 200),
	ar_mining_stats:set_storage_module_data_size(store_id3, unpacked, 1, 200, 3, 50),
	ar_mining_stats:set_storage_module_data_size(store_id4, unpacked, 2, 300, 1, 200),

	?assertEqual(350, get_partition_data_size(1)),
	?assertEqual(200, get_partition_data_size(2)),

	ar_mining_stats:set_storage_module_data_size(store_id2, unpacked, 1, 300, 2, 100),
	ar_mining_stats:set_storage_module_data_size(store_id5, unpacked, 2, 100, 2, 100),

	?assertEqual(250, get_partition_data_size(1)),
	?assertEqual(300, get_partition_data_size(2)),

	reset_all_stats(),
	?assertEqual(0, get_total_data_size()),
	?assertEqual(0, get_partition_data_size(1)),
	?assertEqual(0, get_partition_data_size(2)).

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
	?assertEqual(0.0, get_average({peer, Peer1, Stat, total}, TotalStart1)),
	?assertEqual(0.0, get_average({peer, Peer1, Stat, current}, CurrentStart1)),
	?assertEqual(0.0, get_average({peer, Peer2, Stat, total}, TotalStart2)),
	?assertEqual(0.0, get_average({peer, Peer2, Stat, current}, CurrentStart2)),

	?assertEqual(60.0, get_average({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(2.5, get_average({peer, Peer1, Stat, current}, CurrentStart1 + 12000)),
	?assertEqual(5.0, get_average({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(80.0, get_average({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average({peer, Peer3, Stat, current}, TotalStart1 + 250)),

	?assertEqual(5.0, get_overall_average(peer, Stat, total, TotalStart1 + 10000)),
	?assertEqual(5.0, get_overall_average(peer, Stat, current, TotalStart1 + 10000)),

	Now = CurrentStart2 + 1000,
	reset_count({peer, Peer1, Stat, current}, Now),
	?assertEqual(Now, get_start({peer, Peer1, Stat, current})),
	?assertEqual(60.0, get_average({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average({peer, Peer1, Stat, current}, Now + 12000)),
	?assertEqual(5.0, get_average({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(80.0, get_average({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average({peer, Peer3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(5.0, get_overall_average(peer, Stat, total, TotalStart1 + 10000)),
	?assertEqual(2.0, get_overall_average(peer, Stat, current, CurrentStart2 + 10000)),

	reset_all_stats(),
	?assertEqual(0.0, get_average({peer, Peer1, Stat, total}, TotalStart1 + 500)),
	?assertEqual(0.0, get_average({peer, Peer1, Stat, current}, Now + 12000)),
	?assertEqual(0.0, get_average({peer, Peer2, Stat, total}, TotalStart2 + 4000)),
	?assertEqual(0.0, get_average({peer, Peer2, Stat, current}, CurrentStart2 + 250)),
	?assertEqual(0.0, get_average({peer, Peer3, Stat, total}, TotalStart1 + 4000)),
	?assertEqual(0.0, get_average({peer, Peer3, Stat, current}, CurrentStart1 + 250)),

	?assertEqual(0.0, get_overall_average(peer, Stat, total, TotalStart1 + 10000)),
	?assertEqual(0.0, get_overall_average(peer, Stat, current, CurrentStart2 + 10000)).

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

test_optimal_stats() ->
	?assertEqual(0.0, 
		optimal_partition_read_mibps(
			undefined, ?PARTITION_SIZE,
			floor(10 * ?PARTITION_SIZE), floor(10 * ?PARTITION_SIZE))),
	?assertEqual(200.0, 
		optimal_partition_read_mibps(
			1.0, ?PARTITION_SIZE,
			floor(10 * ?PARTITION_SIZE), floor(10 * ?PARTITION_SIZE))),
	?assertEqual(100.0, 
		optimal_partition_read_mibps(
			2.0, ?PARTITION_SIZE,
			floor(10 * ?PARTITION_SIZE), floor(10 * ?PARTITION_SIZE))),
	?assertEqual(50.0, 
		optimal_partition_read_mibps(
			1.0, floor(0.25 * ?PARTITION_SIZE),
			floor(10 * ?PARTITION_SIZE), floor(10 * ?PARTITION_SIZE))),
	?assertEqual(160.0, 
		optimal_partition_read_mibps(
			1.0, ?PARTITION_SIZE,
			floor(6 * ?PARTITION_SIZE), floor(10 * ?PARTITION_SIZE))).

test_report() ->
	ar_mining_stats:pause_performance_reports(120000),
	reset_all_stats(),
	MiningAddress = crypto:strong_rand_bytes(32),
	Partitions = [
		{1, MiningAddress},
		{2, MiningAddress},
		{3, MiningAddress}
	],
	Peer1 = ar_test_node:peer_ip(peer1),
	Peer2 = ar_test_node:peer_ip(peer2),
	Peer3 = ar_test_node:peer_ip(peer3),
	Peers = [Peer1, Peer2, Peer3],

	Now = erlang:monotonic_time(millisecond),
	WeaveSize = floor(10 * ?PARTITION_SIZE),
	ar_mining_stats:set_total_data_size(floor(0.6 * ?PARTITION_SIZE)),
	ar_mining_stats:set_storage_module_data_size(store_id1, unpacked, 1, floor(0.1 * ?PARTITION_SIZE), 1, floor(0.1 * ?PARTITION_SIZE)),
	ar_mining_stats:set_storage_module_data_size(store_id2, unpacked, 1, floor(0.3 * ?PARTITION_SIZE), 2, floor(0.2 * ?PARTITION_SIZE)),
	ar_mining_stats:set_storage_module_data_size(store_id3, unpacked, 1, floor(0.2 * ?PARTITION_SIZE), 3, floor(0.05 * ?PARTITION_SIZE)),
	ar_mining_stats:set_storage_module_data_size(store_id4, unpacked, 2, ?PARTITION_SIZE, 1, floor(0.25 * ?PARTITION_SIZE)),
	ar_mining_stats:vdf_computed(),
	ar_mining_stats:vdf_computed(),
	ar_mining_stats:vdf_computed(),
	ar_mining_stats:chunk_read(1),
	ar_mining_stats:chunk_read(1),
	ar_mining_stats:chunk_read(1),
	ar_mining_stats:chunk_read(2),
	ar_mining_stats:chunk_read(2),
	ar_mining_stats:hash_computed(1),
	ar_mining_stats:hash_computed(1),
	ar_mining_stats:hash_computed(1),
	ar_mining_stats:hash_computed(2),
	ar_mining_stats:hash_computed(2),
	ar_mining_stats:h1_sent_to_peer(Peer1, 10),
	ar_mining_stats:h1_sent_to_peer(Peer1, 5),
	ar_mining_stats:h1_sent_to_peer(Peer1, 15),
	ar_mining_stats:h1_sent_to_peer(Peer2, 1),
	ar_mining_stats:h1_sent_to_peer(Peer2, 19),
	ar_mining_stats:h1_received_from_peer(Peer2, 10),
	ar_mining_stats:h1_received_from_peer(Peer2, 5),
	ar_mining_stats:h1_received_from_peer(Peer2, 15),
	ar_mining_stats:h1_received_from_peer(Peer1, 1),
	ar_mining_stats:h1_received_from_peer(Peer1, 19),
	ar_mining_stats:h2_sent_to_peer(Peer1),
	ar_mining_stats:h2_sent_to_peer(Peer1),
	ar_mining_stats:h2_sent_to_peer(Peer1),
	ar_mining_stats:h2_sent_to_peer(Peer2),
	ar_mining_stats:h2_sent_to_peer(Peer2),
	ar_mining_stats:h2_received_from_peer(Peer1),
	ar_mining_stats:h2_received_from_peer(Peer1),
	ar_mining_stats:h2_received_from_peer(Peer2),
	ar_mining_stats:h2_received_from_peer(Peer2),
	ar_mining_stats:h2_received_from_peer(Peer2),
	
	Report1 = generate_report([], [], WeaveSize, Now+1000),
	?assertEqual(#report{ now = Now+1000 }, Report1),
	log_report(format_report(Report1, WeaveSize)),

	Report2 = generate_report(Partitions, Peers, WeaveSize, Now+1000),
	ReportString = format_report(Report2, WeaveSize),
	log_report(ReportString),
	?assertEqual(#report{ 
		now = Now+1000,
		vdf_speed = 1.0 / 3.0,
		total_data_size = floor(0.6 * ?PARTITION_SIZE),
		optimal_overall_read_mibps = 190.7998163223283,
		average_read_mibps = 1.25,
		current_read_mibps = 1.25,
		average_hash_hps = 5.0,
		current_hash_hps = 5.0,
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
				average_hash_hps = 0.0,
				current_hash_hps = 0.0
			},
			#partition_report{
				partition_number = 2,
				data_size = floor(0.25 * ?PARTITION_SIZE),
				optimal_read_mibps = 79.49999928474426,
				average_read_mibps = 0.5,
				current_read_mibps = 0.5,
				average_hash_hps = 2.0,
				current_hash_hps = 2.0
			},
			#partition_report{
				partition_number = 1,
				data_size = 734002,
				optimal_read_mibps = 111.299817037584039,
				average_read_mibps = 0.75,
				current_read_mibps = 0.75,
				average_hash_hps = 3.0,
				current_hash_hps = 3.0
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
	Report2).
	