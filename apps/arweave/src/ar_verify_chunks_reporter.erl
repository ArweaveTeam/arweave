%%% The blob storage optimized for fast reads.
-module(ar_verify_chunks_reporter).

-behaviour(gen_server).

-export([start_link/0, update/2]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include("../include/ar.hrl").
-include("../include/ar_verify_chunks.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	verify_reports = #{} :: #{string() => #verify_report{}},
	sample_reports = #{} :: #{string() => #sample_report{}}
}).

-define(REPORT_PROGRESS_INTERVAL, 10000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec update(string(), #verify_report{} | #sample_report{}) -> ok.
update(StoreID, Report) ->
	gen_server:cast(?MODULE, {update, StoreID, Report}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ar_util:cast_after(?REPORT_PROGRESS_INTERVAL, self(), report_progress),
	{ok, #state{}}.


handle_cast({update, StoreID, #verify_report{} = Report}, State) ->
	{noreply, State#state{ verify_reports = maps:put(StoreID, Report, State#state.verify_reports) }};

handle_cast({update, StoreID, #sample_report{} = Report}, State) ->
	{noreply, State#state{ sample_reports = maps:put(StoreID, Report, State#state.sample_reports) }};

handle_cast(report_progress, State) ->
	#state{
		verify_reports = VerifyReports,
		sample_reports = SampleReports
	} = State,

	print_sample_reports(SampleReports),
	print_verify_reports(VerifyReports),
	ar_util:cast_after(?REPORT_PROGRESS_INTERVAL, self(), report_progress),
	{noreply, State};

% handle_cast({sample_update, StoreID, SampleReport}, State) ->
% 	NewSampleReports = maps:put(StoreID, SampleReport, State#state.sample_reports),
% 	print_sampling_header(),
% 	print_sample_report(StoreID, SampleReport),
% 	{noreply, State#state{sample_reports = NewSampleReports}};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call(Call, From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {call, Call}, {from, From}]),
	{reply, ok, State}.

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(Reason, _State) ->
	?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]),
	ok.

print_verify_reports(Reports) when map_size(Reports) == 0 ->
	ok;
print_verify_reports(Reports) ->
	print_verify_header(),
	maps:foreach(
		fun(StoreID, Report) ->
			print_verify_report(StoreID, Report)
		end,
		Reports
	),
	print_verify_footer(),
	ok.

print_verify_header() ->
	ar:console("Verification Report~n", []),
	ar:console("+-------------------------------------------------------------------+-----------+-------+------------+------------+-------------+---------+~n", []),
	ar:console("|                                                    Storage Module | Processed |     % | Errors (#) | Errors (%) | Verify Rate |  Status |~n", []),
	ar:console("+-------------------------------------------------------------------+-----------+-------+------------+------------+-------------+---------+~n", []).

print_verify_footer() ->
	ar:console("+-------------------------------------------------------------------+-----------+-------+------------+------------+-------------+---------+~n~n", []).

print_verify_report(StoreID, Report) ->
	#verify_report{
		total_error_chunks = TotalErrorChunks,
		total_error_bytes = TotalErrorBytes,
		bytes_processed = BytesProcessed,
		progress = Progress,
		start_time = StartTime,
		status = Status
	} = Report,
	Duration = erlang:system_time(millisecond) - StartTime,
	Rate = 1000 * BytesProcessed / Duration,
	ar:console("| ~65s |   ~4B GB | ~4B% | ~10B | ~9.2f% | ~6.1f MB/s | ~7s |~n", 
		[
			StoreID, BytesProcessed div 1000000000, Progress,
			TotalErrorChunks, (TotalErrorBytes * 100) / BytesProcessed, Rate / 1000000,
			Status
		]
	).

print_sample_reports(Reports) when map_size(Reports) == 0 ->
	ok;
print_sample_reports(Reports) ->
	print_sample_header(),
	maps:foreach(
		fun(StoreID, Report) ->
			print_sample_report(StoreID, Report)
		end,
		Reports
	),
	print_sample_footer(),
	ok.

print_sample_report(StoreID, Report) ->
	#sample_report{
		samples = MaxSamples,
		total = Total,
		success = Success,
		failure = Failure
	} = Report,
	ar:console("| ~65s | ~9B | ~4B% | ~6.1f% | ~6.1f% |~n",
		[
			StoreID,
			Total,
			(Total * 100) div MaxSamples,
			(Success * 100) / Total,
			(Failure * 100) / Total
		]).

print_sample_header() ->
	ar:console("Chunk Sample Report~n", []),
	ar:console("+-------------------------------------------------------------------+-----------+-------+---------+---------+~n", []),
	ar:console("|                                                    Storage Module | Processed |     % | Success |   Error |~n", []),
	ar:console("+-------------------------------------------------------------------+-----------+-------+---------+---------+~n", []).

print_sample_footer() ->
	ar:console("+-------------------------------------------------------------------+-----------+-------+---------+---------+~n~n", []).
