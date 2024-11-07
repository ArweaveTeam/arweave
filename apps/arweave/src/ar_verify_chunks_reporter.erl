%%% The blob storage optimized for fast reads.
-module(ar_verify_chunks_reporter).

-behaviour(gen_server).

-export([start_link/0, update/2]).
-export([init/1, handle_cast/2, handle_call/3, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_verify_chunks.hrl").
-include_lib("eunit/include/eunit.hrl").

-record(state, {
	reports = #{} :: #{binary() => #verify_report{}}
}).

-define(REPORT_PROGRESS_INTERVAL, 10000).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec update(binary(), #verify_report{}) -> ok.
update(StoreID, Report) ->
	gen_server:cast(?MODULE, {update, StoreID, Report}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	ar_util:cast_after(?REPORT_PROGRESS_INTERVAL, self(), report_progress),
	{ok, #state{}}.


handle_cast({update, StoreID, Report}, State) ->
	{noreply, State#state{ reports = maps:put(StoreID, Report, State#state.reports) }};

handle_cast(report_progress, State) ->
	#state{
		reports = Reports
	} = State,

	print_reports(Reports),
	ar_util:cast_after(?REPORT_PROGRESS_INTERVAL, self(), report_progress),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_call(Call, From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {call, Call}, {from, From}]),
	{reply, ok, State}.

handle_info(Info, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

print_reports(Reports) when map_size(Reports) == 0 ->
	ok;
print_reports(Reports) ->
	print_header(),
	maps:foreach(
		fun(StoreID, Report) ->
			print_report(StoreID, Report)
		end,
		Reports
	),
	print_footer(),
	ok.

print_header() ->
	ar:console("Verification Report~n", []),
	ar:console("+-------------------------------------------------------------------+-----------+------+----------+-------------+~n", []),
	ar:console("|                                                    Storage Module | Processed |    % |   Errors | Verify Rate |~n", []),
	ar:console("+-------------------------------------------------------------------+-----------+------+----------+-------------+~n", []).

print_footer() ->
	ar:console("+-------------------------------------------------------------------+-----------+------+----------+-------------+~n~n", []).

print_report(StoreID, Report) ->
	#verify_report{
		total_error_bytes = TotalErrorBytes,
		bytes_processed = BytesProcessed,
		progress = Progress,
		start_time = StartTime
	} = Report,
	Duration = erlang:system_time(millisecond) - StartTime,
	Rate = 1000 * BytesProcessed / Duration,
	ar:console("| ~65s |   ~4B GB | ~3B% | ~5.1f GB | ~6.1f MB/s |~n", 
		[
			StoreID, BytesProcessed div 1000000000, Progress,
			TotalErrorBytes / 1000000000, Rate / 1000000
		]
	).