-module(ar_chain_stats).

-behaviour(gen_server).

-include("ar.hrl").
-include("ar_chain_stats.hrl").

-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([log_fork/2, log_fork/3, get_forks/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%%===================================================================
%%% Public interface.
%%%===================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


log_fork(Orphans, ForkRootB) ->
	log_fork(Orphans, ForkRootB, os:system_time(millisecond)).

log_fork([], _ForkRootB, _ForkTime) ->
	%% No fork to log
	ok;
log_fork(Orphans, ForkRootB, ForkTime) ->
	gen_server:cast(?MODULE, {log_fork, Orphans, ForkRootB, ForkTime}).
	

%% @doc Returns all forks that have been logged since the given start time
%% (system time in seconds)
get_forks(StartTime) ->
	case catch gen_server:call(?MODULE, {get_forks, StartTime}) of
		{'EXIT', {timeout, {gen_server, call, _}}} ->
			{error, timeout};
		Reply ->
			Reply
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================
init([]) ->
	%% Trap exit to avoid corrupting any open files on quit..
	process_flag(trap_exit, true),
	{ok, Config} = arweave_config:get_env(),
	ok = ar_kv:open(#{
		path => filename:join([Config#config.data_dir, ?ROCKS_DB_DIR, "forks_db"]),
		name => forks_db}),
	{ok, #{}}.

handle_call({get_forks, StartTime}, _From, State) ->
	{ok, ForksMap} = ar_kv:get_range(forks_db, <<(StartTime * 1000):64>>),
	%% Sort forks by their key (the timestamp when they were detected) - sorts in
	%% chronological / ascending order (i.e. first element of the list is the oldest fork)
	SortedForks = lists:sort(maps:to_list(ForksMap)),
	Forks = [binary_to_term(Fork) || {_Timestamp, Fork} <- SortedForks],
	{reply, Forks, State};
handle_call(_Request, _From, State) ->
	{reply, ok, State}.

handle_cast({log_fork, Orphans, ForkRootB, ForkTime}, State) ->
	do_log_fork(Orphans, ForkRootB, ForkTime),
	{noreply, State};
handle_cast(_Msg, State) ->
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.

terminate(Reason, _state) ->
	?LOG_INFO([{module, ?MODULE}, {pid, self()}, {callback, terminate},{reason, Reason}]),
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================
do_log_fork(Orphans, ForkRootB, ForkTime) ->
	Fork = create_fork(Orphans, ForkRootB, ForkTime),
	ar_kv:put(forks_db, <<ForkTime:64>>, term_to_binary(Fork)),
	record_fork_depth(Orphans, ForkRootB),
	ok.

create_fork(Orphans, ForkRootB, ForkTime) ->
	ForkID = crypto:hash(sha256, list_to_binary(Orphans)),
	#fork{
		id = ForkID,
		height = ForkRootB#block.height + 1,
		timestamp = ForkTime,
		block_ids = Orphans
	}.

record_fork_depth(Orphans, ForkRootB) ->
	record_fork_depth(Orphans, ForkRootB, 0).

record_fork_depth([], _ForkRootB, 0) ->
	ok;
record_fork_depth([], _ForkRootB, N) ->
	ok;
record_fork_depth([H | Orphans], ForkRootB, N) ->
	SolutionHashInfo =
		case ar_block_cache:get(block_cache, H) of
			not_found ->
				%% Should never happen, by construction.
				?LOG_ERROR([{event, block_not_found_in_cache}, {h, ar_util:encode(H)}]),
				[];
			#block{ hash = SolutionH } ->
				[{solution_hash, ar_util:encode(SolutionH)}]
		end,
	LogInfo = [
		{event, orphaning_block}, {block, ar_util:encode(H)}, {depth, N},
		{fork_root, ar_util:encode(ForkRootB#block.indep_hash)},
		{fork_height, ForkRootB#block.height + 1} | SolutionHashInfo],
	?LOG_INFO(LogInfo),
	record_fork_depth(Orphans, ForkRootB, N + 1).

%%%===================================================================
%%% Tests.
%%%===================================================================
forks_test_() ->
	[
		{timeout, 30, fun test_forks/0}
	].

test_forks() ->
	clear_forks_db(),
	StartTimeSeconds = 60,
	ForkRootB1 = #block{ indep_hash = <<"1">>, height = 1 },
	ForkRootB2= #block{ indep_hash = <<"2">>, height = 2 },

	Orphans1 = [<<"a">>],
	Time1 = (StartTimeSeconds * 1000) + 5,
	log_fork(Orphans1, ForkRootB1, Time1),
	ExpectedFork1 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans1)),
		height = 2,
		block_ids = Orphans1,
		timestamp = Time1
	},
	assert_forks_equal([ExpectedFork1], get_forks(StartTimeSeconds)),

	Orphans2 = [<<"b">>, <<"c">>],
	Time2 = (StartTimeSeconds * 1000) + 10,
	log_fork(Orphans2, ForkRootB1, Time2),
	ExpectedFork2 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans2)),
		height = 2,
		block_ids = Orphans2,
		timestamp = Time2
	},
	assert_forks_equal([ExpectedFork1, ExpectedFork2], get_forks(StartTimeSeconds)),

	Orphans3 = [<<"b">>, <<"c">>, <<"d">>],
	Time3 = (StartTimeSeconds * 1000) + 15,
	log_fork(Orphans3, ForkRootB1, Time3),
	ExpectedFork3 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans3)),
		height = 2,
		block_ids = Orphans3,
		timestamp = Time3
	},
	assert_forks_equal(
		[ExpectedFork1, ExpectedFork2, ExpectedFork3],
		get_forks(StartTimeSeconds)),

	Orphans4 = [<<"e">>, <<"f">>, <<"g">>],
	Time4 = (StartTimeSeconds * 1000) + 1000,
	log_fork(Orphans4, ForkRootB2, Time4),
	ExpectedFork4 = #fork{
		id = crypto:hash(sha256, list_to_binary(Orphans4)),
		height = 3,
		block_ids = Orphans4,
		timestamp = Time4
	},
	assert_forks_equal(
		[ExpectedFork1, ExpectedFork2, ExpectedFork3, ExpectedFork4],
		get_forks(StartTimeSeconds)),

	%% Same fork seen again - not sure this is possible, but since we're just tracking
	%% forks based on when they occur, it should be handled.
	Time5 = (StartTimeSeconds * 1000) + 1005,
	log_fork(Orphans3, ForkRootB1, Time5),
	ExpectedFork5 = ExpectedFork3#fork{timestamp = Time5},
	assert_forks_equal(
		[ExpectedFork1, ExpectedFork2, ExpectedFork3, ExpectedFork4, ExpectedFork5],
		get_forks(StartTimeSeconds)),

	%% If the fork is empty, ignore it.
	Time6 = (StartTimeSeconds * 1000) + 1010,
	log_fork([], ForkRootB2, Time6),
	assert_forks_equal(
		[ExpectedFork1, ExpectedFork2, ExpectedFork3, ExpectedFork4, ExpectedFork5],
		get_forks(StartTimeSeconds)),

	%% Check that the cutoff time is handled correctly
	assert_forks_equal(
		[ExpectedFork4, ExpectedFork5],
		get_forks(StartTimeSeconds+1)),
	ok.

assert_forks_equal(ExpectedForks, ActualForks) ->
	?assertEqual(ExpectedForks, ActualForks).

clear_forks_db() ->
	Time = os:system_time(millisecond),
	ar_kv:delete_range(forks_db, integer_to_binary(0), integer_to_binary(Time)).
