-module(ar_chain_stats).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_chain_stats.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([log_fork/2, get_forks/1]).

-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%%===================================================================
%%% Public interface.
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

log_fork([], _ForkRootB) ->
    %% No fork to log
    ok;
log_fork(Orphans, ForkRootB) ->
    gen_server:cast(?MODULE, {log_fork, Orphans, ForkRootB, os:system_time(millisecond)}).

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
    process_flag(trap_exit, true),
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "forks_db"), forks_db),
    {ok, #{}}.

handle_call({get_forks, StartTime}, _From, State) ->
    {ok, ForksMap} = ar_kv:get_range(forks_db, <<(StartTime * 1000):64>>),
    %% Sort forks by their key (the timestamp when they were detected)
    SortedForks = lists:sort(maps:to_list(ForksMap)),
    Forks = [binary_to_term(Fork) || {_Timestamp, Fork} <- SortedForks],
    {reply, Forks, State};
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({log_fork, Orphans, ForkRootB, ForkTime}, State) ->
    log_fork(Orphans, ForkRootB, ForkTime),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _state) ->
    ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================
log_fork(Orphans, ForkRootB, ForkTime) ->
    Fork = create_fork(Orphans, ForkRootB, ForkTime),
    ar_kv:put(forks_db, <<ForkTime:64>>, term_to_binary(Fork)),
    record_fork_depth(Orphans),
    ok.

create_fork(Orphans, ForkRootB, ForkTime) ->
    ForkID = crypto:hash(sha256, list_to_binary(Orphans)),
    #fork{
        id = ForkID,
        height = ForkRootB#block.height + 1,
        timestamp = ForkTime,
        block_ids = Orphans
    }.

record_fork_depth(Orphans) ->
    record_fork_depth(Orphans, 0).

record_fork_depth([], 0) ->
    ok;
record_fork_depth([], N) ->
    prometheus_histogram:observe(fork_recovery_depth, N),
    ok;
record_fork_depth([H | Orphans], N) ->
    ?LOG_INFO([{event, orphaning_block}, {block, ar_util:encode(H)}, {depth, N}]),
    record_fork_depth(Orphans, N + 1).


%%%===================================================================
%%% Tests.
%%%===================================================================
forks_test_() ->
    [
        {timeout, 30, fun test_fork_time/0},
        {timeout, 30, fun test_forks/0}
	].

test_fork_time() ->
    clear_forks_db(),
    ForkRootB1 = #block{ height = 1 },

    Orphans1 = [<<"a">>],
    ExpectedFork1 = #fork{
        id = crypto:hash(sha256, list_to_binary(Orphans1)),
        height = 2,
        block_ids = Orphans1
    },
    Orphans2 = [<<"b">>, <<"c">>],
    ExpectedFork2 = #fork{
        id = crypto:hash(sha256, list_to_binary(Orphans2)),
        height = 2,
        block_ids = Orphans2
    },

    gen_server:cast(?MODULE, {log_fork, Orphans1, ForkRootB1, 2}),
    gen_server:cast(?MODULE, {log_fork, Orphans2, ForkRootB1, 11}),
    assert_forks_equal([ExpectedFork1, ExpectedFork2], get_forks(0)).

test_forks() ->
    clear_forks_db(),
    StartTime = os:system_time(seconds),
    ForkRootB1 = #block{ height = 1 },
    ForkRootB2= #block{ height = 2 },

    Orphans1 = [<<"a">>],
    timer:sleep(5),
    log_fork(Orphans1, ForkRootB1),
    ExpectedFork1 = #fork{
        id = crypto:hash(sha256, list_to_binary(Orphans1)),
        height = 2,
        block_ids = Orphans1
    },
    assert_forks_equal([ExpectedFork1], get_forks(StartTime)),

    Orphans2 = [<<"b">>, <<"c">>],
    timer:sleep(5),
    log_fork(Orphans2, ForkRootB1),
    ExpectedFork2 = #fork{
        id = crypto:hash(sha256, list_to_binary(Orphans2)),
        height = 2,
        block_ids = Orphans2
    },
    assert_forks_equal([ExpectedFork1, ExpectedFork2], get_forks(StartTime)),

    Orphans3 = [<<"b">>, <<"c">>, <<"d">>],
    timer:sleep(5),
    log_fork(Orphans3, ForkRootB1),
    ExpectedFork3 = #fork{
        id = crypto:hash(sha256, list_to_binary(Orphans3)),
        height = 2,
        block_ids = Orphans3
    },
    assert_forks_equal(
        [ExpectedFork1, ExpectedFork2, ExpectedFork3],
        get_forks(StartTime)),

    Orphans4 = [<<"e">>, <<"f">>, <<"g">>],
    timer:sleep(1000),
    log_fork(Orphans4, ForkRootB2),
    ExpectedFork4 = #fork{
        id = crypto:hash(sha256, list_to_binary(Orphans4)),
        height = 3,
        block_ids = Orphans4
    },
    assert_forks_equal(
        [ExpectedFork1, ExpectedFork2, ExpectedFork3, ExpectedFork4],
        get_forks(StartTime)),

    %% Same fork seen again - not sure this is possible, but since we're just tracking
    %% forks based on when they occur, it should be handled.
    timer:sleep(5),
    log_fork(Orphans3, ForkRootB1),
    assert_forks_equal(
        [ExpectedFork1, ExpectedFork2, ExpectedFork3, ExpectedFork4, ExpectedFork3],
        get_forks(StartTime)),

    %% If the fork is empty, ignore it.
    timer:sleep(5),
    log_fork([], ForkRootB2),
    assert_forks_equal(
        [ExpectedFork1, ExpectedFork2, ExpectedFork3, ExpectedFork4, ExpectedFork3],
        get_forks(StartTime)),

    %% Check that the cutoff time is handled correctly
    timer:sleep(5),
    assert_forks_equal(
        [ExpectedFork4, ExpectedFork3],
        get_forks(StartTime+1)).

assert_forks_equal(ExpectedForks, ActualForks) ->
    ExpectedForksStripped = [ Fork#fork{timestamp = undefined} || Fork <- ExpectedForks],
    ActualForksStripped = [ Fork#fork{timestamp = undefined} || Fork <- ActualForks],
    ?assertEqual(ExpectedForksStripped, ActualForksStripped).

clear_forks_db() ->
    Time = os:system_time(millisecond),
    ar_kv:delete_range(forks_db, integer_to_binary(0), integer_to_binary(Time)).
