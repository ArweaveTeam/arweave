%%%===================================================================
%%% @doc Local (non-distributed) test utilities. Unlike `ar_test_node',
%%% these helpers don't require the peer cluster, so they're safe to
%%% use in modules tagged `-test_category([fast])'.
%%%
%%% When adding new helpers here, the rule is: it must work in a
%%% single BEAM with no peer nodes booted. If it needs `remote_call'
%%% or peer-side state, it belongs in `ar_test_node' instead.
%%%===================================================================
-module(ar_test_util).

-export([with_mocked/2, with_mocked/3]).
-export([new_mock/2, mock_function/3, unmock_module/1]).
-export([load_fixture/1]).

-include_lib("eunit/include/eunit.hrl").

-define(DEFAULT_TIMEOUT, 30).

%%%===================================================================
%%% Public API: eunit fixture
%%%===================================================================

%% @doc Wrap TestFun in an eunit `{setup, ...}' fixture that mocks the
%% given module functions via local meck (no peer broadcast). Each
%% module is `meck:new'd with `passthrough', the mocks are installed,
%% the test runs, and all modules are `meck:unload'ed on completion.
%%
%% This is the batch-safe alternative to
%% `ar_test_node:test_with_all_nodes_mocked/2,3', which broadcasts
%% mocks to all peer nodes and therefore requires the slow path.
%%
%% Use this when the mocks just stub out constant-returning helpers
%% or otherwise don't need to be visible on peer-side processes.
with_mocked(Mocks, TestFun) ->
	with_mocked(Mocks, TestFun, ?DEFAULT_TIMEOUT).

with_mocked(Mocks, TestFun, Timeout) ->
	{
		setup,
		fun() ->
			Modules = lists:usort([M || {M, _, _} <- Mocks]),
			lists:foreach(
				fun(M) -> new_mock(M, [passthrough]) end, Modules),
			lists:foreach(
				fun({M, F, Impl}) -> mock_function(M, F, Impl) end, Mocks),
			Modules
		end,
		fun(Modules) ->
			lists:foreach(fun unmock_module/1, Modules)
		end,
		{timeout, Timeout, TestFun}
	}.

%%%===================================================================
%%% Public API: robust local meck primitives
%%%
%%% These are the local-only equivalents of meck:new/2, meck:expect/3,
%%% meck:unload/1 with retry, already-mocked tolerance, and
%%% suspend/resume handling for stuck meck processes.
%%%
%%% `ar_test_node' uses these for the local part of its peer-broadcast
%%% mocking. Code that only needs local mocking should use them
%%% directly (or via with_mocked/2,3 above).
%%%===================================================================

new_mock(Module, Options) ->
	new_mock(Module, Options, 5).

new_mock(_Module, _Options, 0) ->
	ok;
new_mock(Module, Options, Retries) ->
	Options2 = lists:usort([no_link | Options]),
	try
		meck:new(Module, Options2)
	catch
		%% If the mock is already started, treat as success
		error:{already_started, _Pid} ->
			ok;
		%% Retry on other errors
		error:E ->
			?debugFmt("ar_test_util (retries left ~p): Error creating mock for ~p: ~p",
					[Retries - 1, Module, E]),
			timer:sleep(1000),
			new_mock(Module, Options, Retries - 1);
		exit:E ->
			?debugFmt("ar_test_util (retries left ~p): Exit creating mock for ~p: ~p",
					[Retries - 1, Module, E]),
			timer:sleep(1000),
			new_mock(Module, Options, Retries - 1)
	end.

mock_function(Module, Fun, Mock) ->
	mock_function(Module, Fun, Mock, 5).

mock_function(_Module, _Fun, _Mock, 0) ->
	ok;
mock_function(Module, Fun, Mock, Retries) ->
	try
		meck:expect(Module, Fun, Mock)
	catch
		error:E ->
			?debugFmt("ar_test_util (retries left ~p): Error setting mock for ~p: ~p",
					[Retries - 1, Module, E]),
			timer:sleep(1000),
			mock_function(Module, Fun, Mock, Retries - 1);
		exit:E ->
			?debugFmt("ar_test_util (retries left ~p): Exit setting mock for ~p: ~p",
					[Retries - 1, Module, E]),
			timer:sleep(1000),
			mock_function(Module, Fun, Mock, Retries - 1)
	end.

unmock_module(Module) ->
	unmock_module(Module, 5).

unmock_module(_Module, 0) ->
	ok;
unmock_module(Module, Retries) ->
	Pid = erlang:whereis(Module),
	case is_pid(Pid) of
		true ->
			catch sys:suspend(Pid, 5000);
		false ->
			ok
	end,
	try
		timed_meck_unload(Module, 10000)
	catch
		error:{not_mocked, Module} ->
			ok;
		error:E ->
			?debugFmt("ar_test_util (retries left ~p): Error unloading mock for ~p: ~p",
					[Retries - 1, Module, E]),
			resume_if_alive(Pid),
			timer:sleep(1000),
			unmock_module(Module, Retries - 1);
		exit:E ->
			?debugFmt("ar_test_util (retries left ~p): Exit unloading mock for ~p: ~p",
					[Retries - 1, Module, E]),
			resume_if_alive(Pid),
			timer:sleep(1000),
			unmock_module(Module, Retries - 1)
	after
		resume_if_alive(Pid)
	end.

%%%===================================================================
%%% Test fixtures (pure file I/O, no peer involvement)
%%%===================================================================

%% @doc Read a file from the `fixtures/' directory next to this
%% module's source. Pure file I/O — safe to use from fast-tagged
%% tests. `?FILE' is the compile-time path of ar_test_util.erl, which
%% lives next to the `fixtures/' directory.
load_fixture(Fixture) ->
	Dir = filename:dirname(?FILE),
	{ok, Data} = file:read_file(
		filename:join([Dir, "fixtures", Fixture])),
	Data.

%%%===================================================================
%%% Internal helpers
%%%===================================================================

resume_if_alive(Pid) ->
	case is_pid(Pid) andalso erlang:is_process_alive(Pid) of
		true ->
			catch sys:resume(Pid);
		false ->
			ok
	end.

%% meck:unload internally uses gen_server:call(..., infinity), so if the meck
%% process is stuck handling a call from a blocked process, it will hang forever
%% and the catch/retry logic above never fires. Wrap it with a finite timeout
%% and kill the stuck meck process if needed.
%%
%% After killing the meck process we must restore the original module from the
%% beam file on disk, because meck's terminate (which normally does this) did
%% not run.
timed_meck_unload(Module, Timeout) ->
	Caller = self(),
	Ref = make_ref(),
	Worker = spawn(fun() ->
		try
			Result = meck:unload(Module),
			Caller ! {Ref, {ok, Result}}
		catch
			Class:Reason ->
				Caller ! {Ref, {Class, Reason}}
		end
	end),
	receive
		{Ref, {ok, Result}} ->
			Result;
		{Ref, {error, Reason}} ->
			error(Reason);
		{Ref, {exit, Reason}} ->
			exit(Reason)
	after Timeout ->
		exit(Worker, kill),
		MeckProcName = list_to_atom(atom_to_list(Module) ++ "_meck"),
		case erlang:whereis(MeckProcName) of
			undefined ->
				ok;
			MeckPid ->
				exit(MeckPid, kill),
				timer:sleep(100)
		end,
		force_restore_module(Module),
		exit(timed_meck_unload_timeout)
	end.

%% After force-killing the meck process, the module is left with meck-generated
%% stub code and no backing ETS tables. Restore the original beam from disk so
%% processes don't crash in an infinite meck stub loop.
force_restore_module(Module) ->
	OrigName = list_to_atom(atom_to_list(Module) ++ "_meck_original"),
	code:purge(Module),
	code:delete(Module),
	code:purge(OrigName),
	code:delete(OrigName),
	code:load_file(Module).
