%%% @doc Tests for `arweave_config_sup' — the top-level supervisor.
%%%
%%% Strategy: start the application once per suite. The supervisor's
%%% `one_for_all' strategy means killing any child triggers a
%%% full-tree restart; we exploit that to drive the restart test.
-module(arweave_config_sup_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

init_per_suite(Config) ->
	ok = arweave_config:start(),
	Config.

end_per_suite(_Config) ->
	ok = arweave_config:stop().

init_per_testcase(_TestCase, Config) -> Config.

end_per_testcase(_TestCase, _Config) -> ok.

all() ->
	[
		supervisor_starts_all_children,
		one_for_all_restart
	].

%%====================================================================
%% Test cases
%%====================================================================

supervisor_starts_all_children(_Config) ->
	lists:foreach(
		fun(Name) ->
			Pid = whereis(Name),
			case is_pid(Pid) andalso is_process_alive(Pid) of
				true -> ok;
				false -> ct:fail({child_not_alive, Name, Pid})
			end
		end,
		child_names()),
	ok.

one_for_all_restart(_Config) ->
	OriginalPids = [{Name, whereis(Name)} || Name <- child_names()],
	%% Kill a non-supervisor leaf child. Picking `arweave_config_store'
	%% avoids hitting the registered supervisor name itself.
	Victim = arweave_config_store,
	exit(whereis(Victim), kill),
	%% Wait for the supervisor to restart the tree. The new PIDs must
	%% appear under all child names. Bounded polling — no fixed sleep.
	wait_for_restart(OriginalPids, 50),
	NewPids = [{Name, whereis(Name)} || Name <- child_names()],
	lists:foreach(
		fun({{Name, Old}, {Name, New}}) ->
			case is_pid(New) andalso is_process_alive(New) of
				true -> ok;
				false -> ct:fail({child_did_not_restart, Name, Old, New})
			end,
			?assertNotEqual(Old, New)
		end,
		lists:zip(OriginalPids, NewPids)),
	ok.

%%====================================================================
%% Helpers
%%====================================================================

%% Children advertised by `arweave_config_sup:init/1', in the order
%% it lists them.
child_names() ->
	[
		arweave_config_store,
		arweave_config_options_registry,
		arweave_config_signal_handler
	].

%% Spin until every child name resolves to a PID different from the
%% one captured in `Original'. `MaxAttempts' is multiplied by 20ms
%% sleeps between polls.
wait_for_restart(_Original, 0) ->
	ct:fail(restart_timeout);
wait_for_restart(Original, N) ->
	case lists:all(
		fun({Name, Old}) ->
			P = whereis(Name),
			is_pid(P) andalso P =/= Old andalso is_process_alive(P)
		end,
		Original)
	of
		true ->
			ok;
		false ->
			timer:sleep(20),
			wait_for_restart(Original, N - 1)
	end.
