%%% @doc Tests for `arweave_config_signal_handler`.
%%%
%%% IMPORTANT: we never deliver `sigquit` or `sigterm` — those signals
%%% halt the BEAM (`erlang:halt/2`) or stop the node (`init:stop/1`)
%%% which would take down the CT runner. The private `signal/2`
%%% clauses for those two are therefore deliberately excluded.
%%%
%%% Approach: `signal/2` is not exported, but `handle_event/2`
%%% (a gen_event callback) is and it dispatches to `signal/2`. We call
%%% `handle_event/2` directly with an explicit state map and assert
%%% the returned `{ok, NewState}` — no real OS signal is ever raised.
%%% For `sigusr1` this incidentally exercises the `sigusr1/0` helper,
%%% which spawns a separate `arweave_diagnostic:all/0` process; that
%%% process is fire-and-forget and does not affect the SUITE state.
-module(arweave_config_signal_handler_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(MOD, arweave_config_signal_handler).

init_per_suite(Config) ->
	ok = arweave_config:start(),
	Config.

end_per_suite(_Config) ->
	ok = arweave_config:stop().

init_per_testcase(_TestCase, Config) -> Config.

end_per_testcase(_TestCase, _Config) ->
	%% Let any spawned diagnostic processes finish writing logs before
	%% the next case starts.
	timer:sleep(50),
	ok.

all() ->
	[
		signals_list,
		gen_event_registered,
		signal_dispatches_sigusr1,
		signal_dispatches_sigusr2,
		state_counter_increments,
		handle_event_non_atom_event
	].

%%====================================================================
%% Test cases
%%====================================================================

%% Note on `signals/0`: the function is not exported from the module,
%% so we cannot call it directly. Instead `signals_list/1` asserts the
%% set indirectly — every signal advertised by the public type spec
%% (`sigquit`, `sigterm`, `sigusr1`, `sigusr2`) is accepted by
%% `handle_event/2` and bumps its counter. `sigquit` and `sigterm` are
%% NEVER delivered live (they would halt the BEAM); we only assert
%% that the two safe signals dispatch correctly, which proves the
%% gen_event handler is wired up for the supported-signal family.

%% --------------------------------------------------------------------
%% Public API
%% --------------------------------------------------------------------

signals_list(_Config) ->
	lists:foreach(
		fun(Signal) ->
			{ok, S} = ?MOD:handle_event(Signal, #{}),
			?assertEqual(1, maps:get(Signal, S))
		end, [sigusr1, sigusr2]),
	ok.

gen_event_registered(_Config) ->
	Pid = whereis(?MOD),
	?assert(is_pid(Pid)),
	?assert(is_process_alive(Pid)),
	ok.

%% --------------------------------------------------------------------
%% Dispatch via `handle_event/2` (no real OS signal)
%% --------------------------------------------------------------------

signal_dispatches_sigusr1(_Config) ->
	{ok, S1} = ?MOD:handle_event(sigusr1, #{}),
	?assertEqual(1, maps:get(sigusr1, S1)),
	{ok, S2} = ?MOD:handle_event(sigusr1, S1),
	?assertEqual(2, maps:get(sigusr1, S2)),
	ok.

signal_dispatches_sigusr2(_Config) ->
	{ok, S1} = ?MOD:handle_event(sigusr2, #{}),
	?assertEqual(1, maps:get(sigusr2, S1)),
	{ok, S2} = ?MOD:handle_event(sigusr2, S1),
	?assertEqual(2, maps:get(sigusr2, S2)),
	%% Other signals are untouched.
	?assertEqual(error, maps:find(sigusr1, S2)),
	?assertEqual(error, maps:find(sigquit, S2)),
	?assertEqual(error, maps:find(sigterm, S2)),
	ok.

state_counter_increments(_Config) ->
	S0 = #{},
	{ok, S1} = ?MOD:handle_event(sigusr1, S0),
	{ok, S2} = ?MOD:handle_event(sigusr2, S1),
	{ok, S3} = ?MOD:handle_event(sigusr1, S2),
	{ok, S4} = ?MOD:handle_event(sigusr2, S3),
	{ok, S5} = ?MOD:handle_event(sigusr1, S4),
	?assertEqual(3, maps:get(sigusr1, S5)),
	?assertEqual(2, maps:get(sigusr2, S5)),
	ok.

handle_event_non_atom_event(_Config) ->
	State = #{ sigusr1 => 42 },
	?assertEqual({ok, State},
		?MOD:handle_event({some, tuple, event}, State)),
	?assertEqual({ok, State},
		?MOD:handle_event(<<"binary event">>, State)),
	ok.
