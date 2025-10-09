-module(ar_mining_worker_tests).

-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_mining.hrl").
-include_lib("arweave/include/ar_consensus.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(PARTITION, 0).

-define(TESTER_REGISTER_NAME, ?MODULE).
-record(state, {
	worker_pid :: pid(),
	session_key :: binary(),
	candidate :: #mining_candidate{}
}).

-define(compute_h0(WorkerPid, Candidate), {compute_h0, WorkerPid, Candidate}).
-define(compute_h1(WorkerPid, Candidate), {compute_h1, WorkerPid, Candidate}).
-define(compute_h2(WorkerPid, Candidate), {compute_h2, WorkerPid, Candidate}).
-define(get_recall_range(H0, Partition, PartitionUpperBound), {get_recall_range, H0, Partition, PartitionUpperBound}).
-define(get_recall_range_response(RecallRange1, RecallRange2), {get_recall_range_response, RecallRange1, RecallRange2}).
-define(is_recall_range_readable(H0, RecallRange), {is_recall_range_readable, H0, RecallRange}).
-define(is_recall_range_readable_response(IsReadable), {is_recall_range_readable_response, IsReadable}).
-define(read_recall_range(Kind, WorkerPid, Candidate2, RecallRangeStart), {read_recall_range, Kind, WorkerPid, Candidate2, RecallRangeStart}).
-define(read_recall_range_response(), {read_recall_range_response}).
-define(passes_diff_check(SolutionHash, IsPoA1, DiffPair, PackingDifficulty), {passes_diff_check, SolutionHash, IsPoA1, DiffPair, PackingDifficulty}).
-define(passes_diff_check_response(IsPassed), {passes_diff_check_response, IsPassed}).
-define(prepare_and_post_solution(Candidate), {prepare_and_post_solution, Candidate}).

-define(with_setup_each(F), fun() ->
	State = setup_each(),
	try
		F(State)
	catch
		C:E:S ->
			?debugFmt("Error: ~p:~p:~p", [C, E, S]),
			cleanup_each(State),
			throw({C, E})
	after
		cleanup_each(State)
	end
end).

%% ------------------------------------------------------------------------------------------------
%% Fixtures
%% ------------------------------------------------------------------------------------------------

%% This function is called for all tests.
%% It mocks the necessary modules and functions.
%% Mocked functions are sending messages to the ?TESTER_REGISTER_NAME process (the test).
setup_all() ->
	Mocks = [
		{ar_mining_cache, session_exists, fun(_, _) -> true end},
		{ar_mining_hash, compute_h0, fun ar_mining_hash__compute_h0/2},
		{ar_mining_hash, compute_h1, fun ar_mining_hash__compute_h1/2},
		{ar_mining_hash, compute_h2, fun ar_mining_hash__compute_h2/2},
		{ar_block, get_recall_range, fun ar_block__get_recall_range/3},
		{ar_mining_io, is_recall_range_readable, fun ar_mining_io__is_recall_range_readable/2},
		{ar_mining_io, read_recall_range, fun ar_mining_io__read_recall_range/4},
		{ar_node_utils, passes_diff_check, fun ar_node_utils__passes_diff_check/4},
		{ar_mining_server, prepare_and_post_solution, fun ar_mining_server__prepare_and_post_solution/1}
	],
	MockedModules = lists:usort([Module || {Module, _, _} <- Mocks]),
	meck:new(MockedModules, [passthrough]),
	[meck:expect(M, F, Fun) || {M, F, Fun} <- Mocks],
	MockedModules.

%% This function is called for each test.
%% It creates a new worker and registers itself as a tester.
%% It also sets up the worker with the necessary cache limits and sessions.
setup_each() ->
	StepNumber = 1,
	MiningSession = <<"mining_session">>,
	Candidate = #mining_candidate{
		packing_difficulty = ?REPLICA_2_9_PACKING_DIFFICULTY,
		session_key = MiningSession,
		step_number = StepNumber
	},
	{ok, Pid} = ar_mining_worker:start_link(?PARTITION, ?REPLICA_2_9_PACKING_DIFFICULTY),
	ar_mining_worker:set_cache_limits(Pid, 10 * ?MiB, 1_000),
	ar_mining_worker:set_sessions(Pid, [MiningSession]),

	register(?TESTER_REGISTER_NAME, self()),

	#state{
		worker_pid = Pid,
		session_key = MiningSession,
		candidate = Candidate
	}.

%% This function is called after each test.
%% It unregisters the tester and exits the worker.
cleanup_each(State) ->
	cleanup_each_dump_messages(),
	unregister(?TESTER_REGISTER_NAME),
	erlang:unlink(State#state.worker_pid),
	erlang:exit(State#state.worker_pid, kill),
	ok.

cleanup_each_dump_messages() ->
	receive
		Msg ->
			?debugFmt("Unexpected message in queue: ~p", [Msg]),
			cleanup_each_dump_messages()
	after 0 ->
		ok
	end.

%% This function is called after all tests.
%% It unloads the mocked modules.
cleanup_all(MockedModules) ->
	meck:unload(MockedModules).

ar_mining_hash__compute_h0(WorkerPid, Candidate) ->
	?TESTER_REGISTER_NAME ! ?compute_h0(WorkerPid, Candidate).

ar_mining_hash__compute_h1(WorkerPid, Candidate) ->
	?TESTER_REGISTER_NAME ! ?compute_h1(WorkerPid, Candidate).

ar_mining_hash__compute_h2(WorkerPid, Candidate) ->
	?TESTER_REGISTER_NAME ! ?compute_h2(WorkerPid, Candidate).

ar_block__get_recall_range(H0, Partition, PartitionUpperBound) ->
	?TESTER_REGISTER_NAME ! ?get_recall_range(H0, Partition, PartitionUpperBound),
	receive
		?get_recall_range_response(RecallRange1, RecallRange2) -> {RecallRange1, RecallRange2}
	after 1000 ->
		exit(no_get_recall_range_response_received)
	end.

ar_mining_io__is_recall_range_readable(Candidate, RecallRange) ->
	?TESTER_REGISTER_NAME ! ?is_recall_range_readable(Candidate, RecallRange),
	receive
		?is_recall_range_readable_response(IsReadable) -> IsReadable
	after 1000 ->
		exit(no_is_recall_range_readable_response_received)
	end.

ar_mining_io__read_recall_range(Kind, WorkerPid, Candidate, RecallRangeStart) ->
	?TESTER_REGISTER_NAME ! ?read_recall_range(Kind, WorkerPid, Candidate, RecallRangeStart),
	receive
		?read_recall_range_response() -> ok
	after 1000 ->
		exit(no_read_recall_range_response_received)
	end.

ar_node_utils__passes_diff_check(SolutionHash, IsPoA1, DiffPair, PackingDifficulty) ->
	?TESTER_REGISTER_NAME ! ?passes_diff_check(SolutionHash, IsPoA1, DiffPair, PackingDifficulty),
	receive
		?passes_diff_check_response(IsPassed) -> IsPassed
	after 1000 ->
		exit(no_passes_diff_check_response_received)
	end.

%% ar_mining_server:prepare_and_post_solution(Candidate)
ar_mining_server__prepare_and_post_solution(Candidate) ->
	?TESTER_REGISTER_NAME ! ?prepare_and_post_solution(Candidate).

%% ------------------------------------------------------------------------------------------------
%% Helpers
%% ------------------------------------------------------------------------------------------------

handle_compute_h0(StepNumber, H0, State) ->
	?debugFmt("Handling compute_h0 for step ~p (H0 ~p)", [StepNumber, H0]),
	receive
		?compute_h0(Pid, #mining_candidate{step_number = StepNumber} = Candidate) when Pid == State#state.worker_pid ->
			ar_mining_worker:computed_hash(State#state.worker_pid, computed_h0, H0, undefined, Candidate)
	after 1000 ->
		exit(no_compute_h0_message_received)
	end.

handle_compute_h1s(H1s, State) ->
	handle_compute_h1s(H1s, [], State).
%% The order of calculations might be different, but it does not really matter:
%% the "subchunks" we used to represent the recall range are all zeroes and
%% therefore are all equal.
%% In the test we only rely on the fake hash value to mark the hash valid or not,
%% we do not rely on the order in which these hashes will arrive later for
%% difficulty checks.
%% The same stands for H2s below.
handle_compute_h1s([], Acc, State) -> handle_send_computed_h1s(lists:reverse(Acc), State);
handle_compute_h1s([H1 | H1s], Acc, State) ->
	?debugFmt("Handling compute_h1 for H1 ~p", [H1]),
	Acc1 = receive
		?compute_h1(Pid, Candidate) when Pid == State#state.worker_pid -> [{H1, Candidate} | Acc]
	after 1000 ->
		exit(no_compute_h1_message_received)
	end,
	handle_compute_h1s(H1s, Acc1, State).

handle_send_computed_h1s([], _State) -> ok;
handle_send_computed_h1s([{H1, Candidate} | Acc], State) ->
	ar_mining_worker:computed_hash(State#state.worker_pid, computed_h1, H1, <<"Preimage1">>, Candidate),
	handle_send_computed_h1s(Acc, State).

handle_compute_h2s(H2s, State) ->
	handle_compute_h2s(H2s, [], State).
handle_compute_h2s([], Acc, State) -> handle_send_computed_h2s(lists:reverse(Acc), State);
handle_compute_h2s([H2 | H2s], Acc, State) ->
	?debugFmt("Handling compute_h2 for H2 ~p", [H2]),
	Acc1 = receive
		?compute_h2(Pid, Candidate) when Pid == State#state.worker_pid -> [{H2, Candidate} | Acc]
	after 1000 ->
		exit(no_compute_h2_message_received)
	end,
	handle_compute_h2s(H2s, Acc1, State).

handle_send_computed_h2s([], _State) -> ok;
handle_send_computed_h2s([{H2, Candidate} | Acc], State) ->
	ar_mining_worker:computed_hash(State#state.worker_pid, computed_h2, H2, <<"Preimage2">>, Candidate),
	handle_send_computed_h2s(Acc, State).

handle_get_recall_range(H0, RecallRange1, RecallRange2, State) ->
	?debugFmt("Handling get_recall_range for H0 ~p (~p ~p)", [H0, RecallRange1, RecallRange2]),
	receive
		?get_recall_range(H0, _Partition1, _PartitionUpperBound) ->
			State#state.worker_pid ! ?get_recall_range_response(RecallRange1, RecallRange2)
	after 1000 ->
		exit(no_recall_range_message_received)
	end.

handle_is_recall_range_readable(RecallRangeStart, IsReadable, State) ->
	?debugFmt("Handling is_recall_range_readable for RecallRangeStart ~p (IsReadable ~p)", [RecallRangeStart, IsReadable]),
	receive
		?is_recall_range_readable(_Candidate, RecallRangeStart) ->
			State#state.worker_pid ! ?is_recall_range_readable_response(IsReadable)
	after 1000 ->
		exit(no_is_recall_range_readable_message_received)
	end.

handle_read_recall_range(Kind, RecallRangeStart, State) ->
	?debugFmt("Handling read_recall_range for Kind ~p (RecallRangeStart ~p)", [Kind, RecallRangeStart]),
	receive
		?read_recall_range(Kind, _WorkerPid, Candidate, RecallRangeStart) ->
			State#state.worker_pid ! ?read_recall_range_response(),
			Candidate
	after 1000 ->
		exit(no_read_recall_range_message_received)
	end.

handle_passes_diff_checks(HashesMap, _State) when map_size(HashesMap) == 0 -> ok;
handle_passes_diff_checks(HashesMap, State) ->
	HashesMap1 = receive
		?passes_diff_check(Hash, _IsPoA1, _DiffPair, _PackingDifficulty) ->
			{IsPassed, HashesMap_} = maps:take(Hash, HashesMap),
			?debugFmt("Checking difficulty for ~p (IsPassed ~p)", [Hash, IsPassed]),
			State#state.worker_pid ! ?passes_diff_check_response(IsPassed),
			HashesMap_
	after 1000 ->
		exit(no_passes_diff_check_message_received)
	end,
	handle_passes_diff_checks(HashesMap1, State).

handle_prepare_and_post_solution_h1(H1) ->
	?debugFmt("Handling prepare_and_post_solution for H1 ~p", [H1]),
	receive
		?prepare_and_post_solution(#mining_candidate{h1 = H1}) -> ok
	after 1000 ->
		exit(no_prepare_and_post_solution_message_received)
	end.

handle_prepare_and_post_solution_h2(H2) ->
	?debugFmt("Handling prepare_and_post_solution for H2 ~p", [H2]),
	receive
		?prepare_and_post_solution(#mining_candidate{h2 = H2}) -> ok
	after 1000 ->
		exit(no_prepare_and_post_solution_message_received)
	end.

assert_no_messages() ->
	?debugMsg("Asserting no significant messages left"),
	receive
		Msg ->
			?debugFmt("Unexpected message received: ~p", [Msg]),
			error({unexpected_message_received, Msg})
	after 1000 ->
		ok
	end.

generate_recall_range(RecallRangeStart, Difficulty) ->
	RecallRangeSize = ar_block:get_recall_range_size(Difficulty),
	[{RecallRangeStart + RecallRangeSize, <<0:RecallRangeSize/unit:8>>}].

generate_hashes_for_recall_range(Prefix, Difficulty) ->
  [<<Prefix/binary, (integer_to_binary(N))/binary>> || N <- lists:seq(1, ar_block:get_nonces_per_recall_range(Difficulty))].

%% ------------------------------------------------------------------------------------------------
%% Tests
%% ------------------------------------------------------------------------------------------------

mining_worker_test_() ->
	{setup, fun setup_all/0, fun cleanup_all/1,
		[
			?with_setup_each(fun test_no_available_ranges/1),
			?with_setup_each(fun test_only_second_range_available/1),
			?with_setup_each(fun test_only_first_range_available_no_solutions/1),
			?with_setup_each(fun test_both_ranges_available_no_solutions/1),
			?with_setup_each(fun test_both_ranges_available_in_reverse_order_no_solutions/1),
			?with_setup_each(fun test_both_ranges_available_h1_solution/1),
			?with_setup_each(fun test_both_ranges_available_h2_solution/1)
		]
	}.

%% This test checks the worker behavior when it does not have any available
%% recall ranges for a VDF step.
test_no_available_ranges(State) ->
	H0 = <<"H0">>,
	RecallRange1Start = 100,
	RecallRange2Start = 200,
	Candidate1 = State#state.candidate,
	%% 1. Add compute_h0 task
	?debugMsg("Adding compute_h0 task"),
	ar_mining_worker:add_task(State#state.worker_pid, compute_h0, Candidate1),
	%% 2. Worker asks to compute H0
	handle_compute_h0(Candidate1#mining_candidate.step_number, H0, State),
	%% 3. Worker asks for recall ranges for the given H0
	handle_get_recall_range(H0, RecallRange1Start, RecallRange2Start, State),
	%% 4. Worker checks if recall ranges are readable
	handle_is_recall_range_readable(RecallRange1Start, false, State),
	handle_is_recall_range_readable(RecallRange2Start, false, State),
	%% 5. No more messages expected, recall ranges are not readable
	assert_no_messages().

%% This test checks the worker behavior when it has only second recall range
%% available for a VDF step.
test_only_second_range_available(State) ->
	H0 = <<"H0">>,
	RecallRange1Start = 100,
	RecallRange2Start = 200,
	Candidate1 = State#state.candidate,
	%% 1. Add compute_h0 task
	?debugMsg("Adding compute_h0 task"),
	ar_mining_worker:add_task(State#state.worker_pid, compute_h0, Candidate1),
	%% 2. Worker asks to compute H0
	handle_compute_h0(Candidate1#mining_candidate.step_number, H0, State),
	%% 3. Worker asks for recall ranges for the given H0
	handle_get_recall_range(H0, RecallRange1Start, RecallRange2Start, State),
	%% 4. Worker checks if recall ranges are readable
	handle_is_recall_range_readable(RecallRange1Start, false, State),
	handle_is_recall_range_readable(RecallRange2Start, true, State),
	%% 5. No more messages expected, only second recall range is readable
	assert_no_messages().

%% This test checks the worker behavior when it has only first recall range
%% available for a VDF step, but no valid solutions produced.
test_only_first_range_available_no_solutions(State) ->
	H0 = <<"H0">>,
	H1Prefix = <<"H1-">>,
	RecallRange1Start = 100,
	RecallRange2Start = 200,
	Candidate1 = State#state.candidate,
	%% 1. Add compute_h0 task
	?debugMsg("Adding compute_h0 task"),
	ar_mining_worker:add_task(State#state.worker_pid, compute_h0, Candidate1),
	%% 2. Worker asks to compute H0
	handle_compute_h0(Candidate1#mining_candidate.step_number, H0, State),
	%% 3. Worker asks for recall ranges for the given H0
	handle_get_recall_range(H0, RecallRange1Start, RecallRange2Start, State),
	%% 4. Worker checks if recall ranges are readable
	handle_is_recall_range_readable(RecallRange1Start, true, State),
	handle_is_recall_range_readable(RecallRange2Start, false, State),
	%% 5. Worker asks to read only first recall range
	Candidate2 = handle_read_recall_range(chunk1, RecallRange1Start, State),
	%% 6. Providing the first recall range
	RecallRange1 = generate_recall_range(RecallRange1Start, Candidate2#mining_candidate.packing_difficulty),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk1, Candidate2, RecallRange1Start, RecallRange1),
	%% 7. Worker asks to compute H1s for the first recall range.
	%% The number of H1s is equal to the number of nonces in the recall range.
	H1s = generate_hashes_for_recall_range(H1Prefix, Candidate2#mining_candidate.packing_difficulty),
	handle_compute_h1s(H1s, State),
	%% 8. Worker checks if H1s pass the diff check, rejecting all of them
	Hashes1Map = maps:from_list([{H1, false} || H1 <- H1s]),
	handle_passes_diff_checks(Hashes1Map, State),
	%% 9. No more messages expected
	assert_no_messages().

%% This test checks the worker behavior when it has both recall ranges
%% available for a VDF step, but no valid solutions produced.
test_both_ranges_available_no_solutions(State) ->
	H0 = <<"H0">>,
	H1Prefix = <<"H1-">>,
	H2Prefix = <<"H2-">>,
	RecallRange1Start = 100,
	RecallRange2Start = 200,
	Candidate1 = State#state.candidate,
	%% 1. Add compute_h0 task
	?debugMsg("Adding compute_h0 task"),
	ar_mining_worker:add_task(State#state.worker_pid, compute_h0, Candidate1),
	%% 2. Worker asks to compute H0
	handle_compute_h0(Candidate1#mining_candidate.step_number, H0, State),
	%% 3. Worker asks for recall ranges for the given H0
	handle_get_recall_range(H0, RecallRange1Start, RecallRange2Start, State),
	%% 4. Worker checks if recall ranges are readable
	handle_is_recall_range_readable(RecallRange1Start, true, State),
	handle_is_recall_range_readable(RecallRange2Start, true, State),
	%% 5. Worker asks to read both recall ranges
	Candidate2 = handle_read_recall_range(chunk1, RecallRange1Start, State),
	Candidate3 = handle_read_recall_range(chunk2, RecallRange2Start, State),
	%% 6. Providing the recall ranges
	RecallRange1 = generate_recall_range(RecallRange1Start, Candidate3#mining_candidate.packing_difficulty),
	RecallRange2 = generate_recall_range(RecallRange2Start, Candidate3#mining_candidate.packing_difficulty),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk1, Candidate2, RecallRange1Start, RecallRange1),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk2, Candidate3, RecallRange2Start, RecallRange2),
	%% 7. Worker asks to compute H1s for the recall ranges.
	%% The number of H1s is equal to the number of nonces in the recall range.
	H1s = generate_hashes_for_recall_range(H1Prefix, Candidate3#mining_candidate.packing_difficulty),
	handle_compute_h1s(H1s, State),
	%% 8. Worker checks if H1s pass the diff check, rejecting all of them
	Hashes1Map = maps:from_list([{H1, false} || H1 <- H1s]),
	handle_passes_diff_checks(Hashes1Map, State),
	%% 9. Worker asks to compute H2s for the recall ranges.
	%% The number of H2s is equal to the number of nonces in the recall range.
	H2s = generate_hashes_for_recall_range(H2Prefix, Candidate3#mining_candidate.packing_difficulty),
	handle_compute_h2s(H2s, State),
	%% 10. Worker checks if H2s pass the diff check, rejecting all of them
	Hashes2Map = maps:from_list([{H2, false} || H2 <- H2s]),
	handle_passes_diff_checks(Hashes2Map, State),
	%% 11. No more messages expected
	assert_no_messages().

%% This test checks the worker behavior when it has both recall ranges
%% available for a VDF step, but no valid solutions produced.
%% In this test the second recall range is available first.
test_both_ranges_available_in_reverse_order_no_solutions(State) ->
	H0 = <<"H0">>,
	H1Prefix = <<"H1-">>,
	H2Prefix = <<"H2-">>,
	RecallRange1Start = 100,
	RecallRange2Start = 200,
	Candidate1 = State#state.candidate,
	%% 1. Add compute_h0 task
	?debugMsg("Adding compute_h0 task"),
	ar_mining_worker:add_task(State#state.worker_pid, compute_h0, Candidate1),
	%% 2. Worker asks to compute H0
	handle_compute_h0(Candidate1#mining_candidate.step_number, H0, State),
	%% 3. Worker asks for recall ranges for the given H0
	handle_get_recall_range(H0, RecallRange1Start, RecallRange2Start, State),
	%% 4. Worker checks if recall ranges are readable
	handle_is_recall_range_readable(RecallRange1Start, true, State),
	handle_is_recall_range_readable(RecallRange2Start, true, State),
	%% 5. Worker asks to read both recall ranges
	Candidate2 = handle_read_recall_range(chunk1, RecallRange1Start, State),
	Candidate3 = handle_read_recall_range(chunk2, RecallRange2Start, State),
	%% 6. Providing the recall ranges
	RecallRange1 = generate_recall_range(RecallRange1Start, Candidate3#mining_candidate.packing_difficulty),
	RecallRange2 = generate_recall_range(RecallRange2Start, Candidate3#mining_candidate.packing_difficulty),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk2, Candidate3, RecallRange2Start, RecallRange2),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk1, Candidate2, RecallRange1Start, RecallRange1),
	%% 7. Worker asks to compute H1s for the recall ranges.
	%% The number of H1s is equal to the number of nonces in the recall range.
	H1s = generate_hashes_for_recall_range(H1Prefix, Candidate3#mining_candidate.packing_difficulty),
	handle_compute_h1s(H1s, State),
	%% 8. Worker checks if H1s pass the diff check, rejecting all of them
	Hashes1Map = maps:from_list([{H1, false} || H1 <- H1s]),
	handle_passes_diff_checks(Hashes1Map, State),
	%% 9. Worker asks to compute H2s for the recall ranges.
	%% The number of H2s is equal to the number of nonces in the recall range.
	H2s = generate_hashes_for_recall_range(H2Prefix, Candidate3#mining_candidate.packing_difficulty),
	handle_compute_h2s(H2s, State),
	%% 10. Worker checks if H2s pass the diff check, rejecting all of them
	Hashes2Map = maps:from_list([{H2, false} || H2 <- H2s]),
	handle_passes_diff_checks(Hashes2Map, State),
	%% 11. No more messages expected
	assert_no_messages().

%% This test checks the worker behavior when it has both recall ranges
%% available for a VDF step, only one valid H1 solution is produced.
test_both_ranges_available_h1_solution(State) ->
	H0 = <<"H0">>,
	H1Prefix = <<"H1-">>,
	H2Prefix = <<"H2-">>,
	RecallRange1Start = 100,
	RecallRange2Start = 200,
	Candidate1 = State#state.candidate,
	%% 1. Add compute_h0 task
	?debugMsg("Adding compute_h0 task"),
	ar_mining_worker:add_task(State#state.worker_pid, compute_h0, Candidate1),
	%% 2. Worker asks to compute H0
	handle_compute_h0(Candidate1#mining_candidate.step_number, H0, State),
	%% 3. Worker asks for recall ranges for the given H0
	handle_get_recall_range(H0, RecallRange1Start, RecallRange2Start, State),
	%% 4. Worker checks if recall ranges are readable
	handle_is_recall_range_readable(RecallRange1Start, true, State),
	handle_is_recall_range_readable(RecallRange2Start, true, State),
	%% 5. Worker asks to read both recall ranges
	Candidate2 = handle_read_recall_range(chunk1, RecallRange1Start, State),
	Candidate3 = handle_read_recall_range(chunk2, RecallRange2Start, State),
	%% 6. Providing the recall ranges
	RecallRange1 = generate_recall_range(RecallRange1Start, Candidate3#mining_candidate.packing_difficulty),
	RecallRange2 = generate_recall_range(RecallRange2Start, Candidate3#mining_candidate.packing_difficulty),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk1, Candidate2, RecallRange1Start, RecallRange1),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk2, Candidate3, RecallRange2Start, RecallRange2),
	%% 7. Worker asks to compute H1s for the recall ranges.
	%% The number of H1s is equal to the number of nonces in the recall range.
	[ValidH1 | RestH1s] = H1s = generate_hashes_for_recall_range(H1Prefix, Candidate3#mining_candidate.packing_difficulty),
	handle_compute_h1s(H1s, State),
	%% 8. Worker checks if H1s pass the diff check, rejecting all of them but the very first one
	handle_passes_diff_checks(#{ValidH1 => true}, State),
	%% 8.1. Worker posts the valid H1 solution
	handle_prepare_and_post_solution_h1(ValidH1),
	Passes1Map = maps:from_list([{H1, false} || H1 <- RestH1s]),
	handle_passes_diff_checks(Passes1Map, State),
	%% 9. Worker asks to compute H2s for the recall ranges.
	%% The number of H2s is equal to the number of nonces in the recall range.
	%% The very first H2 will never be called to be computed, because the
	%% corresponding H1 passes the difficulty check; therefore, we expect one less
	%% H2 to be computed.
	[_ | RestH2s] = _H2s = generate_hashes_for_recall_range(H2Prefix, Candidate3#mining_candidate.packing_difficulty),
	handle_compute_h2s(RestH2s, State),
	%% 10. Worker checks if H2s pass the diff check, rejecting all of them
	Hashes2Map = maps:from_list([{H2, false} || H2 <- RestH2s]),
	handle_passes_diff_checks(Hashes2Map, State),
	%% 11. No more messages expected
	assert_no_messages().

%% This test checks the worker behavior when it has both recall ranges
%% available for a VDF step, only one valid H2 solution is produced.
test_both_ranges_available_h2_solution(State) ->
	H0 = <<"H0">>,
	H1Prefix = <<"H1-">>,
	H2Prefix = <<"H2-">>,
	RecallRange1Start = 100,
	RecallRange2Start = 200,
	Candidate1 = State#state.candidate,
	%% 1. Add compute_h0 task
	?debugMsg("Adding compute_h0 task"),
	ar_mining_worker:add_task(State#state.worker_pid, compute_h0, Candidate1),
	%% 2. Worker asks to compute H0
	handle_compute_h0(Candidate1#mining_candidate.step_number, H0, State),
	%% 3. Worker asks for recall ranges for the given H0
	handle_get_recall_range(H0, RecallRange1Start, RecallRange2Start, State),
	%% 4. Worker checks if recall ranges are readable
	handle_is_recall_range_readable(RecallRange1Start, true, State),
	handle_is_recall_range_readable(RecallRange2Start, true, State),
	%% 5. Worker asks to read both recall ranges
	Candidate2 = handle_read_recall_range(chunk1, RecallRange1Start, State),
	Candidate3 = handle_read_recall_range(chunk2, RecallRange2Start, State),
	%% 6. Providing the recall ranges
	RecallRange1 = generate_recall_range(RecallRange1Start, Candidate3#mining_candidate.packing_difficulty),
	RecallRange2 = generate_recall_range(RecallRange2Start, Candidate3#mining_candidate.packing_difficulty),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk1, Candidate2, RecallRange1Start, RecallRange1),
	ar_mining_worker:chunks_read(State#state.worker_pid, chunk2, Candidate3, RecallRange2Start, RecallRange2),
	%% 7. Worker asks to compute H1s for the recall ranges.
	%% The number of H1s is equal to the number of nonces in the recall range.
	H1s = generate_hashes_for_recall_range(H1Prefix, Candidate3#mining_candidate.packing_difficulty),
	handle_compute_h1s(H1s, State),
	%% 8. Worker checks if H1s pass the diff check, rejecting all of them but the very first one
	Passes1Map = maps:from_list([{H1, false} || H1 <- H1s]),
	handle_passes_diff_checks(Passes1Map, State),
	%% 9. Worker asks to compute H2s for the recall ranges.
	%% The number of H2s is equal to the number of nonces in the recall range.
	[ValidH2 | RestH2s] = H2s = generate_hashes_for_recall_range(H2Prefix, Candidate3#mining_candidate.packing_difficulty),
	handle_compute_h2s(H2s, State),
	%% 9.1. Worker checks if H2s pass the diff check, rejecting all of them but the very first one
	handle_passes_diff_checks(#{ValidH2 => true}, State),
	%% 10. Worker posts the valid H2 solution
	handle_prepare_and_post_solution_h2(ValidH2),
	%% 11. Worker checks if H2s pass the diff check, rejecting all of them
	Hashes2Map = maps:from_list([{H2, false} || H2 <- RestH2s]),
	handle_passes_diff_checks(Hashes2Map, State),
	%% 12. No more messages expected
	assert_no_messages().
