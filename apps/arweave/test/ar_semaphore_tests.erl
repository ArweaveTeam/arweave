-module(ar_semaphore_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

one_wait_per_process_test_() ->
	with_semaphore_(one_wait_per_process_sem, 4, fun() ->
		?assertEqual(ok, ar_semaphore:acquire(one_wait_per_process_sem, ?DEFAULT_CALL_TIMEOUT)),
		?assertEqual({error, process_already_waiting}, ar_semaphore:acquire(one_wait_per_process_sem, ?DEFAULT_CALL_TIMEOUT))
	end).

wait_for_one_process_at_a_time_test_() ->
	with_semaphore_(wait_for_one_process_at_a_time_sem, 1, fun() ->
		TestPid = self(),
		SleepMs = 500,
		NoMessageMs = 250,
		DoneTimeoutMs = 3000,
		spawn_worker(wait_for_one_process_at_a_time_sem, SleepMs, TestPid, p1),
		spawn_worker(wait_for_one_process_at_a_time_sem, SleepMs, TestPid, p2),
		spawn_worker(wait_for_one_process_at_a_time_sem, SleepMs, TestPid, p3),
		?assert(receive _ -> false after NoMessageMs -> true end),
		Done1 = receive_done(DoneTimeoutMs),
		?assert(receive _ -> false after NoMessageMs -> true end),
		Done2 = receive_done(DoneTimeoutMs),
		?assertNotEqual(Done1, Done2),
		?assert(receive _ -> false after NoMessageMs -> true end),
		Done3 = receive_done(DoneTimeoutMs),
		?assertNotEqual(Done1, Done3),
		?assertNotEqual(Done2, Done3)
	end).

wait_for_two_processes_at_a_time_test_() ->
	with_semaphore_(wait_for_two_processes_at_a_time_sem, 2, fun() ->
		TestPid = self(),
		SleepMs = 400,
		DoneTimeoutMs = 3000,
		spawn_worker(wait_for_two_processes_at_a_time_sem, SleepMs, TestPid, p1),
		spawn_worker(wait_for_two_processes_at_a_time_sem, SleepMs, TestPid, p2),
		spawn_worker(wait_for_two_processes_at_a_time_sem, SleepMs, TestPid, p3),
		spawn_worker(wait_for_two_processes_at_a_time_sem, SleepMs, TestPid, p4),
		Done1 = receive_done(DoneTimeoutMs),
		Done2 = receive_done(DoneTimeoutMs),
		?assertNotEqual(Done1, Done2),
		?assert(receive _ -> false after 250 -> true end),
		Done3 = receive_done(DoneTimeoutMs),
		?assertNotEqual(Done1, Done3),
		?assertNotEqual(Done2, Done3),
		Done4 = receive_done(DoneTimeoutMs),
		?assertNotEqual(Done1, Done4),
		?assertNotEqual(Done2, Done4),
		?assertNotEqual(Done3, Done4)
	end).

spawn_worker(SemaphoreName, SleepMs, TestPid, WorkerID) ->
	spawn_link(fun() ->
		ok = ar_semaphore:acquire(SemaphoreName, ?DEFAULT_CALL_TIMEOUT),
		timer:sleep(SleepMs),
		TestPid ! {done, WorkerID}
	end).

receive_done(TimeoutMs) ->
	receive
		{done, WorkerID} ->
			WorkerID
	after TimeoutMs ->
		?assert(false)
	end.

with_semaphore_(Name, Value, Fun) ->
	{setup,
		fun() -> {ok, _} = ar_semaphore:start_link(Name, Value) end,
		fun(_) -> _ = ar_semaphore:stop(Name) end,
		[Fun]
	}.
