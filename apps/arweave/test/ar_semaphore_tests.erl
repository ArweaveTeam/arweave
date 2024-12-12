-module(ar_semaphore_tests).

-include_lib("eunit/include/eunit.hrl").

one_wait_per_process_test_() ->
	with_semaphore_(one_wait_per_process_sem, 4, fun() ->
		?assertEqual(ok, ar_semaphore:acquire(one_wait_per_process_sem, infinity)),
		?assertEqual({error, process_already_waiting}, ar_semaphore:acquire(one_wait_per_process_sem, infinity))
	end).

wait_for_one_process_at_a_time_test_() ->
	with_semaphore_(wait_for_one_process_at_a_time_sem, 1, fun() ->
		TestPid = self(),
		spawn_link(fun() ->
			ok = ar_semaphore:acquire(wait_for_one_process_at_a_time_sem, infinity),
			timer:sleep(200),
			TestPid ! p1_done
		end),
		spawn_link(fun() ->
			ok = ar_semaphore:acquire(wait_for_one_process_at_a_time_sem, infinity),
			timer:sleep(200),
			TestPid ! p2_done
		end),
		spawn_link(fun() ->
			ok = ar_semaphore:acquire(wait_for_one_process_at_a_time_sem, infinity),
			timer:sleep(200),
			TestPid ! p3_done
		end),
		?assert(receive _ -> false after 190 -> true end),
		?assert(receive p1_done -> true after 20 -> false end),
		?assert(receive _ -> false after 180 -> true end),
		?assert(receive p2_done -> true after 30 -> false end),
		?assert(receive _ -> false after 170 -> true end),
		?assert(receive p3_done -> true after 40 -> false end)
	end).

wait_for_two_processes_at_a_time_test_() ->
	with_semaphore_(wait_for_two_processes_at_a_time_sem, 2, fun() ->
		TestPid = self(),
		spawn_link(fun() ->
			ok = ar_semaphore:acquire(wait_for_two_processes_at_a_time_sem, infinity),
			timer:sleep(400),
			TestPid ! p1_done
		end),
		spawn_link(fun() ->
			ok = ar_semaphore:acquire(wait_for_two_processes_at_a_time_sem, infinity),
			timer:sleep(400),
			TestPid ! p2_done
		end),
		spawn_link(fun() ->
			ok = ar_semaphore:acquire(wait_for_two_processes_at_a_time_sem, infinity),
			timer:sleep(400),
			TestPid ! p3_done
		end),
		spawn_link(fun() ->
			ok = ar_semaphore:acquire(wait_for_two_processes_at_a_time_sem, infinity),
			timer:sleep(400),
			TestPid ! p4_done
		end),
		?assert(receive _ -> false after 360 -> true end),
		?assert(receive p1_done -> true after 40 -> false end),
		?assert(receive p2_done -> true after 40 -> false end),
		?assert(receive _ -> false after 340 -> true end),
		?assert(receive p3_done -> true after 60 -> false end),
		?assert(receive p4_done -> true after 60 -> false end)
	end).

with_semaphore_(Name, Value, Fun) ->
	{setup,
		fun() -> ok = ar_semaphore:start_link(Name, Value) end,
		fun(_) -> _ = ar_semaphore:stop(Name) end,
		[Fun]
	}.
