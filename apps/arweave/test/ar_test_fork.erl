-module(ar_test_fork).

-export([test_on_fork/3]).

-import(ar_test_node, [slave_call/3]).

test_on_fork(ForkHeightFun, ForkHeight, TestFun) ->
	{
		foreach,
		fun() ->
			meck:new(ar_fork, [passthrough]),
			meck:expect(ar_fork, ForkHeightFun, fun() -> ForkHeight end),
			%% Do the same on slave.
			slave_call(meck, new, [ar_fork, [no_link, passthrough]]),
			slave_call(meck, expect, [ar_fork, ForkHeightFun, fun() -> ForkHeight end]),
			ok
		end,
		fun(ok) ->
			meck:unload(ar_fork),
			slave_call(meck, unload, [ar_fork])
		end,
		[
			{timeout, 240, TestFun}
		]
	}.
