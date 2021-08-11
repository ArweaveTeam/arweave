-module(ar_test_events).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-export([
	subscribe_send_cancel/1,
	process_terminated/1
]).

subscribe_send_cancel(Config) ->
	%% Check whether all the "event"-processes are alive.
	%% This list should be aligned with the total number
	%% of running gen_server's by ar_events_sup.
	Processes = [tx, block, testing],
	true = lists:all(fun(P) -> whereis(ar_events:event_to_process(P)) /= undefined end, Processes),
	EventNetworkStateOnStart = sys:get_state(ar_events:event_to_process(testing)),
	ok = ar_events:subscribe(testing),
	already_subscribed = ar_events:subscribe(testing),
	[ok, already_subscribed] = ar_events:subscribe([tx, testing]),

	%% Sender shouldn't receive its own event.
	ok = ar_events:send(testing, 12345),
	receive
		{event, testing, 12345} ->
			ct:fail("Received an unexpected event.")
	after 200 ->
		ok
	end,
	%% Sender should receive an event triggered by another process.
	spawn(fun() -> ar_events:send(testing, 12345) end),
	receive
		{event, testing, 12345} ->
			ok
	after 200 ->
		ct:fail("Did not receive an expected event within 200 milliseconds.")
	end,
	ok = ar_events:cancel(testing),
	EventNetworkStateOnStart = sys:get_state(ar_events:event_to_process(testing)),
	Config.

process_terminated(Config) ->
	%% If a subscriber has been terminated without implicit "cancel" call
	%% it should be cleaned up from the subscription list.
	EventNetworkStateOnStart = sys:get_state(ar_events:event_to_process(testing)),
	spawn(fun() -> ar_events:subscribe(testing) end),
	timer:sleep(200),
	EventNetworkStateOnStart = sys:get_state(ar_events:event_to_process(testing)),
	Config.
