-module(ar_vdf_mining_session_tests).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_consensus.hrl").
-include("ar_mining.hrl").
-include("ar_vdf.hrl").

%% we have to wait to let the ar_events get processed whenever we apply a VDF step
-define(WAIT_TIME, 1000).

%% -------------------------------------------------------------------------------------------------
%% Test Registration
%% -------------------------------------------------------------------------------------------------

mining_session_test_() ->
	{foreach,
		fun ar_vdf_test_utils:setup_external_update/0,
		fun ar_vdf_test_utils:cleanup_external_update/1,
		[
			ar_test_node:test_with_mocked_functions([ar_vdf_test_utils:mock_add_task(), ar_vdf_test_utils:mock_reset_frequency()],
				fun test_mining_session/0, ?TEST_SUITE_TIMEOUT)
		]
	}.

%% -------------------------------------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------------------------------------

test_mining_session() ->
	SessionKey0 = ar_vdf_test_utils:get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	SessionKey3 = {<<"session3">>, 3, 1},
	ar_test_node:mine(),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey0, [], 2, true, undefined),
		"Partial session0, should mine"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([SessionKey0], sets:to_list(ar_mining_server:active_sessions())),
	?assertEqual([2], ar_vdf_test_utils:mined_steps()),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey0, [3], 4, false, undefined),
		"Full session0, should mine"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([SessionKey0], sets:to_list(ar_mining_server:active_sessions())),
	?assertEqual([4, 3], ar_vdf_test_utils:mined_steps()),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 4 },
		ar_vdf_test_utils:apply_external_update(SessionKey0, [], 4, true, undefined),
		"Repeat step, should not mine"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([SessionKey0], sets:to_list(ar_mining_server:active_sessions())),
	?assertEqual([], ar_vdf_test_utils:mined_steps()),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 6, true, SessionKey0),
		"Partial session1, should not mine"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([SessionKey0], sets:to_list(ar_mining_server:active_sessions())),
	?assertEqual([], ar_vdf_test_utils:mined_steps()),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [5], 6, false, SessionKey0),
		"Full session1, should mine"),
	timer:sleep(?WAIT_TIME),
	ar_vdf_test_utils:assert_sessions_equal([SessionKey0, SessionKey1], ar_mining_server:active_sessions()),
	?assertEqual([6, 5], ar_vdf_test_utils:mined_steps()),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey3, [], 16, true, SessionKey2),
		"Partial session3, should not mine"),
	timer:sleep(?WAIT_TIME),
	ar_vdf_test_utils:assert_sessions_equal([SessionKey0, SessionKey1], ar_mining_server:active_sessions()),
	?assertEqual([], ar_vdf_test_utils:mined_steps()),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey3, [15], 16, true, SessionKey2),
		"Full session3, should not mine"),
	timer:sleep(?WAIT_TIME),
	ar_vdf_test_utils:assert_sessions_equal([SessionKey0, SessionKey1], ar_mining_server:active_sessions()),
	?assertEqual([], ar_vdf_test_utils:mined_steps()),
	%% Current session is only updated when applying a new tip block, not when applying a VDF step
	%% from a VDF server.
	?assertEqual(SessionKey0, ar_vdf_test_utils:get_current_session_key()). 