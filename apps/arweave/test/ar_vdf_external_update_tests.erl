-module(ar_vdf_external_update_tests).

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

external_update_test_() ->
	{foreach,
		fun ar_vdf_test_utils:setup_external_update/0,
		fun ar_vdf_test_utils:cleanup_external_update/1,
		[
			ar_test_node:test_with_mocked_functions([ar_vdf_test_utils:mock_add_task(), ar_vdf_test_utils:mock_reset_frequency()],
				fun test_session_overlap/0, ?TEST_SUITE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([ar_vdf_test_utils:mock_add_task(), ar_vdf_test_utils:mock_reset_frequency()],
				fun test_client_ahead/0, ?TEST_SUITE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([ar_vdf_test_utils:mock_add_task(), ar_vdf_test_utils:mock_reset_frequency()],
				fun test_skip_ahead/0, ?TEST_SUITE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([ar_vdf_test_utils:mock_add_task(), ar_vdf_test_utils:mock_reset_frequency()],
				fun test_2_servers_switching/0, ?TEST_SUITE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([ar_vdf_test_utils:mock_add_task(), ar_vdf_test_utils:mock_reset_frequency()],
				fun test_backtrack/0, ?TEST_SUITE_TIMEOUT),
			ar_test_node:test_with_mocked_functions([ar_vdf_test_utils:mock_add_task(), ar_vdf_test_utils:mock_reset_frequency()],
				fun test_2_servers_backtrack/0, ?TEST_SUITE_TIMEOUT)
		]
	}.

%% -------------------------------------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------------------------------------

%% @doc The VDF session key is only updated when a block is procesed by the VDF server. Until that
%% happens the serve will push all VDF steps under the same session key - even if those steps
%% cross an entropy reset line. When a block comes in the server will update the session key
%% *and* move all appropriate steps to that session. Prior to 2.7 this caused VDF clients to
%% process some steps twice - once under the old session key, and once under the new session key.
%% This test asserts that this behavior has been fixed and that VDF clients only process each
%% step once.
test_session_overlap() ->
	SessionKey0 = ar_vdf_test_utils:get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 8, true, SessionKey0),
		"Partial session1, session not found"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 9, true, SessionKey0),
		"Partial session1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 10, true, SessionKey0),
		"Partial session1, interval2"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 11, true, SessionKey0),
		"Partial session1, interval2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey2, [], 12, true, SessionKey1),
		"Partial session2, interval2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = true, step_number = 11 },
		ar_vdf_test_utils:apply_external_update(SessionKey1, [8, 7, 6, 5], 9, false, SessionKey1),
		"Full session1, all steps already seen"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey2, [11, 10], 12, false, SessionKey1),
		"Full session2, some steps already seen"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(
		[<<"8">>, <<"7">>, <<"6">>, <<"5">>, <<"9">>, <<"10">>, <<"11">>, <<"12">>],
		ar_vdf_test_utils:computed_steps()),
	?assertEqual(SessionKey0, ar_vdf_test_utils:get_current_session_key()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20],
		ar_vdf_test_utils:computed_upper_bounds()).


%% @doc This test asserts that the client responds correctly when it is ahead of the VDF server.
test_client_ahead() ->
	SessionKey0 = ar_vdf_test_utils:get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 8 },
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 7, true, SessionKey0),
		"Partial session, client ahead"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 8 },
		ar_vdf_test_utils:apply_external_update(SessionKey1, [6, 5], 7, false, SessionKey0),
		"Full session, client ahead"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(SessionKey0, ar_vdf_test_utils:get_current_session_key()),
	?assertEqual(
		[<<"8">>, <<"7">>, <<"6">>, <<"5">>],
		ar_vdf_test_utils:computed_steps()),
	?assertEqual(
		[10, 10, 10, 10],
		ar_vdf_test_utils:computed_upper_bounds()).

%% @doc
%% Test case:
%% 1. VDF server pushes a partial update that skips too far ahead of the client
%% 2. Simulate the updates that the server would then push (i.e. full session updates of the current
%%    session and maybe previous session)
%%
%% Assert that the client responds correctly and only processes each step once (even though it may
%% see the same step several times as part of the full session updates).
test_skip_ahead() ->
	SessionKey0 = ar_vdf_test_utils:get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [5], 6, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = true, step_number = 6 },
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 8, true, SessionKey0),
		"Partial session1, server ahead"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey2, [], 12, true, SessionKey1),
		"Partial session2, server ahead"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [8, 7, 6, 5], 9, false, SessionKey0),
		"Full session1, all steps already seen"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey2, [11, 10], 12, false, SessionKey1),
		"Full session2, some steps already seen"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(SessionKey0, ar_vdf_test_utils:get_current_session_key()),
	?assertEqual(
		[<<"6">>, <<"5">>, <<"8">>, <<"7">>, <<"9">>, <<"12">>, <<"11">>, <<"10">>],
		ar_vdf_test_utils:computed_steps()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20],
		ar_vdf_test_utils:computed_upper_bounds()).

test_2_servers_switching() ->
	SessionKey0 = ar_vdf_test_utils:get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [6, 5], 7, false, SessionKey0, ar_vdf_test_utils:vdf_server_1()),
		"Full session1 from vdf_server_1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 8, true, SessionKey0, ar_vdf_test_utils:vdf_server_2()),
		"Partial session1 from vdf_server_2"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 9, true, SessionKey0, ar_vdf_test_utils:vdf_server_2()),
		"Partial session1 from vdf_server_2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey2, [], 11, true, SessionKey1, ar_vdf_test_utils:vdf_server_1()),
		"Partial session2 from vdf_server_1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey2, [10], 11, false, SessionKey1, ar_vdf_test_utils:vdf_server_1()),
		"Full session2 from vdf_server_1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 10, true, SessionKey0, ar_vdf_test_utils:vdf_server_2()),
		"Partial session1 from vdf_server_2 (should not change current session)"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 11, true, SessionKey0, ar_vdf_test_utils:vdf_server_2()),
		"Partial session1 from vdf_server_2 (should not change current session)"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 12, true, SessionKey0, ar_vdf_test_utils:vdf_server_2()),
		"Partial session1 from vdf_server_2 (should not change current session)"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(
			SessionKey2, [11, 10], 12, false, SessionKey1, ar_vdf_test_utils:vdf_server_2()),
		"Full session2 from vdf_server_2"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 12 },
		ar_vdf_test_utils:apply_external_update(SessionKey2, [], 12, true, SessionKey1, ar_vdf_test_utils:vdf_server_1()),
		"Partial (repeat) session2 from vdf_server_1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey2, [], 13, true, SessionKey1, ar_vdf_test_utils:vdf_server_1()),
		"Partial (new) session2 from vdf_server_1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey2, [], 14, true, SessionKey1, ar_vdf_test_utils:vdf_server_2()),
		"Partial (new) session2 from vdf_server_2"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(SessionKey0, ar_vdf_test_utils:get_current_session_key()),
	?assertEqual([
		<<"7">>, <<"6">>, <<"5">>, <<"8">>, <<"9">>,
		<<"11">>, <<"10">>, <<"10">>, <<"11">>, <<"12">>,
		<<"12">>, <<"13">>, <<"14">>
	], ar_vdf_test_utils:computed_steps()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20, 20, 20, 20, 20, 20],
		ar_vdf_test_utils:computed_upper_bounds()).

test_backtrack() ->
	SessionKey0 = ar_vdf_test_utils:get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [
			16, 15,				%% interval 3
			14, 13, 12, 11, 10, %% interval 2
			9, 8, 7, 6, 5		%% interval 1
		], 17, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 18, true, SessionKey0),
		"Partial session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey2, [], 15, true, SessionKey1),
		"Partial session2"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 18 },
		ar_vdf_test_utils:apply_external_update(
			SessionKey1, [8, 7, 6, 5], 9, false, SessionKey0),
		"Backtrack. Send full session1."),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(
			SessionKey2, [14, 13, 12, 11, 10], 15, false, SessionKey1),
		"Backtrack. Send full session2"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(SessionKey0, ar_vdf_test_utils:get_current_session_key()),
	?assertEqual([
		<<"17">>,<<"16">>,<<"15">>,<<"14">>,<<"13">>,<<"12">>,
		<<"11">>,<<"10">>,<<"9">>,<<"8">>,<<"7">>,<<"6">>,
		<<"5">>,<<"18">>,<<"15">>
	], ar_vdf_test_utils:computed_steps()),
	?assertEqual(
		[20, 20, 20, 20, 20, 20, 20, 20, 10, 10, 10, 10, 10, 20, 30],
		ar_vdf_test_utils:computed_upper_bounds()).

test_2_servers_backtrack() ->
	SessionKey0 = ar_vdf_test_utils:get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [
			16, 15,				%% interval 3
			14, 13, 12, 11, 10, %% interval 2
			9, 8, 7, 6, 5		%% interval 1
		], 17, false, SessionKey0, ar_vdf_test_utils:vdf_server_1()),
		"Full session1 from vdf_server_1"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(SessionKey1, [], 18, true, SessionKey0, ar_vdf_test_utils:vdf_server_1()),
		"Partial session1 from vdf_server_1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		ar_vdf_test_utils:apply_external_update(SessionKey2, [], 15, true, SessionKey1, ar_vdf_test_utils:vdf_server_2()),
		"Partial session2 from vdf_server_2"),
	?assertEqual(
		ok,
		ar_vdf_test_utils:apply_external_update(
			SessionKey2, [14, 13, 12, 11, 10], 15, false, SessionKey1, ar_vdf_test_utils:vdf_server_2()),
		"Backtrack in session2 from vdf_server_2"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([
		<<"17">>,<<"16">>,<<"15">>,<<"14">>,<<"13">>,<<"12">>,
		<<"11">>,<<"10">>,<<"9">>,<<"8">>,<<"7">>,<<"6">>,
		<<"5">>,<<"18">>,<<"15">>
	], ar_vdf_test_utils:computed_steps()),
	?assertEqual(SessionKey0, ar_vdf_test_utils:get_current_session_key()),
	?assertEqual(
		[20, 20, 20, 20, 20, 20, 20, 20, 10, 10, 10, 10, 10, 20, 30],
		ar_vdf_test_utils:computed_upper_bounds()). 