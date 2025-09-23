-module(ar_vdf_external_update_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_mining.hrl").

-import(ar_test_node, [assert_wait_until_height/2, post_block/2, send_new_block/2]).

%% we have to wait to let the ar_events get processed whenever we apply a VDF step
-define(WAIT_TIME, 1000).

%% -------------------------------------------------------------------------------------------------
%% Test Fixtures
%% -------------------------------------------------------------------------------------------------

setup_external_update() ->
	{ok, Config} = application:get_env(arweave, config),
	[B0] = ar_weave:init(),
	%% Start the testnode with a configured VDF server so that it doesn't compute its own VDF -
	%% this is necessary so that we can test the behavior of apply_external_update without any
	%% auto-computed VDF steps getting in the way.
	_ = ar_test_node:start(
		B0, ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config#config{ 
			nonce_limiter_server_trusted_peers = [
				ar_util:format_peer(vdf_server_1()),
				ar_util:format_peer(vdf_server_2()) 
			],
			mine = true
		}
	),
	ets:new(computed_output, [named_table, ordered_set, public]),
	ets:new(add_task, [named_table, bag, public]),
	Pid = spawn(
		fun() ->
			ok = ar_events:subscribe(nonce_limiter),
			computed_output()
		end
	),
	{Pid, Config}.

cleanup_external_update({Pid, Config}) ->
	exit(Pid, kill),
	ok = application:set_env(arweave, config, Config),
	ets:delete(add_task),
	ets:delete(computed_output).

%% -------------------------------------------------------------------------------------------------
%% Test Registration
%% -------------------------------------------------------------------------------------------------

external_update_test_() ->
    {foreach,
		fun setup_external_update/0,
     	fun cleanup_external_update/1,
		[
			ar_test_node:test_with_mocked_functions([mock_add_task(), mock_reset_frequency()],
				fun test_session_overlap/0, 120),
			ar_test_node:test_with_mocked_functions([mock_add_task(), mock_reset_frequency()],
				fun test_client_ahead/0, 120),
			ar_test_node:test_with_mocked_functions([mock_add_task(), mock_reset_frequency()],
				fun test_skip_ahead/0, 120),
			ar_test_node:test_with_mocked_functions([mock_add_task(), mock_reset_frequency()],
				fun test_2_servers_switching/0, 120),
			ar_test_node:test_with_mocked_functions([mock_add_task(), mock_reset_frequency()],
				fun test_backtrack/0, 120),
			ar_test_node:test_with_mocked_functions([mock_add_task(), mock_reset_frequency()],
				fun test_2_servers_backtrack/0, 120)
		]
    }.

mining_session_test_() ->
    {foreach,
		fun setup_external_update/0,
     	fun cleanup_external_update/1,
	[
		ar_test_node:test_with_mocked_functions([mock_add_task(), mock_reset_frequency()],
			fun test_mining_session/0, 120)
	]
    }.

%% -------------------------------------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------------------------------------

%%
%% external_update_test_
%%

%% @doc The VDF session key is only updated when a block is procesed by the VDF server. Until that
%% happens the serve will push all VDF steps under the same session key - even if those steps
%% cross an entropy reset line. When a block comes in the server will update the session key
%% *and* move all appropriate steps to that session. Prior to 2.7 this caused VDF clients to
%% process some steps twice - once under the old session key, and once under the new session key.
%% This test asserts that this behavior has been fixed and that VDF clients only process each
%% step once.
test_session_overlap() ->
	SessionKey0 = get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey1, [], 8, true, SessionKey0),
		"Partial session1, session not found"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 9, true, SessionKey0),
		"Partial session1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 10, true, SessionKey0),
		"Partial session1, interval2"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 11, true, SessionKey0),
		"Partial session1, interval2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 12, true, SessionKey1),
		"Partial session2, interval2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = true, step_number = 11 },
		apply_external_update(SessionKey1, [8, 7, 6, 5], 9, false, SessionKey1),
		"Full session1, all steps already seen"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [11, 10], 12, false, SessionKey1),
		"Full session2, some steps already seen"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(
		[<<"8">>, <<"7">>, <<"6">>, <<"5">>, <<"9">>, <<"10">>, <<"11">>, <<"12">>],
	computed_steps()),
	?assertEqual(SessionKey0, get_current_session_key()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20],
		computed_upper_bounds()).


%% @doc This test asserts that the client responds correctly when it is ahead of the VDF server.
test_client_ahead() ->
	SessionKey0 = get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 8 },
		apply_external_update(SessionKey1, [], 7, true, SessionKey0),
		"Partial session, client ahead"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 8 },
		apply_external_update(SessionKey1, [6, 5], 7, false, SessionKey0),
		"Full session, client ahead"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(SessionKey0, get_current_session_key()),
	?assertEqual(
		[<<"8">>, <<"7">>, <<"6">>, <<"5">>],
		computed_steps()),
	?assertEqual(
		[10, 10, 10, 10],
		computed_upper_bounds()).

%% @doc
%% Test case:
%% 1. VDF server pushes a partial update that skips too far ahead of the client
%% 2. Simulate the updates that the server would then push (i.e. full session updates of the current
%%    session and maybe previous session)
%%
%% Assert that the client responds correctly and only processes each step once (even though it may
%% see the same step several times as part of the full session updates).
test_skip_ahead() ->
	SessionKey0 = get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [5], 6, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = true, step_number = 6 },
		apply_external_update(SessionKey1, [], 8, true, SessionKey0),
		"Partial session1, server ahead"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [7, 6, 5], 8, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 12, true, SessionKey1),
		"Partial session2, server ahead"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [8, 7, 6, 5], 9, false, SessionKey0),
		"Full session1, all steps already seen"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [11, 10], 12, false, SessionKey1),
		"Full session2, some steps already seen"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(SessionKey0, get_current_session_key()),
	?assertEqual(
		[<<"6">>, <<"5">>, <<"8">>, <<"7">>, <<"9">>, <<"12">>, <<"11">>, <<"10">>],
		computed_steps()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20],
		computed_upper_bounds()).

test_2_servers_switching() ->
	SessionKey0 = get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [6, 5], 7, false, SessionKey0, vdf_server_1()),
		"Full session1 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 8, true, SessionKey0, vdf_server_2()),
		"Partial session1 from vdf_server_2"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 9, true, SessionKey0, vdf_server_2()),
		"Partial session1 from vdf_server_2"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 11, true, SessionKey1, vdf_server_1()),
		"Partial session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [10], 11, false, SessionKey1, vdf_server_1()),
		"Full session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 10, true, SessionKey0, vdf_server_2()),
		"Partial session1 from vdf_server_2 (should not change current session)"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 11, true, SessionKey0, vdf_server_2()),
		"Partial session1 from vdf_server_2 (should not change current session)"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 12, true, SessionKey0, vdf_server_2()),
		"Partial session1 from vdf_server_2 (should not change current session)"),
	?assertEqual(
		ok,
		apply_external_update(
			SessionKey2, [11, 10], 12, false, SessionKey1, vdf_server_2()),
		"Full session2 from vdf_server_2"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 12 },
		apply_external_update(SessionKey2, [], 12, true, SessionKey1, vdf_server_1()),
		"Partial (repeat) session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [], 13, true, SessionKey1, vdf_server_1()),
		"Partial (new) session2 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey2, [], 14, true, SessionKey1, vdf_server_2()),
		"Partial (new) session2 from vdf_server_2"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(SessionKey0, get_current_session_key()),
	?assertEqual([
		<<"7">>, <<"6">>, <<"5">>, <<"8">>, <<"9">>,
		<<"11">>, <<"10">>, <<"10">>, <<"11">>, <<"12">>,
		<<"12">>, <<"13">>, <<"14">>
	], computed_steps()),
	?assertEqual(
		[10, 10, 10, 10, 10, 20, 20, 20, 20, 20, 20, 20, 20],
		computed_upper_bounds()).

test_backtrack() ->
	SessionKey0 = get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [
			16, 15,				%% interval 3
			14, 13, 12, 11, 10, %% interval 2
			9, 8, 7, 6, 5		%% interval 1
		], 17, false, SessionKey0),
		"Full session1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 18, true, SessionKey0),
		"Partial session1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 15, true, SessionKey1),
		"Partial session2"),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 18 },
		apply_external_update(
			SessionKey1, [8, 7, 6, 5], 9, false, SessionKey0),
		"Backtrack. Send full session1."),
	?assertEqual(
		ok,
		apply_external_update(
			SessionKey2, [14, 13, 12, 11, 10], 15, false, SessionKey1),
		"Backtrack. Send full session2"),
	timer:sleep(?WAIT_TIME),
	?assertEqual(SessionKey0, get_current_session_key()),
	?assertEqual([
		<<"17">>,<<"16">>,<<"15">>,<<"14">>,<<"13">>,<<"12">>,
        <<"11">>,<<"10">>,<<"9">>,<<"8">>,<<"7">>,<<"6">>,
        <<"5">>,<<"18">>,<<"15">>
	], computed_steps()),
	?assertEqual(
		[20, 20, 20, 20, 20, 20, 20, 20, 10, 10, 10, 10, 10, 20, 30],
		computed_upper_bounds()).

test_2_servers_backtrack() ->
	SessionKey0 = get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [
			16, 15,				%% interval 3
			14, 13, 12, 11, 10, %% interval 2
			9, 8, 7, 6, 5		%% interval 1
		], 17, false, SessionKey0, vdf_server_1()),
		"Full session1 from vdf_server_1"),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [], 18, true, SessionKey0, vdf_server_1()),
		"Partial session1 from vdf_server_1"),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey2, [], 15, true, SessionKey1, vdf_server_2()),
		"Partial session2 from vdf_server_2"),
	?assertEqual(
		ok,
		apply_external_update(
			SessionKey2, [14, 13, 12, 11, 10], 15, false, SessionKey1, vdf_server_2()),
		"Backtrack in session2 from vdf_server_2"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([
		<<"17">>,<<"16">>,<<"15">>,<<"14">>,<<"13">>,<<"12">>,
        <<"11">>,<<"10">>,<<"9">>,<<"8">>,<<"7">>,<<"6">>,
        <<"5">>,<<"18">>,<<"15">>
	], computed_steps()),
	?assertEqual(SessionKey0, get_current_session_key()),
	?assertEqual(
		[20, 20, 20, 20, 20, 20, 20, 20, 10, 10, 10, 10, 10, 20, 30],
		computed_upper_bounds()).

test_mining_session() ->
	SessionKey0 = get_current_session_key(),
	SessionKey1 = {<<"session1">>, 1, 1},
	SessionKey2 = {<<"session2">>, 2, 1},
	SessionKey3 = {<<"session3">>, 3, 1},
	ar_test_node:mine(),
	?assertEqual(
		ok,
		apply_external_update(SessionKey0, [], 2, true, undefined),
		"Partial session0, should mine"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([SessionKey0], sets:to_list(ar_mining_server:active_sessions())),
	?assertEqual([2], mined_steps()),
	?assertEqual(
		ok,
		apply_external_update(SessionKey0, [3], 4, false, undefined),
		"Full session0, should mine"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([SessionKey0], sets:to_list(ar_mining_server:active_sessions())),
	?assertEqual([4, 3], mined_steps()),
	?assertEqual(
		#nonce_limiter_update_response{ step_number = 4 },
		apply_external_update(SessionKey0, [], 4, true, undefined),
		"Repeat step, should not mine"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([SessionKey0], sets:to_list(ar_mining_server:active_sessions())),
	?assertEqual([], mined_steps()),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey1, [], 6, true, SessionKey0),
		"Partial session1, should not mine"),
	timer:sleep(?WAIT_TIME),
	?assertEqual([SessionKey0], sets:to_list(ar_mining_server:active_sessions())),
	?assertEqual([], mined_steps()),
	?assertEqual(
		ok,
		apply_external_update(SessionKey1, [5], 6, false, SessionKey0),
		"Full session1, should mine"),
	timer:sleep(?WAIT_TIME),
	assert_sessions_equal([SessionKey0, SessionKey1], ar_mining_server:active_sessions()),
	?assertEqual([6, 5], mined_steps()),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey3, [], 16, true, SessionKey2),
		"Partial session3, should not mine"),
	timer:sleep(?WAIT_TIME),
	assert_sessions_equal([SessionKey0, SessionKey1], ar_mining_server:active_sessions()),
	?assertEqual([], mined_steps()),
	?assertEqual(
		#nonce_limiter_update_response{ session_found = false },
		apply_external_update(SessionKey3, [15], 16, true, SessionKey2),
		"Full session3, should not mine"),
	timer:sleep(?WAIT_TIME),
	assert_sessions_equal([SessionKey0, SessionKey1], ar_mining_server:active_sessions()),
	?assertEqual([], mined_steps()),
	%% Current session is only updated when applying a new tip block, not when applying a VDF step
	%% from a VDF server.
	?assertEqual(SessionKey0, get_current_session_key()).

%% -------------------------------------------------------------------------------------------------
%% Helper Functions
%% -------------------------------------------------------------------------------------------------

init(Req, State) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	handle(SplitPath, Req, State).

handle([<<"vdf">>], Req, State) ->
	{ok, Body, _} = ar_http_req:body(Req, ?MAX_BODY_SIZE),
	case ar_serialize:binary_to_nonce_limiter_update(2, Body) of
		{ok, Update} ->
			handle_update(Update, Req, State);
		{error, _} ->
			Response = #nonce_limiter_update_response{ format = 2 },
			Bin = ar_serialize:nonce_limiter_update_response_to_binary(Response),
			{ok, cowboy_req:reply(202, #{}, Bin, Req), State}
	end.

handle_update(Update, Req, State) ->
	{Seed, _, _} = Update#nonce_limiter_update.session_key,
	IsPartial  = Update#nonce_limiter_update.is_partial,
	Session = Update#nonce_limiter_update.session,
	StepNumber = Session#vdf_session.step_number,
	NSteps = length(Session#vdf_session.steps),
	Checkpoints = maps:get(StepNumber, Session#vdf_session.step_checkpoints_map),

	UpdateOutput = hd(Checkpoints),

	SessionOutput = hd(Session#vdf_session.steps),

	?assertNotEqual(Checkpoints, Session#vdf_session.steps),
	%% #nonce_limiter_update.checkpoints should be the checkpoints of the last step so
	%% the head of checkpoints should match the head of the session's steps
	?assertEqual(UpdateOutput, SessionOutput),

	case ets:lookup(computed_output, Seed) of
		[{Seed, FirstStepNumber, LatestStepNumber}] ->
			?assert(not IsPartial orelse StepNumber == LatestStepNumber + 1,
				lists:flatten(io_lib:format(
					"Partial VDF update did not increase by 1, "
					"StepNumber: ~p, LatestStepNumber: ~p",
					[StepNumber, LatestStepNumber]))),

			ets:insert(computed_output, {Seed, FirstStepNumber, StepNumber}),
			{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};
		_ ->
			case IsPartial of
				true ->
					Response = #nonce_limiter_update_response{ session_found = false },
					Bin = ar_serialize:nonce_limiter_update_response_to_binary(Response),
					{ok, cowboy_req:reply(202, #{}, Bin, Req), State};
				false ->
					ets:insert(computed_output, {Seed, StepNumber - NSteps + 1, StepNumber}),
					{ok, cowboy_req:reply(200, #{}, <<>>, Req), State}
			end
	end.

vdf_server_1() ->
	{127,0,0,1,2001}.

vdf_server_2() ->
	{127,0,0,1,2002}.

computed_steps() ->
    lists:reverse(ets:foldl(fun({_, Step, _}, Acc) -> [Step | Acc] end, [], computed_output)).

computed_upper_bounds() ->
    lists:reverse(ets:foldl(fun({_, _, UpperBound}, Acc) -> [UpperBound | Acc] end, [], computed_output)).

mined_steps() ->
	Steps = lists:reverse(ets:foldl(
		fun({_Worker, _Task, Step}, Acc) -> [Step | Acc] end, [], add_task)),
	ets:delete_all_objects(add_task),
	Steps.

computed_output() ->
	receive
		{event, nonce_limiter, {computed_output, Args}} ->
			{_SessionKey, _StepNumber, Output, UpperBound} = Args,
			Key = ets:info(computed_output, size) + 1, % Unique key based on current size, ensures ordering
    		ets:insert(computed_output, {Key, Output, UpperBound}),
			computed_output()
	end.

apply_external_update(SessionKey, ExistingSteps, StepNumber, IsPartial, PrevSessionKey) ->
	apply_external_update(SessionKey, ExistingSteps, StepNumber, IsPartial, PrevSessionKey,
		vdf_server_1()).
apply_external_update(SessionKey, ExistingSteps, StepNumber, IsPartial, PrevSessionKey, Peer) ->
	{Seed, Interval, _Difficulty} = SessionKey,
	Steps = [list_to_binary(integer_to_list(Step)) || Step <- [StepNumber | ExistingSteps]],
	Session = #vdf_session{
		upper_bound = Interval * 10,
		next_upper_bound = (Interval+1) * 10,
		prev_session_key = PrevSessionKey,
		step_number = StepNumber,
		seed = Seed,
		steps = Steps
	},

	Update = #nonce_limiter_update{
		session_key = SessionKey,
		is_partial = IsPartial,
		session = Session
	},
	ar_nonce_limiter:apply_external_update(Update, Peer).

get_current_session_key() ->
	{CurrentSessionKey, _} = ar_nonce_limiter:get_current_session(),
	CurrentSessionKey.

mock_add_task() ->
	{
		ar_mining_worker, add_task,
		fun(Worker, TaskType, Candidate) ->
			ets:insert(add_task, {Worker, TaskType, Candidate#mining_candidate.step_number})
		end
	}.

mock_reset_frequency() ->
	{
		ar_nonce_limiter, get_reset_frequency,
		fun() ->
			5
		end
	}.

assert_sessions_equal(List, Set) ->
	?assertEqual(lists:sort(List), lists:sort(sets:to_list(Set))).
