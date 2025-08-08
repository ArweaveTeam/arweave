-module(ar_vdf_test_utils).

-export([
	setup/0, cleanup/1,
	setup_external_update/0, cleanup_external_update/1,
	vdf_server_1/0, vdf_server_2/0,
	computed_steps/0, computed_upper_bounds/0, mined_steps/0,
	computed_output/0,
	apply_external_update/5, apply_external_update/6,
	get_current_session_key/0,
	mock_add_task/0, mock_reset_frequency/0,
	assert_sessions_equal/2,
	init/2, handle_update/3
]).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_config.hrl").
-include("ar_consensus.hrl").
-include("ar_mining.hrl").
-include("ar_vdf.hrl").

-import(ar_test_node, [assert_wait_until_height/2, post_block/2, send_new_block/2]).

%% we have to wait to let the ar_events get processed whenever we apply a VDF step
-define(WAIT_TIME, 1000).

%% -------------------------------------------------------------------------------------------------
%% Cowboy Handler for VDF Tests (shared by test modules)
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

%% -------------------------------------------------------------------------------------------------
%% Test Fixtures
%% -------------------------------------------------------------------------------------------------

setup() ->
	ets:new(computed_output, [named_table, set, public]),
	{ok, Config} = application:get_env(arweave, config),
	{ok, PeerConfig} = ar_test_node:remote_call(peer1, application, get_env, [arweave, config]),
	{Config, PeerConfig}.

cleanup({Config, PeerConfig}) ->
	application:set_env(arweave, config, Config),
	ar_test_node:remote_call(peer1, application, set_env, [arweave, config, PeerConfig]),
	ets:delete(computed_output).

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
%% Helper Functions
%% -------------------------------------------------------------------------------------------------

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