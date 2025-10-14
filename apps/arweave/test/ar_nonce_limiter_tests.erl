-module(ar_nonce_limiter_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-include_lib("eunit/include/eunit.hrl").


%% @doc Reset the state and stop computing steps automatically. Used in tests.
reset_and_pause() ->
	gen_server:cast(ar_nonce_limiter, reset_and_pause).

%% @doc Do not emit the initialized event. Used in tests.
turn_off_initialized_event() ->
	gen_server:cast(ar_nonce_limiter, turn_off_initialized_event).

%% @doc Get all steps starting from the latest on the current tip. Used in tests.
get_steps() ->
	gen_server:call(ar_nonce_limiter, get_steps).

%% @doc Compute a single step. Used in tests.
step() ->
	Self = self(),
	spawn(
		fun() ->
			ok = ar_events:subscribe(nonce_limiter),
			gen_server:cast(ar_nonce_limiter, compute_step),
			receive
				{event, nonce_limiter, {computed_output, _}} ->
					Self ! done
			end
		end
	),
	receive
		done ->
			ok
	end.


assert_session(B, PrevB) ->
	%% vdf_diffic ulty and next_vdf_difficulty in cached VDF sessions should be
	%% updated whenever a new block is validated.
	#nonce_limiter_info{
		vdf_difficulty = PrevBVDFDifficulty, next_vdf_difficulty = PrevBNextVDFDifficulty
	} = PrevB#block.nonce_limiter_info,
	#nonce_limiter_info{
		vdf_difficulty = BVDFDifficulty, next_vdf_difficulty = BNextVDFDifficulty
	} = B#block.nonce_limiter_info,
	PrevBSessionKey = ar_nonce_limiter:session_key(PrevB#block.nonce_limiter_info),
	BSessionKey = ar_nonce_limiter:session_key(B#block.nonce_limiter_info),

	BSession = ar_nonce_limiter:get_session(BSessionKey),
	?assertEqual(BVDFDifficulty, BSession#vdf_session.vdf_difficulty),
	?assertEqual(BNextVDFDifficulty,
		BSession#vdf_session.next_vdf_difficulty),
	case PrevBSessionKey == BSessionKey of
		true ->
			ok;
		false ->
			PrevBSession = ar_nonce_limiter:get_session(PrevBSessionKey),
			?assertEqual(PrevBVDFDifficulty,
				PrevBSession#vdf_session.vdf_difficulty),
			?assertEqual(PrevBNextVDFDifficulty,
				PrevBSession#vdf_session.next_vdf_difficulty)
	end.

assert_validate(B, PrevB, ExpectedResult) ->
	ar_nonce_limiter:request_validation(B#block.indep_hash, B#block.nonce_limiter_info,
			PrevB#block.nonce_limiter_info),
	BH = B#block.indep_hash,
	receive
		{event, nonce_limiter, {valid, BH}} ->
			case ExpectedResult of
				valid ->
					assert_session(B, PrevB),
					ok;
				_ ->
					?assert(false, iolist_to_binary(io_lib:format("Unexpected "
							"validation success. Expected: ~p.", [ExpectedResult])))
			end;
		{event, nonce_limiter, {invalid, BH, Code}} ->
			case ExpectedResult of
				{invalid, Code} ->
					ok;
				_ ->
					?assert(false, iolist_to_binary(io_lib:format("Unexpected "
							"validation failure: ~p. Expected: ~p.",
							[Code, ExpectedResult])))
			end
	after 2000 ->
		?assert(false, "Validation timeout.")
	end.

assert_step_number(N) ->
	timer:sleep(200),
	?assert(ar_util:do_until(fun() -> ar_nonce_limiter:get_current_step_number() == N end, 100, 1000)).

test_block(StepNumber, Output, Seed, NextSeed, LastStepCheckpoints, Steps,
		VDFDifficulty, NextVDFDifficulty) ->
	#block{ indep_hash = crypto:strong_rand_bytes(48),
			nonce_limiter_info = #nonce_limiter_info{ output = Output,
					global_step_number = StepNumber, seed = Seed, next_seed = NextSeed,
					last_step_checkpoints = LastStepCheckpoints, steps = Steps,
					vdf_difficulty = VDFDifficulty, next_vdf_difficulty = NextVDFDifficulty }
	}.
	
mock_reset_frequency() ->
	{ar_nonce_limiter, get_reset_frequency, fun() -> 5 end}.

applies_validated_steps_test_() ->
	ar_test_node:test_with_mocked_functions([mock_reset_frequency()],
		fun test_applies_validated_steps/0, 60).

test_applies_validated_steps() ->
	reset_and_pause(),
	Seed = crypto:strong_rand_bytes(48),
	NextSeed = crypto:strong_rand_bytes(48),
	NextSeed2 = crypto:strong_rand_bytes(48),
	InitialOutput = crypto:strong_rand_bytes(32),
	B1VDFDifficulty = 3,
	B1NextVDFDifficulty = 3,
	B1 = test_block(1, InitialOutput, Seed, NextSeed, [], [],
			B1VDFDifficulty, B1NextVDFDifficulty),
	turn_off_initialized_event(),
	ar_nonce_limiter:account_tree_initialized([B1]),
	true = ar_util:do_until(fun() -> ar_nonce_limiter:get_current_step_number() == 1 end, 100, 1000),
	assert_session(B1, B1),
	{ok, Output2, _} = ar_nonce_limiter:compute(2, InitialOutput, B1VDFDifficulty),
	B2VDFDifficulty = 3,
	B2NextVDFDifficulty = 4,
	B2 = test_block(2, Output2, Seed, NextSeed, [], [Output2], 
			B2VDFDifficulty, B2NextVDFDifficulty),
	ok = ar_events:subscribe(nonce_limiter),
	assert_validate(B2, B1, valid),
	assert_validate(B2, B1, valid),
	assert_validate(B2#block{ nonce_limiter_info = #nonce_limiter_info{} }, B1, {invalid, 1}),
	N2 = B2#block.nonce_limiter_info,
	assert_validate(B2#block{ nonce_limiter_info = N2#nonce_limiter_info{ steps = [] } },
			B1, {invalid, 4}),
	assert_validate(B2#block{
			nonce_limiter_info = N2#nonce_limiter_info{ steps = [Output2, Output2] } },
			B1, {invalid, 2}),
	assert_step_number(2),
	[step() || _ <- lists:seq(1, 3)],
	assert_step_number(5),
	ar_events:send(node_state, {new_tip, B2, B1}),
	%% We have just applied B2 with a VDF difficulty update => a new session has to be opened.
	assert_step_number(2),
	assert_session(B2, B1),
	{ok, Output3, _} = ar_nonce_limiter:compute(3, Output2, B2VDFDifficulty),
	{ok, Output4, _} = ar_nonce_limiter:compute(4, Output3, B2VDFDifficulty),
	B3VDFDifficulty = 3,
	B3NextVDFDifficulty = 4,
	B3 = test_block(4, Output4, Seed, NextSeed, [], [Output4, Output3],
			B3VDFDifficulty, B3NextVDFDifficulty),
	assert_validate(B3, B2, valid),
	assert_validate(B3, B1, valid),
	%% Entropy reset line crossed at step 5, add entropy and apply next_vdf_difficulty
	{ok, Output5, _} = ar_nonce_limiter:compute(5, ar_nonce_limiter:mix_seed(Output4, NextSeed), B3NextVDFDifficulty),
	B4VDFDifficulty = 4,
	B4NextVDFDifficulty = 5,
	B4 = test_block(5, Output5, NextSeed, NextSeed2, [], [Output5],
			B4VDFDifficulty, B4NextVDFDifficulty),
	[step() || _ <- lists:seq(1, 6)],
	assert_step_number(10),
	assert_validate(B4, B3, valid),
	ar_events:send(node_state, {new_tip, B4, B3}),
	assert_step_number(9),
	assert_session(B4, B3),
	assert_validate(B4, B4, {invalid, 1}),
	% % 5, 6, 7, 8, 9, 10
	B5VDFDifficulty = 5,
	B5NextVDFDifficulty = 6,
	B5 = test_block(10, <<>>, NextSeed, NextSeed2, [], [<<>>],
			B5VDFDifficulty, B5NextVDFDifficulty),
	assert_validate(B5, B4, {invalid, 3}),
	B6VDFDifficulty = 5,
	B6NextVDFDifficulty = 6,
	B6 = test_block(10, <<>>, NextSeed, NextSeed2, [],
			% Steps 10, 9, 8, 7, 6.
			[<<>> | lists:sublist(get_steps(), 4)],
			B6VDFDifficulty, B6NextVDFDifficulty),
	assert_validate(B6, B4, {invalid, 3}),
	Invalid = crypto:strong_rand_bytes(32),
	B7VDFDifficulty = 5,
	B7NextVDFDifficulty = 6,
	B7 = test_block(10, Invalid, NextSeed, NextSeed2, [],
			% Steps 10, 9, 8, 7, 6.
			[Invalid | lists:sublist(get_steps(), 4)],
			B7VDFDifficulty, B7NextVDFDifficulty),
	assert_validate(B7, B4, {invalid, 3}),
	%% Last valid block was B4, so that's the vdf_difficulty to use (not next_vdf_difficulty cause
	%% the next entropy reset line isn't until step 10)
	{ok, Output6, _} = ar_nonce_limiter:compute(6, Output5, B4VDFDifficulty),
	{ok, Output7, _} = ar_nonce_limiter:compute(7, Output6, B4VDFDifficulty),
	{ok, Output8, _} = ar_nonce_limiter:compute(8, Output7, B4VDFDifficulty),
	B8VDFDifficulty = 4,
	%% Change the next_vdf_difficulty to confirm that apply_tip2 handles updating an
	%% existing VDF session
	B8NextVDFDifficulty = 6, 
	B8 = test_block(8, Output8, NextSeed, NextSeed2, [], [Output8, Output7, Output6],
			B8VDFDifficulty, B8NextVDFDifficulty),
	ar_events:send(node_state, {new_tip, B8, B4}),
	timer:sleep(1000),
	assert_session(B8, B4),
	assert_validate(B8, B4, valid),
	ok.
