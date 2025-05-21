-module(ar_vdf_serialize_tests).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_vdf.hrl").

%% -------------------------------------------------------------------------------------------------
%% Test Registration
%% -------------------------------------------------------------------------------------------------

serialize_test_() ->
	[
		{timeout, 120, fun test_serialize_update_format_2/0},
		{timeout, 120, fun test_serialize_update_format_3/0},
		{timeout, 120, fun test_serialize_update_format_4/0},
		{timeout, 120, fun test_serialize_response/0},
		{timeout, 120, fun test_serialize_response_compatibility/0}
	].

%% -------------------------------------------------------------------------------------------------
%% Tests
%% -------------------------------------------------------------------------------------------------

test_serialize_update_format_2() ->
	SessionKey0 = {crypto:strong_rand_bytes(48), 0, 1},
	SessionKey1 = {crypto:strong_rand_bytes(48), 1, 1},
	Checkpoints = [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, 25)],
	Update = #nonce_limiter_update{
		session_key = SessionKey1,
		is_partial = true,
		session = #vdf_session{
			step_checkpoints_map = #{ 1 => Checkpoints },
			upper_bound = 1,
			next_upper_bound = 1,
			prev_session_key = SessionKey0,
			step_number = 1,
			seed = element(1, SessionKey1),
			steps = [crypto:strong_rand_bytes(32)]
		}
	},
	Binary = ar_serialize:nonce_limiter_update_to_binary(2, Update),
	?assertEqual({ok, Update}, ar_serialize:binary_to_nonce_limiter_update(2, Binary)).

test_serialize_update_format_3() ->
	SessionKey0 = {crypto:strong_rand_bytes(48), 0, 1},
	SessionKey1 = {crypto:strong_rand_bytes(48), 1, 1},
	Checkpoints = [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, 25)],
	Update = #nonce_limiter_update{
		session_key = SessionKey1,
		is_partial = true,
		session = #vdf_session{
			step_checkpoints_map = #{ 1 => Checkpoints },
			upper_bound = 1,
			next_upper_bound = 1,
			prev_session_key = SessionKey0,
			step_number = 1,
			seed = element(1, SessionKey1),
			steps = [crypto:strong_rand_bytes(32)]
		}
	},
	Binary = ar_serialize:nonce_limiter_update_to_binary(3, Update),
	?assertEqual({ok, Update}, ar_serialize:binary_to_nonce_limiter_update(3, Binary)).

test_serialize_update_format_4() ->
	SessionKey0 = {crypto:strong_rand_bytes(48), 0, 1},
	SessionKey1 = {crypto:strong_rand_bytes(48), 1, 1},
	Checkpoints = [crypto:strong_rand_bytes(32) || _ <- lists:seq(1, 25)],
	Update = #nonce_limiter_update{
		session_key = SessionKey1,
		is_partial = true,
		session = #vdf_session{
			step_checkpoints_map = #{ 1 => Checkpoints },
			upper_bound = 1,
			next_upper_bound = 1,
			prev_session_key = SessionKey0,
			vdf_difficulty = 10000,
			next_vdf_difficulty = 1,
			step_number = 1,
			seed = element(1, SessionKey1),
			steps = [crypto:strong_rand_bytes(32)]
		}
	},
	Binary = ar_serialize:nonce_limiter_update_to_binary(4, Update),
	?assertEqual({ok, Update}, ar_serialize:binary_to_nonce_limiter_update(4, Binary)).

%% @doc test serializing and deserializing a #nonce_limiter_update_response when the client
%% is running the same node version as the server.
test_serialize_response() ->
	ResponseA = #nonce_limiter_update_response{},
	BinaryA = ar_serialize:nonce_limiter_update_response_to_binary(ResponseA),
	?assertEqual({ok, ResponseA}, ar_serialize:binary_to_nonce_limiter_update_response(BinaryA)),

	ResponseB = #nonce_limiter_update_response{
		session_found = false,
		step_number = 8589934593,
		postpone = 255,
		format = 2
	},
	BinaryB = ar_serialize:nonce_limiter_update_response_to_binary(ResponseB),
	?assertEqual({ok, ResponseB}, ar_serialize:binary_to_nonce_limiter_update_response(BinaryB)).

%% @doc test serializing and deserializing a #nonce_limiter_update_response when the client
%% is running an older node version than the server.
test_serialize_response_compatibility() ->
	BinaryA = << 0:8, 1:8, 5:8 >>,
	ResponseA = #nonce_limiter_update_response{
		session_found = false,
		step_number = 5,
		postpone = 0,
		format = 1
	},
	?assertEqual({ok, ResponseA}, ar_serialize:binary_to_nonce_limiter_update_response(BinaryA)),

	BinaryB = << 1:8, 2:8, 511:16, 120:8 >>,
	ResponseB = #nonce_limiter_update_response{
		session_found = true,
		step_number = 511,
		postpone = 120,
		format = 1
	},
	?assertEqual({ok, ResponseB}, ar_serialize:binary_to_nonce_limiter_update_response(BinaryB)). 