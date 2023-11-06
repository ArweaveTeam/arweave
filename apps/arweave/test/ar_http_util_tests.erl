-module(ar_http_util_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("arweave/include/ar.hrl").

% get_tx_content_type_test() ->
% 	?assertEqual(
% 		none,
% 		content_type_from_tags([])
% 	),
% 	?assertEqual(
% 		{valid, <<"text/plain">>},
% 		content_type_from_tags([
% 			{<<"Content-Type">>, <<"text/plain">>}
% 		])
% 	),
% 	?assertEqual(
% 		{valid, <<"text/html; charset=utf-8">>},
% 		content_type_from_tags([
% 			{<<"Content-Type">>, <<"text/html; charset=utf-8">>}
% 		])
% 	),
% 	?assertEqual(
% 		{valid, <<"application/x.arweave-manifest+json">>},
% 		content_type_from_tags([
% 			{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}
% 		])
% 	),
% 	?assertEqual(
% 		invalid,
% 		content_type_from_tags([
% 			{<<"Content-Type">>, <<"application/javascript\r\nSet-Cookie: foo=bar">>}
% 		])
% 	).

% content_type_from_tags(Tags) ->
% 	ar_http_util:get_tx_content_type(#tx { tags = Tags }).


mining_server_task_queue_test_() ->
	[
		{timeout, 120, fun test_task_queue/0}
	].

test_task_queue() ->
	?LOG_ERROR("START"),
	[B0] = ar_weave:init([]),
	ar_test_node:start(B0),
	lists:foreach(
		fun(Height) ->
			ar_node:mine(),
			ar_test_node:wait_until_height(Height),
			?LOG_ERROR("Height: ~p", [Height])
		end,
		lists:seq(1, 10)
	).
	% SessionKey = {crypto:strong_rand_bytes(32), 0},
	% PrevSessionKey = {crypto:strong_rand_bytes(32), 0},
	% PrevSession = #vdf_session{
	% 	seed = crypto:strong_rand_bytes(32),
	% 	step_number = 0
	% },
	% Session = #vdf_session{
	% 	step_number = 1
	% },
	% Output = crypto:strong_rand_bytes(32),
	% PartitionUpperBound = 1800000 * 140,
	% ar_events:send(nonce_limiter, {computed_output,
	% 	{SessionKey, Session, PrevSessionKey, PrevSession, Output, PartitionUpperBound}}).