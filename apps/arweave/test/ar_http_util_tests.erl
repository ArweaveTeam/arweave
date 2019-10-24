-module(ar_http_util_tests).

-include_lib("eunit/include/eunit.hrl").
-include("src/ar.hrl").

get_tx_content_type_test() ->
	?assertEqual(
		none,
		content_type_from_tags([])
	),
	?assertEqual(
		{valid, <<"text/plain">>},
		content_type_from_tags([
			{<<"Content-Type">>, <<"text/plain">>}
		])
	),
	?assertEqual(
		{valid, <<"text/html; charset=utf-8">>},
		content_type_from_tags([
			{<<"Content-Type">>, <<"text/html; charset=utf-8">>}
		])
	),
	?assertEqual(
		{valid, <<"application/x.arweave-manifest+json">>},
		content_type_from_tags([
			{<<"Content-Type">>, <<"application/x.arweave-manifest+json">>}
		])
	),
	?assertEqual(
		invalid,
		content_type_from_tags([
			{<<"Content-Type">>, <<"application/javascript\r\nSet-Cookie: foo=bar">>}
		])
	).

content_type_from_tags(Tags) ->
	ar_http_util:get_tx_content_type(#tx { tags = Tags }).
