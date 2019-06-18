%% @doc These tests are very strongly inspired by Elixir's Base tests.
%% See https://github.com/elixir-lang/elixir/blob/511a51ba8925daa025d3c2fd410e170c1b651013/lib/elixir/test/elixir/base_test.exs
-module(ar_base32_tests).
-include_lib("eunit/include/eunit.hrl").

encode_empty_string_test() ->
	?assertEqual(<<>>, ar_base32:encode(<<>>)).

encode_with_one_pad_test() ->
	?assertEqual(<<"mzxw6yq">>, ar_base32:encode(<<"foob">>)).

encode_with_three_pads_test() ->
	?assertEqual(<<"mzxw6">>, ar_base32:encode(<<"foo">>)).

encode_with_four_pads_test() ->
	?assertEqual(<<"mzxq">>, ar_base32:encode(<<"fo">>)).

encode_with_six_pads_test() ->
	?assertEqual(<<"mzxw6ytboi">>, ar_base32:encode(<<"foobar">>)),
	?assertEqual(<<"my">>, ar_base32:encode(<<"f">>)).

encode_with_no_pads_test() ->
	?assertEqual(<<"mzxw6ytb">>, ar_base32:encode(<<"fooba">>)).
