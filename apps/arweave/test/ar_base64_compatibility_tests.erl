-module(ar_base64_compatibility_tests).

%%% The compatibility tests to assert the used
%%% Base64URL encoding and decoding functions are
%%% compatible with base64url 1.0.1.

-include_lib("eunit/include/eunit.hrl").

back_to_back_encode_test() ->
	Inputs = [
		42,
		foo,
		"foo",
		{foo},
		<< "zany" >>,
		<< "zan"  >>,
		<< "za"   >>,
		<< "z"	>>,
		<<		>>,
		binary:copy(<<"0123456789">>, 100000)
	],
	lists:foreach(
		fun(Input) ->
			io:format("Running input: ~p...~n", [Input]),
			assert_encode(Input)
		end,
		Inputs
	).


back_to_back_decode_test_() ->
	{timeout, 10, fun test_back_to_back_decode/0}.

test_back_to_back_decode() ->
	Inputs = [
		42,
		foo,
		"foo",
		{foo},
		<< "." >>,
		<< "+" >>,
		<< "!" >>,
		<< "/" >>,
		<< "Σ">>,
		<< "a" >>,
		<< "aa" >>,
		<< "aaa" >>,
		<< "aaaa" >>,
		<< "aaaaa" >>,
		<< "aaaaaa" >>,
		<< "aaaaaaa" >>,
		<< "aaaaaaaa" >>,
		<< "aaaaaaaaa" >>,
		<< "aaaaaaaaaa" >>,
		<< "!~[]" >>,
		<< "emFueQ==" >>,
		<< "emFu"	 >>,
		<< "emE="	 >>,
		<< "eg=="	 >>,
		<<			>>,
		<< " emFu" >>,
		<< "em Fu" >>,
		<< "emFu " >>,
		<< "	"  >>,
		<< "   ="  >>,
		<< "  =="  >>,
		<< "=   "  >>,
		<< "==  "  >>,
		<< "\temFu">>,
		<< "\tem  F  u">>,
		<< "em  F  \t u">>,
		<< "em  F  \tu">>,
		<< "e\nm\nF\nu\n" >>,
		<< "e\nm\nF\nu" >>,
		<< "e\nm\nF\nu " >>,
		<< "AAAA" >>,
		<< "AAA=" >>,
		<< "AAAA=" >>,
		<< "AAA"  >>,
		<< "AA==" >>,
		<< "AA="  >>,
		<< "AA"   >>,
		<< "A=="  >>,
		<< "A="   >>,
		<< "A"	>>,
		<< "=="   >>,
		<< "="	>>,
		<< "=a"   >>,
		<< "==a"  >>,
		<< "===a" >>,
		<< "====a" >>,
		<< "=====a" >>,
		<< "=======a" >>,
		<< "========a" >>,
		<<		>>,
		<<"PDw/Pz8+Pg==">>,
		<<"PDw:Pz8.Pg==">>,
		binary:copy(<<"a">>, 1000000),
		binary:copy(<<"a">>, 1000001),
		binary:copy(<<"a">>, 1000002),
		binary:copy(<<"a">>, 1000003),
		binary:copy(<<"a">>, 1000004),
		binary:copy(<<"a">>, 1000005),
		binary:copy(<<"a">>, 1000006),
		binary:copy(<<"a">>, 1000007),
		binary:copy(<<"a">>, 1000008),
		binary:copy(<<"a">>, 1000009),
		binary:copy(<<"0123456789">>, 100000),
		binary:copy(<<"0123456789_-">>, 100000)
	],
	lists:foreach(
		fun(Input) ->
			io:format("Running input: ~p...~n", [Input]),
			assert_decode(Input)
		end,
		Inputs
	).

assert_encode(Input) ->
	case catch encode(Input) of
		{'EXIT', {badarg, _}} ->
			?assertException(error, badarg, ar_util:encode(Input));
		{'EXIT', {function_clause, _}} ->
			?assertException(error, badarg, ar_util:encode(Input));
		{'EXIT', {badarith, _}} ->
			?assertException(error, badarg, ar_util:encode(Input));
		Output ->
			?assertEqual(Output, ar_util:encode(Input))
	end.

assert_decode(Input) ->
	case catch decode(Input) of
		{'EXIT', {badarg, _}} ->
			?assertException(error, badarg, ar_util:decode(Input));
		{'EXIT', {function_clause, _}} ->
			?assertException(error, badarg, ar_util:decode(Input));
		{'EXIT', {badarith, _}} ->
			?assertException(error, badarg, ar_util:decode(Input));
		Output ->
			?assertEqual(Output, ar_util:decode(Input))
	end.

encode(Bin) when is_binary(Bin) ->
    << << (urlencode_digit(D)) >> || <<D>> <= base64:encode(Bin), D =/= $= >>;
encode(L) when is_list(L) ->
    encode(iolist_to_binary(L));
encode(_) ->
    error(badarg).

decode(Bin) when is_binary(Bin) ->
    Bin2 = case byte_size(Bin) rem 4 of
        2 -> << Bin/binary, "==" >>;
        3 -> << Bin/binary, "=" >>;
        _ -> Bin
    end,
    base64:decode(<< << (urldecode_digit(D)) >> || <<D>> <= Bin2 >>);
decode(L) when is_list(L) ->
    decode(iolist_to_binary(L));
decode(_) ->
    error(badarg).

urlencode_digit($/) -> $_;
urlencode_digit($+) -> $-;
urlencode_digit(D)  -> D.

urldecode_digit($_) -> $/;
urldecode_digit($-) -> $+;
urldecode_digit(D)  -> D.
