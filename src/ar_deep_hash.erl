-module(ar_deep_hash).
-export([hash/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

hash(List) when is_list(List) -> hash_bin_or_list(List).

%%% INTERNAL

hash_bin_or_list(Bin) when is_binary(Bin) ->
	TaggedBin = <<"blob", (integer_to_binary(byte_size(Bin)))/binary, Bin/binary>>,
	crypto:hash(?DEEP_HASH_ALG, TaggedBin);
hash_bin_or_list(List) when is_list(List) ->
	Tag = <<"list", (integer_to_binary(length(List)))/binary>>,
	Acc = crypto:hash(?DEEP_HASH_ALG, Tag),
	hash_list(List, Acc).

hash_list([], Acc) ->
	Acc;
hash_list([Head | List], Acc) ->
	HashPair = <<Acc/binary, (hash_bin_or_list(Head))/binary>>,
	NewAcc = crypto:hash(?DEEP_HASH_ALG, HashPair),
	hash_list(List, NewAcc).


%%% TESTS

hash_test() ->
	V1 = crypto:strong_rand_bytes(32),
	V2 = crypto:strong_rand_bytes(32),
	V3 = crypto:strong_rand_bytes(32),
	V4 = crypto:strong_rand_bytes(32),
	DeepList = [V1, [V2, V3], V4],
	H1 = test_hash(<<"blob", "32", V1/binary>>),
	H2 = test_hash(<<"blob", "32", V2/binary>>),
	H3 = test_hash(<<"blob", "32", V3/binary>>),
	H4 = test_hash(<<"blob", "32", V4/binary>>),
	HSublistTag = test_hash(<<"list", "2">>),
	HSublistHead = test_hash(<<HSublistTag/binary, H2/binary>>),
	HSublist = test_hash(<<HSublistHead/binary, H3/binary>>),
	HListTag = test_hash(<<"list", "3">>),
	HHead = test_hash(<<HListTag/binary, H1/binary>>),
	HWithSublist = test_hash(<<HHead/binary, HSublist/binary>>),
	H = test_hash(<<HWithSublist/binary, H4/binary>>),
	?assertEqual(H, hash(DeepList)).

test_hash(Bin) when is_binary(Bin) ->
	crypto:hash(?DEEP_HASH_ALG, Bin).

hash_empty_list_test() ->
	?assertEqual(test_hash(<<"list", "0">>), hash([])).

hash_uniqueness_test() ->
	?assertNotEqual(
		hash([<<"a">>]),
		hash([[<<"a">>]])
	),
	?assertNotEqual(
		hash([<<"a">>, <<"b">>]),
		hash([<<"b">>, <<"a">>])
	),
	?assertNotEqual(
		hash([<<"a">>, <<>>]),
		hash([<<"a">>])
	),
	?assertNotEqual(
		hash([<<"a">>, <<"b">>]),
		hash([[<<"a">>], <<"b">>])
	),
	?assertNotEqual(
		hash([<<"a">>, [<<"b">>, <<"c">>]]),
		hash([<<"a">>, <<"b">>, <<"c">>])
	),
	?assertNotEqual(
		hash([<<"a">>, [<<"b">>, <<"c">>], [<<"d">>, <<"e">>]]),
		hash([<<"a">>, [<<"b">>, <<"c">>, <<"d">>, <<"e">>]])
	),
	?assertNotEqual(
		hash([<<"a">>, [<<"b">>], <<"c">>, <<"d">>]),
		hash([<<"a">>, [<<"b">>, <<"c">>], <<"d">>])
	).
