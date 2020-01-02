-module(ar_deep_hash).
-export([hash/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

hash(List) when is_list(List) -> hash_bin_or_list(List).

%%% INTERNAL

hash_bin_or_list(Bin) when is_binary(Bin) ->
	Tag = <<"blob", (integer_to_binary(byte_size(Bin)))/binary>>,
	hash_bin(<<(hash_bin(Tag))/binary, (hash_bin(Bin))/binary>>);
hash_bin_or_list(List) when is_list(List) ->
	Tag = <<"list", (integer_to_binary(length(List)))/binary>>,
	block_index(List, hash_bin(Tag)).

block_index([], Acc) ->
	Acc;
block_index([Head | List], Acc) ->
	HashPair = <<Acc/binary, (hash_bin_or_list(Head))/binary>>,
	NewAcc = hash_bin(HashPair),
	block_index(List, NewAcc).

hash_bin(Bin) when is_binary(Bin) ->
	crypto:hash(?DEEP_HASH_ALG, Bin).


%%% TESTS

hash_test() ->
	V1 = crypto:strong_rand_bytes(32),
	V2 = crypto:strong_rand_bytes(32),
	V3 = crypto:strong_rand_bytes(32),
	V4 = crypto:strong_rand_bytes(32),
	DeepList = [V1, [V2, V3], V4],
	H1 = hash_bin(<<(hash_bin(<<"blob", "32">>))/binary, (hash_bin(V1))/binary>>),
	H2 = hash_bin(<<(hash_bin(<<"blob", "32">>))/binary, (hash_bin(V2))/binary>>),
	H3 = hash_bin(<<(hash_bin(<<"blob", "32">>))/binary, (hash_bin(V3))/binary>>),
	H4 = hash_bin(<<(hash_bin(<<"blob", "32">>))/binary, (hash_bin(V4))/binary>>),
	HSublistTag = hash_bin(<<"list", "2">>),
	HSublistHead = hash_bin(<<HSublistTag/binary, H2/binary>>),
	HSublist = hash_bin(<<HSublistHead/binary, H3/binary>>),
	HListTag = hash_bin(<<"list", "3">>),
	HHead = hash_bin(<<HListTag/binary, H1/binary>>),
	HWithSublist = hash_bin(<<HHead/binary, HSublist/binary>>),
	H = hash_bin(<<HWithSublist/binary, H4/binary>>),
	?assertEqual(H, hash(DeepList)).

hash_empty_list_test() ->
	?assertEqual(hash_bin(<<"list", "0">>), hash([])).

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
