-module(ar_packing_tests).

-include_lib("arweave/include/ar.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(CHUNK_OFFSET, 10*256*1024).
-define(ENCODED_TX_ROOT, <<"9d857DmXbSyhX6bgF7CDMDCl0f__RUjryMMvueFN9wE">>).
-define(ENCODED_REWARD_ADDRESS, <<"usuW8f-hpzuA4ZFXxiTbv0OXRWZP2HUlJqP-bGMR8g8">>).

load_fixture(Fixture) ->
	Dir = filename:dirname(?FILE),
	FixtureDir = filename:join(Dir, "fixtures/chunks"),
	FixtureFilename = filename:join(FixtureDir, Fixture),
	{ok, Data} = file:read_file(FixtureFilename),
	Data.

% request_test() ->
% 	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

% 	[B0] = ar_weave:init([]),
% 	ar_test_node:start(B0, RewardAddress),

% 	test_full_chunk(),
% 	test_partial_chunk(),
% 	test_full_chunk_repack(),
% 	test_partial_chunk_repack(),
% 	test_invalid_pad(),
% 	test_request_repack(RewardAddress),
% 	test_request_unpack(RewardAddress).

packing_test_() ->
    {setup, 
     fun setup/0, 
     fun teardown/1, 
     [fun test_full_chunk/0,
      fun test_partial_chunk/0,
      fun test_full_chunk_repack/0,
      fun test_partial_chunk_repack/0,
      fun test_invalid_pad/0,
      fun test_request_repack/0,
      fun test_request_unpack/0]}.

setup() ->
    RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),
    [B0] = ar_weave:init([]),
    ar_test_node:start(B0, RewardAddress),
    RewardAddress.

teardown(_) ->
    % optional cleanup code
    ok.

test_full_chunk() ->
	UnpackedData = load_fixture("unpacked.256kb"),
	Spora25Data = load_fixture("spora25.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:pack(
			unpacked, ?CHUNK_OFFSET, TXRoot, UnpackedData)),
	?assertEqual(
		{ok, Spora25Data},
		ar_packing_server:pack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, UnpackedData)),
	?assertEqual(
		{ok, Spora26Data},
		ar_packing_server:pack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, UnpackedData)),

	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			unpacked, ?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)).

test_partial_chunk() ->
	UnpackedData = load_fixture("unpacked.100kb"),
	Spora25Data = load_fixture("spora25.100kb"),
	Spora26Data = load_fixture("spora26.100kb"),

	ChunkSize = 100*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:pack(
			unpacked, ?CHUNK_OFFSET, TXRoot, UnpackedData)),
	?assertEqual(
		{ok, Spora25Data},
		ar_packing_server:pack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, UnpackedData)),
	?assertEqual(
		{ok, Spora26Data},
		ar_packing_server:pack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, UnpackedData)),

	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			unpacked, ?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData},
		ar_packing_server:unpack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)).

test_full_chunk_repack() ->
	UnpackedData = load_fixture("unpacked.256kb"),
	Spora25Data = load_fixture("spora25.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, unpacked,
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, UnpackedData},
		ar_packing_server:repack(spora_2_5, unpacked, 
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, UnpackedData},
		ar_packing_server:repack({spora_2_6, RewardAddress}, unpacked, 
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, spora_2_5, 
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, none},
		ar_packing_server:repack(spora_2_5, spora_2_5, 
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, UnpackedData},
		ar_packing_server:repack({spora_2_6, RewardAddress}, spora_2_5, 
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, {spora_2_6, RewardAddress}, 
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, UnpackedData},
		ar_packing_server:repack(spora_2_5, {spora_2_6, RewardAddress}, 
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, none},
		ar_packing_server:repack({spora_2_6, RewardAddress}, {spora_2_6, RewardAddress},
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)).

test_partial_chunk_repack() ->
	UnpackedData = load_fixture("unpacked.100kb"),
	Spora25Data = load_fixture("spora25.100kb"),
	Spora26Data = load_fixture("spora26.100kb"),

	ChunkSize = 100*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, unpacked,
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, UnpackedData},
		ar_packing_server:repack(spora_2_5, unpacked,
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, UnpackedData},
		ar_packing_server:repack({spora_2_6, RewardAddress}, unpacked,
			?CHUNK_OFFSET, TXRoot, UnpackedData, ChunkSize)),

	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, spora_2_5,
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, none},
		ar_packing_server:repack(spora_2_5, spora_2_5,
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, UnpackedData},
		ar_packing_server:repack({spora_2_6, RewardAddress}, spora_2_5,
			?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize)),
	?assertEqual(
		{ok, UnpackedData, UnpackedData},
		ar_packing_server:repack(unpacked, {spora_2_6, RewardAddress},
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)),
	?assertEqual(
		{ok, Spora25Data, UnpackedData},
		ar_packing_server:repack(spora_2_5, {spora_2_6, RewardAddress},
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)),
	?assertEqual(
		{ok, Spora26Data, none},
		ar_packing_server:repack({spora_2_6, RewardAddress}, {spora_2_6, RewardAddress},
			?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize)).

test_invalid_pad() ->
	ChunkSize = 100*1024,

	UnpackedData = load_fixture("unpacked.256kb"),
	Spora25Data = load_fixture("spora25.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ShortUnpackedData = binary:part(UnpackedData, 0, ChunkSize),

	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

	?assertEqual(
		{ok, ShortUnpackedData},
		ar_packing_server:unpack(
			spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
		"We don't check the pad when unpacking SPoRA 2.5"),
	?assertEqual(
		{error, invalid_padding},
		ar_packing_server:unpack(
			{spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora26Data, ChunkSize),
			"We do check the pad when unpacking SPoRA 2.6"),
	?assertEqual(
		{ok, ShortUnpackedData, ShortUnpackedData},
		ar_packing_server:repack(
			unpacked, spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
			"We don't check the pad when repacking from SPoRA 2.5"),
	?assertMatch(
		{ok, _, ShortUnpackedData},
		ar_packing_server:repack(
			{spora_2_6, RewardAddress}, spora_2_5, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
			"We don't check the pad when repacking from SPoRA 2.5"),
	?assertEqual(
		{error, invalid_padding},
		ar_packing_server:repack(
			unpacked, {spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
			"We do check the pad when repacking from SPoRA 2.6"),
	?assertMatch(
		{error, invalid_padding},
		ar_packing_server:repack(
			spora_2_5, {spora_2_6, RewardAddress}, ?CHUNK_OFFSET, TXRoot, Spora25Data, ChunkSize),
			"We do check the pad when repacking from SPoRA 2.6").

test_request_repack() ->
	UnpackedData = load_fixture("unpacked.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

	%% unpacked -> unpacked
	ar_packing_server:request_repack(?CHUNK_OFFSET, {
		unpacked,
		unpacked, UnpackedData,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {packed, _, {unpacked, Unpacked1, _, _, _}}} ->
            ?assertEqual(UnpackedData, Unpacked1)
    after 5000 -> 
        erlang:error(timeout)
    end,
	%% unpacked -> packed
	ar_packing_server:request_repack(?CHUNK_OFFSET, {
		{spora_2_6, RewardAddress},
		unpacked, UnpackedData,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {packed, _, {{spora_2_6, RewardAddress}, Packed, _, _, _}}} ->
            ?assertEqual(Spora26Data, Packed)
    after 5000 -> 
        erlang:error(timeout)
    end,
	%% packed -> unpacked
	ar_packing_server:request_repack(?CHUNK_OFFSET, {
		unpacked,
		{spora_2_6, RewardAddress}, Spora26Data,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {packed, _, {unpacked, Unpacked2, _, _, _}}} ->
            ?assertEqual(UnpackedData, Unpacked2)
    after 5000 -> 
        erlang:error(timeout)
    end,
	%% packed -> packed
	ar_packing_server:request_repack(?CHUNK_OFFSET, {
		{spora_2_6, RewardAddress},
		{spora_2_6, RewardAddress}, Spora26Data,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {packed, _, {{spora_2_6, RewardAddress}, Packed2, _, _, _}}} ->
            ?assertEqual(Spora26Data, Packed2)
    after 5000 -> 
        erlang:error(timeout)
    end.

test_request_unpack() ->
	UnpackedData = load_fixture("unpacked.256kb"),
	Spora26Data = load_fixture("spora26.256kb"),

	ChunkSize = 256*1024,
	TXRoot = ar_util:decode(?ENCODED_TX_ROOT),
	RewardAddress = ar_util:decode(?ENCODED_REWARD_ADDRESS),

	%% unpacked -> unpacked
	ar_packing_server:request_unpack(?CHUNK_OFFSET, {
		unpacked,
		UnpackedData,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {unpacked, _, {unpacked, Unpacked1, _, _, _}}} ->
            ?assertEqual(UnpackedData, Unpacked1)
    after 5000 -> 
        erlang:error(timeout)
    end,
	%% packed -> unpacked
	ar_packing_server:request_unpack(?CHUNK_OFFSET, {
		{spora_2_6, RewardAddress}, Spora26Data,
		?CHUNK_OFFSET, TXRoot, ChunkSize}),
	receive
        {chunk, {unpacked, _, {{spora_2_6, RewardAddress}, Unpacked2, _, _, _}}} ->
            ?assertEqual(UnpackedData, Unpacked2)
    after 5000 -> 
        erlang:error(timeout)
    end.