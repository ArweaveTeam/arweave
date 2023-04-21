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

full_chunk_test() ->
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

partial_chunk_test() ->
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

full_chunk_repack_test() ->
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

partial_chunk_repack_test() ->
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

invalid_pad_test() ->
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