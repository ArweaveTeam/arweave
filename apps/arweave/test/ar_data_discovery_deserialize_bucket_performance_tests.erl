-module(ar_data_discovery_deserialize_bucket_performance_tests).
-test_peers([peer1]).

-include_lib("eunit/include/eunit.hrl").

-include("ar.hrl").
-include("ar_data_discovery.hrl").
-include("ar_sync_buckets.hrl").


max_bucket_advertisement_performance_test_() ->
	Mocks = [
		{ar_node, get_weave_size,
				% 1000 TB
				fun() -> 1000_000_000_000_000 end},
		{ar_sync_buckets, get_default_sync_bucket_size,
				fun() -> 10_000_000_000 end},
		{ar_sync_buckets, get_network_data_bucket_size,
				fun() -> 10_000_000_000 end},
		{ar_sync_buckets, get_network_footprint_bucket_size,
				fun() -> 37888 end}
	],
	{setup, fun setup_nodes/0, fun cleanup_nodes/1, [
		ar_test_node:test_with_all_nodes_mocked(Mocks, fun max_sync_buckets/0),
		ar_test_node:test_with_all_nodes_mocked(Mocks, fun max_footprint_buckets/0)
	]}.

setup_nodes() ->
	{MainAddr, PeerAddr, B0} = make_genesis(),
	ar_test_node:start(#{
		b0 => B0,
		addr => MainAddr,
		config => #{[storage_modules] => []}
	}),
	ar_test_node:start_peer(peer1, #{
		b0 => B0,
		addr => PeerAddr,
		config => #{[storage_modules] => []}
	}).

cleanup_nodes(_) ->
	catch ar_test_node:remote_call(peer1, ar_test_util, unmock_module,
			[ar_global_sync_record]),
	cleanup_tables().

make_genesis() ->
	MainWallet = ar_wallet:new_keyfile(),
	PeerWallet = ar_wallet:new_keyfile(),
	MainAddr = ar_wallet:to_address(MainWallet),
	PeerAddr = ar_wallet:to_address(PeerWallet),
	[B0] = ar_weave:init([
		{MainAddr, ?AR(1000), <<>>},
		{PeerAddr, ?AR(1000), <<>>}
	]),
	{MainAddr, PeerAddr, B0}.

max_sync_buckets() ->
	run_test(sync).

max_footprint_buckets() ->
	run_test(footprint).

run_test(EndpointType) ->
	#{
		expected_bucket_size := ExpectedBucketSize,
		endpoint := Endpoint,
		mocked_function := MockedFunction,
		table := Table,
		cast_tag := CastTag
	} = endpoint_spec(EndpointType),
	{BucketCount, SerializedBuckets} = generate_max_bucket_payload(ExpectedBucketSize),
	BucketSize = ExpectedBucketSize * ?MAX_SYNC_BUCKET_SIZE_RATIO,
	UnboundedExpandedBucketCount = BucketCount * ?MAX_SYNC_BUCKET_SIZE_RATIO,
	%% Iteration is bounded by the current weave size: a peer's payload can
	%% only expand into sub-buckets that fall within the weave we know about.
	ExpectedRows = expected_inserted_rows(EndpointType, UnboundedExpandedBucketCount),
	install_bucket_mock(MockedFunction, SerializedBuckets),
	Peer = ar_test_node:peer_ip(peer1),
	ets:delete_all_objects(Table),
	{FetchMs, {ok, Buckets}} = timer:tc(ar_http_iface_client, Endpoint, [Peer]),
	{BeforeTableSize, BeforeTableMemoryWords, BeforeEtsMemory, BeforeTotalMemory, BeforeRSS} =
		memory_snapshot(Table),
	{InsertMs, ok} = timer:tc(fun() ->
		gen_server:cast(ar_data_discovery, {CastTag, Peer, Buckets}),
		_ = sys:get_state(ar_data_discovery, infinity),
		ok
	end),
	{AfterTableSize, AfterTableMemoryWords, AfterEtsMemory, AfterTotalMemory, AfterRSS} =
		memory_snapshot(Table),
	RowsInserted = AfterTableSize - BeforeTableSize,
	MemoryWords = AfterTableMemoryWords - BeforeTableMemoryWords,
	WordSize = erlang:system_info(wordsize),
	MemoryBytes = MemoryWords * WordSize,
	EtsMemoryBytes = AfterEtsMemory - BeforeEtsMemory,
	TotalMemoryBytes = AfterTotalMemory - BeforeTotalMemory,
	RSSBytes = memory_delta(AfterRSS, BeforeRSS),
	print_measurement(#{
		endpoint_type => EndpointType,
		serialized_bytes => byte_size(SerializedBuckets),
		bucket_size => BucketSize,
		coarse_bucket_count => BucketCount,
		expanded_bucket_count => UnboundedExpandedBucketCount,
		expected_rows => ExpectedRows,
		fetch_decode_us => FetchMs,
		insert_us => InsertMs,
		rows_inserted => RowsInserted,
		ets_memory_words => MemoryWords,
		ets_memory_bytes => MemoryBytes,
		erlang_ets_memory_bytes => EtsMemoryBytes,
		total_memory_bytes => TotalMemoryBytes,
		rss_bytes => RSSBytes
	}),
	?assertEqual(ExpectedRows, RowsInserted),
	ets:delete_all_objects(Table).

expected_inserted_rows(sync, UnboundedExpandedBucketCount) ->
	WeaveSize = ar_node:get_weave_size(),
	BucketSize = ar_sync_buckets:get_network_data_bucket_size(),
	MaxSubBucketExclusive = (WeaveSize + BucketSize - 1) div BucketSize,
	min(UnboundedExpandedBucketCount, MaxSubBucketExclusive);
expected_inserted_rows(footprint, UnboundedExpandedBucketCount) ->
	WeaveSize = ar_node:get_weave_size(),
	MaxFootprintOffset = ar_footprint_record:max_offset(WeaveSize),
	BucketSize = ar_sync_buckets:get_network_footprint_bucket_size(),
	MaxSubBucketExclusive =
		(MaxFootprintOffset + BucketSize - 1) div BucketSize,
	min(UnboundedExpandedBucketCount, MaxSubBucketExclusive).

endpoint_spec(sync) ->
	#{
		expected_bucket_size => ar_sync_buckets:get_default_sync_bucket_size(),
		endpoint => get_sync_buckets,
		mocked_function => get_serialized_sync_buckets,
		table => ar_data_discovery,
		cast_tag => add_peer_sync_buckets
	};
endpoint_spec(footprint) ->
	#{
		expected_bucket_size =>
				ar_sync_buckets:get_network_footprint_bucket_size(),
		endpoint => get_footprint_buckets,
		mocked_function => get_serialized_footprint_buckets,
		table => ar_data_discovery_footprint_buckets,
		cast_tag => add_peer_footprint_buckets
	}.

install_bucket_mock(Function, SerializedBuckets) ->
	ok = ar_test_node:remote_call(peer1, ar_test_util, new_mock,
			[ar_global_sync_record, [no_link, passthrough]]),
	ok = ar_test_node:remote_call(peer1, ar_test_util, mock_function,
			[ar_global_sync_record, Function, fun() -> {ok, SerializedBuckets} end]).

generate_max_bucket_payload(ExpectedBucketSize) ->
	BucketSize = ExpectedBucketSize * ?MAX_SYNC_BUCKET_SIZE_RATIO,
	Count = get_max_bucket_count(BucketSize, ?MAX_SYNC_BUCKETS_SIZE),
	SerializedBuckets = serialize_buckets(BucketSize, Count),
	{ok, _} = ar_sync_buckets:deserialize(SerializedBuckets, ExpectedBucketSize),
	{Count, SerializedBuckets}.

get_max_bucket_count(BucketSize, MaxSerializedSize) ->
	get_max_bucket_count(BucketSize, MaxSerializedSize, 0, MaxSerializedSize + 1).

get_max_bucket_count(_BucketSize, _MaxSerializedSize, Low, High) when Low + 1 >= High ->
	Low;
get_max_bucket_count(BucketSize, MaxSerializedSize, Low, High) ->
	Mid = (Low + High) div 2,
	case byte_size(serialize_buckets(BucketSize, Mid)) =< MaxSerializedSize of
		true ->
			get_max_bucket_count(BucketSize, MaxSerializedSize, Mid, High);
		false ->
			get_max_bucket_count(BucketSize, MaxSerializedSize, Low, Mid)
	end.

serialize_buckets(BucketSize, Count) ->
	term_to_binary({BucketSize, maps:from_list([{N, 1} || N <- lists:seq(0, Count - 1)])}).

memory_snapshot(Table) ->
	{ets:info(Table, size), ets:info(Table, memory), erlang:memory(ets),
			erlang:memory(total), rss_bytes()}.

rss_bytes() ->
	case catch list_to_integer(string:trim(os:cmd("ps -o rss= -p " ++ os:getpid()))) of
		RSSKilobytes when is_integer(RSSKilobytes) ->
			RSSKilobytes * 1024;
		_ ->
			unknown
	end.

memory_delta(unknown, _) ->
	unknown;
memory_delta(_, unknown) ->
	unknown;
memory_delta(After, Before) ->
	After - Before.

print_measurement(Result) ->
	io:format(user,
		"~nmax_bucket_measurement"
		" endpoint_type=~p"
		" serialized_bytes=~B"
		" bucket_size=~B"
		" coarse_bucket_count=~B"
		" expanded_bucket_count=~B"
		" expected_rows=~B"
		" fetch_decode_ms=~.3f"
		" insert_ms=~.3f"
		" rows_inserted=~B"
		" ets_memory_words=~B"
		" ets_memory_bytes=~B"
		" erlang_ets_memory_bytes_delta=~B"
		" total_memory_bytes_delta=~B"
		" rss_bytes_delta=~p~n",
		[
			maps:get(endpoint_type, Result),
			maps:get(serialized_bytes, Result),
			maps:get(bucket_size, Result),
			maps:get(coarse_bucket_count, Result),
			maps:get(expanded_bucket_count, Result),
			maps:get(expected_rows, Result),
			maps:get(fetch_decode_us, Result) / 1000,
			maps:get(insert_us, Result) / 1000,
			maps:get(rows_inserted, Result),
			maps:get(ets_memory_words, Result),
			maps:get(ets_memory_bytes, Result),
			maps:get(erlang_ets_memory_bytes, Result),
			maps:get(total_memory_bytes, Result),
			maps:get(rss_bytes, Result)
		]).

cleanup_tables() ->
	ets:delete_all_objects(ar_data_discovery),
	ets:delete_all_objects(ar_data_discovery_footprint_buckets).
