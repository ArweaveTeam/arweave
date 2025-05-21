-module(ar_data_sync_records_footprints_test).

-include_lib("eunit/include/eunit.hrl").

-include("../include/ar.hrl").
-include("../include/ar_consensus.hrl").

records_footprints_test_() ->
	{timeout, 120, fun test_records_footprints/0}.

test_records_footprints() ->
	Wallet = ar_wallet:new_keyfile(),
	Addr = ar_wallet:to_address(Wallet),
	[B0] = ar_weave:init([{Addr, ?AR(1000), <<>>}]),
	ar_test_node:start(#{
		b0 => B0,
		addr => Addr,
		storage_modules => [
			{262144 * 3, 0, {replica_2_9, Addr}}
		]
	}),
	Peer = ar_test_node:peer_ip(main),
	%% The partition 1 is not configured.
	?assertEqual(not_found, ar_http_iface_client:get_footprints(Peer, 1, 0)),
	{ok, Footprint1} = ar_http_iface_client:get_footprints(Peer, 0, 0),
	?assertEqual(ar_intervals:from_list([{2, 0}]), Footprint1),
	{ok, Footprint1_1} = ar_http_iface_client:get_footprints(Peer, 0, 1),
	?assertEqual(ar_intervals:from_list([{5, 4}]), Footprint1_1),
	%% We have 2 footprints with 4 chunks in each in partition 0.
	?assertEqual({error, footprint_number_too_large},
			ar_http_iface_client:get_footprints(Peer, 0, 2)),
	?assertEqual({error, negative_footprint_number},
			ar_http_iface_client:get_footprints(Peer, 0, -1)),
	?assertEqual({error, negative_partition_number},
			ar_http_iface_client:get_footprints(Peer, -1, 0)).
