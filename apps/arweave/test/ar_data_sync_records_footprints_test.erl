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
	{ok, {Packing, Footprint1}} = ar_http_iface_client:get_footprints(Peer, 0, 0),
	?assertEqual({replica_2_9, Addr}, Packing),
	?assertEqual(ar_intervals:from_list([{1, 0}]), Footprint1),
	{ok, {Packing, Footprint1_1}} = ar_http_iface_client:get_footprints(Peer, 0, 1),
	?assertEqual(ar_intervals:from_list([{4, 3}]), Footprint1_1),
	{ok, {Packing, Footprint1_2}} = ar_http_iface_client:get_footprints(Peer, 0, 2),
	?assertEqual(ar_intervals:from_list([{7, 6}]), Footprint1_2),
	%% The replica 2.9 entropy size configured for tests is only 3 sub-chunks.
	?assertEqual({error, footprint_number_too_large},
			ar_http_iface_client:get_footprints(Peer, 0, 3)),
	?assertEqual({error, negative_footprint_number},
			ar_http_iface_client:get_footprints(Peer, 0, -1)),
	?assertEqual({error, negative_partition_number},
			ar_http_iface_client:get_footprints(Peer, -1, 0)).
