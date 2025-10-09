-module(ar_data_sync_recovers_from_corruption_test).

-include_lib("eunit/include/eunit.hrl").

-include("../include/ar.hrl").
-include("../include/ar_consensus.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-import(ar_test_node, [assert_wait_until_height/2]).

recovers_from_corruption_test_() ->
	{timeout, 300, fun test_recovers_from_corruption/0}.

test_recovers_from_corruption() ->
	?LOG_DEBUG([{event, test_recovers_from_corruption_start}]),
	ar_test_data_sync:setup_nodes(),
	StoreID = ar_storage_module:id(hd(ar_storage_module:get_all(262144 * 3))),
	?debugFmt("Corrupting ~s...", [StoreID]),
	[ar_chunk_storage:write_chunk(PaddedEndOffset, << 0:(262144*8) >>, #{}, StoreID)
			|| PaddedEndOffset <- lists:seq(262144, 262144 * 3, 262144)],
	ar_test_node:mine(),
	ar_test_node:assert_wait_until_height(main, 1). 