-module(ar_p3_tests).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(ar_test_node, [start/0, master_peer/0, slave_peer/0]).


basic_test() ->
	Response = ar_http:req(#{
		method => get,
		peer => slave_peer(),
		path => "/info"
	}),
	?debugFmt("Response: ~p", [Response]).