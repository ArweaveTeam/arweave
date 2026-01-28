%%%===================================================================
%%% @doc a module to mock `inet'.
%%% @end
%%%===================================================================
-module(ar_test_inet_mock).
-export([getaddrs/2]).

%%--------------------------------------------------------------------
%% @doc a function to mock `inet:getaddrs/2'. mostly used to test
%% internal resolver feature in `ar_peers' and `ar_util'.
%% @end
%%--------------------------------------------------------------------
getaddrs("single.record.local", _) ->
	{ok, [{127, 0, 0, 1}]};
getaddrs("multi.record.local", _) ->
	{ok, [
		{127,0,0,2},
		{127,0,0,3},
		{127,0,0,4},
		{127,0,0,5}
	]};
getaddrs("error.record.local", _) ->
	{error, not_found};
getaddrs(_, _) ->
	{error, invalid}.

