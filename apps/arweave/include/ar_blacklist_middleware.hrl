-define(THROTTLE_PERIOD, 30000).

-define(BAN_CLEANUP_INTERVAL, 60000).

-define(RPM_BY_PATH(Path), fun() ->
	?RPM_BY_PATH(Path, #{})()
end).

-define(RPM_BY_PATH(Path, LimitByIP), fun() ->
	{ok, Config} = application:get_env(arweave, config),
	?RPM_BY_PATH(Path, LimitByIP, Config#config.requests_per_minute_limit)()
end).

-ifdef(DEBUG).
-define(RPM_BY_PATH(Path, LimitByIP, DefaultPathLimit), fun() ->
	case Path of
		[<<"chunk">> | _] ->
			{chunk,	maps:get(chunk, LimitByIP, 12000)}; % ~50 MB/s.
		[<<"chunk2">> | _] ->
			{chunk,	maps:get(chunk, LimitByIP, 12000)}; % ~50 MB/s.
		[<<"data_sync_record">> | _] ->
			{data_sync_record, maps:get(data_sync_record, LimitByIP, 1000)};
		[<<"recent_hash_list_diff">> | _] ->
			{recent_hash_list_diff,	maps:get(recent_hash_list_diff, LimitByIP, 120)};
		[<<"hash_list">>] ->
			{block_index, maps:get(block_index, LimitByIP, 10)};
		[<<"hash_list2">>] ->
			{block_index, maps:get(block_index, LimitByIP, 10)};
		[<<"block_index">>] ->
			{block_index, maps:get(block_index, LimitByIP, 10)};
		[<<"block_index2">>] ->
			{block_index, maps:get(block_index, LimitByIP, 10)};
		[<<"wallet_list">>] ->
			{wallet_list, maps:get(wallet_list, LimitByIP, 10)};
		[<<"block">>, _Type, _ID, <<"wallet_list">>] ->
			{wallet_list, maps:get(wallet_list, LimitByIP, 60)};
		[<<"block">>, _Type, _ID, <<"hash_list">>] ->
			{block_index, maps:get(block_index, LimitByIP, 10)};
		_ ->
			{default, maps:get(default, LimitByIP, DefaultPathLimit)}
	end
end).
-else.
-define(RPM_BY_PATH(Path, LimitByIP, DefaultPathLimit), fun() ->
	case Path of
		[<<"chunk">> | _] ->
			{chunk,	maps:get(chunk, LimitByIP, 12000)}; % ~50 MB/s.
		[<<"chunk2">> | _] ->
			{chunk,	maps:get(chunk, LimitByIP, 12000)}; % ~50 MB/s.
		[<<"data_sync_record">> | _] ->
			{data_sync_record,	maps:get(data_sync_record, LimitByIP, 40)};
		[<<"recent_hash_list_diff">> | _] ->
			{recent_hash_list_diff,	maps:get(recent_hash_list_diff, LimitByIP, 240)};
		[<<"hash_list">>] ->
			{block_index, maps:get(block_index, LimitByIP, 2)};
		[<<"hash_list2">>] ->
			{block_index, maps:get(block_index, LimitByIP, 2)};
		[<<"block_index">>] ->
			{block_index, maps:get(block_index, LimitByIP, 2)};
		[<<"block_index2">>] ->
			{block_index, maps:get(block_index, LimitByIP, 2)};
		[<<"wallet_list">>] ->
			{wallet_list, maps:get(wallet_list, LimitByIP, 2)};
		[<<"block">>, _Type, _ID, <<"wallet_list">>] ->
			{wallet_list, maps:get(wallet_list, LimitByIP, 2)};
		[<<"block">>, _Type, _ID, <<"hash_list">>] ->
			{block_index, maps:get(block_index, LimitByIP, 2)};
		_ ->
			{default, maps:get(default, LimitByIP, DefaultPathLimit)}
	end
end).
-endif.
