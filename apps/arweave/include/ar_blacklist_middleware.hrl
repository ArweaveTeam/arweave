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
			{data_sync_record,	maps:get(data_sync_record, LimitByIP, 400)};
		[<<"recent_hash_list_diff">> | _] ->
			{recent_hash_list_diff,	maps:get(recent_hash_list_diff, LimitByIP, 60)};
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
			{recent_hash_list_diff,	maps:get(recent_hash_list_diff, LimitByIP, 60)};
		_ ->
			{default, maps:get(default, LimitByIP, DefaultPathLimit)}
	end
end).
-endif.
