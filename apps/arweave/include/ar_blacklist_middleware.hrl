-define(THROTTLE_PERIOD, 30000).

-define(BAN_CLEANUP_INTERVAL, 60000).

-define(RPM_BY_PATH(Path), fun() ->
	DefaultPathLimit =
		case ar_meta_db:get(requests_per_minute_limit) of
			not_found ->
				?DEFAULT_REQUESTS_PER_MINUTE_LIMIT;
			Limit ->
				Limit
		end,
	?RPM_BY_PATH(Path, DefaultPathLimit)()
end).

-ifdef(DEBUG).
-define(RPM_BY_PATH(Path, DefaultPathLimit), fun() ->
	case Path of
		[<<"chunk">> | _]            -> {chunk,            120000};
		[<<"data_sync_record">> | _] -> {data_sync_record, 200};
		_ ->                            {default,          DefaultPathLimit}
	end
end).
-else.
-define(RPM_BY_PATH(Path, DefaultPathLimit), fun() ->
	case Path of
		[<<"chunk">> | _]            -> {chunk,            12000}; % sufficient to upload 50 MB/s
		[<<"data_sync_record">> | _] -> {data_sync_record, 40};
		_ ->                            {default,          DefaultPathLimit}
	end
end).
-endif.
