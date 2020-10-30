-define(THROTTLE_PERIOD, 30000).

-define(BAN_CLEANUP_INTERVAL, 60000).

-define(RPM_BY_PATH(Path), fun() ->
	DefaultPathLimit =
		case ar_meta_db:get(requests_per_minute_limit) of
			not_found ->
				900;
			Limit ->
				Limit
		end,
	?RPM_BY_PATH(Path, DefaultPathLimit)()
end).

-ifdef(DEBUG).
-define(RPM_BY_PATH(Path, DefaultPathLimit), fun() ->
	case Path of
		[<<"chunk">> | _]            -> {chunk,            6000};
		[<<"data_sync_record">> | _] -> {data_sync_record, 50};
		_ ->                            {default,          DefaultPathLimit}
	end
end).
-else.
-define(RPM_BY_PATH(Path, DefaultPathLimit), fun() ->
	case Path of
		[<<"chunk">> | _]            -> {chunk,            12000}; % sufficient to upload 50 MB/s
		[<<"data_sync_record">> | _] -> {data_sync_record, 20};
		_ ->                            {default,          DefaultPathLimit}
	end
end).
-endif.
