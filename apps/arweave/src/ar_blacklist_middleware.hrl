-define(THROTTLE_PERIOD, 30000).

-define(BAN_CLEANUP_INTERVAL, 60000).

-ifdef(DEBUG).
-define(RPM_BY_PATH(Path), fun() ->
	case Path of
		[<<"chunk">> | _]            -> {chunk,            6000};
		[<<"data_sync_record">> | _] -> {data_sync_record, 50};
		_ ->                            {default,          900}
	end
end).
-else.
-define(RPM_BY_PATH(Path), fun() ->
	case Path of
		[<<"chunk">> | _]            -> {chunk,            12000}; % sufficient to upload 50 MB/s
		[<<"data_sync_record">> | _] -> {data_sync_record, 20};
		_ ->                            {default,          900}
	end
end).
-endif.
