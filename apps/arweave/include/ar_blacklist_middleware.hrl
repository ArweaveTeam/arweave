-define(THROTTLE_PERIOD, 30000).

-define(BAN_CLEANUP_INTERVAL, 60000).

-define(RPM_BY_PATH(Path), fun() ->
	?RPM_BY_PATH(Path, #{})()
end).

-define(RPM_BY_PATH(Path, LimitByIP), fun() ->
	{ok, Config} = arweave_config:get_env(),
	?RPM_BY_PATH(Path, LimitByIP, Config#config.requests_per_minute_limit)()
end).

-ifdef(AR_TEST).
-define(RPM_BY_PATH(Path, LimitByIP, DefaultPathLimit), fun() ->
	case Path of
		[<<"chunk">> | _] ->
			{chunk,	maps:get(chunk, LimitByIP, DefaultPathLimit)}; % ~50 MB/s.
		[<<"chunk2">> | _] ->
			{chunk,	maps:get(chunk, LimitByIP, DefaultPathLimit)}; % ~50 MB/s.
		[<<"data_sync_record">> | _] ->
			{data_sync_record, maps:get(data_sync_record, LimitByIP, DefaultPathLimit)};
		[<<"recent_hash_list_diff">> | _] ->
			{recent_hash_list_diff,	maps:get(recent_hash_list_diff, LimitByIP, DefaultPathLimit)};
		[<<"hash_list">>] ->
			{block_index, maps:get(block_index, LimitByIP, DefaultPathLimit)};
		[<<"hash_list2">>] ->
			{block_index, maps:get(block_index, LimitByIP, DefaultPathLimit)};
		[<<"block_index">>] ->
			{block_index, maps:get(block_index, LimitByIP, DefaultPathLimit)};
		[<<"block_index2">>] ->
			{block_index, maps:get(block_index, LimitByIP, DefaultPathLimit)};
		[<<"wallet_list">>] ->
			{wallet_list, maps:get(wallet_list, LimitByIP, DefaultPathLimit)};
		[<<"block">>, _Type, _ID, <<"wallet_list">>] ->
			{wallet_list, maps:get(wallet_list, LimitByIP, DefaultPathLimit)};
		[<<"block">>, _Type, _ID, <<"hash_list">>] ->
			{block_index, maps:get(block_index, LimitByIP, DefaultPathLimit)};
		[<<"vdf">>] ->
			{get_vdf, maps:get(get_vdf, LimitByIP, DefaultPathLimit)};
		[<<"vdf">>, <<"session">>] ->
			{get_vdf_session, maps:get(get_vdf_session, LimitByIP, DefaultPathLimit)};
		[<<"vdf2">>, <<"session">>] ->
			{get_vdf_session, maps:get(get_vdf_session, LimitByIP, DefaultPathLimit)};
		[<<"vdf3">>, <<"session">>] ->
			{get_vdf_session, maps:get(get_vdf_session, LimitByIP, DefaultPathLimit)};
		[<<"vdf4">>, <<"session">>] ->
			{get_vdf_session, maps:get(get_vdf_session, LimitByIP, DefaultPathLimit)};
		[<<"vdf">>, <<"previous_session">>] ->
			{get_previous_vdf_session, maps:get(get_previous_vdf_session, LimitByIP, DefaultPathLimit)};
		[<<"vdf2">>, <<"previous_session">>] ->
			{get_previous_vdf_session, maps:get(get_previous_vdf_session, LimitByIP, DefaultPathLimit)};
		[<<"vdf4">>, <<"previous_session">>] ->
			{get_previous_vdf_session, maps:get(get_previous_vdf_session, LimitByIP, DefaultPathLimit)};
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
		[<<"footprints">> | _] ->
			%% 262144 * 1024 (chunks per footprint) * 200 (rpm) / 60 (seconds) =~ 800 MB/s
			{footprints,	maps:get(footprints, LimitByIP, 200)};
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
		[<<"vdf">>] ->
			{get_vdf, maps:get(get_vdf, LimitByIP, 180)};
		[<<"vdf">>, <<"session">>] ->
			{get_vdf_session, maps:get(get_vdf_session, LimitByIP, 60)};
		[<<"vdf2">>, <<"session">>] ->
			{get_vdf_session, maps:get(get_vdf_session, LimitByIP, 60)};
		[<<"vdf3">>, <<"session">>] ->
			{get_vdf_session, maps:get(get_vdf_session, LimitByIP, 60)};
		[<<"vdf4">>, <<"session">>] ->
			{get_vdf_session, maps:get(get_vdf_session, LimitByIP, 60)};
		[<<"vdf">>, <<"previous_session">>] ->
			{get_previous_vdf_session, maps:get(get_previous_vdf_session, LimitByIP, 60)};
		[<<"vdf2">>, <<"previous_session">>] ->
			{get_previous_vdf_session, maps:get(get_previous_vdf_session, LimitByIP, 60)};
		[<<"vdf4">>, <<"previous_session">>] ->
			{get_previous_vdf_session, maps:get(get_previous_vdf_session, LimitByIP, 60)};
		_ ->
			{default, maps:get(default, LimitByIP, DefaultPathLimit)}
	end
end).
-endif.
