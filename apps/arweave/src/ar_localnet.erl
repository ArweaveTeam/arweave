-module(ar_localnet).

-export([start/0, start/1, submit_snapshot_data/0, format_packing/1,
		mine_one_block/0, mine_until_height/1, create_snapshot/0]).

-include("ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("kernel/include/file.hrl").

-define(WAIT_UNTIL_JOINED_TIMEOUT, 200_000).
-define(DEFAULT_SNAPSHOT_DIR, "localnet_snapshot").

-ifndef(START_FROM_STATE_SEARCH_DEPTH).
	-define(LOCALNET_START_FROM_STATE_SEARCH_DEPTH, 100).
-else.
	-define(LOCALNET_START_FROM_STATE_SEARCH_DEPTH, ?START_FROM_STATE_SEARCH_DEPTH).
-endif.

-define(LOCALNET_DATA_DIR, ".tmp/data_localnet_main").

%% @doc Start a node from the localnet_snapshot directory.
%% Disable mining (can be triggered by request).
%% Configure a single storage module with the first 20 MiB of partition 0.
%% Seed the storage module with data.
start() ->
	start(#config{
		data_dir = ?LOCALNET_DATA_DIR
	}).

start(SnapshotDir) when is_list(SnapshotDir) ->
	start(#config{
		data_dir = ?LOCALNET_DATA_DIR,
		start_from_state = SnapshotDir
	});
start(SnapshotDir) when is_atom(SnapshotDir) ->
	start(#config{
		data_dir = ?LOCALNET_DATA_DIR,
		start_from_state = atom_to_list(SnapshotDir)
	});
start(Config) ->
	SnapshotDir = snapshot_dir(Config),
	DataDir = Config#config.data_dir,
	arweave_config:start(),
	ok = filelib:ensure_dir(DataDir ++ "/"),
	MiningAddr =
		case Config#config.mining_addr of
			not_set ->
				ar_wallet:to_address(ar_wallet:new_keyfile({?ECDSA_SIGN_ALG, secp256k1}, wallet_address, DataDir));
			Addr ->
				Addr
		end,
	StorageModules =
		case Config#config.storage_modules of
			[] ->
				[{21 * ?MiB, 0, {replica_2_9, MiningAddr}}];
			ConfiguredStorageModules ->
				ConfiguredStorageModules
		end,
	ok = arweave_config:set_env(Config#config{
		mining_addr = MiningAddr,
		storage_modules = StorageModules,
		start_from_latest_state = true,
		start_from_state = SnapshotDir,
		disk_cache_size = 128,
		max_disk_pool_buffer_mb = 128,
		max_disk_pool_data_root_buffer_mb = 128,
		auto_join = true,
		peers = [],
		cm_exit_peer = not_set,
		cm_peers = [],
		local_peers = [],
		mine = false,
		disk_space_check_frequency = 1000,
		sync_jobs = 0,
		disk_pool_jobs = 1,
		header_sync_jobs = 0,
		debug = true
	}),
	ar:start_dependencies(),
	wait_until_joined(),
	submit_snapshot_data(),
	io:format("~n~nLocalnet node started~n"),
	io:format("  Snapshot: ~s~n", [SnapshotDir]),
	io:format("  Data dir: ~s~n", [DataDir]),
	io:format("  Mining address: ~s~n", [ar_util:encode(MiningAddr)]),
	io:format("  Storage modules:~n"),
	lists:foreach(fun({Size, Partition, Packing}) ->
		io:format("    - partition ~B, size ~B MB, packing ~s~n",
			[Partition, Size div (1_000_000), format_packing(Packing)])
	end, StorageModules),
	io:format("~nMining is disabled. Call ar_localnet:mine_one_block/0 to mine a block.~n"
			"Call ar_localnet:mine_until_height/1 to mine until the given height.~n~n").

%% @doc Mine one block.
mine_one_block() ->
	ar_node_worker:mine_one_block().

%% @doc Mine blocks until the given height is reached.
mine_until_height(Height) ->
	ar_node_worker:mine_until_height(Height).

%% @doc Create a reproducible snapshot in localnet_snapshot_[mainnet_starting_height]_[localnet_end_height].
create_snapshot() ->
	{ok, Config} = arweave_config:get_env(),
	case ar_node:is_joined() of
		false ->
			{error, node_not_joined};
		true ->
			SnapshotDir = snapshot_dir(Config),
			case open_snapshot_databases(SnapshotDir) of
				{ok, CloseSnapshotDbs} ->
					SnapshotResult =
						case ar_storage:read_block_index(SnapshotDir) of
							not_found ->
								{error, snapshot_block_index_not_found};
							BI ->
								MainnetStartHeight = length(BI) - 1,
								LocalnetEndHeight = ar_node:get_height(),
								NewSnapshotDir = snapshot_dir_name(MainnetStartHeight, LocalnetEndHeight),
								create_snapshot(SnapshotDir, Config#config.data_dir, NewSnapshotDir)
						end,
					CloseSnapshotDbs(),
					SnapshotResult;
				{error, _} = Error ->
					Error
			end
	end.

wait_until_joined() ->
	ar_util:do_until(
		fun() -> ar_node:is_joined() end,
		100,
		?WAIT_UNTIL_JOINED_TIMEOUT
	 ).

store_snapshot_data2(SnapshotDir) ->
	case ar_node:get_block_index() of
		[] ->
			{error, empty_block_index};
		BI ->
			Height = length(BI) - 1,
			SearchDepth = min(Height, ?LOCALNET_START_FROM_STATE_SEARCH_DEPTH),
			io:format("Copying snapshot data into data_dir from ~s~n", [SnapshotDir]),
			io:format("  Height: ~B, search depth: ~B~n", [Height, SearchDepth]),
			case read_recent_blocks_local(BI, SearchDepth) of
				{Skipped, _Blocks} ->
					io:format("  Local blocks available (skipped: ~B). Skip backfill.~n",
						[Skipped]),
					ok;
				not_found ->
					store_snapshot_data3(BI, Height, SearchDepth, SnapshotDir)
			end
	end.

store_snapshot_data3(BI, Height, SearchDepth, SnapshotDir) ->
	case read_recent_blocks_from_snapshot(BI, SearchDepth, SnapshotDir) of
		not_found ->
			{error, block_headers_not_found};
		{Skipped, Blocks} ->
			io:format("  Recent blocks: ~B (skipped: ~B)~n",
				[length(Blocks), Skipped]),
			BI2 = lists:nthtail(Skipped, BI),
			Height2 = Height - Skipped,
			RewardHistoryBI = reward_history_bi(Height2, BI2),
			BlockTimeHistoryBI = block_time_history_bi(BI2),
			io:format("  Reward history: ~B entries~n", [length(RewardHistoryBI)]),
			io:format("  Block time history: ~B entries~n", [length(BlockTimeHistoryBI)]),
			case store_snapshot_blocks_from_list(Blocks) of
				{ok, TxIds} ->
					io:format("  Tx headers to copy: ~B~n", [length(TxIds)]),
					case store_snapshot_tx_headers(TxIds, SnapshotDir) of
						ok ->
							case store_snapshot_history_entries(
								RewardHistoryBI,
								start_from_state_reward_history_db,
								reward_history_db,
								reward_history
							) of
								ok ->
									case store_snapshot_history_entries(
										BlockTimeHistoryBI,
										start_from_state_block_time_history_db,
										block_time_history_db,
										block_time_history
									) of
										ok ->
											store_snapshot_wallet_list(Blocks, SnapshotDir, SearchDepth);
										{error, _} = Error ->
											Error
									end;
								{error, _} = Error ->
									Error
							end;
						{error, _} = Error ->
							Error
					end;
				{error, _} = Error ->
					Error
			end
	end.

format_packing({spora_2_6, Addr}) ->
	io_lib:format("spora_2_6(~s)", [ar_util:encode(Addr)]);
format_packing({composite, Addr, Diff}) ->
	io_lib:format("composite(~s, ~B)", [ar_util:encode(Addr), Diff]);
format_packing({replica_2_9, Addr}) ->
	io_lib:format("(replica_2_9, ~s)", [ar_util:encode(Addr)]);
format_packing(unpacked) ->
	"unpacked";
format_packing(Other) ->
	io_lib:format("~p", [Other]).

submit_snapshot_data() ->
	{ok, Config} = arweave_config:get_env(),
	SnapshotDir = snapshot_dir(Config),
	io:format("Seeding data from snapshot...~n"),
	SnapshotTXs = snapshot_txs(),
	BlockStarts = lists:sort(maps:keys(SnapshotTXs)),
	{TotalBigChunk, TotalSmallChunk} =
		lists:foldl(
			fun(BlockStart, {AccBigChunk, AccSmallChunk}) ->
				TXIDs = maps:get(BlockStart, SnapshotTXs),
				TXs = lists:map(fun(TXID) ->
					Filename = filename:join([SnapshotDir, "seed_txs",
						binary_to_list(TXID) ++ ".json"]),
					case file:read_file(Filename) of
						{ok, JSON} ->
							Map = jiffy:decode(JSON, [return_maps]),
							#tx{
								id = ar_util:decode(TXID),
								data_size = binary_to_integer(maps:get(<<"data_size">>, Map, <<"0">>)),
								data = ar_util:decode(maps:get(<<"data">>, Map, <<>>)),
								data_root = ar_util:decode(maps:get(<<"data_root">>, Map, <<>>)),
								format = maps:get(<<"format">>, Map, 1)
							};
						{error, Reason} ->
							io:format("Failed to read ~s: ~p~n", [Filename, Reason]),
							error({missing_tx_file, TXID})
					end
				end, TXIDs),
				{BlockBigChunk, BlockSmallChunk} = submit_block_data(BlockStart, TXs),
				{AccBigChunk + BlockBigChunk, AccSmallChunk + BlockSmallChunk}
		end,
		{0, 0},
		BlockStarts
	),
	io:format("Seeding completed. Total size: ~B bytes (~B big chunks, ~B small chunks).~n",
		[TotalBigChunk + TotalSmallChunk, TotalBigChunk, TotalSmallChunk]),
	{TotalBigChunk, TotalSmallChunk}.

submit_block_data(BlockStart, TXs) ->
	{BlockStart, BlockEnd, TXRoot, Height} = ar_block_index:get_block_bounds_with_height(BlockStart),
	SortedTXs = lists:sort(TXs),
	SizeTaggedTXs = ar_block:generate_size_tagged_list_from_txs(SortedTXs, Height),
	SizeTaggedTXsNoPadding = [T || {{ID, _}, _} = T <- SizeTaggedTXs, ID /= padding],
	TXs2 = [TX#tx{ data_root = DR }
		|| {TX, {{_ID, DR}, _End}} <- lists:zip(SortedTXs, SizeTaggedTXsNoPadding)
	],
	{_, Tree} = ar_merkle:generate_tree([{DR, End} || {{_, DR}, End} <- SizeTaggedTXs]),
	Entries = [
		{DR, TX#tx.data_size, BlockStart + End - TX#tx.data_size,
			ar_merkle:generate_path(TXRoot, End - 1, Tree)}
		|| {TX, {{_ID, DR}, End}} <- lists:zip(TXs2, SizeTaggedTXsNoPadding),
			TX#tx.data_size > 0
	],
	ar_data_root_sync:store_data_roots_sync(BlockStart, BlockEnd, TXRoot, Entries),
	lists:foreach(fun(TX) ->
		case TX#tx.data_size > 0 of
			true ->
				Data = TX#tx.data,
				TXID = TX#tx.id,
				case ar_storage:write_tx_data(TX#tx.data_root, Data, TXID) of
					ok ->
						ok;
					{error, Errors} ->
						io:format("  Failed to write data for tx ~s: ~p~n",
							[binary_to_list(ar_util:encode(TXID)), Errors])
				end;
			false ->
				ok
		end
	end, TXs2),
	lists:foldl(
		fun(TX, {AccBigChunk, AccSmallChunk}) ->
			DataSize = TX#tx.data_size,
			BigChunkSize = (DataSize div ?DATA_CHUNK_SIZE) * ?DATA_CHUNK_SIZE,
			SmallChunkSize = DataSize rem ?DATA_CHUNK_SIZE,
			{AccBigChunk + BigChunkSize, AccSmallChunk + SmallChunkSize}
		end,
		{0, 0},
		TXs2
	).

snapshot_txs() ->
	#{
		0 => [<<"t81tluHdoePSxjq7qG-6TMqBKmQLYr5gupmfvW25Y_o">>],
		599058 => [<<"QAQ-134At0mSPVrwBzTTUalyL_zqE_dMR_WggkZvF5E">>],
		1039029 => [<<"-B7wF8TF5AodemKM2UjeFySwA_-Q12Ai8z9FSqgIEyA">>, <<"vYnzbcbBQbPQB7GKrXzPlz1MuT9cfnNI_NBVajaTnPg">>, <<"Lt7WJclVu4iYHqGHIYIBia3ABMnvmd5cW4ELIzUTfPE">>],
		1074629 => [<<"JfTiLBj5Gxr1v7JwoNf9-7sRAiLOrg1AZ6kqwSkEpTc">>, <<"YMnQwrWWVRmkMs0B41lz-VdixskatlPcY7j4r0iSLbQ">>, <<"dMTZgKHD-NkP3iM5RjFNhppiwfTlYd-Imi9aA6IK0So">>],
		1113021 => [<<"qHvSpQXYh9RZmXIoIOexmDs0iQgjCubl6KSsgg7cDz8">>],
		1179126 => [<<"8TiSScQCv06oS9b8Tt5WBnf7sUVgzPAFGJ3Lq2bt8rY">>],
		1621676 => [<<"U_1PPd40n2grpuhkMJcMXPVuJhtaQoUWei63iN2rS7o">>],
		2057001 => [<<"m1DnUoXf7wMtIGkkDZAALobw0GbGehfEMX_jNLvs3i8">>],
		2507233 => [<<"JUf6alhhrfuL22XuQ0yrZ6_xBFBIQqi85wRxv2nUCMs">>],
		3065027 => [<<"EDt8sO0AWKJyNeUxd-U6ihy0rgRKUPjpfRGarEHlOCs">>],
		3103419 => [<<"UH3C65dDo62rp5ciK3XzyhufE71xorL7r7MWVwdhavk">>],
		3551503 => [<<"FbeSRhJR00VPygimhm47VwirSeBATnlf240hv4a2G4E">>],
		3853182 => [<<"ViCjDXb4IEZcXBtlYvTm3HCB6cf4gDbrXCCdvVVgB1g">>],
		3898843 => [<<"Jo3rf0JPJR2kCHBqZG71xouWzuOSY-MXJufpfzFl7sE">>, <<"Tg9QZvUPJoAZKRkPhPgQrgnlTY6s9UxRSQaMw6shhOU">>],
		3933160 => [<<"uNiZ8TfAQ8GWjtbqhVi90qO3U5dl9afmKE1-KbHQYM4">>],
		4748637 => [<<"ujON59jsellR3M8hq9unBPISOwRgEVUogdi3FG_pVMk">>],
		4913142 => [<<"U_UF7e-hOd5uLIj10fYZVxQ5mXyZUxvMxhWWgAMaj0s">>],
		4984947 => [<<"_AiF52l4uqTkKOVpQw9hr6l6FCIdWs8PCFtFxEBOopU">>],
		5052944 => [<<"Ace-njSprwHMwZaW5nuD0y1lKFoaafU3T8d7PLBeEIA">>],
		5168327 => [<<"UbW68tRQtThl9ah8tJb-X_af5M8FHYARiGZFiPGk_90">>],
		5339549 => [<<"JDS1sGkpC0ua7UGfpLEJSF-jXUnjAs2fa5V7y6rccdY">>, <<"hMfNPSlINViUDVnor18GgPs0Ut0i9XY7dwM9MVOL-2I">>],
		5530545 => [<<"MCCCpl9AGNAzy3WvM5lniJ88iC3-8NPiiWIsxcLZZxQ">>],
		5573275 => [<<"F3c9tsVvmCiFNxK7hVEzROraVm477QdyQ8t6afBs5E4">>],
		5716883 => [<<"uOqsnEjVGQCbtrKI7QbHYxbbLUdCKC-792SgZr5KUKM">>],
		5951779 => [<<"Ie-fxxzdBweiA0N1ZbzUqXhNI310uDUmaBc3ajlV6YY">>, <<"ycjvsn3A9cUMjnbDaSUpf1HRQd4duP9AL1YVwSjwuAQ">>],
		6012107 => [<<"1VknqhhAXRQ6hzeZL-IMVBznTFCdiWcwlXhzpLKS8Zk">>],
		6049618 => [<<"wntmnG9yRP9aoioRDILKkmSZqdemR-XDCIKJS-wpRYw">>, <<"WJTACYoRG89VIpjzsIZLIy93U7HoC4OJyLy6WAlqv-0">>],
		6095199 => [<<"EayO1EsmOinnbi-NVa2V7cVraoI0TZ6xE5-sNU7fc94">>, <<"8CPVZq-zPdMQ2to1P91vl6XBXyL7sLH8-vNclnOCug8">>, <<"QyQL1TYdwmguUIBjTV-shWqrwS6AwxhZ6lf7Rx-vxH0">>, <<"vvPtX1U0EZS9PMsQBVk3mjD9yS6EHIt0FXdKf2dOELw">>],
		6441166 => [<<"MklsZ_cDz470C40UGZUJoVfMeVA89-r7SHxuomBeCPM">>],
		6441564 => [<<"Hs-Yj4ZE9ACfQIjzS8E-qvxSkQALsCIDHwcLEMnlz90">>],
		6441962 => [<<"I4ifBnOF6OQFautfisGFTVIn2NsrvqrdnQ-O7JOMouE">>],
		6808479 => [<<"7fat_nqzDJCTfJMqyEpOcavt1cZNM-tfSzASJd0wrHo">>, <<"YcTBCg3mLRFByb1cnjrq9DzEBnnOT9jQtfYEE34QZ1M">>],
		6820776 => [<<"Re-7lkSGlYP4SFddz0rrXIF0r4MVYZuagjkVpEm79bY">>, <<"hXNDNwQ6zA7aHAqvfBj_az9CovV37bJywdgPdb_ooIA">>],
		6906339 => [<<"qDEFXj8hSgOuuqWM52y6pbUX1cyp7bS4qItfctgtVx8">>],
		6992421 => [<<"9hX3cS3Vjr6vAqJW3WtPN665NpLJegcxyaDZO2esElM">>],
		7145431 => [<<"kVNsLH0kpIkFnBBGWxoIajVLSpvzmsKHpsATPAcR86Y">>],
		7177405 => [<<"hB1Hj0mfuh_x3ijhqkw1s3wdCh8qdPz_IMs0MPraVCk">>],
		7193892 => [<<"ivWTdg5M9XqjP-Iu4C97r3qZQhotJgfF17g__7EH7VM">>],
		7308719 => [<<"lxtOUAEj-E1jb6J8uGCRlRgJDHJyFOu0O73jQHnAhpg">>, <<"ntnx85KcYZ_ZhR6dL2A_p8foCmStgD-69ODoOUdipiA">>, <<"yo8VtPVXWBpTqLbLL-ZeOmZTW2HTqTzsf9RPzgHM-bQ">>, <<"_gduN41u7Xxac_Gm3pBQI3icoKhOfiRV2TKhDnlyakU">>, <<"5iK4mPnFqGdUxpiZmGtTbj7xoSC2una7sjsbUyZkOmM">>, <<"iWUFDucATDE8gjbsL-9KpOIW9l8Ipsh1wliv4e05xhg">>],
		7308855 => [<<"oiYeEvWqOkaHzCSunznZ09U_tuHqP1UyZkRrKYHgNBw">>],
		7465544 => [<<"xK4fFG-PbnQx6EGmmj1A0JVWQ9Bg7q-FncaU7hHk9ds">>],
		7470098 => [<<"eGYHUFl46laNa8v_WjdadvCkIErWqmx0hoia7PCSmSw">>],
		7516201 => [<<"2dxNaIAvkAuL_N2qpTGSl7d7rU3Hu7d4l4IkYb9jgDU">>],
		7737695 => [<<"jStDc8gP5lyHVSFIJiT_2RrXhT26GpAhNItDEje07_Y">>],
		8693975 => [<<"_BN_07s59sawk5e9YcjHTX2qtYX9q7nCBYrlSWXoEsc">>],
		8721209 => [<<"4tlIV1x4YRWtNMut11ox9SS-lWt3xIzcXnrBBbNxGYs">>],
		8740888 => [<<"p4oyXU5C3T0ZycNhEwBZ0MbpV0j3voWV4mr__3fhOek">>],
		8770977 => [<<"cTmKy32Fbmlybl-WtbyuVFNhO11Efr4e_rGbzwAkPbs">>, <<"zNae10gPNkFt5aRVaSL2eSgxZiRDG79B9oDIeYqyzDY">>],
		9155991 => [<<"8qtH9T9jgYLHH-xi39w1OCNJykqew1O5qzrDkhAxN0U">>],
		9489880 => [<<"vDtQzZ9jl6r7yzczhoKhvzekCQYx-qskwYdzQO92eWo">>],
		9525187 => [<<"MQD8-8yIZwNC4A006TC1FVZSyCDHeIAN6YpDbTiX2RU">>],
		9840597 => [<<"1QGjyW1AEFlrFAs6VtUcmwOVOEZJjxaBR_z61W9mftI">>],
		9870742 => [<<"K0w8hOO1oCu4sQipWDQGyEFvn6kAXO-M93neMZmRoUc">>, <<"HoEZ6sK46bzTg4Jzrfy1kHFzkFQgI2UMm9pm0qJS3as">>],
		10104161 => [<<"rTaanqa6Z5KxtBV4Kj2Fu2KKqAWlstE0JeUbZ3AuN3o">>],
		10324897 => [<<"rAARxLc7tOdjUXEdNmSpOtsJIAw0XS229YHO1KOeUqI">>],
		10337196 => [<<"CCH2h2MzMP7WMh0Xf3GYL7zZDbU7E4CZPJWngp1qmDc">>],
		10411070 => [<<"xaB3eS6qbtKSrfFACMcYpgxWRtaJfT1kmOVpyaE45tI">>, <<"Hv0Q5APV6ARfDXDpxI-07R1YFSJAQpxTFh1Z8_nCk3U">>, <<"xCUsF5aatMdiiUAkGjg29_TiQGKqXpbzoMsB0yI-Dd8">>, <<"TNj-jk-KpKzz84xb1SRiKqyp8LNBnONxA9SIXs3XU7k">>],
		10766840 => [<<"AoCuo7S7ewDIqhYheBX6AjShrbyTgIv6Fp1AwQgmGqg">>],
		11188140 => [<<"sEw-yqeADuF0n_M6jTPLrOgH3coalIQHYPLrwM87nmo">>],
		11386662 => [<<"tn3FQGSVFt_TE5nyQNpuf_gnHdaWF85hZg1iE5hPQSE">>],
		11392406 => [<<"WwgngUwH7mXX15tdbfcjG_9gX2t8N8wbbfW2N34b3dA">>],
		11392470 => [<<"3DSCNJ5H9Hpyy7auT9qG5vom9jHBrCgjs48w_R6iSJg">>],
		11490753 => [<<"Zu9CSLWidXEnbSAQVuXGk62eMrVAGQb4qHmrtQrOQIQ">>],
		12665298 => [<<"Yk_dta-f75GShvyUvXq132pohaNpiQgerfIKJA0vdCw">>],
		13324206 => [<<"goAmthhGPdbYUqbAymyG_MjBUWVdS9OBm78mOoiITHo">>],
		13489960 => [<<"RJzScDd1IYIVaVOMo8zV2sXaGE4ZtKxwO2ONPFK-ou4">>],
		13543865 => [<<"ehTWq16I6ixhFOVkpTKi7s4jgYjNzGJ5CoJW3xjHDTE">>],
		13577296 => [<<"D29DVKVYAe74sAj9NBQ351rI6SseWZ5MMsSedGtydS8">>, <<"q8aw85uHTIPxuXcv2Awts4JVVHEMCl7J-61WfnvbYuQ">>],
		13583145 => [<<"4QcodvSlgZnuz5uWGmBARsGUJ5XaYORIO5jYM1dTucI">>],
		14080582 => [<<"clMyhm_qgwUJq68xb8Yf9EEaN3F7jgdqgKnKgjVRom8">>],
		14080694 => [<<"uoTzfoaN81h2_JyFkrvXTLFMnoSlWiuc9Yu1CmsFkH0">>],
		14228100 => [<<"NBjbIMFIdd6jFhSZ20izEke9Ju8jMuvYl8O4bqe4wC4">>],
		14262305 => [<<"vFP1U-4lk3GypDZFceLvRXjoadcB2FRKrcNQf9WjzpQ">>],
		14714324 => [<<"HOMVwtocaJIRPdCeKgzorJZJq1jw_lVGz0pQ3POj7No">>],
		14714340 => [<<"IqJf6iISeiEj3oof9491-jQX4drDZ92VoFuZqNmoixk">>],
		15161433 => [<<"d8CQoDBSrekoGZXqTatc7Y5JkHtNviX1D3JD-fxFDmU">>],
		15337962 => [<<"U2DZlRhnzhZrC7GsVNX0TxnXbHh03P3g-cU4fkHpiXA">>],
		15355055 => [<<"PgxqlgdluUGnmGCal3dgB6PYCd5S7FtBpI0zKDc8-AY">>],
		15355071 => [<<"XrtNbxWFUGlP-SYqQm8aYawQJU6H7CSyHpRZM1iLdKg">>],
		15355087 => [<<"rcc-B4OWqf0dbVY7Eq6q3pRDHLUjJ8tix8UeLQ4D68w">>],
		15355103 => [<<"0KMeq830vwvxUUM7RLCwE0ve4i0h_XHugbUTCkPNH-M">>],
		15359953 => [<<"IeEkQUBq3aE2CSbCF2Bk126lLaLZEYjUPJ_IO601tZg">>],
		15365216 => [<<"7M4KyVB4Wr-Le3Knb7JExgnsXTtG7718JIlhVBNstlE">>],
		15366225 => [<<"A5oMEDa7ZEm1kjPlXpwjuZd40rqP6eo3GobNGQY4HlY">>],
		15435544 => [<<"R5utplMYRQsJwA9Y63cL3Na4mXtYzE4gWG6g6zwgEQE">>],
		15762143 => [<<"lF7NSIz6CNf8WsMNQl8It8HbJem3MAllokozblLdU5A">>],
		15864737 => [<<"sMF6pWIkJFygBbR2IS10liEsjsLAMDja_E9_yUvUgeI">>],
		15905630 => [<<"D_3jwPKLfcTpWcrDV1Q7k3D4sMtyfw7vd45D2C9pUNA">>],
		15915756 => [<<"VL10zUkfmLz5eRxQsZi0G5wsfo8mvyN3p82updP17D4">>],
		16002235 => [<<"B_F4zIV1I5DXM-lR-Ko1tVUTTSmLCOYR7PoY8V8wFas">>],
		16004299 => [<<"FIrCkHY8jVkXcIkWYbMpuQSRYxavkOQ3wtUZPwMS1hM">>],
		16017728 => [<<"bhEMgsj4Yf5tdCDlwK9KpHmsgVLAsBDPOLtYeUDLw0M">>],
		16106226 => [<<"oNZMr_dB-L40nSUj6Fc19-FGteHQu7ZaRZu9_mgM1BI">>],
		16114723 => [<<"TMjINkrJIS3kbGu8bmcVt_34TaFN8lINFQPR_YGzHss">>, <<"ks0ODNqrNY4CCDxJcrgRY324WykCeTiSH4Tmdi30I2E">>],
		16119806 => [<<"rY4cJeAtYkg3bnTdqk4Vb0ojEcfS76L4B-iqyvQZ2VA">>],
		16169319 => [<<"ldoaD2NbG9VRhLOXddM1ypoAU3W5gR_zabUWZa4r6lM">>],
		16197187 => [<<"hPnpcoVcfRdkyUyhYSFNhsEcz7nQU0UU-fPSiRalDvw">>],
		16197716 => [<<"JeP9HaxmjN-TcbCkhKDIQejkGdKTlOgp68O5cy_2GRc">>],
		16199303 => [<<"JNCYRy7XYR_20vvXEAwpT43ovKB23np9yE9cqQfsIJk">>, <<"U4o0STLxwOEf42F4DF22ooOoA5Ykdp5j_D1io-4w1lc">>],
		16376488 => [<<"jOFeroI0Oz4TWcOx8mgv4iOZLv6ncbRXFRtJfqS4Pq0">>, <<"o9ArU5IxydvpJo2iiPI-p4EGBwlpBlyFIfbnz8Qrg6c">>],
		16455497 => [<<"y9wJkLq6Q0hKSDD67ilFqtMMatw9qpsKM9W2uy2Rfjc">>],
		16560237 => [<<"DlRct3GdPx7oYi3MSdmv16CgGWqhLJjbrKcIfU0E48I">>],
		16985772 => [<<"MGDpPk3LsexVpFBF43-FIIvc0vyeEDroYcIONJ6abd0">>],
		17081857 => [<<"fAnOUj-jmlzPMtIN90ZvowG9VUmBtD36MZ8-tRP1Ut4">>],
		17251267 => [<<"bcbIZq2gy8ivQiUlEch7tjNoCcUMTTLhInMlj9P2P88">>, <<"hxyn3yZ7-LCgKqfkCljyM7Hq7HJnmPnEKaXoybXJjHo">>],
		17334749 => [<<"hQvPcHPcBhyxv7GPx-E3bZWiNBhnCpFIDwWa3XBcYEU">>],
		17335147 => [<<"BFfNP1eCeYIkLiWWAVvHNLzk1N2pxkOChFzQbdv1IiA">>],
		17416549 => [<<"Cdcx7-UZJN324I9L47rrph9dIVy8RwfJa9mY7cJp9gk">>, <<"Hiu5cti9FefwcvT6xRCIoADUMkuDEm_6pZo92CK3fiw">>],
		17416947 => [<<"oO7raEVlJC6KhfK-UbNuppzbYPGdKWbh1e6rOymd_-o">>],
		17455331 => [<<"ojgJyXT8qwRXj1hOVx2gbeJDT0xEOIye0o9EbfU2LRM">>],
		17670889 => [<<"JDG-HBsrHGDodot2clC3nNkRKV5cvuhRWZjCwVFHG_Q">>],
		17750387 => [<<"G6JD1n-FXMSyTSryo0HoX7L3i7e4KEFK_ekDMEn9Bcg">>],
		17785886 => [<<"NixeAD5Y_8sQfcrMBWkODQuoXgJouUBmQmQzwTzlaKU">>],
		18017285 => [<<"P5KQo3QSWLzTLWkq3wgJlii11CEUSKMG_O2NMN6y_8c">>],
		18199428 => [<<"zavm_CqSq0KuWfc-E0JccEyrrQzjigxt7yuW1ceYjE0">>],
		18248736 => [<<"OGA55Jyg2c-Jhkx5zDNyiDvbFZiRXF0S_JESMhWAWcs">>],
		18834003 => [<<"kLP-8ILxdLSAQsrC6IwvfqQL6Loq2Q6lqOzwrnb6QoE">>],
		19175334 => [<<"1fzKf0Ygc-z3ejpZ1ZLOiNBYDRzViGRdPLtUqRS1nKY">>],
		19774875 => [<<"fr3nkF8AHXTcq9bT_b7x2X7Mun2A--Ssb7eyoKgQEwI">>],
		20029848 => [<<"_KI9ocPARF5JjaDPIbtpqw2hj_qRonw-AERjWOs5ZYM">>],
		20076163 => [<<"n1GVITzrvCF95Vz7l6hH7fdYzebDDAJav5z4-9C7lB4">>],
		20250499 => [<<"p0MVPvnv_lkWwfhSuSCgQ3NUj83shBffAx1NKPn4oy8">>],
		20424523 => [<<"CQv9OVOCzntq2DRqNJ9j_WnWPcsniyGRXpt4i_a8Iy0">>],
		20599699 => [<<"35wYULjhQBiTFh9u-PJz6ki0v7Zi1whk_AhowUt99Ac">>]
	}.

snapshot_dir(Config) ->
	case Config#config.start_from_state of
		not_set ->
			?DEFAULT_SNAPSHOT_DIR;
		Dir ->
			Dir
	end.

snapshot_dir_name(MainnetStartHeight, LocalnetEndHeight) ->
	lists:flatten(
		io_lib:format("localnet_snapshot_~B_~B", [MainnetStartHeight, LocalnetEndHeight])
	).

open_snapshot_databases(SnapshotDir) ->
	case ar_storage:open_start_from_state_databases(SnapshotDir) of
		ok ->
			{ok, fun ar_storage:close_start_from_state_databases/0};
		{error, _} = Error ->
			Error
	end.

create_snapshot(SnapshotDir, DataDir, NewSnapshotDir) ->
	case ensure_snapshot_dir(NewSnapshotDir) of
		ok ->
			case maybe_backfill_snapshot_data(SnapshotDir) of
				ok ->
					ok = copy_from_dir(DataDir, SnapshotDir, NewSnapshotDir, "wallets"),
					ok = copy_from_dir(DataDir, SnapshotDir, NewSnapshotDir, "ar_tx_blacklist"),
					ok = copy_from_dir(DataDir, SnapshotDir, NewSnapshotDir, "data_sync_state"),
					ok = copy_from_dir(DataDir, SnapshotDir, NewSnapshotDir, "header_sync_state"),
					ok = copy_from_dir(DataDir, SnapshotDir, NewSnapshotDir, "mempool"),
					ok = copy_from_dir(DataDir, SnapshotDir, NewSnapshotDir, "peers"),
					ok = copy_required_dir(SnapshotDir, NewSnapshotDir, "seed_txs"),
					case create_snapshot_rocksdb(NewSnapshotDir) of
						ok ->
							io:format("Snapshot created: ~s~n", [NewSnapshotDir]),
							{ok, NewSnapshotDir};
						{error, _} = Error ->
							Error
					end;
				{error, _} = Error ->
					Error
			end;
		{error, _} = Error ->
			Error
	end.

maybe_backfill_snapshot_data(SnapshotDir) ->
	case open_snapshot_databases(SnapshotDir) of
		{ok, CloseSnapshotDbs} ->
			Result = store_snapshot_data2(SnapshotDir),
			CloseSnapshotDbs(),
			Result;
		{error, _} = Error ->
			Error
	end.

create_snapshot_rocksdb(NewSnapshotDir) ->
	case ar_node:get_block_index() of
		[] ->
			{error, empty_block_index};
		BI ->
			Height = length(BI) - 1,
			SearchDepth = min(Height, ?LOCALNET_START_FROM_STATE_SEARCH_DEPTH),
			case open_snapshot_dbs(NewSnapshotDir) of
				{ok, SnapshotDbs} ->
					Result =
						create_snapshot_rocksdb2(
							BI,
							Height,
							SearchDepth,
							SnapshotDbs
						),
					close_snapshot_dbs(SnapshotDbs),
					Result;
				{error, _} = Error ->
					Error
			end
	end.

create_snapshot_rocksdb2(BI, Height, SearchDepth, SnapshotDbs) ->
	case write_block_index_snapshot(BI, maps:get(block_index, SnapshotDbs)) of
		ok ->
			create_snapshot_rocksdb3(
				BI,
				Height,
				SearchDepth,
				SnapshotDbs
			);
		{error, _} = Error ->
			Error
	end.

create_snapshot_rocksdb3(BI, Height, SearchDepth, SnapshotDbs) ->
	case read_recent_blocks_local(BI, SearchDepth) of
		not_found ->
			{error, block_headers_not_found};
		{Skipped, Blocks} ->
			io:format("Snapshot: recent blocks ~B (skipped: ~B)~n",
				[length(Blocks), Skipped]),
			BI2 = lists:nthtail(Skipped, BI),
			Height2 = Height - Skipped,
			RewardHistoryBI = reward_history_bi(Height2, BI2),
			BlockTimeHistoryBI = block_time_history_bi(BI2),
			case store_snapshot_blocks_in_snapshot(Blocks, SnapshotDbs) of
				{ok, TxIds} ->
					create_snapshot_rocksdb4(
						BI,
						Blocks,
						RewardHistoryBI,
						BlockTimeHistoryBI,
						SnapshotDbs,
						TxIds,
						SearchDepth
					);
				{error, _} = Error ->
					Error
			end
	end.

create_snapshot_rocksdb4(BI, Blocks, RewardHistoryBI, BlockTimeHistoryBI, SnapshotDbs, TxIds,
		SearchDepth) ->
	case store_tx_headers(TxIds, maps:get(tx, SnapshotDbs)) of
		ok ->
			case copy_history_entries(
				RewardHistoryBI,
				reward_history_db,
				maps:get(reward_history, SnapshotDbs),
				reward_history
			) of
				ok ->
					case copy_history_entries(
						BlockTimeHistoryBI,
						block_time_history_db,
						maps:get(block_time_history, SnapshotDbs),
						block_time_history
					) of
						ok ->
							case store_latest_wallet_list_from_blocks(Blocks,
								maps:get(account_tree, SnapshotDbs), SearchDepth) of
								ok ->
									verify_snapshot_rocksdb(BI, Blocks,
										maps:get(block_index, SnapshotDbs),
										maps:get(block, SnapshotDbs),
										maps:get(account_tree, SnapshotDbs));
								{error, _} = Error ->
									io:format("Snapshot: error storing wallet list: ~p~n", [Error]),
									Error
							end;
						{error, _} = Error ->
							io:format("Snapshot: error copying block time history: ~p~n", [Error]),
							Error
					end;
				{error, _} = Error ->
					io:format("Snapshot: error copying reward history: ~p~n", [Error]),
					Error
			end;
		{error, _} = Error ->
			io:format("Snapshot: error storing tx headers: ~p~n", [Error]),
			Error
	end.

open_snapshot_dbs(NewSnapshotDir) ->
	%% We cannot use ar_storage:open_databases/0 since it always targets the
	%% live data_dir and would collide with the running node's DB names.
	RocksDir = filename:join([NewSnapshotDir, ?ROCKS_DB_DIR]),
	Dbs = [
		{tx_confirmation, snapshot_tx_confirmation_db, "ar_storage_tx_confirmation_db"},
		{tx, snapshot_tx_db, "ar_storage_tx_db"},
		{block, snapshot_block_db, "ar_storage_block_db"},
		{reward_history, snapshot_reward_history_db, "reward_history_db"},
		{block_time_history, snapshot_block_time_history_db, "block_time_history_db"},
		{block_index, snapshot_block_index_db, "block_index_db"},
		{account_tree, snapshot_account_tree_db, "account_tree_db"}
	],
	open_snapshot_write_dbs(Dbs, RocksDir, #{}, []).

open_snapshot_write_dbs([], _RocksDir, DbMap, Opened) ->
	{ok, DbMap#{opened => Opened}};
open_snapshot_write_dbs([{Key, Name, DirName} | Rest], RocksDir, DbMap, Opened) ->
	Path = filename:join([RocksDir, DirName]),
	case ar_kv:open(#{ path => Path, name => Name }) of
		ok ->
			open_snapshot_write_dbs(Rest, RocksDir, DbMap#{ Key => Name }, [Name | Opened]);
		{error, _} = Error ->
			close_snapshot_dbs(DbMap#{ opened => Opened }),
			Error
	end.

close_snapshot_dbs(SnapshotDbs) ->
	Opened = maps:get(opened, SnapshotDbs, []),
	lists:foreach(fun ar_kv:close/1, Opened).

reward_history_bi(Height, BI) ->
	InterimRewardHistoryLength0 = (Height - ar_fork:height_2_8()) + 21600,
	InterimRewardHistoryLength =
		case InterimRewardHistoryLength0 > 0 of
			true ->
				InterimRewardHistoryLength0;
			false ->
				0
		end,
	RewardHistoryBI0 = ar_rewards:trim_buffered_reward_history(Height, BI),
	lists:sublist(RewardHistoryBI0, InterimRewardHistoryLength).

block_time_history_bi(BI) ->
	lists:sublist(BI,
		ar_block_time_history:history_length() + ar_block:get_consensus_window_size()).

write_block_index_snapshot(BI, SnapshotBlockIndexDb) ->
	write_block_index_snapshot2(0, <<>>, lists:reverse(BI), SnapshotBlockIndexDb).

write_block_index_snapshot2(_Height, _PrevH, [], _SnapshotBlockIndexDb) ->
	ok;
write_block_index_snapshot2(Height, PrevH, [{H, WeaveSize, TXRoot} | BI],
		SnapshotBlockIndexDb) ->
	Bin = term_to_binary({H, WeaveSize, TXRoot, PrevH}),
	case ar_kv:put(SnapshotBlockIndexDb, << Height:256 >>, Bin) of
		ok ->
			write_block_index_snapshot2(Height + 1, H, BI, SnapshotBlockIndexDb);
		Error ->
			Error
	end.

store_snapshot_blocks_in_snapshot(Blocks, SnapshotDbs) ->
	BlockDb = maps:get(block, SnapshotDbs),
	TxConfirmationDb = maps:get(tx_confirmation, SnapshotDbs),
	store_snapshot_blocks_with_dbs(Blocks, BlockDb, TxConfirmationDb, "Snapshot").

store_block_snapshot(B, SnapshotBlockDb) ->
	TxIds = lists:map(fun tx_id/1, B#block.txs),
	BlockBin = ar_serialize:block_to_binary(B#block{ txs = TxIds }),
	ar_kv:put(SnapshotBlockDb, B#block.indep_hash, BlockBin).

tx_id(#tx{ id = TXID }) ->
	TXID;
tx_id(TXID) ->
	TXID.

copy_tx_confirmations([], _Height, _BlockHash, _SnapshotTxConfDb) ->
	ok;
copy_tx_confirmations([TXID | Rest], Height, BlockHash, SnapshotTxConfDb) ->
	case tx_id(TXID) of
		TXID2 when is_binary(TXID2) ->
			case ar_kv:get(tx_confirmation_db, TXID2) of
				{ok, Bin} ->
					case ar_kv:put(SnapshotTxConfDb, TXID2, Bin) of
						ok ->
							copy_tx_confirmations(Rest, Height, BlockHash, SnapshotTxConfDb);
						Error ->
							Error
					end;
				not_found ->
					case ar_kv:put(SnapshotTxConfDb, TXID2, term_to_binary({Height, BlockHash})) of
						ok ->
							copy_tx_confirmations(Rest, Height, BlockHash, SnapshotTxConfDb);
						Error ->
							Error
					end;
				{error, _} = Error ->
					Error
			end;
		_ ->
			copy_tx_confirmations(Rest, Height, BlockHash, SnapshotTxConfDb)
	end.

store_tx_headers(TxIds, SnapshotTxDb) ->
	lists:foldl(
		fun(TXID, Acc) ->
			case Acc of
				ok ->
					case read_tx_local(TXID) of
						#tx{} = TX ->
							store_tx_header_snapshot(TX, SnapshotTxDb);
						unavailable ->
							{error, {tx_unavailable, tx_id(TXID)}};
						Error ->
							{error, {tx_unavailable, tx_id(TXID), Error}}
					end;
				{error, _} = Error ->
					Error
			end
		end,
		ok,
		TxIds
	).

store_tx_header_snapshot(TX, SnapshotTxDb) ->
	TX2 =
		case TX#tx.format of
			1 ->
				TX;
			_ ->
				TX#tx{ data = <<>> }
		end,
	ar_kv:put(SnapshotTxDb, TX2#tx.id, ar_serialize:tx_to_binary(TX2)).

copy_history_entries(HistoryBI, SourceDb, DestDb, Label) ->
	lists:foldl(
		fun({BH, _, _}, Acc) ->
			case Acc of
				ok ->
					case ar_kv:get(SourceDb, BH) of
						{ok, Bin} ->
							ar_kv:put(DestDb, BH, Bin);
						not_found ->
							{error, {Label, not_found, BH}};
						{error, Reason} ->
							{error, {Label, Reason, BH}}
					end;
				{error, _} = Error ->
					Error
			end
		end,
		ok,
		HistoryBI
	).

store_latest_wallet_list_from_blocks(Blocks, SnapshotAccountTreeDb, SearchDepth) ->
	case find_wallet_tree_with_search(Blocks, SearchDepth, fun ar_storage:read_wallet_list/1) of
		{ok, {B, Tree}} ->
			{RootHash, _UpdatedTree, UpdateMap} = ar_block:hash_wallet_list(Tree),
			case RootHash == B#block.wallet_list of
				true ->
					store_account_tree_update_snapshot(
						B#block.height,
						RootHash,
						UpdateMap,
						SnapshotAccountTreeDb
					);
				false ->
					{error, {wallet_list_root_mismatch, RootHash, B#block.wallet_list}}
			end;
		not_found ->
			{error, wallet_list_not_found}
	end.

find_wallet_tree_with_search([], _SearchDepth, _ReadWalletFun) ->
	not_found;
find_wallet_tree_with_search(Blocks, SearchDepth, ReadWalletFun) ->
	find_wallet_tree_with_search(Blocks, SearchDepth, 0, ReadWalletFun).

find_wallet_tree_with_search(_Blocks, Skipped, Skipped, _ReadWalletFun) ->
	not_found;
find_wallet_tree_with_search(Blocks, SearchDepth, Skipped, ReadWalletFun) ->
	{IsLast, B} =
		case length(Blocks) >= ar_block:get_consensus_window_size() of
			true ->
				{false, lists:nth(ar_block:get_consensus_window_size(), Blocks)};
			false ->
				{true, lists:last(Blocks)}
		end,
	case ReadWalletFun(B#block.wallet_list) of
		{ok, Tree} ->
			{ok, {B, Tree}};
		_ ->
			case IsLast of
				true ->
					not_found;
				false ->
					find_wallet_tree_with_search(tl(Blocks), SearchDepth, Skipped + 1, ReadWalletFun)
			end
	end.

store_account_tree_update_snapshot(_Height, _RootHash, Map, SnapshotAccountTreeDb) ->
	maps:fold(
		fun({H, Prefix}, Value, Acc) ->
			case Acc of
				ok ->
					Prefix2 =
						case Prefix of
							root ->
								<<>>;
							_ ->
								Prefix
						end,
					DBKey = << H/binary, Prefix2/binary >>,
					case ar_kv:get(SnapshotAccountTreeDb, DBKey) of
						not_found ->
							ar_kv:put(SnapshotAccountTreeDb, DBKey, term_to_binary(Value));
						{ok, _} ->
							ok;
						{error, Reason} ->
							{error, {account_tree_read_failed, Reason}}
					end;
				{error, _} = Error ->
					Error
			end
		end,
		ok,
		Map
	).

verify_snapshot_rocksdb(BI, Blocks, SnapshotBlockIndexDb, SnapshotBlockDb,
		SnapshotAccountTreeDb) ->
	Height = length(BI) - 1,
	case read_block_index_from_db(SnapshotBlockIndexDb) of
		not_found ->
			{error, snapshot_block_index_not_found};
		SnapshotBI ->
			case length(SnapshotBI) == length(BI) of
				true ->
					case verify_recent_blocks_from_blocks(Blocks, SnapshotBlockDb) of
						ok ->
							case validate_wallet_root_from_blocks(Blocks, SnapshotAccountTreeDb) of
								ok ->
									ok;
								{error, _} = Error ->
									Error
							end;
						{error, _} = Error ->
							Error
					end;
				false ->
					{error, {snapshot_block_index_length_mismatch, Height}}
			end
	end.

read_block_index_from_db(DbName) ->
	case ar_kv:get_prev(DbName, <<"a">>) of
		none ->
			not_found;
		{ok, << Height:256 >>, _V} ->
			{ok, Map} = ar_kv:get_range(DbName, << 0:256 >>, << Height:256 >>),
			read_block_index_from_map(Map, 0, Height, <<>>, [])
	end.

read_block_index_from_map(_Map, Height, End, _PrevH, BI) when Height > End ->
	BI;
read_block_index_from_map(Map, Height, End, PrevH, BI) ->
	V = maps:get(<< Height:256 >>, Map, not_found),
	case V of
		not_found ->
			not_found;
		_ ->
			case binary_to_term(V) of
				{H, WeaveSize, TXRoot, PrevH} ->
					read_block_index_from_map(Map, Height + 1, End, H,
						[{H, WeaveSize, TXRoot} | BI]);
				{_, _, _, _} ->
					not_found
			end
	end.

verify_recent_blocks_from_blocks([], _SnapshotBlockDb) ->
	ok;
verify_recent_blocks_from_blocks([B | Rest], SnapshotBlockDb) ->
	BH = B#block.indep_hash,
	case ar_kv:get(SnapshotBlockDb, BH) of
		{ok, _} ->
			verify_recent_blocks_from_blocks(Rest, SnapshotBlockDb);
		not_found ->
			{error, {snapshot_block_missing, BH}};
		{error, _} = Error ->
			Error
	end.

validate_wallet_root_from_blocks([], _SnapshotAccountTreeDb) ->
	{error, wallet_list_not_found};
validate_wallet_root_from_blocks([B | Rest], SnapshotAccountTreeDb) ->
	WalletList = B#block.wallet_list,
	case ar_kv:get_prev(SnapshotAccountTreeDb, << WalletList/binary >>) of
		none ->
			validate_wallet_root_from_blocks(Rest, SnapshotAccountTreeDb);
		{ok, _, _} ->
			ok
	end.

read_tx_local(TXID) ->
	case TXID of
		#tx{} = TX ->
			TX;
		_ ->
			ar_storage:read_tx(TXID)
	end.

read_recent_blocks_local(BI, SearchDepth) ->
	ar_node:read_recent_blocks(BI, SearchDepth, not_set).

read_recent_blocks_from_snapshot(BI, SearchDepth, SnapshotDir) ->
	ar_node:read_recent_blocks(BI, SearchDepth, SnapshotDir).


store_snapshot_blocks_from_list(Blocks) ->
	store_snapshot_blocks_with_dbs(Blocks, block_db, tx_confirmation_db, "Startup copy").

store_snapshot_blocks_with_dbs(Blocks, BlockDb, TxConfirmationDb, LogPrefix) ->
	case lists:foldl(
		fun(B, Acc) ->
			case Acc of
				{ok, TxIdSet} ->
					io:format("~s: block ~s height ~B~n",
						[LogPrefix, ar_util:encode(B#block.indep_hash), B#block.height]),
					case store_block_snapshot(B, BlockDb) of
						ok ->
							TxIds = lists:map(fun tx_id/1, B#block.txs),
							case copy_tx_confirmations(TxIds, B#block.height,
									B#block.indep_hash, TxConfirmationDb) of
								ok ->
									{ok, sets:union(TxIdSet, sets:from_list(TxIds))};
								{error, _} = Error ->
									io:format("~s: error confirmations for block ~s: ~p~n",
										[LogPrefix, ar_util:encode(B#block.indep_hash), Error]),
									Error
							end;
						{error, _} = Error ->
							io:format("~s: error storing block ~s: ~p~n",
								[LogPrefix, ar_util:encode(B#block.indep_hash), Error]),
							Error
					end;
				{error, _} = Error ->
					Error
			end
		end,
		{ok, sets:new()},
		Blocks
	) of
		{ok, TxIdSet} ->
			{ok, sets:to_list(TxIdSet)};
		{error, _} = Error ->
			Error
	end.


store_snapshot_tx_headers(TxIds, SnapshotDir) ->
	io:format("Startup copy: tx headers to copy ~B~n", [length(TxIds)]),
	lists:foldl(
		fun(TXID, Acc) ->
			case Acc of
				ok ->
					TXID2 = tx_id(TXID),
					case ar_kv:get(tx_db, TXID2) of
						{ok, _} ->
							ok;
						not_found ->
							case ar_storage:read_tx(TXID2, SnapshotDir) of
								#tx{} = TX ->
									store_tx_header_snapshot(TX, tx_db);
								unavailable ->
									io:format("Startup copy: missing tx header ~s~n",
										[ar_util:encode(TXID2)]),
									{error, {tx_unavailable, TXID2}};
								Error ->
									io:format("Startup copy: error reading tx ~s: ~p~n",
										[ar_util:encode(TXID2), Error]),
									{error, {tx_unavailable, TXID2, Error}}
							end;
						{error, _} = Error ->
							io:format("Startup copy: error reading tx db ~s: ~p~n",
								[ar_util:encode(TXID2), Error]),
							{error, {tx_db_read_failed, TXID2, Error}}
					end;
				{error, _} = Error ->
					Error
			end
		end,
		ok,
		TxIds
	).

store_snapshot_history_entries(HistoryBI, SourceDb, DestDb, Label) ->
	lists:foldl(
		fun({BH, _, _}, Acc) ->
			case Acc of
				ok ->
					case ar_kv:get(DestDb, BH) of
						{ok, _} ->
							ok;
						not_found ->
							case ar_kv:get(SourceDb, BH) of
								{ok, Bin} ->
									ar_kv:put(DestDb, BH, Bin);
								not_found ->
									io:format("Startup copy: missing ~p entry ~s~n",
										[Label, ar_util:encode(BH)]),
									{error, {Label, not_found, BH}};
								{error, Reason} ->
									io:format("Startup copy: error ~p entry ~s: ~p~n",
										[Label, ar_util:encode(BH), Reason]),
									{error, {Label, Reason, BH}}
							end;
						{error, _} = Error ->
							io:format("Startup copy: error reading ~p entry ~s: ~p~n",
								[Label, ar_util:encode(BH), Error]),
							{error, {Label, Error, BH}}
					end;
				{error, _} = Error ->
					Error
			end
		end,
		ok,
		HistoryBI
	).

store_snapshot_wallet_list(Blocks, SnapshotDir, SearchDepth) ->
	case find_wallet_tree_with_search(Blocks, SearchDepth,
			fun(WalletList) -> read_wallet_list_from_snapshot(WalletList, SnapshotDir) end) of
		{ok, {B, Tree}} ->
			{RootHash, _UpdatedTree, UpdateMap} = ar_block:hash_wallet_list(Tree),
			io:format("Startup copy: wallet list root ~s height ~B~n",
				[ar_util:encode(RootHash), B#block.height]),
			case RootHash == B#block.wallet_list of
				true ->
					store_account_tree_update_snapshot(
						B#block.height,
						RootHash,
						UpdateMap,
						account_tree_db
					);
				false ->
					{error, {wallet_list_root_mismatch, RootHash, B#block.wallet_list}}
			end;
		not_found ->
			{error, wallet_list_not_found}
	end.


read_wallet_list_from_snapshot(WalletList, SnapshotDir) ->
	case ar_storage:read_wallet_list(WalletList) of
		{ok, Tree} ->
			{ok, Tree};
		_ ->
			ar_storage:read_wallet_list(WalletList, SnapshotDir)
	end.

ensure_snapshot_dir(NewSnapshotDir) ->
	case file:read_file_info(NewSnapshotDir) of
		{ok, _} ->
			{error, {snapshot_dir_exists, NewSnapshotDir}};
		{error, enoent} ->
			filelib:ensure_dir(filename:join(NewSnapshotDir, "placeholder") ++ "/");
		{error, Reason} ->
			{error, {snapshot_dir_unavailable, Reason}}
	end.

copy_from_dir(PrimaryDir, FallbackDir, TargetDir, Name) ->
	PrimaryPath = filename:join([PrimaryDir, Name]),
	FallbackPath = filename:join([FallbackDir, Name]),
	TargetPath = filename:join([TargetDir, Name]),
	case exists_on_disk(PrimaryPath) of
		true ->
			copy_any(PrimaryPath, TargetPath);
		false ->
			case exists_on_disk(FallbackPath) of
				true ->
					copy_any(FallbackPath, TargetPath);
				false ->
					ok
			end
	end.

copy_required_dir(SourceDir, TargetDir, Name) ->
	SourcePath = filename:join([SourceDir, Name]),
	TargetPath = filename:join([TargetDir, Name]),
	case exists_on_disk(SourcePath) of
		true ->
			copy_any(SourcePath, TargetPath);
		false ->
			{error, {snapshot_path_missing, SourcePath}}
	end.

exists_on_disk(Path) ->
	case file:read_file_info(Path) of
		{ok, _} ->
			true;
		_ ->
			false
	end.

copy_any(SourcePath, TargetPath) ->
	case file:read_file_info(SourcePath) of
		{ok, #file_info{ type = directory }} ->
			copy_dir(SourcePath, TargetPath);
		{ok, #file_info{ type = regular }} ->
			filelib:ensure_dir(TargetPath),
			case file:copy(SourcePath, TargetPath) of
				{ok, _} ->
					ok;
				Error ->
					Error
			end;
		{ok, #file_info{ type = symlink }} ->
			copy_symlink(SourcePath, TargetPath);
		{ok, #file_info{ type = other }} ->
			{error, {unsupported_type, SourcePath}};
		{error, Reason} ->
			{error, {read_file_info_failed, SourcePath, Reason}}
	end.

copy_dir(SourceDir, TargetDir) ->
	case file:list_dir(SourceDir) of
		{ok, Entries} ->
			ok = filelib:ensure_dir(filename:join([TargetDir, "placeholder"]) ++ "/"),
			lists:foldl(
				fun(Entry, Acc) ->
					case Acc of
						ok ->
							SourcePath = filename:join([SourceDir, Entry]),
							TargetPath = filename:join([TargetDir, Entry]),
							copy_any(SourcePath, TargetPath);
						{error, _} = Error ->
							Error
					end
				end,
				ok,
				Entries
			);
		{error, Reason} ->
			{error, {list_dir_failed, SourceDir, Reason}}
	end.

copy_symlink(SourcePath, TargetPath) ->
	case file:read_link(SourcePath) of
		{ok, LinkTarget} ->
			filelib:ensure_dir(TargetPath),
			file:make_symlink(LinkTarget, TargetPath);
		{error, Reason} ->
			{error, {read_link_failed, SourcePath, Reason}}
	end.

