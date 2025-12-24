-module(ar_localnet).

-export([start/0, start/1, submit_snapshot_data/0, format_packing/1,
		mine_one_block/0, mine_until_height/1]).

-include("ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(WAIT_UNTIL_JOINED_TIMEOUT, 200_000).

%% @doc Start a node from the localnet_snapshot directory.
%% Disable mining (can be triggered by request).
%% Configure a single storage module with the first 20 MiB of partition 0.
%% Seed the storage module with data.
start() ->
	start(#config{
		data_dir = ".tmp/data_localnet_main"
	}).

start(Config) ->
	SnapshotDir = "localnet_snapshot",
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

wait_until_joined() ->
	ar_util:do_until(
		fun() -> ar_node:is_joined() end,
		100,
		?WAIT_UNTIL_JOINED_TIMEOUT
	 ).

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
	io:format("Seeding data from snapshot...~n"),
	SnapshotTXs = snapshot_txs(),
	BlockStarts = lists:sort(maps:keys(SnapshotTXs)),
	{TotalBigChunk, TotalSmallChunk} =
		lists:foldl(
			fun(BlockStart, {AccBigChunk, AccSmallChunk}) ->
				TXIDs = maps:get(BlockStart, SnapshotTXs),
				TXs = lists:map(fun(TXID) ->
					Filename = "localnet_snapshot/seed_txs/" ++ binary_to_list(TXID) ++ ".json",
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

