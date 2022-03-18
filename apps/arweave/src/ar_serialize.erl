%%% @doc The module contains serialisation/deserialisation utility functions.
-module(ar_serialize).

-export([json_struct_to_block/1, block_to_json_struct/1,
		block_to_binary/1, binary_to_block/1,
		block_announcement_to_binary/1, binary_to_block_announcement/1,
		binary_to_block_announcement_response/1, block_announcement_response_to_binary/1,
		tx_to_binary/1, binary_to_tx/1,
		poa_to_binary/1, binary_to_poa/1,
		json_struct_to_poa/1, poa_to_json_struct/1,
		tx_to_json_struct/1, json_struct_to_tx/1, json_struct_to_v1_tx/1,
		etf_to_wallet_chunk_response/1, wallet_list_to_json_struct/3,
		wallet_to_json_struct/1, json_struct_to_wallet_list/1,
		block_index_to_json_struct/1, json_struct_to_block_index/1,
		jsonify/1, dejsonify/1, json_decode/1, json_decode/2,
		query_to_json_struct/1, json_struct_to_query/1,
		chunk_proof_to_json_map/1, json_map_to_chunk_proof/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

block_to_binary(#block{ indep_hash = H, previous_block = PrevH, timestamp = TS,
		nonce = Nonce, height = Height, diff = Diff, cumulative_diff = CDiff,
		last_retarget = LastRetarget, hash = Hash, block_size = BlockSize,
		weave_size = WeaveSize, reward_addr = Addr, tx_root = TXRoot,
		wallet_list = WalletList, hash_list_merkle = HashListMerkle,
		reward_pool = RewardPool,
		packing_2_5_threshold = Threshold,
		strict_data_split_threshold = StrictChunkThreshold,
		usd_to_ar_rate = Rate,
		scheduled_usd_to_ar_rate = ScheduledRate,
		poa = #poa{ option = Option, chunk = Chunk, data_path = DataPath,
				tx_path = TXPath }, tags = Tags, txs = TXs }) ->
	Addr2 = case Addr of unclaimed -> <<>>; _ -> Addr end,
	{RateDividend, RateDivisor} = case Rate of undefined -> {undefined, undefined};
			_ -> Rate end,
	{ScheduledRateDividend, ScheduledRateDivisor} =
			case ScheduledRate of
				undefined ->
					{undefined, undefined};
				_ ->
					ScheduledRate
			end,
	<< H:48/binary, (encode_bin(PrevH, 8))/binary, (encode_int(TS, 8))/binary,
			(encode_bin(Nonce, 16))/binary, (encode_int(Height, 8))/binary,
			(encode_int(Diff, 16))/binary, (encode_int(CDiff, 16))/binary,
			(encode_int(LastRetarget, 8))/binary, (encode_bin(Hash, 8))/binary,
			(encode_int(BlockSize, 16))/binary, (encode_int(WeaveSize, 16))/binary,
			(encode_bin(Addr2, 8))/binary, (encode_bin(TXRoot, 8))/binary,
			(encode_bin(WalletList, 8))/binary, (encode_bin(HashListMerkle, 8))/binary,
			(encode_int(RewardPool, 8))/binary, (encode_int(Threshold, 8))/binary,
			(encode_int(StrictChunkThreshold, 8))/binary,
			(encode_int(RateDividend, 8))/binary,
			(encode_int(RateDivisor, 8))/binary,
			(encode_int(ScheduledRateDividend, 8))/binary,
			(encode_int(ScheduledRateDivisor, 8))/binary, (encode_int(Option, 8))/binary,
			(encode_bin(Chunk, 24))/binary, (encode_bin(TXPath, 24))/binary,
			(encode_bin(DataPath, 24))/binary, (encode_bin_list(Tags, 16, 16))/binary,
			(encode_transactions(TXs))/binary >>.

encode_int(undefined, SizeBits) ->
	<< 0:SizeBits >>;
encode_int(N, SizeBits) ->
	Bin = binary:encode_unsigned(N, big),
	<< (byte_size(Bin)):SizeBits, Bin/binary >>.

encode_bin(undefined, SizeBits) ->
	<< 0:SizeBits >>;
encode_bin(Bin, SizeBits) ->
	<< (byte_size(Bin)):SizeBits, Bin/binary >>.

encode_bin_list(Tags, LenBits, ElemSizeBits) ->
	encode_bin_list(Tags, [], 0, LenBits, ElemSizeBits).

encode_bin_list([], Encoded, N, LenBits, _ElemSizeBits) ->
	<< N:LenBits, (iolist_to_binary(Encoded))/binary >>;
encode_bin_list([Tag | Tags], Encoded, N, LenBits, ElemSizeBits) ->
	encode_bin_list(Tags, [encode_bin(Tag, ElemSizeBits) | Encoded], N + 1, LenBits,
			ElemSizeBits).

encode_transactions(TXs) ->
	encode_transactions(TXs, [], 0).

encode_transactions([], Encoded, N) ->
	<< N:16, (iolist_to_binary(Encoded))/binary >>;
encode_transactions([<< TXID:32/binary >> | TXs], Encoded, N) ->
	encode_transactions(TXs, [<< 32:24, TXID:32/binary >> | Encoded], N + 1);
encode_transactions([TX | TXs], Encoded, N) ->
	Bin = encode_tx(TX),
	TXSize = byte_size(Bin),
	encode_transactions(TXs, [<< TXSize:24, Bin/binary >> | Encoded], N + 1).

encode_tx(#tx{ format = Format, id = TXID, last_tx = LastTX, owner = Owner,
		tags = Tags, target = Target, quantity = Quantity, data = Data,
		data_size = DataSize, data_root = DataRoot, signature = Signature,
		reward = Reward }) ->
	<< Format:8, TXID:32/binary,
			(encode_bin(LastTX, 8))/binary, (encode_bin(Owner, 16))/binary,
			(encode_bin(Target, 8))/binary, (encode_int(Quantity, 8))/binary,
			(encode_int(DataSize, 16))/binary, (encode_bin(DataRoot, 8))/binary,
			(encode_bin(Signature, 16))/binary, (encode_int(Reward, 8))/binary,
			(encode_bin(Data, 24))/binary, (encode_tx_tags(Tags))/binary >>.

encode_tx_tags(Tags) ->
	encode_tx_tags(Tags, [], 0).

encode_tx_tags([], Encoded, N) ->
	<< N:16, (iolist_to_binary(Encoded))/binary >>;
encode_tx_tags([{Name, Value} | Tags], Encoded, N) ->
	TagNameSize = byte_size(Name),
	TagValueSize = byte_size(Value),
	Tag = << TagNameSize:16, TagValueSize:16, Name/binary, Value/binary >>,
	encode_tx_tags(Tags, [Tag | Encoded], N + 1).

binary_to_block(<< H:48/binary, PrevHSize:8, PrevH:PrevHSize/binary,
		TSSize:8, TS:(TSSize * 8),
		NonceSize:16, Nonce:NonceSize/binary,
		HeightSize:8, Height:(HeightSize * 8),
		DiffSize:16, Diff:(DiffSize * 8),
		CDiffSize:16, CDiff:(CDiffSize * 8),
		LastRetargetSize:8, LastRetarget:(LastRetargetSize * 8),
		HashSize:8, Hash:HashSize/binary,
		BlockSizeSize:16, BlockSize:(BlockSizeSize * 8),
		WeaveSizeSize:16, WeaveSize:(WeaveSizeSize * 8),
		AddrSize:8, Addr:AddrSize/binary,
		TXRootSize:8, TXRoot:TXRootSize/binary, % 0 or 32
		WalletListSize:8, WalletList:WalletListSize/binary,
		HashListMerkleSize:8, HashListMerkle:HashListMerkleSize/binary,
		RewardPoolSize:8, RewardPool:(RewardPoolSize * 8),
		PackingThresholdSize:8, Threshold:(PackingThresholdSize * 8),
		StrictChunkThresholdSize:8, StrictChunkThreshold:(StrictChunkThresholdSize * 8),
		RateDividendSize:8, RateDividend:(RateDividendSize * 8),
		RateDivisorSize:8, RateDivisor:(RateDivisorSize * 8),
		SchedRateDividendSize:8, SchedRateDividend:(SchedRateDividendSize * 8),
		SchedRateDivisorSize:8, SchedRateDivisor:(SchedRateDivisorSize * 8),
		PoAOptionSize:8, PoAOption:(PoAOptionSize * 8),
		ChunkSize:24, Chunk:ChunkSize/binary,
		TXPathSize:24, TXPath:TXPathSize/binary,
		DataPathSize:24, DataPath:DataPathSize/binary,
		Rest/binary >>) when NonceSize =< 512 ->
	Threshold2 = case PackingThresholdSize of 0 -> undefined; _ -> Threshold end,
	StrictChunkThreshold2 = case StrictChunkThresholdSize of 0 -> undefined;
			_ -> StrictChunkThreshold end,
	Rate = case RateDivisorSize of 0 -> undefined;
			_ -> {RateDividend, RateDivisor} end,
	ScheduledRate = case SchedRateDivisor of 0 -> undefined;
			_ -> {SchedRateDividend, SchedRateDivisor} end,
	Addr2 = case Addr of <<>> -> unclaimed; _ -> Addr end,
	B = #block{ indep_hash = H, previous_block = PrevH, timestamp = TS,
			nonce = Nonce, height = Height, diff = Diff, cumulative_diff = CDiff,
			last_retarget = LastRetarget, hash = Hash, block_size = BlockSize,
			weave_size = WeaveSize, reward_addr = Addr2, tx_root = TXRoot,
			wallet_list = WalletList, hash_list_merkle = HashListMerkle,
			reward_pool = RewardPool, packing_2_5_threshold = Threshold2,
			strict_data_split_threshold = StrictChunkThreshold2,
			usd_to_ar_rate = Rate, scheduled_usd_to_ar_rate = ScheduledRate,
			poa = #poa{ option = PoAOption, chunk = Chunk, data_path = DataPath,
					tx_path = TXPath }},
	parse_block_tags_transactions(Rest, B);
binary_to_block(_Bin) ->
	{error, invalid_block_input}.

parse_block_tags_transactions(Bin, B) ->
	case parse_block_tags(Bin) of
		{error, Reason} ->
			{error, Reason};
		{ok, Tags, Rest} ->
			parse_block_transactions(Rest, B#block{ tags = Tags })
	end.

parse_block_transactions(Bin, B) ->
	case parse_block_transactions(Bin) of
		{error, Reason} ->
			{error, Reason};
		{ok, TXs} ->
			{ok, B#block{ txs = TXs }}
	end.

parse_block_tags(<< TagsLen:16, Rest/binary >>) when TagsLen =< 2048 ->
	parse_block_tags(TagsLen, Rest, []);
parse_block_tags(_Bin) ->
	{error, invalid_tags_input}.

parse_block_tags(0, Rest, Tags) ->
	{ok, Tags, Rest};
parse_block_tags(N, << TagSize:16, Tag:TagSize/binary, Rest/binary >>, Tags) ->
	parse_block_tags(N - 1, << Rest/binary >>, [Tag | Tags]);
parse_block_tags(_N, _Bin, _Tags) ->
	{error, invalid_tag_input}.

parse_block_transactions(<< Count:16, Rest/binary >>) when Count =< 1000 ->
	parse_block_transactions(Count, Rest, []);
parse_block_transactions(_Bin) ->
	{error, invalid_transactions_input}.

parse_block_transactions(0, <<>>, TXs) ->
	{ok, TXs};
parse_block_transactions(N, << Size:24, Bin:Size/binary, Rest/binary >>, TXs)
		when N > 0 ->
	case parse_tx(Bin) of
		{error, Reason} ->
			{error, Reason};
		{ok, TX} ->
			parse_block_transactions(N - 1, Rest, [TX | TXs])
	end;
parse_block_transactions(_N, _Rest, _TXs) ->
	{error, invalid_transactions2_input}.

parse_tx(<< TXID:32/binary >>) ->
	{ok, TXID};
parse_tx(<< Format:8, TXID:32/binary,
		LastTXSize:8, LastTX:LastTXSize/binary,
		OwnerSize:16, Owner:OwnerSize/binary,
		TargetSize:8, Target:TargetSize/binary,
		QuantitySize:8, Quantity:(QuantitySize * 8),
		DataSizeSize:16, DataSize:(DataSizeSize * 8),
		DataRootSize:8, DataRoot:DataRootSize/binary,
		SignatureSize:16, Signature:SignatureSize/binary,
		RewardSize:8, Reward:(RewardSize * 8),
		DataEncodingSize:24, Data:DataEncodingSize/binary,
		Rest/binary >>) when Format == 1 orelse Format == 2 ->
	case parse_tx_tags(Rest) of
		{error, Reason} ->
			{error, Reason};
		{ok, Tags} ->
			{ok, #tx{ format = Format, id = TXID, last_tx = LastTX,
					owner = Owner, target = Target, quantity = Quantity,
					data_size = DataSize, data_root = DataRoot,
					signature = Signature, reward = Reward, data = Data,
					tags = Tags }}
	end;
parse_tx(_Bin) ->
	{error, invalid_tx_input}.

parse_tx_tags(<< TagsLen:16, Rest/binary >>) when TagsLen =< 2048 ->
	parse_tx_tags(TagsLen, Rest, []);
parse_tx_tags(_Bin) ->
	{error, invalid_tx_tags_input}.

parse_tx_tags(0, <<>>, Tags) ->
	{ok, Tags};
parse_tx_tags(N, << TagNameSize:16, TagValueSize:16,
		TagName:TagNameSize/binary, TagValue:TagValueSize/binary, Rest/binary >>, Tags)
		when N > 0 ->
	parse_tx_tags(N - 1, Rest, [{TagName, TagValue} | Tags]);
parse_tx_tags(_N, _Bin, _Tags) ->
	{error, invalid_tx_tag_input}.

tx_to_binary(TX) ->
	Bin = encode_tx(TX),
	TXSize = byte_size(Bin),
	<< TXSize:24, Bin/binary >>.

binary_to_tx(<< Size:24, Bin:Size/binary >>) ->
	parse_tx(Bin);
binary_to_tx(_Rest) ->
	{error, invalid_input}.

block_announcement_to_binary(#block_announcement{ indep_hash = H,
		previous_block = PrevH, tx_prefixes = L, chunk_offset = O }) ->
	<< H:48/binary, PrevH:48/binary, (encode_int(O, 8))/binary,
			(encode_tx_prefixes(L))/binary >>.

encode_tx_prefixes(L) ->
	<< (length(L)):16, (encode_tx_prefixes(L, []))/binary >>.

encode_tx_prefixes([], Encoded) ->
	iolist_to_binary(Encoded);
encode_tx_prefixes([Prefix | Prefixes], Encoded) ->
	encode_tx_prefixes(Prefixes, [<< Prefix:8/binary >> | Encoded]).

binary_to_block_announcement(<< H:48/binary, PrevH:48/binary,
		OffsetSize:8, Offset:(OffsetSize * 8), N:16, Rest/binary >>) ->
	Offset2 = case OffsetSize of 0 -> undefined; _ -> Offset end,
	case parse_tx_prefixes(N, Rest) of
		{error, Reason} ->
			{error, Reason};
		{ok, Prefixes} ->
			{ok, #block_announcement{ indep_hash = H, previous_block = PrevH,
					chunk_offset = Offset2, tx_prefixes = Prefixes }}
	end;
binary_to_block_announcement(_Rest) ->
	{error, invalid_input}.

parse_tx_prefixes(N, Bin) ->
	parse_tx_prefixes(N, Bin, []).

parse_tx_prefixes(0, <<>>, Prefixes) ->
	{ok, Prefixes};
parse_tx_prefixes(N, << Prefix:8/binary, Rest/binary >>, Prefixes) when N > 0 ->
	parse_tx_prefixes(N - 1, Rest, [Prefix | Prefixes]);
parse_tx_prefixes(_N, _Rest, _Prefixes) ->
	{error, invalid_tx_prefixes_input}.

binary_to_block_announcement_response(<< ChunkMissing:8, Rest/binary >>)
		when ChunkMissing == 1 orelse ChunkMissing == 0 ->
	ChunkMissing2 = case ChunkMissing of 1 -> true; _ -> false end,
	case parse_missing_tx_indices(Rest) of
		{ok, Indices} ->
			{ok, #block_announcement_response{ missing_chunk = ChunkMissing2,
					missing_tx_indices = Indices }};
		{error, Reason} ->
			{error, Reason}
	end;
binary_to_block_announcement_response(_Rest) ->
	{error, invalid_block_announcement_response_input}.

parse_missing_tx_indices(Bin) ->
	parse_missing_tx_indices(Bin, []).

parse_missing_tx_indices(<<>>, Indices) ->
	{ok, Indices};
parse_missing_tx_indices(<< Index:16, Rest/binary >>, Indices) ->
	parse_missing_tx_indices(Rest, [Index | Indices]);
parse_missing_tx_indices(_Rest, _Indices) ->
	{error, invalid_missing_tx_indices_input}.

block_announcement_response_to_binary(#block_announcement_response{
		missing_tx_indices = L, missing_chunk = Reply }) ->
	<< (case Reply of true -> 1; _ -> 0 end):8, (encode_missing_tx_indices(L))/binary >>.

encode_missing_tx_indices(L) ->
	encode_missing_tx_indices(L, []).

encode_missing_tx_indices([], Encoded) ->
	iolist_to_binary(Encoded);
encode_missing_tx_indices([Index | Indices], Encoded) ->
	encode_missing_tx_indices(Indices, [<< Index:16 >> | Encoded]).

poa_to_binary(#{ chunk := Chunk, tx_path := TXPath, data_path := DataPath,
		packing := Packing }) ->
	Packing2 = case Packing of unpacked -> <<"unpacked">>;
			spora_2_5 -> <<"spora_2_5">> end,
	<< (encode_bin(Chunk, 24))/binary, (encode_bin(TXPath, 24))/binary,
			(encode_bin(DataPath, 24))/binary, (encode_bin(Packing2, 8))/binary >>.

binary_to_poa(<< ChunkSize:24, Chunk:ChunkSize/binary,
		TXPathSize:24, TXPath:TXPathSize/binary,
		DataPathSize:24, DataPath:DataPathSize/binary,
		PackingSize:8, Packing:PackingSize/binary >>)
		when Packing == <<"unpacked">> orelse Packing == <<"spora_2_5">> ->
	Packing2 = case Packing of <<"unpacked">> -> unpacked;
			<<"spora_2_5">> -> spora_2_5 end,
	{ok, #{ chunk => Chunk, data_path => DataPath, tx_path => TXPath,
			packing => Packing2 }};
binary_to_poa(_Rest) ->
	{error, invalid_input}.

%% @doc Take a JSON struct and produce JSON string.
jsonify(JSONStruct) ->
	iolist_to_binary(jiffy:encode(JSONStruct)).

%% @doc Decode JSON string into a JSON struct.
%% @deprecated In favor of json_decode/1
dejsonify(JSON) ->
	case json_decode(JSON) of
		{ok, V} -> V;
		{error, Reason} -> throw({error, Reason})
	end.

json_decode(JSON) ->
	json_decode(JSON, []).

json_decode(JSON, Opts) ->
	ReturnMaps = proplists:get_bool(return_maps, Opts),
	JiffyOpts =
		case ReturnMaps of
			true -> [return_maps];
			false -> []
		end,
	case catch jiffy:decode(JSON, JiffyOpts) of
		{'EXIT', {Reason, _Stacktrace}} ->
			{error, Reason};
		DecodedJSON ->
			{ok, DecodedJSON}
	end.

%% @doc Convert a block record into a JSON struct.
block_to_json_struct(
	#block{
		nonce = Nonce,
		previous_block = PrevHash,
		timestamp = TimeStamp,
		last_retarget = LastRetarget,
		diff = Diff,
		height = Height,
		hash = Hash,
		indep_hash = IndepHash,
		txs = TXs,
		tx_root = TXRoot,
		wallet_list = WalletList,
		reward_addr = RewardAddr,
		tags = Tags,
		reward_pool = RewardPool,
		weave_size = WeaveSize,
		block_size = BlockSize,
		cumulative_diff = CDiff,
		hash_list_merkle = MR,
		poa = POA
	} = B) ->
	{JSONDiff, JSONCDiff} =
		case Height >= ar_fork:height_1_8() of
			true ->
				{integer_to_binary(Diff), integer_to_binary(CDiff)};
			false ->
				{Diff, CDiff}
	end,
	{JSONRewardPool, JSONBlockSize, JSONWeaveSize} =
		case Height >= ar_fork:height_2_4() of
			true ->
				{integer_to_binary(RewardPool), integer_to_binary(BlockSize),
					integer_to_binary(WeaveSize)};
			false ->
				{RewardPool, BlockSize, WeaveSize}
	end,
	Tags =
		case Height >= ar_fork:height_2_5() of
			true ->
				[ar_util:encode(Tag) || Tag <- Tags];
			false ->
				Tags
		end,
	JSONElements =
		[
			{nonce, ar_util:encode(Nonce)},
			{previous_block, ar_util:encode(PrevHash)},
			{timestamp, TimeStamp},
			{last_retarget, LastRetarget},
			{diff, JSONDiff},
			{height, Height},
			{hash, ar_util:encode(Hash)},
			{indep_hash, ar_util:encode(IndepHash)},
			{txs,
				lists:map(
					fun(TXID) when is_binary(TXID) ->
						ar_util:encode(TXID);
					(TX) ->
						ar_util:encode(TX#tx.id)
					end,
					TXs
				)
			},
			{tx_root, ar_util:encode(TXRoot)},
			{tx_tree, []},
			{wallet_list, ar_util:encode(WalletList)},
			{reward_addr,
				if RewardAddr == unclaimed -> list_to_binary("unclaimed");
				true -> ar_util:encode(RewardAddr)
				end
			},
			{tags, Tags},
			{reward_pool, JSONRewardPool},
			{weave_size, JSONWeaveSize},
			{block_size, JSONBlockSize},
			{cumulative_diff, JSONCDiff},
			{hash_list_merkle, ar_util:encode(MR)},
			{poa, poa_to_json_struct(POA)}
		],
	JSONElements2 =
		case Height < ?FORK_1_6 of
			true ->
				KeysToDelete = [cumulative_diff, hash_list_merkle],
				delete_keys(KeysToDelete, JSONElements);
			false ->
				JSONElements
		end,
	JSONElements3 =
		case Height >= ar_fork:height_2_4() of
			true ->
				delete_keys([tx_tree], JSONElements2);
			false ->
				JSONElements2
		end,
	JSONElements4 =
		case Height >= ar_fork:height_2_5() of
			true ->
				{RateDividend, RateDivisor} = B#block.usd_to_ar_rate,
				{ScheduledRateDividend, ScheduledRateDivisor} = B#block.scheduled_usd_to_ar_rate,
				[
					{usd_to_ar_rate,
						[integer_to_binary(RateDividend), integer_to_binary(RateDivisor)]},
					{scheduled_usd_to_ar_rate,
						[integer_to_binary(ScheduledRateDividend),
							integer_to_binary(ScheduledRateDivisor)]},
					{packing_2_5_threshold, integer_to_binary(B#block.packing_2_5_threshold)},
					{strict_data_split_threshold,
							integer_to_binary(B#block.strict_data_split_threshold)}
					| JSONElements3
				];
			false ->
				JSONElements3
		end,
	{JSONElements4}.

delete_keys([], Proplist) ->
	Proplist;
delete_keys([Key | Keys], Proplist) ->
	delete_keys(
		Keys,
		lists:keydelete(Key, 1, Proplist)
	).

%% @doc Convert parsed JSON blocks fields from a HTTP request into a block.
json_struct_to_block(JSONBlock) when is_binary(JSONBlock) ->
	json_struct_to_block(dejsonify(JSONBlock));
json_struct_to_block({BlockStruct}) ->
	Height = find_value(<<"height">>, BlockStruct),
	true = is_integer(Height),
	Fork_2_5 = ar_fork:height_2_5(),
	TXIDs = find_value(<<"txs">>, BlockStruct),
	WalletList = find_value(<<"wallet_list">>, BlockStruct),
	HashList = find_value(<<"hash_list">>, BlockStruct),
	TagsValue = find_value(<<"tags">>, BlockStruct),
	Tags =
		case Height >= Fork_2_5 of
			true ->
				[ar_util:decode(Tag) || Tag <- TagsValue];
			false ->
				true = (byte_size(list_to_binary(TagsValue)) =< 2048),
				TagsValue
		end,
	Fork_1_8 = ar_fork:height_1_8(),
	CDiff =
		case find_value(<<"cumulative_diff">>, BlockStruct) of
			_ when Height < ?FORK_1_6 -> 0;
			undefined -> 0; % In case it's an invalid block (in the pre-fork format).
			BinaryCDiff when Height >= Fork_1_8 -> binary_to_integer(BinaryCDiff);
			CD -> CD
		end,
	Diff =
		case find_value(<<"diff">>, BlockStruct) of
			BinaryDiff when Height >= Fork_1_8 -> binary_to_integer(BinaryDiff);
			D -> D
		end,
	MR =
		case find_value(<<"hash_list_merkle">>, BlockStruct) of
			_ when Height < ?FORK_1_6 -> <<>>;
			undefined -> <<>>; % In case it's an invalid block (in the pre-fork format).
			R -> ar_util:decode(R)
		end,
	RewardAddr =
		case find_value(<<"reward_addr">>, BlockStruct) of
			<<"unclaimed">> -> unclaimed;
			AddrBinary -> ar_wallet:base64_address_with_optional_checksum_to_decoded_address(AddrBinary)
		end,
	{RewardPool, BlockSize, WeaveSize} =
		case Height >= ar_fork:height_2_4() of
			true ->
				{
					binary_to_integer(find_value(<<"reward_pool">>, BlockStruct)),
					binary_to_integer(find_value(<<"block_size">>, BlockStruct)),
					binary_to_integer(find_value(<<"weave_size">>, BlockStruct))
				};
			false ->
				{
					find_value(<<"reward_pool">>, BlockStruct),
					find_value(<<"block_size">>, BlockStruct),
					find_value(<<"weave_size">>, BlockStruct)
				}
		end,
	{Rate, ScheduledRate, Packing_2_5_Threshold, StrictDataSplitThreshold} =
		case Height >= Fork_2_5 of
			true ->
				[RateDividendBinary, RateDivisorBinary] =
					find_value(<<"usd_to_ar_rate">>, BlockStruct),
				[ScheduledRateDividendBinary, ScheduledRateDivisorBinary] =
					find_value(<<"scheduled_usd_to_ar_rate">>, BlockStruct),
				{{binary_to_integer(RateDividendBinary), binary_to_integer(RateDivisorBinary)},
					{binary_to_integer(ScheduledRateDividendBinary),
						binary_to_integer(ScheduledRateDivisorBinary)},
							binary_to_integer(find_value(<<"packing_2_5_threshold">>,
								BlockStruct)),
							binary_to_integer(find_value(<<"strict_data_split_threshold">>,
								BlockStruct))};
			false ->
				{undefined, undefined, undefined, undefined}
		end,
	Timestamp = find_value(<<"timestamp">>, BlockStruct),
	true = is_integer(Timestamp),
	LastRetarget = find_value(<<"last_retarget">>, BlockStruct),
	true = is_integer(LastRetarget),
	#block{
		nonce = ar_util:decode(find_value(<<"nonce">>, BlockStruct)),
		previous_block = ar_util:decode(find_value(<<"previous_block">>, BlockStruct)),
		timestamp = Timestamp,
		last_retarget = LastRetarget,
		diff = Diff,
		height = Height,
		hash = ar_util:decode(find_value(<<"hash">>, BlockStruct)),
		indep_hash = ar_util:decode(find_value(<<"indep_hash">>, BlockStruct)),
		txs = [ar_util:decode(TXID) || TXID <- TXIDs],
		hash_list =
			case HashList of
				undefined -> unset;
				_		  -> [ar_util:decode(Hash) || Hash <- HashList]
			end,
		wallet_list = ar_util:decode(WalletList),
		reward_addr = RewardAddr,
		tags = Tags,
		reward_pool = RewardPool,
		weave_size = WeaveSize,
		block_size = BlockSize,
		cumulative_diff = CDiff,
		hash_list_merkle = MR,
		tx_root =
			case find_value(<<"tx_root">>, BlockStruct) of
				undefined -> <<>>;
				Root -> ar_util:decode(Root)
			end,
		poa =
			case find_value(<<"poa">>, BlockStruct) of
				undefined -> #poa{};
				POAStruct -> json_struct_to_poa(POAStruct)
			end,
		usd_to_ar_rate = Rate,
		scheduled_usd_to_ar_rate = ScheduledRate,
		packing_2_5_threshold = Packing_2_5_Threshold,
		strict_data_split_threshold = StrictDataSplitThreshold
	}.

%% @doc Convert a transaction record into a JSON struct.
tx_to_json_struct(
	#tx{
		id = ID,
		format = Format,
		last_tx = Last,
		owner = Owner,
		tags = Tags,
		target = Target,
		quantity = Quantity,
		data = Data,
		reward = Reward,
		signature = Sig,
		data_size = DataSize,
		data_root = DataRoot
	}) ->
	{
		[
			{format,
				case Format of
					undefined ->
						1;
					_ ->
						Format
				end},
			{id, ar_util:encode(ID)},
			{last_tx, ar_util:encode(Last)},
			{owner, ar_util:encode(Owner)},
			{tags,
				lists:map(
					fun({Name, Value}) ->
						{
							[
								{name, ar_util:encode(Name)},
								{value, ar_util:encode(Value)}
							]
						}
					end,
					Tags
				)
			},
			{target, ar_util:encode(Target)},
			{quantity, integer_to_binary(Quantity)},
			{data, ar_util:encode(Data)},
			{data_size, integer_to_binary(DataSize)},
			{data_tree, []},
			{data_root, ar_util:encode(DataRoot)},
			{reward, integer_to_binary(Reward)},
			{signature, ar_util:encode(Sig)}
		]
	}.

poa_to_json_struct(POA) ->
	{[
		{option, integer_to_binary(POA#poa.option)},
		{tx_path, ar_util:encode(POA#poa.tx_path)},
		{data_path, ar_util:encode(POA#poa.data_path)},
		{chunk, ar_util:encode(POA#poa.chunk)}
	]}.

json_struct_to_poa({JSONStruct}) ->
	#poa{
		option = binary_to_integer(find_value(<<"option">>, JSONStruct)),
		tx_path = ar_util:decode(find_value(<<"tx_path">>, JSONStruct)),
		data_path = ar_util:decode(find_value(<<"data_path">>, JSONStruct)),
		chunk = ar_util:decode(find_value(<<"chunk">>, JSONStruct))
	}.

%% @doc Convert parsed JSON tx fields from a HTTP request into a
%% transaction record.
json_struct_to_tx(JSONTX) when is_binary(JSONTX) ->
	json_struct_to_tx(dejsonify(JSONTX));
json_struct_to_tx({TXStruct}) ->
	json_struct_to_tx(TXStruct, true).

json_struct_to_v1_tx(JSONTX) when is_binary(JSONTX) ->
	{TXStruct} = dejsonify(JSONTX),
	json_struct_to_tx(TXStruct, false).

json_struct_to_tx(TXStruct, ComputeDataSize) ->
	Tags = case find_value(<<"tags">>, TXStruct) of
		undefined -> [];
		Xs -> Xs
	end,
	Data = ar_util:decode(find_value(<<"data">>, TXStruct)),
	Format =
		case find_value(<<"format">>, TXStruct) of
			undefined -> 1;
			N when is_integer(N) -> N;
			N when is_binary(N) -> binary_to_integer(N)
		end,
	#tx{
		format = Format,
		id = ar_util:decode(find_value(<<"id">>, TXStruct)),
		last_tx = ar_util:decode(find_value(<<"last_tx">>, TXStruct)),
		owner = ar_util:decode(find_value(<<"owner">>, TXStruct)),
		tags = [{ar_util:decode(Name), ar_util:decode(Value)}
				%% Only the elements matching this pattern are included in the list.
				|| {[{<<"name">>, Name}, {<<"value">>, Value}]} <- Tags],
		target = ar_wallet:base64_address_with_optional_checksum_to_decoded_address(find_value(<<"target">>, TXStruct)),
		quantity = binary_to_integer(find_value(<<"quantity">>, TXStruct)),
		data = Data,
		reward = binary_to_integer(find_value(<<"reward">>, TXStruct)),
		signature = ar_util:decode(find_value(<<"signature">>, TXStruct)),
		data_size = parse_data_size(Format, TXStruct, Data, ComputeDataSize),
		data_root =
			case find_value(<<"data_root">>, TXStruct) of
				undefined -> <<>>;
				DR -> ar_util:decode(DR)
			end
	}.

parse_data_size(1, _TXStruct, Data, true) ->
	byte_size(Data);
parse_data_size(_Format, TXStruct, _Data, _ComputeDataSize) ->
	binary_to_integer(find_value(<<"data_size">>, TXStruct)).

etf_to_wallet_chunk_response(ETF) ->
	catch etf_to_wallet_chunk_response_unsafe(ETF).

etf_to_wallet_chunk_response_unsafe(ETF) ->
	#{ next_cursor := NextCursor, wallets := Wallets } = binary_to_term(ETF, [safe]),
	true = is_binary(NextCursor) orelse NextCursor == last,
	lists:foreach(
		fun({Addr, {Balance, LastTX}})
				when is_binary(Addr), is_binary(LastTX), is_integer(Balance) ->
			ok
		end,
		Wallets
	),
	{ok, #{ next_cursor => NextCursor, wallets => Wallets }}.

%% @doc Convert a wallet list into a JSON struct.
%% The order of the wallets is somewhat weird for historical reasons. If the reward address,
%% appears in the list for the first time, it is placed on the first position. Except for that,
%% wallets are sorted in the alphabetical order.
wallet_list_to_json_struct(RewardAddr, IsRewardAddrNew, WL) ->
	List = ar_patricia_tree:foldr(
		fun(Addr, {Balance, LastTX}, Acc) ->
			case Addr == RewardAddr andalso IsRewardAddrNew of
				true ->
					Acc;
				false ->
					[wallet_to_json_struct({Addr, Balance, LastTX}) | Acc]
			end
		end,
		[],
		WL
	),
	case {ar_patricia_tree:get(RewardAddr, WL), IsRewardAddrNew} of
		{not_found, _} ->
			List;
		{_, false} ->
			List;
		{{Balance, LastTX}, true} ->
			%% Place the reward wallet first, for backwards-compatibility.
			[wallet_to_json_struct({RewardAddr, Balance, LastTX}) | List]
	end.

wallet_to_json_struct({Address, Balance, Last}) ->
	{
		[
			{address, ar_util:encode(Address)},
			{balance, list_to_binary(integer_to_list(Balance))},
			{last_tx, ar_util:encode(Last)}
		]
	}.

%% @doc Convert parsed JSON from fields into a valid wallet list.
json_struct_to_wallet_list(JSON) when is_binary(JSON) ->
	json_struct_to_wallet_list(dejsonify(JSON));
json_struct_to_wallet_list(WalletsStruct) ->
	lists:foldl(
		fun(WalletStruct, Acc) ->
			{Address, Balance, LastTX} = json_struct_to_wallet(WalletStruct),
			ar_patricia_tree:insert(Address, {Balance, LastTX}, Acc)
		end,
		ar_patricia_tree:new(),
		WalletsStruct
	).

json_struct_to_wallet({Wallet}) ->
	Address = ar_util:decode(find_value(<<"address">>, Wallet)),
	Balance = binary_to_integer(find_value(<<"balance">>, Wallet)),
	Last = ar_util:decode(find_value(<<"last_tx">>, Wallet)),
	{Address, Balance, Last}.

%% @doc Find the value associated with a key in parsed a JSON structure list.
find_value(Key, List) ->
	case lists:keyfind(Key, 1, List) of
		{Key, Val} -> Val;
		false -> undefined
	end.

%% @doc Convert an ARQL query into a JSON struct
query_to_json_struct({Op, Expr1, Expr2}) ->
	{
		[
			{op, list_to_binary(atom_to_list(Op))},
			{expr1, query_to_json_struct(Expr1)},
			{expr2, query_to_json_struct(Expr2)}
		]
	};
query_to_json_struct(Expr) ->
	Expr.

%% @doc Convert parsed JSON from fields into an internal ARQL query.
json_struct_to_query(QueryJSON) ->
	case json_decode(QueryJSON) of
		{ok, Decoded} ->
			{ok, do_json_struct_to_query(Decoded)};
		{error, _} ->
			{error, invalid_json}
	end.

do_json_struct_to_query({Query}) ->
	{
		list_to_existing_atom(binary_to_list(find_value(<<"op">>, Query))),
		do_json_struct_to_query(find_value(<<"expr1">>, Query)),
		do_json_struct_to_query(find_value(<<"expr2">>, Query))
	};
do_json_struct_to_query(Query) ->
	Query.

%% @doc Generate a JSON structure representing a block index.
block_index_to_json_struct(BI) ->
	lists:map(
		fun
			({BH, WeaveSize, TXRoot}) ->
				Keys1 = [{<<"hash">>, ar_util:encode(BH)}],
				Keys2 =
					case WeaveSize of
						not_set ->
							Keys1;
						_ ->
							[{<<"weave_size">>, integer_to_binary(WeaveSize)} | Keys1]
					end,
				Keys3 =
					case TXRoot of
						not_set ->
							Keys2;
						_ ->
							[{<<"tx_root">>, ar_util:encode(TXRoot)} | Keys2]
					end,
				{Keys3};
			(BH) ->
				ar_util:encode(BH)
		end,
		BI
	).

%% @doc Convert a JSON structure into a block index.
json_struct_to_block_index(JSONStruct) ->
	lists:map(
		fun
			(Hash) when is_binary(Hash) ->
				ar_util:decode(Hash);
			({JSON}) ->
				Hash = ar_util:decode(find_value(<<"hash">>, JSON)),
				WeaveSize =
					case find_value(<<"weave_size">>, JSON) of
						undefined ->
							not_set;
						WS ->
							binary_to_integer(WS)
					end,
				TXRoot =
					case find_value(<<"tx_root">>, JSON) of
						undefined ->
							not_set;
						R ->
							ar_util:decode(R)
					end,
				{Hash, WeaveSize, TXRoot}
		end,
		JSONStruct
	).

chunk_proof_to_json_map(Map) ->
	#{ chunk := Chunk, tx_path := TXPath, data_path := DataPath, packing := Packing } = Map,
	SerializedPacking =
		case Packing of
			unpacked ->
				<<"unpacked">>;
			spora_2_5 ->
				<<"spora_2_5">>
		end,
	#{
		chunk => ar_util:encode(Chunk),
		tx_path => ar_util:encode(TXPath),
		data_path => ar_util:encode(DataPath),
		packing => SerializedPacking
	}.

json_map_to_chunk_proof(JSON) ->
	Map = #{
		data_root => ar_util:decode(maps:get(<<"data_root">>, JSON, <<>>)),
		chunk => ar_util:decode(maps:get(<<"chunk">>, JSON)),
		data_path => ar_util:decode(maps:get(<<"data_path">>, JSON)),
		tx_path => ar_util:decode(maps:get(<<"tx_path">>, JSON, <<>>)),
		data_size => binary_to_integer(maps:get(<<"data_size">>, JSON, <<"0">>))
	},
	Map2 =
		case maps:get(<<"packing">>, JSON, <<"unpacked">>) of
			<<"unpacked">> ->
				maps:put(packing, unpacked, Map);
			<<"spora_2_5">> ->
				maps:put(packing, spora_2_5, Map);
			_ ->
				error(unsupported_packing)
		end,
	case maps:get(<<"offset">>, JSON, none) of
		none ->
			Map2;
		Offset ->
			Map2#{ offset => binary_to_integer(Offset) }
	end.

%%% Tests: ar_serialize

block_to_binary_test() ->
	Dir = filename:dirname(?FILE),
	BlockFixtureDir = filename:join(Dir, "../test/fixtures/blocks"),
	TXFixtureDir = filename:join(Dir, "../test/fixtures/txs"),
	{ok, BlockFixtures} = file:list_dir(BlockFixtureDir),
	test_block_to_binary([filename:join(BlockFixtureDir, Name)
			|| Name <- BlockFixtures], TXFixtureDir).

test_block_to_binary([], _TXFixtureDir) ->
	ok;
test_block_to_binary([Fixture | Fixtures], TXFixtureDir) ->
	{ok, Bin} = file:read_file(Fixture),
	B = binary_to_term(Bin),
	?debugFmt("Block ~s, height ~B.~n", [ar_util:encode(B#block.indep_hash),
			B#block.height]),
	test_block_to_binary(B),
	RandomTags = [crypto:strong_rand_bytes(rand:uniform(2048))
			|| _ <- lists:seq(1, rand:uniform(2048))],
	B2 = B#block{ tags = RandomTags },
	test_block_to_binary(B2),
	B3 = B#block{ reward_addr = unclaimed },
	test_block_to_binary(B3),
	{ok, TXFixtures} = file:list_dir(TXFixtureDir),
	TXs =
		lists:foldl(
			fun(TXFixture, Acc) ->
				{ok, TXBin} = file:read_file(filename:join(TXFixtureDir, TXFixture)),
				TX = binary_to_term(TXBin),
				maps:put(TX#tx.id, TX, Acc)
			end,
			#{},
			TXFixtures),
	BlockTXs = [maps:get(TXID, TXs) || TXID <- B#block.txs],
	B4 = B#block{ txs = BlockTXs },
	test_block_to_binary(B4),
	BlockTXs2 = [case rand:uniform(2) of 1 -> TX#tx.id; _ -> TX end
			|| TX <- BlockTXs],
	B5 = B#block{ txs = BlockTXs2 },
	test_block_to_binary(B5),
	TXIDs = [TX#tx.id || TX <- BlockTXs],
	B6 = B#block{ txs = TXIDs },
	test_block_to_binary(B6),
	test_block_to_binary(Fixtures, TXFixtureDir).

test_block_to_binary(B) ->
	{ok, B2} = binary_to_block(block_to_binary(B)),
	?assertEqual(B#block{ txs = [] }, B2#block{ txs = [] }),
	?assertEqual(true, compare_txs(B#block.txs, B2#block.txs)),
	lists:foreach(
		fun	(TX) when is_record(TX, tx)->
				?assertEqual({ok, TX}, binary_to_tx(tx_to_binary(TX)));
			(_TXID) ->
				ok
		end,
		B#block.txs
	).

compare_txs([TXID | TXs], [#tx{ id = TXID } | TXs2]) ->
	compare_txs(TXs, TXs2);
compare_txs([#tx{ id = TXID } | TXs], [TXID | TXs2]) ->
	compare_txs(TXs, TXs2);
compare_txs([TXID | TXs], [TXID | TXs2]) ->
	compare_txs(TXs, TXs2);
compare_txs([], []) ->
	true;
compare_txs(_TXs, _TXs2) ->
	false.

block_announcement_to_binary_test() ->
	A = #block_announcement{ indep_hash = crypto:strong_rand_bytes(48),
			previous_block = crypto:strong_rand_bytes(48) },
	?assertEqual({ok, A}, binary_to_block_announcement(
			block_announcement_to_binary(A))),
	A2 = A#block_announcement{ chunk_offset = 0 },
	?assertEqual({ok, A2}, binary_to_block_announcement(
			block_announcement_to_binary(A2))),
	A3 = A#block_announcement{ chunk_offset = 1000000000000000000000 },
	?assertEqual({ok, A3}, binary_to_block_announcement(
			block_announcement_to_binary(A3))),
	A4 = A3#block_announcement{ tx_prefixes = [crypto:strong_rand_bytes(8)
			|| _ <- lists:seq(1, 1000)] },
	?assertEqual({ok, A4}, binary_to_block_announcement(
			block_announcement_to_binary(A4))).

block_announcement_response_to_binary_test() ->
	A = #block_announcement_response{},
	?assertEqual({ok, A}, binary_to_block_announcement_response(
			block_announcement_response_to_binary(A))),
	A2 = A#block_announcement_response{ missing_chunk = true,
			missing_tx_indices = lists:seq(0, 999) },
	?assertEqual({ok, A2}, binary_to_block_announcement_response(
			block_announcement_response_to_binary(A2))).

poa_to_binary_test() ->
	Proof = #{ chunk => crypto:strong_rand_bytes(1), data_path => <<>>,
			tx_path => <<>>, packing => unpacked },
	?assertEqual({ok, Proof}, binary_to_poa(poa_to_binary(Proof))),
	Proof2 = Proof#{ chunk => crypto:strong_rand_bytes(256 * 1024) },
	?assertEqual({ok, Proof2}, binary_to_poa(poa_to_binary(Proof2))),
	Proof3 = Proof2#{ data_path => crypto:strong_rand_bytes(1024),
			packing => spora_2_5, tx_path => crypto:strong_rand_bytes(1024) },
	?assertEqual({ok, Proof3}, binary_to_poa(poa_to_binary(Proof3))).

%% @doc Convert a new block into JSON and back, ensure the result is the same.
block_roundtrip_test() ->
	[B] = ar_weave:init(),
	JSONStruct = jsonify(block_to_json_struct(B)),
	BRes = json_struct_to_block(JSONStruct),
	?assertEqual(B, BRes#block{ hash_list = B#block.hash_list, size_tagged_txs = [] }).

%% @doc Convert a new TX into JSON and back, ensure the result is the same.
tx_roundtrip_test() ->
	TXBase = ar_tx:new(<<"test">>),
	TX =
		TXBase#tx {
			format = 2,
			tags = [{<<"Name1">>, <<"Value1">>}],
			data_root = << 0:256 >>
		},
	JsonTX = jsonify(tx_to_json_struct(TX)),
	?assertEqual(
		TX,
		json_struct_to_tx(JsonTX)
	).

wallet_list_roundtrip_test() ->
	[B] = ar_weave:init(),
	{ok, WL} = ar_storage:read_wallet_list(B#block.wallet_list),
	JSONWL = jsonify(wallet_list_to_json_struct(B#block.reward_addr, false, WL)),
	ExpectedWL = ar_patricia_tree:foldr(fun(K, V, Acc) -> [{K, V} | Acc] end, [], WL),
	ActualWL = ar_patricia_tree:foldr(
		fun(K, V, Acc) -> [{K, V} | Acc] end, [], json_struct_to_wallet_list(JSONWL)
	),
	?assertEqual(ExpectedWL, ActualWL).

block_index_roundtrip_test() ->
	[B] = ar_weave:init(),
	HL = [B#block.indep_hash, B#block.indep_hash],
	JSONHL = jsonify(block_index_to_json_struct(HL)),
	HL = json_struct_to_block_index(dejsonify(JSONHL)),
	BI = [{B#block.indep_hash, 1, <<"Root">>}, {B#block.indep_hash, 2, <<>>}],
	JSONBI = jsonify(block_index_to_json_struct(BI)),
	BI = json_struct_to_block_index(dejsonify(JSONBI)).

query_roundtrip_test() ->
	Query = {'equals', <<"TestName">>, <<"TestVal">>},
	QueryJSON = ar_serialize:jsonify(
		ar_serialize:query_to_json_struct(
			Query
		)
	),
	?assertEqual({ok, Query}, ar_serialize:json_struct_to_query(QueryJSON)).
