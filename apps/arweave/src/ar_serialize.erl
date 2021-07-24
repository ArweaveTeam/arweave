-module(ar_serialize).

-export([
	json_struct_to_block/1,
	block_to_json_struct/1,
	json_struct_to_poa/1,
	poa_to_json_struct/1,
	tx_to_json_struct/1,
	json_struct_to_tx/1, json_struct_to_v1_tx/1,
	etf_to_wallet_chunk_response/1,
	wallet_list_to_json_struct/3,
	wallet_to_json_struct/1,
	json_struct_to_wallet_list/1,
	block_index_to_json_struct/1,
	json_struct_to_block_index/1,
	jsonify/1,
	dejsonify/1,
	json_decode/1,
	json_decode/2,
	query_to_json_struct/1,
	json_struct_to_query/1,
	chunk_proof_to_json_map/1,
	json_map_to_chunk_proof/1,
	signature_type_to_binary/1,
	binary_to_signature_type/1
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Module containing serialisation/deserialisation utility functions
%%% for use in HTTP server.

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
							integer_to_binary(ScheduledRateDivisor)]}
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
	TXIDs = find_value(<<"txs">>, BlockStruct),
	WalletList = find_value(<<"wallet_list">>, BlockStruct),
	HashList = find_value(<<"hash_list">>, BlockStruct),
	Tags = find_value(<<"tags">>, BlockStruct),
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
			AddrBinary -> ar_util:decode(AddrBinary)
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
	{Rate, ScheduledRate} =
		case Height >= ar_fork:height_2_5() of
			true ->
				[RateDividendBinary, RateDivisorBinary] =
					find_value(<<"usd_to_ar_rate">>, BlockStruct),
				[ScheduledRateDividendBinary, ScheduledRateDivisorBinary] =
					find_value(<<"scheduled_usd_to_ar_rate">>, BlockStruct),
				{{binary_to_integer(RateDividendBinary), binary_to_integer(RateDivisorBinary)},
					{binary_to_integer(ScheduledRateDividendBinary),
						binary_to_integer(ScheduledRateDivisorBinary)}};
			false ->
				{undefined, undefined}
		end,
	#block{
		nonce = ar_util:decode(find_value(<<"nonce">>, BlockStruct)),
		previous_block =
			ar_util:decode(
				find_value(<<"previous_block">>, BlockStruct)
			),
		timestamp = find_value(<<"timestamp">>, BlockStruct),
		last_retarget = find_value(<<"last_retarget">>, BlockStruct),
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
		scheduled_usd_to_ar_rate = ScheduledRate
	}.

%% @doc Convert a transaction record into a JSON struct.
tx_to_json_struct(
	#tx {
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
		signature_type = SigType,
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
			{signature, ar_util:encode(Sig)},
			{signature_type, signature_type_to_binary(SigType)}
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
	#poa {
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
	#tx {
		format = Format,
		id = ar_util:decode(find_value(<<"id">>, TXStruct)),
		last_tx = ar_util:decode(find_value(<<"last_tx">>, TXStruct)),
		owner = ar_util:decode(find_value(<<"owner">>, TXStruct)),
		tags =
			[
					{ar_util:decode(Name), ar_util:decode(Value)}
				||
					{[{<<"name">>, Name}, {<<"value">>, Value}]} <- Tags
			],
		target = ar_util:decode(find_value(<<"target">>, TXStruct)),
		quantity = binary_to_integer(find_value(<<"quantity">>, TXStruct)),
		data = Data,
		reward = binary_to_integer(find_value(<<"reward">>, TXStruct)),
		signature = ar_util:decode(find_value(<<"signature">>, TXStruct)),
		signature_type = binary_to_signature_type(find_value(<<"signature_type">>, TXStruct)),
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
			aes_256_cbc ->
				<<"aes_256_cbc">>
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
			<<"aes_256_cbc">> ->
				maps:put(packing, aes_256_cbc, Map)
		end,
	case maps:get(<<"offset">>, JSON, none) of
		none ->
			Map2;
		Offset ->
			Map2#{ offset => binary_to_integer(Offset) }
	end.

signature_type_to_binary(SigType) ->
	case SigType of
		{?RSA_SIGN_ALG, 65537} -> <<"PS256_65537">>;
		{?ECDSA_SIGN_ALG, secp256k1} -> <<"ES256K">>;
		{?EDDSA_SIGN_ALG, ed25519} -> <<"Ed25519">>
	end.

binary_to_signature_type(List) ->
	case List of
		undefined -> ?DEFAULT_KEY_TYPE;
		<<"PS256_65537">> -> {?RSA_SIGN_ALG, 65537};
		<<"ES256K">> -> {?ECDSA_SIGN_ALG, secp256k1};
		<<"Ed25519">> -> {?EDDSA_SIGN_ALG, ed25519};
		_ -> throw(invalid_signature_type)
	end.

%%% Tests: ar_serialize

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
			data_root = << 0:256 >>,
			signature_type = ?DEFAULT_KEY_TYPE
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
