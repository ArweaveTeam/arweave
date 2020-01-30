-module(ar_serialize).
-export([full_block_to_json_struct/1, json_struct_to_full_block/1]).
-export([json_struct_to_block/1, block_to_json_struct/1]).
-export([json_struct_to_poa/1, poa_to_json_struct/1]).
-export([tx_to_json_struct/1, json_struct_to_tx/1]).
-export([wallet_list_to_json_struct/1, json_struct_to_wallet_list/1]).
-export([block_index_to_json_struct/1, json_struct_to_block_index/1]).
-export([jsonify/1, dejsonify/1, json_decode/1, json_decode/2]).
-export([query_to_json_struct/1, json_struct_to_query/1]).
-include("ar.hrl").
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
	try
		{ok, jiffy:decode(JSON, JiffyOpts)}
	catch
		{error, Reason} -> {error, Reason}
	end.

%% @doc Convert a block record into a JSON struct.
block_to_json_struct(
	#block {
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
		tx_tree = TXTree,
		wallet_list = WalletList,
		reward_addr = RewardAddr,
		tags = Tags,
		reward_pool = RewardPool,
		weave_size = WeaveSize,
		block_size = BlockSize,
		cumulative_diff = CDiff,
		hash_list_merkle = MR,
		poa = POA,
		votables = Votables
	}) ->
	{JSONDiff, JSONCDiff} = case ar_fork:height_1_8() of
		H when Height >= H ->
			{integer_to_binary(Diff), integer_to_binary(CDiff)};
		_ ->
			{Diff, CDiff}
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
			{tx_tree, tree_to_json_struct(TXTree)},
			{wallet_list,
				case is_binary(WalletList) of
					true -> ar_util:encode(WalletList);
					false ->
						lists:map(
							fun({Wallet, Qty, Last}) ->
								{
									[
										{wallet, ar_util:encode(Wallet)},
										{quantity, Qty},
										{last_tx, ar_util:encode(Last)}
									]
								}
							end,
							WalletList
						)
				end
			},
			{reward_addr,
				if RewardAddr == unclaimed -> list_to_binary("unclaimed");
				true -> ar_util:encode(RewardAddr)
				end
			},
			{tags, Tags},
			{reward_pool, RewardPool},
			{weave_size, WeaveSize},
			{block_size, BlockSize},
			{cumulative_diff, JSONCDiff},
			{hash_list_merkle, ar_util:encode(MR)},
			{poa, poa_to_json_struct(POA)},
			{votables,
				[
					{[{name, list_to_binary(Name)}, {value, integer_to_binary(Value)}]}
				||
					{Name, Value} <- Votables
				]
			}
		],
	case Height < ?FORK_1_6 of
		true ->
			KeysToDelete = [cumulative_diff, hash_list_merkle],
			{delete_keys(KeysToDelete, JSONElements)};
		false ->
			{JSONElements}
	end.

delete_keys([], Proplist) ->
	Proplist;
delete_keys([Key | Keys], Proplist) ->
	delete_keys(
		Keys,
		lists:keydelete(Key, 1, Proplist)
	).

%% @doc Convert a full block record into a JSON struct.
full_block_to_json_struct(B = #block { txs = TXs }) ->
	{JSONElements} = block_to_json_struct(B#block { txs = [] }),
	{
		[
			{txs, lists:map(fun tx_safely_to_json_struct/1, TXs)}
		|
			lists:keydelete(txs, 1, JSONElements)
		]
	}.

%% @doc Convert parsed JSON blocks fields from a HTTP request into a block.
json_struct_to_block(JSONBlock) when is_binary(JSONBlock) ->
	json_struct_to_block(dejsonify(JSONBlock));
json_struct_to_block({BlockStruct}) ->
	Height = find_value(<<"height">>, BlockStruct),
	Votables =
		case find_value(<<"votables">>, BlockStruct) of
			undefined -> [];
			Struct ->
				lists:map(
					fun({Props}) ->
						{
							binary_to_list(find_value(<<"name">>, Props)),
							binary_to_integer(find_value(<<"value">>, Props))
						}
					end,
					Struct
				)
		end,
	TXs = find_value(<<"txs">>, BlockStruct),
	WalletList = find_value(<<"wallet_list">>, BlockStruct),
	HashList = find_value(<<"hash_list">>, BlockStruct),
	Tags = find_value(<<"tags">>, BlockStruct),
	Fork_1_8 = ar_fork:height_1_8(),
	CDiff = case find_value(<<"cumulative_diff">>, BlockStruct) of
		_ when Height < ?FORK_1_6 -> 0;
		undefined -> 0; % In case it's an invalid block (in the pre-fork format)
		BinaryCDiff when Height >= Fork_1_8 -> binary_to_integer(BinaryCDiff);
		CD -> CD
	end,
	Diff = case find_value(<<"diff">>, BlockStruct) of
		BinaryDiff when Height >= Fork_1_8 -> binary_to_integer(BinaryDiff);
		D -> D
	end,
	MR = case find_value(<<"hash_list_merkle">>, BlockStruct) of
		_ when Height < ?FORK_1_6 -> <<>>;
		undefined -> <<>>; % In case it's an invalid block (in the pre-fork format)
		R -> ar_util:decode(R)
	end,
	#block {
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
		txs =
			lists:map(
			fun (TX) when is_binary(TX) ->
					ar_util:decode(TX);
				(TX) ->
					Temp = json_struct_to_tx(TX),
					Temp#tx.id
			end,
			TXs
		),
		hash_list =
			case HashList of
				undefined -> unset;
				_		  -> [ar_util:decode(Hash) || Hash <- HashList]
			end,
		wallet_list =
			case is_binary(WalletList) of
				false ->
					[
						{
							ar_util:decode(Wallet),
							Qty,
							ar_util:decode(Last)
						}
					||
						{
							[
								{<<"wallet">>, Wallet},
								{<<"quantity">>, Qty},
								{<<"last_tx">>, Last}
							]
						}
						<- WalletList
					];
				true -> ar_util:decode(WalletList)
			end,
		reward_addr =
			case find_value(<<"reward_addr">>, BlockStruct) of
				<<"unclaimed">> -> unclaimed;
				StrAddr -> ar_util:decode(StrAddr)
			end,
		tags = Tags,
		reward_pool = find_value(<<"reward_pool">>, BlockStruct),
		weave_size = find_value(<<"weave_size">>, BlockStruct),
		block_size = find_value(<<"block_size">>, BlockStruct),
		cumulative_diff = CDiff,
		hash_list_merkle = MR,
		tx_root =
			case find_value(<<"tx_root">>, BlockStruct) of
				undefined -> <<>>;
				Root -> ar_util:decode(Root)
			end,
		poa =
			case find_value(<<"poa">>, BlockStruct) of
				undefined -> undefined;
				POAStruct -> json_struct_to_poa(POAStruct)
			end,
		tx_tree =
			case find_value(<<"tx_tree">>, BlockStruct) of
				undefined -> [];
				POAStruct -> json_struct_to_tree(POAStruct)
			end,
		votables = Votables
	}.

%% @doc Convert parsed JSON blocks fields from a HTTP request into a
%% full block record.
json_struct_to_full_block(JSON) when is_binary(JSON) ->
	json_struct_to_full_block(dejsonify(JSON));
json_struct_to_full_block({BlockStruct}) ->
	TXs = find_value(<<"txs">>, BlockStruct),
	TempStruct =
		{
			[
				{<<"txs">>, []}
			|
				lists:keydelete(<<"txs">>, 1, BlockStruct)
			]
		},
	B = json_struct_to_block(TempStruct),
	B#block {
		txs = lists:map(fun json_struct_to_tx/1, TXs)
	}.

tx_safely_to_json_struct(TX) when is_record(TX, tx) ->
	tx_to_json_struct(TX);
tx_safely_to_json_struct(TX) when is_binary(TX) ->
	tx_to_json_struct(ar_storage:read_tx(TX)).

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
		data_size = DataSize,
		data_tree = DataTree,
		data_root = DataRoot
	}) ->
	{
		[
			{format, Format},
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
			{data_tree, tree_to_json_struct(DataTree)},
			{data_root, ar_util:encode(DataRoot)},
			{reward, integer_to_binary(Reward)},
			{signature, ar_util:encode(Sig)}
		]
	}.

%% @doc Transform proofs of access into/out of JSON format.
poa_to_json_struct(undefined) -> <<"undefined">>;
poa_to_json_struct(B) when is_record(B, block) -> <<"undefined">>;
poa_to_json_struct(POA) ->
	{[
		{option, POA#poa.option},
		{recall_block,
			block_to_json_struct(
				(POA#poa.recall_block)#block {
					poa = undefined,
					wallet_list =
						ar_block:hash_wallet_list(
							(POA#poa.recall_block)#block.wallet_list
						),
					hash_list = []
				}
			)
		},
		{tx_path, ar_util:encode(POA#poa.tx_path)},
		{tx,
			case POA#poa.tx of
				undefined -> <<"undefined">>;
				TX -> tx_to_json_struct(TX)
			end
		},
		{data_path, ar_util:encode(POA#poa.data_path)},
		{chunk, ar_util:encode(POA#poa.chunk)}
	]}.

json_struct_to_poa(<<"undefined">>) -> undefined;
json_struct_to_poa({JSONStruct}) ->
	#poa {
		option = find_value(<<"option">>, JSONStruct),
		recall_block = json_struct_to_block(find_value(<<"recall_block">>, JSONStruct)),
		tx_path = ar_util:decode(find_value(<<"tx_path">>, JSONStruct)),
		tx =
			case find_value(<<"tx">>, JSONStruct) of
				<<"undefined">> -> undefined;
				TXStruct -> json_struct_to_tx(TXStruct)
			end,
		data_path = ar_util:decode(find_value(<<"data_path">>, JSONStruct)),
		chunk = ar_util:decode(find_value(<<"chunk">>, JSONStruct))
	}.

%% @doc Transform merkle trees to and from JSON objects.
%% At the moment, just drop it.
tree_to_json_struct(_) -> [].

json_struct_to_tree(_) -> [].

%% @doc Convert parsed JSON tx fields from a HTTP request into a
%% transaction record.
json_struct_to_tx(JSONTX) when is_binary(JSONTX) ->
	json_struct_to_tx(dejsonify(JSONTX));
json_struct_to_tx({TXStruct}) ->
	Tags = case find_value(<<"tags">>, TXStruct) of
		undefined -> [];
		Xs -> Xs
	end,
	Data = ar_util:decode(find_value(<<"data">>, TXStruct)),
	#tx {
		format =
			case find_value(<<"format">>, TXStruct) of
				undefined -> 1;
				N -> N
			end,
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
		data_size =
			case find_value(<<"data_size">>, TXStruct) of
				undefined -> byte_size(Data);
				X -> binary_to_integer(X)
			end,
		data_tree =
			case find_value(<<"data_tree">>, TXStruct) of
				undefined -> [];
				T -> json_struct_to_tree(T)
			end,
		data_root =
			case find_value(<<"data_root">>, TXStruct) of
				undefined -> <<>>;
				DR -> ar_util:decode(DR)
			end
	}.

%% @doc Convert a wallet list into a JSON struct.
wallet_list_to_json_struct([]) -> [];
wallet_list_to_json_struct([Wallet | WalletList]) ->
	EncWallet = wallet_to_json_struct(Wallet),
	[EncWallet | wallet_list_to_json_struct(WalletList)].
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
	lists:map(fun json_struct_to_wallet/1, WalletsStruct).

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
		fun({BH, WeaveSize}) ->
			{
				[
					{<<"hash">>, ar_util:encode(BH)},
					{<<"weave_size">>, integer_to_binary(WeaveSize)}
				]
			};
		   (BH) -> ar_util:encode(BH)
		end,
		BI
	).

%% @doc Convert a JSON structure into a block index.
json_struct_to_block_index(JSONStruct) ->
	lists:map(
		fun(Hash) when is_binary(Hash) ->
				ar_util:decode(Hash);
		   ({JSON}) ->
				Hash = ar_util:decode(find_value(<<"hash">>, JSON)),
				WeaveSize = binary_to_integer(find_value(<<"weave_size">>, JSON)),
				{Hash, WeaveSize}
		end,
		JSONStruct
	).

%%% Tests: ar_serialize

%% @doc Convert a new block into JSON and back, ensure the result is the same.
block_roundtrip_test() ->
	[B] = ar_weave:init(),
	JSONStruct = jsonify(block_to_json_struct(B)),
	BRes = json_struct_to_block(JSONStruct),
	?assertEqual(
		B,
		BRes#block { hash_list = B#block.hash_list }
	).

%% @doc Convert a new block into JSON and back, ensure the result is the same.
%% Input contains transaction, output only transaction IDs.
block_tx_roundtrip_test() ->
	[B] = ar_weave:init(),
	TXBase = ar_tx:new(<<"test">>),
	B2 = B#block {txs = [TXBase], tags = ["hello", "world", "example"] },
	JsonB = jsonify(full_block_to_json_struct(B2)),
	BRes = json_struct_to_block(JsonB),
	?assertEqual(TXBase#tx.id, hd(BRes#block.txs)),
	?assertEqual(B2#block.hash, BRes#block.hash).

%% @doc Convert a new block into JSON and back, ensure the result is the same.
full_block_roundtrip_test() ->
	[B] = ar_weave:init(),
	TXBase = ar_tx:new(<<"test">>),
	B2 = B#block {txs = [TXBase], tags = ["hello", "world", "example"] },
	JsonB = jsonify(full_block_to_json_struct(B2)),
	BRes = json_struct_to_full_block(JsonB),
	?assertEqual(
		B2,
		BRes#block { hash_list = B#block.hash_list }
	).

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
	WL = B#block.wallet_list,
	JSONWL = jsonify(wallet_list_to_json_struct(WL)),
	WL = json_struct_to_wallet_list(JSONWL).

block_index_roundtrip_test() ->
	[B] = ar_weave:init(),
	HL = [B#block.indep_hash, B#block.indep_hash],
	JSONHL = jsonify(block_index_to_json_struct(HL)),
	HL = json_struct_to_block_index(dejsonify(JSONHL)),
	BI = [{B#block.indep_hash, 1}, {B#block.indep_hash, 2}],
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
