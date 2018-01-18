-module(ar_serialize).
-export([block_to_json_struct/1, json_struct_to_block/1, tx_to_json_struct/1, json_struct_to_tx/1]).
-export([jsonify/1, dejsonify/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Module containing serialisation/deserialisation utility functions
%%% for use in HTTP server.

%% @doc Take a JSON struct and produce JSON string.
jsonify(JSONStruct) ->
  lists:flatten(json2:encode(JSONStruct)).

%% @doc Decode JSON string into JSON struct.
dejsonify(JSON) ->
  json2:decode_string(JSON).

%% @doc Translate a block into JSON struct.
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
		hash_list = HashList,
		wallet_list = WalletList,
		reward_addr = RewardAddr
	}) ->
	{struct,
		[
			{nonce, ar_util:encode(Nonce)},
			{previous_block, ar_util:encode(PrevHash)},
			{timestamp, TimeStamp},
			{last_retarget, LastRetarget},
			{diff, Diff},
			{height, Height},
			{hash, ar_util:encode(Hash)},
			{indep_hash, ar_util:encode(IndepHash)},
			{txs, {array, lists:map(fun tx_to_json_struct/1, TXs) }},
			{hash_list,
				{array, lists:map(fun ar_util:encode/1, HashList)}
			},
			{wallet_list,
				{array,
					lists:map(
						fun({Wallet, Qty, Last}) ->
							{struct,
								[
									{wallet, ar_util:encode(Wallet)},
									{quantity, Qty},
									{last_tx, ar_util:encode(Last)}
								]
							}
						end,
						WalletList
					)
				}
			},
			{reward_addr,
				if RewardAddr == unclaimed -> "unclaimed";
				true -> ar_util:encode(RewardAddr)
				end
			}
		]
	}.

%% @doc Translate fields parsed json from HTTP request into a block.
json_struct_to_block(JSONList) when is_list(JSONList) ->
	case dejsonify(JSONList) of
		{ok, Block} -> json_struct_to_block(Block);
		{_, {error, Reason}, _} ->
			ar:report([{json_error, Reason}])
	end;
json_struct_to_block({struct, BlockStruct}) ->
	{array, TXs} = find_value("txs", BlockStruct),
	{array, WalletList} = find_value("wallet_list", BlockStruct),
	{array, HashList} = find_value("hash_list", BlockStruct),
	#block {
		nonce = ar_util:decode(find_value("nonce", BlockStruct)),
		previous_block =
			ar_util:decode(find_value("previous_block", BlockStruct)),
		timestamp = find_value("timestamp", BlockStruct),
		last_retarget = find_value("last_retarget", BlockStruct),
		diff = find_value("diff", BlockStruct),
		height = find_value("height", BlockStruct),
		hash = ar_util:decode(find_value("hash", BlockStruct)),
		indep_hash = ar_util:decode(find_value("indep_hash", BlockStruct)),
		txs = lists:map(fun json_struct_to_tx/1, TXs),
		hash_list = [ ar_util:decode(Hash) || Hash <- HashList ],
		wallet_list =
			[
				{ar_util:decode(Wallet), Qty, ar_util:decode(Last)}
			||
				{struct, [{"wallet", Wallet}, {"quantity", Qty}, {"last_tx", Last}]}
					<- WalletList
			],
		reward_addr =
			case find_value("reward_addr", BlockStruct) of
				"unclaimed" -> unclaimed;
				StrAddr -> ar_util:decode(StrAddr)
			end
	}.

%% @doc Translate a transaction into JSON.
tx_to_json_struct(
	#tx {
		id = ID,
		last_tx = Last,
		owner = Owner,
		tags = Tags,
		target = Target,
		quantity = Quantity,
		type = Type,
		data = Data,
		reward = Reward,
		signature = Sig
	}) ->
	{struct,
		[
			{id, ar_util:encode(ID)},
			{last_tx, ar_util:encode(Last)},
			{owner, ar_util:encode(Owner)},
			{tags, {array, Tags}},
			{target, ar_util:encode(Target)},
			{quantity, integer_to_list(Quantity)},
			{type, atom_to_list(Type)},
			{data, ar_util:encode(Data)},
			{reward, integer_to_list(Reward)},
			{signature, ar_util:encode(Sig)}
		]
	}.

%% @doc Translate parsed JSON from fields to a transaction.
json_struct_to_tx(JSONList) when is_list(JSONList) ->
	case dejsonify(JSONList) of
		{ok, TXStruct} -> json_struct_to_tx(TXStruct);
		{_, {error, Reason}, _} -> ar:report([{json_error, Reason}])
	end;
json_struct_to_tx({struct, TXStruct}) ->
	{array, Tags} = find_value("tags", TXStruct),
	#tx {
		id = ar_util:decode(find_value("id", TXStruct)),
		last_tx = ar_util:decode(find_value("last_tx", TXStruct)),
		owner = ar_util:decode(find_value("owner", TXStruct)),
		tags = Tags,
		target = ar_util:decode(find_value("target", TXStruct)),
		quantity = list_to_integer(find_value("quantity", TXStruct)),
		type = list_to_existing_atom(find_value("type", TXStruct)),
		data = ar_util:decode(find_value("data", TXStruct)),
		reward = list_to_integer(find_value("reward", TXStruct)),
		signature = ar_util:decode(find_value("signature", TXStruct))
	}.

%% @doc Find the value associated with a key in a JSON structure list.
find_value(Key, List) ->
	case lists:keyfind(Key, 1, List) of
		{Key, Val} -> Val;
		false -> undefined
	end.

%% @doc Convert a new block into JSON and back, ensure the result is the same.
block_roundtrip_test() ->
	[B] = ar_weave:init(),
	JsonB = jsonify(block_to_json_struct(B)),
	B1 = json_struct_to_block(JsonB),
	B = B1.

%% @doc Convert a new TX into JSON and back, ensure the result is the same.
tx_roundtrip_test() ->
	TX = ar_tx:new(<<"TEST">>),
	JsonTX = jsonify(tx_to_json_struct(TX)),
	TX1 = json_struct_to_tx(JsonTX),
	TX = TX1.
