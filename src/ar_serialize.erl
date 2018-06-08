-module(ar_serialize).
-export([full_block_to_json_struct/1, block_to_json_struct/1, json_struct_to_block/1, json_struct_to_full_block/1, tx_to_json_struct/1, json_struct_to_tx/1]).
-export([wallet_list_to_json_struct/1, hash_list_to_json_struct/1, json_struct_to_hash_list/1, json_struct_to_wallet_list/1]).
-export([jsonify/1, dejsonify/1]).
-export([query_to_json_struct/1, json_struct_to_query/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Module containing serialisation/deserialisation utility functions
%%% for use in HTTP server.

%% @doc Take a JSON struct and produce JSON string.
jsonify(JSONStruct) ->
	%lists:flatten(JSONStruct).
	jiffy:encode(JSONStruct).	
	%json2:encode(JSONStruct)).

%% @doc Decode JSON string into JSON struct.
dejsonify(<<>>) -> <<>>;
dejsonify(JSON) ->
	jiffy:decode(JSON).
%   json2:decode_string(JSON).

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
        reward_addr = RewardAddr,
		tags = Tags,
		reward_pool = RewardPool,
		weave_size = WeaveSize,
		block_size = BlockSize
	}) ->
	{
		[
			{nonce, ar_util:encode(Nonce)},
			{previous_block, ar_util:encode(PrevHash)},
			{timestamp, TimeStamp},
			{last_retarget, LastRetarget},
			{diff, Diff},
			{height, Height},
			{hash, ar_util:encode(Hash)},
			{indep_hash, ar_util:encode(IndepHash)},
			{txs, lists:map(fun ar_util:encode/1, TXs)},
			{hash_list, lists:map(fun ar_util:encode/1, HashList)},
			{wallet_list,
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
			},
			{reward_addr,
				if RewardAddr == unclaimed -> list_to_binary("unclaimed");
				true -> ar_util:encode(RewardAddr)
				end
            },
			{tags, Tags},
			{reward_pool, RewardPool},
			{weave_size, WeaveSize},
			{block_size, BlockSize}
		]
	}.
%% @doc Translate a full block into JSON struct.
full_block_to_json_struct(
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
        reward_addr = RewardAddr,
		tags = Tags,
		reward_pool = RewardPool,
		weave_size = WeaveSize,
		block_size = BlockSize
	}) ->
	{
		[
			{nonce, ar_util:encode(Nonce)},
			{previous_block, ar_util:encode(PrevHash)},
			{timestamp, TimeStamp},
			{last_retarget, LastRetarget},
			{diff, Diff},
			{height, Height},
			{hash, ar_util:encode(Hash)},
			{indep_hash, ar_util:encode(IndepHash)},
			{txs, lists:map(fun tx_to_json_struct/1, TXs)},
			{hash_list, lists:map(fun ar_util:encode/1, HashList)},
			{wallet_list,
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
			},
			{reward_addr,
				if RewardAddr == unclaimed -> list_to_binary("unclaimed");
				true -> ar_util:encode(RewardAddr)
				end
            },
			{tags, Tags},
			{reward_pool, RewardPool},
			{weave_size, WeaveSize},
			{block_size, BlockSize}
		]
	}.

%% @doc Translate fields parsed json from HTTP request into a block.
json_struct_to_block(JSONBlock) when is_binary(JSONBlock) ->
	case dejsonify(JSONBlock) of
		{error, Error} -> ar:report([{error, Error}]);
		BlockStruct -> json_struct_to_block(BlockStruct)
	end;
json_struct_to_block(JSONBlock) ->
	{BlockStruct} = JSONBlock,
	TXs = find_value(<<"txs">>, BlockStruct),
	WalletList = find_value(<<"wallet_list">>, BlockStruct),
    HashList = find_value(<<"hash_list">>, BlockStruct),
    Tags = find_value(<<"tags">>, BlockStruct),
	#block {
		nonce = ar_util:decode(find_value(<<"nonce">>, BlockStruct)),
		previous_block = ar_util:decode(find_value(<<"previous_block">>, BlockStruct)),
		timestamp = find_value(<<"timestamp">>, BlockStruct),
		last_retarget = find_value(<<"last_retarget">>, BlockStruct),
		diff = find_value(<<"diff">>, BlockStruct),
		height = find_value(<<"height">>, BlockStruct),
		hash = ar_util:decode(find_value(<<"hash">>, BlockStruct)),
		indep_hash = ar_util:decode(find_value(<<"indep_hash">>, BlockStruct)),
		txs = lists:map(fun ar_util:decode/1, TXs),
		hash_list = [ ar_util:decode(Hash) || Hash <- HashList ],
		wallet_list =
			[
				{ar_util:decode(Wallet), Qty, ar_util:decode(Last)}
			||
				{[{<<"wallet">>, Wallet}, {<<"quantity">>, Qty}, {<<"last_tx">>, Last}]}
					<- WalletList
			],
		reward_addr =
			case find_value(<<"reward_addr">>, BlockStruct) of
				<<"unclaimed">> -> unclaimed;
				StrAddr -> ar_util:decode(StrAddr)
            end,
		tags = Tags,
		reward_pool = find_value(<<"reward_pool">>, BlockStruct),
		weave_size = find_value(<<"weave_size">>, BlockStruct),
		block_size = find_value(<<"block_size">>, BlockStruct)
	}.
%% @doc Translate fields parsed json from HTTP request into a full block.
json_struct_to_full_block(JSONBlock) when is_binary(JSONBlock) ->
	case dejsonify(JSONBlock) of
		{error, Error} -> ar:report([{error, Error}]);
		BlockStruct -> json_struct_to_full_block(BlockStruct)
	end;
json_struct_to_full_block(JSONBlock) ->
	{BlockStruct} = JSONBlock,
	TXs = find_value(<<"txs">>, BlockStruct),
	WalletList = find_value(<<"wallet_list">>, BlockStruct),
    HashList = find_value(<<"hash_list">>, BlockStruct),
    Tags = find_value(<<"tags">>, BlockStruct),
	#block {
		nonce = ar_util:decode(find_value(<<"nonce">>, BlockStruct)),
		previous_block = ar_util:decode(find_value(<<"previous_block">>, BlockStruct)),
		timestamp = find_value(<<"timestamp">>, BlockStruct),
		last_retarget = find_value(<<"last_retarget">>, BlockStruct),
		diff = find_value(<<"diff">>, BlockStruct),
		height = find_value(<<"height">>, BlockStruct),
		hash = ar_util:decode(find_value(<<"hash">>, BlockStruct)),
		indep_hash = ar_util:decode(find_value(<<"indep_hash">>, BlockStruct)),
		txs = lists:map(fun json_struct_to_tx/1, TXs),
		hash_list = [ ar_util:decode(Hash) || Hash <- HashList ],
		wallet_list =
			[
				{ar_util:decode(Wallet), Qty, ar_util:decode(Last)}
			||
				{[{<<"wallet">>, Wallet}, {<<"quantity">>, Qty}, {<<"last_tx">>, Last}]}
					<- WalletList
			],
		reward_addr =
			case find_value(<<"reward_addr">>, BlockStruct) of
				<<"unclaimed">> -> unclaimed;
				StrAddr -> ar_util:decode(StrAddr)
            end,
		tags = Tags,
		reward_pool = find_value(<<"reward_pool">>, BlockStruct),
		weave_size = find_value(<<"weave_size">>, BlockStruct),
		block_size = find_value(<<"block_size">>, BlockStruct)
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
		data = Data,
		reward = Reward,
		signature = Sig
	}) ->
	{
		[
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
			{reward, integer_to_binary(Reward)},
			{signature, ar_util:encode(Sig)}
		]
	}.

%% @doc Translate parsed JSON from fields to a transaction.
json_struct_to_tx(JSONTX) when is_binary(JSONTX) ->
	case dejsonify(JSONTX) of
		{error, Error} -> ar:report([{error, Error}]);
		TXStruct -> json_struct_to_tx(TXStruct)
	end;
json_struct_to_tx(JSONTX) ->
	{TXStruct} = JSONTX,
	Tags = case find_value(<<"tags">>, TXStruct) of
		undefined -> [];
		Xs -> Xs
	end,
	#tx {
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
		data = ar_util:decode(find_value(<<"data">>, TXStruct)),
		reward = binary_to_integer(find_value(<<"reward">>, TXStruct)),
		signature = ar_util:decode(find_value(<<"signature">>, TXStruct))
	}.

%% @doc Translate a wallet list into JSON.
wallet_list_to_json_struct([]) -> [];
wallet_list_to_json_struct([Wallet|WalletList]) ->
    EncWallet = wallet_to_json_struct(Wallet),
    [EncWallet | wallet_list_to_json_struct(WalletList)].
wallet_to_json_struct({Address, Balance, Last}) ->
    {
        [
            {address, ar_util:encode(Address)},
            {balance, integer_to_list(Balance)},
            {last_tx, ar_util:encode(Last)}
        ]
    }.

%% @doc Translate parsed JSON from fields into a valid wallet list.
json_struct_to_wallet_list(JSONList) when is_binary(JSONList) ->
	case dejsonify(JSONList) of
		{error, Error} -> ar:report([{error, Error}]);
		ListStruct -> json_struct_to_wallet_list(ListStruct)
	end;
json_struct_to_wallet_list(WalletsStruct) ->
    lists:foldr(
        fun(X, Acc) -> [json_struct_to_wallet(X) | Acc] end,
        [],
        WalletsStruct
    ).
json_struct_to_wallet({Wallet}) ->
    Address = ar_util:decode(find_value(<<"address">>, Wallet)),
    Balance = list_to_integer(find_value(<<"balance">>, Wallet)),
    Last = ar_util:decode(find_value(<<"last_tx">>, Wallet)),
    {Address, Balance, Last}.

%% @doc Translate a hash list into JSON.
hash_list_to_json_struct([]) -> [];
hash_list_to_json_struct([Hash|HashList]) ->
    EncHash = ar_util:encode(binary_to_list(Hash)),
    [EncHash | hash_list_to_json_struct(HashList)].

%% @doc Translate parsed JSON from fields into a valid hash list.
json_struct_to_hash_list(JSONList) when is_binary(JSONList) ->
	case dejsonify(JSONList) of
		{error, Error} -> ar:report([{error, Error}]);
		ListStruct -> json_struct_to_hash_list(ListStruct)
	end;
json_struct_to_hash_list(HashesStruct) ->
    lists:foldr(
        fun(X, Acc) -> [ar_util:decode(X)|Acc] end,
        [],
        HashesStruct
    ).
%% @doc Find the value associated with a key in a JSON structure list.
find_value(Key, List) ->
	case lists:keyfind(Key, 1, List) of
		{Key, Val} -> Val;
		false -> undefined
	end.

%% @doc Convert a ARQL query into a JSON struct
query_to_json_struct({Op, Expr1, Expr2}) ->
	{
		[	
			{op, list_to_binary(atom_to_list(Op))},
			{expr1, query_to_json_struct(Expr1)},
			{expr2, query_to_json_struct(Expr2)}
		]
	};
query_to_json_struct(Expr) ->
	ar_util:encode(Expr).

%% @doc Convert a JSON struct to an ARQL query
json_struct_to_query(QueryJSON) ->
	case dejsonify (QueryJSON) of
        {error, Error} -> {error, Error};
        Query -> do_json_struct_to_query(Query)
    end.
do_json_struct_to_query({Query}) ->
	{
		list_to_existing_atom(binary_to_list(find_value(<<"op">>, Query))),
		do_json_struct_to_query(find_value(<<"expr1">>, Query)),
		do_json_struct_to_query(find_value(<<"expr2">>, Query))
	};
do_json_struct_to_query(Query) ->
	ar_util:decode(Query).


%% @doc Convert a new block into JSON and back, ensure the result is the same.
block_roundtrip_test() ->
    [B] = ar_weave:init(),
    BTags = B#block { tags = [<<"hello">>, "world", 1] },
	JsonB = jsonify(block_to_json_struct(BTags)),
	BTags = json_struct_to_block(JsonB).

%% @doc Convert a new block into JSON and back, ensure the result is the same.
full_block_roundtrip_test() ->
    [B] = ar_weave:init(),
	TXBase = ar_tx:new(<<"test">>),
    BTags = B#block {txs = [TXBase], tags = ["hello", "world", "example"] },
	JsonB = jsonify(full_block_to_json_struct(BTags)),
	BTags = json_struct_to_full_block(JsonB),
	(json_struct_to_full_block(JsonB))#block.txs.

full_block_unavailable_roundtrip_test() ->
    [B] = ar_weave:init(),
	TXBase = ar_tx:new(<<"test">>),
    BTags = B#block {txs = [TXBase], tags = ["hello", "world", "example"] },
	JsonB = jsonify(full_block_to_json_struct(BTags)),
	BTags = json_struct_to_full_block(JsonB),
	(json_struct_to_full_block(JsonB))#block.txs.

%% @doc Convert a new TX into JSON and back, ensure the result is the same.
tx_roundtrip_test() ->
	TXBase = ar_tx:new(<<"test">>),
	TX = TXBase#tx { tags = [{<<"Name1">>, <<"Value1">>}] },
	JsonTX = jsonify(tx_to_json_struct(TX)),
	TX = json_struct_to_tx(JsonTX).

wallet_list_roundtrip_test() ->
    [B] = ar_weave:init(),
    WL = B#block.wallet_list,
    JsonWL = jsonify(wallet_list_to_json_struct(WL)),
    WL = json_struct_to_wallet_list(JsonWL).

hash_list_roundtrip_test() ->
    [B] = ar_weave:init(),
    HL = [B#block.indep_hash, B#block.indep_hash],
    JsonHL = jsonify(hash_list_to_json_struct(HL)),
    HL = json_struct_to_hash_list(JsonHL).

query_roundtrip_test() ->
	Query = {'equals', <<"TestName">>, <<"TestVal">>},
	ar:d(QueryJSON = ar_serialize:jsonify(
		ar_serialize:query_to_json_struct(
			Query
			)
		)),
	Query = ar_serialize:json_struct_to_query(QueryJSON).