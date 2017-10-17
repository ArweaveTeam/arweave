-module(ar_serialize).
-export([block_to_json/1, json_to_block/1, tx_to_json/1, json_to_tx/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Module containing serialisation/deserialisation utility functions
%%% for use in HTTP server.

%% @doc turns IOList JSON representation into flat list.
to_json(Input) ->
  lists:flatten(Input).

%% @doc Translate a block into json for HTTP.
block_to_json(
	#block {
		nonce = Nonce,
		timestamp = TimeStamp,
		last_retarget = LastRetarget,
		diff = Diff,
		height = Height,
		hash = Hash,
		indep_hash = IndepHash,
		txs = Txs,
		hash_list = Hash_list,
		wallet_list = Wallet_list
	}) ->
	EncodedB =
		{struct,
			[
				{nonce, base64:encode_to_string_to_string(Nonce)},
				{timestamp, TimeStamp},
				{last_retarget, LastRetarget},
				{diff, Diff},
				{height, Height},
				{hash, Hash},
				{indep_hash, IndepHash},
				{txs, {array, lists:map(fun tx_to_json/1, Txs) }},
				{hash_list, Hash_list},
				{wallet_list, Wallet_list}
			]
		},
	to_json(json2:encode(EncodedB)).

%% @doc Translate fields parsed json from HTTP request into a block.
json_to_block(Json) ->
	case json2:decode_string(Json) of
		{ok,{struct, Block}}  ->
			B = Block#block {
				nonce = base64:decode(Block#block.nonce),
				txs = lists:map(fun json_to_tx/1, Block#block.txs)
			},
    add_fields_to_record(B, Block);
		{_, {error, Reason}, _} ->
			ar:report([{json_error, Reason}])
	end.

%% @doc Adds fields to Record if they do not already exist.
add_fields_to_record(Record, Fields) ->
  lists.map(fun() ->)

%% @doc Translate a transaction into json for HTTP.
tx_to_json(
	#tx {
		id = ID,
		owner = Owner,
		tags = Tags,
		target = Target,
		quantity = Quantity,
		type = Type,
		data = Data,
		signature = Sig
	}) ->
	EncodedTx =
		{struct,
			[
				{id, ID},
				{owner, Owner},
				{tags, {array, Tags}},
				{target, Target},
				{quantity, Quantity},
				{type, Type},
				{data, base64:encode_to_string(Data)},
				{signature, Sig}
			]
		},
	to_json(json2:encode(EncodedTx)).

%% @doc Translate parsed json from fields to a transaction.
json_to_tx(Json) ->
	case json2:decode_string(Json) of
		{ok,{struct, TX}} ->
			#tx { data = base64:decode(TX#tx.data) };
		{_, {error, Reason}, _} ->
			ar:report([{json_error, Reason}])
	end.

%% @doc Convert a new block into JSON and back, ensure the result is the same.
block_roundtrip_test() ->
	[B] = ar_weave:init(),
	JsonB = block_to_json(B),
	B1 = json_to_block(JsonB),
	B = B1.

%% @doc Convert a new TX into JSON and back, ensure the result is the same.
tx_roundtrip_test() ->
	TX = ar_tx:new(<<"TEST">>),
	JsonTX = tx_to_json(TX),
	TX1 = json_to_tx(JsonTX),
	TX = TX1.
