-module(ar_serialize).
-export([block_to_json/1, json_to_block/1, tx_to_json/1, json_to_tx/1]).
-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

%%% Module containing serialisation/deserialisation utility functions
%%% for use in HTTP server.

%% @doc Translate a block into json for HTTP
block_to_json(
  #block {
    height = Height,
    hash = Hash,
    nonce = Nonce,
    txs = Txs,
    wallet_list = Wallet_list,
    hash_list = Hash_list
  }) ->
    EncodedB =
   	{struct,
		[
      {height, Height},
      {hash, Hash},
			{nonce, base64:encode(Nonce)},
      {txs, {array, lists:map(fun tx_to_json/1, Txs) }},
      {wallet_list, Wallet_list},
      {hash_list, Hash_list}
		]
	},
    json2:encode(EncodedB).

%% @doc Translate fields parsed json from HTTP request into a block
json_to_block(Json) ->
  case json2:decode_string(Json) of
    {done, {ok, Block} , _} ->
      DecodedBlock =
        Block#block {
          nonce = base64:decode(Block#block.nonce),
          txs = lists:map(fun json_to_tx/1, Block#block.txs)
        },
      DecodedBlock;
    {_, {error, Reason}, _} ->
      ar:report([{json_error, Reason}])
  end.

%% @doc Translate a transaction into json for HTTP
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
          {tags, Tags},
          {target, Target},
          {quantity, Quantity},
          {type, Type},
          {data, base64:encode(Data)},
          {signature, Sig}
        ]
      },
  json2:encode(EncodedTx).

%% @doc Translate parsed json from fields to a transaction
json_to_tx(Json) ->
  case json2:decode_string(Json) of
    {ok,{struct, TX}} ->
      DecodedTx =
        #tx { data = base64:decode(Tx#tx.data) },
      DecodedTx;
    {_, {error, Reason}, _} ->
      ar:report([{json_error, Reason}])
  end.

block_roundtrip_test() ->
  B = ar_weave:init(),
  JsonB = block_to_json(B),
  B1 = json_to_block(JsonB),
  B = B1.

tx_roundtrip_test() ->
  TX = ar_tx:new(<<"TEST">>),
  JsonTX = tx_to_json(TX),
  TX1 = json_to_tx(JsonTX),
  TX = TX1.
