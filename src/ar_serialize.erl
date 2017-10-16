-module(ar_serialize).
-export([block_to_fields/1, fields_to_block/1, tx_to_fields/1, fields_to_tx/1]).
-include("ar.hrl").
-include("../lib/yaws/json2.erl").
-include_lib("eunit/include/eunit.hrl").

%%% Module containing serialisation/deserialisation utility functions
%%% for use in HTTP server.

%% @doc Translate a block into json for HTTP
block_to_json(
  B =
  #block {
    nonce = Nonce,
    txs = Txs
  }) ->
    EncodedB =
      #block {
        nonce = base64:encode(Nonce),
        txs = lists:foreach(base64:encode(), Txs)
      },
    json2:encode_object(EncodedB).

%% @doc Translate fields parsed json from HTTP request into a block
json_to_block(Charlist) ->
  case json2:decode(CharList) of
    {_, {ok, Block} , _} ->
      DecodedBlock =
        Block#block {
          nonce = base64:decode(Block#block.nonce),
          txs = lists.foreach(base64:decode(), Block#block.txs)
        },
      DecodedBlock;
    {_, {error, Reason}, _} ->
      ar:report([{json_error, Reason}])
  end.

%% @doc Translate a transaction into json for HTTP
tx_to_json(
  TX = #tx { data = Data }) ->
    EncodedTx = #tx { data = base64:encode(Data) },
    json2:encode_object(EncodedTx).

%% @doc Translate parsed json from fields to a transaction
json_to_txs(Charlist) ->
  case json2:decode(CharList) of
    {_, {ok, Tx} , _} ->
      DecodedTx =
        Tx#tx { data = base64:decode(Tx#tx.data) },
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
