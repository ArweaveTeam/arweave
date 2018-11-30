%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface).

-export([start/0, start/1, start/2, start/3, start/4, start/5, handle/2, handle_event/3]).
-export([send_new_block/4, send_new_block/6, send_new_tx/2, get_block/3]).
-export([get_tx/2, get_tx_data/2, get_full_block/3, get_block_subfield/3, add_peer/1]).
-export([get_encrypted_block/2, get_encrypted_full_block/2]).
-export([get_info/1, get_info/2, get_peers/1, get_pending_txs/1, has_tx/2]).
-export([get_time/1, get_height/1]).
-export([get_wallet_list/2, get_hash_list/1, get_hash_list/2]).
-export([get_current_block/1, get_current_block/2]).
-export([reregister/1, reregister/2]).

start() -> ar_http_iface_server:start().
start(A) -> ar_http_iface_server:start(A).
start(A,B) -> ar_http_iface_server:start(A,B).
start(A,B,C) -> ar_http_iface_server:start(A,B,C).
start(A,B,C,D) -> ar_http_iface_server:start(A,B,C,D).
start(A,B,C,D,E) -> ar_http_iface_server:start(A,B,C,D,E).

handle(A,B) ->
    ar_http_iface_server:handle(A,B).

handle_event(A,B,C) ->
    ar_http_iface_server:handle_event(A,B,C).

reregister(A) ->
     ar_http_iface_server:reregister(A).

reregister(A,B) ->
     ar_http_iface_server:reregister(A,B).

send_new_block(A,B,C,D) ->
     ar_http_iface_client:send_new_block(A,B,C,D).

send_new_block(A,B,C,D,E,F) ->
     ar_http_iface_client:send_new_block(A,B,C,D,E,F).

send_new_tx(A,B) ->
     ar_http_iface_client:send_new_tx(A,B).

get_block(A,B,C) ->
     ar_http_iface_client:get_block(A,B,C).

get_tx(A,B) ->
     ar_http_iface_client:get_tx(A,B).

get_tx_data(A,B) ->
     ar_http_iface_client:get_tx_data(A,B).

get_full_block(A,B,C) ->
     ar_http_iface_client:get_full_block(A,B,C).

get_block_subfield(A,B,C) ->
     ar_http_iface_client:get_block_subfield(A,B,C).

add_peer(A) ->
     ar_http_iface_client:add_peer(A).

get_encrypted_block(A,B) ->
     ar_http_iface_client:get_encrypted_block(A,B).

get_encrypted_full_block(A,B) ->
     ar_http_iface_client:get_encrypted_full_block(A,B).

get_info(A) ->
     ar_http_iface_client:get_info(A).

get_info(A,B) ->
     ar_http_iface_client:get_info(A,B).

get_peers(A) ->
     ar_http_iface_client:get_peers(A).

get_pending_txs(A) ->
     ar_http_iface_client:get_pending_txs(A).

has_tx(A,B) ->
     ar_http_iface_client:has_tx(A,B).

get_time(A) ->
     ar_http_iface_client:get_time(A).

get_height(A) ->
     ar_http_iface_client:get_height(A).

get_wallet_list(A,B) ->
     ar_http_iface_client:get_wallet_list(A,B).

get_hash_list(A) ->
     ar_http_iface_client:get_hash_list(A).

get_hash_list(A,B) ->
     ar_http_iface_client:get_hash_list(A,B).

get_current_block(A) ->
     ar_http_iface_client:get_current_block(A).

get_current_block(A,B) ->
     ar_http_iface_client:get_current_block(A,B).
