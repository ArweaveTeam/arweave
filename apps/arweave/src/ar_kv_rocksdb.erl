-module(ar_kv_rocksdb).

-behaviour(ar_kv).

%% @doc AR KV behaviour callbacks
-export([init/2, init_seek/2]).
-export([put/3, get/2, seek/2, seek_next/1, erase/2, close/1]).

%% @doc initialize rocksdb
init(DB, Args) when is_list(DB) ->
    {ok, Handle} = rocksdb:open(DB, Args),
    Handle.

init_seek(DB, Args) ->
    {ok, Itr} = rocksdb:iterator(DB, Args),
    Itr.

%% @doc Store an item into rocksdb
put(Ref, Key, Value) ->
    rocksdb:put(Ref, term_to_binary(Key), term_to_binary(Value), []),
    ok.

%% @doc Get an item from rocksdb
get(Ref, Key) ->
    rocksdb:get(Ref, term_to_binary(Key), []).

%% @doc Seek an item from rocksdb
seek(Itr, Key) when is_reference(Itr) ->
    handle_itr(rocksdb:iterator_move(Itr, {seek, term_to_binary(Key)})).

%% @doc Seek next an item from rocksdb
seek_next(Itr) when is_reference(Itr) ->
    handle_itr(rocksdb:iterator_move(Itr, next)).

%% @doc Erase an item from rocksdb
erase(Ref, Key) ->
    rocksdb:delete(Ref, term_to_binary(Key), []),
    ok.

%% @doc Erase an item from rocksdb
close(Ref) ->
    rocksdb:close(Ref),
    ok.

handle_itr({ok, Key, Value}) -> {Key, Value};
handle_itr(_) -> not_found.