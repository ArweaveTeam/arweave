-module(ar_kv).

-callback init(term(), term()) -> term().
-callback init_seek(term(), term()) -> term().
-callback put(term(), term(), term()) -> ok.
-callback get(term(), term()) -> term().
-callback seek(term(), term()) -> {term(), term()}.
-callback seek_next(term(), term()) -> {term(), term()}.
-callback erase(term(), term()) -> ok.
-callback close(term()) -> ok.

-optional_callbacks([init/2, init_seek/2, seek/2, seek_next/2, close/1]).

-export([init/2, init_seek/2]).
-export([put/3, get/2, seek/2,seek_next/2, erase/2, close/1]).

-define(KV_ENGINE, (ar_meta_db:get(kv_engine))).

%% @doc Initialise the KV store
init(Name, Args) ->
    backend_op(fun() -> ?KV_ENGINE:init(Name, Args) end).

%% @doc Initialise seek iterator for KV store
init_seek(Ref, Args) ->
    backend_op(fun() -> ?KV_ENGINE:init_seek(Ref, Args) end).

%% @doc Put an entry, Value, by Key
put(Ref, Key, Value) ->
    backend_op(fun() -> ?KV_ENGINE:put(Ref, Key, Value) end).

%% @doc Get an entry by Key
get(Ref, Key) ->
    backend_op(fun() -> ?KV_ENGINE:get(Ref, Key) end).

%% @doc Seek nearest Key by Position
seek(Ref, Pos) ->
    backend_op(fun() -> ?KV_ENGINE:seek(Ref, Pos) end).

%% @doc Seek next nearest Key by Position
seek_next(Ref, Pos) ->
    backend_op(fun() -> ?KV_ENGINE:seek_next(Ref, Pos) end).

%% @doc Erase an entry by Key
erase(Ref, Key) ->
    backend_op(fun() -> ?KV_ENGINE:get(Ref, Key) end).

%% @doc Close the KV store
close(Ref) ->
    backend_op(fun() -> ?KV_ENGINE:close(Ref) end).

%% internal backend operation
backend_op(Op) ->
    try
        Op()
    catch
        _:Reason -> {error, Reason}
    end.