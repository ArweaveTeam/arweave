%%% @doc Peer compatibility register.
%%%
%%%
%%%
%%% @end
-module(arweave_throttling_peer_compatibility_register).

-export([
    init/0,
    is_peer_marked_incompatible/1,
    mark_incompatible/1,
    mark_compatible/1
   ]).


-ifdef(AR_TEST).
-export([cleanup/0]).
-else.
-compile({nowarn_unused_function, [{cleanup, 0}]}).
-endif.

%% @doc Create table
init() ->
    ?MODULE = ets:new(?MODULE, [named_table, set, public]),
    ok.

%% @doc Check if the peer has been marked as incompatible
is_peer_marked_incompatible(Peer) ->
    try ets:lookup(?MODULE, Peer) of
        [] ->
            false;
        [{Peer, Value}] ->
            Value
    catch
        _:Reason ->
            {error, Reason}
    end.

%% @doc Mark peer as incompatible
mark_incompatible(Peer) ->
    ets:insert(?MODULE, {Peer, true}),
    ok.

%% @doc Mark peer as compatible
mark_compatible(Peer) ->
    is_peer_marked_incompatible(Peer) andalso ets:delete(?MODULE, Peer),
    ok.

%% @doc Delete tables
cleanup() ->
    catch ets:delete(?MODULE),
    ok.
