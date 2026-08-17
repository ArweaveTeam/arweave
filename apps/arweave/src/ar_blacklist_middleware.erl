-module(ar_blacklist_middleware).
-test_category([fast]).

-export([start/0, ban_peer/2, is_peer_banned/1, cleanup_ban/1]).
-export([start_link/0]).

-ifdef(AR_TEST).
-export([reset/0]).
-endif.

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

-define(BAN_CLEANUP_INTERVAL, 60000).

start_link() ->
    {ok, spawn_link(fun() -> start() end)}.

start() ->
    ?LOG_INFO([{start, ?MODULE}, {pid, self()}]),
    {ok, _} = ar_timer:apply_after(
                ?BAN_CLEANUP_INTERVAL,
                ?MODULE,
                cleanup_ban,
                [ets:whereis(?MODULE)],
                #{ skip_on_shutdown => false }
               ).

%% Ban a peer completely for TTLSeconds seconds. Since we cannot trust the port,
%% we ban the whole IP address.
ban_peer(Peer, TTLSeconds) ->
    ?LOG_DEBUG([{event, ban_peer}, {peer, arweave_lib_util:format_peer(Peer)}, {seconds, TTLSeconds}]),
    Key = {ban, peer_to_ip_addr(Peer)},
    Expires = os:system_time(seconds) + TTLSeconds,
    Expires2 =
        case ets:lookup(?MODULE, Key) of
            [{_, Existing}] when Existing > Expires ->
                Existing;
            _ ->
                Expires
        end,
    ets:insert(?MODULE, {Key, Expires2}).

is_peer_banned(Peer) ->
    Key = {ban, peer_to_ip_addr(Peer)},
    case ets:lookup(?MODULE, Key) of
        [] -> not_banned;
        [_] -> banned
    end.

cleanup_ban(TableID) ->
    case ets:whereis(?MODULE) of
        TableID ->
            Now = os:system_time(seconds),
            Folder = fun
                         ({{ban, _} = Key, Expires}, Acc) when Expires < Now ->
                        [Key | Acc];
                (_, Acc) ->
                        Acc
                end,
    RemoveKeys = ets:foldl(Folder, [], ?MODULE),
    Delete = fun(Key) -> ets:delete(?MODULE, Key) end,
    lists:foreach(Delete, RemoveKeys),
    _ = ar_timer:apply_after(
          ?BAN_CLEANUP_INTERVAL,
          ?MODULE,
          cleanup_ban,
          [TableID],
          #{ skip_on_shutdown => true }
         );
_ ->
    table_owner_died
    end.

                                                %private functions
peer_to_ip_addr({A, B, C, D, _}) -> {A, B, C, D}.

-ifdef(AR_TEST).
-include_lib("eunit/include/eunit.hrl").

reset() ->
    true = ets:delete_all_objects(?MODULE),
    ok.

%%%===================================================================
%%% Tests.
%%%===================================================================

ban_peer_never_shortens_existing_ban_test() ->
    ensure_test_table(),
    reset(),
    Peer = {127, 0, 0, 1, 1984},
    Key = {ban, {127, 0, 0, 1}},
    ban_peer(Peer, 1000),
    [{_, LongExpires}] = ets:lookup(?MODULE, Key),
    ban_peer(Peer, 10),
    ?assertEqual([{Key, LongExpires}], ets:lookup(?MODULE, Key)),
    ban_peer(Peer, 5000),
    [{_, LongerExpires}] = ets:lookup(?MODULE, Key),
    ?assert(LongerExpires > LongExpires),
    reset().

%% The ban table is created by ar_sup at node boot; create it here when the test
%% runs without a booted node so the test is self-contained either way.
ensure_test_table() ->
    case ets:whereis(?MODULE) of
        undefined ->
            ets:new(?MODULE, [set, public, named_table]);
        _ ->
            ?MODULE
    end.

-endif.
