-module(ar_blacklist_middleware).

-export([start/0, ban_peer/2, is_peer_banned/1, cleanup_ban/1]).
-export([start_link/0]).

-ifdef(AR_TEST).
-export([reset/0]).
-endif.

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include_lib("arweave/include/ar_blacklist_middleware.hrl").
-include_lib("eunit/include/eunit.hrl").

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

reset() ->
    true = ets:delete_all_objects(?MODULE),
    ok.

%% Ban a peer completely for TTLSeconds seoncds. Since we cannot trust the port,
%% we ban the whole IP address.
ban_peer(Peer, TTLSeconds) ->
	?LOG_DEBUG([{event, ban_peer}, {peer, ar_util:format_peer(Peer)}, {seconds, TTLSeconds}]),
	Key = {ban, peer_to_ip_addr(Peer)},
	Expires = os:system_time(seconds) + TTLSeconds,
	ets:insert(?MODULE, {Key, Expires}).

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
