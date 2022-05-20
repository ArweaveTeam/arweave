-module(ar_blacklist_middleware).

-behaviour(cowboy_middleware).

-export([start/0, execute/2, reset/0, reset_rate_limit/3,
		ban_peer/2, is_peer_banned/1, cleanup_ban/1, decrement_ip_addr/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_blacklist_middleware.hrl").
-include_lib("eunit/include/eunit.hrl").

execute(Req, Env) ->
	IpAddr = requesting_ip_addr(Req),
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(blacklist, Config#config.disable) of
		true ->
			{ok, Req, Env};
		_ ->
			case increment_ip_addr(IpAddr, Req) of
				block -> {stop, blacklisted(Req)};
				pass -> {ok, Req, Env}
			end
	end.

start() ->
	?LOG_INFO([{event, ar_blacklist_middleware_start}]),
	ets:new(ar_blacklist_middleware, [set, public, named_table]),
	{ok, _} =
		timer:apply_after(
			?BAN_CLEANUP_INTERVAL,
			?MODULE,
			cleanup_ban,
			[ets:whereis(?MODULE)]
		),
	ok.

%% Ban a peer completely for TTLSeconds seoncds. Since we cannot trust the port,
%% we ban the whole IP address.
ban_peer(Peer, TTLSeconds) ->
	Key = {ban, peer_to_ip_addr(Peer)},
	Expires = os:system_time(seconds) + TTLSeconds,
	ar_events:send(peer, {banned, Peer}),
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
			{ok, _} =
				timer:apply_after(
					?BAN_CLEANUP_INTERVAL,
					?MODULE,
					cleanup_ban,
					[TableID]
				);
		_ ->
			table_owner_died
	end.

%private functions
blacklisted(Req) ->
	cowboy_req:reply(
		429,
		#{<<"connection">> => <<"close">>},
		<<"Too Many Requests">>,
		Req
	).

reset() ->
	true = ets:delete_all_objects(?MODULE),
	ok.

reset_rate_limit(TableID, IpAddr, Path) ->
	case ets:whereis(?MODULE) of
		TableID ->
			ets:delete(?MODULE, {rate_limit, IpAddr, Path});
		_ ->
			table_owner_died
	end.

increment_ip_addr(IpAddr, Req) ->
	case ets:whereis(?MODULE) of 
		undefined -> pass;
		_ -> update_ip_addr(IpAddr, Req, 1)
	end.

decrement_ip_addr(IpAddr, Req) ->
	case ets:whereis(?MODULE) of 
		undefined -> pass;
		_ -> update_ip_addr(IpAddr, Req, -1)
	end.

update_ip_addr(IpAddr, Req, Diff) ->
	{PathKey, Limit}  = get_key_limit(IpAddr, Req),
	%% Divide by 2 as the throttle period is 30 seconds.
	RequestLimit = Limit div 2,
	Key = {rate_limit, IpAddr, PathKey},
	case ets:update_counter(?MODULE, Key, {2, Diff}, {Key, 0}) of
		1 ->
			timer:apply_after(
				?THROTTLE_PERIOD,
				?MODULE,
				reset_rate_limit,
				[ets:whereis(?MODULE), IpAddr, PathKey]
			),
			pass;
		Count when Count =< RequestLimit ->
			pass;
		_ ->
			block
	end.

requesting_ip_addr(Req) ->
	{IpAddr, _} = cowboy_req:peer(Req),
	IpAddr.

peer_to_ip_addr({A, B, C, D, _}) -> {A, B, C, D}.

get_key_limit(IPAddr, Req) ->
	Path = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	{ok, Config} = application:get_env(arweave, config),
	Map = maps:get(IPAddr, Config#config.requests_per_minute_limit_by_ip, #{}),
	?RPM_BY_PATH(Path, Map)().
