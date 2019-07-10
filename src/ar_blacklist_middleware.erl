-module(ar_blacklist_middleware).
-behaviour(cowboy_middleware).

%% cowboy_middleware callbacks
-export([execute/2]).
-export([start/0]).
-export([reset/0, reset_rate_limit/1]).
-export([ban_peer/2, is_peer_banned/1, cleanup_ban/0]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(THROTTLE_PERIOD, 30000).
-define(BAN_CLEANUP_INTERVAL, 60000).

execute(Req, Env) ->
	IpAddr = requesting_ip_addr(Req),
	case ar_meta_db:get(blacklist) of
		false ->
			{ok, Req, Env};
		_ ->
			case increment_ip_addr(IpAddr) of
				block -> {stop, blacklisted(Req)};
				pass -> {ok, Req, Env}
			end
	end.

start() ->
	ar:report([{?MODULE, start}]),
	ets:new(?MODULE, [set, public, named_table]),
	{ok, _} = timer:apply_interval(?BAN_CLEANUP_INTERVAL, ?MODULE, cleanup_ban, []),
	ok.

%% Ban a peer completely for TTLSeconds seoncds. Since we cannot trust the port,
%% we ban the whole IP address.
ban_peer(Peer, TTLSeconds) ->
	Key = {ban, peer_to_ip_addr(Peer)},
	Expires = os:system_time(seconds) + TTLSeconds,
	ets:insert(?MODULE, {Key, Expires}).

is_peer_banned(Peer) ->
	Key = {ban, peer_to_ip_addr(Peer)},
	case ets:lookup(?MODULE, Key) of
		[] -> not_banned;
		[_] -> banned
	end.

cleanup_ban() ->
	Now = os:system_time(seconds),
	Folder = fun
		({{ban, _} = Key, Expires}, Acc) when Expires < Now ->
			[Key | Acc];
		(_, Acc) ->
			Acc
	end,
	RemoveKeys = ets:foldl(Folder, [], ?MODULE),
	Delete = fun(Key) -> ets:delete(?MODULE, Key) end,
	lists:foreach(Delete, RemoveKeys).

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

reset_rate_limit(IpAddr) ->
	ets:delete(?MODULE, {rate_limit, IpAddr}),
	ok.

increment_ip_addr(IpAddr) ->
	RequestLimit = ar_meta_db:get(requests_per_minute_limit) div 2, % Dividing by 2 as throttle period is 30 seconds.
	Key = {rate_limit, IpAddr},
	case ets:update_counter(?MODULE, Key, {2, 1}, {Key, 0}) of
		1 ->
			timer:apply_after(?THROTTLE_PERIOD, ?MODULE, reset_rate_limit, [IpAddr]),
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
