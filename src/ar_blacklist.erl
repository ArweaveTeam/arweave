-module(ar_blacklist).
-behaviour(cowboy_middleware).

%% cowboy_middleware callbacks
-export([execute/2]).
-export([start/0]).
-export([reset_counters/0, reset_counter/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(THROTTLE_TABLE, http_throttle_list).
-define(THROTTLE_PERIOD, 30000).

execute(Req, Env) ->
	IpAddr = requesting_ip_addr(Req),
	case ar_meta_db:get(blacklist) of
		false -> {ok, Req, Env};
		_ ->
			case increment_ip(IpAddr) of
				true -> {stop, blacklisted(Req)};
				false -> {ok, Req, Env}
			end
	end.

start() ->
	ar:report([{?MODULE, start}]),
	ets:new(?THROTTLE_TABLE, [set, public, named_table]),
%	{ok,_} = timer:apply_interval(?THROTTLE_PERIOD,?MODULE, reset_counters, []),
	ok.

%private functions
blacklisted(Req) ->
	cowboy_req:reply(
		429,
		#{<<"connection">> => <<"close">>},
		<<"Too Many Requests">>,
		Req
	).

reset_counters() ->
	true = ets:delete_all_objects(?THROTTLE_TABLE),
	ok.

reset_counter(IpAddr) ->
	ets:delete(?THROTTLE_TABLE, IpAddr),
	ok.

increment_ip(IpAddr) ->
	Count = ets:update_counter(?THROTTLE_TABLE, IpAddr, {2,1}, {IpAddr,0}),
	case Count of
		1 -> timer:apply_after(?THROTTLE_PERIOD, ?MODULE, reset_counter, [IpAddr]);
		_ -> ok
	end,
	Count > ar_meta_db:get(requests_per_minute_limit) div 2. % Dividing by 2 as throttle period is 30 seconds.

requesting_ip_addr(Req) ->
	{IpAddr, _} = cowboy_req:peer(Req),
	IpAddr.
