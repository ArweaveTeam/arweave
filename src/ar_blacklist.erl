-module(ar_blacklist).
-behaviour(cowboy_middleware).

%% cowboy_middleware callbacks
-export([execute/2]).
-export([start/0]).
-export([reset/0, reset/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(THROTTLE_PERIOD, 30000).

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

reset() ->
	true = ets:delete_all_objects(?MODULE),
	ok.

reset(IpAddr) ->
	ets:delete(?MODULE, {rate_limit, IpAddr}),
	ets:delete(?MODULE, {ban, IpAddr}),
	ok.

increment_ip_addr(IpAddr) ->
	RequestLimit = ar_meta_db:get(requests_per_minute_limit) div 2, % Dividing by 2 as throttle period is 30 seconds.
	Key = {rate_limit, IpAddr},
	case ets:update_counter(?MODULE, Key, {2, 1}, {Key, 0}) of
		1 ->
			timer:apply_after(?THROTTLE_PERIOD, ?MODULE, reset, [IpAddr]),
			pass;
		Count when Count =< RequestLimit ->
			pass;
		_ ->
			block
	end.

requesting_ip_addr(Req) ->
	{IpAddr, _} = cowboy_req:peer(Req),
	IpAddr.
