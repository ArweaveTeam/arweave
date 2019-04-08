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
	Peer = cowboy_binary_peer_address(Req),
	case ar_meta_db:get(blacklist) of
		false -> {ok, Req, Env};
		_ ->
			case increment_ip(Peer) of
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

reset_counter(Peer) ->
	%ar:report([{reset_counter, Peer}]),
	ets:delete(?THROTTLE_TABLE, Peer),
	ok.

increment_ip(Peer) ->
	Count = ets:update_counter(?THROTTLE_TABLE, Peer, {2,1}, {Peer,0}),
	case Count of
		1 -> timer:apply_after(?THROTTLE_PERIOD, ?MODULE, reset_counter, [Peer]);
		_ -> ok
	end,
	Count > ?MAX_REQUESTS. % yup, just logical expr that evaulates true of false.

cowboy_binary_peer_address(Req) ->
	{IpAddr, _Port} = cowboy_req:peer(Req),
	list_to_binary(inet:ntoa(IpAddr)).
