-module(ar_blacklist).
-behaviour(elli_handler).

%% elli_handler callbacks
-export([handle/2, handle_event/3]).
-export([reset_counters/0, reset_counter/1]).

-include("ar.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(THROTTLE_TABLE, http_throttle_list).
-define(THROTTLE_PERIOD, 30000).

handle(Req, _Config) ->
	Peer = elli_request:peer(Req),
	Ret =
		case increment_ip(Peer) of
			true -> blacklisted(Req);
			false -> ignore
		end,
	% ar:report([{?MODULE, handle}, {Peer, Ret}]),
	Ret.

handle_event(elli_startup, [], Config) ->
	ar:report([{?MODULE, starting}, {handle_event, elli_startup}]),
	ets:new(?THROTTLE_TABLE, [set, public, named_table]),
%	{ok,_} = timer:apply_interval(?THROTTLE_PERIOD,?MODULE, reset_counters, []),
	ok;
handle_event(_, _, _) ->
	ok.

%private functions
blacklisted(Req) ->
	Body    = <<"Too Many Requests">>,
	Size    = list_to_binary(integer_to_list(size(Body))),
	Headers = [{"Connection", "close"}, {"Content-Length", Size}],
	{429,  Headers, Body}.

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
