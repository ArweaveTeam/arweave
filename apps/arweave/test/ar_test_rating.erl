-module(ar_test_rating).

-include_lib("common_test/include/ct.hrl").
-include_lib("arweave/include/ar.hrl").

-include_lib("arweave/include/ar_rating.hrl").

-export([
	peer_join_leave_rejoin/1,
	check_rate_and_triggers/1,
	check_get_top_n_get_banned/1

]).

peer_join_leave_rejoin(_Config) ->
	{state, Joined, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating),
	case Joined of
		false ->
			true = check_network_join(),
			{state, true, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating);
		_ ->
			ok
	end,
	Peer = peer1,
	case ar_kv:get(RatingDB, term_to_binary(Peer)) of
		not_found ->
			ok;
		{ok, _} ->
			ct:fail("Peer is already exist");
		{error, E1} ->
			ct:fail("Something went wrong ~p", [E1]);
		WTF1 ->
			ct:fail("WTF ~p", [WTF1])
	end,
	ok = ar_events:send(peer, {joined, Peer, "localhost", 1234}),
	% just to make sure if this message processed
	timer:sleep(200),
	Rating = case ar_kv:get(RatingDB, term_to_binary(Peer)) of
		not_found ->
			ct:fail("Peer is not found");
		{ok, _} ->
			ok;
		{error, E2} ->
			ct:fail("Something went wrong ~p", [E2]);
		WTF2 ->
			ct:fail("WTF ~p", [WTF2])
	end,
	case ets:lookup(ar_rating, {peer, Peer}) of
		[] ->
			ct:fail("Peer is not found");
		[{_, _}] ->
			ok;
		WTF3 ->
			ct:fail("expecting {Rating, History}. got ~p", [WTF3])
	end,


	% restart ar_rating process to clear the state
	gen_server:stop(ar_rating),
	% wait a bit.
	timer:sleep(100),
	% ets table should belongs to the supervisor so this restart shouldnt
	% affect it
	case ets:lookup(ar_rating, {peer, Peer}) of
		[] ->
			ct:fail("Peer is not found");
		[{_, _}] ->
			ok;
		WTF4 ->
			ct:fail("expecting {Rating, History}. got ~p", [WTF4])
	end,
	% restarting process shouldn't affect 'joined' state.
	{state, true, _Changed1, _Rates1, _Triggers1, RatingDB1} = sys:get_state(ar_rating),
	% test peer leaving
	ok = ar_events:send(peer, {left, Peer}),
	timer:sleep(100),
	case ets:lookup(ar_rating, {peer, Peer}) of
		[] ->
			ok;
		[{_, _}] ->
			ct:fail("Peer is still there");
		WTF5 ->
			ct:fail("expecting empty list. got ~p", [WTF5])
	end,
	ok = ar_events:send(peer, {joined, Peer, "google.com", 8000}),
	timer:sleep(100),
	% Rating value should be the same as it was before the leaving
	% (this value was assigned earlier. see code above)
	Rating = case ar_kv:get(RatingDB1, term_to_binary(Peer)) of
		not_found ->
			ct:fail("Peer is not found");
		{ok, _} ->
			ok;
		{error, E3} ->
			ct:fail("Something went wrong ~p", [E3]);
		WTF6 ->
			ct:fail("WTF ~p", [WTF6])
	end.

check_rate_and_triggers(_Config) ->
	Rates = #{
		{request, tx} => 10,
		{request, block} => 20,
		{request, chunk} => 30,
		{push, tx} => 1000,
		{push, block} => 2000,
		{response, tx} => 1000,
		{response, block} => 2000,
		{response, chunk} => 3000,
		{response, any} => 1000,
		{request, malformed} => -1000,
		{response, malformed} => -10000,
		{response, request_timeout} => -1000,
		{response, connect_timeout} => -1, % can not be 0, otherwise trigger wont be called
		{response, not_found} => -500,
		{push, malformed} => -10000,
		{attack, any} => -10000
	},
	Triggers = #{
		{push, block} => {30, 3600, bonus, 500},
		{request, malformed} => {10, 3600, ban, 60},
		{request, tx} => {60, 60, penalty, 10},
		{response, connect_timeout} => {5, 300, offline, 0},
		{attack, any} => {1, 0, ban, 1440}
	},

	% override default rates and triggers
	ar_rating:set_triggers(Triggers),
	ar_rating:set_rates(Rates),

	{state, Joined, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating),
	case Joined of
		false ->
			true = check_network_join(),
			{state, true, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating);
		_ ->
			ok
	end,
	% ========================================================================================
	% CASE accounting income requests (positive) : join peer, do some requests, check rating
	% make sure if this peer was joined. otherwise all the events will be ignored
	% ========================================================================================
	PeerIncReq = peerIncReq,
	ok = ar_events:send(peer, {joined, PeerIncReq, "localhost", 1984}),
	timer:sleep(100),
	% the age of this peer should be enough to make rating be viable. (current
	% age influence is getting close to 1 in around 10 days
	T = os:system_time(second),
	case ets:lookup(ar_rating, {peer, PeerIncReq}) of
		[{_, Rating1}] ->
			ets:insert(ar_rating, {{peer, PeerIncReq}, Rating1#rating{since = T - 100000}});
		_ ->
			ct:fail("got wrong rating from ets")
	end,
	% generate events
	EventPeer = #event_peer{
		peer = PeerIncReq,
		time = 0
	},
	ok = ar_events:send(peer, {request, tx, EventPeer}),
	ok = ar_events:send(peer, {request, block, EventPeer}),
	ok = ar_events:send(peer, {request, chunk, EventPeer}),
	timer:sleep(100),
	gen_server:cast(ar_rating, compute_ratings),
	timer:sleep(100),
	RateRequestTX = maps:get({request, tx}, Rates, 0),
	RateRequestBlock = maps:get({request, block}, Rates, 0),
	RateRequestChunk = maps:get({request, chunk}, Rates, 0),
	PeerIncReq_rating = ar_rating:rate_with_parameters(RateRequestTX, [{time, 0}])
				+ ar_rating:rate_with_parameters(RateRequestBlock, [{time, 0}])
				+ ar_rating:rate_with_parameters(RateRequestChunk, [{time, 0}]),
	case ets:lookup(ar_rating, {peer, PeerIncReq}) of
		[{_, Rating2}] ->
			Influence = ar_rating:influence(Rating2),
			True = trunc(Influence * PeerIncReq_rating) == Rating2#rating.r,
			True = true;
		_ ->
			ct:fail("got wrong rating from ets")
	end,

	% ========================================================================================
	% CASE accounting income requests (negative) : peer is already joined, do some malformed requests,
	% check rating
	% ========================================================================================
	% make zero rating
	ets:insert(ar_rating, {{peer, PeerIncReq}, #rating{since = T - 100000}}),
	ok = ar_events:send(peer, {request, malformed, EventPeer}),
	timer:sleep(100),
	gen_server:cast(ar_rating, compute_ratings),
	timer:sleep(100),
	RateMalformedRequest = maps:get({request, malformed}, Rates, 0),
	PeerIncReqMalformed_rating = ar_rating:rate_with_parameters(RateMalformedRequest, [{time, 0}]),
	case ets:lookup(ar_rating, {peer, PeerIncReq}) of
		[{_, Rating3}] ->
			Influence1 = ar_rating:influence(Rating3),
			True1 = trunc(Influence1 * PeerIncReqMalformed_rating) == Rating3#rating.r,
			True1 = true;
		_ ->
			ct:fail("got wrong rating from ets")
	end,

	% ========================================================================================
	% CASE accounting push/response (with time influence) : peer is already joined, do some requests
	% with different timing, check rating
	% ========================================================================================
	% make zero rating
	ets:insert(ar_rating, {{peer, PeerIncReq}, #rating{since = T - 100000}}),
	EventPeer100 = #event_peer{
		peer = PeerIncReq,
		time = 100
	},
	EventPeer1000 = #event_peer{
		peer = PeerIncReq,
		time = 1000
	},
	EventPeer2000 = #event_peer{
		peer = PeerIncReq,
		time = 2000
	},
	% (1000 - 100) + (1000 - 1000) + (1000 - 2000) = -100 * Influence
	ok = ar_events:send(peer, {push, tx, EventPeer100}),
	ok = ar_events:send(peer, {push, tx, EventPeer1000}), % should make bonus = 0 due to long time
	ok = ar_events:send(peer, {push, tx, EventPeer2000}), % should make bonus negative due to long time
	timer:sleep(100),
	gen_server:cast(ar_rating, compute_ratings),
	timer:sleep(100),
	RatePushTX = maps:get({push, tx}, Rates, 0),
	PeerIncReqPushTX_rating = ar_rating:rate_with_parameters(RatePushTX, [{time, 100}])
								+ ar_rating:rate_with_parameters(RatePushTX, [{time, 1000}])
								+ ar_rating:rate_with_parameters(RatePushTX, [{time, 2000}]),
	case ets:lookup(ar_rating, {peer, PeerIncReq}) of
		[{_, Rating4}] ->
			Influence2 = ar_rating:influence(Rating4),
			True2 = trunc(Influence2 * PeerIncReqPushTX_rating) == Rating4#rating.r,
			True2 = true;
		_ ->
			ct:fail("got wrong rating from ets")
	end,

	% ========================================================================================
	% CASE trigger 'bonus' : peer is already joined, do some requests, catch 'bonus' event,
	% check rating
	% ========================================================================================
	% make zero rating
	ets:insert(ar_rating, {{peer, PeerIncReq}, #rating{since = T - 100000}}),
	lists:map(fun(_) ->
					ar_events:send(peer, {push, block, EventPeer1000})
			  end, lists:seq(1,31)),
	timer:sleep(100),
	gen_server:cast(ar_rating, compute_ratings),
	timer:sleep(100),
	case ets:lookup(ar_rating, {peer, PeerIncReq}) of
		[{_, Rating5}] ->
			Influence3 = ar_rating:influence(Rating5),
			% 2000 - bonus for the {push, block}
			% -1000 - for the low timing
			% for the first 29 events peer got bonuses = 29 * (2000 - 1000) => 29000
			% for the 30th event = 1000 + extra 500 (trigger: bonus)
			% for the 31 - the same: bonus 1000 + extra 500 (trigger: bonus)
			% so in total peer should have 32000 and the final rate
			% will be trunc(32000 * Influence)
			True3 = trunc(Influence3 * (500+500+1000*31)) == Rating5#rating.r,
			True3 = true;
		_ ->
			ct:fail("got wrong rating from ets")
	end,


	% ========================================================================================
	% CASE trigger 'penalty' : peer is already joined, do some requests, catch 'penalty' event,
	% check rating
	% ========================================================================================
	% make zero rating
	ets:insert(ar_rating, {{peer, PeerIncReq}, #rating{r = 1000, since = T - 100000}}),
	lists:map(fun(_) ->
					ar_events:send(peer, {request, tx, EventPeer})
			  end, lists:seq(1,61)),
	timer:sleep(100),
	gen_server:cast(ar_rating, compute_ratings),
	timer:sleep(100),
	case ets:lookup(ar_rating, {peer, PeerIncReq}) of
		[{_, Rating6}] ->
			Influence4 = ar_rating:influence(Rating6),
			% 10 - bonus for the {request, tx}
			% for the first 59 events peer got bonuses = 59 * 10 => 590
			% for the 60th event = bonus 10 + extra -10 (trigger: penalty)
			% for the 61 - the same: bonus 10 + extra -10 (trigger: penalty)
			% so in total peer should have 590 and the final rate
			% will be trunc(590 * Influence)
			True4 = trunc(Influence4 * (590)) == Rating6#rating.r,
			True4 = true;
		_ ->
			ct:fail("got wrong rating from ets")
	end,
	% ========================================================================================
	% CASE trigger 'ban' for a single event : peer is already joined, do some requests,
	% catch 'ban' event, check rating
	% ========================================================================================
	% make zero rating
	ets:insert(ar_rating, {{peer, PeerIncReq}, #rating{since = T - 100000}}),
	% subscribe for the 'access' events to catch a 'ban'
	ar_events:subscribe([access]),
	T1 = os:system_time(second),
	ar_events:send(peer, {attack, any, EventPeer}),
	BanTimeAttack = wait_ban(PeerIncReq),
	BanTimeAttack = T1 +60*1440,
	ar_events:cancel(access),

	% ========================================================================================
	% CASE trigger 'ban' : peer is already joined, do some requests, catch 'ban' event,
	% check rating
	% ========================================================================================
	% make zero rating
	ets:insert(ar_rating, {{peer, PeerIncReq}, #rating{since = T - 100000}}),
	% subscribe for the 'access' events to catch a 'ban'
	ar_events:subscribe([access]),
	T2 = os:system_time(second),
	lists:map(fun(_) ->
					ar_events:send(peer, {request, malformed, EventPeer})
			  end, lists:seq(1,11)),
	BanTime = wait_ban(PeerIncReq),
	BanTime = T2 +60*60,
	ar_events:cancel(access),

	% ========================================================================================
	% CASE trigger 'offline' : peer is already joined, do some requests, catch 'offline' event,
	% check rating, check status offline/online
	% ========================================================================================
	% make zero rating
	ets:insert(ar_rating, {{peer, PeerIncReq}, #rating{since = T - 100000}}),
	ar_events:subscribe([peer]),
	lists:map(fun(_) ->
					ar_events:send(peer, {response, connect_timeout, EventPeer})
			  end, lists:seq(1,7)),

	wait_offline(PeerIncReq),
	case ets:lookup(ar_rating, {peer, PeerIncReq}) of
		[] ->
			ok;
		_ ->
			ct:fail("peer is still online")
	end,
	ok.

check_get_top_n_get_banned(_Config) ->
	{state, Joined, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating),
	case Joined of
		false ->
			true = check_network_join(),
			{state, true, _Changed, Rates, _Triggers, RatingDB} = sys:get_state(ar_rating);
		_ ->
			ok
	end,

	RateSamples = [{peerA,100,false}, {peerB,1,false}, {peerC,1,true},
				   {peerD,200,false}, {peerE,300,true}, {peerF,250, false},
				   {peerG,10, false}, {peerH,15, false}, {peerI,1000, true},
				   {peerK,999, false}, {peerL,2,false}, {peerM,8,true},
				   {peerN, 99,false}],
	lists:map(fun({Peer, R, Banned}) ->
				Ban = case Banned of
						true ->
							  os:system_time(second) + 10000000;
						_ ->
							  0
					end,
				Rating = #rating{r = R, ban = Ban},
				RatingBinary = term_to_binary(Rating),
				PeerBinary = term_to_binary(Peer),
				ar_kv:put(RatingDB, PeerBinary, RatingBinary),
				ets:insert(ar_rating, {{peer, Peer}, Rating})
			end, RateSamples),

	% insert random data (to ets) to make shure if selected only valid {peer, Peer} values
	ets:insert(ar_rating, {peer, peerA, #rating{r=999999999}}),
	ets:insert(ar_rating, {peerA, peer, #rating{r=777777777}}),
	% ...and add a couple of valid ones into the ets tabe
	ets:insert(ar_rating, {{peer, peerX}, #rating{ban=0, r=888}}),
	ets:insert(ar_rating, {{peer, peerY}, #rating{ban=os:system_time(second) + 10000000, r=888}}),

	% get_top (from RocksDB)
	[{peerK,999,undefined,1984}] = ar_rating:get_top(1),

	[{peerK,999,undefined,1984}, {peerF,250,undefined,1984}, {peerD,200,undefined,1984},
	 {peerA,100,undefined,1984}, {peerN,99,undefined,1984}, {peerH,15,undefined,1984},
	 {peerG,10,undefined,1984}] = ar_rating:get_top(7),

	[{peerK,999,undefined,1984}, {peerF,250,undefined,1984}, {peerD,200,undefined,1984},
	 {peerA,100,undefined,1984}, {peerN,99,undefined,1984}, {peerH,15,undefined,1984},
	 {peerG,10,undefined,1984}, {peerL,2,undefined,1984}, {peerB,1,undefined,1984}] = ar_rating:get_top(100),

	%
	% get_top_joined (from ETS)
	[{peerK,999,undefined,1984}] = ar_rating:get_top_joined(1),

	[{peerK,999,undefined,1984}, {peerX, 888, undefined, 1984}, {peerF,250,undefined,1984},
	 {peerD,200,undefined,1984}, {peerA,100,undefined,1984}, {peerN,99,undefined,1984},
	 {peerH,15,undefined,1984} ] = ar_rating:get_top_joined(7),

	[{peerK,999,undefined,1984}, {peerX, 888, undefined, 1984}, {peerF,250,undefined,1984},
	 {peerD,200,undefined,1984}, {peerA,100,undefined,1984}, {peerN,99,undefined,1984},
	 {peerH,15,undefined,1984},  {peerG,10,undefined,1984}, {peerL,2,undefined,1984},
	 {peerB,1,undefined,1984}] = ar_rating:get_top_joined(100),

	% get_banned
	[{peerM,8,undefined,1984},{peerI,1000,undefined,1984},
	 {peerE,300,undefined,1984},{peerC,1,undefined,1984}] = ar_rating:get_banned(),

	% get peer info {Rating, Banned, Host, Port}.
	{100, 0, undefined, 1984} = ar_rating:get(peerA),
	ok.


%% Private functions

check_network_join() ->
	{state, Joined, _Changed, _Rates, _Triggers, _RatingDB} = sys:get_state(ar_rating),
	check_network_join(Joined).
check_network_join(false) ->
	% emulate event that we joined to the arweave network. otherwise everything
	% will be ignored
	ar_events:send(network, joined),
	% should be enough
	timer:sleep(100),
	{state, Joined, _Changed, _Rates, _Triggers, _RatingDB} = sys:get_state(ar_rating),
	Joined;
check_network_join(true) ->
	true.

wait_ban(Peer) ->
	receive
		{event, access, {ban, Peer, BanTime}} ->
			BanTime;
		X ->
			ct:fail("wrong message on 'access ban' event awaiting: ~p", [X])
	after 300 ->
		ct:fail("timeout on 'access ban' event awaiting")
	end.

wait_offline(Peer) ->
	receive
		{event, peer, {left, Peer}} ->
			ok;
		_ ->
			% ignore
			wait_offline(Peer)
	after 300 ->
		ct:fail("timeout on 'peer left' event awaiting")
	end.
