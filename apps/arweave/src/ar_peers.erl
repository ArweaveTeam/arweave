%%% @doc Tracks the availavility and performance of the network peers.
-module(ar_peers).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, get_peers/0, get_trusted_peers/0, is_public_peer/1,
		stats/0, discover_peers/0, rank_peers/1]).

-export([block_connections/0, unblock_connections/0]). % Only used in tests.

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% The frequency in milliseconds of ranking the known peers.
-ifdef(DEBUG).
-define(RANK_PEERS_FREQUENCY_MS, 2 * 1000).
-else.
-define(RANK_PEERS_FREQUENCY_MS, 2 * 60 * 1000).
-endif.

%% The frequency in milliseconds of asking some peers for their peers.
-ifdef(DEBUG).
-define(GET_MORE_PEERS_FREQUENCY_MS, 5000).
-else.
-define(GET_MORE_PEERS_FREQUENCY_MS, 240 * 1000).
-endif.

%% Peers to never add to the peer list.
-define(PEER_PERMANENT_BLACKLIST, []).

%% The maximum number of peers to return from get_peers/0.
-define(MAX_PEERS, 1000).

%% The number of failed requests in a row we tolerate before dropping the peer.
-define(TOLERATE_FAILURE_COUNT, 20).

-record(performance, {
	bytes = 0,
	time = 0,
	transfers = 0,
	failures = 0,
	rating = 0
}).

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

block_connections() ->
	ets:insert(?MODULE, {block_connections}),
	ok.

unblock_connections() ->
	ets:delete(?MODULE, block_connections),
	ok.

get_peers() ->
	case ets:member(?MODULE, block_connections) of
		true ->
			[];
		false ->
			case ets:lookup(?MODULE, peers) of
				[] ->
					[];
				[{_, Peers}] ->
					Peers
			end
	end.

get_trusted_peers() ->
	case ets:member(?MODULE, block_connections) of
		true ->
			[];
		false ->
			{ok, Config} = application:get_env(arweave, config),
			Config#config.peers
	end.

%% @doc Return true if the given peer has a public IPv4 address.
%% https://en.wikipedia.org/wiki/Reserved_IP_addresses.
is_public_peer({Oct1, Oct2, Oct3, Oct4, _Port}) ->
	is_public_peer({Oct1, Oct2, Oct3, Oct4});
is_public_peer({0, _, _, _}) ->
	false;
is_public_peer({10, _, _, _}) ->
	false;
is_public_peer({127, _, _, _}) ->
	false;
is_public_peer({100, Oct2, _, _}) when Oct2 >= 64 andalso Oct2 =< 127 ->
	false;
is_public_peer({169, 254, _, _}) ->
	false;
is_public_peer({172, Oct2, _, _}) when Oct2 >= 16 andalso Oct2 =< 31 ->
	false;
is_public_peer({192, 0, 0, _}) ->
	false;
is_public_peer({192, 0, 2, _}) ->
	false;
is_public_peer({192, 88, 99, _}) ->
	false;
is_public_peer({192, 168, _, _}) ->
	false;
is_public_peer({198, 18, _, _}) ->
	false;
is_public_peer({198, 19, _, _}) ->
	false;
is_public_peer({198, 51, 100, _}) ->
	false;
is_public_peer({203, 0, 113, _}) ->
	false;
is_public_peer({Oct1, _, _, _}) when Oct1 >= 224 ->
	false;
is_public_peer(_) ->
	true.

%% @doc Print statistics about the current peers.
stats() ->
	Connected = get_peers(),
	io:format("Connected peers, in preference order:~n"),
	stats(Connected),
	io:format("Other known peers:~n"),
	All = ets:foldl(fun({{peer, Peer}, _}, Acc) -> [Peer | Acc];
			(_, Acc) -> Acc end, [], ?MODULE),
	stats(All -- Connected).
stats(Peers) ->
	lists:foreach(fun(Peer) -> format_stats(Peer, get_or_init_performance(Peer)) end,
			Peers).

discover_peers() ->
	case ets:lookup(?MODULE, peers) of
		[] ->
			ok;
		[{_, []}] ->
			ok;
		[{_, Peers}] ->
			Peer = ar_util:pick_random(Peers),
			discover_peers(get_peer_peers(Peer))
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(peer),
	load_peers(),
	gen_server:cast(?MODULE, rank_peers),
	gen_server:cast(?MODULE, ping_peers),
	timer:apply_interval(?GET_MORE_PEERS_FREQUENCY_MS, ?MODULE, discover_peers, []),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({add_peer, Peer}, State) ->
	case ets:member(?MODULE, {peer, Peer}) of
		true ->
			ok;
		false ->
			ets:insert(?MODULE, {{peer, Peer}, #performance{}})
	end,
	{noreply, State};

handle_cast(rank_peers, State) ->
	Total =
		case ets:lookup(?MODULE, rating_total) of
			[] ->
				0;
			[{_, T}] ->
				T
		end,
	Peers =
		ets:foldl(
			fun	({{peer, Peer}, Performance}, Acc) ->
					%% Bigger score increases the chances to end up on the top
					%% of the peer list, but at the same time the ranking is
					%% probabilistic to always give everyone a chance to improve
					%% in the competition (i.e., reduce the advantage gained by
					%% being the first to earn a reputation).
					Score = rand:uniform() * Performance#performance.rating
							/ (Total + 0.0001),
					[{Peer, Score} | Acc];
				(_, Acc) ->
					Acc
			end,
			[],
			?MODULE
		),
	prometheus_gauge:set(arweave_peer_count, length(Peers)),
	ets:insert(?MODULE, {peers, lists:sublist(rank_peers(Peers), ?MAX_PEERS)}),
	ar_util:cast_after(?RANK_PEERS_FREQUENCY_MS, ?MODULE, rank_peers),
	{noreply, State};

handle_cast(ping_peers, State) ->
	[{peers, Peers}] = ets:lookup(?MODULE, peers),
	ping_peers(lists:sublist(Peers, 100)),
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
	{noreply, State}.

handle_info({event, peer, {made_request, Peer}}, State) ->
	case ets:member(?MODULE, {peer, Peer}) of
		true ->
			ok;
		false ->
			case check_external_peer(Peer) of
				ok ->
					ets:insert(?MODULE, {{peer, Peer}, #performance{}});
				_ ->
					ok
			end
	end,
	{noreply, State};

handle_info({event, peer, {served_tx, Peer, TimeDelta, Size}}, State) ->
	update_rating(Peer, TimeDelta, Size),
	{noreply, State};

handle_info({event, peer, {served_block, Peer, TimeDelta, Size}}, State) ->
	update_rating(Peer, TimeDelta, Size),
	{noreply, State};

handle_info({event, peer, {gossiped_tx, Peer, TimeDelta, Size}}, State) ->
	%% Only the first peer who sent the given transaction is rated.
	%% Otherwise, one may exploit the endpoint to gain reputation.
	case check_external_peer(Peer) of
		ok ->
			update_rating(Peer, TimeDelta, Size);
		_ ->
			ok
	end,
	{noreply, State};

handle_info({event, peer, {gossiped_block, Peer, TimeDelta, Size}}, State) ->
	%% Only the first peer who sent the given block is rated.
	%% Otherwise, one may exploit the endpoint to gain reputation.
	case check_external_peer(Peer) of
		ok ->
			update_rating(Peer, TimeDelta, Size);
		_ ->
			ok
	end,
	{noreply, State};

handle_info({event, peer, {served_chunk, Peer, TimeDelta, Size}}, State) ->
	update_rating(Peer, TimeDelta, Size),
	{noreply, State};

handle_info({event, peer, {bad_response, {Peer, _Type, _Reason}}}, State) ->
	Performance = get_or_init_performance(Peer),
	Failures = Performance#performance.failures,
	case Failures + 1 > ?TOLERATE_FAILURE_COUNT of
		true ->
			remove_peer(Peer);
		false ->
			Performance2 = Performance#performance{ failures = Failures + 1 },
			ets:insert(?MODULE, {{peer, Peer}, Performance2})
	end,
	{noreply, State};

handle_info({event, peer, {banned, BannedPeer}}, State) ->
	remove_peer(BannedPeer),
	{noreply, State};

handle_info({'EXIT', _, normal}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING("event: unhandled_info, message: ~p", [Message]),
	{noreply, State}.

terminate(_Reason, _State) ->
	store_peers().

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_peer_peers(Peer) ->
	case ar_http_iface_client:get_peers(Peer) of
		unavailable -> [];
		Peers -> Peers
	end.

discover_peers([]) ->
	ok;
discover_peers([Peer | Peers]) ->
	case ets:member(?MODULE, {peer, Peer}) of
		true ->
			ok;
		false ->
			IsPublic = is_public_peer(Peer),
			IsBanned = ar_blacklist_middleware:is_peer_banned(Peer) == banned,
			IsBlacklisted = lists:member(Peer, ?PEER_PERMANENT_BLACKLIST),
			case IsPublic andalso not IsBanned andalso not IsBlacklisted of
				false ->
					ok;
				true ->
					case ar_http_iface_client:get_info(Peer, height) of
						info_unavailable ->
							ok;
						_ ->
							gen_server:cast(?MODULE, {add_peer, Peer})
					end
			end
	end,
	discover_peers(Peers).

format_stats(Peer, Perf) ->
	io:format("\t~s ~.2f kB/s (~p transfers, ~B failures)~n",
		[string:pad(ar_util:format_peer(Peer), 20, trailing, $ ),
			(Perf#performance.bytes / 1024) / ((Perf#performance.time + 1) / 1000000),
			Perf#performance.transfers, Perf#performance.failures]).

load_peers() ->
	case ar_storage:read_term(peers) of
		not_found ->
			ok;
		{ok, {TotalRating, Records}} ->
			ets:insert(?MODULE, {rating_total, TotalRating}),
			load_peers(Records)
	end.

load_peers([]) ->
	ok;
load_peers([{Peer, Performance} | Peers]) ->
	ets:insert(?MODULE, {{peer, Peer}, Performance}),
	load_peers(Peers).

ping_peers(Peers) when length(Peers) < 100 ->
	ar_util:pmap(fun ar_http_iface_client:add_peer/1, Peers);
ping_peers(Peers) ->
	{Send, Rest} = lists:split(100, Peers),
	ar_util:pmap(fun ar_http_iface_client:add_peer/1, Send),
	ping_peers(Rest).

-ifdef(DEBUG).
%% Do not filter out loopback IP addresses with custom port in the debug mode
%% to allow multiple local VMs to peer with each other.
is_loopback_ip({127, _, _, _, Port}) ->
	{ok, Config} = application:get_env(arweave, config),
	Port == Config#config.port;
is_loopback_ip({_, _, _, _, _}) ->
	false.
-else.
%% @doc Is the IP address in question a loopback ('us') address?
is_loopback_ip({A, B, C, D, _Port}) -> is_loopback_ip({A, B, C, D});
is_loopback_ip({127, _, _, _}) -> true;
is_loopback_ip({0, _, _, _}) -> true;
is_loopback_ip({169, 254, _, _}) -> true;
is_loopback_ip({255, 255, 255, 255}) -> true;
is_loopback_ip({_, _, _, _}) -> false.
-endif.

%% @doc Return a ranked list of peers.
rank_peers(ScoredPeers) ->
	SortedReversed = lists:reverse(
			lists:sort(fun({_, S1}, {_, S2}) -> S1 >= S2 end, ScoredPeers)),
	GroupedBySubnet =
		lists:foldl(
			fun({{A, B, _C, _D, _Port}, _Score} = Peer, Acc) ->
				maps:update_with({A, B}, fun(L) -> [Peer | L] end, [Peer], Acc)
			end,
			#{},
			SortedReversed
		),
	ScoredSubnetPeers =
		maps:fold(
			fun(_Subnet, SubnetPeers, Acc) ->
				element(2, lists:foldl(
					fun({Peer, Score}, {N, Acc2}) ->
						%% At first we take the best peer from every subnet,
						%% then take the second best from every subnet, etc.
						{N + 1, [{Peer, {-N, Score}} | Acc2]}
					end,
					{0, Acc},
					SubnetPeers
				))
			end,
			[],
			GroupedBySubnet
		),
	[Peer || {Peer, _} <- lists:sort(fun({_, S1}, {_, S2}) -> S1 >= S2 end,
			ScoredSubnetPeers)].

check_external_peer(Peer) ->
	IsLoopbackIP = is_loopback_ip(Peer),
	IsBlacklisted = lists:member(Peer, ?PEER_PERMANENT_BLACKLIST),
	IsBanned = ar_blacklist_middleware:is_peer_banned(Peer) == banned,
	case {IsLoopbackIP, IsBlacklisted, IsBanned} of
		{true, _, _} ->
			reject;
		{_, true, _} ->
			reject;
		{_, _, true} ->
			reject;
		_ ->
			ok
	end.

update_rating(Peer, TimeDelta, Size) ->
	Performance = get_or_init_performance(Peer),
	Total = get_total_rating(),
	#performance{ bytes = Bytes, time = Time,
			rating = Rating, transfers = N } = Performance,
	Bytes2 = Bytes + Size,
	Time2 = Time + TimeDelta / 1000,
	Performance2 = Performance#performance{ bytes = Bytes2, time = Time2,
			rating = Rating2 = Bytes2 / (Time2 + 1), failures = 0, transfers = N + 1 },
	Total2 = Total - Rating + Rating2,
	ets:insert(?MODULE, [{{peer, Peer}, Performance2}, {rating_total, Total2}]).

get_or_init_performance(Peer) ->
	case ets:lookup(?MODULE, {peer, Peer}) of
		[] ->
			#performance{};
		[{_, Performance}] ->
			Performance
	end.

get_total_rating() ->
	case ets:lookup(?MODULE, rating_total) of
		[] ->
			0;
		[{_, Total}] ->
			Total
	end.

remove_peer(RemovedPeer) ->
	Total = get_total_rating(),
	Performance = get_or_init_performance(RemovedPeer),
	ets:insert(?MODULE, {rating_total, Total - Performance#performance.rating}),
	ets:delete(?MODULE, {peer, RemovedPeer}),
	case ets:lookup(?MODULE, peers) of
		[] ->
			ok;
		[{_, Peers}] ->
			Peers2 = [Peer || Peer <- Peers, Peer /= RemovedPeer],
			ets:insert(?MODULE, {peers, Peers2})
	end.

store_peers() ->
	case ets:lookup(?MODULE, rating_total) of
		[] ->
			ok;
		[{_, Total}] ->
			Records =
				ets:foldl(
					fun	({{peer, Peer}, Performance}, Acc) ->
							[{Peer, Performance} | Acc];
						(_, Acc) ->
							Acc
					end,
					[],
					?MODULE
				),
			ar_storage:write_term(peers, {Total, Records})
	end.
