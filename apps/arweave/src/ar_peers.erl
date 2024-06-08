%%% @doc Tracks the availability and performance of the network peers.
-module(ar_peers).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_peers.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, get_peers/1, get_peer_performances/1, get_trusted_peers/0,
	is_public_peer/1,
	get_peer_release/1, stats/1, discover_peers/0, add_peer/2,
	resolve_and_cache_peer/2, rate_fetched_data/4, rate_fetched_data/6,
	rate_gossiped_data/4, issue_warning/3
]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% The frequency in seconds of re-resolving DNS of peers configured by domain names.
-define(STORE_RESOLVED_DOMAIN_S, 60).

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

%% Minimum average_success we'll tolerate before dropping a peer.
-define(MINIMUM_SUCCESS, 0.8).
%% The alpha value in an EMA calculation is somewhat unintuitive:
%%
%% NewEma = (1 - Alpha) * OldEma + Alpha * NewValue
%%
%% When calculating the SuccessEma the NewValue is always either 1 or 0. So if we want to see how
%% many consecutive failures it will take to drop the SuccessEma from 1 to 0.5 (i.e. 50% failure
%% rate), a number of terms in the equation drop out and we're left with:
%%
%% 0.5 = (1 - Alpha) ^ N
%%
%% Where N is the number of consecutive failures.
%%
%% Setting Alpha to 0.1 we can determine the number of consecutive failures:
%% 0.5 = 0.9 ^ N
%% log(0.5) = N * log(0.9)
%% N = log(0.5) / log(0.9)
%% N = 6.58
%%
%% And if we want to set the Alpha such that it takes 20 consecutive failures to go from 1 to 0.5:
%% 0.5 = (1 - Alpha) ^ 20
%% log(0.5) = 20 * log(1 - Alpha)
%% 1 - Alpha = 10 ^ (log(0.5) / 20)
%% Alpha = 1 - 10 ^ (log(0.5) / 20)
%% Alpha = 0.035
-define(SUCCESS_ALPHA, 0.035).
%% The THROUGHPUT_ALPHA is even harder to intuit since the values being averaged can be any
%% positive number and are not just limited to 0 or 1. Perhaps one way to think about it is:
%% When a datapoint is first added to the average it is scaled by Alpha, and then every time
%% another datapoint is added, the contribution of all prior datapoints are scaled by (1-Alpha).
%% So how many new datapoints will it take to reduce the contribution of an earlier datapoint
%% to "virtually" 0?
%%
%% If we assume "virtually 0" is the same as 1% of its true value (i.e. if the datapoint was
%% originaly 100, it now contributes 1 to the average), then we can use a similar equation as
%% the SUCCESS_ALPHA equation to determine how many datapoints materially contribute to the average:
%%
%% 0.01 = (1 - Alpha) ^ N) * Alpha
%%
%% The additional "* Alpha" term is to account for the scaling that happens when a datapoint is
%% first added.
%%
%% With an Alpha of 0.05 we're essentially saying that the last ~31 datapoints contribute 99% of
%% the average:
%%
%% 0.01 = ((1 - 0.05) ^ N) * 0.05
%% 0.01 / 0.05 = (1 - 0.05) ^ N
%% log(0.2) = N * log(0.95)
%% N = log(0.2) / log(0.95)
%% N = 31.38
-define(THROUGHPUT_ALPHA, 0.05).

%% When processing block rejected events for blocks received from a peer, we handle rejections
%% differently based on the rejection reason.
-define(BLOCK_REJECTION_WARNING, [
	failed_to_fetch_first_chunk,
	failed_to_fetch_second_chunk,
	failed_to_fetch_chunk
]).
-define(BLOCK_REJECTION_BAN, [
	invalid_previous_solution_hash,
	invalid_last_retarget,
	invalid_difficulty,
	invalid_cumulative_difficulty,
	invalid_hash_preimage,
	invalid_nonce_limiter_seed_data,
	invalid_partition_number,
	invalid_nonce,
	invalid_pow,
	invalid_poa,
	invalid_poa2,
	invalid_nonce_limiter,
	invalid_nonce_limiter_cache_mismatch,
	invalid_chunk_hash,
	invalid_chunk2_hash
]).
-define(BLOCK_REJECTION_IGNORE, [
	invalid_signature,
	invalid_proof_size,
	invalid_first_chunk,
	invalid_second_chunk,
	invalid_poa2_recall_byte2_undefined,
	invalid_hash,
	invalid_timestamp,
	invalid_resigned_solution_hash,
	invalid_nonce_limiter_global_step_number
]).

%% We only do scoring of this many TCP ports per IP address. When there are not enough slots,
%% we remove the peer from the first slot.
-define(DEFAULT_PEER_PORT_MAP, {empty_slot, empty_slot, empty_slot, empty_slot, empty_slot,
		empty_slot, empty_slot, empty_slot, empty_slot, empty_slot}).

-record(state, {}).

%%%===================================================================
%%% Public interface.
%%%===================================================================

%% @doc Start the server.
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Return the list of peers in the given ranking order.
%% Rating is an estimate of the peer's effective throughput in bytes per millisecond.
%%
%% 'lifetime' considers all data ever received from this peer and is most useful when we care
%% more about identifying "good samaritans" rather than maximizing throughput (e.g. when
%% polling for new blocks are determing which peer's blocks to validated first).
%%
%% 'current' weights recently received data higher than old data and is most useful when we care
%% more about maximizing throughput (e.g. when syncing chunks).
get_peers(Ranking) ->
	case catch ets:lookup(?MODULE, {peers, Ranking}) of
		{'EXIT', _} ->
			[];
		[] ->
			[];
		[{_, Peers}] ->
			Peers
	end.

get_peer_performances(Peers) ->
	lists:foldl(
		fun(Peer, Map) ->
			Performance = get_or_init_performance(Peer),
			maps:put(Peer, Performance, Map)
		end,
		#{},
		Peers).

-if(?NETWORK_NAME == "arweave.N.1").
resolve_peers([]) ->
	[];
resolve_peers([RawPeer | Peers]) ->
	case ar_util:safe_parse_peer(RawPeer) of
		{ok, Peer} ->
			[Peer | resolve_peers(Peers)];
		{error, invalid} ->
			?LOG_WARNING([{event, failed_to_resolve_trusted_peer},
					{peer, RawPeer}]),
			resolve_peers(Peers)
	end.

get_trusted_peers() ->
	{ok, Config} = application:get_env(arweave, config),
	case Config#config.peers of
		[] ->
			ArweavePeers = ["sfo-1.na-west-1.arweave.net", "ams-1.eu-central-1.arweave.net",
					"fra-1.eu-central-2.arweave.net", "blr-1.ap-central-1.arweave.net",
					"sgp-1.ap-central-2.arweave.net"],
			resolve_peers(ArweavePeers);
		Peers ->
			Peers
	end.
-else.
get_trusted_peers() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.peers.
-endif.

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

%% @doc Return the release nubmer reported by the peer.
%% Return -1 if the release is not known.
get_peer_release(Peer) ->
	case catch ets:lookup(?MODULE, {peer, Peer}) of
		[{_, #performance{ release = Release }}] ->
			Release;
		_ ->
			-1
	end.

rate_fetched_data(Peer, DataType, LatencyMicroseconds, DataSize) ->
	rate_fetched_data(Peer, DataType, ok, LatencyMicroseconds, DataSize, 1).
rate_fetched_data(Peer, DataType, ok, LatencyMicroseconds, DataSize, Concurrency) ->
	gen_server:cast(?MODULE,
		{valid_data, Peer, DataType, LatencyMicroseconds / 1000, DataSize, Concurrency});
rate_fetched_data(Peer, DataType, _, _LatencyMicroseconds, _DataSize, _Concurrency) ->
	gen_server:cast(?MODULE, {invalid_data, Peer, DataType}).

rate_gossiped_data(Peer, DataType, LatencyMicroseconds, DataSize) ->
	case check_peer(Peer) of
		ok ->
			gen_server:cast(?MODULE,
				{valid_data, Peer, DataType,  LatencyMicroseconds / 1000, DataSize, 1});
		_ ->
			ok
	end.

issue_warning(Peer, _Type, _Reason) ->
	gen_server:cast(?MODULE, {warning, Peer}).

add_peer(Peer, Release) ->
	gen_server:cast(?MODULE, {add_peer, Peer, Release}).

%% @doc Print statistics about the current peers.
stats(Ranking) ->
	Connected = get_peers(Ranking),
	io:format("Connected peers, in ~s order:~n", [Ranking]),
	stats(Ranking, Connected),
	io:format("Other known peers:~n"),
	All = ets:foldl(
		fun
			({{peer, Peer}, _}, Acc) -> [Peer | Acc];
			(_, Acc) -> Acc
		end,
		[],
		?MODULE
	),
	stats(All -- Connected).
stats(Ranking, Peers) ->
	lists:foreach(
		fun(Peer) -> format_stats(Ranking, Peer, get_or_init_performance(Peer)) end,
		Peers
	).

discover_peers() ->
	case get_peers(lifetime) of
		[] ->
			ok;
		Peers ->
			Peer = ar_util:pick_random(Peers),
			discover_peers(get_peer_peers(Peer))
	end.

%% @doc Resolve the domain name of the given peer (if the given peer is an IP address)
%% and cache it. Invalidate the cache after ?STORE_RESOLVED_DOMAIN_S seconds.
%%
%% Return {ok, Peer} | {error, Reason}.
resolve_and_cache_peer(RawPeer, Type) ->
	Now = os:system_time(second),
	case ets:lookup(?MODULE, {raw_peer, RawPeer}) of
		[] ->
			case ar_util:safe_parse_peer(RawPeer) of
				{ok, Peer} ->
					ets:insert(?MODULE, {{raw_peer, RawPeer}, {Peer, Now}}),
					ets:insert(?MODULE, {{Type, Peer}, RawPeer}),
					{ok, Peer};
				Error ->
					Error
			end;
		[{_, {Peer, Timestamp}}] ->
			case Timestamp + ?STORE_RESOLVED_DOMAIN_S < Now of
				true ->
					case ar_util:safe_parse_peer(RawPeer) of
						{ok, Peer2} ->
							%% The cache entry has expired.
							ets:delete(?MODULE, {Type, {Peer, Timestamp}}),
							ets:insert(?MODULE, {{raw_peer, RawPeer}, {Peer2, Now}}),
							ets:insert(?MODULE, {{Type, Peer2}, RawPeer}),
							{ok, Peer2};
						Error ->
							Error
					end;
				false ->
					{ok, Peer}
			end
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(block),
	load_peers(),
	gen_server:cast(?MODULE, rank_peers),
	gen_server:cast(?MODULE, ping_peers),
	timer:apply_interval(?GET_MORE_PEERS_FREQUENCY_MS, ?MODULE, discover_peers, []),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
	{reply, ok, State}.

handle_cast({add_peer, Peer, Release}, State) ->
	maybe_add_peer(Peer, Release),
	{noreply, State};

handle_cast(rank_peers, State) ->
	LifetimePeers = score_peers(lifetime),
	CurrentPeers = score_peers(current),
	prometheus_gauge:set(arweave_peer_count, length(LifetimePeers)),
	set_ranked_peers(lifetime, rank_peers(LifetimePeers)),
	set_ranked_peers(current, rank_peers(CurrentPeers)),
	ar_util:cast_after(?RANK_PEERS_FREQUENCY_MS, ?MODULE, rank_peers),
	{noreply, State};

handle_cast(ping_peers, State) ->
	Peers = get_peers(lifetime),
	ping_peers(lists:sublist(Peers, 100)),
	{noreply, State};

handle_cast({valid_data, Peer, _DataType, LatencyMilliseconds, DataSize, Concurrency},
		State) ->
	update_rating(Peer, LatencyMilliseconds, DataSize, Concurrency, true),
	{noreply, State};

handle_cast({invalid_data, Peer, _DataType}, State) ->
	update_rating(Peer, false),
	{noreply, State};

handle_cast({warning, Peer}, State) ->
	Performance = update_rating(Peer, false),
	case Performance#performance.average_success < ?MINIMUM_SUCCESS of
		true ->
			remove_peer(Peer);
		false ->
			ok
	end,
	{noreply, State};

handle_cast(Cast, State) ->
	?LOG_WARNING([{event, unhandled_cast}, {module, ?MODULE}, {cast, Cast}]),
	{noreply, State}.

handle_info({event, block, {rejected, Reason, _H, Peer}}, State) when Peer /= no_peer ->
	IssueBan = lists:member(Reason, ?BLOCK_REJECTION_BAN),
	IssueWarning = lists:member(Reason, ?BLOCK_REJECTION_WARNING),
	Ignore = lists:member(Reason, ?BLOCK_REJECTION_IGNORE),

	case {IssueBan, IssueWarning, Ignore} of
		{true, false, false} ->
			ar_blacklist_middleware:ban_peer(Peer, ?BAD_BLOCK_BAN_TIME),
			remove_peer(Peer);
		{false, true, false} ->
			issue_warning(Peer, block_rejected, Reason);
		{false, false, true} ->
			%% ignore
			ok;
		_ ->
			%% Ever reason should be in exactly 1 list.
			error("invalid block rejection reason")
	end,
	{noreply, State};

handle_info({event, block, _}, State) ->
	{noreply, State};

handle_info({'EXIT', _, normal}, State) ->
	{noreply, State};

handle_info(Message, State) ->
	?LOG_WARNING([{event, unhandled_info}, {module, ?MODULE}, {message, Message}]),
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

get_or_init_performance(Peer) ->
	case ets:lookup(?MODULE, {peer, Peer}) of
		[] ->
			#performance{};
		[{_, Performance}] ->
			Performance
	end.

set_performance(Peer, Performance) ->
	ets:insert(?MODULE, [{{peer, Peer}, Performance}]).

get_total_rating(Rating) ->
	case ets:lookup(?MODULE, {rating_total, Rating}) of
		[] ->
			0;
		[{_, Total}] ->
			Total
	end.

set_total_rating(Rating, Total) ->
	ets:insert(?MODULE, {{rating_total, Rating}, Total}).

recalculate_total_rating(Rating) ->
	TotalRating = ets:foldl(
		fun	({{peer, _Peer}, Performance}, Acc) ->
				Acc + get_peer_rating(Rating, Performance);
			(_, Acc) ->
				Acc
		end,
		0,
		?MODULE
	),
	set_total_rating(Rating, TotalRating).

get_peer_rating(Rating, Performance) ->
	case Rating of
		lifetime ->
			Performance#performance.lifetime_rating;
		current ->
			Performance#performance.current_rating
	end.

discover_peers([]) ->
	ok;
discover_peers([Peer | Peers]) ->
	case ets:member(?MODULE, {peer, Peer}) of
		true ->
			ok;
		false ->
			case check_peer(Peer, is_public_peer(Peer)) of
				ok ->
					case ar_http_iface_client:get_info(Peer, release) of
						Release when is_integer(Release) ->
							maybe_add_peer(Peer, Release);
						_ ->
							ok
					end;
				_ ->
					ok
			end
	end,
	discover_peers(Peers).

format_stats(lifetime, Peer, Perf) ->
	KB = Perf#performance.total_bytes / 1024,
	io:format(
		"\t~s ~.2f kB/s (~.2f kB, ~.2f success, ~p transfers)~n",
		[string:pad(ar_util:format_peer(Peer), 21, trailing, $\s),
			float(Perf#performance.lifetime_rating), KB,
			Perf#performance.average_success, Perf#performance.total_transfers]);
format_stats(current, Peer, Perf) ->
	io:format(
		"\t~s ~.2f kB/s (~.2f success)~n",
		[string:pad(ar_util:format_peer(Peer), 21, trailing, $\s),
			float(Perf#performance.current_rating),
			Perf#performance.average_success]).

load_peers() ->
	case ar_storage:read_term(peers) of
		not_found ->
			ok;
		{ok, {_TotalRating, Records}} ->
			?LOG_INFO([{event, polling_saved_peers}]),
			ar:console("Polling saved peers...~n"),
			load_peers(Records),
			recalculate_total_rating(lifetime),
			recalculate_total_rating(current),
			?LOG_INFO([{event, polled_saved_peers}]),
			ar:console("Polled saved peers.~n")
	end.

load_peers(Peers) when length(Peers) < 20 ->
	ar_util:pmap(fun load_peer/1, Peers);
load_peers(Peers) ->
	{Peers2, Peers3} = lists:split(20, Peers),
	ar_util:pmap(fun load_peer/1, Peers2),
	load_peers(Peers3).

load_peer({Peer, Performance}) ->
	case ar_http_iface_client:get_info(Peer, network) of
		info_unavailable ->
			?LOG_DEBUG([{event, peer_unavailable}, {peer, ar_util:format_peer(Peer)}]),
			ok;
		<<?NETWORK_NAME>> ->
			maybe_rotate_peer_ports(Peer),
			case Performance of
				{performance, TotalBytes, _TotalLatency, Transfers, _Failures, Rating} ->
					%% For backwards compatibility.
					set_performance(Peer, #performance{
						total_bytes = TotalBytes,
						total_throughput = Rating,
						total_transfers = Transfers,
						average_throughput = Rating,
						lifetime_rating = Rating,
						current_rating = Rating
					});
				{performance, TotalBytes, _TotalLatency, Transfers, _Failures, Rating, Release} ->
					%% For backwards compatibility.
					set_performance(Peer, #performance{
						release = Release,
						total_bytes = TotalBytes,
						total_throughput = Rating,
						total_transfers = Transfers,
						average_throughput = Rating,
						lifetime_rating = Rating,
						current_rating = Rating
					});
				{performance, 3,
						_Release, _TotalBytes, _TotalThroughput, _TotalTransfers,
						_AverageLatency, _AverageThroughput, _AverageSuccess, _LifetimeRating,
						_CurrentRating} ->
					%% Going forward whenever we change the #performance record we should increment the
					%% version field so we can match on it when doing a load. Here we're handling the
					%% version 3 format.
					set_performance(Peer, Performance)
			end,
			ok;
		Network ->
			?LOG_DEBUG([{event, peer_from_the_wrong_network},
					{peer, ar_util:format_peer(Peer)}, {network, Network}]),
			ok
	end.

maybe_rotate_peer_ports(Peer) ->
	{IP, Port} = get_ip_port(Peer),
	case ets:lookup(?MODULE, {peer_ip, IP}) of
		[] ->
			ets:insert(?MODULE, {{peer_ip, IP},
					{erlang:setelement(1, ?DEFAULT_PEER_PORT_MAP, Port), 1}});
		[{_, {PortMap, Position}}] ->
			case is_in_port_map(Port, PortMap) of
				{true, _} ->
					ok;
				false ->
					MaxSize = erlang:size(?DEFAULT_PEER_PORT_MAP),
					case Position < MaxSize of
						true ->
							ets:insert(?MODULE, {{peer_ip, IP},
									{erlang:setelement(Position + 1, PortMap, Port),
									Position + 1}});
						false ->
							RemovedPeer = construct_peer(IP, element(1, PortMap)),
							PortMap2 = shift_port_map_left(PortMap),
							PortMap3 = erlang:setelement(MaxSize, PortMap2, Port),
							ets:insert(?MODULE, {{peer_ip, IP}, {PortMap3, MaxSize}}),
							remove_peer(RemovedPeer)
					end
			end
	end.

get_ip_port({A, B, C, D, Port}) ->
	{{A, B, C, D}, Port}.

construct_peer({A, B, C, D}, Port) ->
	{A, B, C, D, Port}.

is_in_port_map(Port, PortMap) ->
	is_in_port_map(Port, PortMap, erlang:size(PortMap), 1).

is_in_port_map(_Port, _PortMap, Max, N) when N > Max ->
	false;
is_in_port_map(Port, PortMap, Max, N) ->
	case element(N, PortMap) == Port of
		true ->
			{true, N};
		false ->
			is_in_port_map(Port, PortMap, Max, N + 1)
	end.

shift_port_map_left(PortMap) ->
	shift_port_map_left(PortMap, erlang:size(PortMap), 1).

shift_port_map_left(PortMap, Max, N) when N == Max ->
	erlang:setelement(N, PortMap, empty_slot);
shift_port_map_left(PortMap, Max, N) ->
	PortMap2 = erlang:setelement(N, PortMap, element(N + 1, PortMap)),
	shift_port_map_left(PortMap2, Max, N + 1).

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

score_peers(Rating) ->
	Total = get_total_rating(Rating),
	ets:foldl(
		fun ({{peer, Peer}, Performance}, Acc) ->
				%% Bigger score increases the chances to end up on the top
				%% of the peer list, but at the same time the ranking is
				%% probabilistic to always give everyone a chance to improve
				%% in the competition (i.e., reduce the advantage gained by
				%% being the first to earn a reputation).
				Score = rand:uniform() * get_peer_rating(Rating, Performance)
						/ (Total + 0.0001),
				[{Peer, Score} | Acc];
			(_, Acc) ->
				Acc
		end,
		[],
		?MODULE
	).

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
	[Peer || {Peer, _} <- lists:sort(
		fun({_, S1}, {_, S2}) -> S1 >= S2 end,
		ScoredSubnetPeers
	)].

set_ranked_peers(Rating, Peers) ->
	ets:insert(?MODULE, {{peers, Rating}, lists:sublist(Peers, ?MAX_PEERS)}).

check_peer(Peer) ->
	check_peer(Peer, not is_loopback_ip(Peer)).
check_peer(Peer, IsPeerScopeValid) ->
	IsBlacklisted = lists:member(Peer, ?PEER_PERMANENT_BLACKLIST),
	IsBanned = ar_blacklist_middleware:is_peer_banned(Peer) == banned,
	case IsPeerScopeValid andalso not IsBlacklisted andalso not IsBanned of
		true ->
			ok;
		false ->
			reject
	end.

update_rating(Peer, IsSuccess) ->
	update_rating(Peer, undefined, undefined, 1, IsSuccess).
update_rating(Peer, LatencyMilliseconds, DataSize, Concurrency, false)
  		when LatencyMilliseconds =/= undefined; DataSize =/= undefined ->
	%% Don't credit peers for failed requests.
	update_rating(Peer, undefined, undefined, Concurrency, false);
update_rating(Peer, 0, _DataSize, Concurrency, IsSuccess) ->
	update_rating(Peer, undefined, undefined, Concurrency, IsSuccess);
update_rating(Peer, 0.0, _DataSize, Concurrency, IsSuccess) ->
	update_rating(Peer, undefined, undefined, Concurrency, IsSuccess);
update_rating(Peer, LatencyMilliseconds, DataSize, Concurrency, IsSuccess) ->
	Performance = get_or_init_performance(Peer),

	#performance{
		total_bytes = TotalBytes,
		total_throughput = TotalThroughput,
		total_transfers = TotalTransfers,
		average_latency = AverageLatency,
		average_throughput = AverageThroughput,
		average_success = AverageSuccess,
		lifetime_rating = LifetimeRating,
		current_rating = CurrentRating
	} = Performance,
	TotalBytes2 = case DataSize of
		undefined -> TotalBytes;
		_ -> TotalBytes + DataSize
	end,
	AverageLatency2 = case LatencyMilliseconds of
		undefined -> AverageLatency;
		_ -> calculate_ema(AverageLatency, LatencyMilliseconds, ?THROUGHPUT_ALPHA)
	end,
	%% In order to approximate the impact of multiple concurrent requests we multiply
	%% DataSize by the Concurrency value. We do this *only* when updating the AverageThroughput
	%% value so that it doesn't distort the TotalThroughput.
	AverageThroughput2 = case LatencyMilliseconds of
		undefined -> AverageThroughput;
		_ -> calculate_ema(
			AverageThroughput, (DataSize * Concurrency) / LatencyMilliseconds, ?THROUGHPUT_ALPHA)
	end,
	TotalThroughput2 = case LatencyMilliseconds of
		undefined -> TotalThroughput;
		_ -> TotalThroughput + (DataSize / LatencyMilliseconds)
	end,
	TotalTransfers2 = case DataSize of
		undefined -> TotalTransfers;
		_ -> TotalTransfers + 1
	end,
	AverageSuccess2 = calculate_ema(AverageSuccess, ar_util:bool_to_int(IsSuccess), ?SUCCESS_ALPHA),
	%% Rating is an estimate of the peer's effective throughput in bytes per millisecond.
	%% 'lifetime' considers all data ever received from this peer
	%% 'current' considers recently received data
	LifetimeRating2 = case TotalTransfers2 > 0 of
		true -> (TotalThroughput2 / TotalTransfers2) * AverageSuccess2;
		_ -> LifetimeRating
	end,
	CurrentRating2 = case AverageThroughput2 > 0 of
		true -> AverageThroughput2 * AverageSuccess2;
		_ -> CurrentRating
	end,
	Performance2 = Performance#performance{
		total_bytes = TotalBytes2,
		total_throughput = TotalThroughput2,
		total_transfers = TotalTransfers2,
		average_latency = AverageLatency2,
		average_throughput = AverageThroughput2,
		average_success = AverageSuccess2,
		lifetime_rating = LifetimeRating2,
		current_rating = CurrentRating2
	},
	TotalLifetimeRating = get_total_rating(lifetime),
	TotalLifetimeRating2 = TotalLifetimeRating - LifetimeRating + LifetimeRating2,
	TotalCurrentRating = get_total_rating(current),
	TotalCurrentRating2 = TotalCurrentRating - CurrentRating + CurrentRating2,

	maybe_rotate_peer_ports(Peer),
	set_performance(Peer, Performance2),
	set_total_rating(lifetime, TotalLifetimeRating2),
	set_total_rating(current, TotalCurrentRating2),
	Performance2.

calculate_ema(OldEMA, Value, Alpha) ->
	Alpha * Value + (1 - Alpha) * OldEMA.

maybe_add_peer(Peer, Release) ->
	maybe_rotate_peer_ports(Peer),
	case ets:lookup(?MODULE, {peer, Peer}) of
		[{_, #performance{ release = Release }}] ->
			ok;
		[{_, Performance}] ->
			set_performance(Peer, Performance#performance{ release = Release });
		[] ->
			case check_peer(Peer) of
				ok ->
					set_performance(Peer, #performance{ release = Release });
				_ ->
					ok
			end
	end.

remove_peer(RemovedPeer) ->
	?LOG_DEBUG([
		{event, remove_peer},
		{peer, ar_util:format_peer(RemovedPeer)}
	]),
	Performance = get_or_init_performance(RemovedPeer),
	TotalLifetimeRating = get_total_rating(lifetime),
	TotalCurrentRating = get_total_rating(current),
	set_total_rating(lifetime, TotalLifetimeRating - get_peer_rating(lifetime, Performance)),
	set_total_rating(current, TotalCurrentRating - get_peer_rating(current, Performance)),
	ets:delete(?MODULE, {peer, RemovedPeer}),
	remove_peer_port(RemovedPeer),
	ar_events:send(peer, {removed, RemovedPeer}).

remove_peer_port(Peer) ->
	{IP, Port} = get_ip_port(Peer),
	case ets:lookup(?MODULE, {peer_ip, IP}) of
		[] ->
			ok;
		[{_, {PortMap, Position}}] ->
			case is_in_port_map(Port, PortMap) of
				false ->
					ok;
				{true, N} ->
					PortMap2 = erlang:setelement(N, PortMap, empty_slot),
					case is_port_map_empty(PortMap2) of
						true ->
							ets:delete(?MODULE, {peer_ip, IP});
						false ->
							ets:insert(?MODULE, {{peer_ip, IP}, {PortMap2, Position}})
					end
			end
	end.

is_port_map_empty(PortMap) ->
	is_port_map_empty(PortMap, erlang:size(PortMap), 1).

is_port_map_empty(_PortMap, Max, N) when N > Max ->
	true;
is_port_map_empty(PortMap, Max, N) ->
	case element(N, PortMap) of
		empty_slot ->
			is_port_map_empty(PortMap, Max, N + 1);
		_ ->
			false
	end.

store_peers() ->
	case get_total_rating(lifetime) of
		0 ->
			ok;
		Total ->
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

%%%===================================================================
%%% Tests.
%%%===================================================================

rotate_peer_ports_test() ->
	Peer = {2, 2, 2, 2, 1},
	maybe_rotate_peer_ports(Peer),
	[{_, {PortMap, 1}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(1, element(1, PortMap)),
	remove_peer(Peer),
	?assertEqual([], ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}})),
	maybe_rotate_peer_ports(Peer),
	Peer2 = {2, 2, 2, 2, 2},
	maybe_rotate_peer_ports(Peer2),
	[{_, {PortMap2, 2}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(1, element(1, PortMap2)),
	?assertEqual(2, element(2, PortMap2)),
	remove_peer(Peer),
	[{_, {PortMap3, 2}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(empty_slot, element(1, PortMap3)),
	?assertEqual(2, element(2, PortMap3)),
	Peer3 = {2, 2, 2, 2, 3},
	Peer4 = {2, 2, 2, 2, 4},
	Peer5 = {2, 2, 2, 2, 5},
	Peer6 = {2, 2, 2, 2, 6},
	Peer7 = {2, 2, 2, 2, 7},
	Peer8 = {2, 2, 2, 2, 8},
	Peer9 = {2, 2, 2, 2, 9},
	Peer10 = {2, 2, 2, 2, 10},
	Peer11 = {2, 2, 2, 2, 11},
	maybe_rotate_peer_ports(Peer3),
	maybe_rotate_peer_ports(Peer4),
	maybe_rotate_peer_ports(Peer5),
	maybe_rotate_peer_ports(Peer6),
	maybe_rotate_peer_ports(Peer7),
	maybe_rotate_peer_ports(Peer8),
	maybe_rotate_peer_ports(Peer9),
	maybe_rotate_peer_ports(Peer10),
	[{_, {PortMap4, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(empty_slot, element(1, PortMap4)),
	?assertEqual(2, element(2, PortMap4)),
	?assertEqual(10, element(10, PortMap4)),
	maybe_rotate_peer_ports(Peer8),
	maybe_rotate_peer_ports(Peer9),
	maybe_rotate_peer_ports(Peer10),
	[{_, {PortMap5, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(empty_slot, element(1, PortMap5)),
	?assertEqual(2, element(2, PortMap5)),
	?assertEqual(3, element(3, PortMap5)),
	?assertEqual(9, element(9, PortMap5)),
	?assertEqual(10, element(10, PortMap5)),
	maybe_rotate_peer_ports(Peer11),
	[{_, {PortMap6, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(element(2, PortMap5), element(1, PortMap6)),
	?assertEqual(3, element(2, PortMap6)),
	?assertEqual(4, element(3, PortMap6)),
	?assertEqual(5, element(4, PortMap6)),
	?assertEqual(11, element(10, PortMap6)),
	maybe_rotate_peer_ports(Peer11),
	[{_, {PortMap7, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(element(2, PortMap5), element(1, PortMap7)),
	?assertEqual(3, element(2, PortMap7)),
	?assertEqual(4, element(3, PortMap7)),
	?assertEqual(5, element(4, PortMap7)),
	?assertEqual(11, element(10, PortMap7)),
	remove_peer(Peer4),
	[{_, {PortMap8, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(empty_slot, element(3, PortMap8)),
	?assertEqual(3, element(2, PortMap8)),
	?assertEqual(5, element(4, PortMap8)),
	remove_peer(Peer2),
	remove_peer(Peer3),
	remove_peer(Peer5),
	remove_peer(Peer6),
	remove_peer(Peer7),
	remove_peer(Peer8),
	remove_peer(Peer9),
	remove_peer(Peer10),
	[{_, {PortMap9, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(11, element(10, PortMap9)),
	remove_peer(Peer11),
	?assertEqual([], ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}})).

update_rating_test() ->
	ets:delete_all_objects(?MODULE),
	Peer1 = {1, 2, 3, 4, 1984},
	Peer2 = {5, 6, 7, 8, 1984},

	?assertEqual(#performance{}, get_or_init_performance(Peer1)),
	?assertEqual(0, get_total_rating(lifetime)),
	?assertEqual(0, get_total_rating(current)),

	update_rating(Peer1, true),
	?assertEqual(#performance{}, get_or_init_performance(Peer1)),
	?assertEqual(0, get_total_rating(lifetime)),
	?assertEqual(0, get_total_rating(current)),


	update_rating(Peer1, false),
	assert_performance(#performance{ average_success = 0.965 }, get_or_init_performance(Peer1)),
	?assertEqual(0, get_total_rating(lifetime)),
	?assertEqual(0, get_total_rating(current)),


	%% Failed transfer should impact bytes or latency
	update_rating(Peer1, 1000, 100, 1, false),
	assert_performance(#performance{
			average_success = 0.9312 },
		get_or_init_performance(Peer1)),
	?assertEqual(0, get_total_rating(lifetime)),
	?assertEqual(0, get_total_rating(current)),


	%% Test successful transfer
	update_rating(Peer1, 1000, 100, 1, true),
	assert_performance(#performance{
			total_bytes = 100,
			total_throughput = 0.1,
			total_transfers = 1,
			average_latency = 50,
			average_throughput = 0.005,
			average_success = 0.9336,
			lifetime_rating = 0.0934,
			current_rating = 0.0047 },
		get_or_init_performance(Peer1)),
	?assertEqual(0.0934, round(get_total_rating(lifetime), 4)),
	?assertEqual(0.0047, round(get_total_rating(current), 4)),

	%% Test concurrency
	update_rating(Peer1, 1000, 50, 10, true),
	assert_performance(#performance{
			total_bytes = 150,
			total_throughput = 0.15,
			total_transfers = 2,
			average_latency = 97.5,
			average_throughput = 0.0298,
			average_success = 0.936,
			lifetime_rating = 0.0702,
			current_rating = 0.0278 },
		get_or_init_performance(Peer1)),
	?assertEqual(0.0702, round(get_total_rating(lifetime), 4)),
	?assertEqual(0.0278, round(get_total_rating(current), 4)),

	%% With 2 peers total rating should be the sum of both
	update_rating(Peer2, 1000, 100, 1, true),
	assert_performance(#performance{
			total_bytes = 100,
			total_throughput = 0.1,
			total_transfers = 1,
			average_latency = 50,
			average_throughput = 0.005,
			average_success = 1,
			lifetime_rating = 0.1,
			current_rating = 0.005 },
		get_or_init_performance(Peer2)),
	?assertEqual(0.1702, round(get_total_rating(lifetime), 4)),
	?assertEqual(0.0328, round(get_total_rating(current), 4)).

block_rejected_test_() ->
	[
		{timeout, 30, fun test_block_rejected/0}
	].

test_block_rejected() ->
	ar_blacklist_middleware:cleanup_ban(whereis(ar_blacklist_middleware)),
	Peer = {127, 0, 0, 1, ar_test_node:get_unused_port()},
	ar_peers:add_peer(Peer, -1),

	ar_events:send(block, {rejected, invalid_signature, <<>>, Peer}),
	timer:sleep(5000),

	?assertEqual(#{Peer => #performance{}}, ar_peers:get_peer_performances([Peer])),
	?assertEqual(not_banned, ar_blacklist_middleware:is_peer_banned(Peer)),

	ar_events:send(block, {rejected, failed_to_fetch_first_chunk, <<>>, Peer}),
	timer:sleep(5000),

	?assertEqual(
		#{Peer => #performance{ average_success = 0.965 }},
		ar_peers:get_peer_performances([Peer])),
	?assertEqual(not_banned, ar_blacklist_middleware:is_peer_banned(Peer)),

	ar_events:send(block, {rejected, invalid_previous_solution_hash, <<>>, Peer}),
	timer:sleep(5000),

	?assertEqual(#{Peer => #performance{}}, ar_peers:get_peer_performances([Peer])),
	?assertEqual(banned, ar_blacklist_middleware:is_peer_banned(Peer)).

rate_data_test() ->
	ets:delete_all_objects(?MODULE),
	Peer1 = {1, 2, 3, 4, 1984},

	?assertEqual(#performance{}, get_or_init_performance(Peer1)),
	?assertEqual(0, get_total_rating(lifetime)),
	?assertEqual(0, get_total_rating(current)),

	ar_peers:rate_fetched_data(Peer1, chunk, {error, timeout}, 1000000, 100, 10),
	timer:sleep(500),
	assert_performance(#performance{ average_success = 0.965 }, get_or_init_performance(Peer1)),
	?assertEqual(0, get_total_rating(lifetime)),
	?assertEqual(0, get_total_rating(current)),

	ar_peers:rate_fetched_data(Peer1, block, 1000000, 100),
	timer:sleep(500),
	assert_performance(#performance{
			total_bytes = 100,
			total_throughput = 0.1,
			total_transfers = 1,
			average_latency = 50,
			average_throughput = 0.005,
			average_success = 0.9662,
			lifetime_rating = 0.0966,
			current_rating = 0.0048 },
		get_or_init_performance(Peer1)),
	?assertEqual(0.0966, round(get_total_rating(lifetime), 4)),
	?assertEqual(0.0048, round(get_total_rating(current), 4)),

	ar_peers:rate_fetched_data(Peer1, tx, ok, 1000000, 100, 2),
	timer:sleep(500),
	assert_performance(#performance{
			total_bytes = 200,
			total_throughput = 0.2,
			total_transfers = 2,
			average_latency = 97.5,
			average_throughput = 0.0148,
			average_success = 0.9674,
			lifetime_rating = 0.0967,
			current_rating = 0.0143 },
		get_or_init_performance(Peer1)),
	?assertEqual(0.0967, round(get_total_rating(lifetime), 4)),
	?assertEqual(0.0143, round(get_total_rating(current), 4)),

	ar_peers:rate_gossiped_data(Peer1, block, 1000000, 100),
	timer:sleep(500),
	assert_performance(#performance{
			total_bytes = 300,
			total_throughput = 0.3,
			total_transfers = 3,
			average_latency = 142.625,
			average_throughput = 0.019,
			average_success = 0.9685,
			lifetime_rating = 0.0969,
			current_rating = 0.0184 },
		get_or_init_performance(Peer1)),
	?assertEqual(0.0969, round(get_total_rating(lifetime), 4)),
	?assertEqual(0.0184, round(get_total_rating(current), 4)).

assert_performance(Expected, Actual) ->
	?assertEqual(Expected#performance.total_bytes, Actual#performance.total_bytes),
	?assertEqual(
		round(Expected#performance.total_throughput, 4),
		round(Actual#performance.total_throughput, 4)),
	?assertEqual(Expected#performance.total_transfers, Actual#performance.total_transfers),
	?assertEqual(
		round(Expected#performance.average_latency, 4),
		round(Actual#performance.average_latency, 4)),
	?assertEqual(
		round(Expected#performance.average_throughput, 4),
		round(Actual#performance.average_throughput, 4)),
	?assertEqual(
		round(Expected#performance.average_success, 4),
		round(Actual#performance.average_success, 4)),
	?assertEqual(
		round(Expected#performance.lifetime_rating, 4),
		round(Actual#performance.lifetime_rating, 4)),
	?assertEqual(
		round(Expected#performance.current_rating, 4),
		round(Actual#performance.current_rating, 4)).

round(Float, N) ->
    Multiplier = math:pow(10, N),
    round(Float * Multiplier) / Multiplier.
