%%% @doc Tracks the availability and performance of the network peers.
-module(ar_peers).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_peers.hrl").

-include_lib("eunit/include/eunit.hrl").

-export([start_link/0, get_peers/0, get_peer_performances/1, get_trusted_peers/0, is_public_peer/1,
	get_peer_release/1, stats/0, discover_peers/0, add_peer/2, rank_peers/1,
	resolve_and_cache_peer/2, rate_response/4, rate_fetched_data/4, rate_fetched_data/6,
	rate_gossiped_data/3
]).

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
-define(MINIMUM_SUCCESS, 0.5).
-define(THROUGHPUT_ALPHA, 0.1).
-define(SUCCESS_ALPHA, 0.01).

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
	invalid_nonce_limiter
]).
-define(BLOCK_REJECTION_IGNORE, [
	invalid_signature,
	invalid_hash,
	invalid_timestamp,
	invalid_resigned_solution_hash,
	invalid_nonce_limiter_cache_mismatch
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

get_peers() ->
	case catch ets:lookup(?MODULE, peers) of
		{'EXIT', _} ->
			[];
		[] ->
			[];
		[{_, Peers}] ->
			Peers
	end.

get_peer_performances(Peers) ->
	[get_or_init_performance(Peer) || Peer <- Peers].

-if(?NETWORK_NAME == "arweave.N.1").
get_trusted_peers() ->
	{ok, Config} = application:get_env(arweave, config),
	case Config#config.peers of
		[] ->
			ArweavePeers = ["sfo-1.na-west-1.arweave.net", "ams-1.eu-central-1.arweave.net",
					"fra-1.eu-central-2.arweave.net", "blr-1.ap-central-1.arweave.net",
					"sgp-1.ap-central-2.arweave.net"
			],
			resolve_peers(ArweavePeers);
		Peers ->
			Peers
	end.
-else.
get_trusted_peers() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.peers.
-endif.

resolve_peers([]) ->
	[];
resolve_peers([RawPeer | Peers]) ->
	case ar_util:safe_parse_peer(RawPeer) of
		{ok, Peer} ->
			[Peer | resolve_peers(Peers)];
		{error, invalid} ->
			?LOG_WARNING([{event, failed_to_resolve_trusted_peer}, {peer, RawPeer}]),
			resolve_peers(Peers)
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

%% @doc Return the release nubmer reported by the peer.
%% Return -1 if the release is not known.
get_peer_release(Peer) ->
	case catch ets:lookup(?MODULE, {peer, Peer}) of
		[{_, #performance{ release = Release }}] ->
			Release;
		_ ->
			-1
	end.

rate_response({_Host, _Port}, _, _, _) ->
	%% Only track requests for IP-based peers as the rest of the stack assumes an IP-based peer.
	ok;
rate_response(Peer, PathLabel, get, Response) ->
	gen_server:cast(
		?MODULE, {rate_response, Peer, PathLabel, get, ar_metrics:get_status_class(Response)}
	);
rate_response(_Peer, _PathLabel, _Method, _Response) ->
	ok.

rate_fetched_data(Peer, DataType, LatencyMicroseconds, DataSize) ->
	rate_fetched_data(Peer, DataType, ok, LatencyMicroseconds, DataSize, 1).
rate_fetched_data(Peer, DataType, ok, LatencyMicroseconds, DataSize, Concurrency) ->
	gen_server:cast(?MODULE,
		{fetched_data, Peer, DataType, LatencyMicroseconds, DataSize, Concurrency});
rate_fetched_data(Peer, DataType, _, _LatencyMicroseconds, _DataSize, _Concurrency) ->
	gen_server:cast(?MODULE, {invalid_fetched_data, Peer, DataType}).

rate_gossiped_data(Peer, DataType, DataSize) ->
	gen_server:cast(?MODULE, {gossiped_data, Peer, DataType, DataSize}).

issue_warning(Peer, _Type, _Reason) ->
	gen_server:cast(?MODULE, {warning, Peer}).

add_peer(Peer, Release) ->
	gen_server:cast(?MODULE, {add_peer, Peer, Release}).

%% @doc Print statistics about the current peers.
stats() ->
	Connected = get_peers(),
	io:format("Connected peers, in preference order:~n"),
	stats(Connected),
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
stats(Peers) ->
	lists:foreach(
		fun(Peer) -> format_stats(Peer, get_or_init_performance(Peer)) end,
		Peers
	).

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

%% @doc Resolve the domain name of the given peer (if the given peer is an IP address)
%% and cache it. Return {ok, Peer} | {error, Reason}.
resolve_and_cache_peer(RawPeer, Type) ->
	case ar_util:safe_parse_peer(RawPeer) of
		{ok, Peer} ->
			case ets:lookup(?MODULE, {raw_peer, RawPeer}) of
				[] ->
					ets:insert(?MODULE, {{raw_peer, RawPeer}, Peer}),
					ets:insert(?MODULE, {{Type, Peer}, RawPeer});
				[{_, Peer}] ->
					ok;
				[{_, PreviousPeer}] ->
					%% This peer is configured with a domain name rather than IP address,
					%% and the IP underlying the domain name has changed.
					ets:delete(?MODULE, {Type, PreviousPeer}),
					ets:insert(?MODULE, {{raw_peer, RawPeer}, Peer}),
					ets:insert(?MODULE, {{Type, Peer}, RawPeer})
			end,
			{ok, Peer};
		Error ->
			Error
	end.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
	process_flag(trap_exit, true),
	[ok, ok] = ar_events:subscribe(block),
	load_peers(),
	gen_server:cast(?MODULE, rank_peers),
	gen_server:cast(?MODULE, ping_peers),
	timer:apply_interval(?GET_MORE_PEERS_FREQUENCY_MS, ?MODULE, discover_peers, []),
	{ok, #state{}}.

handle_call(Request, _From, State) ->
	?LOG_WARNING("event: unhandled_call, request: ~p", [Request]),
	{reply, ok, State}.

handle_cast({add_peer, Peer, Release}, State) ->
	maybe_add_peer(Peer, Release),
	{noreply, State};

handle_cast(rank_peers, State) ->
	Total = get_total_rating(),
	Peers =
		ets:foldl(
			fun
				({{peer, Peer}, Performance}, Acc) ->
					%% Bigger score increases the chances to end up on the top
					%% of the peer list, but at the same time the ranking is
					%% probabilistic to always give everyone a chance to improve
					%% in the competition (i.e., reduce the advantage gained by
					%% being the first to earn a reputation).
					Score =
						rand:uniform() * Performance#performance.rating /
							(Total + 0.0001),
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
	stats(),
	{noreply, State};

handle_cast(ping_peers, State) ->
	[{peers, Peers}] = ets:lookup(?MODULE, peers),
	ping_peers(lists:sublist(Peers, 100)),
	{noreply, State};

handle_cast({rate_response, Peer, PathLabel, get, Status}, State) ->
	case Status of
		"success" ->
			update_rating(Peer, true);
		"redirection" ->
			%% don't update rating
			ok;
		"client-error" ->
			%% don't update rating
			ok;
		_ ->
			update_rating(Peer, false)
	end,
	?LOG_DEBUG([
		{event, update_rating},
		{update_type, response},
		{path, PathLabel},
		{status, Status},
		{peer, ar_util:format_peer(Peer)}
	]),
	{noreply, State};

handle_cast({fetched_data, Peer, DataType, LatencyMicroseconds, DataSize, Concurrency}, State) ->
	?LOG_DEBUG([
		{event, update_rating},
		{update_type, fetched_data},
		{data_type, DataType},
		{peer, ar_util:format_peer(Peer)},
		{latency, LatencyMicroseconds  / 1000},
		{data_size, DataSize},
		{concurrency, Concurrency}
	]),
	update_rating(Peer, LatencyMicroseconds, DataSize, Concurrency, true),
	{noreply, State};


handle_cast({invalid_fetched_data, Peer, DataType}, State) ->
	?LOG_DEBUG([
		{event, update_rating},
		{update_type, invalid_fetched_data},
		{data_type, DataType},
		{peer, ar_util:format_peer(Peer)}
	]),
	update_rating(Peer, false),
	{noreply, State};

handle_cast({gossiped_data, Peer, DataType, DataSize}, State) ->
	case check_peer(Peer) of
		ok ->
			%% Since gossiped data is pushed to us we don't know the latency, but we do want
			%% to incentivize peers to gossip data quickly and frequently, so we will assign
			%% a latency that is guaranteed to improve the peer's rating:
			%% 1. Calculate the latency that would be required to transfer DataSize bytes at the
			%%    peer's current average rate.
			%% 2. Scale that latency by ?GOSSIP_ADVANTAGE and rate using the scaled latency
			Performance = get_or_init_performance(Peer),
			#performance{
				average_bytes = AverageBytes,
				average_latency = AverageLatency
			} = Performance,
			AverageThroughput = AverageBytes / AverageLatency,
			GossipLatency = (DataSize / AverageThroughput) * ?GOSSIP_ADVANTAGE,
			LatencyMicroseconds = GossipLatency * 1000,
			?LOG_DEBUG([
				{event, update_rating},
				{update_type, gossiped_data},
				{data_type, DataType},
				{peer, ar_util:format_peer(Peer)},
				{latency, LatencyMicroseconds / 1000},
				{data_size, DataSize}
			]),
			update_rating(Peer, LatencyMicroseconds, DataSize, 1, true);
		_ ->
			ok
	end,

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
	?LOG_WARNING("event: unhandled_cast, cast: ~p", [Cast]),
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

handle_info({event, block, {new, B,
		#{ source := {peer, Peer}, query_block_time := QueryBlockTime }}}, State) ->
	DataSize = byte_size(term_to_binary(B)),
	case QueryBlockTime of
		undefined -> ar_peers:rate_gossiped_data(Peer, block, DataSize);
		_ -> ar_peers:rate_fetched_data(Peer, block, QueryBlockTime, DataSize)
	end,
	{noreply, State};

handle_info({event, block, _}, State) ->
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

get_or_init_performance(Peer) ->
	case ets:lookup(?MODULE, {peer, Peer}) of
		[] ->
			#performance{};
		[{_, Performance}] ->
			Performance
	end.

set_performance(Peer, Performance) ->
	ets:insert(?MODULE, [{{peer, Peer}, Performance}]).

get_total_rating() ->
	case ets:lookup(?MODULE, rating_total) of
		[] ->
			0;
		[{_, Total}] ->
			Total
	end.

set_total_rating(Total) ->
	ets:insert(?MODULE, {rating_total, Total}).

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
						{<<"release">>, Release} when is_integer(Release) ->
							maybe_add_peer(Peer, Release);
						_ ->
							ok
					end;
				_ ->
					ok
			end
	end,
	discover_peers(Peers).

format_stats(Peer, Perf) ->
	KB = Perf#performance.average_bytes / 1024,
	io:format(
		"\t~s ~.2f kB/s (~.2f kB, ~B latency, ~.2f success, ~p transfers)~n",
		[string:pad(ar_util:format_peer(Peer), 21, trailing, $\s),
			float(Perf#performance.rating), KB, trunc(Perf#performance.average_latency),
			Perf#performance.average_success, Perf#performance.transfers]).

load_peers() ->
	case ar_storage:read_term(peers) of
		not_found ->
			ok;
		{ok, {_TotalRating, Records}} ->
			?LOG_INFO([{event, polling_saved_peers}]),
			ar:console("Polling saved peers...~n"),
			load_peers(Records),
			TotalRating =
				ets:foldl(
					fun	({{peer, _Peer}, Performance}, Acc) ->
							Acc + Performance#performance.rating;
						(_, Acc) ->
							Acc
					end,
					0,
					?MODULE
				),
			ets:insert(?MODULE, {rating_total, TotalRating}),
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
	case ar_http_iface_client:get_info(Peer, name) of
		info_unavailable ->
			?LOG_DEBUG([{event, peer_unavailable}, {peer, ar_util:format_peer(Peer)}]),
			ok;
		<<?NETWORK_NAME>> ->
			may_be_rotate_peer_ports(Peer),
			case Performance of
				{performance, TotalBytes, TotalLatency, Transfers, _Failures, Rating} ->
					%% For compatibility with a few nodes already storing the records
					%% without the release field.
					set_performance(Peer, #performance{
						total_bytes = TotalBytes,
						total_latency = TotalLatency,
						transfers = Transfers,
						rating = Rating
					});
				{performance, TotalBytes, TotalLatency, Transfers, _Failures, Rating, Release} ->
					%% For compatibility with nodes storing records from before the introduction of
					%% the version field
					set_performance(Peer, #performance{
						release = Release,
						total_bytes = TotalBytes,
						total_latency = TotalLatency,
						transfers = Transfers,
						rating = Rating
					});
				{performance, 3,
						_Release, _AverageBytes, _TotalBytes, _AverageLatency, _TotalLatency,
						_Transfers, _AverageSuccess, _Rating} ->
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

may_be_rotate_peer_ports(Peer) ->
	{IP, Port} = get_ip_port(Peer),
	case ets:lookup(?MODULE, {peer_ip, IP}) of
		[] ->
			ets:insert(?MODULE, {{peer_ip, IP},
					{erlang:setelement(1, ?DEFAULT_PEER_PORT_MAP, Port), 1}}
			);
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

%% @doc Return a ranked list of peers.
rank_peers(ScoredPeers) ->
	SortedReversed = lists:reverse(
		lists:sort(fun({_, S1}, {_, S2}) -> S1 >= S2 end, ScoredPeers)
	),
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
update_rating(Peer, LatencyMicroseconds, DataSize, Concurrency, IsSuccess) ->
	Performance = get_or_init_performance(Peer),
	Total = get_total_rating(),
	LatencyMilliseconds = LatencyMicroseconds / 1000,
	#performance{
		average_bytes = AverageBytes,
		total_bytes = TotalBytes,
		average_latency = AverageLatency,
		total_latency = TotalLatency,
		average_success = AverageSuccess,
		rating = Rating,
		transfers = Transfers
	} = Performance,
	TotalBytes2 = case DataSize of
		undefined -> TotalBytes;
		_ -> TotalBytes + DataSize
	end,
	%% AverageBytes is the average number of bytes transferred during the AverageLatency time
	%% period. In order to approximate the impact of multiple concurrent requests we multiply
	%% DataSize by the Concurrency value. We do this *only* when updating the AverageBytes
	%% value so that it doesn't distort the TotalBytes.
	AverageBytes2 = case DataSize of
		undefined -> AverageBytes;
		_ -> calculate_ema(AverageBytes, (DataSize * Concurrency), ?THROUGHPUT_ALPHA)
	end,
	TotalLatency2 = case LatencyMilliseconds of
		undefined -> TotalLatency;
		_ -> TotalLatency + LatencyMilliseconds
	end,
	AverageLatency2 = case LatencyMilliseconds of
		undefined -> AverageLatency;
		_ -> calculate_ema(AverageLatency, LatencyMilliseconds, ?THROUGHPUT_ALPHA)
	end,
	Transfers2 = case DataSize of
		undefined -> Transfers;
		_ -> Transfers + 1
	end,
	AverageSuccess2 = calculate_ema(AverageSuccess, ar_util:bool_to_int(IsSuccess), ?SUCCESS_ALPHA),
	%% Rating is an estimate of the peer's effective throughput in bytes per second.
	Rating2 = (AverageBytes2 / AverageLatency2) * AverageSuccess2,
	Performance2 = Performance#performance{
		average_bytes = AverageBytes2,
		total_bytes = TotalBytes2,
		average_latency = AverageLatency2,
		total_latency = TotalLatency2,
		average_success = AverageSuccess2,
		rating = Rating2,
		transfers = Transfers2
	},
	Total2 = Total - Rating + Rating2,
	may_be_rotate_peer_ports(Peer),
	set_performance(Peer, Performance2),
	set_total_rating(Total2),
	Performance2.

calculate_ema(OldEMA, Value, Alpha) ->
	Alpha * Value + (1 - Alpha) * OldEMA.

maybe_add_peer(Peer, Release) ->
	may_be_rotate_peer_ports(Peer),
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
	Total = get_total_rating(),
	set_total_rating(Total - Performance#performance.rating),
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
	Records =
		ets:foldl(
			fun
				({{peer, Peer}, Performance}, Acc) ->
					[{Peer, Performance} | Acc];
				(_, Acc) ->
					Acc
			end,
			[],
			?MODULE
		),
	case Records of
		[] ->
			ok;
		_ ->     
			ar_storage:write_term(peers, Records)
	end.

%%%===================================================================
%%% Tests.
%%%===================================================================

rotate_peer_ports_test() ->
	Peer = {2, 2, 2, 2, 1},
	may_be_rotate_peer_ports(Peer),
	[{_, {PortMap, 1}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(1, element(1, PortMap)),
	remove_peer(Peer),
	?assertEqual([], ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}})),
	may_be_rotate_peer_ports(Peer),
	Peer2 = {2, 2, 2, 2, 2},
	may_be_rotate_peer_ports(Peer2),
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
	may_be_rotate_peer_ports(Peer3),
	may_be_rotate_peer_ports(Peer4),
	may_be_rotate_peer_ports(Peer5),
	may_be_rotate_peer_ports(Peer6),
	may_be_rotate_peer_ports(Peer7),
	may_be_rotate_peer_ports(Peer8),
	may_be_rotate_peer_ports(Peer9),
	may_be_rotate_peer_ports(Peer10),
	[{_, {PortMap4, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(empty_slot, element(1, PortMap4)),
	?assertEqual(2, element(2, PortMap4)),
	?assertEqual(10, element(10, PortMap4)),
	may_be_rotate_peer_ports(Peer8),
	may_be_rotate_peer_ports(Peer9),
	may_be_rotate_peer_ports(Peer10),
	[{_, {PortMap5, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(empty_slot, element(1, PortMap5)),
	?assertEqual(2, element(2, PortMap5)),
	?assertEqual(3, element(3, PortMap5)),
	?assertEqual(9, element(9, PortMap5)),
	?assertEqual(10, element(10, PortMap5)),
	may_be_rotate_peer_ports(Peer11),
	[{_, {PortMap6, 10}}] = ets:lookup(?MODULE, {peer_ip, {2, 2, 2, 2}}),
	?assertEqual(element(2, PortMap5), element(1, PortMap6)),
	?assertEqual(3, element(2, PortMap6)),
	?assertEqual(4, element(3, PortMap6)),
	?assertEqual(5, element(4, PortMap6)),
	?assertEqual(11, element(10, PortMap6)),
	may_be_rotate_peer_ports(Peer11),
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
