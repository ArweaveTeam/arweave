%%% @doc Tracks the availability and performance of the network peers.
-module(ar_peers).
-behaviour(gen_server).
-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_peers.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-export([
         add_peer/2,
         connected_peer/1,
         disconnected_peer/1,
         discover_peers/0,
         filter_peers/2,
         get_connection_timestamp_peer/1,
         get_inbound_peer_counts/0,
         get_inbound_peers/0,
         get_peer_performances/1,
         get_peer_release/1,
         get_peers/1,
         get_tag/2,
         get_trusted_peers/0,
         is_connected_peer/1,
         is_public_peer/1,
         issue_warning/3,
    pick_peers/2,
         rate_fetched_data/4,
    rate_fetched_data/5,
         rate_gossiped_data/4,
         resolve_and_cache_peer/2,
         resolve_and_cache_peer/3,
         set_tag/3,
         start_link/0,
         stats/1
        ]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(AR_TEST).
-export([expire_inbound_peers/0, get_or_init_performance/1, get_peer_rating/2,
    load_peer/1, maybe_rotate_peer_ports/1, observe_inbound_peer/3, remove_peer/2,
    set_ranked_peers/2, update_rating/2, update_rating/4]).
-endif.

%% The frequency in seconds of re-resolving DNS of peers configured by domain names.
-define(STORE_RESOLVED_DOMAIN_S, 60).

%% The frequency in milliseconds of ranking the known peers.
-ifdef(AR_TEST).
-define(RANK_PEERS_FREQUENCY_MS, 2 * 1000).
-else.
-define(RANK_PEERS_FREQUENCY_MS, 2 * 60 * 1000).
-endif.

%% The frequency in milliseconds of asking some peers for their peers.
-ifdef(AR_TEST).
-define(GET_MORE_PEERS_FREQUENCY_MS, 5000).
-else.
-define(GET_MORE_PEERS_FREQUENCY_MS, 240 * 1000).
-endif.

%% The amount of time probing of discovered peers needs to return.
-define(PEER_PROBE_TIMEOUT, 5000).

%% Peers to never add to the peer list.
-define(PEER_PERMANENT_BLACKLIST, []).

-define(INBOUND_PEER_WINDOW_S, 3600).
-define(MAX_INBOUND_PEERS, 10000).
-define(MAX_INBOUND_RELEASE_LABELS, 32).

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
                              invalid_recall_byte,
                              invalid_recall_byte2,
                              invalid_poa,
                              invalid_poa2,
                              invalid_nonce_limiter,
                              invalid_nonce_limiter_cache_mismatch,
                              invalid_packing_difficulty
                             ]).

-define(BLOCK_REJECTION_IGNORE, [
                                 invalid_signature,
                                 invalid_proof_size,
                                 invalid_first_chunk,
                                 invalid_second_chunk,
                                 invalid_poa2_recall_byte2_undefined,
                                 invalid_hash,
                                 invalid_payload,
                                 invalid_timestamp,
                                 invalid_resigned_solution_hash,
                                 invalid_nonce_limiter_global_step_number,
                                 invalid_first_unpacked_chunk,
                                 invalid_second_unpacked_chunk,
                                 invalid_first_unpacked_chunk_hash,
                                 invalid_second_unpacked_chunk_hash
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

%%--------------------------------------------------------------------
%% @doc Return the list of peers in the given ranking order.
%%      Rating is an estimate of the peer's effective throughput in
%%      bytes per millisecond.
%%
%%      `lifetime' considers all data ever received from this peer and
%%      is most useful when we care more about identifying "good
%%      samaritans" rather than maximizing throughput (e.g. when
%%      polling for new blocks are determing which peer's blocks to
%%      validated first).
%%
%%      `current' weights recently received data higher than old data
%%      and is most useful when we care more about maximizing throughput
%%      (e.g. when syncing chunks).
%%
%% @end
%%--------------------------------------------------------------------
get_peers(Ranking) ->
    case catch ets:lookup(?MODULE, {peers, Ranking}) of
        {'EXIT', _} ->
            [];
        [] ->
            [];
        [{{peers, lifetime}, Peers}] ->
            Peers;
        [{{peers, current}, Peers}] ->
            filter_peers(Peers, {timestamp, ?CURRENT_PEERS_LIST_FILTER});
        [{_, Peers}] ->
            Peers
    end.

%% @doc Uniformly sample up to N peers from the given peer list.
pick_peers(Peers, N) ->
    pick_peers(Peers, length(Peers), N).

pick_peers(Peers, PeerLen, N) when N >= PeerLen ->
    Peers;
pick_peers([], _PeerLen, _N) ->
    [];
pick_peers(_Peers, _PeerLen, N) when N =< 0 ->
    [];
pick_peers(Peers, _PeerLen, N) ->
    %% Sample N candidates uniformly so callers keep exploring the whole peer list.
    arweave_util:pick_random(Peers, N).

filter_peers(Peers, {timestamp, Seconds})
  when is_integer(Seconds) ->
    Timefilter = erlang:system_time(seconds) - Seconds,
    Tag = {connection, last},
    Pattern = {{ar_tags, ?MODULE, '$1', Tag}, '$3'},
    Guard = [{'>=', '$3', Timefilter}],
    Select = ['$1'],
    TaggedPeers = ets:select(?MODULE, [{Pattern, Guard, Select}]),
    [ P || T <- TaggedPeers, P <- Peers, T =:= P ].

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
    case arweave_util:safe_parse_peer(RawPeer) of
        {ok, Peer} ->
            Peer ++ resolve_peers(Peers);
        {error, invalid} ->
            ?LOG_WARNING([{event, failed_to_resolve_trusted_peer},
                          {peer, RawPeer}]),
            resolve_peers(Peers)
    end.

get_trusted_peers() ->
    case arweave_config:get([peers, trusted]) of
        [] ->
            ArweavePeers = [
                            "asia.peers.arweave.xyz",
                            "europe.peers.arweave.xyz",
                            "india.peers.arweave.xyz",
                            "north-america.peers.arweave.xyz",
                            "oceania.peers.arweave.xyz"
                           ],
            resolve_peers(ArweavePeers);
        Peers ->
            Peers
    end.
-else.
get_trusted_peers() ->
    arweave_config:get([peers, trusted]).
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
    rate_fetched_data(Peer, DataType, ok, LatencyMicroseconds, DataSize).
rate_fetched_data(Peer, DataType, ok, LatencyMicroseconds, DataSize) ->
    try
        gen_server:cast(?MODULE,
            {valid_data, Peer, DataType, LatencyMicroseconds / 1000, DataSize})
    catch
        _:_ -> ok
    end;
rate_fetched_data(Peer, DataType, _, _LatencyMicroseconds, _DataSize) ->
    try
        gen_server:cast(?MODULE, {invalid_data, Peer, DataType})
    catch
        _:_ -> ok
    end.

rate_gossiped_data(Peer, DataType, LatencyMicroseconds, DataSize) ->
    case check_peer(Peer) of
        ok ->
            gen_server:cast(?MODULE,
                {valid_data, Peer, DataType, LatencyMicroseconds / 1000, DataSize});
        _ ->
            ok
    end.

issue_warning(Peer, _Type, _Reason) ->
    gen_server:cast(?MODULE, {warning, Peer}).

%% @doc Register a peer observed in an inbound P2P request.
add_peer(Peer, Release) ->
    At = erlang:monotonic_time(second),
    gen_server:cast(?MODULE, {add_peer, Peer, Release, At}).

%% @doc Return the last hour of inbound endpoints for local census queries.
get_inbound_peers() ->
    Now = erlang:monotonic_time(second),
    #{observed_at => erlang:system_time(second),
      window_seconds => ?INBOUND_PEER_WINDOW_S,
      peers => [#{peer => Peer, release => Release,
                  seconds_since_last_inbound => Now - At}
          || {Peer, Release, At} <- recent_inbound_peers(Now)]}.

%% @doc Count inbound endpoints by release within the last hour.
get_inbound_peer_counts() ->
    Rows = recent_inbound_peers(erlang:monotonic_time(second)),
    Counts = lists:foldl(fun({_Peer, Release, _At}, Acc) ->
        maps:update_with(Release, fun(N) -> N + 1 end, 1, Acc)
    end, #{}, Rows),
    UnknownCount = maps:get(unknown, Counts, 0),
    ReleaseCounts = maps:to_list(maps:remove(unknown, Counts)),
    SortKeys = [{-Count, Release} || {Release, Count} <- ReleaseCounts],
    Sorted = lists:sort(SortKeys),
    {Named, Rest} = lists:split(
        min(?MAX_INBOUND_RELEASE_LABELS, length(Sorted)), Sorted),
    NamedCounts = [{Release, -NegativeCount}
        || {NegativeCount, Release} <- Named],
    OtherCount = lists:sum([-NegativeCount || {NegativeCount, _} <- Rest]),
    NamedCounts ++ [{unknown, UnknownCount}, {other, OtherCount}].

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
    case get_peers(current) of
        [] ->
            ok;
        Peers ->
            Peer = arweave_util:pick_random(Peers),
            discover_peers(get_peer_peers(Peer))
    end.

%%--------------------------------------------------------------------
%% @doc
%% @see resolve_and_cache_peer/3
%% @end
%%--------------------------------------------------------------------
-spec resolve_and_cache_peer(RawPeer, Type) -> Return when
      RawPeer :: string(),
      Type :: term(),
      Return :: {ok, {A,A,A,A,Port}} | {error, term()},
      A :: pos_integer(),
      Port :: pos_integer().

resolve_and_cache_peer(RawPeer, Type) ->
    resolve_and_cache_peer(RawPeer, Type, #{}).

%%--------------------------------------------------------------------
%% @doc Resolve the  domain name of the given peer  (if the given peer
%% is  an  IP  address)  and  cache it.  Invalidate  the  cache  after
%% `?STORE_RESOLVED_DOMAIN_S seconds.'  Return {ok, Peer} | {error,
%% Reason}.
%% @end
%%--------------------------------------------------------------------
-spec resolve_and_cache_peer(RawPeer, Type, Opts) -> Return when
      RawPeer :: string(),
      Type :: term(),
      Opts :: map(),
      Return :: {ok, {A,A,A,A,Port}} | {error, term()},
      A :: pos_integer(),
      Port :: pos_integer().

resolve_and_cache_peer(RawPeer, Type, Opts) ->
    Now = maps:get(now, Opts, erlang:system_time(second)),
    CacheTTL = maps:get(cache_ttl, Opts,
                        ?STORE_RESOLVED_DOMAIN_S),
    State = #{
              raw_peer => RawPeer,
              type => Type,
              now => Now,
              opts => Opts,
              cache_ttl => CacheTTL
             },

                                                % first check if the peer as string (so using a name
                                                % record) is present in ets table. If the peer is not present
                                                % in cache, then it will be updated. Else, the timestamp needs
                                                % to be checked.
    case ets:lookup(?MODULE, {raw_peer, RawPeer}) of
        [] ->
            resolve_and_cache_peer_empty(State);
        [{_, {CachedPeer, CachedTimestamp}}] ->
            NewState = State#{
                              cache_timestamp => CachedTimestamp,
                              cache_peer => CachedPeer
                             },
            resolve_and_cache_peer2(CachedPeer, NewState)
    end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc check if peer cache did not expired.
%% @end
%%--------------------------------------------------------------------
resolve_and_cache_peer2(CachedPeer, State) ->
    Now = maps:get(now, State),
    CachedTimestamp = maps:get(cache_timestamp, State),
    CacheTTL = maps:get(cache_ttl, State),

                                                % if the peer present in cache expired, it needs to be
                                                % refreshed, else it can be returned.
    case CachedTimestamp + CacheTTL < Now of
        true ->
            resolve_and_cache_peer_refresh(CachedPeer, State);
        false ->
            {ok, CachedPeer}
    end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc the cache expired.
%% @end
%%--------------------------------------------------------------------
resolve_and_cache_peer_refresh(_CachedPeer, State) ->
    RawPeer = maps:get(raw_peer, State),
    Opts = maps:get(opts, State, #{}),

                                                % the cache entry expired, in this case, raw peer needs to be
                                                % reparsed and checked. It will return a list of peers.
    case arweave_util:safe_parse_peer(RawPeer, Opts) of
        {ok, NewPeers} when is_list(NewPeers) ->
            %% The cache entry has expired.
            cache_update_peers(NewPeers, State);
        {error, Error} ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc No peer cached available, we need to update it.
%% @end
%%--------------------------------------------------------------------
resolve_and_cache_peer_empty(State) ->
    RawPeer = maps:get(raw_peer, State),
    case arweave_util:safe_parse_peer(RawPeer) of
        {ok, Peers} when is_list(Peers) ->
            cache_insert_peers(Peers, State);
        {error, Error} ->
            {error, Error}
    end.

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc insert peers in the cache, when the peer is a DNS containing
%% more than one entry.
%% @end
%%--------------------------------------------------------------------
cache_insert_peers(Peers, State) ->
    cache_insert_peers(Peers, [], State).

cache_insert_peers([], Buffer, _State) ->
    [Peer] = arweave_util:pick_random(Buffer, 1),
    {ok, Peer};
cache_insert_peers([Peer|Rest], Buffer, State) ->
    RawPeer = maps:get(raw_peer, State),
    Type = maps:get(type, State),
    Now = maps:get(now, State),
    _ = ets:insert(?MODULE, {{raw_peer, RawPeer}, {Peer, Now}}),
    _ = ets:insert(?MODULE, {{Type, Peer}, RawPeer}),
    cache_insert_peers(Rest, [Peer|Buffer], State).

%%--------------------------------------------------------------------
%% @hidden
%% @private
%% @doc Update a list of peers.
%% @end
%%--------------------------------------------------------------------
cache_update_peers(Peers, State) ->
    cache_update_peers(Peers, [], State).

cache_update_peers([], Buffer, _State) ->
    [Peer] = arweave_util:pick_random(Buffer, 1),
    {ok, Peer};
cache_update_peers([Peer|Rest], Buffer, State) ->
    RawPeer = maps:get(raw_peer, State),
    CacheTimestamp = maps:get(cache_timestamp, State),
    Type = maps:get(type, State),
    Now = maps:get(now, State),
    ets:delete(?MODULE, {Type, {Peer, CacheTimestamp}}),
    ets:insert(?MODULE, {{raw_peer, RawPeer}, {Peer, Now}}),
    ets:insert(?MODULE, {{Type, Peer}, RawPeer}),
    cache_update_peers(Rest, [Peer|Buffer], State).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init([]) ->
    VerifyMode = arweave_config:get([verify, mode]),
    case VerifyMode of
        false ->
            process_flag(trap_exit, true),
            ok = ar_events:subscribe(block),
            load_peers(),
            gen_server:cast(?MODULE, rank_peers),
            gen_server:cast(?MODULE, ping_peers),
            _ = ar_timer:apply_interval(
                  ?GET_MORE_PEERS_FREQUENCY_MS,
                  ?MODULE,
                  discover_peers,
                  [],
                  #{ skip_on_shutdown => true }
                 ),
            ?LOG_INFO([{event, ar_peers_initialized}]),
            {ok, #state{}};
        _ ->
            {ok, #state{}}
    end.

handle_call(Request, _From, State) ->
    ?LOG_WARNING([{event, unhandled_call}, {module, ?MODULE}, {request, Request}]),
    {reply, ok, State}.

handle_cast({add_peer, Peer, Release, At}, State) ->
    observe_inbound_peer(Peer, Release, At),
    maybe_add_peer(Peer, Release),
    {noreply, State};

handle_cast(rank_peers, State) ->
    expire_inbound_peers(),
    LifetimePeers = score_peers(lifetime),
    CurrentPeers = score_peers(current),
    arweave_metrics:gauge_set(arweave_peer_count, length(LifetimePeers)),
    set_ranked_peers(lifetime, rank_peers(LifetimePeers)),
    set_ranked_peers(current, rank_peers(CurrentPeers)),
    arweave_util:cast_after(?RANK_PEERS_FREQUENCY_MS, self(), rank_peers),
    {noreply, State};

handle_cast(ping_peers, State) ->
    Peers = get_peers(lifetime),
    ping_peers(lists:sublist(Peers, 100)),
    {noreply, State};

handle_cast({valid_data, Peer, _DataType, LatencyMilliseconds, DataSize}, State) ->
    update_rating(Peer, LatencyMilliseconds, DataSize, true),
    {noreply, State};

handle_cast({invalid_data, Peer, _DataType}, State) ->
    update_rating(Peer, false),
    {noreply, State};

handle_cast({warning, Peer}, State) ->
    %% Discard warnings for peers that are no longer registered. Without
    %% this gate, update_rating's set_performance would re-create the
    %% removed peer's entry with a fresh #performance{} (average_success =
    %% 0), the threshold check would then trip, and remove_peer would fire
    %% again — repeating the resurrection-then-removal cycle for every
    %% stale in-flight warning. Each cycle emits another `{event, peer,
    %% {removed, _}}' event downstream.
    case is_registered(Peer) of
        false ->
            ok;
        true ->
            Performance = update_rating(Peer, false),
            case Performance#performance.average_success < ?MINIMUM_SUCCESS of
                true ->
                    remove_peer(low_success, Peer);
                false ->
                    ok
            end
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
            remove_peer(banned, Peer);
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

terminate(Reason, _State) ->
    store_peers(),
    ?LOG_INFO([{module, ?MODULE},{pid, self()},{callback, terminate},{reason, Reason}]).

%%%===================================================================
%%% Private functions.
%%%===================================================================

get_peer_peers(Peer) ->
    case ar_http_iface_client:get_peers(Peer) of
        unavailable -> [];
        Peers -> Peers
    end.

%% @doc True iff Peer has a `{peer, Peer}` registry entry — i.e. it is
%% currently tracked by ar_peers. Use this to gate operations that would
%% otherwise resurrect an already-removed peer's entry.
is_registered(Peer) ->
    ets:member(?MODULE, {peer, Peer}).

get_or_init_performance(Peer) ->
    ets:lookup_element(?MODULE, {peer, Peer}, 2, #performance{}).

set_performance(Peer, Performance) ->
    ets:insert(?MODULE, [{{peer, Peer}, Performance}]).

%% @doc The peer's rating, DERIVED from the stored accumulators: an estimate of
%% effective throughput in bytes per millisecond, discounted by success rate.
%% `lifetime' considers all data ever received from this peer; `current'
%% weights recently received data higher.
get_peer_rating(lifetime, #performance{ total_transfers = TotalTransfers,
        total_throughput = TotalThroughput, average_success = AverageSuccess })
        when TotalTransfers > 0 ->
    (TotalThroughput / TotalTransfers) * AverageSuccess;
get_peer_rating(lifetime, _Performance) ->
    0.0;
get_peer_rating(current, #performance{ average_throughput = AverageThroughput,
        average_success = AverageSuccess }) ->
    AverageThroughput * AverageSuccess.

discover_peers(Peers) ->
    %% We are trying to make discovery more efficient to avoid potential
    %% blocking behaviour that could be used to DOS the system.
    %% We can perform the probing (calling the `/info` endpoint) of remote nodes
    %% in parallel.
    %% An adversary might submit a half open IP and port 1000 times, in this case
    %% pmap would be "demultiplexed" since the http client will use the same
    %% connection process for the same peer.
    %% Overall, we don't have to wait the sum of all response times, we will wait
    %% for only the longest response time.
    UniquePeers = lists:usort(Peers),
    arweave_util:pmap(
        fun probe_and_maybe_add_peer/1, UniquePeers, ?PEER_PROBE_TIMEOUT),
    ok.

probe_and_maybe_add_peer(Peer) ->
    case is_registered(Peer) of
        true ->
            ok;
        false ->
            case check_peer(Peer, is_public_peer(Peer)) of
                ok ->
                    case ar_http_iface_client:get_info(Peer) of
                        info_unavailable ->
                            ok;
                        Info ->
                            case maps:get(atom_to_binary(network), Info, no_key) of
                                <<?NETWORK_NAME>> ->
                                    case maps:get(atom_to_binary(release), Info, no_key) of
                                        Release when is_integer(Release) ->
                                            maybe_add_peer(Peer, Release);
                                        no_key ->
                                            maybe_add_peer(Peer, 0)
                                    end;
                                _ ->
                                    ok
                            end
                    end;
                _ ->
                    ok
            end
    end.

format_stats(lifetime, Peer, Perf) ->
    KB = Perf#performance.total_bytes / 1024,
    io:format(
      "\t~s ~.2f kB/s (~.2f kB, ~.2f success, ~p transfers)~n",
      [string:pad(arweave_util:format_peer(Peer), 21, trailing, $\s),
            float(get_peer_rating(lifetime, Perf)), KB,
       Perf#performance.average_success, Perf#performance.total_transfers]);
format_stats(current, Peer, Perf) ->
    io:format(
      "\t~s ~.2f kB/s (~.2f success)~n",
      [string:pad(arweave_util:format_peer(Peer), 21, trailing, $\s),
            float(get_peer_rating(current, Perf)),
       Perf#performance.average_success]).

load_peers() ->
    case ar_storage:read_term(peers) of
        not_found ->
            ok;
        {ok, {_TotalRating, Records}} ->
            ?LOG_INFO([{event, polling_saved_peers}, {records, length(Records)}]),
            ar:console("Polling saved peers...~n"),
            load_peers(Records),
            ?LOG_INFO([{event, polled_saved_peers}]),
            ar:console("Polled saved peers.~n");
        {ok, {_TotalRating, Records, Tags}} ->
            ?LOG_INFO([{event, polling_saved_peers}, {records, length(Records)}]),
            ar:console("Polling saved peers...~n"),
            load_peers(Records),
            [ ets:insert(?MODULE, {K, V}) || {K, V} <- Tags ],
            ?LOG_INFO([{event, polled_saved_peers}]),
            ar:console("Polled saved peers.~n")
    end.

load_peers(Peers) ->
    {Batch, Rest} = arweave_util:split_at_most(20, Peers),
    Result = arweave_util:pmap(fun load_peer/1, Batch),
    case Rest of
        [] -> Result;
        _ -> load_peers(Rest)
    end.

load_peer({Peer, Performance}) ->
    case ar_http_iface_client:get_info(Peer, network) of
        info_unavailable ->
            ?LOG_DEBUG([{event, peer_unavailable}, {peer, arweave_util:format_peer(Peer)}]),
            ok;
        <<?NETWORK_NAME>> ->
            maybe_rotate_peer_ports(Peer),
            case Performance of
                {performance, TotalBytes, _TotalLatency, Transfers, _Failures, Rating} ->
                    %% For backwards compatibility. The legacy Rating was the
                    %% completed AVERAGE, while total_throughput is a running
                    %% SUM that get_peer_rating/2 divides by total_transfers —
                    %% scale the average back up by the transfer count, or an
                    %% established peer reloads at Rating/Transfers and stays
                    %% de-ranked for ~Transfers further samples.
                    set_performance(Peer, #performance{
                                             total_bytes = TotalBytes,
                        total_throughput = Rating * Transfers,
                                             total_transfers = Transfers,
                        average_throughput = Rating
                                            });
                {performance, TotalBytes, _TotalLatency, Transfers, _Failures, Rating, Release} ->
                    %% For backwards compatibility; see the scaling note above.
                    set_performance(Peer, #performance{
                                             release = Release,
                                             total_bytes = TotalBytes,
                        total_throughput = Rating * Transfers,
                                             total_transfers = Transfers,
                        average_throughput = Rating
                                            });
                {performance, 3,
                        Release, TotalBytes, TotalThroughput, TotalTransfers,
                        _AverageLatency, AverageThroughput, AverageSuccess,
                        _LifetimeRating, _CurrentRating} ->
                    %% Version 3: drop the never-read average_latency and the
                    %% stored ratings (now derived on read).
                    set_performance(Peer, #performance{
                        release = Release,
                        total_bytes = TotalBytes,
                        total_throughput = TotalThroughput,
                        total_transfers = TotalTransfers,
                        average_throughput = AverageThroughput,
                        average_success = AverageSuccess
                    });
                {performance, 4,
                        Release, TotalBytes, TotalThroughput, TotalTransfers,
                        AverageThroughput, AverageSuccess,
                        _LifetimeRating, _CurrentRating} ->
                    %% Version 4: drop the stored ratings (now derived on read).
                    set_performance(Peer, #performance{
                        release = Release,
                        total_bytes = TotalBytes,
                        total_throughput = TotalThroughput,
                        total_transfers = TotalTransfers,
                        average_throughput = AverageThroughput,
                        average_success = AverageSuccess
                    });
                #performance{ version = 5 } ->
                    %% Going forward whenever we change the #performance record we should
                    %% increment the version field so we can match on it when doing a load.
                    set_performance(Peer, Performance)
            end,
            ok;
        Network ->
            ?LOG_DEBUG([{event, peer_from_the_wrong_network},
                        {peer, arweave_util:format_peer(Peer)}, {network, Network}]),
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
                            remove_peer(rotated, RemovedPeer)
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

ping_peers(Peers) ->
    {Batch, Rest} = arweave_util:split_at_most(100, Peers),
    Result = arweave_util:pmap(fun ar_http_iface_client:add_peer/1, Batch),
    case Rest of
        [] -> Result;
        _ -> ping_peers(Rest)
    end.

-ifdef(AR_TEST).
%% Do not filter out loopback IP addresses with custom port in the debug mode
%% to allow multiple local VMs to peer with each other.
is_loopback_ip({127, _, _, _, Port}) ->
    ConfigPort = arweave_config:get([port]),
    Port == ConfigPort;
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
    ets:foldl(
      fun ({{peer, Peer}, Performance}, Acc) ->
              %% Bigger score increases the chances to end up on the top
              %% of the peer list, but at the same time the ranking is
              %% probabilistic to always give everyone a chance to improve
              %% in the competition (i.e., reduce the advantage gained by
                %% being the first to earn a reputation). (Only the relative
                %% order matters downstream, so no normalization is needed.)
                Score = rand:uniform() * get_peer_rating(Rating, Performance),
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
    ets:insert(?MODULE, {{peers, Rating}, lists:sublist(Peers, ?MAX_PEER_DISCOVERY_LIST_LEN)}).

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
    update_rating(Peer, undefined, undefined, IsSuccess).
update_rating(Peer, LatencyMilliseconds, DataSize, false)
  when LatencyMilliseconds =/= undefined; DataSize =/= undefined ->
    %% Don't credit peers for failed requests.
    update_rating(Peer, undefined, undefined, false);
update_rating(Peer, 0, _DataSize, IsSuccess) ->
    update_rating(Peer, undefined, undefined, IsSuccess);
update_rating(Peer, +0.0, _DataSize, IsSuccess) ->
    update_rating(Peer, undefined, undefined, IsSuccess);
update_rating(Peer, LatencyMilliseconds, DataSize, IsSuccess) ->
    Performance = get_or_init_performance(Peer),

    #performance{
       total_bytes = TotalBytes,
       total_throughput = TotalThroughput,
       total_transfers = TotalTransfers,
       average_throughput = AverageThroughput,
        average_success = AverageSuccess
      } = Performance,
    TotalBytes2 = case DataSize of
                      undefined -> TotalBytes;
                      _ -> TotalBytes + DataSize
                  end,
    AverageThroughput2 = case LatencyMilliseconds of
                             undefined -> AverageThroughput;
                             _ -> arweave_util:ema(
            AverageThroughput, DataSize / LatencyMilliseconds, ?THROUGHPUT_ALPHA)
                         end,
    TotalThroughput2 = case LatencyMilliseconds of
                           undefined -> TotalThroughput;
                           _ -> TotalThroughput + (DataSize / LatencyMilliseconds)
                       end,
    TotalTransfers2 = case DataSize of
                          undefined -> TotalTransfers;
                          _ -> TotalTransfers + 1
                      end,
    AverageSuccess2 = arweave_util:ema(AverageSuccess, arweave_util:bool_to_int(IsSuccess), ?SUCCESS_ALPHA),
    Performance2 = Performance#performance{
                     total_bytes = TotalBytes2,
                     total_throughput = TotalThroughput2,
                     total_transfers = TotalTransfers2,
                     average_throughput = AverageThroughput2,
        average_success = AverageSuccess2
                    },
    maybe_rotate_peer_ports(Peer),
    set_performance(Peer, Performance2),
    Performance2.

observe_inbound_peer(Peer, Release, At) ->
    case ets:lookup(ar_inbound_peers, Peer) of
        [{Peer, _, StoredAt}] when StoredAt > At ->
            ok;
        Existing ->
            do_observe_inbound_peer(Peer, Release, At, Existing)
    end.

do_observe_inbound_peer(Peer, Release, At, Existing) ->
    case Existing == [] andalso
            ets:info(ar_inbound_peers, size) >= ?MAX_INBOUND_PEERS of
        true ->
            ok;
        false ->
            ets:insert(ar_inbound_peers,
                {Peer, normalize_inbound_release(Release), At}),
            ok
    end.

expire_inbound_peers() ->
    Cutoff = erlang:monotonic_time(second) - ?INBOUND_PEER_WINDOW_S,
    ets:select_delete(ar_inbound_peers, [{{'_', '_', '$1'},
        [{'=<', '$1', Cutoff}], [true]}]).

recent_inbound_peers(Now) ->
    Rows = try ets:tab2list(ar_inbound_peers)
    catch error:badarg ->
        []
    end,
    Cutoff = Now - ?INBOUND_PEER_WINDOW_S,
    lists:filter(fun({_Peer, _Release, At}) ->
        At > Cutoff andalso At =< Now
    end, Rows).

normalize_inbound_release(Release) when is_integer(Release), Release >= 0,
        Release =< 65535 ->
    Release;
normalize_inbound_release(_) ->
    unknown.

maybe_add_peer(Peer, Release) ->
    maybe_rotate_peer_ports(Peer),
    %% If we've just added his peer, flag it as active and connected.
    connected_peer(Peer),
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

remove_peer(Reason, RemovedPeer) ->
    case Reason of
        rotated ->
            ok;
        _ ->
            ?LOG_DEBUG([
                        {event, remove_peer},
                        {peer, arweave_util:format_peer(RemovedPeer)},
                        {reason, Reason}
                       ])
    end,
    ets:delete(?MODULE, {peer, RemovedPeer}),
    remove_peer_tags(RemovedPeer),
    remove_peer_port(RemovedPeer),
    ar_events:send(peer, {removed, RemovedPeer}).

remove_peer_tags(Peer) ->
    ets:match_delete(?MODULE, {{ar_tags, ?MODULE, Peer, '_'}, '_'}).

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
                  fun   ({{peer, Peer}, Performance}, Acc) ->
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
            Tags = ets:foldl(fun ({{ar_tags, _, _, _}, _} = Tag, Acc) ->
                                     [Tag|Acc];
                                 (_, Acc) -> Acc
                             end, [], ?MODULE),
            ?LOG_INFO([{event, store_peers}
                      , {records, length(Records)}
                      , {tags, length(Tags)}]),
            %% The leading 0 keeps the on-disk tuple shape older releases read
            %% (they ignored the total-rating slot too).
            ar_storage:write_term(peers, {0, Records, Tags})
    end.

%%--------------------------------------------------------------------
%% @hidden
%% @doc internal function to tag a peer.
%% @end
%%--------------------------------------------------------------------
set_tag(Peer, Tag, Value) ->
    ets:insert(?MODULE, {{ar_tags, ?MODULE, Peer, Tag}, Value}).

%%--------------------------------------------------------------------
%% @hidden
%% @doc internal function to get tag value set on a peer.
%% @end
%%--------------------------------------------------------------------
get_tag(Peer, Tag) ->
    Pattern = {{ar_tags, ?MODULE, Peer, Tag}, '$1'},
    Guard = [],
    Select = ['$1'],
    case ets:select(?MODULE, [{Pattern, Guard, Select}]) of
        [] -> {error, not_found};
        [V] -> {ok, V}
    end.

%%--------------------------------------------------------------------
%% @doc defined a peer as connected (in HTTP sense).
%% @end
%%--------------------------------------------------------------------
connected_peer(Peer) ->
    set_tag(Peer, {connection, last}, erlang:system_time(second)),
    set_tag(Peer, {connection, active}, true).

%%--------------------------------------------------------------------
%% @doc defined a peer as disconnected (in HTTP sense).
%% @end
%%--------------------------------------------------------------------
disconnected_peer(Peer) ->
    set_tag(Peer, {connection, active}, false).

%%--------------------------------------------------------------------
%% @doc returns peer's timestamp.
%% @end
%%--------------------------------------------------------------------
get_connection_timestamp_peer(Peer) ->
    case get_tag(Peer, {connection, last}) of
        {ok, V} -> V;
        _ -> undefined
    end.

%%--------------------------------------------------------------------
%% @doc returns the HTTP connection state of a peer.
%% @end
%%--------------------------------------------------------------------
is_connected_peer(Peer) ->
    case get_tag(Peer, {connection, active}) of
        {ok, V} -> V;
        {error, _} -> false
    end.
