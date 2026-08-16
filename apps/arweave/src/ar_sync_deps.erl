%%% @doc The sync pipeline's injectable dependency boundary. Operations the
%%% simulator replaces go through the module returned by m/0.
%%% ar_sync_deps_mainnet is the production implementation; ar_sync_sim_tests
%%% installs ar_sync_deps_sim to run the real pipeline against a controlled
%%% world.
%%%
%%% Deliberately NOT in the boundary: time (ar_timer), configuration
%%% (with_test_config/1), metrics, and internal discovery cache operations.
-module(ar_sync_deps).

%% Each delegates to m(), so call sites read ar_sync_deps:get_chunk_binary(...).
-export([pick_peers/2, is_throttled/2,
        rate_fetched_data/5, get_chunk_binary/3,
        is_chunk_cache_full/0, chunk_cache_size/0, chunk_cache_size/1,
        chunk_cache_size_limit/0,
        increment_chunk_cache_size/1,
        is_disk_space_sufficient/1, is_footprint_record_initialized/1,
        store_fetched_chunk/5, unsynced_intervals/3,
        unsynced_footprint_intervals/3,
        get_next_not_blacklisted_byte/1, get_next_synced_interval/4,
        get_peer_release/1, get_peers/1, is_joined/0, get_weave_size/0,
        get_sync_buckets/2, fetch_chunk_intervals/2]).

-export([m/0]).

-ifdef(AR_TEST).
-export([override_module/1, reset_all_overrides/0]).
-endif.

%%%===================================================================
%%% Callbacks.
%%%===================================================================

%% Peer selection and request accounting.
-callback pick_peers(Peers :: [term()], Count :: pos_integer()) -> [term()].
-callback is_throttled(Peer :: term(), Path :: string()) -> boolean().
-callback rate_fetched_data(Peer :: term(), DataType :: atom(),
    Result :: term(), ElapsedUs :: term(), Bytes :: non_neg_integer()) -> ok.

%% Chunk transfer.
-callback get_chunk_binary(Peer :: term(), Offset :: non_neg_integer(),
    Packing :: term()) ->
        {ok, Proof :: map(), Time :: term(), TransferSize :: term()}
        | {error, term()}.

%% Chunk cache and disk.
-callback is_chunk_cache_full() -> boolean().
-callback chunk_cache_size() -> non_neg_integer().
-callback chunk_cache_size(StoreID :: term()) -> non_neg_integer().
-callback chunk_cache_size_limit() -> pos_integer().
%% The store's write throughput in bytes/ms, or undefined before it has
%% written anything. A capacity, not a utilisation: an idle store keeps the
%% rate it last demonstrated.
-callback increment_chunk_cache_size(StoreID :: term()) -> ok.
-callback is_disk_space_sufficient(StoreID :: term()) -> term().
-callback is_footprint_record_initialized(StoreID :: term()) -> boolean().

%% Write handoff of a fetched chunk.
-callback store_fetched_chunk(StoreID :: term(), Peer :: term(),
    Byte :: non_neg_integer(), Proof :: map(), TaskRef :: term()) -> ok.

%% Local need: what StoreID still lacks in [Start, End) as the sweeper
%% discovers it (sync records minus the blacklist).
-callback unsynced_intervals(Start :: non_neg_integer(),
    End :: non_neg_integer(), StoreID :: term()) -> term().
-callback unsynced_footprint_intervals(Partition :: non_neg_integer(),
    Footprint :: non_neg_integer(), StoreID :: term()) -> term().

%% ar_sync_discovery's own outward dependencies — the peer registry,
%% node state, and peer metadata endpoints — so the REAL
%% discovery gen_server can run against the simulated world (a sim-side
%% model of discovery would only ever test a copy of its admission
%% logic; regressions there stay invisible).
-callback get_peer_release(Peer :: term()) -> integer().
-callback get_peers(Type :: current) -> [term()].
-callback is_joined() -> boolean().
-callback get_weave_size() -> non_neg_integer().
-callback get_sync_buckets(Peer :: term(), Mode :: byte | footprint) ->
    {ok, term()} | {error, term()}.
%% `none' as the byte query's right bound selects the legacy left-bounded
%% endpoint. Footprint queries return `not_found' for unsupported data.
-callback fetch_chunk_intervals(Peer :: term(),
    Query :: {byte, Start :: non_neg_integer(),
        Right :: non_neg_integer() | none, Limit :: pos_integer()}
        | {footprint, Partition :: non_neg_integer(), Footprint :: non_neg_integer()}) ->
        {ok, term()} | {error, term()} | not_found.

%% Local node state the fetch worker consults mid-range.
-callback get_next_not_blacklisted_byte(Byte :: non_neg_integer()) ->
    non_neg_integer().
-callback get_next_synced_interval(Byte :: non_neg_integer(),
    End :: non_neg_integer(), ID :: term(), StoreID :: term()) ->
    {non_neg_integer(), non_neg_integer()} | not_found.

%%%===================================================================
%%% Selection.
%%%===================================================================

%% @doc The configured dependency implementation. Compiled to the
%% mainnet module outside AR_TEST: production cannot be redirected, even
%% by writing the persistent_term directly.
-ifdef(AR_TEST).
m() ->
    persistent_term:get({?MODULE, module}, ar_sync_deps_mainnet).
-else.
m() ->
    ar_sync_deps_mainnet.
-endif.

%%%===================================================================
%%% Delegation.
%%%===================================================================

pick_peers(Peers, Count) -> (m()):pick_peers(Peers, Count).
is_throttled(Peer, Path) -> (m()):is_throttled(Peer, Path).
rate_fetched_data(Peer, DataType, Result, ElapsedUs, Bytes) ->
    (m()):rate_fetched_data(Peer, DataType, Result, ElapsedUs, Bytes).
get_chunk_binary(Peer, Offset, Packing) ->
    (m()):get_chunk_binary(Peer, Offset, Packing).
is_chunk_cache_full() -> (m()):is_chunk_cache_full().
chunk_cache_size() -> (m()):chunk_cache_size().
chunk_cache_size(StoreID) -> (m()):chunk_cache_size(StoreID).
chunk_cache_size_limit() -> (m()):chunk_cache_size_limit().
increment_chunk_cache_size(StoreID) -> (m()):increment_chunk_cache_size(StoreID).
is_disk_space_sufficient(StoreID) -> (m()):is_disk_space_sufficient(StoreID).
is_footprint_record_initialized(StoreID) ->
    (m()):is_footprint_record_initialized(StoreID).
store_fetched_chunk(StoreID, Peer, Byte, Proof, TaskRef) ->
    (m()):store_fetched_chunk(StoreID, Peer, Byte, Proof, TaskRef).
unsynced_intervals(Start, End, StoreID) ->
    (m()):unsynced_intervals(Start, End, StoreID).
unsynced_footprint_intervals(Partition, Footprint, StoreID) ->
    (m()):unsynced_footprint_intervals(Partition, Footprint, StoreID).
get_next_not_blacklisted_byte(Byte) -> (m()):get_next_not_blacklisted_byte(Byte).
get_next_synced_interval(Byte, End, ID, StoreID) ->
    (m()):get_next_synced_interval(Byte, End, ID, StoreID).
get_peer_release(Peer) -> (m()):get_peer_release(Peer).
get_peers(Type) -> (m()):get_peers(Type).
is_joined() -> (m()):is_joined().
get_weave_size() -> (m()):get_weave_size().
get_sync_buckets(Peer, Mode) -> (m()):get_sync_buckets(Peer, Mode).
fetch_chunk_intervals(Peer, Query) -> (m()):fetch_chunk_intervals(Peer, Query).

-ifdef(AR_TEST).

%% @doc Install an alternative implementation (node-global; tests must call
%% reset_all_overrides/0 when done).
override_module(Module) ->
    persistent_term:put({?MODULE, module}, Module).

reset_all_overrides() ->
    persistent_term:erase({?MODULE, module}),
    ok.

-endif.
