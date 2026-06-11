%%% @doc Named, semantic waits for asynchronous test conditions
%%% (sync-record coverage, HTTP availability, partition state, etc.),
%%% all behind one polling primitive. Prefer a named helper; add one
%%% here rather than writing a fresh `ar_util:do_until' loop in a test.
%%% Reserve raw `timer:sleep' for deliberately time-based behaviour,
%%% never as a stand-in for a condition wait.
%%%
%%% Every public helper returns `ok' on success or
%%% `{error, {timeout, Name}}' on deadline; helpers that yield a value
%%% on success are typed individually below.
%%%
%%% Remote-observing helpers take the node first: `main' runs in this
%%% BEAM, any other atom routes via `ar_test_node:remote_call/4'.
-module(ar_test_await).

-export([
	%% --- ar_sync_record waits ---
	%% `chunk_recorded' reads the gen_server; the `http_*_recorded' pair
	%% reads the HTTP API. The two views of the same record can diverge
	%% during boot or while caches warm, so pick deliberately.
	chunk_recorded/3,               %% (Node, Offset, Opts) — see docstring
	http_chunks_recorded/3,         %% (Node, Start, End)
	http_chunks_not_recorded/3,     %% (Node, Start, End)
	entropy_prepared/4,             %% (Node, StoreID, Start, End)
	entropy_not_prepared/4,         %% (Node, StoreID, Start, End)
	all_entropy_prepared/1,         %% (Node)
	global_sync_record_matches/2,   %% (Options, Expected)
	global_sync_record_excludes/2,  %% (Options, Range)

	%% --- HTTP-mediated chunk fetches ---
	http_chunk_matches/3,           %% (Node, Offset, ExpectedFields)
	http_chunk_matches/4,           %% (Node, Offset, ExpectedFields, Opts)
	http_tx_data_matches/3,         %% (Node, TXID, Expected)
	http_tx_data/2,                 %% (Node, TXID) -> {ok, Body}
	http_post_chunk_status/3,       %% (Node, Proof, Status)

	%% --- Other state observables ---
	tx_offset_known/1,              %% (TXID) -> {ok, Offset} | {error, _}
	partition_empty/3,              %% (Node, PartitionNumber, Packing)
	partition_at_size/4,            %% (Node, PartitionNumber, Packing, Size)
	disk_pool_chunk_count/1,        %% (Pred)
	application_stopped/2,          %% (App, Timeout)
	ar_kv_stopped/1,                %% (Timeout)

	%% --- Block / chain state ---
	node_height/2,                  %% (Node, TargetHeight) -> {ok, BI}
	block_stored/1,                 %% (HashOrHeight) -> Block
	block_stored/2,                 %% (HashOrHeight, IncludeTXs) -> Block
	block_index_matches/2,          %% (Node, BI)
	node_joined/1,                  %% (Node)
	txs_stored/1,                   %% (TXIDs)

	%% --- Mempool / TX state ---
	txs_ready_for_mining/2,         %% (Node, TXs)
	tx_in_mempool/2,                %% (Node, TXID)
	mempool_drained/1,              %% (Node)
	tx_confirmed/2,                 %% (Node, TXID)

	%% --- Mining / VDF state ---
	mining_paused/1,                %% (Node)
	vdf_step/2,                     %% (Node, StepNumber)

	%% --- Peer / distribution state ---
	http_ready/1,                   %% (Node)
	node_down/1,                    %% (NodeName)
	peer_listed/2,                  %% (Node, PeerIP)

	%% --- Data roots availability ---
	%% `data_roots_available' reads the gen_server; the `http_' variant
	%% reads `GET /data_roots/<Start>'. The two views can diverge during
	%% boot or while caches warm, so pick deliberately.
	data_roots_available/2,         %% (Peer, Block)
	http_data_roots_available/2,    %% (Peer, Block)

	%% --- Public polling primitive ---
	%% For custom predicates; prefer a named helper above. `/2' uses the
	%% standard timeout, `/3' takes a custom one for rare sub-100s waits.
	until/2, until/3
]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").

%% Poll interval for every wait (10 Hz).
-define(POLL_INTERVAL_MS, 100).

%% `?TIMEOUT_MS' is the give-up budget for positive waits; the success
%% path returns as soon as the predicate flips true. 5 minutes covers
%% the slowest known case (peer cold start with RandomX init).
%%
%% `?TIMEOUT_NEGATIVE_MS' is the literal sleep before the single check
%% done by the verify-doesn't-happen helpers (`entropy_not_prepared',
%% `partition_empty', `http_chunks_not_recorded'). The conditions they
%% observe only grow, so one late check matches polling; keep it short.
-define(TIMEOUT_MS, 300_000).
-define(TIMEOUT_NEGATIVE_MS, 15_000).

%%%===================================================================
%%% ar_sync_record waits.
%%%===================================================================
%%%
%%% `chunk_recorded' reads the live process via
%%% `ar_sync_record:is_recorded'; `http_chunks_recorded' /
%%% `http_chunks_not_recorded' read an outside client's view via
%%% `GET /sync_record' (plus `GET /footprints/...' for replica_2_9).
%%% The two can disagree during boot or while caches warm.

%% @doc Wait until `Offset' is recorded in `ar_data_sync' on `Node'.
%% `Opts' optionally narrows the match: `packing' restricts to a
%% packing (keyed `{ar_data_sync, Packing}'), `store_id' to a storage
%% module. Each defaults to "any", so `#{}' matches anywhere.
-spec chunk_recorded(Node :: atom(), Offset :: non_neg_integer(),
		Opts :: map()) ->
	ok | {error, {timeout, term()}}.
chunk_recorded(Node, Offset, Opts) ->
	do_until_true(chunk_recorded,
		fun() -> is_chunk_recorded(Node, Offset, Opts) end).

%% @doc Wait until `Node' reports the byte range `[Start, End)' as
%% recorded via its sync-record HTTP endpoint (plus footprint
%% intervals for replica_2_9 — `GET /footprints/...').
-spec http_chunks_recorded(Node :: atom(),
		Start :: non_neg_integer(), End :: non_neg_integer()) ->
	ok | {error, {timeout, term()}}.
http_chunks_recorded(Node, Start, End) ->
	do_until_true(http_chunks_recorded,
		fun() -> has_range(Node, Start, End) end).

%% @doc Sleeps `?TIMEOUT_NEGATIVE_MS' then checks once: returns `ok'
%% if `Node''s sync_record HTTP endpoint does NOT report `[Start, End)'
%% as covered, `{error, http_chunks_recorded}' if it does. Doesn't
%% poll — sync record coverage is monotonic.
-spec http_chunks_not_recorded(Node :: atom(),
		Start :: non_neg_integer(), End :: non_neg_integer()) ->
	ok | {error, http_chunks_recorded}.
http_chunks_not_recorded(Node, Start, End) ->
	timer:sleep(?TIMEOUT_NEGATIVE_MS),
	case has_range(Node, Start, End) of
		true -> {error, http_chunks_recorded};
		false -> ok
	end.

%% @doc Wait until the entropy sync record on `Node' covers the full
%% `[Start, End)' range for `StoreID' — i.e. `replica_2_9' entropy is
%% prepared for that range.
-spec entropy_prepared(Node :: atom(), StoreID :: term(),
		Start :: non_neg_integer(), End :: non_neg_integer()) ->
	ok | {error, {timeout, term()}}.
entropy_prepared(Node, StoreID, Start, End) ->
	RangeSize = End - Start,
	do_until_true(entropy_prepared,
		fun() ->
			on(Node, ar_sync_record, get_intersection_size,
				[End, Start, ar_entropy_storage:sync_record_id(),
					StoreID]) >= RangeSize
		end).

%% @doc Inverse of `entropy_prepared/4': after `?TIMEOUT_NEGATIVE_MS'
%% returns `ok' if no entropy covers `[Start, End)', else
%% `{error, has_entropy}'.
-spec entropy_not_prepared(Node :: atom(), StoreID :: term(),
		Start :: non_neg_integer(), End :: non_neg_integer()) ->
	ok | {error, has_entropy}.
entropy_not_prepared(Node, StoreID, Start, End) ->
	timer:sleep(?TIMEOUT_NEGATIVE_MS),
	case on(Node, ar_sync_record, get_intersection_size,
			[End, Start, ar_entropy_storage:sync_record_id(), StoreID]) of
		0 -> ok;
		_ -> {error, has_entropy}
	end.

%% @doc Wait until every replica_2_9 storage module on `Node' has its
%% entropy prepared over its full range. Other modules are skipped.
-spec all_entropy_prepared(Node :: atom()) -> ok.
all_entropy_prepared(Node) ->
	StorageModuleConfigs = on(Node, arweave_config, get, [[storage_modules]]),
	lists:foreach(
		fun(Config) ->
			case arweave_config:config_to_storage_module(Config) of
				{_, _, {replica_2_9, _}} = Module ->
					StoreID = ar_storage_module:id(Module),
					%% Derive the range from `Module' directly: a registry
					%% lookup would miss, since these modules live on `Node'
					%% not the test runner's local BEAM.
					{Start, End} = ar_storage_module:module_range(Module),
					ok = entropy_prepared(Node, StoreID, Start, End);
				_ ->
					ok
			end
		end,
		StorageModuleConfigs).

%% @doc Wait until the global sync record served for `Options' decodes
%% to exactly the interval list `Expected'.
-spec global_sync_record_matches(Options :: map(), Expected :: [term()]) ->
	ok | {error, {timeout, term()}}.
global_sync_record_matches(Options, Expected) ->
	do_until_true(global_sync_record_matches,
		fun() -> global_sync_record_to_list(Options) == Expected end).

%% @doc Wait until the global sync record served for `Options' no longer
%% intersects `Range'.
-spec global_sync_record_excludes(Options :: map(), Range :: term()) ->
	ok | {error, {timeout, term()}}.
global_sync_record_excludes(Options, Range) ->
	do_until_true(global_sync_record_excludes,
		fun() ->
			ar_intervals:is_empty(
				ar_intervals:intersection(global_sync_record(Options), Range))
		end).

%%%===================================================================
%%% HTTP-mediated chunk fetches.
%%%===================================================================

%% @doc Wait until `GET /chunk' at `Offset' on `Node' returns 200 with a
%% proof matching every entry in `ExpectedFields' (a map of raw-binary
%% `chunk' / `tx_path' / `data_path'; absent keys are unchecked, so
%% `#{}' matches any 200). Returns `{ok, Proof}' with the decoded poa
%% map. `Opts' may carry `packing' for the fetch.
-spec http_chunk_matches(Node :: atom(), Offset :: non_neg_integer(),
		ExpectedFields :: map()) ->
	{ok, map()} | {error, {timeout, term()}}.
http_chunk_matches(Node, Offset, ExpectedFields) ->
	http_chunk_matches(Node, Offset, ExpectedFields, #{}).

-spec http_chunk_matches(Node :: atom(), Offset :: non_neg_integer(),
		ExpectedFields :: map(), Opts :: map()) ->
	{ok, map()} | {error, {timeout, term()}}.
http_chunk_matches(Node, Offset, ExpectedFields, Opts) ->
	Packing = maps:get(packing, Opts, undefined),
	do_until(http_chunk_matches,
		fun() ->
			case ar_test_node:get_chunk(Node, Offset, Packing) of
				{ok, {{<<"200">>, _}, _, EncodedProof, _, _}} ->
					Proof = ar_serialize:json_map_to_poa_map(
						jiffy:decode(EncodedProof, [return_maps])),
					case chunk_fields_match(Proof, ExpectedFields) of
						true -> {true, Proof};
						false -> false
					end;
				_ ->
					false
			end
		end).

chunk_fields_match(Proof, ExpectedFields) ->
	maps:fold(
		fun(_, _, false) -> false;
		   (K, V, true) -> maps:get(K, Proof, undefined) =:= V
		end,
		true, ExpectedFields).

%% @doc Wait until `GET /tx/<TXID>/data' on `Node' returns 200 with a
%% body equal to `Expected'.
-spec http_tx_data_matches(Node :: atom(), TXID :: binary(),
		Expected :: binary()) ->
	ok | {error, {timeout, term()}}.
http_tx_data_matches(Node, TXID, Expected) ->
	do_until_true(http_tx_data_matches,
		fun() ->
			case request_tx_data(Node, TXID) of
				{ok, {{<<"200">>, _}, _, Expected, _, _}} -> true;
				_ -> false
			end
		end).

%% @doc Wait until `Node' serves `GET /tx/<TXID>/data' with a non-empty 200 and
%% return the body. The endpoint can transiently 404 after a restart/rejoin until
%% the stored chunks become servable again.
-spec http_tx_data(Node :: atom(), TXID :: binary()) ->
	{ok, binary()} | {error, {timeout, term()}}.
http_tx_data(Node, TXID) ->
	do_until(http_tx_data,
		fun() ->
			case request_tx_data(Node, TXID) of
				{ok, {{<<"200">>, _}, _, Body, _, _}} when Body =/= <<>> -> {true, Body};
				_ -> false
			end
		end).

%% `GET /tx/<TXID>/data' from `Node'.
request_tx_data(Node, TXID) ->
	ar_http:req(#{ method => get, peer => ar_test_node:peer_ip(Node),
		path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)) ++ "/data" }).

%% @doc Wait until `POST /chunk' of `Proof' to `Node' responds with the
%% HTTP status `Status' (e.g. `<<"303">>').
-spec http_post_chunk_status(Node :: atom(), Proof :: term(),
		Status :: binary()) ->
	ok | {error, {timeout, term()}}.
http_post_chunk_status(Node, Proof, Status) ->
	do_until_true(http_post_chunk_status,
		fun() ->
			case ar_test_node:post_chunk(Node, ar_serialize:jsonify(Proof)) of
				{ok, {{Status, _}, _, _, _, _}} -> true;
				_ -> false
			end
		end).

%%%===================================================================
%%% Other state observables.
%%%===================================================================

%% @doc Wait until `ar_data_sync:get_tx_offset(TXID)' returns
%% `{ok, Offset}'. Returns the offset on success.
-spec tx_offset_known(TXID :: binary()) ->
	{ok, term()} | {error, {timeout, term()}}.
tx_offset_known(TXID) ->
	do_until(tx_offset_known,
		fun() ->
			case ar_data_sync:get_tx_offset(TXID) of
				{ok, Offset} -> {true, Offset};
				_ -> false
			end
		end).

%% @doc After `?TIMEOUT_NEGATIVE_MS', returns `ok' if `Node''s partition
%% data size is still 0, else `{error, not_empty}'.
-spec partition_empty(Node :: atom(),
		PartitionNumber :: non_neg_integer(), Packing :: term()) ->
	ok | {error, not_empty}.
partition_empty(Node, PartitionNumber, Packing) ->
	timer:sleep(?TIMEOUT_NEGATIVE_MS),
	case on(Node, ar_mining_stats, get_partition_data_size,
			[PartitionNumber, Packing]) of
		0 -> ok;
		_ -> {error, not_empty}
	end.

%% @doc Wait until `Node''s partition data size equals `Size' and stays
%% equal across two reads ~250ms apart. The double read guards against
%% `chunk_copy_worker' transiently invalidating a chunk between modules.
%% Match-fails if the final read isn't `Size'.
-spec partition_at_size(Node :: atom(),
		PartitionNumber :: non_neg_integer(), Packing :: term(),
		Size :: non_neg_integer()) ->
	non_neg_integer().
partition_at_size(Node, PartitionNumber, Packing, Size) ->
	_ = do_until(partition_at_size,
		fun() ->
			case on(Node, ar_mining_stats, get_partition_data_size,
					[PartitionNumber, Packing]) of
				Size ->
					timer:sleep(250),
					Size =:= on(Node, ar_mining_stats, get_partition_data_size,
						[PartitionNumber, Packing]);
				_ ->
					false
			end
		end),
	Size = on(Node, ar_mining_stats, get_partition_data_size,
		[PartitionNumber, Packing]).

%% @doc Wait until `Pred' holds for the current disk-pool chunk count
%% (`length(ar_disk_pool:debug_get_chunks())').
-spec disk_pool_chunk_count(Pred :: fun((non_neg_integer()) -> boolean())) ->
	ok | {error, {timeout, term()}}.
disk_pool_chunk_count(Pred) ->
	do_until_true(disk_pool_chunk_count,
		fun() -> Pred(length(ar_disk_pool:debug_get_chunks())) end).

%% @doc Wait until `App' has no application master process.
-spec application_stopped(App :: atom(), Timeout :: non_neg_integer()) ->
	ok | {error, {timeout, term()}}.
application_stopped(App, Timeout) ->
	do_until_true(application_stopped,
		fun() -> not is_pid(application_controller:get_master(App)) end,
		Timeout).

%% @doc Wait until the supervised `ar_kv' process and ETS table are gone.
-spec ar_kv_stopped(Timeout :: non_neg_integer()) -> ok | {error, {timeout, term()}}.
ar_kv_stopped(Timeout) ->
	do_until_true(ar_kv_stopped,
		fun() -> {whereis(ar_kv), ets:info(ar_kv)} =:= {undefined, undefined} end,
		Timeout).

%% @doc Wait until `ar_data_roots:are_synced/4' on `Peer' reports the
%% block's `[block_start, weave_size)' range as fully available under
%% `?DEFAULT_MODULE'. Observes the in-process `ar_data_roots' state.
-spec data_roots_available(Peer :: atom(), Block :: term()) ->
	ok | {error, {timeout, term()}}.
data_roots_available(Peer, Block) ->
	Start = data_roots_block_start(Block),
	End = Block#block.weave_size,
	TXRoot = Block#block.tx_root,
	do_until_true(data_roots_available,
		fun() ->
			on(Peer, ar_data_roots, are_synced,
				[Start, End, TXRoot, ?DEFAULT_MODULE])
		end).

%% @doc Wait until `Peer' serves a non-empty data-roots payload for
%% `Block' whose reported block size equals the block's weave size
%% delta. Observes the HTTP API (`GET /data_roots/<Start>').
-spec http_data_roots_available(Peer :: atom(), Block :: term()) ->
	ok | {error, {timeout, term()}}.
http_data_roots_available(Peer, Block) ->
	Expected = Block#block.block_size,
	do_until_true(http_data_roots_available,
		fun() ->
			case fetch_data_roots(Peer, Block) of
				{ok, Body} ->
					case ar_serialize:binary_to_data_roots(Body) of
						{ok, {_TXRoot, Expected, _Entries}} -> true;
						_ -> false
					end;
				_ ->
					false
			end
		end).

%%%===================================================================
%%% Block / chain state.
%%%===================================================================

%% @doc Wait until `Node''s tip height is `>= TargetHeight'. Returns
%% `{ok, BI}' with the block index seen when the predicate flipped true.
-spec node_height(Node :: atom(), TargetHeight :: integer()) ->
	{ok, term()} | {error, {timeout, term()}}.
node_height(Node, TargetHeight) ->
	do_until(node_height,
		fun() ->
			case on(Node, ar_node, get_blocks, []) of
				BI when length(BI) - 1 >= TargetHeight -> {true, BI};
				_ -> false
			end
		end).

%% @doc Wait until a locally stored block and all its transaction
%% headers are readable, then return the block (raising on timeout).
%% Returns the block seen by the successful iteration so callers don't
%% race a second read.
-spec block_stored(HashOrHeight :: term()) -> term().
block_stored(HashOrHeight) ->
	block_stored(HashOrHeight, false).

-spec block_stored(HashOrHeight :: term(), IncludeTXs :: boolean()) -> term().
block_stored(HashOrHeight, IncludeTXs) ->
	{ok, B} = do_until(block_and_txs_stored,
		fun() ->
			case ar_storage:read_block(HashOrHeight) of
				unavailable ->
					false;
				B ->
					TXs = ar_storage:read_tx(B#block.txs),
					case lists:any(fun(TX) -> TX == unavailable end, TXs) of
						true ->
							false;
						false ->
							case IncludeTXs of
								true -> {true, B#block{ txs = TXs }};
								false -> {true, B}
							end
					end
			end
		end),
	B.

%% @doc Wait until `Node''s block index exactly equals `BI'.
-spec block_index_matches(Node :: atom(), BI :: term()) ->
	ok | {error, {timeout, term()}}.
block_index_matches(Node, BI) ->
	do_until_true(block_index_matches,
		fun() -> on(Node, ar_node, get_blocks, []) =:= BI end).

%% @doc Wait until `Node' has joined the network.
-spec node_joined(Node :: atom()) ->
	ok | {error, {timeout, term()}}.
node_joined(Node) ->
	do_until_true(node_joined,
		fun() -> on(Node, ar_node, is_joined, []) end).

%% @doc Wait until every TXID in `TXIDs' has a stored `#tx{}' header
%% (none read back as `unavailable').
-spec txs_stored(TXIDs :: [binary()]) ->
	ok | {error, {timeout, term()}}.
txs_stored(TXIDs) ->
	do_until_true(txs_stored,
		fun() ->
			lists:all(fun(TX) -> is_record(TX, tx) end,
				ar_storage:read_tx(TXIDs))
		end).

%%%===================================================================
%%% Mempool / TX state.
%%%===================================================================

%% @doc Wait until every TX in `TXs' appears in `Node''s
%% ready-for-mining set.
-spec txs_ready_for_mining(Node :: atom(), TXs :: [term()]) ->
	ok | {error, {timeout, term()}}.
txs_ready_for_mining(Node, TXs) ->
	IDs = [TX#tx.id || TX <- TXs],
	do_until_true(txs_ready_for_mining,
		fun() ->
			MinedIDs = on(Node, ar_node, get_ready_for_mining_txs, []),
			lists:all(fun(ID) -> lists:member(ID, MinedIDs) end, IDs)
		end).

%% @doc Wait until `TXID' appears in `Node''s mempool map.
-spec tx_in_mempool(Node :: atom(), TXID :: binary()) ->
	ok | {error, {timeout, term()}}.
tx_in_mempool(Node, TXID) ->
	do_until_true(tx_in_mempool,
		fun() ->
			maps:is_key(TXID, on(Node, ar_mempool, get_map, []))
		end).

%% @doc Wait until `Node''s mempool no longer holds any TX IDs.
-spec mempool_drained(Node :: atom()) ->
	ok | {error, {timeout, term()}}.
mempool_drained(Node) ->
	do_until_true(mempool_drained,
		fun() ->
			on(Node, ar_mempool, get_all_txids, []) =:= []
		end).

%% @doc Wait until `TXID' has at least one block confirmation on `Node'.
-spec tx_confirmed(Node :: atom(), TXID :: binary()) ->
	ok | {error, {timeout, term()}}.
tx_confirmed(Node, TXID) ->
	do_until_true(tx_confirmed,
		fun() ->
			ar_test_node:get_tx_confirmations(Node, TXID) > 0
		end).

%%%===================================================================
%%% Mining / VDF state.
%%%===================================================================

%% @doc Wait until `Node''s mining server reports paused.
-spec mining_paused(Node :: atom()) ->
	ok | {error, {timeout, term()}}.
mining_paused(Node) ->
	do_until_true(mining_paused,
		fun() -> on(Node, ar_mining_server, is_paused, []) end).

%% @doc Wait until `Node''s `ar_nonce_limiter' has reached at least
%% `StepNumber'.
-spec vdf_step(Node :: atom(), StepNumber :: non_neg_integer()) ->
	ok | {error, {timeout, term()}}.
vdf_step(Node, StepNumber) ->
	do_until_true(vdf_step,
		fun() ->
			try on(Node, ar_nonce_limiter, get_current_step_number, []) of
				N when is_integer(N) -> N >= StepNumber;
				_ -> false
			catch
				exit:{timeout, _} -> false
			end
		end).

%%%===================================================================
%%% Peer / distribution state.
%%%===================================================================

%% @doc Wait until `Node''s HTTP `/info' endpoint responds with 200.
%% Re-fetches the port each iteration so a stale cached port can't lock
%% us onto the wrong peer.
-spec http_ready(Node :: atom()) ->
	ok | {error, {timeout, term()}}.
http_ready(Node) ->
	NodeName = ar_test_node:peer_name(Node),
	do_until_true(http_ready,
		fun() ->
			case (catch rpc:call(NodeName, arweave_config, get, [[port]], 2000)) of
				Port when is_integer(Port), Port > 0 ->
					case ar_http:req(#{
							method => get,
							peer => {127, 0, 0, 1, Port},
							path => "/info",
							timeout => 2000 }) of
						{ok, {{<<"200">>, _}, _, _, _, _}} -> true;
						_ -> false
					end;
				_ ->
					false
			end
		end).

%% @doc Wait until `NodeName' has left the local distribution.
-spec node_down(NodeName :: atom()) ->
	ok | {error, {timeout, term()}}.
node_down(NodeName) ->
	do_until_true(node_down,
		fun() -> not lists:member(NodeName, nodes()) end).

%% @doc Wait until `Node''s lifetime peer list contains `PeerIP'.
-spec peer_listed(Node :: atom(), PeerIP :: term()) ->
	ok | {error, {timeout, term()}}.
peer_listed(Node, PeerIP) ->
	do_until_true(peer_listed,
		fun() ->
			lists:member(PeerIP, on(Node, ar_peers, get_peers, [lifetime]))
		end).

%%%===================================================================
%%% Internal polling primitive.
%%%===================================================================

%% @doc Poll `Predicate' until it returns `true' or the standard timeout
%% elapses, using monotonic time so wall-clock jumps don't move the
%% deadline. Exceptions from `Predicate' propagate; wrap your own
%% try/catch to retry transient failures.
-spec until(Name :: term(), Predicate :: fun(() -> boolean())) ->
	ok | {error, {timeout, term()}}.
until(Name, Predicate) ->
	do_until_true(Name, Predicate).

%% @doc `until/2' with a custom timeout, for rare sub-100s waits.
-spec until(Name :: term(), Predicate :: fun(() -> boolean()),
		Timeout :: non_neg_integer()) ->
	ok | {error, {timeout, term()}}.
until(Name, Predicate, Timeout) ->
	do_until_true(Name, Predicate, Timeout).

%% Polls until the predicate signals success: `true' returns
%% `{ok, true}', `{true, Value}' returns `{ok, Value}', anything else
%% retries. On deadline returns `{error, {timeout, Name}}'.
do_until(Name, Predicate) ->
	do_until(Name, Predicate, ?TIMEOUT_MS).

do_until(Name, Predicate, Timeout) ->
	Deadline = erlang:monotonic_time(millisecond) + Timeout,
	do_until_loop(Name, Predicate, Deadline).

%% `do_until' for boolean predicates: collapses success to `ok'.
do_until_true(Name, Predicate) ->
	do_until_true(Name, Predicate, ?TIMEOUT_MS).

do_until_true(Name, Predicate, Timeout) ->
	case do_until(Name, Predicate, Timeout) of
		{ok, _} -> ok;
		E -> E
	end.

do_until_loop(Name, Predicate, Deadline) ->
	case Predicate() of
		true ->
			{ok, true};
		{true, Value} ->
			{ok, Value};
		_ ->
			case erlang:monotonic_time(millisecond) >= Deadline of
				true ->
					{error, {timeout, Name}};
				false ->
					timer:sleep(?POLL_INTERVAL_MS),
					do_until_loop(Name, Predicate, Deadline)
			end
	end.

%%%===================================================================
%%% Internal helpers.
%%%===================================================================

%% @doc Apply `M:F(A)' locally for `main', else via
%% `ar_test_node:remote_call/4' on `Node'.
on(main, M, F, A) ->
	apply(M, F, A);
on(Node, M, F, A) ->
	ar_test_node:remote_call(Node, M, F, A).


%% Predicate for `chunk_recorded/3'. Picks `ar_sync_record:is_recorded/2'
%% or `/3' by whether `Opts' restricts the store, tags the lookup with
%% the packing key when given, and treats any non-`false' reply as a hit.
is_chunk_recorded(Node, Offset, Opts) ->
	Tag = case maps:get(packing, Opts, any) of
		any -> ar_data_sync;
		Packing -> {ar_data_sync, Packing}
	end,
	Args = case maps:get(store_id, Opts, any) of
		any -> [Offset, Tag];
		StoreID -> [Offset, Tag, StoreID]
	end,
	on(Node, ar_sync_record, is_recorded, Args) =/= false.

%% Decode `Options'' serialized global sync record into an `ar_intervals'
%% set. Raises on a fetch or decode failure (a hard fault, not poll-again).
global_sync_record(Options) ->
	{ok, Binary} = ar_global_sync_record:get_serialized_sync_record(Options),
	{ok, Global} = ar_intervals:safe_from_etf(Binary),
	Global.

global_sync_record_to_list(Options) ->
	ar_intervals:to_list(global_sync_record(Options)).

data_roots_block_start(B) ->
	B#block.weave_size - B#block.block_size.

fetch_data_roots(Peer, Block) ->
	Start = data_roots_block_start(Block),
	case ar_http:req(#{
			method => get,
			peer => ar_test_node:peer_ip(Peer),
			path => "/data_roots/" ++ integer_to_list(Start) }) of
		{ok, {{<<"200">>, _}, _, Body, _, _}} -> {ok, Body};
		_ -> not_found
	end.

%% @doc True iff the union of `Node''s `/sync_record' intervals and its
%% replica_2_9 `/footprints/...' intervals fully contains
%% `[StartOffset, EndOffset)'. Raises on sync-record fetch failure (a
%% hard fault, not a poll-again condition).
has_range(Node, StartOffset, EndOffset) ->
	NodeIP = ar_test_node:peer_ip(Node),
	case ar_http_iface_client:get_sync_record(NodeIP) of
		{ok, RegularIntervals} ->
			FootprintIntervals = collect_footprint_intervals(
				NodeIP, StartOffset, EndOffset),
			AllIntervals = ar_intervals:union(RegularIntervals, FootprintIntervals),
			interval_contains(AllIntervals, StartOffset, EndOffset);
		Error ->
			erlang:error({sync_record_fetch_failed,
				[{node, Node}, {range, {StartOffset, EndOffset}}, {error, Error}]})
	end.

collect_footprint_intervals(NodeIP, StartOffset, EndOffset) ->
	StartPartition = ar_replica_2_9:get_entropy_partition(StartOffset + 1),
	LastPartition = ar_replica_2_9:get_entropy_partition(EndOffset + 1),
	FootprintsPerPartition = ar_footprint_record:get_footprints_per_partition(),
	collect_footprint_intervals(NodeIP, StartPartition, LastPartition,
		0, FootprintsPerPartition - 1, ar_intervals:new()).

collect_footprint_intervals(_NodeIP, Partition, LastPartition,
		_Footprint, _MaxFootprint, Acc) when Partition > LastPartition ->
	Acc;
collect_footprint_intervals(NodeIP, Partition, LastPartition,
		Footprint, MaxFootprint, Acc) when Footprint > MaxFootprint ->
	collect_footprint_intervals(NodeIP, Partition + 1, LastPartition,
		0, MaxFootprint, Acc);
collect_footprint_intervals(NodeIP, Partition, LastPartition,
		Footprint, MaxFootprint, Acc) ->
	FootprintByteIntervals =
		case ar_http_iface_client:get_footprints(NodeIP, Partition, Footprint) of
			{ok, FootprintIntervals} ->
				ar_footprint_record:get_intervals_from_footprint_intervals(
					FootprintIntervals);
			not_found ->
				?LOG_INFO([{event, footprint_record_not_found},
					{node_ip, NodeIP}, {partition, Partition},
					{footprint, Footprint}]),
				ar_intervals:new();
			Error ->
				erlang:error({footprint_fetch_failed,
					[{node_ip, NodeIP}, {partition, Partition},
					 {footprint, Footprint}, {error, Error}]})
		end,
	NewAcc = ar_intervals:union(Acc, FootprintByteIntervals),
	collect_footprint_intervals(NodeIP, Partition, LastPartition,
		Footprint + 1, MaxFootprint, NewAcc).

interval_contains(Intervals, Start, End) when End > Start ->
	case gb_sets:iterator_from({Start, Start}, Intervals) of
		Iter -> interval_contains2(Iter, Start, End)
	end.

interval_contains2(Iter, Start, End) ->
	case gb_sets:next(Iter) of
		none ->
			false;
		{{IntervalEnd, IntervalStart}, _}
				when IntervalStart =< Start andalso IntervalEnd >= End ->
			true;
		_ ->
			false
	end.
