-module(ar_downloader).
-behaviour(gen_server).

-export([start_link/1, enqueue_front/1, enqueue_random/1]).
-export([init/1, handle_cast/2, handle_call/3, terminate/2]).
-export([reset/0]).

-export([store_height_hash_index/1]).

-include("ar.hrl").
-include("ar_data_sync.hrl").

%% The frequency of processing items in the queue.
-define(PROCESS_ITEM_INTERVAL_MS, 150).
-define(INITIAL_BACKOFF_INTERVAL_S, 30).
%% The maximum exponential backoff interval for failing requests.
-define(MAX_BACKOFF_INTERVAL_S, 2 * 60 * 60).
%% The data above this size is synced chunk by chunk in ar_data_sync.
-define(SYNC_DATA_BELOW_SIZE, ?MAX_SERVED_TX_DATA_SIZE).
-ifdef(DEBUG).
-define(INTERVAL_IN_MINUTE(_), 50).
-else.
-define(INTERVAL_IN_MINUTE(Minutes), 1000 * 60 * Minutes).
-endif.
%%% This module contains the core transaction and block downloader.
%%% After the node has joined the network, this process is started,
%%% which continually downloads data until either the drive is full
%%% or the entire weave has been mirrored.

%%%===================================================================
%%% Public interface.
%%%===================================================================

start_link(_Args) ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

enqueue_front(Item) ->
	gen_server:cast(?MODULE, {enqueue_front, Item}).

enqueue_random(Item) ->
	gen_server:cast(?MODULE, {enqueue_random, Item}).

reset() ->
	gen_server:cast(?MODULE, reset).

store_height_hash_index(#block{ height = Height, indep_hash = IH }) ->
	gen_server:cast(?MODULE, {store_height_hash_index, {integer_to_binary(Height), IH}}),
	ok;
store_height_hash_index(_) ->
	{error, invalid_block}.

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================

init(_Args) ->
	ar:info([{event, ar_downloader_start}]),
	%% The message queue of this process is expected to be huge
	%% when the node joins. The flag makes VM store messages off
	%% heap and do not perform expensive GC on them.
	process_flag(message_queue_data, off_heap),
	gen_server:cast(?MODULE, process_item),
	gen_server:cast(?MODULE, init_stored_height_hash_index),
	gen_server:cast(?MODULE, stored_block_index),
	gen_server:cast(?MODULE, cleanup),
	{ok, #{ queue => queue:new() }}.

handle_cast({enqueue_front, Item}, #{ queue := Queue } = State) ->
	{noreply, State#{ queue => maybe_enqueue(Item, front, Queue) }};
handle_cast({enqueue_random, Item}, #{ queue := Queue } = State) ->
	{noreply, State#{ queue => maybe_enqueue(Item, random, Queue) }};

handle_cast(process_item, #{ queue := Queue } = State) ->
	prometheus_gauge:set(downloader_queue_size, queue:len(Queue)),
	UpdatedQueue = case is_full_disk() of
		true ->
			ar:warn([
				{event, downloader_process_item_disc_space_is_full},
				{state, State}
			]),
			Queue;
		false ->
			process_item(Queue)
	end,
	timer:apply_after(?PROCESS_ITEM_INTERVAL_MS, gen_server, cast, [?MODULE, process_item]),
	{noreply, State#{ queue => UpdatedQueue }};

handle_cast(cleanup, #{ stored_height_hash_index := DB } = State) ->
	start_cleanup(is_full_disk(), DB),
	timer:apply_after(?INTERVAL_IN_MINUTE(10), gen_server, cast, [?MODULE, cleanup]),
	{noreply, State};

handle_cast(init_stored_height_hash_index, State) ->
	Name = "stored_height_hash_index",
	RocksDBDir = filename:join(ar_meta_db:get(data_dir), ?ROCKS_DB_DIR),
	Filename = filename:join(RocksDBDir, Name),
	ok = filelib:ensure_dir(Filename ++ "/"),
	{ok, DB} = rocksdb:open(Filename, [{create_if_missing, true}, {max_open_files, 100000}]),
	{noreply, State#{ stored_height_hash_index => DB }};

handle_cast(stored_block_index, #{ stored_height_hash_index := DB } = State) ->
	case ar_node:get_block_index(whereis(http_entrypoint_node)) of
		[] ->
			timer:apply_after(500, gen_server, cast, [?MODULE, stored_block_index]);
		BI ->
			lists:foreach(fun({BH, _, _}) ->
				case ar_storage:read_block(BH) of
					#block{ height = Height, indep_hash = IH } ->
						rocksdb:put(DB, integer_to_binary(Height), IH, []);
					_ ->
						ok
				end
			end, BI)
	end,
	{noreply, State};

handle_cast({store_height_hash_index, {Key, Value}}, #{ stored_height_hash_index := DB } = State) ->
	rocksdb:put(DB, Key, Value, []),
	{noreply, State};

handle_cast(reset, State) ->
	{noreply, State#{ queue => queue:new() }}.

handle_call(get_stored_height_hash_index, _, #{ stored_height_hash_index := DB } = State) ->
	{reply, DB, State};

handle_call(_, _, State) ->
	{reply, not_implemented, State}.

terminate(Reason, #{ stored_height_hash_index := DB }) ->
	ar:info([{event, ar_downloader_terminate}, {reason, Reason}]),
	ar_kv:close(DB).

%%%===================================================================
%%% Private functions.
%%%===================================================================

maybe_enqueue(Item, Place, Queue) ->
	case has_item(Item) of
		false ->
			InsertedItem = case Item of
				{tx_data, TX} when is_record(TX, tx) ->
					{tx_data, {TX#tx.id, TX#tx.data_root}};
				_ ->
					Item
			end,
			case Place of
				front ->
					enqueue_front(InsertedItem, Queue);
				random ->
					enqueue_random(InsertedItem, Queue)
			end;
		true ->
			Queue
	end.

has_item({block, {H, _TXRoot}}) ->
	ar_storage:lookup_block_filename(H) /= unavailable;
has_item({tx, {ID, _BH}}) ->
	ar_storage:lookup_tx_filename(ID) /= unavailable;
has_item({tx_data, TX}) when is_record(TX, tx) ->
	not tx_needs_data(TX);
has_item({tx_data, {ID, _DataRoot}}) when is_binary(ID) ->
	filelib:is_file(ar_storage:tx_data_filepath(ID)).

tx_needs_data(#tx{ format = 1 }) ->
	false;
tx_needs_data(#tx{ format = 2, data_size = 0 }) ->
	false;
tx_needs_data(#tx{ format = 2, data_size = DataSize }) when DataSize > ?SYNC_DATA_BELOW_SIZE ->
	false;
tx_needs_data(#tx{ format = 2 } = TX) ->
	not filelib:is_file(ar_storage:tx_data_filepath(TX)).

enqueue_front(Item, Queue) ->
	queue:in_r({Item, initial_backoff()}, Queue).

enqueue_random(Item, Queue) ->
	enqueue_random(Item, initial_backoff(), Queue).

enqueue_random(Item, Backoff, Queue) ->
	%% Pick a position from [0, queue size].
	{Q1, Q2} = queue:split(rand:uniform(queue:len(Queue) + 1) - 1, Queue),
	queue:join(Q1, queue:in_r({Item, Backoff}, Q2)).

initial_backoff() ->
	{os:system_time(seconds) + ?INITIAL_BACKOFF_INTERVAL_S, ?INITIAL_BACKOFF_INTERVAL_S}.

process_item(Queue) ->
	Now = os:system_time(seconds),
	case queue:out(Queue) of
		{empty, _Queue} ->
			Queue;
		{{value, {Item, {BackoffTimestamp, _} = Backoff}}, UpdatedQueue}
				when BackoffTimestamp > Now ->
			enqueue_random(Item, Backoff, UpdatedQueue);
		{{value, {{block, {H, TXRoot}}, Backoff}}, UpdatedQueue} ->
			case download_block(H, TXRoot) of
				{error, _Reason} ->
					UpdatedBackoff = update_backoff(Now, Backoff),
					enqueue_random({block, {H, TXRoot}}, UpdatedBackoff, UpdatedQueue);
				{ok, TXIDs} ->
					Items = lists:map(fun(TXID) -> {tx, {TXID, H}} end, TXIDs),
					lists:foldl(
						fun(Item, Acc) ->
							maybe_enqueue(Item, front, Acc)
						end,
						UpdatedQueue,
						Items
					)
			end;
		{{value, {{tx, {ID, BH}}, Backoff}}, UpdatedQueue} ->
			case download_tx(ID, BH) of
				{error, _Reason} ->
					UpdatedBackoff = update_backoff(Now, Backoff),
					enqueue_random({tx, {ID, BH}}, UpdatedBackoff, UpdatedQueue);
				{ok, TX} ->
					maybe_enqueue({tx_data, TX}, front, UpdatedQueue)
			end;
		{{value, {{tx_data, {ID, DataRoot}}, Backoff}}, UpdatedQueue} when is_binary(ID) ->
			case download_transaction_data(ID, DataRoot) of
				{error, _Reason} ->
					UpdatedBackoff = update_backoff(Now, Backoff),
					enqueue_random({tx_data, {ID, DataRoot}}, UpdatedBackoff, UpdatedQueue);
				ok ->
					UpdatedQueue
			end
	end.

download_block(H, TXRoot) ->
	case ar_storage:read_block_shadow(H) of
		unavailable ->
			Peers = ar_bridge:get_remote_peers(whereis(http_bridge_node)),
			download_block(Peers, H, TXRoot);
		B ->
			{ok, B#block.txs}
	end.

update_backoff(Now, {_Timestamp, Interval}) ->
	UpdatedInterval = min(?MAX_BACKOFF_INTERVAL_S, Interval * 2),
	{Now + UpdatedInterval, UpdatedInterval}.

download_block(Peers, H, TXRoot) when is_binary(H) ->
	Fork_2_0 = ar_fork:height_2_0(),
	case ar_http_iface_client:get_block_shadow(Peers, H) of
		unavailable ->
			ar:warn([
				{event, downloader_failed_to_download_block_header},
				{block, ar_util:encode(H)}
			]),
			{error, block_header_unavailable};
		{Peer, #block{ height = Height } = B} when Height >= Fork_2_0 ->
			case ar_weave:indep_hash(B) of
				H ->
					write_block(B);
				_ ->
					ar:warn([
						{event, downloader_block_hash_mismatch},
						{block, ar_util:encode(H)},
						{peer, ar_util:format_peer(Peer)}
					]),
					{error, block_hash_mismatch}
			end;
		{Peer, B} ->
			case TXRoot of
				not_set ->
					write_block(B);
				_ ->
					case ar_http_iface_client:get_txs(Peers, #{}, B) of
						{ok, TXs} ->
							case ar_block:generate_tx_root_for_block(B#block{ txs = TXs }) of
								TXRoot ->
									write_block(B);
								_ ->
									ar:warn([
										{event, downloader_block_tx_root_mismatch},
										{block, ar_util:encode(H)},
										{peer, ar_util:format_peer(Peer)}
									]),
									{error, block_tx_root_mismatch}
							end;
						{error, txs_exceed_block_size_limit} ->
							ar:warn([
								{event, downloader_block_txs_exceed_block_size_limit},
								{block, ar_util:encode(H)},
								{peer, ar_util:format_peer(Peer)}
							]),
							{error, txs_exceed_block_size_limit};
						{error, tx_not_found} ->
							ar:warn([
								{event, downloader_block_tx_not_found},
								{block, ar_util:encode(H)},
								{peer, ar_util:format_peer(Peer)}
							]),
							{error, tx_not_found}
					end
			end
	end.

write_block(B) ->
	case ar_storage:write_block(B, do_not_write_wallet_list) of
		ok ->
			ar_arql_db:insert_block(B),
			{ok, B#block.txs};
		{error, Reason} = Error ->
			ar:warn([
				{event, downloader_failed_to_write_block},
				{block, ar_util:encode(B#block.indep_hash)},
				{reason, Reason}
			]),
			Error
	end.

download_tx(ID, BH) ->
	case ar_storage:read_tx(ID) of
		unavailable ->
			Peers = ar_bridge:get_remote_peers(whereis(http_bridge_node)),
			case ar_http_iface_client:get_tx_from_remote_peer(Peers, ID) of
				TX when is_record(TX, tx) ->
					case ar_tx:verify_tx_id(ID, TX) of
						false ->
							ar:warn([
								{event, downloader_tx_id_mismatch},
								{tx, ar_util:encode(ID)}
							]),
							{error, block_hash_mismatch};
						true ->
							case ar_storage:write_tx(TX) of
								ok ->
									StoreTags = case ar_meta_db:get(arql_tags_index) of
										true ->
											store_tags;
										_ ->
											do_not_store_tags
									end,
									ar_arql_db:insert_tx(BH, TX, StoreTags),
									{ok, TX};
								{error, Reason} = Error ->
									ar:warn([
										{event, downloader_failed_to_write_tx},
										{tx, ar_util:encode(ID)},
										{reason, Reason}
									]),
									Error
							end
					end;
				_ ->
					ar:warn([
						{event, downloader_failed_to_download_tx_header},
						{tx, ar_util:encode(ID)}
					]),
					{error, tx_header_unavailable}
			end;
		TX ->
			{ok, TX}
	end.

download_transaction_data(ID, DataRoot) ->
	Peers = ar_bridge:get_remote_peers(whereis(http_bridge_node)),
	case ar_http_iface_client:get_tx_data(Peers, ID) of
		unavailable ->
			ar:warn([
				{event, downloader_failed_to_download_tx_data},
				{tx, ar_util:encode(ID)}
			]),
			{error, tx_data_unavailable};
		Data ->
			case ar_storage:write_tx_data(DataRoot, Data) of
				ok ->
					ok;
				{error, invalid_data_root} ->
					%% Ignore this case as we could have gotten a TX with
					%% a custom split. Its data can be synced by ar_data_sync.
					%% ar_downloader is currently syncing data only to ensure
					%% a smooth migration to 2.1.
					ok;
				{error, Reason} = Error ->
					ar:warn([
						{event, downloader_failed_to_write_tx_data},
						{tx, ar_util:encode(ID)},
						{reason, Reason}
					]),
					Error
			end
	end.

start_cleanup(true, DB) ->
	{ok, Iterator} = rocksdb:iterator(DB, []),
	TillHeight = get_till_height(rocksdb:iterator_move(Iterator, last)),
	case rocksdb:iterator_move(Iterator, {seek, <<>>}) of
		{error,invalid_iterator} ->
			ok;
		{ok, Key, BH} ->
			KeepClean = 200 * 1000 * 1000,
			case ar_storage:lookup_block_filename(BH) of
				unavailable ->
					rocksdb:delete(DB, Key, []),
					cleanup(rocksdb:iterator_move(Iterator, next), DB, Iterator, KeepClean, 0, TillHeight);
				BlockPath ->
					case ar_storage:read_block(BH) of
						unavailable ->
							rocksdb:delete(DB, Key, []),
							cleanup(rocksdb:iterator_move(Iterator, next), DB, Iterator, KeepClean, 0, TillHeight);
						Block ->
							DeletedSize = cleanup_all(BlockPath, Block),
							rocksdb:delete(DB, Key, []),
							cleanup(rocksdb:iterator_move(Iterator, next), DB, Iterator, KeepClean, DeletedSize, TillHeight)
					end
			end
	end;
start_cleanup(_, _) ->
	ok.

cleanup({error,invalid_iterator}, _, _, _, _, _) ->
	ok;
cleanup({ok, Key, _}, _, _, _, _, Key) ->
	ok;
cleanup({ok, Key, BH}, Iterator, DB, Size, CurrentSize, TillHeight) ->
	case Size > CurrentSize of
		true ->
			case ar_storage:lookup_block_filename(BH) of
				unavailable ->
					rocksdb:delete(DB, Key, []),
					cleanup(rocksdb:iterator_move(Iterator, next), Iterator, DB, Size, CurrentSize, TillHeight);
				BlockPath ->
					case ar_storage:read_block(BH) of
						unavailable ->
							rocksdb:delete(DB, Key, []),
							cleanup(rocksdb:iterator_move(Iterator, next), Iterator, DB, Size, CurrentSize, TillHeight);
						Block ->
							DeletedSize = cleanup_all(BlockPath, Block),
							NewCurrentSize = CurrentSize + DeletedSize,
							rocksdb:delete(DB, Key, []),
							cleanup(rocksdb:iterator_move(Iterator, next), Iterator, DB, Size, NewCurrentSize, TillHeight)
					end
				end;
		false ->
			ok
	end.

cleanup_all(BlockPath, #block{ txs = TXs, wallet_list_hash = WalletListHash }) ->
	WalletLisPath = ar_storage:wallet_list_filepath(WalletListHash),
	WalletBlockSum = lists:foldr(fun(Path, Acc) ->
		NewAcc = Acc + filelib:file_size(Path),
		file:delete(Path),
		NewAcc
	end, 0, [WalletLisPath, BlockPath]),
	TXsSum = lists:foldr(fun(TX, Acc) ->
		TXPath = ar_storage:lookup_tx_filename(TX),
		NewAcc = Acc + filelib:file_size(TXPath),
		file:delete(TXPath),
		NewAcc
	end, 0, TXs),
	TXsSum + WalletBlockSum.

is_full_disk() ->
	{US, DS} = case {ar_meta_db:get(used_space), ar_meta_db:get(disk_space)} of
		{not_found, not_found} ->
			{0, 0};
		Res ->
			Res
	end,
	US + (100 * 1024 * 1024) >= DS.

get_till_height({ok, Height, _}) ->
	integer_to_binary(binary_to_integer(Height) - ?STORE_BLOCKS_BEHIND);
get_till_height(_) ->
	<<>>.
