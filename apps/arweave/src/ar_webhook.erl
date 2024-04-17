%% This Source Code Form is subject to the terms of the GNU General
%% Public License, v. 2.0. If a copy of the GPLv2 was not distributed
%% with this file, You can obtain one at
%% https://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html
%%
-module(ar_webhook).

-behaviour(gen_server).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include_lib("arweave/include/ar_config.hrl").

-define(NUMBER_OF_TRIES, 10).
-define(WAIT_BETWEEN_TRIES, 30 * 1000).

-define(BASE_HEADERS, [
	{<<"content-type">>, <<"application/json">>}
]).

-define(MAX_TX_OFFSET_CACHE_SIZE, 100_000). %% Max number of transactions cached

%% Functions which update or query the #tx_offset_cache are named with the `cache_` prefix
%% e.g. `cache_is_tx_data_marked_synced/2`.
-record(tx_offset_cache, {
	txid_to_timestamp = #{}, % Map TXID => Timestamp
	timestamp_txid = gb_sets:new(), % Ordered set of {Timestatmp, TXID}
	txid_to_start_offset_end_offset = #{}, % Map TXID => {Start, End}
	end_offset_txid_start_offset = gb_sets:new(), % Ordered set of {End, TXID, Start}
	size = 0,
	txid_to_status = #{} % Map TXID => synced | unsynced
}).

%% Internal state definition.
-record(state, {
	url,
	headers,

	listen_to_block_stream = false,
	listen_to_transaction_stream = false,
	listen_to_transaction_data_stream = false,

	tx_offset_cache = #tx_offset_cache{}
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
	gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%					   {ok, State, Timeout} |
%%					   ignore |
%%					   {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Hook) ->
	?LOG_DEBUG("Started web hook for ~p", [Hook]),
	State = lists:foldl(
		fun (transaction, Acc) ->
				ar_events:subscribe(tx),
				Acc#state{ listen_to_transaction_stream = true };
			(block, Acc) ->
				ok = ar_events:subscribe(block),
				Acc#state{ listen_to_block_stream = true };
			(transaction_data, Acc) ->
				ar_events:subscribe(tx),
				ok = ar_events:subscribe(sync_record),
				Acc#state{ listen_to_transaction_data_stream = true };
			(_, Acc) ->
				?LOG_WARNING("Wrong event name in webhook ~p", [Hook]),
				Acc
		end,
		#state{},
		Hook#config_webhook.events
	),
	State2 = State#state{
		url = Hook#config_webhook.url,
		headers = Hook#config_webhook.headers
	},
	{ok, State2}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%									 {reply, Reply, State} |
%%									 {reply, Reply, State, Timeout} |
%%									 {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, Reply, State} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
	?LOG_ERROR("unhandled call: ~p", [Request]),
	{reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%									{noreply, State, Timeout} |
%%									{stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
	?LOG_ERROR([{event, unhandled_cast}, {module, ?MODULE}, {message, Msg}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%									 {noreply, State, Timeout} |
%%									 {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({event, block, {new, Block, _Source}},
		#state{ listen_to_block_stream = true } = State) ->
	URL = State#state.url,
	Headers = State#state.headers,
	call_webhook(URL, Headers, Block, block),
	{noreply, State};

handle_info({event, block, _}, State) ->
	{noreply, State};

handle_info({event, tx, {new, TX, _Source}},
		#state{ listen_to_transaction_stream = true } = State) ->
	URL = State#state.url,
	Headers = State#state.headers,
	call_webhook(URL, Headers, TX, transaction),
	{noreply, State};

handle_info({event, tx, {registered_offset, TXID, EndOffset, Size}},
		#state{ listen_to_transaction_data_stream = true } = State) ->
	Start = EndOffset - Size,
	Cache = State#state.tx_offset_cache,
	case cache_is_tx_data_marked_synced(TXID, Cache) of
		true ->
			%% This event has already been processed for TXID, no need to process again
			{noreply, State};
		false ->
			Cache2 = cache_add_tx_offset_data([{TXID, Start, EndOffset}], Cache),
			State2 = State#state{ tx_offset_cache = Cache2 },
			{noreply,
				maybe_call_transaction_data_synced_webhook(Start, EndOffset, TXID,
						no_store_id, State2)}
	end;

handle_info({event, tx, {orphaned, TX}},
		#state{ listen_to_transaction_data_stream = true } = State) ->
	URL = State#state.url,
	Headers = State#state.headers,
	Payload = #{ event => transaction_orphaned, txid => ar_util:encode(TX#tx.id) },
	call_webhook(URL, Headers, Payload, transaction_orphaned),
	Cache = State#state.tx_offset_cache,
	Cache2 = cache_remove_tx_offset_data(TX#tx.id, Cache),
	{noreply, State#state{ tx_offset_cache = Cache2 }};

handle_info({event, tx, _}, State) ->
	{noreply, State};

handle_info({event, sync_record, {add_range, Start, End, ar_data_sync, StoreID}},
		#state{ listen_to_transaction_data_stream = true } = State) ->
	case StoreID of
		"default" ->
			%% The disk pool data is not guaranteed to stay forever so we
			%% do not want to push it here.
			{noreply, State};
		_ ->
			{noreply, handle_sync_record_add_range(Start, End, StoreID, State)}
	end;

handle_info({event, sync_record, {global_remove_range, Start, End}},
		#state{ listen_to_transaction_data_stream = true } = State) ->
	{noreply, handle_sync_record_remove_range(Start, End, State)};

handle_info({event, sync_record, _}, State) ->
	{noreply, State};

handle_info(Info, State) ->
	?LOG_ERROR([{event, unhandled_info}, {module, ?MODULE}, {info, Info}]),
	{noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
	ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

call_webhook(URL, Headers, Entity, Event) ->
	do_call_webhook(URL, Headers, Entity, Event, 0).

do_call_webhook(URL, Headers, Entity, Event, N) when N < ?NUMBER_OF_TRIES ->
	#{ host := Host, path := Path } = Map = uri_string:parse(URL),
	Query = maps:get(query, Map, <<>>),
	Peer = case maps:get(port, Map, undefined) of
		undefined ->
			case maps:get(scheme, Map, undefined) of
				"https" ->
					{binary_to_list(Host), 443};
				_ ->
					{binary_to_list(Host), 1984}
			end;
		Port ->
			{binary_to_list(Host), Port}
	end,
	case catch
		ar_http:req(#{
			method => post,
			peer => Peer,
			path => binary_to_list(<<Path/binary, Query/binary>>),
			headers => ?BASE_HEADERS ++ Headers,
			body => to_json(Entity),
			timeout => 10000,
			is_peer_request => false
		})
	of
		{ok, {{<<"200">>, _}, _, _, _, _}} = Result ->
			?LOG_INFO([
				{event, webhook_call_success},
				{webhook_event, Event},
				{id, entity_id(Entity)},
				{url, URL},
				{headers, Headers},
				{response, Result}
			]),
			ok;
		Error ->
			?LOG_ERROR([
				{event, webhook_call_failure},
				{webhook_event, Event},
				{id, entity_id(Entity)},
				{url, URL},
				{headers, Headers},
				{response, Error},
				{retry_in, ?WAIT_BETWEEN_TRIES}
			]),
			timer:sleep(?WAIT_BETWEEN_TRIES),
			do_call_webhook(URL, Headers, Entity, Event, N + 1)
	end;
do_call_webhook(URL, Headers, Entity, Event, _N) ->
	?LOG_WARNING([{event, gave_up_webhook_call},
		{webhook_event, Event},
		{id, entity_id(Entity)},
		{url, URL},
		{headers, Headers},
		{number_of_tries, ?NUMBER_OF_TRIES},
		{wait_between_tries, ?WAIT_BETWEEN_TRIES}
	]),
	ok.

entity_id(#block{ indep_hash = ID }) -> ar_util:encode(ID);
entity_id(#tx{ id = ID }) -> ar_util:encode(ID);
entity_id(#{ txid := TXID }) -> TXID.

to_json(#block{} = Block) ->
	{JSONKVPairs} = ar_serialize:block_to_json_struct(Block),
	JSONStruct = {lists:keydelete(wallet_list, 1, JSONKVPairs)},
	ar_serialize:jsonify({[{block, JSONStruct}]});
to_json(#tx{} = TX) ->
	{JSONKVPairs1} = ar_serialize:tx_to_json_struct(TX),
	JSONKVPairs2 = lists:keydelete(data, 1, JSONKVPairs1),
	JSONKVPairs3 = [{data_size, TX#tx.data_size} | JSONKVPairs2],
	JSONStruct = {JSONKVPairs3},
	ar_serialize:jsonify({[{transaction, JSONStruct}]});
to_json(Map) when is_map(Map) ->
	jiffy:encode(Map).

handle_sync_record_add_range(Start, End, StoreID, State) ->
	handle_sync_record_add_range(Start, End, StoreID, State, 0).

handle_sync_record_add_range(Start, End, StoreID, State, N) when N < ?NUMBER_OF_TRIES ->
	case get_tx_offset_data(Start, End, State#state.tx_offset_cache) of
		{ok, Data, Cache} ->
			process_updated_tx_data(Data, StoreID, State#state{ tx_offset_cache = Cache });
		not_found ->
			State;
		Error ->
			?LOG_WARNING([{event, failed_to_process_webhook_sync_record_add_range},
					{error, io_lib:format("~p", [Error])},
					{range_start, Start},
					{range_end, End}]),
			timer:sleep(?WAIT_BETWEEN_TRIES),
			handle_sync_record_add_range(Start, End, State, N + 1)
	end;
handle_sync_record_add_range(Start, End, _StoreID, State, _N) ->
	?LOG_WARNING([{event, gave_up_webhook_tx_offset_data_fetch},
		{range_start, Start},
		{range_end, End},
		{number_of_tries, ?NUMBER_OF_TRIES},
		{wait_between_tries, ?WAIT_BETWEEN_TRIES}
	]),
	State.

process_updated_tx_data([], _StoreID, State) ->
	State;
process_updated_tx_data([{TXID, Start, End} | Data], StoreID, State) ->
	Cache = State#state.tx_offset_cache,
	case cache_is_tx_data_marked_synced(TXID, Cache) of
		true ->
			%% transaction_data_synced webhook has already been sent, advance to next range
			process_updated_tx_data(Data, StoreID, State);
		false ->
			%% Maybe send the transaction_data_synced webhook before advancing to the next range
			process_updated_tx_data(Data, StoreID,
				maybe_call_transaction_data_synced_webhook(Start, End, TXID, StoreID, State))
	end.

%% @doc Return a list of {TXID, Start2, End2} for recorded transactions from the given range.
get_tx_offset_data(Start, End, Cache) ->
	case cache_get_tx_offset_data(Start, End, Cache) of
		[] ->
			case ar_data_sync:get_tx_offset_data_in_range(Start, End) of
				{ok, []} ->
					not_found;
				{ok, Data} ->
					Cache2 = cache_add_tx_offset_data(Data, Cache),
					{ok, Data, Cache2};
				Error ->
					Error
			end;
		List ->
			{ok, List, Cache}
	end.

cache_get_tx_offset_data(Start, End, Cache) ->
	#tx_offset_cache{
		end_offset_txid_start_offset = Set
	} = Cache,
	%% 32-byte TXID > atom n => we choose any element with End at least Start + 1.
	Iterator = gb_sets:iterator_from({Start + 1, n, n}, Set),
	cache_get_tx_offset_data_from_iterator(End, Iterator).

cache_get_tx_offset_data_from_iterator(End, Iterator) ->
	case gb_sets:next(Iterator) of
		none ->
			[];
		{{End2, TXID, Start2}, Iterator2} when Start2 < End ->
			[{TXID, Start2, End2} | cache_get_tx_offset_data_from_iterator(End, Iterator2)];
		_ ->
			[]
	end.

cache_add_tx_offset_data([], Cache) ->
	Cache;
cache_add_tx_offset_data([{TXID, Start, End} | Data], Cache) ->
	#tx_offset_cache{
		txid_to_timestamp = Map,
		timestamp_txid = Set,
		end_offset_txid_start_offset = OffsetSet,
		txid_to_start_offset_end_offset = OffsetMap,
		txid_to_status = StatusMap,
		size = Size
	} = Cache,
	Timestamp = erlang:monotonic_time(microsecond),
	Set2 =
		case maps:get(TXID, Map, not_found) of
			not_found ->
				Set;
			PrevTimestamp ->
				gb_sets:del_element({PrevTimestamp, TXID}, Set)
		end,
	Map2 = maps:put(TXID, Timestamp, Map),
	check_offset_set_consistency(Start, End, TXID, OffsetSet),
	Set3 = gb_sets:add_element({Timestamp, TXID}, Set2),
	OffsetSet2 = gb_sets:add_element({End, TXID, Start}, OffsetSet),
	OffsetMap2 =
		case maps:get(TXID, OffsetMap, not_found) of
			not_found ->
				maps:put(TXID, {Start, End}, OffsetMap);
			{Start, End} ->
				OffsetMap;
			{Start2, End2} ->
				?LOG_WARNING([{event, inconsistent_tx_offset_cache},
						{check, txid_to_start_offset_end_offset},
						{cached_tx_start_offset, Start2},
						{cached_tx_end_offset, End2},
						{tx_start_offset, Start},
						{tx_end_offset, End},
						{txid, ar_util:encode(TXID)}]),
				maps:put(TXID, {Start, End}, OffsetMap)
		end,
	Cache2 = Cache#tx_offset_cache{
		txid_to_timestamp = Map2,
		timestamp_txid = Set3,
		end_offset_txid_start_offset = OffsetSet2,
		txid_to_start_offset_end_offset = OffsetMap2,
		txid_to_status = maps:remove(TXID, StatusMap),
		size = Size + 1
	},
	Cache3 = maybe_remove_oldest(Cache2),
	cache_add_tx_offset_data(Data, Cache3).

check_offset_set_consistency(Start, End, TXID, OffsetSet) ->
	%% 32-byte TXID > atom n => we choose any element with End at least Start + 1.
	Iterator = gb_sets:iterator_from({Start + 1, n, n}, OffsetSet),
	check_offset_set_consistency2(Start, End, TXID, Iterator).

check_offset_set_consistency2(Start, End, TXID, Iterator) ->
	case gb_sets:next(Iterator) of
		none ->
			ok;
		{{End2, TXID2, Start2}, _Iterator2} when Start2 < End ->
			?LOG_WARNING([{event, inconsistent_tx_offset_cache},
					{check, end_offset_txid_start_offset},
					{cached_tx_start_offset, Start2},
					{cached_tx_end_offset, End2},
					{cached_txid, ar_util:encode(TXID2)},
					{tx_start_offset, Start},
					{tx_end_offset, End},
					{txid, ar_util:encode(TXID)}]);
		{_, Iterator2} ->
			check_offset_set_consistency2(Start, End, TXID, Iterator2)
	end.

maybe_remove_oldest(Cache) ->
	#tx_offset_cache{
		txid_to_timestamp = Map,
		timestamp_txid = Set,
		end_offset_txid_start_offset = OffsetSet,
		txid_to_start_offset_end_offset = OffsetMap,
		txid_to_status = StatusMap,
		size = Size
	} = Cache,
	case Size > ?MAX_TX_OFFSET_CACHE_SIZE of
		false ->
			Cache;
		true ->
			{{_Timestamp, TXID}, Set2} = gb_sets:take_smallest(Set),
			{Start, End} = maps:get(TXID, OffsetMap),
			OffsetSet2 = gb_sets:del_element({End, TXID, Start}, OffsetSet),
			Cache#tx_offset_cache{
				txid_to_timestamp = maps:remove(TXID, Map),
				timestamp_txid = Set2,
				end_offset_txid_start_offset = OffsetSet2,
				txid_to_start_offset_end_offset = maps:remove(TXID, OffsetMap),
				txid_to_status = maps:remove(TXID, StatusMap),
				size = Size - 1
			}
	end.

cache_remove_tx_offset_data(TXID, Cache) ->
	#tx_offset_cache{
	   txid_to_timestamp = Map,
	   timestamp_txid = Set,
	   end_offset_txid_start_offset = OffsetSet,
	   txid_to_start_offset_end_offset = OffsetMap,
	   size = Size,
	   txid_to_status = StatusMap
	} = Cache,
	case maps:get(TXID, Map, not_found) of
		not_found ->
			Cache;
		Timestamp ->
			Set2 = gb_sets:del_element({Timestamp, TXID}, Set),
			Map2 = maps:remove(TXID, Map),
			Size2 = Size - 1,
			{Start, End} = maps:get(TXID, OffsetMap),
			OffsetSet2 = gb_sets:del_element({End, TXID, Start}, OffsetSet),
			OffsetMap2 = maps:remove(TXID, OffsetMap),
			StatusMap2 = maps:remove(TXID, StatusMap),
			Cache#tx_offset_cache{
				txid_to_timestamp = Map2,
				timestamp_txid = Set2,
				end_offset_txid_start_offset = OffsetSet2,
				txid_to_start_offset_end_offset = OffsetMap2,
				size = Size2,
				txid_to_status = StatusMap2
			}
	end.

cache_mark_tx_data_synced(TXID, Cache) ->
	#tx_offset_cache{ txid_to_status = Map } = Cache,
	Cache#tx_offset_cache{ txid_to_status = maps:put(TXID, synced, Map) }.

cache_is_tx_data_marked_synced(TXID, Cache) ->
	maps:get(TXID, Cache#tx_offset_cache.txid_to_status, false) == synced.

cache_mark_tx_data_unsynced(TXID, Cache) ->
	#tx_offset_cache{ txid_to_status = Map } = Cache,
	Cache#tx_offset_cache{ txid_to_status = maps:put(TXID, unsynced, Map) }.

cache_is_tx_data_marked_unsynced(TXID, Cache) ->
	maps:get(TXID, Cache#tx_offset_cache.txid_to_status, false) == unsynced.

maybe_call_transaction_data_synced_webhook(Start, End, TXID, MaybeStoreID, State) ->
	Cache = State#state.tx_offset_cache,
	Cache2 =
		case is_synced_by_storage_modules(Start, End, MaybeStoreID) of
			true ->
				URL = State#state.url,
				Headers = State#state.headers,
				Payload = #{ event => transaction_data_synced,
						txid => ar_util:encode(TXID) },
				call_webhook(URL, Headers, Payload, transaction_data_synced),
				cache_mark_tx_data_synced(TXID, Cache);
			false ->
				Cache
		end,
	State#state{ tx_offset_cache = Cache2 }.

is_synced_by_storage_modules(Start, End, StoreID) ->
	case ar_storage_module:get_cover(Start, End, StoreID) of
		not_found ->
			false;
		Intervals ->
			is_synced_by_storage_modules(Intervals)
	end.

is_synced_by_storage_modules([]) ->
	true;
is_synced_by_storage_modules([{Start, End, StoreID} | Intervals]) ->
	case ar_sync_record:get_next_unsynced_interval(Start, End, ar_data_sync, StoreID) of
		not_found ->
			is_synced_by_storage_modules(Intervals);
		_I ->
			false
	end.

handle_sync_record_remove_range(Start, End, State) ->
	handle_sync_record_remove_range(Start, End, State, 0).

handle_sync_record_remove_range(Start, End, State, N) when N < ?NUMBER_OF_TRIES ->
	case get_tx_offset_data(Start, End, State#state.tx_offset_cache) of
		{ok, Data, Cache} ->
			process_removed_tx_data(Data, State#state{ tx_offset_cache = Cache });
		not_found ->
			State;
		Error ->
			?LOG_WARNING([{event, failed_to_process_webhook_sync_record_remove_range},
					{error, io_lib:format("~p", [Error])},
					{range_start, Start},
					{range_end, End}]),
			timer:sleep(?WAIT_BETWEEN_TRIES),
			handle_sync_record_remove_range(Start, End, State, N + 1)
	end;
handle_sync_record_remove_range(Start, End, State, _N) ->
	?LOG_WARNING([{event, gave_up_webhook_tx_offset_data_fetch},
		{range_start, Start},
		{range_end, End},
		{number_of_tries, ?NUMBER_OF_TRIES},
		{wait_between_tries, ?WAIT_BETWEEN_TRIES}
	]),
	State.

process_removed_tx_data([], State) ->
	State;
process_removed_tx_data([{TXID, _Start, _End} | Data], State) ->
	Cache = State#state.tx_offset_cache,
	case cache_is_tx_data_marked_unsynced(TXID, Cache) of
		true ->
			process_removed_tx_data(Data, State);
		false ->
			URL = State#state.url,
			Headers = State#state.headers,
			Payload = #{ event => transaction_data_removed, txid => ar_util:encode(TXID) },
			call_webhook(URL, Headers, Payload, transaction_data_removed),
			Cache2 = cache_mark_tx_data_unsynced(TXID, Cache),
			State2 = State#state{ tx_offset_cache = Cache2 },
			process_removed_tx_data(Data, State2)
	end.
