-module(ar_mempool).

-include_lib("arweave/include/ar.hrl").

-export([reset/0, load_from_disk/0, add_tx/2, drop_txs/1, drop_txs/3,
		get_map/0, get_all_txids/0, take_chunk/2, get_tx/1, has_tx/1, 
		get_priority_set/0, get_last_tx_map/0, get_origin_tx_map/0,
		get_propagation_queue/0, del_from_propagation_queue/2]).

reset() ->
	ets:insert(node_state, [
		{mempool_size, {0, 0}},
		{tx_priority_set, gb_sets:new()},
		{tx_propagation_queue, gb_sets:new()},
		{last_tx_map, maps:new()},
		{origin_tx_map, maps:new()}
	]).

load_from_disk() ->
	case ar_storage:read_term(mempool) of
		{ok, {SerializedTXs, _MempoolSize}} ->
			TXs = maps:map(fun(_, {TX, St}) -> {deserialize_tx(TX), St} end, SerializedTXs),

			{MempoolSize2, PrioritySet2, PropagationQueue2, LastTXMap2, OriginTXMap2} = 
				maps:fold(
					fun(TXID, {TX, Status}, {MempoolSize, PrioritySet, PropagationQueue, LastTXMap, OriginTXMap}) ->
						MetaData = {_, _, Timestamp} = init_tx_metadata(TX, Status),
						ets:insert(node_state, {{tx, TXID}, MetaData}),
						ets:insert(tx_prefixes, {ar_node_worker:tx_id_prefix(TXID), TXID}),
						Q = case Status of
							ready_for_mining ->
								PropagationQueue;
							_ ->
								add_to_propagation_queue(PropagationQueue, TX, Timestamp)
						end,
						{
							increase_mempool_size(MempoolSize, TX),
							add_to_priority_set(PrioritySet, TX, Status, Timestamp),
							Q,
							add_to_last_tx_map(LastTXMap, TX),
							add_to_origin_tx_map(OriginTXMap, TX)
						}
					end,
					{
						{0, 0},
						gb_sets:new(),
						gb_sets:new(),
						maps:new(),
						maps:new()
					},
					TXs
				),

			ets:insert(node_state, [
				{mempool_size, MempoolSize2},
				{tx_priority_set, PrioritySet2},
				{tx_propagation_queue, PropagationQueue2},
				{last_tx_map, LastTXMap2},
				{origin_tx_map, OriginTXMap2}
			]);
		not_found ->
			reset();
		{error, Error} ->
			?LOG_ERROR([{event, failed_to_load_mempool}, {reason, Error}]),
			reset()
	end.

add_tx(#tx{ id = TXID } = TX, Status) ->
	{MetaData, MempoolSize, PrioritySet, PropagationQueue, LastTXMap, OriginTXMap} =
		case get_tx_metadata(TXID) of
			not_found ->
				{_, _, Timestamp} = init_tx_metadata(TX, Status),
				ets:insert(tx_prefixes, {ar_node_worker:tx_id_prefix(TXID), TXID}),
				{
					{TX, Status, Timestamp},
					increase_mempool_size(get_mempool_size(), TX),
					add_to_priority_set(get_priority_set(), TX, Status, Timestamp),
					add_to_propagation_queue(get_propagation_queue(), TX, Timestamp),
					add_to_last_tx_map(get_last_tx_map(), TX),
					add_to_origin_tx_map(get_origin_tx_map(), TX)
				};
			{TX, PrevStatus, Timestamp} -> 
				{
					{TX, Status, Timestamp},
					get_mempool_size(),
					add_to_priority_set(get_priority_set(),TX, PrevStatus, Status, Timestamp),
					get_propagation_queue(),
					get_last_tx_map(),
					get_origin_tx_map()
				}
		end,
	% Insert all data at the same time to ensure atomicity
	ets:insert(node_state, [
		{{tx, TXID}, MetaData},
		{mempool_size, MempoolSize},
		{tx_priority_set, PrioritySet},
		{tx_propagation_queue, PropagationQueue},
		{last_tx_map, LastTXMap},
		{origin_tx_map, OriginTXMap}
	]),
	
	case ar_node:is_joined() of
		true ->
			% 1. Drop unconfirmable transactions:
			%    - those with clashing last_tx
			%    - those which overspend an account
			% 2. If the mempool is too large, drop low priority transactions until the
			%    mempool is small enough
			% To limit revalidation work, all of these checks assume every TX in the
			% mempool has previously been validated.
			drop_txs(find_clashing_txs(TX)),
			drop_txs(find_overspent_txs(TX)),
			drop_txs(find_low_priority_txs(gb_sets:iterator(PrioritySet), MempoolSize));
		false ->
			noop
	end,
	ok.

drop_txs(DroppedTXs) ->
	drop_txs(DroppedTXs, true, true).
drop_txs([], _RemoveTXPrefixes, _DropFromDiskPool) ->
	ok;
drop_txs(DroppedTXs, RemoveTXPrefixes, DropFromDiskPool) ->
	{MempoolSize2, PrioritySet2, PropagationQueue2, LastTXMap2, OriginTXMap2} =
		lists:foldl(
			fun(TX, {MempoolSize, PrioritySet, PropagationQueue, LastTXMap, OriginTXMap}) ->
				TXID = TX#tx.id,
				case get_tx_metadata(TXID) of
					not_found ->
						{MempoolSize, PrioritySet, PropagationQueue, LastTXMap, OriginTXMap};

					{_, Status, Timestamp} ->
						ets:delete(node_state, {tx, TXID}),
						case RemoveTXPrefixes of
							true ->
								ets:delete_object(tx_prefixes, {ar_node_worker:tx_id_prefix(TXID), TXID});
							false ->
								ok
						end,
						case DropFromDiskPool of
							true ->
								may_be_drop_from_disk_pool(TX);
							false ->
								ok
						end,
						{
							decrease_mempool_size(MempoolSize, TX),
							del_from_priority_set(PrioritySet, TX, Status, Timestamp),
							del_from_propagation_queue(PropagationQueue, TX, Timestamp),
							del_from_last_tx_map(LastTXMap, TX),
							del_from_origin_tx_map(OriginTXMap, TX)
						}
				end
			end,
			{
				get_mempool_size(),
				get_priority_set(),
				get_propagation_queue(),
				get_last_tx_map(),
				get_origin_tx_map()
			},
			DroppedTXs
		),
	ets:insert(node_state, [
		{mempool_size, MempoolSize2},
		{tx_priority_set, PrioritySet2},
		{tx_propagation_queue, PropagationQueue2},
		{last_tx_map, LastTXMap2},
		{origin_tx_map, OriginTXMap2}
	]).

get_map() ->
	gb_sets:fold(
		fun({_Utility, TXID, Status}, Acc) ->
			Acc#{TXID => Status}
		end,
		#{},
		get_priority_set()
	).

get_all_txids() ->
	gb_sets:fold(
		fun({_Utility, TXID, _Status}, Acc) ->
			[TXID | Acc]
		end,
		[],
		get_priority_set()
	).

take_chunk(Mempool, Size) ->
	take_chunk(Mempool, Size, []).
take_chunk(Mempool, 0, Taken) ->
	{ok, Taken, Mempool};
take_chunk([], _Size, Taken) ->
	{ok, Taken, []};
take_chunk(Mempool, Size, Taken) ->
	TXID = lists:last(Mempool),
	RemainingMempool = lists:droplast(Mempool),
	case get_tx(TXID) of
		not_found ->
			take_chunk(RemainingMempool, Size, Taken);
		TX ->
			take_chunk(RemainingMempool, Size - 1, [TX | Taken])
	end.

get_tx_metadata(TXID) ->
	case ets:lookup(node_state, {tx, TXID}) of
		[{_, {TX, Status, Timestamp}}] ->
			{TX, Status, Timestamp};
		_ ->
			not_found
	end.

get_tx(TXID) ->
	case get_tx_metadata(TXID) of
		not_found ->
			not_found;
		{TX, _Status, _Timestamp} ->
			TX
	end.

has_tx(TXID) ->
	ets:member(node_state, {tx, TXID}).

get_priority_set() ->
	case ets:lookup(node_state, tx_priority_set) of
		[{tx_priority_set, Set}] ->
			Set;
		_ ->
			gb_sets:new()
	end.

get_propagation_queue() ->
	case ets:lookup(node_state, tx_propagation_queue) of
		[{tx_propagation_queue, Q}] ->
			Q;
		_ ->
			gb_sets:new()
	end.

get_last_tx_map() ->
	case ets:lookup(node_state, last_tx_map) of
		[{last_tx_map, Map}] ->
			Map;
		_ ->
			maps:new()
	end.

get_origin_tx_map() ->
	case ets:lookup(node_state, origin_tx_map) of
		[{origin_tx_map, Map}] ->
			Map;
		_ ->
			maps:new()
	end.


del_from_propagation_queue(Priority, TXID) ->
	ets:insert(node_state, {
		tx_propagation_queue,
		del_from_propagation_queue(ar_mempool:get_propagation_queue(), Priority, TXID)
	}).
del_from_propagation_queue(PropagationQueue, TX = #tx{}, Timestamp) ->
	Priority = {ar_tx:utility(TX), Timestamp},
	del_from_propagation_queue(PropagationQueue, Priority, TX#tx.id);
del_from_propagation_queue(PropagationQueue, Priority, TXID)
	when is_bitstring(TXID) ->
	gb_sets:del_element({Priority, TXID}, PropagationQueue).


%% ------------------------------------------------------------------
%% Private Functions
%% ------------------------------------------------------------------

get_mempool_size() ->
	case ets:lookup(node_state, mempool_size) of
		[{mempool_size, MempoolSize}] ->
			MempoolSize;
		_ ->
			{0, 0}
	end.

init_tx_metadata(TX, Status) ->
	{TX, Status, -os:system_time(microsecond)}.

add_to_priority_set(PrioritySet, TX, Status, Timestamp) ->
	Priority = {ar_tx:utility(TX), Timestamp},
	gb_sets:add_element({Priority, TX#tx.id, Status}, PrioritySet).

add_to_priority_set(PrioritySet, TX, PrevStatus, Status, Timestamp) ->
	Priority = {ar_tx:utility(TX), Timestamp},
	gb_sets:add_element({Priority, TX#tx.id, Status},
		gb_sets:del_element({Priority, TX#tx.id, PrevStatus},
			PrioritySet
		)
	).

del_from_priority_set(PrioritySet, TX, Status, Timestamp) ->
	Priority = {ar_tx:utility(TX), Timestamp},
	gb_sets:del_element({Priority, TX#tx.id, Status}, PrioritySet).

add_to_propagation_queue(PropagationQueue, TX, Timestamp) ->
	Priority = {ar_tx:utility(TX), Timestamp},
	gb_sets:add_element({Priority, TX#tx.id}, PropagationQueue).

%% @doc Store a map of last_tx TXIDs to a priority set of TXs that use
%% that last_tx. We actually store the TXIDs of the TXs to avoid bloating
%% the ets table. The trade off is that we have to do a TXID to TX lookup
%% when resolving last_tx clashes.
add_to_last_tx_map(LastTXMap, TX) ->
	Element = unconfirmed_tx(TX),
	Set2 = case maps:get(TX#tx.last_tx, LastTXMap, not_found) of
		not_found ->
			gb_sets:from_list([Element]);
		Set ->
			gb_sets:add_element(Element, Set)
	end,
	maps:put(TX#tx.last_tx, Set2, LastTXMap).

del_from_last_tx_map(LastTXMap, TX) ->
	Element = unconfirmed_tx(TX),
	case maps:get(TX#tx.last_tx, LastTXMap, not_found) of
		not_found ->
			LastTXMap;
		Set ->
			maps:put(TX#tx.last_tx, gb_sets:del_element(Element, Set), LastTXMap)
	end.

%% @doc Store a map of addresses to a priority set of TXs that spend
%% from that address. We actually store the TXIDs of the TXs to avoid bloating
%% the ets table. The trade off is that we have to do a TXID to TX lookup
%% when resolving overspends.
add_to_origin_tx_map(OriginTXMap, TX) ->
	Element = unconfirmed_tx(TX),
	Origin = ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type),
	Set2 = case maps:get(Origin, OriginTXMap, not_found) of
		not_found ->
			gb_sets:from_list([Element]);
		Set ->
			gb_sets:add_element(Element, Set)
	end,
	maps:put(Origin, Set2, OriginTXMap).

del_from_origin_tx_map(OriginTXMap, TX) ->
	Element = unconfirmed_tx(TX),
	Origin = ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type),
	case maps:get(Origin, OriginTXMap, not_found) of
		not_found ->
			OriginTXMap;
		Set ->
			maps:put(Origin, gb_sets:del_element(Element, Set), OriginTXMap)
	end.

unconfirmed_tx(TX = #tx{}) ->
	{ar_tx:utility(TX), TX#tx.id}.
	

increase_mempool_size(
	_MempoolSize = {MempoolHeaderSize, MempoolDataSize}, TX = #tx{}) ->
	{HeaderSize, DataSize} = tx_mempool_size(TX),
	{MempoolHeaderSize + HeaderSize, MempoolDataSize + DataSize}.

decrease_mempool_size(
	_MempoolSize = {MempoolHeaderSize, MempoolDataSize}, TX = #tx{}) ->
	{HeaderSize, DataSize} = tx_mempool_size(TX),
	{MempoolHeaderSize - HeaderSize, MempoolDataSize - DataSize}.

tx_mempool_size(#tx{ format = 1, data = Data }) ->
	{?TX_SIZE_BASE + byte_size(Data), 0};
tx_mempool_size(#tx{ format = 2, data = Data }) ->
	{?TX_SIZE_BASE, byte_size(Data)}.

deserialize_tx(Bin) when is_binary(Bin) ->
	{ok, TX} = ar_serialize:binary_to_tx(Bin),
	TX;
deserialize_tx(TX) ->
	ar_storage:migrate_tx_record(TX).



may_be_drop_from_disk_pool(#tx{ format = 1 }) ->
	ok;
may_be_drop_from_disk_pool(TX) ->
	ar_data_sync:maybe_drop_data_root_from_disk_pool(TX#tx.data_root, TX#tx.data_size,
			TX#tx.id).


find_low_priority_txs(Iterator, {MempoolHeaderSize, MempoolDataSize})
		when
			MempoolHeaderSize > ?MEMPOOL_HEADER_SIZE_LIMIT;
			MempoolDataSize > ?MEMPOOL_DATA_SIZE_LIMIT ->
	{{_Utility, TXID, _Status} = _Element, Iterator2} = gb_sets:next(Iterator),
	TX = get_tx(TXID),
	case should_drop_low_priority_tx(TX, {MempoolHeaderSize, MempoolDataSize}) of
		true ->
			MempoolSize2 = decrease_mempool_size({MempoolHeaderSize, MempoolDataSize}, TX),
			[TX | find_low_priority_txs(Iterator2, MempoolSize2)];
		false ->
			find_low_priority_txs(Iterator2, {MempoolHeaderSize, MempoolDataSize})
	end;
find_low_priority_txs(_Iterator, {_MempoolHeaderSize, _MempoolDataSize}) ->
	[].

should_drop_low_priority_tx(_TX, {MempoolHeaderSize, _MempoolDataSize})
		when MempoolHeaderSize > ?MEMPOOL_HEADER_SIZE_LIMIT ->
	true;
should_drop_low_priority_tx(TX, {_MempoolHeaderSize, MempoolDataSize})
		when MempoolDataSize > ?MEMPOOL_DATA_SIZE_LIMIT ->
	TX#tx.format == 2 andalso byte_size(TX#tx.data) > 0;
should_drop_low_priority_tx(_TX, {_MempoolHeaderSize, _MempoolDataSize}) ->
	false.


%% @doc identify any transactions that refer to the same last_tx
%% (where last_tx is the last confirmed transaction in the wallet).
%% Only 1 of these transactions will confirm, so we want to drop
%% all the others to prevent a mempool spam attack.
find_clashing_txs(#tx{ last_tx = <<>> }) ->
	[];
find_clashing_txs(TX = #tx{}) ->
	Wallets = ar_wallets:get(ar_tx:get_addresses([TX])),
	find_clashing_txs(TX, Wallets).

find_clashing_txs(TX = #tx{}, Wallets) when is_map(Wallets) ->
	case ar_tx:check_last_tx(Wallets, TX) of
		true ->
			ClashingTXIDs = maps:get(TX#tx.last_tx, get_last_tx_map(), gb_sets:new()),
			filter_clashing_txs(ClashingTXIDs);
		_ ->
			[]
	end;
find_clashing_txs(_TX, _Wallets) ->
	[].

%% @doc Only the highest priority TX will be kept, others will be dropped.
%% Priority is defined as:
%% 1. ar_tx:utility
%% 2. alphanumeric order of TXID (z is higher priority than a)
%%
%% Adding the TXID term to the priority calculation (rather than local
%% timestamp), ensures that the sorting will be stable and deterministic
%% across peers and so all peers will drop the same clashing TXs regardless
%% of the order in which the transactions are received.
filter_clashing_txs(ClashingTXIDs) ->
	case gb_sets:is_empty(ClashingTXIDs) of
		true ->
			[];
		false ->
			% Exclude the highest priority TX from the list of TXs to be dropped
			{_, UncomfirmableTXIDs} = gb_sets:take_largest(ClashingTXIDs),
			to_txs(UncomfirmableTXIDs)
	end.

%% @doc identify any transactions that would overspend an account if
%% they were to be confirmed. Since those transactions won't confirm
%% we want to drop them to prevent a mempool spam attack (e.g.
%% an attacker posts hundreds or thousands of overspend transactions
%% which saturate the mempool, but for which only 1 will ever be
%% confirmed)
%%
%% Note: when doing the overspend calculation any unconfirmed deposit
%% transactions are ignored. This is to prevent a second potentially 
%% malicious scenario like the following:
%%
%% Peer A: receives deposit TX and several spend TXs,
%%         all TXs are added to the mempool
%% Peer B: receives only the spend TXs, and all are dropped from the mempool
%% Peer A: publishes block
%% Peer B: needs to request potentially many TXs from peer A since their
%%         mempools differ
%% A malicious attacker could exploit this to greatly increase the overall
%% network traffic, slow down block propagation, and increases fork incidence.
%%
%% By ignoring unconfirmed deposit TXs, and ensuring a globally consistent
%% sort order (e.g. (format, reward, TXID)) this malicious scenario is
%% prevented.
find_overspent_txs(<<>>) ->
	[];
find_overspent_txs(TX)
		when TX#tx.reward > 0 orelse TX#tx.quantity > 0  ->
	Origin = ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type),
	SpentTXIDs = maps:get(Origin, get_origin_tx_map(), gb_sets:new()),
	% We only care about the origin wallet since we aren't tracking
	% unconfirmed deposits
	Wallet = ar_wallets:get(Origin),
	B = ar_node:get_current_block(),
	Denomination = B#block.denomination,
	find_overspent_txs(Origin, SpentTXIDs, Wallet, Denomination);
find_overspent_txs(_TX) ->
	[].

find_overspent_txs(Origin, SpentTXIDs, Wallet, Denomination) ->
	case gb_sets:is_empty(SpentTXIDs) of
		true ->
			[];
		false ->
			{{_, TXID}, SpentTXIDs2} = gb_sets:take_largest(SpentTXIDs),
			TX = get_tx(TXID),
			Wallet2 = ar_node_utils:apply_tx(Wallet, Denomination, TX),
			case is_overspent(Origin, Wallet2) of
				false ->
					find_overspent_txs(Origin, SpentTXIDs2, Wallet2, Denomination);
				true ->
					RemainingTXs = to_txs(SpentTXIDs2),
					% TX and any remaining in SortedTXs2 overspend the account
					[TX | RemainingTXs]
			end
	end.

is_overspent(Origin, Wallet) ->
	% not_found should only occur when spending from an address that is not
	% present in the blockchain. For example if a user tries to spend from
	% an address before the initial deposit to that address has confirmed.
	case maps:get(Origin, Wallet, not_found) of
		not_found ->
			true;
		{Quantity, _} when Quantity < 0 ->
			true;
		{Quantity, _, _Denomination} when Quantity < 0 ->
			true;
		_ ->
			false
	end.

to_txs(TXIDs) when is_list(TXIDs) ->
	[get_tx(TXID) || TXID <- TXIDs];
to_txs(TXIDs) ->
	to_txs([TXID || {_, TXID} <- gb_sets:to_list(TXIDs)]).