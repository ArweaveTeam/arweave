-ifndef(AR_P3_HRL).
-define(AR_P3_HRL, true).

-include_lib("ar.hrl").

-define(P3_ENDPOINTS, [
		<<"/mine">>,
		<<"/tx/ready_for_mining">>,
		<<"/info">>,
		<<"/time">>,
		<<"/tx/pending">>,
		<<"/queue">>,
		<<"/tx/{hash}/status">>,
		<<"/tx/{hash}">>,
		<<"/tx2/{hash}">>,
		<<"/unconfirmed_tx/{hash}">>,
		<<"/unconfirmed_tx2/{hash}">>,
		<<"/arql">>,
		<<"/tx/{hash}/data.{extension}">>,
		<<"/sync_buckets">>,
		<<"/data_sync_record">>,
		<<"/data_sync_record/{start}/{limit}">>,
		<<"/chunk/{offset}">>,
		<<"/chunk2/{offset}">>,
		<<"/tx/{id}/offset">>,
		<<"/chunk">>,
		<<"/block_announcement">>,
		<<"/block">>,
		<<"/block2">>,
		<<"/wallet">>,
		<<"/tx">>,
		<<"/tx2">>,
		<<"/unsigned_tx">>,
		<<"/peers">>,
		<<"/price/{bytes}">>,
		<<"/price2/{bytes}">>,
		<<"/optimistic_price/{bytes}">>,
		<<"/price/{bytes}/{address}">>,
		<<"/price2/{bytes}/{address}">>,
		<<"/optimistic_price/{bytes}/{address}">>,
		<<"/v2price/{bytes}">>,
		<<"/v2price/{bytes}/{address}">>,
		<<"/reward_history/{block_hash}">>,
		<<"/hash_list">>,
		<<"/block_index">>,
		<<"/block_index2">>,
		<<"/hash_list/{from}/{to}">>,
		<<"/block_index/{from}/{to}">>,
		<<"/block_index2/{from}/{to}">>,
		<<"/recent_hash_list">>,
		<<"/recent_hash_list_diff">>,
		<<"/wallet_list">>,
		<<"/wallet_list/{root_hash}">>,
		<<"/wallet_list/{root_hash}/{cursor}">>,
		<<"/wallet_list/{root_hash}/{address}/balance">>,
		<<"/peers">>,
		<<"/wallet/{address}/balance">>,
		<<"/wallet/{address}/last_tx">>,
		<<"/tx_anchor">>,
		<<"/wallet/{address}/txs">>,
		<<"/wallet/{address}/txs/{earliest_tx}">>,
		<<"/wallet/{address}/deposits">>,
		<<"/wallet/{address}/deposits/{earliest_deposit}">>,
		<<"/block/height/{height}">>,
		<<"/block/hash/{hash}">>,
		<<"/block2/height/{height}">>,
		<<"/block2/hash/{hash}">>,
		<<"/block/height/{height}/{field}">>,
		<<"/block/hash/{hash}/{field}">>,
		<<"/block/height/{height}/wallet/{address}/balance">>,
		<<"/block/current">>,
		<<"/current_block">>,
		<<"/tx/{hash}/{field}">>,
		<<"/height">>,
		<<"/{hash}">>,
		<<"/vdf">>
	]).

-define(P3_RATE_TYPES, [
		<<"request">>,
		<<"byte">>
	]).

-record(p3_ar, {
	price,
	address
}).

-record(p3_arweave, {
	ar = #p3_ar{}
}).

-record(p3_rates, {
	rate_type,
	arweave = #p3_arweave{}
}).

-record(p3_service, {
	endpoint,
	mod_seq,
	rates = #p3_rates{}
}).

-endif.
