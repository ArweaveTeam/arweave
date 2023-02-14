-ifndef(AR_P3_HRL).
-define(AR_P3_HRL, true).

-include_lib("ar.hrl").

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
