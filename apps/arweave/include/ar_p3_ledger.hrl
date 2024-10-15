-ifndef(AR_P3_LEDGER_HRL).

-define(AR_P3_LEDGER_HRL, true).

-define(ar_p3_ledger_booking_type_debit, debit).
-define(ar_p3_ledger_booking_type_credit, credit).

-define(ar_p3_ledger_booking_account_equity, equity).
-define(ar_p3_ledger_booking_account_liability, liability).
-define(ar_p3_ledger_booking_account_service, service).

-record(ar_p3_ledger_booking_v1, {
	id :: ar_p3_ledger:record_id(),
	type :: ar_p3_ledger:booking_type(),
	counterpart :: ar_p3_ledger:booking_account(),
	asset :: ar_p3_ledger:booking_asset(),
	amount :: ar_p3_ledger:booking_amount(),
	meta :: ar_p3_ledger:booking_meta()
}).

-record(ar_p3_ledger_checkpoint_v1, {
	id :: ar_p3_ledger:record_id(),
	assets_map :: ar_p3_ledger:assets_map()
}).

-endif.
