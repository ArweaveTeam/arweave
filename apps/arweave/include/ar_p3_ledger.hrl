-ifndef(AR_P3_LEDGER_HRL).

-define(AR_P3_LEDGER_HRL, true).

-define(ar_p3_ledger_record_type_in, debit).
-define(ar_p3_ledger_record_type_out, credit).

-define(ar_p3_ledger_record_account_tokens, tokens).
-define(ar_p3_ledger_record_account_credits, credits).
-define(ar_p3_ledger_record_account_services, services).

-record(ar_p3_ledger_record_v1, {
	id :: ar_p3_ledger:record_id(),
	type :: ar_p3_ledger:record_type(),
	counterpart :: ar_p3_ledger:record_account(),
	asset :: binary(),
	amount :: non_neg_integer(),
	meta :: #{binary() := binary()}
}).

-record(ar_p3_ledger_checkpoint_v1, {
	id :: ar_p3_ledger:record_id(),
	assets_map :: ar_p3_ledger:assets_map()
}).

-endif.
