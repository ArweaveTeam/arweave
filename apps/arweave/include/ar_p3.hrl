-ifndef(AR_P3_HRL).
-define(AR_P3_HRL, true).

-include_lib("ar.hrl").

-define(P3_RATE_TYPES, [
		<<"request">>
	]).

-define(TO_P3_ASSET(Network, Token), <<Network/binary, "/", Token/binary>>).
-define(FROM_P3_ASSET(Asset), list_to_tuple(binary:split(Asset, <<"/">>))).

-define(ARWEAVE_AR, <<"arweave/AR">>).

-define(P3_ENDPOINT_HEADER, <<"endpoint">>).
-define(P3_ADDRESS_HEADER, <<"address">>).
-define(P3_MOD_SEQ_HEADER, <<"modseq">>).
-define(P3_PRICE_HEADER, <<"price">>).
-define(P3_ANCHOR_HEADER, <<"anchor">>).
-define(P3_TIMEOUT_HEADER, <<"timeout">>).
-define(P3_SIGNATURE_HEADER, <<"signature">>).

-record(p3_service, {
	endpoint,
	mod_seq,
	rate_type,
	rates = #{}
}).

-record(p3_payment, {
	address,
	minimum_balance = 0,
	confirmations = 3
}).

-record(p3_config, {
	payments = #{},
	services = #{}
}).

-record(p3_account, {
	address,
	public_key,
	asset,
	balance,
	count,
	timestamp
}).

-record(p3_transaction, {
	id,
	address,
	amount,
	description,
	timestamp
}).

-endif.
