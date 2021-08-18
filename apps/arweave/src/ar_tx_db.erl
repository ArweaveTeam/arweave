%%% @doc Database for storing error codes for failed transactions, so that a user
%%% can get the error reason when polling the status of a transaction. The entries
%%% have a TTL. The DB is a singleton.
%%% @end
-module(ar_tx_db).

-export([get_error_codes/1, put_error_codes/2, ensure_error/1, clear_error_codes/1]).

-include_lib("arweave/include/ar.hrl").

-include_lib("eunit/include/eunit.hrl").

%% @doc Put an Erlang term into the meta DB. Typically these are
%% write-once values.
put_error_codes(TXID, ErrorCodes) ->
	ets:insert(?MODULE, {TXID, ErrorCodes}),
	{ok, _} = timer:apply_after(1800*1000, ?MODULE, clear_error_codes, [TXID]),
	ok.

%% @doc Retreive a term from the meta db.
get_error_codes(TXID) ->
	case ets:lookup(?MODULE, TXID) of
		[{_, ErrorCodes}] -> {ok, ErrorCodes};
		[] -> not_found
	end.

%% @doc Writes an unknown error code if there are not already any error codes
%% for this TX.
ensure_error(TXID) ->
	case ets:lookup(?MODULE, TXID) of
		[_] -> ok;
		[] -> put_error_codes(TXID, ["unknown_error"])
	end.

%% @doc Removes all error codes for this TX.
clear_error_codes(TXID) ->
	ets:delete(?MODULE, TXID).

%%%===================================================================
%%% Tests.
%%%===================================================================

read_write_test() ->
	put_error_codes(mocked_txid1, mocked_error),
	put_error_codes(mocked_txid2, mocked_error),
	ensure_error(mocked_txid3),
	assert_clear_error_codes(mocked_txid1),
	assert_clear_error_codes(mocked_txid2),
	assert_clear_error_codes(mocked_txid3).

assert_clear_error_codes(TXID) ->
	Fetched = get_error_codes(TXID),
	?assertMatch({ok, _}, Fetched),
	clear_error_codes(TXID),
	?assert(not_found == get_error_codes(TXID)),
	ok.

tx_db_test() ->
	{_, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	Wallets = [
		{ar_wallet:to_address(Pub1), ?AR(10000), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(10000), <<>>}
	],
	WL = maps:from_list([{A, {B, LTX}} || {A, B, LTX} <- Wallets]),
	OrphanedTX1 = ar_tx:new(Pub1, ?AR(1), ?AR(5000), <<>>),
	BadTX = OrphanedTX1#tx{ owner = Pub1, signature = <<"BAD">> },
	Timestamp = os:system_time(seconds),
	?assert(not ar_tx:verify(BadTX, {1, 4}, 1, WL, Timestamp)),
	Expected = {ok, ["same_owner_as_target", "tx_id_not_valid", "tx_signature_not_valid"]},
	?assertEqual(Expected, get_error_codes(BadTX#tx.id)),
	OrphanedTX2 = ar_tx:new(Pub1, ?AR(1), ?AR(5000), <<>>),
	SignedTX = ar_tx:sign_v1(OrphanedTX2, Priv2, Pub2),
	?assert(ar_tx:verify(SignedTX, {1, 4}, 1, WL, Timestamp)),
	clear_error_codes(BadTX#tx.id),
	clear_error_codes(SignedTX#tx.id),
	ok.
