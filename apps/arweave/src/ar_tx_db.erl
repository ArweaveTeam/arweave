-module(ar_tx_db).
-export([start/0, get_error_codes/1, put_error_codes/2, ensure_error/1, clear_error_codes/1]).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
%%% Database for storing error codes for failed transactions, so that a user
%%% can get the error reason when polling the status of a transaction. The entries
%%% has a TTL. The DB is a singleton.

%% @doc Create a DB. This will fail if the DB already exists.
start() ->
	spawn(
		fun() ->
			ar:report([starting_tx_db]),
			ets:new(?MODULE, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	% Add a short wait to ensure that the table has been created
	% before returning.
	receive after 250 -> ok end.

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

%%% Test

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
	ar_storage:clear(),
	{_, Pub1} = ar_wallet:new(),
	{Priv2, Pub2} = ar_wallet:new(),
	[B0] = ar_weave:init([
		{ar_wallet:to_address(Pub1), ?AR(10000), <<>>},
		{ar_wallet:to_address(Pub2), ?AR(10000), <<>>}
	]),
	%% Test bad transaction
	OrphanedTX1 = ar_tx:new(Pub1, ?AR(1), ?AR(5000), <<>>),
	BadTX = OrphanedTX1#tx { owner = Pub1, signature = <<"BAD">> },
	Timestamp = os:system_time(seconds),
	ExpectedErrorCodes = [same_owner_as_target, tx_id_not_valid, tx_signature_not_valid],
	?assertEqual({invalid, ExpectedErrorCodes}, ar_tx:verify(BadTX, 8, 1, B0#block.wallet_list, Timestamp)),
	?assertEqual({ok, ExpectedErrorCodes}, get_error_codes(BadTX#tx.id)),
	%% Test good transaction
	OrphanedTX2 = ar_tx:new(Pub1, ?AR(1), ?AR(5000), <<>>),
	SignedTX = ar_tx:sign(OrphanedTX2, Priv2, Pub2),
	?assertEqual(valid, ar_tx:verify(SignedTX, 8, 1, B0#block.wallet_list, Timestamp)),
	clear_error_codes(BadTX#tx.id),
	clear_error_codes(SignedTX#tx.id),
	ok.
