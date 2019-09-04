-module(ar_tx_db).

-export([start/0]).
-export([log/2, put_error_codes/2, get_error_codes/1, ensure_error/1]).
-export([clear_records/1, get_tx_log/1]).

-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").

%%% Event log for transactions. Temporarily stores transaction processing events.
%%% Used for troubleshooting and reporting validation failures to users.
%%% The entries have a TTL.

%% @doc Create a DB. This will fail if the DB already exists.
start() ->
	spawn(
		fun() ->
			ar:report([starting_tx_db]),
			ets:new(?MODULE, [set, public, named_table]),
			receive stop -> ok end
		end
	),
	%% Add a short wait to ensure that the table has been created
	%% before returning.
	receive after 250 -> ok end.

put_error_codes(TXID, ErrorCodes) ->
	log(TXID, {validation_error, ErrorCodes}).

get_error_codes(TXID) ->
	Log = get_tx_log(TXID),
	search_for_validation_error(Log).

search_for_validation_error([{{validation_error, ErrorCodes}, _} | _] ) ->
	{ok, ErrorCodes};
search_for_validation_error([_ | Tail]) ->
	search_for_validation_error(Tail);
search_for_validation_error([]) ->
	not_found.

%% @doc Writes an unknown error code if there are not already any error codes
%% for this TX.
ensure_error(TXID) ->
	case get_tx_log(TXID) of
		[{{validation_error, ErrorCodes}, _} | _] ->
			{ok, ErrorCodes};
		_ ->
			log(TXID, {validation_error, ["unknown_error"]})
	end.

%% @doc Removes all records for this TX.
clear_records(TXID) ->
	ets:delete(?MODULE, TXID).

log(TXID, Event) ->
	Events = case get_tx_log(TXID) of
		[] ->
			{ok, _} = timer:apply_after(3600 * 1000, ?MODULE, clear_records, [TXID]),
			[];
		CurrentEvents ->
			CurrentEvents
	end,
	Timestamp = os:system_time(millisecond),
	ets:insert(?MODULE, {TXID, [{Event, Timestamp} | Events]}),
	ok.

get_tx_log(TXID) ->
	case ets:lookup(?MODULE, TXID) of
		[{_, Events}] ->
			Events;
		[] ->
			[]
	end.

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
	clear_records(TXID),
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
	?assert(not ar_tx:verify(BadTX, 8, 1, B0#block.wallet_list, Timestamp)),
	Expected = {ok, ["same_owner_as_target", "tx_id_not_valid", "tx_signature_not_valid"]},
	?assertEqual(Expected, get_error_codes(BadTX#tx.id)),
	%% Test good transaction
	OrphanedTX2 = ar_tx:new(Pub1, ?AR(1), ?AR(5000), <<>>),
	SignedTX = ar_tx:sign(OrphanedTX2, Priv2, Pub2),
	?assert(ar_tx:verify(SignedTX, 8, 1, B0#block.wallet_list, Timestamp)),
	clear_records(BadTX#tx.id),
	clear_records(SignedTX#tx.id),
	ok.
