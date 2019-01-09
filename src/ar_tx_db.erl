-module(ar_tx_db).
-export([start/0, get_error_codes/1, put_error_codes/2, ensure_error/1, clear_error_codes/1]).
-include_lib("eunit/include/eunit.hrl").
-include("ar.hrl").
%%% Database for storing error codes for failed transactions, so that a user
%%% can get the error reason when polling the status of a transaction. The entries
%%% has a TTL. The DB is a singleton.

%% @doc Create the DB. This will fail if a DB already exists.
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
	{ok, _} = timer:apply_after(1800*1000, ?MODULE, remove, [TXID]),
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
		[] -> put_error_codes(TXID, ["unknown_error "])
	end.

%% @doc Removes all error codes for this TX.
clear_error_codes(TXID) ->
	ets:delete(?MODULE, TXID).

tx_db_test() ->
	ar_storage:clear(),
	{Priv1, Pub1} = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub1), ?AR(10000), <<>>}]),
	OrphanedTX = ar_tx:new(Pub1, ?AR(1), ?AR(5000), <<>>),
	TX = OrphanedTX#tx { owner = Pub1 , signature = <<"BAD">>},
	SignedTX = ar_tx:sign(TX, Priv1, Pub1),
	ar_tx:verify(TX, 8, B0#block.wallet_list),
	timer:sleep(500),
	{ok, ["tx_signature_not_valid ", "tx_id_not_valid "]} = get_error_codes(TX#tx.id),
	ar_tx:verify(SignedTX, 8, B0#block.wallet_list).
