%%%
%%%
-module(ar_p3_ledger).

-export([start_link/1, transfer/6, total/1, total_equity/1, total_liability/1, total_service/1]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3_ledger.hrl").



%%% ==================================================================
%%% Public types.
%%% ==================================================================



%% The record (booking or checkpoint) identifier.
%% The identifier must be sortable in two ways:
%% 1. Time. Older records must have bigger identifier.
%% 2. Checkpoint. If the checkpoint and the booking are created at the same
%%    point in time, the checkpoint identifier must be bigger, to ensure the
%%    records ordering.
-type record_id() :: binary().

%% The booking type.
-type booking_type() ::
	?ar_p3_ledger_booking_type_debit
	| ?ar_p3_ledger_booking_type_credit.

%% The booking account.
-type booking_account() ::
	?ar_p3_ledger_booking_account_equity
	| ?ar_p3_ledger_booking_account_liability
	| ?ar_p3_ledger_booking_account_service.

%% The booking asset.
-type booking_asset() :: binary().

%% The booking amount.
-type booking_amount() :: non_neg_integer().

%% The booking meta.
-type booking_meta() :: #{binary() := binary()}.

%% The map of assets and their amounts (relative values, considering credits
%% to be negative).
-type assets_map() :: #{binary() := integer()}.

-export_type([record_id/0, booking_asset/0, booking_amount/0, booking_meta/0]).



%%% ==================================================================
%%% Definitions.
%%% ==================================================================



-define(via_reg_name(PeerAddress), {n, l, {?MODULE, PeerAddress}}).
-define(ledger_databases_relative_dir, "p3_ledger").
-define(db_dir(PeerAddress), filename:join([get_data_dir(), ?ledger_databases_relative_dir, PeerAddress])).
-define(log_dir(PeerAddress), filename:join([get_base_log_dir(), ?ROCKS_DB_DIR, ?ledger_databases_relative_dir, PeerAddress])).
-define(ledger_checkpoint_interval_records, 100).

-define(booking_constant_suffix, 16#b).
-define(booking_countertype(T), case T of
	?ar_p3_ledger_booking_type_debit -> ?ar_p3_ledger_booking_type_credit;
	?ar_p3_ledger_booking_type_credit -> ?ar_p3_ledger_booking_type_debit
end).
-define(is_valid_account(Account), (
	?ar_p3_ledger_booking_account_equity == Account orelse
	?ar_p3_ledger_booking_account_liability == Account orelse
	?ar_p3_ledger_booking_account_service == Account
)).

-define(ledger_db_options, [
	{create_if_missing, true},
	{create_missing_column_families, true},
	{allow_mmap_reads, false},
	{allow_mmap_writes, false}
]).

-define(ledger_cf_options, [
	{max_open_files, 100},
	{block_based_table_options, [
		{bloom_filter_policy, 10}
	]}
]).

-record(cf, {
	cf_handle :: rocksdb:cf_handle() | undefined,
	assets_map = #{} :: assets_map(),
	dangling_bookings = 0 :: non_neg_integer()
}).

-record(state, {
	peer_address :: binary(),
	db_dir :: file:filename_all(),
	log_dir :: file:filename_all(),
	db_handle :: rocksdb:db_handle() | undefined,
	cfs = #{
		?ar_p3_ledger_booking_account_equity => #cf{},
		?ar_p3_ledger_booking_account_liability => #cf{},
		?ar_p3_ledger_booking_account_service => #cf{}
	} :: #{booking_account() := #cf{}}
}).
-type state() :: #state{}.

-define(msg_continue_open_rocksdb, {msg_continue_open_rocksdb}).
-define(msg_continue_load_ledger, {msg_continue_load_current_values}).

-define(msg_call_transfer(DebitBooking, CreditBooking), {msg_call_transfer, DebitBooking, CreditBooking}).
-define(msg_call_total(), {msg_call_total}).



%%% ==================================================================
%%% Public interface.
%%% ==================================================================



-spec start_link(PeerAddress :: binary()) ->
	Ret :: {ok, Pid :: pid()}
		| {error, Reason :: term()}
		| ignore.

start_link(PeerAddress) ->
	gen_server:start_link({via, gproc, ?via_reg_name(PeerAddress)}, ?MODULE, [PeerAddress], []).



%%%
%%% @doc Create a new transfer.
%%%
%%% Each transfer created two records in two tables: one debit (the "booking")
%%% and one credit (the "counterbooking"). The amounts in both records will be
%%% the same, which implies that all the records amounts are non-negative. The
%%% ids in both records will also (and must) be the same, this is a requirement
%%% for automatic repair to work properly.
%%%
%%% The way to look at the transfer: paying with credited value for debited
%%% value, while measuring the values in the same units.
%%%
%%% Examples:
%%%
%%% Deposit: In this case the node operator is "paying" for deposited tokens
%%% with a liability to return it in form of services.
%%%
%%% 	transfer(
%%% 		?ar_p3_ledger_booking_account_equity,
%%% 		?ar_p3_ledger_booking_account_liability,
%%% 		<<"arweave/AR">>, 1000, #{...}
%%% 	).
%%%
%%% Service record: In this case the node operator is "refinancing" its
%%% liability with the actual provided (credited) service.
%%%
%%% 	transfer(
%%% 		?ar_p3_ledger_booking_account_liability,
%%% 		?ar_p3_ledger_booking_account_service,
%%% 		<<"arweave/AR">>, 1000, #{...}
%%% 	).
%%%

-spec transfer(
	PeerAddress :: binary(),
	DebitAccount :: booking_account(),
	CreditAccount :: booking_account(),
	Asset :: booking_asset(),
	Amount :: booking_amount(),
	Meta :: booking_meta()
) ->
	Ret :: {ok, Id :: record_id()} | {error, Reason :: term()}.

transfer(PeerAddress, DebitAccount, CreditAccount, Asset, Amount, Meta) ->
	DebitBooking = new_booking(?ar_p3_ledger_booking_type_debit, CreditAccount, Asset, Amount, Meta),
	CreditBooking = new_counterbooking(DebitAccount, DebitBooking),
	with_peer_ledger(PeerAddress, fun(Pid) ->
		case gen_server:call(Pid, ?msg_call_transfer(DebitBooking, CreditBooking)) of
			ok -> {ok, DebitBooking#ar_p3_ledger_booking_v1.id};
			{error, _Reason} = E -> E
		end
	end).



%%%
%%% @doc Collect the `total` amounts for all three accounts.
%%%

-spec total(PeerAddress :: binary()) ->
	Ret :: {ok, #{booking_account() := assets_map()}}
		| {error, Reason :: term()}.

total(PeerAddress) ->
	with_existing_peer_ledger(PeerAddress, fun(Pid) ->
		gen_server:call(Pid, ?msg_call_total())
	end).



%%%
%%% @doc Collect the `total` amounts for equity account.
%%% Represents the total amount of token received from the peer as deposits.
%%%

-spec total_equity(PeerAddress :: binary()) ->
	Ret :: {ok, #{booking_account() := assets_map()}}
		| {error, Reason :: term()}.



total_equity(PeerAddress) ->
	case total(PeerAddress) of
		{ok, #{?ar_p3_ledger_booking_account_equity := Amount}} -> {ok, Amount};
		E -> E
	end.



%%%
%%% @doc Collect the `total` amounts for liability account.
%%% Represents the total amount of services the node operator 'owes' to the
%%% peer.
%%%

-spec total_liability(PeerAddress :: binary()) ->
	Ret :: {ok, #{booking_account() := assets_map()}}
		| {error, Reason :: term()}.



total_liability(PeerAddress) ->
	case total(PeerAddress) of
		{ok, #{?ar_p3_ledger_booking_account_liability := Amount}} -> {ok, Amount};
		E -> E
	end.



%%%
%%% @doc Collect the `total` amounts for service account.
%%% Represents the total amount of services the node operator already served to
%%% the peer.
%%%

-spec total_service(PeerAddress :: binary()) ->
	Ret :: {ok, #{booking_account() := assets_map()}}
		| {error, Reason :: term()}.



total_service(PeerAddress) ->
	case total(PeerAddress) of
		{ok, #{?ar_p3_ledger_booking_account_service := Amount}} -> {ok, Amount};
		E -> E
	end.



%%% ==================================================================
%%% gen_server callbacks.
%%% ==================================================================



-spec init([PeerAddress :: binary()]) ->
	Ret :: {ok, S0 :: state(), {continue, term()}}.

init([PeerAddress]) ->
	DbDir = ?db_dir(PeerAddress),
	LogDir = ?log_dir(PeerAddress),
	ok = filelib:ensure_dir(DbDir ++ "/"),
	ok = filelib:ensure_dir(LogDir ++ "/"),
	{ok, #state{
		peer_address = PeerAddress,
		db_dir = DbDir,
		log_dir = LogDir
	}, {continue, ?msg_continue_open_rocksdb}}.



-spec handle_continue(Msg :: term(), S0 :: state()) ->
	Ret :: {noreply, S1 :: state(), {continue, term()}}
		| {noreply, S1 :: state()}.

handle_continue(?msg_continue_open_rocksdb, #state{
	cfs = #{
		?ar_p3_ledger_booking_account_equity := EquityCf,
		?ar_p3_ledger_booking_account_liability := LiabilityCf,
		?ar_p3_ledger_booking_account_service := ServiceCf
	} = Cfs0
} = S0) ->
	case rocksdb:open(S0#state.db_dir, ?ledger_db_options, [
		{"equity", ?ledger_cf_options},
		{"liability", ?ledger_cf_options},
		{"service", ?ledger_cf_options}
	]) of
		{ok, DbHandle, [EquityCfHandle, LiabilityCfHandle, ServiceCfHandle]} ->
			?LOG_INFO([]),
			S1 = S0#state{
				db_handle = DbHandle,
				cfs = Cfs0#{
					?ar_p3_ledger_booking_account_equity := EquityCf#cf{cf_handle = EquityCfHandle},
					?ar_p3_ledger_booking_account_liability := LiabilityCf#cf{cf_handle = LiabilityCfHandle},
					?ar_p3_ledger_booking_account_service := ServiceCf#cf{cf_handle = ServiceCfHandle}
				}
			},
			{noreply, S1, {continue, ?msg_continue_load_ledger}};
		{error, OpenError} ->
			?LOG_ERROR([{event, db_operation_failed}, {op, open}, {reason, io_lib:format("~p", [OpenError])}]),
			{stop, {failed_to_open_database, OpenError}, S0}
	end;

handle_continue(?msg_continue_load_ledger, #state{
	cfs = #{
		?ar_p3_ledger_booking_account_equity := EquityCf,
		?ar_p3_ledger_booking_account_liability := LiabilityCf,
		?ar_p3_ledger_booking_account_service := ServiceCf
	} = Cfs0
} = S0) ->
	try
		{EquityCfDanglingBookings, EquityCfAssets} = load_assets(?ar_p3_ledger_booking_account_equity, S0),
		{LiabilityCfDanglingBookings, LiabilityCfAssets} = load_assets(?ar_p3_ledger_booking_account_liability, S0),
		{ServiceCfDanglingBookings, ServiceCfAssets} = load_assets(?ar_p3_ledger_booking_account_service, S0),

		S1 = S0#state{
			cfs = Cfs0#{
				?ar_p3_ledger_booking_account_equity :=
					EquityCf#cf{assets_map = EquityCfAssets, dangling_bookings = EquityCfDanglingBookings},
				?ar_p3_ledger_booking_account_liability :=
					LiabilityCf#cf{assets_map = LiabilityCfAssets, dangling_bookings = LiabilityCfDanglingBookings},
				?ar_p3_ledger_booking_account_service :=
					ServiceCf#cf{assets_map = ServiceCfAssets, dangling_bookings = ServiceCfDanglingBookings}
			}
		},

		{noreply, S1}
	catch
		_:E -> {stop, E, S0}
	end;

handle_continue(Request, S0) ->
	?LOG_WARNING([{event, unhandled_continue}, {continue, Request}]),
	{noreply, S0}.



-spec handle_call(Msg :: term(), From :: {pid(), term()}, S0 :: state()) ->
	Ret :: {reply, Reply :: term(), S1 :: state()}.

handle_call(?msg_call_transfer(
	#ar_p3_ledger_booking_v1{id = Id, counterpart = CreditAccount, asset = Asset, amount = Amount} = DebitBooking,
	#ar_p3_ledger_booking_v1{id = Id, counterpart = DebitAccount, asset = Asset, amount = Amount} = CreditBooking
), _From, S0)
when ?is_valid_account(DebitAccount)
andalso ?is_valid_account(CreditAccount)
andalso DebitAccount /= CreditAccount ->
	{ok, S1} = insert_booking(DebitAccount, DebitBooking, S0),
	{ok, S2} = insert_booking(CreditAccount, CreditBooking, S1),
	{reply, ok, S2};

handle_call(?msg_call_transfer(_DebitBooking, _CreditBooking), _From, S0) ->
	{reply, {error, bad_accounts}, S0};

handle_call(?msg_call_total(), _From, #state{
	cfs = Cfs
} = S0) ->
	Ret = maps:map(fun(_Account, #cf{assets_map = AssetsMap}) ->
		AssetsMap
	end, Cfs),
	{reply, {ok, Ret}, S0};

handle_call(Request, _From, S0) ->
	?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
	{reply, ok, S0}.



-spec handle_cast(Msg :: term(), S0 :: state()) ->
	Ret :: {noreply, S1 :: state()}.

handle_cast(Cast, S0) ->
	?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
	{noreply, S0}.



-spec handle_info(Msg :: term(), S0 :: state()) ->
	Ret :: {noreply, S1 :: state()}.

handle_info(Message, S0) ->
	?LOG_WARNING([{event, unhandled_info}, {message, Message}]),
	{noreply, S0}.



-spec terminate(Reason :: term(), S0 :: state()) ->
	Ret :: term().

terminate(_Reason, _State) ->
	ok.



%%% ==================================================================
%%% Private functions.
%%% ==================================================================



get_data_dir() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.data_dir.



get_base_log_dir() ->
	{ok, Config} = application:get_env(arweave, config),
	Config#config.log_dir.



new_booking_id() ->
  Bin = erl_snowflake:generate_unsafe(bin),
  <<Bin/binary, ?booking_constant_suffix:8/unsigned-integer>>.



new_checkpoint_id(<<Bin:8/binary, N:8/unsigned-integer>>) ->
  <<Bin/binary, (N + 1):8>>.



new_booking(Type, Counterpart, Asset, Amount, Meta) ->
	new_booking(new_booking_id(), Type, Counterpart, Asset, Amount, Meta).



new_booking(Id, Type, Counterpart, Asset, Amount, Meta) ->
  #ar_p3_ledger_booking_v1{
		id = Id, type = Type, counterpart = Counterpart,
		asset = Asset, amount = Amount, meta = Meta
	}.



new_counterbooking(Account, Booking) ->
  Booking#ar_p3_ledger_booking_v1{
    type = ?booking_countertype(Booking#ar_p3_ledger_booking_v1.type),
    counterpart = Account
  }.



new_checkpoint(BookingId, Assets) ->
  #ar_p3_ledger_checkpoint_v1{
    id = new_checkpoint_id(BookingId),
    assets_map = Assets
  }.


%%%
%%% Loads the list of assets and the cumulative value of each asset from the
%%% database, considering debit bookings to be positive and credit bookings to
%%% be negative.
%%% The function returns a 2-tuple:
%%% - Dangling bookings counter: the number of bookings that are not yet
%%% 	inluded into checkpoint;
%%% - Assets map: ?MODULE:assets_map()
%%%
load_assets(Account, #state{db_handle = DbHandle, cfs = Cfs0}) ->
	Cf = maps:get(Account, Cfs0),
	case rocksdb:iterator(DbHandle, Cf#cf.cf_handle, []) of
		{ok, ItrHandle} -> load_assets_recursive(
			Account, ItrHandle, rocksdb:iterator_move(ItrHandle, last), {0, #{}}
		);
		{error, Reason} -> throw({error, {Account, Reason}})
	end.



load_assets_recursive(Account, ItrHandle, {ok, _Id, Value}, {Count, AccAssets}) ->
	case decode_record(Value) of
		#ar_p3_ledger_booking_v1{
			type = ?ar_p3_ledger_booking_type_debit,
			asset = Asset,
			amount = Amount
		} ->
			load_assets_recursive(
				Account,
				ItrHandle,
				rocksdb:iterator_move(ItrHandle, prev),
				{Count + 1, merge_checkpoint_assets(AccAssets, #{Asset => Amount})}
			);
		#ar_p3_ledger_booking_v1{
			type = ?ar_p3_ledger_booking_type_credit,
			asset = Asset,
			amount = Amount
		} ->
			load_assets_recursive(
				Account,
				ItrHandle,
				rocksdb:iterator_move(ItrHandle, prev),
				{Count + 1, merge_checkpoint_assets(AccAssets, #{Asset => -1 * Amount})}
			);
		#ar_p3_ledger_checkpoint_v1{assets_map = Assets} ->
			{Count, merge_checkpoint_assets(AccAssets, Assets)}
	end;

load_assets_recursive(_Account, ItrHandle, {error, invalid_iterator}, Acc) ->
	ok = rocksdb:iterator_close(ItrHandle),
	Acc;

load_assets_recursive(Account, ItrHandle, {error, iterator_closed}, _Acc) ->
	ok = rocksdb:iterator_close(ItrHandle),
	throw({error, {Account, iterator_closed}}).




merge_checkpoint_assets(Assets0, Assets1) ->
  maps:fold(fun(Asset, Amount, Acc) ->
    maps:put(Asset, Amount + maps:get(Asset, Acc, 0), Acc)
  end, Assets0, Assets1).



%%%
%%% @doc Inserts the booking into proper rocksdb table and ensures to insert
%%% checkpoints in configured intervals.
%%%

insert_booking(
	Account,
	#ar_p3_ledger_booking_v1{id = Id, asset = Asset, amount = Amount} = Booking,
	#state{db_handle = DbHandle, cfs = Cfs0} = S0
) ->
	Cf0 = maps:get(Account, Cfs0),
	ok = rocksdb:put(DbHandle, Cf0#cf.cf_handle, Id, encode_record(Booking), []),

	Cf1 = Cf0#cf{
		assets_map = maps:update_with(
			Asset, fun(Amount0) -> Amount0 + Amount end, Amount, Cf0#cf.assets_map),
		dangling_bookings = Cf0#cf.dangling_bookings + 1
	},

	Cfs1 = case Cf1 of
		#cf{assets_map = AssetsMap, dangling_bookings = DanglingBookings}
		when DanglingBookings == ?ledger_checkpoint_interval_records ->
			Checkpoint = new_checkpoint(Id, AssetsMap),
			ok = rocksdb:put(DbHandle, Cf1#cf.cf_handle, Checkpoint#ar_p3_ledger_checkpoint_v1.id, encode_record(Checkpoint), []),
			Cfs0#{Account := Cf1#cf{dangling_bookings = 0}};
		_ ->
			Cfs0#{Account := Cf1}
	end,
	{ok, S0#state{cfs = Cfs1}}.



%%%
%%% @doc Will call a callback on started ledger process.
%%%
with_peer_ledger(PeerAddress, Fun) ->
	case gproc:where(?via_reg_name(PeerAddress)) of
		undefined -> with_peer_ledger_started(PeerAddress, Fun);
		Pid -> apply(Fun, [Pid])
	end.



%%% @doc will call a callback on started ledger process only if the ledger
%%% existed before the call.
with_existing_peer_ledger(PeerAddress, Fun) ->
	case gproc:where(?via_reg_name(PeerAddress)) of
		undefined ->
			case filelib:is_dir(?db_dir(PeerAddress)) of
				true -> with_peer_ledger_started(PeerAddress, Fun);
				false -> {error, no_exist}
			end;
		Pid -> apply(Fun, [Pid])
	end.



%%%
%%% @doc Will call a callback of freshly started ledger process.
%%% NB: since the function might be called consurrently, it is exposed to the
%%% race condition: several processes might call this function at the same time
%%% and therefore attempt to start a ledger process.
%%% The process start is serialized via the supervisor process (`ar_p3_sup`), so
%%% only the first caller will get the `{ok, Pid}` response; others will get
%%% `{error, {already_started, Pid}}` error and will procees from there.
%%%
with_peer_ledger_started(PeerAddress, Fun) ->
	case ar_p3_sup:start_ledger(PeerAddress) of
		{ok, Pid, _Info} -> apply(Fun, [Pid]);
		{ok, Pid} -> apply(Fun, [Pid]);
		{error, {already_started, Pid}} -> apply(Fun, [Pid]);
		{error, Reason} -> {error, Reason}
	end.



%%% ==================================================================
%%% Private functions: encode/decode.
%%% ==================================================================



%%
%% One should never change the numbers under these definitions: the stored
%% records will still have older values, which will lead to failed reads from
%% the database.
%% If you want to add a new field into the booking record, consider adding the
%% new mapping and append it to the list below.
%% If, for some reason, you want to change the definitions below, or you want to
%% remove a field from the booking record, consider creating a new erlang record
%% (#ar_p3_ledger_booking_vN{}) and support both older and newer erlang records
%% within this module.
%% The older records support might be dropped once you're sure these do not
%% exist in production nodes. This might be the case if:
%% - The older booking records are "compacted" (deleted) from the database,
%% 	 since we only need the records from the latest checkpoint and later in
%%   order to be consistent;
%% - There is a way to migrate the rocksdb records from older to newer format.
%%
-define(ar_p3_ledger_booking_field_id, 0).
-define(ar_p3_ledger_booking_field_type, 1).
-define(ar_p3_ledger_booking_field_counterpart, 2).
-define(ar_p3_ledger_booking_field_asset, 3).
-define(ar_p3_ledger_booking_field_amount, 4).
-define(ar_p3_ledger_booking_field_meta, 5).



encode_record(#ar_p3_ledger_booking_v1{
	id = Id, type = Type, counterpart = Counterpart,
	asset = Asset, amount = Amount, meta = Meta
}) ->
	Map = #{
		?ar_p3_ledger_booking_field_id => Id,
		?ar_p3_ledger_booking_field_type => encode_type(Type),
		?ar_p3_ledger_booking_field_counterpart => encode_account(Counterpart),
		?ar_p3_ledger_booking_field_asset => Asset,
		?ar_p3_ledger_booking_field_amount => Amount,
		?ar_p3_ledger_booking_field_meta => Meta
	},
	msgpack:pack(Map).



encode_type(?ar_p3_ledger_booking_type_debit) -> 0;
encode_type(?ar_p3_ledger_booking_type_credit) -> 1.



encode_account(?ar_p3_ledger_booking_account_equity) -> 0;
encode_account(?ar_p3_ledger_booking_account_liability) -> 1;
encode_account(?ar_p3_ledger_booking_account_service) -> 2.



decode_record(Bin) ->
	#{
		?ar_p3_ledger_booking_field_id := Id,
		?ar_p3_ledger_booking_field_type := EncodedType,
		?ar_p3_ledger_booking_field_counterpart := EncodedCounterpart,
		?ar_p3_ledger_booking_field_asset := Asset,
		?ar_p3_ledger_booking_field_amount := Amount,
		?ar_p3_ledger_booking_field_meta := Meta
	} = msgpack:unpack(Bin),
	#ar_p3_ledger_booking_v1{
		id = Id, type = decode_type(EncodedType), counterpart = decode_account(EncodedCounterpart),
		asset = Asset, amount = Amount, meta = Meta
	}.



decode_type(0) -> ?ar_p3_ledger_booking_type_debit;
decode_type(1) -> ?ar_p3_ledger_booking_type_credit.



decode_account(0) -> ?ar_p3_ledger_booking_account_equity;
decode_account(1) -> ?ar_p3_ledger_booking_account_liability;
decode_account(2) -> ?ar_p3_ledger_booking_account_service.
