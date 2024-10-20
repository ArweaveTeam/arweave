%%%
%%% @doc P3 Ledger implementation.
%%%
%%% Every peer is represented by a separate copy of ledger. The ledger process
%%% registers itself in gproc. Every read and write to the peer ledger is
%%% serialized via the process mailbox (gen_server:call).
%%%
%%% The ledger is written in such a way that it tracks the value movement from
%%% the node operator perspective; this mean that positive net for an account
%%% represents received value, while negative net represents provided value.
%%% This also means that if one needs to find a value from the peer perspective,
%%% the account total (see below) should be negated.
%%%
%%% The ledger itself contains a rocksdb database with three column families,
%%% one per virtual account:
%%% - tokens (?ar_p3_ledger_record_account_tokens): an account tracking the
%%%   token income. Sum of all records in this column family will represent a
%%%   total amount of token received from the peer in form of deposits;
%%% - credits (?ar_p3_ledger_record_account_credits): an account tracking the
%%%   credited value. Sum of all records in this column family will represent
%%%   an amount of liability for the peer (hence the negative value); the
%%%   negated value will represent the amount of credits available for the peer;
%%% - services (?ar_p3_ledger_record_account_services): an account tracking the
%%%   served value. Sum of all records in this column family will represent an
%%%   amount of services that were provided to the peer in the past (hence the
%%%   negative value); The negated value will represent the sum of all services
%%%   received by the peer.
%%%
%%% Each transfer creates two records in two tables: one "in" and one "out".
%%% The amounts in both records will be the same, which implies that all the
%%% records amounts are non-negative, which allows for tracking the value
%%% transfers for both parties, depending on the record interpretation. The ids
%%% in both records will also (and must) be the same.
%%% When doing the summation for a virtual account (column family), the default
%%% interpretation for the records is to calculate values for the node operator.
%%% The "in" records are considered positive, and the "out" records are
%%% considered negative.
%%%
%%% Record identifier.
%%%
%%% The identifier must be sortable in two ways:
%%% 1. Time. Older records must have bigger identifier.
%%% 2. Transaction vs Checkpoint. If the checkpoint and the transaction record
%%%    are created at the same point in time, the checkpoint identifier must be
%%%    bigger, to ensure the records ordering.
%%%
%%% Checkpoints and caching.
%%%
%%% Calculating the sum of all records on every database read is not effective.
%%% To mitigate this calculations, a special type of record is inserted in
%%% configured intervals: checkpoints. Each checkpoint contains the asset map,
%%% which contains the sum of all records for a given account, from the node
%%% operator perspective.
%%%
%%% When started, the ledger process will open the database and read all three
%%% column families from the last record upon reaching the checkpoint. At this
%%% point the read stops and the summation result is cached in the process
%%% state.
%%%
%%% Shutdown timer.
%%%
%%% The started ledger process will eventually terminate when not used for some
%%% time. The actual timing is defined with `?shutdown_timeout_ms`.
%%% The way shutdown timer is implemented might seem controversial. Instead of
%%% using the gen_server callbacks timers, it relies on classic `send_after`
%%% timers, because callback timers are reset of every incoming message. In case
%%% of heavily used ledger (for example, the peer is actively draining its
%%% deposit), this might give undesired overhead. Another effect is that every
%%% message will drop the timer, including unhandled calls, casts, etc., while
%%% the classic timer enables us to precisely control which messages have effect
%%% on shutdown time.
%%% The unwanted effect for this solution: the actual shutdown time will always
%%% be in the interval [?shutdown_timeout_ms .. ?shutdown_timeout_ms*2] after
%%% the last significant message.
%%%
-module(ar_p3_ledger).

-export([
	start_link/3, transfer/6, deposit/4, service/4, total/1, total_tokens/1,
	total_credits/1, total_services/1
]).
-export([init/1, handle_continue/2, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).
-ifdef(TEST).
	-export([db_dir/1, via_reg_name/1]).
-endif.

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3_ledger.hrl").



%%% ==================================================================
%%% Public types.
%%% ==================================================================



-type record_id() :: binary().

-type record_type() ::
	?ar_p3_ledger_record_type_in
	| ?ar_p3_ledger_record_type_out.

-type record_account() ::
	?ar_p3_ledger_record_account_tokens
	| ?ar_p3_ledger_record_account_credits
	| ?ar_p3_ledger_record_account_services.

-type assets_map() :: #{binary() := integer()}.

-export_type([record_id/0, record_type/0, record_account/0]).



%%% ==================================================================
%%% Definitions.
%%% ==================================================================



-define(via_reg_name(PeerAddress), {n, l, {?MODULE, PeerAddress}}).
-define(ledger_databases_relative_dir, "p3_ledger").
-define(db_dir(PeerAddress), binary_to_list(filename:join([get_data_dir(), ?ledger_databases_relative_dir, PeerAddress]))).
-define(log_dir(PeerAddress), binary_to_list(filename:join([get_base_log_dir(), ?ROCKS_DB_DIR, ?ledger_databases_relative_dir, PeerAddress]))).

-define(record_transaction_constant_suffix, 16#b).
-define(record_countertype(T), case T of
	?ar_p3_ledger_record_type_in -> ?ar_p3_ledger_record_type_out;
	?ar_p3_ledger_record_type_out -> ?ar_p3_ledger_record_type_in
end).
-define(is_valid_account(Account), (
	?ar_p3_ledger_record_account_tokens == Account orelse
	?ar_p3_ledger_record_account_credits == Account orelse
	?ar_p3_ledger_record_account_services == Account
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

-record(account, {
	cf_handle :: rocksdb:cf_handle() | undefined,
	assets_map = #{} :: assets_map(),
	dangling_records = 0 :: non_neg_integer()
}).

-record(timer, {
	armed = false :: boolean(),
	timer_ref :: reference() | undefined,
	secret_ref :: reference() | undefined
}).

-record(state, {
	peer_address :: binary(),
	db_dir :: file:filename_all(),
	log_dir :: file:filename_all(),
	checkpoint_interval_records :: pos_integer(),
	shutdown_time_ms :: pos_integer(),
	db_handle :: rocksdb:db_handle() | undefined,
	accounts = #{
		?ar_p3_ledger_record_account_tokens => #account{},
		?ar_p3_ledger_record_account_credits => #account{},
		?ar_p3_ledger_record_account_services => #account{}
	} :: #{record_account() := #account{}},
	shutdown_timer = #timer{} :: #timer{}
}).

-define(msg_continue_open_rocksdb, {msg_continue_open_rocksdb}).
-define(msg_continue_load_ledger, {msg_continue_load_current_values}).
-define(msg_continue_start_shutdown_timer, {msg_continue_start_shutdown_timer}).

-define(msg_call_transfer(InRecord, OutRecord), {msg_call_transfer, InRecord, OutRecord}).
-define(msg_call_total(), {msg_call_total}).

-define(msg_info_timer_alarm, {msg_info_timer_alarm}).



%%% ==================================================================
%%% Public interface.
%%% ==================================================================



start_link(CheckpointIntervalRecords, ShutdownTimeoutMs, PeerAddress) ->
	gen_server:start_link({via, gproc, ?via_reg_name(PeerAddress)}, ?MODULE, [CheckpointIntervalRecords, ShutdownTimeoutMs, PeerAddress], []).



-spec deposit(
	PeerAddress :: binary(),
	Asset :: binary(),
	Amount :: non_neg_integer(),
	Meta :: #{binary() := binary()}
) ->
	Ret :: {ok, Id :: record_id()} | {error, Reason :: term()}.

deposit(PeerAddress, Asset, Amount, Meta) ->
	transfer(PeerAddress, ?ar_p3_ledger_record_account_tokens, ?ar_p3_ledger_record_account_credits, Asset, Amount, Meta).



-spec service(
	PeerAddress :: binary(),
	Asset :: binary(),
	Amount :: non_neg_integer(),
	Meta :: #{binary() := binary()}
) ->
	Ret :: {ok, Id :: record_id()} | {error, Reason :: term()}.

service(PeerAddress, Asset, Amount, Meta) ->
	transfer(PeerAddress, ?ar_p3_ledger_record_account_credits, ?ar_p3_ledger_record_account_services, Asset, Amount, Meta).



%%%
%%% @doc Create a new transfer.
%%%
%%% The way to look at the transfer: paying with credited ("out") value for
%%% debited ("in") value, while measuring the values in the same units.
%%%
%%% Examples:
%%%
%%% Note: include the "ar_p3_legder.hrl" file.
%%%
%%% Deposit: The node operator is crediting the peer for deposited tokens.
%%%
%%% 	transfer(
%%% 		?ar_p3_ledger_record_account_tokens,
%%% 		?ar_p3_ledger_record_account_credits,
%%% 		<<"arweave/AR">>, 1000, #{...}
%%% 	).
%%%
%%% Service record: The node operator is gaining back its credits providing the
%%% actual services.
%%%
%%% 	transfer(
%%% 		?ar_p3_ledger_record_account_credits,
%%% 		?ar_p3_ledger_record_account_services,
%%% 		<<"arweave/AR">>, 1000, #{...}
%%% 	).
%%%

-spec transfer(
	PeerAddress :: binary(),
	InAccount :: record_account(),
	OutAccount :: record_account(),
	Asset :: binary(),
	Amount :: non_neg_integer(),
	Meta :: #{binary() := binary()}
) ->
	Ret :: {ok, Id :: record_id()} | {error, Reason :: term()}.

transfer(PeerAddress, InAccount, OutAccount, Asset, Amount, Meta) ->
	InRecord = new_record(?ar_p3_ledger_record_type_in, OutAccount, Asset, Amount, Meta),
	OutRecord = new_counterrecord(InAccount, InRecord),
	with_peer_ledger(PeerAddress, fun(Pid) ->
		case gen_server:call(Pid, ?msg_call_transfer(InRecord, OutRecord)) of
			ok -> {ok, InRecord#ar_p3_ledger_record_v1.id};
			{error, _Reason} = E -> E
		end
	end).



%%%
%%% @doc Collect the `total` amounts for all three accounts.
%%%

-spec total(PeerAddress :: binary()) ->
	Ret :: {ok, #{record_account() := assets_map()}}
		| {error, Reason :: term()}.

total(PeerAddress) ->
	with_existing_peer_ledger(PeerAddress, fun(Pid) ->
		gen_server:call(Pid, ?msg_call_total())
	end).



%%%
%%% @doc Collect the `total` amounts for tokens account.
%%% Represents the total amount of token received from the peer as deposits.
%%%

-spec total_tokens(PeerAddress :: binary()) ->
	Ret :: {ok, #{record_account() := assets_map()}}
		| {error, Reason :: term()}.

total_tokens(PeerAddress) ->
	case total(PeerAddress) of
		{ok, #{?ar_p3_ledger_record_account_tokens := Amount}} -> {ok, Amount};
		E -> E
	end.



%%%
%%% @doc Collect the `total` amounts for credits account.
%%% Represents the total amount of services the node operator 'owes' to the
%%% peer (negative).
%%%

-spec total_credits(PeerAddress :: binary()) ->
	Ret :: {ok, #{record_account() := assets_map()}}
		| {error, Reason :: term()}.

total_credits(PeerAddress) ->
	case total(PeerAddress) of
		{ok, #{?ar_p3_ledger_record_account_credits := Amount}} -> {ok, Amount};
		E -> E
	end.



%%%
%%% @doc Collect the `total` amounts for services account.
%%% Represents the total amount of services the node operator already served to
%%% the peer (negative).
%%%

-spec total_services(PeerAddress :: binary()) ->
	Ret :: {ok, #{record_account() := assets_map()}}
		| {error, Reason :: term()}.

total_services(PeerAddress) ->
	case total(PeerAddress) of
		{ok, #{?ar_p3_ledger_record_account_services := Amount}} -> {ok, Amount};
		E -> E
	end.



-ifdef(TEST).
	db_dir(PeerAddress) -> ?db_dir(PeerAddress).
	via_reg_name(PeerAddress) -> ?via_reg_name(PeerAddress).
-endif.



%%% ==================================================================
%%% gen_server callbacks.
%%% ==================================================================



init([CheckpointIntervalRecords, ShutdownTimeoutMs, PeerAddress]) ->
	process_flag(trap_exit, true),
	DbDir = ?db_dir(PeerAddress),
	LogDir = ?log_dir(PeerAddress),
	ok = filelib:ensure_dir(DbDir ++ "/"),
	ok = filelib:ensure_dir(LogDir ++ "/"),
	{ok, #state{
		peer_address = PeerAddress,
		db_dir = DbDir,
		log_dir = LogDir,
		shutdown_time_ms = ShutdownTimeoutMs,
		checkpoint_interval_records = CheckpointIntervalRecords
	}, {continue, ?msg_continue_open_rocksdb}}.



handle_continue(?msg_continue_open_rocksdb, #state{
	accounts = #{
		?ar_p3_ledger_record_account_tokens := EquityAccount,
		?ar_p3_ledger_record_account_credits := LiabilityAccount,
		?ar_p3_ledger_record_account_services := ServiceAccount
	} = Accounts0
} = S0) ->
	case rocksdb:open(S0#state.db_dir, ?ledger_db_options, [
		{"default", ?ledger_cf_options},
		{"tokens", ?ledger_cf_options},
		{"credits", ?ledger_cf_options},
		{"service", ?ledger_cf_options}
	]) of
		{ok, DbHandle, [_Default, EquityAccountHandle, LiabilityAccountHandle, ServiceAccountHandle]} ->
			S1 = S0#state{
				db_handle = DbHandle,
				accounts = Accounts0#{
					?ar_p3_ledger_record_account_tokens := EquityAccount#account{cf_handle = EquityAccountHandle},
					?ar_p3_ledger_record_account_credits := LiabilityAccount#account{cf_handle = LiabilityAccountHandle},
					?ar_p3_ledger_record_account_services := ServiceAccount#account{cf_handle = ServiceAccountHandle}
				}
			},
			{noreply, S1, {continue, ?msg_continue_load_ledger}};

		{error, OpenError} ->
			?LOG_ERROR([{event, db_operation_failed}, {op, open}, {reason, io_lib:format("~p", [OpenError])}]),
			{stop, {failed_to_open_database, OpenError}, S0}
	end;

handle_continue(?msg_continue_load_ledger, #state{
	accounts = #{
		?ar_p3_ledger_record_account_tokens := EquityAccount,
		?ar_p3_ledger_record_account_credits := LiabilityAccount,
		?ar_p3_ledger_record_account_services := ServiceAccount
	} = Accounts0
} = S0) ->
	try
		{EquityAccountDanglingRecords, EquityAccountAssets} = load_assets(?ar_p3_ledger_record_account_tokens, S0),
		{LiabilityAccountDanglingRecords, LiabilityAccountAssets} = load_assets(?ar_p3_ledger_record_account_credits, S0),
		{ServiceAccountDanglingRecords, ServiceAccountAssets} = load_assets(?ar_p3_ledger_record_account_services, S0),

		S1 = S0#state{
			accounts = Accounts0#{
				?ar_p3_ledger_record_account_tokens :=
					EquityAccount#account{assets_map = EquityAccountAssets, dangling_records = EquityAccountDanglingRecords},
				?ar_p3_ledger_record_account_credits :=
					LiabilityAccount#account{assets_map = LiabilityAccountAssets, dangling_records = LiabilityAccountDanglingRecords},
				?ar_p3_ledger_record_account_services :=
					ServiceAccount#account{assets_map = ServiceAccountAssets, dangling_records = ServiceAccountDanglingRecords}
			}
		},

		{noreply, S1, {continue, ?msg_continue_start_shutdown_timer}}
	catch
		_:E -> {stop, E, S0}
	end;

handle_continue(?msg_continue_start_shutdown_timer, #state{shutdown_timer = Timer0} = S0) ->
	Timer1 = maybe_cancel_timer(Timer0),
	SecretRef = erlang:make_ref(),
	TimerRef = erlang:send_after(S0#state.shutdown_time_ms, self(), ?msg_info_timer_alarm),
	S1 = S0#state{shutdown_timer = Timer1#timer{
		timer_ref = TimerRef,
		secret_ref = SecretRef
	}},
	{noreply, S1};

handle_continue(Request, S0) ->
	?LOG_WARNING([{event, unhandled_continue}, {continue, Request}]),
	{noreply, S0}.



handle_call(?msg_call_transfer(
	#ar_p3_ledger_record_v1{id = Id, counterpart = OutAccount, asset = Asset, amount = Amount} = InRecord,
	#ar_p3_ledger_record_v1{id = Id, counterpart = InAccount, asset = Asset, amount = Amount} = OutRecord
), _From, S0)
when ?is_valid_account(InAccount)
andalso ?is_valid_account(OutAccount)
andalso InAccount /= OutAccount ->
	{ok, S1} = insert_record(InAccount, InRecord, S0),
	{ok, S2} = insert_record(OutAccount, OutRecord, S1),
	{reply, ok, maybe_disarm_timer(S2)};

handle_call(?msg_call_transfer(_InRecord, _OutRecord), _From, S0) ->
	{reply, {error, bad_accounts}, maybe_disarm_timer(S0)};

handle_call(?msg_call_total(), _From, #state{
	accounts = Accounts
} = S0) ->
	Ret = maps:map(fun(_Account, #account{assets_map = AssetsMap}) ->
		AssetsMap
	end, Accounts),
	{reply, {ok, Ret}, maybe_disarm_timer(S0)};

handle_call(Request, _From, S0) ->
	?LOG_WARNING([{event, unhandled_call}, {request, Request}]),
	{reply, ok, S0}.



handle_cast(Cast, S0) ->
	?LOG_WARNING([{event, unhandled_cast}, {cast, Cast}]),
	{noreply, S0}.



handle_info(?msg_info_timer_alarm, #state{shutdown_timer = #timer{armed = false} = Timer0} = S0) ->
	?LOG_DEBUG([{event, shutdown_timer}, {stage, armed}]),
	{noreply, S0#state{shutdown_timer = Timer0#timer{armed = true}}, {continue, ?msg_continue_start_shutdown_timer}};

handle_info(?msg_info_timer_alarm, #state{shutdown_timer = #timer{armed = true} = _Timer0} = S0) ->
	?LOG_DEBUG([{event, shutdown_timer}, {stage, terminating}]),
	{stop, shutdown, S0};

handle_info(Message, S0) ->
	?LOG_WARNING([{event, unhandled_info}, {message, Message}]),
	{noreply, S0}.



terminate(_Reason, S0) ->
	_ = db_flush(S0),
	_ = wal_sync(S0),
	_ = close(S0),
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



new_record_id() ->
	Bin = erl_snowflake:generate_unsafe(bin),
	<<Bin/binary, ?record_transaction_constant_suffix:8/unsigned-integer>>.



new_checkpoint_id(<<Bin:8/binary, N:8/unsigned-integer>>) ->
	<<Bin/binary, (N + 1):8>>.



new_record(Type, Counterpart, Asset, Amount, Meta) ->
	new_record(new_record_id(), Type, Counterpart, Asset, Amount, Meta).



new_record(Id, Type, Counterpart, Asset, Amount, Meta) ->
	#ar_p3_ledger_record_v1{
		id = Id, type = Type, counterpart = Counterpart,
		asset = Asset, amount = Amount, meta = Meta
	}.



new_counterrecord(Account, Record) ->
	Record#ar_p3_ledger_record_v1{
		type = ?record_countertype(Record#ar_p3_ledger_record_v1.type),
		counterpart = Account
	}.



new_checkpoint(RecordId, Assets) ->
	#ar_p3_ledger_checkpoint_v1{
		id = new_checkpoint_id(RecordId),
		assets_map = Assets
	}.



-spec load_assets(Account :: record_account(), S0 :: #state{}) ->
	Ret :: {
		DanglingRecords :: non_neg_integer(),
		Assets :: assets_map()
	}.

load_assets(AccountName, #state{db_handle = DbHandle, accounts = Accounts0}) ->
	Account = maps:get(AccountName, Accounts0),
	case rocksdb:iterator(DbHandle, Account#account.cf_handle, []) of
		{ok, ItrHandle} -> load_assets_recursive(
			Account, ItrHandle, rocksdb:iterator_move(ItrHandle, last), {0, #{}}
		);
		{error, Reason} -> throw({error, {Account, Reason}})
	end.



load_assets_recursive(Account, ItrHandle, {ok, _Id, Value}, {Count, AccAssets}) ->
	case decode_record(Value) of
		#ar_p3_ledger_record_v1{asset = Asset} = R0 ->
			load_assets_recursive(
				Account,
				ItrHandle,
				rocksdb:iterator_move(ItrHandle, prev),
				{Count + 1, merge_checkpoint_assets(AccAssets, #{Asset => record_signed_value(R0)})}
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



record_signed_value(#ar_p3_ledger_record_v1{type = ?ar_p3_ledger_record_type_in, amount = Amount}) -> Amount;
record_signed_value(#ar_p3_ledger_record_v1{type = ?ar_p3_ledger_record_type_out, amount = Amount}) -> -1 * Amount.



merge_checkpoint_assets(Assets0, Assets1) ->
	maps:fold(fun(Asset, Amount, Acc) ->
		maps:put(Asset, Amount + maps:get(Asset, Acc, 0), Acc)
	end, Assets0, Assets1).



%%%
%%% @doc Inserts the record into proper rocksdb table and ensures to insert
%%% checkpoints in configured intervals.
%%%

insert_record(
	Account,
	#ar_p3_ledger_record_v1{id = Id, asset = Asset} = Record,
	#state{db_handle = DbHandle, accounts = Accounts0} = S0
) ->
	Account0 = maps:get(Account, Accounts0),
	ok = rocksdb:put(DbHandle, Account0#account.cf_handle, Id, encode_record(Record), []),

	SignedAmount = record_signed_value(Record),
	Account1 = Account0#account{
		assets_map = maps:update_with(
			Asset, fun(Amount0) -> Amount0 + SignedAmount end, SignedAmount, Account0#account.assets_map),
		dangling_records = Account0#account.dangling_records + 1
	},

	Accounts1 = case Account1 of
		#account{assets_map = AssetsMap, dangling_records = DanglingRecords}
		when DanglingRecords >= S0#state.checkpoint_interval_records ->
			Checkpoint = new_checkpoint(Id, AssetsMap),
			ok = rocksdb:put(DbHandle, Account1#account.cf_handle, Checkpoint#ar_p3_ledger_checkpoint_v1.id, encode_record(Checkpoint), []),
			Accounts0#{Account := Account1#account{dangling_records = 0}};
		_ ->
			Accounts0#{Account := Account1}
	end,
	{ok, S0#state{accounts = Accounts1}}.



%%%
%%% @doc Will call a callback on started ledger process.
%%%

with_peer_ledger(PeerAddress, Fun) ->
	case gproc:where(?via_reg_name(PeerAddress)) of
		undefined -> with_peer_ledger_started(PeerAddress, Fun);
		Pid -> apply(Fun, [Pid])
	end.



%%%
%%% @doc will call a callback on started ledger process only if the ledger
%%% existed before the call.
%%%

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



maybe_cancel_timer(#timer{timer_ref = undefined} = Timer0) -> Timer0;
maybe_cancel_timer(#timer{timer_ref = TimerRef} = Timer0) ->
	_ = erlang:cancel_timer(TimerRef),
	Timer0#timer{timer_ref = undefined, secret_ref = undefined}.



maybe_disarm_timer(#state{shutdown_timer = Timer0} = S0) ->
	S0#state{shutdown_timer = Timer0#timer{armed = false}}.



db_flush(#state{db_handle = undefined}) -> ok;
db_flush(#state{db_handle = DbHandle}) ->
	case rocksdb:flush(DbHandle, [{wait, true}, {allow_write_stall, false}]) of
		{error, FlushError} ->
			?LOG_ERROR([{event, db_operation_failed}, {op, db_flush}, {reason, io_lib:format("~p", [FlushError])}]),
			{error, failed};
		_ ->
			?LOG_DEBUG([{event, db_operation}, {op, db_flush}]),
			ok
	end.



wal_sync(#state{db_handle = undefined}) -> ok;
wal_sync(#state{db_handle = DbHandle}) ->
	case rocksdb:sync_wal(DbHandle) of
		{error, SyncError} ->
			?LOG_ERROR([{event, db_operation_failed}, {op, wal_sync}, {reason, io_lib:format("~p", [SyncError])}]),
			{error, failed};
		_ ->
			?LOG_DEBUG([{event, db_operation}, {op, wal_sync}]),
			ok
	end.



close(#state{db_handle = undefined}) -> ok;
close(#state{db_handle = DbHandle}) ->
	try
		case rocksdb:close(DbHandle) of
			ok ->
				?LOG_DEBUG([{event, db_operation}, {op, close}]);
			{error, CloseError} ->
				?LOG_ERROR([{event, db_operation_failed}, {op, close}, {error, io_lib:format("~p", [CloseError])}])
		end
	catch
		Exc ->
			?LOG_ERROR([{event, ar_kv_failed}, {op, close}, {reason, io_lib:format("~p", [Exc])}])
	end.



%%% ==================================================================
%%% Private functions: encode/decode.
%%% ==================================================================



%%
%% One should never change the numbers under these definitions: the stored
%% records will still have older values, which will lead to failed reads from
%% the database.
%% If you want to add a new field into the record record, consider adding the
%% new mapping and append it to the list below.
%% If, for some reason, you want to change the definitions below, or you want to
%% remove a field from the record record, consider creating a new erlang record
%% (#ar_p3_ledger_record_vN{}) and support both older and newer erlang records
%% within this module.
%% The older records support might be dropped once you're sure these do not
%% exist in production nodes. This might be the case if:
%% - The older record records are "compacted" (deleted) from the database,
%%   since we only need the records from the latest checkpoint and later in
%%   order to be consistent;
%% - There is a way to migrate the rocksdb records from older to newer format.
%%

-define(ar_p3_ledger_common_field_version, 0).

-define(ar_p3_ledger_record_field_id, 1).
-define(ar_p3_ledger_record_field_type, 2).
-define(ar_p3_ledger_record_field_counterpart, 3).
-define(ar_p3_ledger_record_field_asset, 4).
-define(ar_p3_ledger_record_field_amount, 5).
-define(ar_p3_ledger_record_field_meta, 6).

-define(ar_p3_ledger_checkpoint_field_id, 1).
-define(ar_p3_ledger_checkpoint_field_assets_map, 2).



encode_record(#ar_p3_ledger_record_v1{
	id = Id, type = Type, counterpart = Counterpart,
	asset = Asset, amount = Amount, meta = Meta
}) ->
	Map = #{
		?ar_p3_ledger_common_field_version => 1,
		?ar_p3_ledger_record_field_id => Id,
		?ar_p3_ledger_record_field_type => encode_type(Type),
		?ar_p3_ledger_record_field_counterpart => encode_account(Counterpart),
		?ar_p3_ledger_record_field_asset => Asset,
		?ar_p3_ledger_record_field_amount => Amount,
		?ar_p3_ledger_record_field_meta => Meta
	},
	msgpack:pack(Map);

encode_record(#ar_p3_ledger_checkpoint_v1{id = Id, assets_map = AssetsMap}) ->
	Map = #{
		?ar_p3_ledger_common_field_version => 1,
		?ar_p3_ledger_checkpoint_field_id => Id,
		?ar_p3_ledger_checkpoint_field_assets_map => AssetsMap
	},
	msgpack:pack(Map).



encode_type(?ar_p3_ledger_record_type_in) -> 0;
encode_type(?ar_p3_ledger_record_type_out) -> 1.



encode_account(?ar_p3_ledger_record_account_tokens) -> 0;
encode_account(?ar_p3_ledger_record_account_credits) -> 1;
encode_account(?ar_p3_ledger_record_account_services) -> 2.



decode_record(Bin) ->
	case msgpack:unpack(Bin) of
		{ok, #{
			?ar_p3_ledger_common_field_version := 1,
			?ar_p3_ledger_record_field_id := Id,
			?ar_p3_ledger_record_field_type := EncodedType,
			?ar_p3_ledger_record_field_counterpart := EncodedCounterpart,
			?ar_p3_ledger_record_field_asset := Asset,
			?ar_p3_ledger_record_field_amount := Amount,
			?ar_p3_ledger_record_field_meta := Meta
		}} ->
			#ar_p3_ledger_record_v1{
				id = Id, type = decode_type(EncodedType), counterpart = decode_account(EncodedCounterpart),
				asset = Asset, amount = Amount, meta = Meta
			};
		{ok, #{
			?ar_p3_ledger_common_field_version := 1,
			?ar_p3_ledger_checkpoint_field_id := Id,
			?ar_p3_ledger_checkpoint_field_assets_map := AssetsMap
		}} ->
			#ar_p3_ledger_checkpoint_v1{id = Id, assets_map = AssetsMap};
		{error, _Reason} = E ->
			throw(E);
		_ ->
			throw({error, {unexpected_record, Bin}})
	end.



decode_type(0) -> ?ar_p3_ledger_record_type_in;
decode_type(1) -> ?ar_p3_ledger_record_type_out.



decode_account(0) -> ?ar_p3_ledger_record_account_tokens;
decode_account(1) -> ?ar_p3_ledger_record_account_credits;
decode_account(2) -> ?ar_p3_ledger_record_account_services.



%%% ==================================================================
%%% Unit tests.
%%% ==================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").



create_transaction_v1_records_test() ->
	InRecord = new_record(?ar_p3_ledger_record_type_in, ?ar_p3_ledger_record_account_credits, <<"test/TST">>, 1000, #{}),
	OutRecord = new_counterrecord(?ar_p3_ledger_record_account_tokens, InRecord),

	?assertEqual(InRecord#ar_p3_ledger_record_v1.id, OutRecord#ar_p3_ledger_record_v1.id, "record ids are the same").



transaction_v1_record_encode_decode_test() ->
	Record = new_record(?ar_p3_ledger_record_type_in, ?ar_p3_ledger_record_account_credits, <<"test/TST">>, 1000, #{}),
	EncodedRecord = encode_record(Record),

	?assertEqual(Record, decode_record(EncodedRecord), "decoded record is equal to original record").



create_checkpoint_v1_record_test() ->
	Id = new_record_id(),
	Checkpoint = new_checkpoint(Id, #{<<"test/TST">> => 1000}),

	?assert(Checkpoint#ar_p3_ledger_checkpoint_v1.id > Id, "checkpoint id is greater than original id").



checkpoint_v1_record_encode_decode_test() ->
	Checkpoint = new_checkpoint(new_record_id(), #{<<"test/TST">> => 1000}),
	EncodedCheckpoint = encode_record(Checkpoint),

	?assertEqual(Checkpoint, decode_record(EncodedCheckpoint), "decoded checkpoint is equal to original checkpoint").



-endif.
