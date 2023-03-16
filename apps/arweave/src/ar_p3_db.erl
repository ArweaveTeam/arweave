-module(ar_p3_db).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").

-export([
	get_or_create_account/3, get_account/1, get_transaction/2, get_balance/1, get_balance/2,
	post_deposit/3, post_charge/4, reverse_transaction/2,
	get_scan_height/0, set_scan_height/1]).
-export([start_link/0, init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%%%===================================================================
%%% Public interface.
%%%===================================================================
start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_or_create_account(
		Address,
		{{_, _} = _SignatureType, _Pub} = PublicKey,
		Asset = ?ARWEAVE_AR) ->
	validated_call(Address, {get_or_create_account, Address, PublicKey, Asset});
get_or_create_account(
		_Address,
		{{_, _} = _SignatureType, _Pub} = _PublicKey,
		_Asset) ->
	{error, unsupported_asset}.

get_account(Address) ->
	validated_call(Address, {get_account, Address}).

get_transaction(Address, Id) ->
	validated_call(Address, {get_transaction, Address, Id}).

get_balance(Address) ->
	get_balance(Address, ?ARWEAVE_AR).
get_balance(Address, Asset) ->
	case get_account(Address) of
		{ok, Account = #p3_account{ asset = Asset}} ->
			{ok, Account#p3_account.balance};
		{ok, _} ->
			{ok, 0};
		{error, not_found} ->
			{ok, 0};
		Error ->
			Error
	end.

post_deposit(Address, Amount, TXID) ->
	validated_call(Address, {post_deposit, Address, Amount, TXID}).

post_charge(Address, Amount, Minimum, Request) ->
	validated_call(Address, {post_charge, Address, Amount, Minimum, Request}).

reverse_transaction(Address, Id) ->
	validated_call(Address, {reverse_transaction, Address, Id}).

set_scan_height(Height) ->
	gen_server:call(?MODULE, {set_scan_height, Height}).

get_scan_height() ->
	gen_server:call(?MODULE, {get_scan_height}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================
init([]) ->
	process_flag(trap_exit, true),
	%% Database for general P3 state data (e.g. last scanned block height)
	ok = ar_kv:open(filename:join(?ROCKS_DB_DIR, "ar_p3_ledger_db"),
			ar_p3_state_db),
	{ok, #{}}.

handle_call({get_or_create_account, Address, PublicKey, Asset}, _From, State) ->
	case get_account2(Address) of
		not_found ->
			{reply, create_account(Address, PublicKey, Asset), State};
		Account = #p3_account{ address = Address, public_key = PublicKey, asset = Asset } ->
			{reply, {ok, Account}, State};
		_ ->
			{reply, {error, account_mismatch}, State}
	end;

handle_call({get_account, Address}, _From, State) ->
	case get_account2(Address) of
		not_found ->
			{reply, {error, not_found}, State};
		Account ->
			{reply, {ok, Account}, State}
	end;

handle_call({get_transaction, Address, Id}, _From, State) ->
	case get_transaction2(Address, Id) of
		not_found ->
			{reply, {error, not_found}, State};
		Transaction ->
			{reply, {ok, Transaction}, State}
	end;

handle_call({post_deposit, Address, Amount, TXID}, _From, State) ->
	case get_account2(Address) of
		not_found ->
			{reply, {error, not_found}, State};
		Account ->
			{reply, post_tx_transaction(Account, Amount, TXID), State}
	end;
	
handle_call({post_charge, Address, Amount, Minimum, Request}, _From, State) ->
	case get_account2(Address) of
		not_found ->
			{reply, {error, not_found}, State};
		Account ->
			{reply, post_request_transaction(Account, Amount, Minimum, Request), State}
	end;

handle_call({reverse_transaction, Address, Id}, _From, State) ->
	Account = get_account2(Address),
	Transaction = get_transaction2(Address, Id),
	case {Account, Transaction} of
		{not_found, _} ->
			{reply, {error, not_found}, State};
		{_, not_found} ->
			{reply, {error, not_found}, State};
		_ ->
			{reply, post_reverse_transaction(Account, Transaction), State}
	end;

handle_call({set_scan_height, Height}, _From, State) ->
	{reply, set_scan_height2(Height), State};

handle_call({get_scan_height}, _From, State) ->
	{reply, get_scan_height2(), State}.

handle_cast(stop, State) ->
	{stop, normal, State}.

handle_info(_Message, State) ->
	{noreply, State}.

terminate(_Reason, _State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

create_account(Address, PublicKey, Asset) ->
	DatabaseId = binary_to_list(ar_util:encode(Address)),
	BasicOpts = [{max_open_files, 10000}],

	ColumnFamilyDescriptors = [
		{"default", BasicOpts},
		{"p3_account", BasicOpts},
		{"p3_tx", BasicOpts}],
	ok = ar_kv:open(
		filename:join([?ROCKS_DB_DIR, "ar_p3_ledger_db", DatabaseId]),
		ColumnFamilyDescriptors, [],
		[{?MODULE, Address}, {p3_account, Address}, {p3_tx, Address}]),
	Account = #p3_account{
		address = Address,
		public_key = PublicKey,
		asset = Asset,
		balance = 0,
		count = 0,
		timestamp = os:system_time(microsecond)
	},
	ok = put_account(Account),
	{ok, Account}.

get_account2(Address) ->
	case safe_get({p3_account, Address}, Address, not_found) of
		not_found ->
			not_found;
		Account ->
			binary_to_term(Account)
	end.

put_account(Account) ->
	Address = Account#p3_account.address,
	ar_kv:put({p3_account, Address}, Address, term_to_binary(Account)).

request_label(#{ method := Method, path := Path })
  		when is_bitstring(Method), is_bitstring(Path) ->
	{ok, <<Method/bitstring, " ", Path/bitstring>>};
request_label(_) ->
	{error, invalid_request}.

get_transaction2(Address, Id) ->
	case safe_get({p3_tx, Address}, term_to_binary(Id), not_found) of
		not_found ->
			not_found;
		Transaction ->
			binary_to_term(Transaction)
	end.

get_next_id(Account) ->
	Account#p3_account.count + 1.

post_tx_transaction(_Account, _Amount, undefined) ->
	{error, invalid_txid};
post_tx_transaction(_Account, Amount, _TXID)  when Amount =< 0 ->
	{error, invalid_amount};
post_tx_transaction(Account, Amount, TXID) ->
	post_transaction(Account, TXID, Amount, TXID).

post_request_transaction(_Account, _Amount, _Minimum, undefined) ->
	{error, invalid_request};
post_request_transaction(_Account, Amount, _Minimum, _Request) when Amount < 0 ->
	{error, invalid_amount};
post_request_transaction(_Account, _Amount, Minimum, _Request) when not is_integer(Minimum) ->
	{error, invalid_minimum};
post_request_transaction(Account, Amount, Minimum, _Request)
  		when Account#p3_account.balance - Amount < Minimum ->
	{error, insufficient_funds};
post_request_transaction(Account, Amount, _Minimum, Request) ->
	case request_label(Request) of
		{ok, RequestLabel} ->
			post_transaction(Account, get_next_id(Account), -Amount, RequestLabel);
		Error ->
			Error
	end.

post_reverse_transaction(Account, Transaction) ->
	BinaryId = case Transaction#p3_transaction.id of
		Id when is_integer(Id) ->
			integer_to_binary(Id);
		Id when is_list(Id) ->
			list_to_binary(Id);
		Id when is_binary(Id) ->
			Id
	end,
	Description = <<"REVERSE:", BinaryId/binary>>,
	post_transaction(
		Account,
		get_next_id(Account),
		-Transaction#p3_transaction.amount,
		Description).

post_transaction(Account, Id, Amount, Description)
  		when is_integer(Amount) ->
	Address = Account#p3_account.address,

	Transaction2 = case get_transaction2(Address, Id) of
		not_found ->
			Transaction = #p3_transaction{
				id = Id,
				address = Address,
				amount = Amount,
				description = Description,
				timestamp = os:system_time(microsecond)
			},
			ok = ar_kv:put({p3_tx, Address}, term_to_binary(Id), term_to_binary(Transaction)),
			ok = put_account(Account#p3_account{
				count = Account#p3_account.count + 1,
				balance = Account#p3_account.balance + Amount}
			),
			Transaction;
		Transaction ->
			Transaction
	end,
	{ok, Transaction2};
post_transaction(_Account, _Id, _Amount, _Description) ->
	{error, invalid_amount}.

set_scan_height2(Height) when
		is_integer(Height), Height >= 0 ->
	case get_scan_height2() < Height of
		true ->
			ar_kv:put(ar_p3_state_db, <<"scan_height">>, term_to_binary(Height)),
			{ok, Height};
		false ->
			{error, invalid_height}
	end;
set_scan_height2(_) ->
	{error, invalid_height}.

get_scan_height2() ->
	case safe_get(ar_p3_state_db, <<"scan_height">>, not_found) of
		not_found ->
			0;
		Height ->
			binary_to_term(Height)
	end.

validate_address(<<>>) ->
	false;
validate_address(Address) when not is_binary(Address) ->
	false;
validate_address(_Address) ->
	true.

validated_call(Address, Message) ->
	case validate_address(Address) of
		true ->
			gen_server:call(?MODULE, Message);
		false ->
			{error, invalid_address}
	end.

safe_get(Name, Key, Default) ->
	Result = try
		ar_kv:get(Name, Key)
	catch
		error:{badmatch,[]} ->
			not_found
	end,
	case Result of
		{ok, Value} ->
			Value;
		not_found ->
			Default;
		_ ->	
			Result
	end.
