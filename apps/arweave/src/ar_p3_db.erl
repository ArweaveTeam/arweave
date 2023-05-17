-module(ar_p3_db).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").

-export([
	get_or_create_account/3, get_account/1, get_transaction/2, get_balance/1, get_balance/2,
	post_deposit/3, post_charge/4]).
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

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================
init([]) ->
	process_flag(trap_exit, true),
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
	case safe_get({p3_tx, Address}, term_to_binary(Id), not_found) of
		not_found ->
			{reply, {error, not_found}, State};
		Transaction ->
			{reply, {ok, binary_to_term(Transaction)}, State}
	end;

handle_call({post_deposit, Address, Amount, TXID}, _From, State) ->
	case get_account2(Address) of
		not_found ->
			{reply, {error, not_found}, State};
		Account ->
			{reply, post_deposit2(Account, Amount, TXID), State}
	end;
	
handle_call({post_charge, Address, Amount, Minimum, Request}, _From, State) ->
	case get_account2(Address) of
		not_found ->
			{reply, {error, not_found}, State};
		Account ->
			{reply, post_charge2(Account, Amount, Minimum, Request), State}
	end.

handle_cast(_Message, State) ->
	{noreply, State}.

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

post_deposit2(_Account, Amount, _TXID) when Amount =< 0 ->
	{error, invalid_amount};
post_deposit2(Account, Amount, TXID) ->
	post_transaction(Account, Amount, TXID, undefined).

post_charge2(_Account, Amount, _Minimum, _Request) when Amount =< 0 ->
	{error, invalid_amount};
post_charge2(_Account, _Amount, Minimum, _Request) when not is_integer(Minimum) ->
	{error, invalid_minimum};
post_charge2(Account, Amount, Minimum, _Request)
  		when Account#p3_account.balance - Amount < Minimum ->
	{error, insufficient_funds};
post_charge2(Account, Amount, _Minimum, Request) ->
	case request_label(Request) of
		{ok, RequestLabel} ->
			post_transaction(Account, -Amount, undefined, RequestLabel);
		Error ->
			Error
	end.

request_label(#{ method := Method, path := Path })
  		when is_bitstring(Method), is_bitstring(Path) ->
	{ok, <<Method/bitstring, " ", Path/bitstring>>};
request_label(_) ->
	{error, invalid_request}.

post_transaction(Account, Amount, TXID, Request)
  		when is_integer(Amount) ->
	Address = Account#p3_account.address,
	Transaction = #p3_transaction{
		account = Address,
		amount = Amount,
		txid = TXID,
		request = Request,
		timestamp = os:system_time(microsecond)
	},
	Id = Account#p3_account.count + 1,
	ok = ar_kv:put({p3_tx, Address}, term_to_binary(Id), term_to_binary(Transaction)),
	ok = put_account(Account#p3_account{
		count = Id,
		balance = Account#p3_account.balance + Amount}
	),	
	{ok, {Id, Transaction}};
post_transaction(_Account, _Amount, _TXID, _Request) ->
	{error, invalid_amount}.

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
