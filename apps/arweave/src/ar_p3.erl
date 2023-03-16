-module(ar_p3).

-behaviour(gen_server).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_p3.hrl").

-export([start_link/0, allow_request/1, reverse_charge/1, get_balance/3, get_rates_json/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-ifdef(TEST).
-define(MAX_BLOCK_SCAN, 4).
-else.
-define(MAX_BLOCK_SCAN, 200).
-endif.

%%%===================================================================
%%% Public interface.
%%%===================================================================
allow_request(Req) ->
	gen_server:call(?MODULE, {allow_request, Req}).

reverse_charge({Address, Id}) ->
	gen_server:call(?MODULE, {reverse_charge, Address, Id}).

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_balance(Address, Network, Token) ->
	gen_server:call(?MODULE, {get_balance, Address, ?TO_P3_ASSET(Network, Token)}).

get_rates_json() ->
	gen_server:call(?MODULE, {get_rates_json}).

%%%===================================================================
%%% Generic server callbacks.
%%%===================================================================
init([]) ->
	process_flag(trap_exit, true),
	ok = ar_events:subscribe(node_state),
	{ok, Config} = application:get_env(arweave, config),
	ar_p3_config:validate_config(Config).

handle_call({allow_request, Req}, _From, State) ->
	case handle_request(Req, State) of
		{ok, _} ->
			{reply, {true, ok}, State};
		{error, insufficient_funds} ->
			{reply, {false, insufficient_funds}, State};
		{error, stale_mod_seq} ->
			{reply, {false, stale_mod_seq}, State};
		{error, _Error} ->
			{reply, {false, invalid_header}, State}
	end;

handle_call({reverse_charge, Address, Id}, _From, State) ->
	{reply, ar_p3_db:reverse_transaction(Address, Id), State};

handle_call({get_balance, Address, Asset}, _From, State) ->
	{reply, ar_p3_db:get_balance(Address, Asset), State};

handle_call({get_rates_json}, _From, State) ->
	{reply, ar_p3_config:get_json(State), State}.

handle_cast(stop, State) ->
	{stop, normal, State}.

handle_info({event, node_state, {new_tip, B, _PrevB}}, State) ->
	NumConfirmations = ar_p3_config:get_payments_value(
							State, ?ARWEAVE_AR, #p3_payment.confirmations),
	DepositAddress = ar_p3_config:get_payments_value(
							State, ?ARWEAVE_AR, #p3_payment.address),
	case {NumConfirmations, DepositAddress} of
		{undefined, _} -> ok;
		{_, undefined} -> ok;
		{NumConfirmations, DepositAddress} ->
			scan_blocks_for_deposits(B#block.height - NumConfirmations + 1, DepositAddress)
	end,
	{noreply, State};

handle_info({event, node_state, _}, State) ->
	{noreply, State}.

terminate(Reason, State) ->
	ok.

%%%===================================================================
%%% Private functions.
%%%===================================================================

%%--------------------------------------------------------------------
%% handle_call({request}) helpers.
%%
%% Handle a web request and decide whether a charge is required and if
%% so, levy that charge
%%--------------------------------------------------------------------
handle_request(Req, P3Config) when
		is_record(P3Config, p3_config) ->
	case ar_p3_config:get_service_config(P3Config, Req) of
		undefined ->
			{ok, not_p3_service};
		ServiceConfig ->
			validate_request(Req, P3Config, ServiceConfig)
	end.

validate_request(Req, P3Config, ServiceConfig) when
		is_record(ServiceConfig, p3_service) ->
	case validate_header(Req, ServiceConfig) of
		{ok, Account} ->
			apply_charge(Account, Req, P3Config, ServiceConfig);
		Error ->
			Error
	end.

apply_charge(Account, Req, P3Config, ServiceConfig) when
		is_record(Account, p3_account) ->
	case validate_asset(Account, P3Config, ServiceConfig) of
		{ok, {MinimumBalance, Amount}} ->
			ar_p3_db:post_charge(Account#p3_account.address, Amount, MinimumBalance, Req);
		Error ->
			Error
	end.

validate_asset(Account, P3Config, ServiceConfig) ->
	Asset = Account#p3_account.asset,
	MinimumBalance = ar_p3_config:get_payments_value(
							P3Config, Asset, #p3_payment.minimum_balance),
	Amount = ar_p3_config:get_rate(ServiceConfig, Asset),
	case {MinimumBalance, Amount} of
		{undefined, _} ->
			{error, invalid_config};
		{_, undefined} ->
			{error, invalid_config};
		{MinimumBalance, Amount} ->
			{ok, {MinimumBalance, Amount}}
	end.

validate_header(Req, ServiceConfig) when is_record(ServiceConfig, p3_service) ->
	case {
		validate_mod_seq(Req, ServiceConfig),
		validate_endpoint(Req, ServiceConfig),
		validate_address(Req)
	} of
		{{ok, _}, {ok, _}, {ok, DecodedAddress}} ->
			validate_signature(DecodedAddress, Req);
		{{error, stale_mod_seq}, _, _} ->
			{error, stale_mod_seq};
		_ ->
			{error, invalid_header}
	end.

validate_mod_seq(Req, ServiceConfig) ->
	validate_mod_seq(cowboy_req:header(?P3_MOD_SEQ_HEADER, Req), Req, ServiceConfig).
validate_mod_seq(<<>>, _Req, _ServiceConfig) ->
	{error, invalid_mod_seq};
validate_mod_seq(undefined, _Req, _ServiceConfig) ->
	{ok, undefined};
validate_mod_seq(ModSeq, Req, ServiceConfig) when is_binary(ModSeq) ->
	validate_mod_seq(binary_to_integer(ModSeq), Req, ServiceConfig);
validate_mod_seq(ModSeq, _Req, ServiceConfig) when is_integer(ModSeq) ->
	case ModSeq == ServiceConfig#p3_service.mod_seq of
		true ->
			{ok, ModSeq};
		false ->
			{error, stale_mod_seq}
	end;
validate_mod_seq(_ModSeq, _Req, _ServiceConfig) ->
	{error, invalid_mod_seq}.	

validate_endpoint(Req, ServiceConfig) ->
	Endpoint = cowboy_req:header(?P3_ENDPOINT_HEADER, Req),
	case Endpoint == ServiceConfig#p3_service.endpoint of
		true ->
			{ok, Endpoint};
		false ->
			{error, invalid_endpoint}
	end.

validate_address(Req) ->
	Address = cowboy_req:header(?P3_ADDRESS_HEADER, Req),
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Address) of
		{ok, DecodedAddress} ->
			{ok, DecodedAddress};
		_ ->
			{error, invalid_address}
	end.

validate_signature(DecodedAddress, Req) ->
	case ar_util:safe_decode(cowboy_req:header(?P3_SIGNATURE_HEADER, Req)) of
		{ok, DecodedSignature} ->
			validate_signature(DecodedAddress, DecodedSignature, Req);
		Result ->
			{error, invalid_signature}
	end.

validate_signature(DecodedAddress, DecodedSignature, Req) ->
	case get_or_try_to_create_account(DecodedAddress) of
		{ok, Account} ->
			PubKey = Account#p3_account.public_key,
			Message = build_message(Req),
			case ar_wallet:verify(PubKey, Message, DecodedSignature) of
				true ->
					validate_price(Account, Req);
				false ->
					{error, invalid_signature}
			end;
		Error ->
			{error, invalid_signature}
	end.

get_or_try_to_create_account(DecodedAddress) ->
	case ar_p3_db:get_account(DecodedAddress) of
		{ok, Account} ->
			{ok, Account};
		{error, not_found} ->
			try_to_create_account(DecodedAddress);
		Error ->
			Error
	end.

try_to_create_account(DecodedAddress) ->
	case ar_storage:read_tx(ar_wallets:get_last_tx(DecodedAddress)) of
		unavailable ->
			{error, not_found};
		TX ->
			PublicKey = {TX#tx.signature_type, TX#tx.owner},
			ar_p3_db:get_or_create_account(DecodedAddress, PublicKey, ?ARWEAVE_AR)
	end.

validate_price(Account, Req) ->
	Price = cowboy_req:header(?P3_PRICE_HEADER, Req, ?ARWEAVE_AR),
	case Price == Account#p3_account.asset of
		true ->
			{ok, Account};
		false ->
			{error, invalid_price}
	end.

build_message(Req) ->
	Endpoint = cowboy_req:header(?P3_ENDPOINT_HEADER, Req, <<>>),
	Address = cowboy_req:header(?P3_ADDRESS_HEADER, Req, <<>>),
	ModSeq = cowboy_req:header(?P3_MOD_SEQ_HEADER, Req, <<>>), % cowboy requires lowercase headers
	Price = cowboy_req:header(?P3_PRICE_HEADER, Req, <<>>),
	Anchor = cowboy_req:header(?P3_ANCHOR_HEADER, Req, <<>>),
	Timeout = cowboy_req:header(?P3_TIMEOUT_HEADER, Req, <<>>),
	Message = concat([
		Endpoint,
		Address,
		ModSeq,
		Price,
		Anchor,
		Timeout
	]),
	list_to_binary(Message).

%% @doc from: https://gist.github.com/grantwinney/1a6620865d333ec227be8865b83285a2
concat(Elements) ->
    NonBinaryElements = [case Element of _ when is_binary(Element) -> binary_to_list(Element); _ -> Element end || Element <- Elements],
    lists:concat(NonBinaryElements).

%%--------------------------------------------------------------------
%% handle_info({new_tip}) helpers.
%%
%% Scan the block chain for new deposits and apply them.
%%--------------------------------------------------------------------
scan_blocks_for_deposits(LastConfirmedBlockHeight, DepositAddress) ->
	LastScannedBlockHeight = ar_p3_db:get_scan_height(),
	case LastConfirmedBlockHeight > LastScannedBlockHeight of
		true ->
			%% Scan all blocks since the last one we scanned. Unless it's been a while
			%% since we scanned, in which case cap the the history of blocks scanned to
			%% ?MAX_BLOCK_SCAN.
			ScanBlockHeight = max(
				LastScannedBlockHeight,
				LastConfirmedBlockHeight - ?MAX_BLOCK_SCAN
			) + 1,
			case scan_block_for_deposits(ScanBlockHeight, DepositAddress) of
				unavailable ->
					%% If we can't get the block, we'll try again later.
					ok;
				_ ->
					{ok, _} = ar_p3_db:set_scan_height(ScanBlockHeight),
					scan_blocks_for_deposits(LastConfirmedBlockHeight, DepositAddress)
			end;
		false ->
			ok
	end.

scan_block_for_deposits(BlockHeight, DepositAddress) when BlockHeight >= 0 ->
	case get_block_txs(BlockHeight) of
		unavailable ->
			unavailable;
		TXs ->
			apply_deposits(TXs, DepositAddress)
	end;
scan_block_for_deposits(_, _) ->
	ok.

get_block_txs(Height) ->
	BlockHash = ar_block_index:get_element_by_height(Height),
	case ar_block_cache:get(block_cache, BlockHash) of
		not_found ->
			case ar_storage:read_block(BlockHash) of
				unavailable ->
					unavailable;
				B ->
					ar_storage:read_tx(B#block.txs)
			end;
		B ->
			B#block.txs
	end.

apply_deposits([], _DepositAddress) ->
	ok;
apply_deposits([TX|TXs], DepositAddress) ->
	case TX#tx.target == DepositAddress of
		true ->
			apply_deposit(TX);
		false ->
			ok
	end,
	apply_deposits(TXs, DepositAddress).

apply_deposit(TX) ->
	Sender = ar_wallet:to_address(TX#tx.owner, TX#tx.signature_type),
	PublicKey = {TX#tx.signature_type, TX#tx.owner},
	{ok, _} = ar_p3_db:get_or_create_account(Sender, PublicKey, ?ARWEAVE_AR),
	{ok, _} = ar_p3_db:post_deposit(Sender, TX#tx.quantity, TX#tx.id),
	ok.

