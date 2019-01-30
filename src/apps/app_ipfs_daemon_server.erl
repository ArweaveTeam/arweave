-module(app_ipfs_daemon_server).
-export([start/0, stop/0, handle/3]).
-export([put_key_wallet_address/2, put_key_wallet/2, get_key_q_wallet/1, del_key/1]).
-export([already_reported/2]).
-include("ar.hrl").

-type elli_http_request() :: term().
-type elli_http_response() :: {non_neg_integer(), list(), binary()}.
-type path() :: binary() | list(binary()).

-record(ipfsar_key_q_wal, {api_key, queue_pid, wallet}).

%% @doc Start anything that needs to be started.  So far, just the mnesia table.
start() ->
	%% assume mnesia already started (used in ar_tx_search)
	TabDef = [
		{attributes, record_info(fields, ipfsar_key_q_wal)},
		{disc_copies, [node()]}
	],
	mnesia:create_table(ipfsar_key_q_wal, TabDef).

%% @doc Stop all the queues.
stop() ->
	mnesia:foldl(fun(#ipfsar_key_q_wal{queue_pid=Q}, _) ->
			app_queue:stop(Q)
		end,
		ok,
		ipfsar_key_q_wal).

%% @doc Find the wallet keyfile for the given address, and call put_key_wal/2.
-spec put_key_wallet_address(binary(), binary()) -> ok.
put_key_wallet_address(Key, Addr) ->
	Filename = <<"wallets/arweave_keyfile_", Addr/binary, ".json">>,
	Wallet = ar_wallet:load_keyfile(Filename),
	put_key_wallet(Key, Wallet).

%% @doc Add a new {api_key, queue, wallet} tuple to the db.
put_key_wallet(K, W) ->
	Q = app_queue:start(W),
	F = fun() -> mnesia:write(#ipfsar_key_q_wal{api_key=K, queue_pid=Q, wallet=W}) end,
	mnesia:activity(transaction, F).

%% @doc get queue and wallet for the api key.
get_key_q_wallet(APIKey) ->
	case mnesia:dirty_select(
			ipfsar_key_q_wal,
			[{
				#ipfsar_key_q_wal{api_key=APIKey, queue_pid='$1', wallet='$2'},
				[],
				[['$1', '$2']]
			}]) of
		[[Queue, Wallet]] -> {ok, Queue, Wallet};
		_                 -> {error, not_found}
	end.

%% @doc remove record with api key from the db.
del_key(APIKey) ->
	mnesia:delete({ipfsar_key_q_wal, APIKey}),
	ok.

%% @doc Handle /api/ipfs/... calls.
-spec handle(atom(), list(binary()), elli_http_request()) ->
	elli_http_response().
handle(Method, Path, Req) ->
	case is_app_running() of
		true  -> really_handle(Method, Path, Req);
		false -> {503, [], <<"Service not running">>}
	end.

%%% Private functions.

%%% Handlers

really_handle('POST', [<<"getsend">>], Req) ->
	case validate_request(<<"getsend">>, Req) of
		{error, Response} ->
			Response;
		{ok, Args} ->
			process_request(<<"getsend">>, Args)
	end;
really_handle(_,_,_) -> {404, [], <<"Endpoint not found">>}.

%%% Validators

%% @doc validate a request and return required info
-spec validate_request(path(), elli_http_request()) ->
	{ok, list()} | {error, elli_http_response()}.
validate_request(<<"getsend">>, Req) ->
	case request_to_struct(Req) of
		{ok, Struct} ->
			case all_fields(Struct, [<<"api_key">>, <<"ipfs_hash">>]) of
				{ok, [APIKey, IPFSHash]} ->
					case is_authorized(APIKey) of
						{ok, Queue, Wallet} ->
							case already_reported(APIKey, IPFSHash) of
								{ok, _} ->
									{ok, [APIKey, Queue, Wallet, IPFSHash]};
								{error, _} ->
									{error, {208, [], <<"Hash already reported by this user">>}}
							end;
						{error, _} ->
							{error, {401, [], <<"Invalid API Key">>}}
					end;
				error ->
					{error, {400, [], <<"Invalid json fields">>}}
			end;
		{error, _} ->
			{error, {400, [], <<"Invalid json">>}}
	end.

%%% Processors

%% @doc Process a validated request.
-spec process_request(path(), list()) -> elli_http_response().
process_request(<<"getsend">>, [_APIKey, Queue, Wallet, IPFSHash]) ->
	{ok, Data} = ar_ipfs:cat_data_by_hash(IPFSHash),
	UnsignedTX = #tx{tags=[{<<"IPFS-Add">>, IPFSHash}], data=Data},
	case sufficient_funds(Wallet, byte_size(Data)) of
		ok ->
			app_queue:add(Queue, UnsignedTX),
			%% tx will be added to ar_tx_search db after mined into block.
			{200, [], <<"Data queued for distribution">>};
		{error, _} ->
			{402, [], <<"Insufficient funds in wallet">>}
	end.

%%% Helpers

%% @doc return values for keys in key-val list - *if* all keys present.
-spec all_fields(list({term(), term()}), list()) -> list() | error.
all_fields(KVs, Keys) ->
	lists:foldr(fun
		(_, error) -> error;
		(Key, {ok, Acc}) ->
			case lists:keyfind(Key, 1, KVs) of
				{Key, Val} -> {ok, [Val|Acc]};
				false      -> error
			end
		end,
	{ok, []}, Keys).

%% @doc is the ipfs->ar service running?
is_app_running() ->
	try
		mnesia:table_info(ipfsar_key_q_wal, type),
		true
	catch
		exit: _ ->
			false
	end.

%% @doc Given a request, returns the json body as a struct (or error).
request_to_struct(Req) ->
	try
		BlockJSON = elli_request:body(Req),
		{Struct} = ar_serialize:dejsonify(BlockJSON),
		{ok, Struct}
	catch
		Exception:Reason ->
			{error, {Exception, Reason}}
	end.

%% @doc Check if the API key is on the books. If so, return their wallet.
is_authorized(APIKey) ->
	?MODULE:get_key_q_wallet(APIKey).

%% @doc Check if this user has already ipfs pinned this hash with us.
%% n.b.: just checks whether the hash has been mined into a tx, likelihood
%% of separate users uploading the same hash is low.
already_reported(_APIKey, IPFSHash) ->
	case ar_tx_search:get_entries(<<"IPFS-Add">>, IPFSHash) of
		[]      -> {ok, new_hash};
	    _       -> {error, already_reported}
	end.

%% @doc Does the wallet have sufficient funds to submit the data.
-ifdef(DEBUG).
sufficient_funds(_, _) -> ok.
-else.
sufficient_funds(Wallet, DataSize) ->
	Diff = ar_node:get_current_diff(whereis(http_entrypoint_node)),
	Cost = ar_tx:calculate_min_tx_cost(DataSize, Diff),
	Balance = ar_node:get_balance(
		whereis(http_entrypoint_node),
		ar_wallet:to_address(Wallet)),
	case Balance > Cost of
		true  -> ok;
		false -> {error, insufficient_funds}
	end.
-endif.
