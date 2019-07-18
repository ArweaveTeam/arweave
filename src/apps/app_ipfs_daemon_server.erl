-module(app_ipfs_daemon_server).
-export([start/0, stop/0, handle/3]).
-export([put_key_wallet_address/2, put_key_wallet/2, get_key_q_wallet/1, get_keys/0, del_key/1]).
-export([already_reported/2]).
-export([cleaner_upper/0, ipfs_getter/4, sufficient_funds/2]).
-include("ar.hrl").

-ifdef(DEBUG).
-define(WAIT_CLEANER, 3 * 60 * 1000).
-define(WAIT_RETRY, 1 * 60 * 1000).
-define(MAX_IPFSAR_PENDING, 100).
-else.
-define(WAIT_CLEANER, 30 * 60 * 1000).
-define(WAIT_RETRY, 5 * 60 * 1000).
-define(MAX_IPFSAR_PENDING, 100).
-endif.

-type ar_wallet() :: tuple().
-type cowboy_http_method() :: binary().
-type cowboy_http_request() :: term().
-type cowboy_http_response() :: {non_neg_integer(), map(), iolist(), cowboy_http_request()}.
-type path() :: list(binary()).
%	POST [<<"getsend">>]
%	GET  [<<"balance">>, APIKey]
%	GET  [<<"status">>, APIKey]
%	GET  [IPFSHash]
%	GET  [IPFSHash, <<"tx">>]
%	DEL  [APIKey, IPFSHash]
-type ipfs_status() :: pending | queued | nofunds | mined | ipfs_error.

-record(ipfsar_key_q_wal, {api_key, queue_pid, wallet}).
-record(ipfsar_ipfs_status, {
	api_key,
	ipfs_hash,
	status :: ipfs_status(),
	timestamp :: non_neg_integer()
	}).
-record(ipfsar_most_recent, {api_key, ipfs_hash, status, timestamp}).

%% @doc Start anything that needs to be started.  So far, just the mnesia table.
start() ->
	%% assume mnesia already started (used in ar_tx_search)
	mnesia_create_table(ipfsar_key_q_wal,   set, record_info(fields, ipfsar_key_q_wal)),
	mnesia_create_table(ipfsar_ipfs_status, bag, record_info(fields, ipfsar_ipfs_status)),
	mnesia_create_table(ipfsar_most_recent, set, record_info(fields, ipfsar_most_recent)),
	spawn(?MODULE, cleaner_upper, []).

%% @doc Stop all the queues.
stop() ->
	mnesia:foldl(fun(#ipfsar_key_q_wal{queue_pid=Q}, _) ->
			app_queue:stop(Q)
		end,
		ok,
		ipfsar_key_q_wal).

%% @doc Find the wallet keyfile for the given address, and call put_key_wal/2.
%% Returns pid for the generated queue.
-spec put_key_wallet_address(binary(), binary()) -> {ok, pid()}.
put_key_wallet_address(Key, Addr) ->
	Filename = <<"wallets/arweave_keyfile_", Addr/binary, ".json">>,
	Wallet = ar_wallet:load_keyfile(Filename),
	put_key_wallet(Key, Wallet).

%% @doc Add a new {api_key, queue, wallet} tuple to the db.
-spec put_key_wallet(binary(), ar_wallet()) -> {ok, pid()}.
put_key_wallet(K, W) ->
	Q = app_queue:start(W),
	F = fun() -> mnesia:write(#ipfsar_key_q_wal{api_key=K, queue_pid=Q, wallet=W}) end,
	mnesia:activity(transaction, F),
	{ok, Q}.

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

%% @doc return list of current API keys.
get_keys() ->
	mnesia_get_keys().

%% @doc remove record with api key from the db.
del_key(APIKey) ->
	case get_key_q_wallet(APIKey) of
		{error, not_found} -> pass;
		{ok, Queue, _}     -> app_queue:stop(Queue)
	end,
	F = fun() -> mnesia:delete({ipfsar_key_q_wal, APIKey}) end,
	mnesia:activity(transaction, F).

%% @doc Handle /api/ipfs/... calls.
-spec handle(atom(), path(), cowboy_http_request()) -> cowboy_http_response().
handle(Method, Path, Req) ->
	case validate_request(Method, Path, Req) of
		{error, Response} ->
			handle208(Response);
		{ok, Args} ->
			handle208(process_request(Method, Path, Args, Req))
	end.

%%% Private functions.

% Cowlib does not yet support status code 208 properly.
% See https://github.com/ninenines/cowlib/pull/79
handle208({208, Headers, Body, Req}) ->
	{<<"208 Already Reported">>, Headers, Body, Req};
handle208(Response) -> Response.

%%% Handlers

%%% Validators

%% @doc validate a request and return required info
validate_request(<<"POST">>, [<<"getsend">>], Req) ->
	case validate_req_fields_auth(Req, [<<"api_key">>, <<"ipfs_hash">>]) of
		{ok, [APIKey, IPFSHash], Queue, Wallet} ->
			case already_reported(APIKey, IPFSHash) of
				{ok, _} ->
					case current_status(APIKey) of
						{ok, nofunds} ->
							{error, {402, #{}, <<"Insufficient funds in wallet">>, Req}};
						_ ->
							case length(queued_status(APIKey, pending)) > ?MAX_IPFSAR_PENDING of
								true ->
									{error, {429, #{}, <<"Too many requests pending">>, Req}};
								false ->
									{ok, [APIKey, Queue, Wallet, IPFSHash]}
							end
					end;
				{error, _} ->
					{error, {208, #{}, <<"Hash already reported by this user">>, Req}}
			end;
		{error, Response} ->
			{error, Response}
	end;
validate_request(<<"GET">>, [APIKey, <<"status">>|Options], Req) ->
	case is_authorized(APIKey, Req) of
		{ok, _Queue, _Wallet} ->
			ar:d({get_status, options, Options}),
			{ok, parse_get_status_options(Options)};
		{error, Response} ->
			{error, Response}
	end;
validate_request(<<"GET">>, [APIKey, <<"balance">>], Req) ->
	case is_authorized(APIKey, Req) of
		{ok, _Queue, Wallet} ->
			{ok, [Wallet]};
		{error, _} ->
			{error, {401, #{}, <<"Invalid API Key">>, Req}}
	end;
validate_request(<<"DEL">>, [APIKey, _IPFSHash], Req) ->
	case is_authorized(APIKey, Req) of
		{ok, _Queue, _Wallet} ->
			{ok, []};
		{error, _} ->
			{error, {401, #{}, <<"Invalid API Key">>, Req}}
	end;
validate_request(<<"GET">>, [APIKey, _IPFSHash, <<"tx">>], Req) ->
	case is_authorized(APIKey, Req) of
		{ok, _Queue, _Wallet} ->
			{ok, []};
		{error, _} ->
			{error, {401, #{}, <<"Invalid API Key">>, Req}}
	end;
validate_request(<<"GET">>, [_IPFSHash], _Req) ->
	{ok, []};
validate_request(_, _, Req) ->
	{error, {400, #{}, <<"Unrecognised request">>, Req}}.

%%% Processors

%% @doc Process a validated request.
-spec process_request(cowboy_http_method(), path(), list(), cowboy_http_request()) -> cowboy_http_response().
process_request(<<"POST">>, [<<"getsend">>], [APIKey, Queue, Wallet, IPFSHash], Req) ->
	status_update(APIKey, IPFSHash, pending),
	spawn(?MODULE, ipfs_getter, [APIKey, Queue, Wallet, IPFSHash]),
	{200, #{}, <<"Request sent to queue">>, Req};
process_request(<<"GET">>, [APIKey, <<"status">>|_], [all, 0], Req) ->
	ar:d({get_status, process, all}),
	JsonS = lists:reverse(lists:sort(lists:foldl(fun
			([T,H,S], Acc) ->
				Tiso = ar_ipfs:rfc3339_timestamp(T),
				[{[{timestamp, Tiso}, {ipfs_hash, H}, {status, S}]}|Acc];
			([], Acc) ->
				Acc
		 end,
		[],
		queued_status(APIKey)))),
	JsonB = ar_serialize:jsonify(JsonS),
	{200, #{}, JsonB, Req};
process_request(<<"GET">>, [APIKey, <<"status">>|_], [Limit, Offset], Req) ->
	ar:d({get_status, process, Limit, Offset}),
	JsonS = lists:reverse(lists:sort(lists:foldl(fun
			([T,H,S], Acc) ->
				Tiso = ar_ipfs:rfc3339_timestamp(T),
				[{[{timestamp, Tiso}, {ipfs_hash, H}, {status, S}]}|Acc];
			([], Acc) ->
				Acc
		 end,
		[],
		queued_status(APIKey)))),
	JsonB = ar_serialize:jsonify(safe_offset_limit(JsonS, Offset, Limit)),
	{200, #{}, JsonB, Req};
process_request(<<"GET">>, [_APIKey, <<"balance">>], [Wallet], Req) ->
	Address = ar_wallet:to_address(Wallet),
	Balance = ar_node:get_balance(whereis(http_entrypoint_node), Wallet),
	JsonS = {[
		{address, ar_util:encode(Address)},
		{balance, integer_to_binary(Balance)}]},
	JsonB = ar_serialize:jsonify(JsonS),
	{200, #{}, JsonB, Req};
process_request(<<"GET">>, [IPFSHash], [], Req) ->
	case ar_tx_search:get_entries(<<"IPFS-Add">>, IPFSHash) of
		[]  ->
			{404, #{}, <<"IPFS hash not found.">>, Req};
		[TXID|_] ->
			case ar_storage:lookup_tx_filename(TXID) of
				unavailable ->
					{404, #{}, <<"IPFS hash not found.">>, Req};
				Filename ->
					TX = ar_storage:do_read_tx(Filename),
					{200, #{}, TX#tx.data, Req}
			end
	end;
process_request(<<"GET">>, [APIKey, IPFSHash, <<"tx">>], [], Req) ->
	case lists:reverse(lists:sort(queued_status_hash(APIKey, IPFSHash))) of
		[] ->
			{404, #{}, <<"IPFS hash not found.">>, Req};
		[[_, mined]|_] ->
			case ar_tx_search:get_entries(<<"IPFS-Add">>, IPFSHash) of
				[]  -> {404, #{}, <<"IPFS hash not found.">>, Req};
				TXs ->
					TXEs = lists:map(fun ar_util:encode/1, TXs),
					{200, #{}, ar_serialize:jsonify(TXEs), Req}
			end;
		[[T,S]|_] ->
			Tiso = ar_ipfs:rfc3339_timestamp(T),
			JsonS = {[{timestamp, Tiso}, {ipfs_hash, IPFSHash}, {status, S}]},
			JsonB = ar_serialize:jsonify(JsonS),
			{200, #{}, JsonB, Req}
	end;
process_request(<<"DEL">>, [APIKey, IPFSHash], [], Req) ->
	case queued_status_hash(APIKey, IPFSHash) of
		[]          -> {404, [], <<"Hash not found.">>};
		[_, mined]  -> {400, [], <<"Hash already mined.">>};
		[_, queued] -> {400, [], <<"Hash already queued.">>};
		Found ->
			lists:foreach(fun([T,S]) ->
				mnesia_del_obj(#ipfsar_ipfs_status{
					api_key=APIKey, timestamp=T, ipfs_hash=IPFSHash, status=S})
				end, Found),
			{200, #{}, <<"Removed from queue.">>, Req}
	end;
process_request(_, _, _, Req) ->
	{404, #{}, <<"Request not recognised">>, Req}.

%%% Helpers

%% @doc return values for keys in key-val list - *if* all keys present.
-spec all_fields(list({term(), term()}), list()) -> {ok, binary(), list()} | error.
all_fields(KVs, Keys) ->
	MaybeAPIKey =
		case lists:keyfind(<<"api_key">>, 1, KVs) of
			{<<"api_key">>, V} -> {ok, V};
			false              -> error
		end,
	MaybeValues = lists:foldr(fun
		(_, error) -> error;
		(Key, {ok, Acc}) ->
			case lists:keyfind(Key, 1, KVs) of
				{Key, Val} -> {ok, [Val|Acc]};
				false      -> error
			end
		end,
		{ok, []}, Keys),
	case {MaybeAPIKey, MaybeValues} of
		{{ok, K}, {ok, Vs}} -> {ok, K, Vs};
		_ -> error
	end.

%% @doc Check if this user has already ipfs pinned this hash with us.
%% n.b.: just checks whether the hash has been mined into a tx, likelihood
%% of separate users uploading the same hash is low.
already_reported(APIKey, IPFSHash) ->
	%% case ar_tx_search:get_entries(<<"IPFS-Add">>, IPFSHash) of
	case queued_status_hash(APIKey, IPFSHash) of
		[] -> {ok, new_hash};
		_  -> {error, already_reported}
	end.

%% @doc remove old ipfsar_ipfs_status records.
cleaner_upper() ->
	Keys = mnesia_get_keys(),
	lists:foreach(fun(APIKey) ->
			THSs = queued_status(APIKey),
			lists:foreach(fun
					([_, _, mined])   -> pass;
					([_, _, nofunds]) -> pass;
					([_, H, ipfs_error]) ->
						{ok, Queue, Wallet} = get_key_q_wallet(APIKey),
						timer:apply_after(
							?WAIT_RETRY, ?MODULE, ipfs_getter,
							[APIKey, Queue, Wallet, H]);
					([_, H, _]) -> % pending | queued
						case hash_mined(H) of
							false -> pass;
							true  -> status_update(APIKey, H, mined)
						end
				end,
				THSs)
		end,
		Keys),
	timer:apply_after(?WAIT_CLEANER, ?MODULE, cleaner_upper, []).

ipfs_getter(APIKey, Queue, Wallet, IPFSHash) ->
	ar:d({app_ipfs_daemon, ipfs_getter, IPFSHash, getting}),
	Status = case ar_ipfs:cat_data_by_hash(IPFSHash) of
		{ok, Data} ->
			ar:d({app_ipfs_daemon, ipfs_getter, IPFSHash, got}),
			pin_provide_hash(Data, IPFSHash),
			UnsignedTX = #tx{tags=[{<<"IPFS-Add">>, IPFSHash}], data=Data},
			ar:d({app_ipfs_daemon, ipfs_getter, IPFSHash, tx_created}),
			case ?MODULE:sufficient_funds(Wallet, byte_size(Data)) of
				ok ->
					app_queue:add(maybe_restart_queue(APIKey, Queue, Wallet), UnsignedTX),
					%% tx will be added to ar_tx_search db after being mined into a block.
					queued;
				{error, _} ->
					nofunds
				end;
		{error, _Reason} ->
			ar:d({app_ipfs_daemon, ipfs_getter, IPFSHash, ipfs_error}),
			ipfs_error
	end,
	status_update(APIKey, IPFSHash, Status).

pin_provide_hash(Data, IPFSHash) ->
	{ok, _Hash2} = ar_ipfs:add_data(Data, IPFSHash),
	spawn(ar_ipfs, dht_provide_hash, [IPFSHash]).

current_status(APIKey) ->
	case mnesia:dirty_select(
			ipfsar_most_recent,
			[{
				#ipfsar_most_recent{api_key=APIKey, status='$1', _='_'},
				[],
				['$1']
			}]) of
		[Status] -> {ok, Status};
		_        -> {error, not_found}
	end.

hash_mined(Hash) ->
	case ar_tx_search:get_entries(<<"IPFS-Add">>, Hash) of
		[] -> false;
		_  -> true
	end.

%% @doc is the ipfs->ar service running?
is_app_running() ->
	try
		mnesia:table_info(ipfsar_key_q_wal, type),
		true
	catch
		exit:_ ->
			false
	end.

%% @doc Check if the API key is on the books. If so, return their wallet.
is_authorized(APIKey, Req) ->
	case is_app_running() of
		true  -> ?MODULE:get_key_q_wallet(APIKey);
		false -> {error, {503, #{}, <<"Service not running">>, Req}}
	end.

maybe_restart_queue(APIKey, Queue, Wallet) ->
	case is_process_alive(Queue) of
		true  ->
			Queue;
		false ->
			{ok, Q2} = put_key_wallet(APIKey, Wallet),
			spawn(fun() -> requeue_hashes(APIKey, Q2, Wallet) end),
			Q2
	end.

parse_get_status_options([<<"limit">>, N]) ->
	[safe_binary_to_integer(N,1), 0];
parse_get_status_options([<<"limit">>, N, <<"offset">>, M]) ->
	[safe_binary_to_integer(N,1), safe_binary_to_integer(M,0)];
parse_get_status_options(_) ->
	[all, 0].

safe_binary_to_integer(B, Default) ->
	try   binary_to_integer(B)
	catch _ -> Default
	end.

queued_status(APIKey) ->
	mnesia:dirty_select(
			ipfsar_ipfs_status,
			[{
				#ipfsar_ipfs_status{api_key=APIKey,
					timestamp='$1', ipfs_hash='$2', status='$3'},
				[],
				[['$1','$2','$3']]
			}]).

queued_status(APIKey, Status) ->
	mnesia:dirty_select(
			ipfsar_ipfs_status,
			[{
				#ipfsar_ipfs_status{
					api_key=APIKey, status=Status,
					timestamp='$1', ipfs_hash='$2'},
				[],
				[['$1','$2']]
			}]).

queued_status_hash(APIKey, IPFSHash) ->
	mnesia:dirty_select(
			ipfsar_ipfs_status,
			[{
				#ipfsar_ipfs_status{
					api_key=APIKey, status='$2',
					timestamp='$1', ipfs_hash=IPFSHash},
				[],
				[['$1','$2']]
			}]).

%% @doc Given a request, returns the json body as a struct (or error).
request_to_struct(Req) ->
	case ar_http_iface_handler:read_complete_body(Req) of
		{ok, JSON, ReadReq} ->
			case ar_serialize:json_decode(JSON) of
				{ok, {Struct}} ->
					{ok, Struct, ReadReq};
				{error, _} ->
					{error, {400, #{}, <<"Invalid json">>, ReadReq}}
			end;
		{error, body_size_too_large, TooLargeReq} ->
			{error, reply_with_413(TooLargeReq)}
	end.

reply_with_413(Req) ->
	{413, #{}, <<"Payload too large">>, Req}.

requeue_hashes(APIKey, Queue, Wallet) ->
	S = queued,
	THs = queued_status(APIKey, S),
	lists:foreach(fun([T,H]) ->
			mnesia_del_obj(#ipfsar_ipfs_status{
				api_key=APIKey, status=S, timestamp=T, ipfs_hash=H}),
			case hash_mined(H) of
				true  -> pass;
				false -> spawn(?MODULE, ipfs_getter, [APIKey, Queue, Wallet, H])
			end
		end,
	THs).

safe_offset_limit([], _, _) -> [];
safe_offset_limit(Xs, Offset, _) when Offset >= length(Xs) -> [];
safe_offset_limit(Xs, Offset, Limit) ->
	Ys = lists:nthtail(Offset, Xs),
	case Limit >= length(Ys) of
		true  -> Ys;
		false ->
			{Zs,_} = lists:split(Limit, Ys),
			Zs
	end.

status_delete(APIKey, IPFSHash) ->
	lists:foreach(fun([T,S]) ->
			mnesia_del_obj(#ipfsar_ipfs_status{
				api_key=APIKey, ipfs_hash=IPFSHash,
				status=S, timestamp=T})
		end,
		queued_status_hash(APIKey, IPFSHash)).

status_update(APIKey, IPFSHash, Status) ->
	status_delete(APIKey, IPFSHash),
	TS = timestamp(),
	R1 = #ipfsar_ipfs_status{
		api_key=APIKey, ipfs_hash=IPFSHash,
		status=Status, timestamp=TS},
	mnesia_write(R1),
	R2 = #ipfsar_most_recent{
		api_key=APIKey, ipfs_hash=IPFSHash,
		status=Status, timestamp=TS},
	mnesia_write(R2).

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

timestamp() ->
	{utc, calendar:universal_time()}.

validate_req_fields_auth(Req, FieldsRequired) ->
	case request_to_struct(Req) of
		{ok, Struct, NewReq} ->
			case all_fields(Struct, FieldsRequired) of
				{ok, APIKey, ReqFields} ->
					case is_authorized(APIKey, NewReq) of
						{ok, Queue, Wallet} ->
								{ok, ReqFields, Queue, Wallet};
						{error, _} ->
							{error, {401, #{}, <<"Invalid API Key">>, NewReq}}
					end;
				error ->
					{error, {400, #{}, <<"Invalid json fields">>, NewReq}}
			end;
		{error, Response} ->
			{error, Response}
	end.

mnesia_create_table(Name, Type, Info) ->
	TabDef = [{attributes, Info}, {disc_copies, [node()]}, {type, Type}],
	mnesia:create_table(Name, TabDef).

mnesia_del_obj(Obj) ->
	F = fun() -> mnesia:delete_object(Obj) end,
	mnesia:activity(transaction, F).

mnesia_get_keys() ->
	mnesia:dirty_select(
		ipfsar_key_q_wal,
		[{
			#ipfsar_key_q_wal{api_key='$1', _='_'},
			[],
			['$1']
		}]).

mnesia_write(Record) ->
	F = fun() -> mnesia:write(Record) end,
	mnesia:activity(transaction, F).
