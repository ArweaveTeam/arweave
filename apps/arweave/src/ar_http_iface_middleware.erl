-module(ar_http_iface_middleware).
-behaviour(cowboy_middleware).
-export([execute/2]).
-include("ar.hrl").
-define(HANDLER_TIMEOUT, 55000).

%%%===================================================================
%%% Cowboy handler callbacks.
%%%===================================================================

%% To allow prometheus_cowboy2_handler to be run when the
%% cowboy_router middleware matches on the /metrics route, this
%% middleware runs between the cowboy_router and cowboy_handler
%% middlewares. It uses the `handler` env value set by cowboy_router
%% to determine whether or not it should run, otherwise it lets
%% the cowboy_handler middleware run prometheus_cowboy2_handler.
execute(Req, #{ handler := ar_http_iface_handler } = Env) ->
	Pid = self(),
	Req1 = with_arql_semaphore_req_field(Req, Env),
	HandlerPid = spawn_link(fun() ->
		Pid ! {handled, handle(Req1, Pid)}
	end),
	{ok, TimeoutRef} = timer:send_after(
		?HANDLER_TIMEOUT,
		{timeout, HandlerPid, Req}
	),
	loop(TimeoutRef);
execute(Req, Env) ->
	{ok, Req, Env}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

with_arql_semaphore_req_field(Req, #{ arql_semaphore := Name }) ->
	Req#{ '_ar_http_iface_middleware_arql_semaphore' => Name }.

%% @doc In order to be able to have a handler-side timeout, we need to
%% handle the request asynchronously. However, cowboy doesn't allow
%% reading the request's body from a process other than its handler's.
%% This following loop function allows us to work around this
%% limitation. (see https://github.com/ninenines/cowboy/issues/1374)
loop(TimeoutRef) ->
	receive
		{handled, {Status, Headers, Body, HandledReq}} ->
			timer:cancel(TimeoutRef),
			CowboyStatus = handle208(Status),
			RepliedReq = cowboy_req:reply(CowboyStatus, Headers, Body, HandledReq),
			{stop, RepliedReq};
		{read_complete_body, From, Req} ->
			Term = ar_http_req:body(Req),
			From ! {read_complete_body, Term},
			loop(TimeoutRef);
		{read_body_chunk, From, Req, Size, Timeout} ->
			Term = ar_http_req:read_body_chunk(Req, Size, Timeout),
			From ! {read_body_chunk, Term},
			loop(TimeoutRef);
		{timeout, HandlerPid, InitialReq} ->
			unlink(HandlerPid),
			exit(HandlerPid, handler_timeout),
			ar:warn([
				handler_timeout,
				{method, cowboy_req:method(InitialReq)},
				{path, cowboy_req:path(InitialReq)}
			]),
			RepliedReq = cowboy_req:reply(500, #{}, <<"Handler timeout">>, InitialReq),
			{stop, RepliedReq}
	end.

handle(Req, Pid) ->
	%% Inform ar_bridge about new peer, performance rec will be updated from cowboy_metrics_h
	%% (this is leftover from update_performance_list)
	Peer = arweave_peer(Req),
	handle(Peer, Req, Pid).

handle(Peer, Req, Pid) ->
	Method = cowboy_req:method(Req),
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	case ar_meta_db:get(http_logging) of
		true ->
			ar:info(
				[
					http_request,
					{method, Method},
					{path, SplitPath},
					{peer, Peer}
				]
			);
		_ ->
			do_nothing
	end,
	case ar_meta_db:get({peer, Peer}) of
		not_found ->
			ar_bridge:add_remote_peer(whereis(http_bridge_node), Peer);
		_ ->
			do_nothing
	end,
	case handle(Method, SplitPath, Req, Pid) of
		{Status, Hdrs, Body, HandledReq} ->
			{Status, maps:merge(?CORS_HEADERS, Hdrs), Body, HandledReq};
		{Status, Body, HandledReq} ->
			{Status, ?CORS_HEADERS, Body, HandledReq}
	end.

%% @doc Return network information from a given node.
%% GET request to endpoint /info
handle(<<"GET">>, [], Req, _Pid) ->
	return_info(Req);

handle(<<"GET">>, [<<"info">>], Req, _Pid) ->
	return_info(Req);

%% @doc Some load balancers use 'HEAD's rather than 'GET's to tell if a node
%% is alive. Appease them.
handle(<<"HEAD">>, [], Req, _Pid) ->
	{200, #{}, <<>>, Req};
handle(<<"HEAD">>, [<<"info">>], Req, _Pid) ->
	{200, #{}, <<>>, Req};

%% @doc Return permissive CORS headers for all endpoints
handle(<<"OPTIONS">>, [<<"block">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
		    <<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"tx">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
		    <<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"peer">>|_], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
		    <<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"arql">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
		    <<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, _, Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET">>}, <<"OK">>, Req};

%% @doc Return the current universal time in seconds.
handle(<<"GET">>, [<<"time">>], Req, _Pid) ->
	{200, #{}, integer_to_binary(os:system_time(second)), Req};

%% @doc Return all mempool transactions.
%% GET request to endpoint /tx/pending.
handle(<<"GET">>, [<<"tx">>, <<"pending">>], Req, _Pid) ->
	{200, #{},
			ar_serialize:jsonify(
				%% Should encode
				lists:map(
					fun ar_util:encode/1,
					ar_node:get_pending_txs(whereis(http_entrypoint_node), [id_only])
				)
			),
	Req};

%% @doc Return outgoing transaction priority queue.
%% GET request to endpoint /queue.
handle(<<"GET">>, [<<"queue">>], Req, _Pid) ->
	{200, #{}, ar_serialize:jsonify(
		lists:map(
			fun({TXID, Reward, Size}) ->
				{[{tx, TXID}, {reward, Reward}, {size, Size}]}
			end,
			ar_tx_queue:show_queue())),
	Req};

%% @doc Return additional information about the transaction with the given identifier (hash).
%% GET request to endpoint /tx/{hash}.
handle(<<"GET">>, [<<"tx">>, Hash, <<"status">>], Req, _Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	case get_tx_filename(Hash) of
		{ok, _} ->
			case catch ar_arql_db:select_block_by_tx_id(Hash) of
				{ok, #{
					height := Height,
					indep_hash := EncodedIndepHash
				}} ->
					PseudoTags = [
						{<<"block_height">>, Height},
						{<<"block_indep_hash">>, EncodedIndepHash}
					],
					CurrentBI = ar_node:get_block_index(whereis(http_entrypoint_node)),
					case lists:member(ar_util:decode(EncodedIndepHash), ?BI_TO_BHL(CurrentBI)) of
						false ->
							{404, #{}, <<"Not Found.">>, Req};
						true ->
							CurrentHeight = ar_node:get_height(whereis(http_entrypoint_node)),
							%% First confirmation is when the TX is in the latest block.
							NumberOfConfirmations = CurrentHeight - Height + 1,
							Status = PseudoTags ++ [{<<"number_of_confirmations">>, NumberOfConfirmations}],
							{200, #{}, ar_serialize:jsonify({Status}), Req}
					end;
				not_found ->
					{404, #{}, <<"Not Found.">>, Req};
				{'EXIT', {timeout, {gen_server, call, [ar_arql_db, _]}}} ->
					{503, #{}, <<"ArQL unavailable.">>, Req}
			end;
		{response, {Status, Headers, Body}} ->
			{Status, Headers, Body, Req}
	end;


% @doc Return a transaction specified via the the transaction id (hash)
%% GET request to endpoint /tx/{hash}
handle(<<"GET">>, [<<"tx">>, Hash], Req, _Pid) ->
	case get_tx_filename(Hash) of
		{ok, Filename} ->
			{200, #{}, sendfile(Filename), Req};
		{response, {Status, Headers, Body}} ->
			{Status, Headers, Body, Req}
	end;

%% @doc Return the transaction IDs of all txs where the tags in post match the given set of key value pairs.
%% POST request to endpoint /arql with body of request being a logical expression valid in ar_parser.
%%
%% Example logical expression.
%%	{
%%		op:		{ and | or | equals }
%%		expr1:	{ string | logical expression }
%%		expr2:	{ string | logical expression }
%%	}
%%
handle(<<"POST">>, [<<"arql">>], Req, Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	case read_complete_body(Req, Pid) of
		{ok, QueryJSON, Req2} ->
			case ar_serialize:json_struct_to_query(QueryJSON) of
				{ok, Query} ->
					case catch ar_arql_db:eval_legacy_arql(Query) of
						EncodedTXIDs when is_list(EncodedTXIDs) ->
							Body = ar_serialize:jsonify(EncodedTXIDs),
							{200, #{}, Body, Req2};
						bad_query ->
							{400, #{}, <<"Invalid query.">>, Req2};
						sqlite_parser_stack_overflow ->
							{400, #{}, <<"The query nesting depth is too big.">>, Req2};
						{'EXIT', {timeout, {gen_server, call, [ar_arql_db, _]}}} ->
							{503, #{}, <<"ArQL unavailable.">>, Req2}
					end;
				{error, _} ->
					{400, #{}, <<"Invalid ARQL query.">>, Req2}
			end;
		{error, body_size_too_large} ->
			{413, #{}, <<"Payload too large">>, Req}
	end;

%% @doc Return the data field of the transaction specified via the transaction ID (hash) served as HTML.
%% GET request to endpoint /tx/{hash}/data.html
handle(<<"GET">>, [<<"tx">>, Hash, << "data.", _/binary >>], Req, _Pid) ->
	case hash_to_filename(tx, Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>, Req};
		{error, _, unavailable} ->
			{404, #{}, sendfile("data/not_found.html"), Req};
		{ok, Filename} ->
			T = ar_storage:read_tx_file(Filename),
			case ar_http_util:get_tx_content_type(T) of
				{valid, ContentType} ->
					{200, #{ <<"content-type">> => ContentType }, T#tx.data, Req};
				none ->
					{200, #{ <<"content-type">> => <<"text/html">> }, T#tx.data, Req};
				invalid ->
					{421, #{}, <<>>, Req}
			end
	end;

%% @doc Share a new block to a peer.
%% POST request to endpoint /block with the body of the request being a JSON encoded block
%% as specified in ar_serialize.
handle(<<"POST">>, [<<"block">>], Req, Pid) ->
	post_block(request, {Req, Pid});

%% @doc Generate a wallet and receive a secret key identifying it.
%% Requires internal_api_secret startup option to be set.
%% WARNING: only use it if you really really know what you are doing.
handle(<<"POST">>, [<<"wallet">>], Req, _Pid) ->
	case check_internal_api_secret(Req) of
		pass ->
			WalletAccessCode = ar_util:encode(crypto:strong_rand_bytes(32)),
			{{_, PubKey}, _} = ar_wallet:new_keyfile(WalletAccessCode),
			ResponseProps = [
				{<<"wallet_address">>, ar_util:encode(ar_wallet:to_address(PubKey))},
				{<<"wallet_access_code">>, WalletAccessCode}
			],
			{200, #{}, ar_serialize:jsonify({ResponseProps}), Req};
		{reject, {Status, Headers, Body}} ->
			{Status, Headers, Body, Req}
	end;

%% @doc Share a new transaction with a peer.
%% POST request to endpoint /tx with the body of the request being a JSON encoded tx as
%% specified in ar_serialize.
handle(<<"POST">>, [<<"tx">>], Req, Pid) ->
	case post_tx_parse_id({Req, Pid}) of
		{error, invalid_hash, Req2} ->
			{400, #{}, <<"Invalid hash.">>, Req2};
		{error, tx_already_processed, Req2} ->
			{208, #{}, <<"Transaction already processed.">>, Req2};
		{error, invalid_json, Req2} ->
			{400, #{}, <<"Invalid JSON.">>, Req2};
		{error, body_size_too_large, Req2} ->
			{413, #{}, <<"Payload too large">>, Req2};
		{ok, TX} ->
			{PeerIP, _Port} = cowboy_req:peer(Req),
			case handle_post_tx(PeerIP, TX) of
				ok ->
					{200, #{}, <<"OK">>, Req};
				{error_response, {Status, Headers, Body}} ->
					ar_bridge:unignore_id(TX#tx.id),
					{Status, Headers, Body, Req}
			end
	end;

%% @doc Sign and send a tx to the network.
%% Fetches the wallet by the provided key generated via POST /wallet.
%% Requires internal_api_secret startup option to be set.
%% WARNING: only use it if you really really know what you are doing.
handle(<<"POST">>, [<<"unsigned_tx">>], Req, Pid) ->
	case check_internal_api_secret(Req) of
		pass ->
			case read_complete_body(Req, Pid) of
				{ok, Body, Req2} ->
					{UnsignedTXProps} = ar_serialize:dejsonify(Body),
					WalletAccessCode = proplists:get_value(<<"wallet_access_code">>, UnsignedTXProps),
					%% ar_serialize:json_struct_to_tx/1 requires all properties to be there,
					%% so we're adding id, owner and signature with bogus values. These
					%% will later be overwritten in ar_tx:sign/2
					FullTxProps = lists:append(
						proplists:delete(<<"wallet_access_code">>, UnsignedTXProps),
						[
							{<<"id">>, ar_util:encode(<<"id placeholder">>)},
							{<<"owner">>, ar_util:encode(<<"owner placeholder">>)},
							{<<"signature">>, ar_util:encode(<<"signature placeholder">>)}
						]
					),
					KeyPair = ar_wallet:load_keyfile(ar_wallet:wallet_filepath(WalletAccessCode)),
					UnsignedTX = ar_serialize:json_struct_to_tx({FullTxProps}),
					Height = ar_node:get_height(whereis(http_entrypoint_node)),
					SignedTX = case ar_fork:height_2_0() of
						H when Height >= H ->
							ar_tx:sign(UnsignedTX, KeyPair);
						_ ->
							ar_tx:sign_pre_fork_2_0(UnsignedTX, KeyPair)
					end,
					{PeerIP, _Port} = cowboy_req:peer(Req),
					case handle_post_tx(PeerIP, SignedTX) of
						ok ->
							{200, #{}, ar_serialize:jsonify({[{<<"id">>, ar_util:encode(SignedTX#tx.id)}]}), Req2};
						{error_response, {Status, Headers, ErrBody}} ->
							{Status, Headers, ErrBody, Req2}
					end;
				{error, body_size_too_large} ->
					{413, #{}, <<"Payload too large">>, Req}
			end;
		{reject, {Status, Headers, Body}} ->
			{Status, Headers, Body, Req}
	end;

%% @doc Return the list of peers held by the node.
%% GET request to endpoint /peers
handle(<<"GET">>, [<<"peers">>], Req, _Pid) ->
	{200, #{},
		ar_serialize:jsonify(
			[
				list_to_binary(ar_util:format_peer(P))
			||
				P <- ar_bridge:get_remote_peers(whereis(http_bridge_node)),
				P /= arweave_peer(Req)
			]
		),
	Req};

%% @doc Return the estimated transaction fee.
%% The endpoint is pessimistic, it computes the difficulty of the new block
%% as if it has been just mined and uses the smaller of the two difficulties
%% to estimate the price.
%% GET request to endpoint /price/{bytes}
handle(<<"GET">>, [<<"price">>, SizeInBytesBinary], Req, _Pid) ->
	{200, #{}, integer_to_binary(estimate_tx_price(SizeInBytesBinary, no_wallet)), Req};

%% @doc Return the estimated reward cost of transactions with a data body size of 'bytes'.
%% The endpoint is pessimistic, it computes the difficulty of the new block
%% as if it has been just mined and uses the smaller of the two difficulties
%% to estimate the price.
%% GET request to endpoint /price/{bytes}/{address}
handle(<<"GET">>, [<<"price">>, SizeInBytesBinary, Addr], Req, _Pid) ->
	case ar_util:safe_decode(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>, Req};
		{ok, AddrOK} ->
			{200, #{}, integer_to_binary(estimate_tx_price(SizeInBytesBinary, AddrOK)), Req}
	end;

handle(<<"GET">>, [<<"legacy_hash_list">>], Req, _Pid) ->
	ok = ar_semaphore:acquire(block_index_semaphore, infinity),
	{ok, HL} = ar_node:get_legacy_hash_list(whereis(http_entrypoint_node)),
	{200, #{},
		ar_serialize:jsonify(
			lists:map(fun ar_util:encode/1, HL)
		),
	Req};

%% @doc Return the current hash list held by the node.
%% GET request to endpoint /block_index
handle(<<"GET">>, [<<"hash_list">>], Req, _Pid) ->
	handle(<<"GET">>, [<<"block_index">>], Req, _Pid);

handle(<<"GET">>, [<<"block_index">>], Req, _Pid) ->
	ok = ar_semaphore:acquire(block_index_semaphore, infinity),
	BI = ar_node:get_block_index(whereis(http_entrypoint_node)),
	{200, #{},
		ar_serialize:jsonify(
			ar_serialize:block_index_to_json_struct(format_bi_for_peer(BI, Req))
		),
	Req};

%% @doc Return the current wallet list held by the node.
%% GET request to endpoint /wallet_list
handle(<<"GET">>, [<<"wallet_list">>], Req, _Pid) ->
	Node = whereis(http_entrypoint_node),
	WalletList = ar_node:get_wallet_list(Node),
	{200, #{},
		ar_serialize:jsonify(
			ar_serialize:wallet_list_to_json_struct(WalletList)
		),
	Req};

%% @doc Share your nodes IP with another peer.
%% POST request to endpoint /peers with the body of the request being your
%% nodes network information JSON encoded as specified in ar_serialize.
% NOTE: Consider returning remaining timeout on a failed request
handle(<<"POST">>, [<<"peers">>], Req, Pid) ->
	case read_complete_body(Req, Pid) of
		{ok, BlockJSON, Req2} ->
			case ar_serialize:dejsonify(BlockJSON) of
				{Struct} ->
					{<<"network">>, NetworkName} = lists:keyfind(<<"network">>, 1, Struct),
					case (NetworkName == <<?NETWORK_NAME>>) of
						false ->
							{400, #{}, <<"Wrong network.">>, Req2};
						true ->
							Peer = arweave_peer(Req2),
							case ar_meta_db:get({peer, Peer}) of
								not_found ->
									ar_bridge:add_remote_peer(whereis(http_bridge_node), Peer);
								X -> X
							end,
							{200, #{}, [], Req2}
					end;
				_ -> {400, #{}, "Wrong network", Req2}
			end;
		{error, body_size_too_large} ->
			{413, #{}, <<"Payload too large">>, Req}
	end;

%% @doc Return the balance of the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/balance
handle(<<"GET">>, [<<"wallet">>, Addr, <<"balance">>], Req, _Pid) ->
	case ar_util:safe_decode(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>, Req};
		{ok, AddrOK} ->
			%% ar_node:get_balance/2 can time out which is not suitable for this
			%% use-case. It would be better if it never timed out so that Cowboy
			%% would handle the timeout instead.
			case ar_node:get_balance(whereis(http_entrypoint_node), AddrOK) of
				node_unavailable ->
					{503, #{}, <<"Internal timeout.">>, Req};
				Balance ->
					{200, #{}, integer_to_binary(Balance), Req}
			end
	end;

%% @doc Return the last transaction ID (hash) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/last_tx
handle(<<"GET">>, [<<"wallet">>, Addr, <<"last_tx">>], Req, _Pid) ->
	case ar_util:safe_decode(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>, Req};
		{ok, AddrOK} ->
			{200, #{},
				ar_util:encode(
					?OK(ar_node:get_last_tx(whereis(http_entrypoint_node), AddrOK))
				),
			Req}
	end;

%% @doc Return a block anchor to use for building transactions.
handle(<<"GET">>, [<<"tx_anchor">>], Req, _Pid) ->
	case ar_node:get_block_index(whereis(http_entrypoint_node)) of
		[] ->
			{400, #{}, <<"The node has not joined the network yet.">>, Req};
		BI when is_list(BI) ->
			{
				200,
				#{},
				ar_util:encode(
					element(1, lists:nth(min(length(BI), (?MAX_TX_ANCHOR_DEPTH)) div 2 + 1, BI))
				),
				Req
			}
	end;

%% @doc Return transaction identifiers (hashes) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txs
handle(<<"GET">>, [<<"wallet">>, Addr, <<"txs">>], Req, _Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	{Status, Headers, Body} = handle_get_wallet_txs(Addr, none),
	{Status, Headers, Body, Req};

%% @doc Return transaction identifiers (hashes) starting from the earliest_tx for the wallet
%% specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txs/{earliest_tx}
handle(<<"GET">>, [<<"wallet">>, Addr, <<"txs">>, EarliestTX], Req, _Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	{Status, Headers, Body} = handle_get_wallet_txs(Addr, ar_util:decode(EarliestTX)),
	{Status, Headers, Body, Req};

%% @doc Return identifiers (hashes) of transfer transactions depositing to the given wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/deposits
handle(<<"GET">>, [<<"wallet">>, Addr, <<"deposits">>], Req, _Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	case catch ar_arql_db:select_txs_by([{to, [Addr]}]) of
		TXMaps when is_list(TXMaps) ->
			TXIDs = lists:map(fun(#{ id := ID }) -> ID end, TXMaps),
			{200, #{}, ar_serialize:jsonify(TXIDs), Req};
		{'EXIT', {timeout, {gen_server, call, [ar_arql_db, _]}}} ->
			{503, #{}, <<"ArQL unavailable.">>, Req}
	end;

%% @doc Return identifiers (hashes) of transfer transactions depositing to the given wallet_address
%% starting from the earliest_deposit.
%% GET request to endpoint /wallet/{wallet_address}/deposits/{earliest_deposit}
handle(<<"GET">>, [<<"wallet">>, Addr, <<"deposits">>, EarliestDeposit], Req, _Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	case catch ar_arql_db:select_txs_by([{to, [Addr]}]) of
		TXMaps when is_list(TXMaps) ->
			TXIDs = lists:map(fun(#{ id := ID }) -> ID end, TXMaps),
			{Before, After} = lists:splitwith(fun(T) -> T /= EarliestDeposit end, TXIDs),
			FilteredTXs = case After of
				[] ->
					Before;
				[EarliestDeposit | _] ->
					Before ++ [EarliestDeposit]
			end,
			{200, #{}, ar_serialize:jsonify(FilteredTXs), Req};
		{'EXIT', {timeout, {gen_server, call, [ar_arql_db, _]}}} ->
			{503, #{}, <<"ArQL unavailable.">>, Req}
	end;

%% @doc Return the encrypted blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/encrypted
%handle(<<"GET">>, [<<"block">>, <<"hash">>, Hash, <<"encrypted">>], _Req) ->
	%ar:d({resp_block_hash, Hash}),
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, ar_http_iface_middleware:split_path(cowboy_req:path(Req))}]),
	%case ar_key_db:get(ar_util:decode(Hash)) of
	%	[{Key, Nonce}] ->
	%		return_encrypted_block(
	%			ar_node:get_block(
	%				whereis(http_entrypoint_node),
	%				ar_util:decode(Hash)
	%			),
	%			Key,
	%			Nonce
	%		);
	%	not_found ->
	%		ar:d(not_found_block),
	%		return_encrypted_block(unavailable)
	% end;

%% @doc Return the blockshadow corresponding to the indep_hash / height.
%% GET request to endpoint /block/{height|hash}/{indep_hash|height}
handle(<<"GET">>, [<<"block">>, Type, ID], Req, _Pid) ->
	Filename =
		case Type of
			<<"hash">> ->
				case hash_to_filename(block, ID) of
					{error, invalid}        -> invalid_hash;
					{error, _, unavailable} -> unavailable;
					{ok, Fn}                -> Fn
				end;
			<<"height">> ->
				try binary_to_integer(ID) of
					Int ->
						ar_storage:lookup_block_filename(Int)
				catch _:_ ->
					invalid_height
				end
		end,
	case Filename of
		invalid_hash ->
			{400, #{}, <<"Invalid height.">>, Req};
		invalid_height ->
			{400, #{}, <<"Invalid hash.">>, Req};
		unavailable ->
			{404, #{}, <<"Block not found.">>, Req};
		_  ->
			case {ar_meta_db:get(api_compat), cowboy_req:header(<<"x-block-format">>, Req, <<"2">>)} of
				{false, <<"1">>} ->
					{426, #{}, <<"Client version incompatible.">>, Req};
				{_, _} ->
					{200, #{}, sendfile(Filename), Req}
			end
	end;

%% @doc Return block or block field.
handle(<<"GET">>, [<<"block">>, Type, IDBin, Field], Req, _Pid) ->
	case validate_get_block_type_id(Type, IDBin) of
		{error, {Status, Headers, Body}} ->
			{Status, Headers, Body, Req};
		{ok, ID} ->
			process_request(get_block, [Type, ID, Field], Req)
	end;

%% @doc Return the current block.
%% GET request to endpoint /current_block
%% GET request to endpoint /block/current
handle(<<"GET">>, [<<"block">>, <<"current">>], Req, Pid) ->
	case ar_node:get_block_index(whereis(http_entrypoint_node)) of
		[] -> {404, #{}, <<"Block not found.">>, Req};
		[{IndepHash,_}|_] ->
			handle(<<"GET">>, [<<"block">>, <<"hash">>, ar_util:encode(IndepHash)], Req, Pid)
	end;

%% DEPRECATED (12/07/2018)
handle(<<"GET">>, [<<"current_block">>], Req, Pid) ->
	handle(<<"GET">>, [<<"block">>, <<"current">>], Req, Pid);

%% @doc Return a given field of the transaction specified by the transaction ID (hash).
%% GET request to endpoint /tx/{hash}/{field}
%%
%% {field} := { id | last_tx | owner | tags | target | quantity | data | signature | reward }
%%
handle(<<"GET">>, [<<"tx">>, Hash, Field], Req, _Pid) ->
	case hash_to_filename(tx, Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>, Req};
		{error, ID, unavailable} ->
			case is_a_pending_tx(ID) of
				true ->
					{202, #{}, <<"Pending">>, Req};
				false ->
					{404, #{}, <<"Not Found.">>, Req}
			end;
		{ok, Filename} ->
			case Field of
				<<"tags">> ->
					TX = ar_storage:read_tx_file(Filename),
					{200, #{}, ar_serialize:jsonify(
						lists:map(
							fun({Name, Value}) ->
								{
									[
										{name, ar_util:encode(Name)},
										{value, ar_util:encode(Value)}
									]
								}
							end,
							TX#tx.tags
						)
					), Req};
				_ ->
					{ok, JSONBlock} = file:read_file(Filename),
					{TXJSON} = ar_serialize:dejsonify(JSONBlock),
					Res = val_for_key(Field, TXJSON),
					{200, #{}, Res, Req}
			end
	end;

%% @doc Return the current block hieght, or 500
handle(Method, [<<"height">>], Req, _Pid)
		when (Method == <<"GET">>) or (Method == <<"HEAD">>) ->
	case ar_node:get_height(whereis(http_entrypoint_node)) of
		-1 -> {503, #{}, <<"Node has not joined the network yet.">>, Req};
		H -> {200, #{}, integer_to_binary(H), Req}
	end;

%% @doc If we are given a hash with no specifier (block, tx, etc), assume that
%% the user is requesting the data from the TX associated with that hash.
%% Optionally allow a file extension.
handle(<<"GET">>, [<<Hash:43/binary, MaybeExt/binary>>], Req, Pid) ->
	handle(<<"GET">>, [<<"tx">>, Hash, <<"data.", MaybeExt/binary>>], Req, Pid);

%% @doc Catch case for requests made to unknown endpoints.
%% Returns error code 400 - Request type not found.
handle(_, _, Req, _Pid) ->
	not_found(Req).

% Cowlib does not yet support status code 208 properly.
% See https://github.com/ninenines/cowlib/pull/79
handle208(208) -> <<"208 Already Reported">>;
handle208(Status) -> Status.

arweave_peer(Req) ->
	{{IpV4_1, IpV4_2, IpV4_3, IpV4_4}, _TcpPeerPort} = cowboy_req:peer(Req),
	ArweavePeerPort =
		case cowboy_req:header(<<"x-p2p-port">>, Req) of
			undefined -> ?DEFAULT_HTTP_IFACE_PORT;
			Binary -> binary_to_integer(Binary)
		end,
	{IpV4_1, IpV4_2, IpV4_3, IpV4_4, ArweavePeerPort}.

format_bi_for_peer(BI, Req) ->
	case cowboy_req:header(<<"x-block-format">>, Req, <<"2">>) of
		<<"2">> -> ?BI_TO_BHL(BI);
		_ -> BI
	end.

sendfile(Filename) ->
	{sendfile, 0, filelib:file_size(Filename), Filename}.

arql_semaphore(#{'_ar_http_iface_middleware_arql_semaphore' := Name}) ->
	Name.

not_found(Req) ->
	{400, #{}, <<"Request type not found.">>, Req}.

%% @doc Get the filename for an encoded TX id.
get_tx_filename(Hash) ->
	case hash_to_filename(tx, Hash) of
		{error, invalid} ->
			{response, {400, #{}, <<"Invalid hash.">>}};
		{error, ID, unavailable} ->
			case is_a_pending_tx(ID) of
				true ->
					{response, {202, #{}, <<"Pending">>}};
				false ->
					case ar_tx_db:get_error_codes(ID) of
						{ok, ErrorCodes} ->
							ErrorBody = list_to_binary(lists:join(" ", ErrorCodes)),
							{response, {410, #{}, ErrorBody}};
						not_found ->
							{response, {404, #{}, <<"Not Found.">>}}
					end
			end;
		{ok, Filename} ->
			{ok, Filename}
	end.

estimate_tx_price(SizeInBytesBinary, WalletAddr) ->
	SizeInBytes = binary_to_integer(SizeInBytesBinary),
	Node = whereis(http_entrypoint_node),
	Height = ar_node:get_height(Node),
	CurrentDiff = ar_node:get_diff(Node),
	%% Add a safety buffer to prevent transactions
	%% from being rejected after a retarget when the
	%% difficulty drops
	NextDiff = ar_difficulty:twice_smaller_diff(CurrentDiff),
	Timestamp  = os:system_time(seconds),
	CurrentDiffPrice = estimate_tx_price(SizeInBytes, CurrentDiff, Height, WalletAddr, Timestamp),
	NextDiffPrice = estimate_tx_price(SizeInBytes, NextDiff, Height + 1, WalletAddr, Timestamp),
	max(NextDiffPrice, CurrentDiffPrice).

estimate_tx_price(SizeInBytes, Diff, Height, WalletAddr, Timestamp) ->
	case WalletAddr of
		no_wallet ->
			ar_tx:calculate_min_tx_cost(
				SizeInBytes,
				Diff,
				Height,
				Timestamp
			);
		Addr ->
			ar_tx:calculate_min_tx_cost(
				SizeInBytes,
				Diff,
				Height,
				ar_node:get_wallet_list(whereis(http_entrypoint_node)),
				Addr,
				Timestamp
			)
	end.

handle_get_wallet_txs(Addr, EarliestTXID) ->
	case ar_util:safe_decode(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, _} ->
			case catch ar_arql_db:select_txs_by([{from, [Addr]}]) of
				TXMaps when is_list(TXMaps) ->
					TXIDs = lists:map(
						fun(#{ id := ID }) -> ar_util:decode(ID) end,
						TXMaps
					),
					RecentTXIDs = get_wallet_txs(EarliestTXID, TXIDs),
					EncodedTXIDs = lists:map(fun ar_util:encode/1, RecentTXIDs),
					{200, #{}, ar_serialize:jsonify(EncodedTXIDs)};
				{'EXIT', {timeout, {gen_server, call, [ar_arql_db, _]}}} ->
					{503, #{}, <<"ArQL unavailable.">>}
			end
	end.

%% @doc Returns a list of all TX IDs starting with the last one to EarliestTXID (inclusive)
%% for the same wallet.
get_wallet_txs(EarliestTXID, TXIDs) ->
	lists:reverse(get_wallet_txs(EarliestTXID, TXIDs, [])).

get_wallet_txs(_EarliestTXID, [], Acc) ->
	Acc;
get_wallet_txs(EarliestTXID, [TXID | TXIDs], Acc) ->
	case TXID of
		EarliestTXID ->
			[EarliestTXID | Acc];
		_ ->
			get_wallet_txs(EarliestTXID, TXIDs, [TXID | Acc])
	end.

handle_post_tx(PeerIP, TX) ->
	Node = whereis(http_entrypoint_node),
	MempoolTXs = ar_node:get_pending_txs(Node),
	Height = ar_node:get_height(Node),
	case verify_mempool_txs_size(MempoolTXs, TX, Height) of
		invalid ->
			handle_post_tx_no_mempool_space_response();
		valid ->
			handle_post_tx(PeerIP, Node, TX, Height, [MTX#tx.id || MTX <- MempoolTXs])
	end.

handle_post_tx(PeerIP, Node, TX, Height, MempoolTXIDs) ->
	WalletList = ar_node:get_wallet_list(Node),
	OwnerAddr = ar_wallet:to_address(TX#tx.owner),
	case lists:keyfind(OwnerAddr, 1, WalletList) of
		{_, Balance, _} when (TX#tx.reward + TX#tx.quantity) > Balance ->
			ar:info([
				submitted_txs_exceed_balance,
				{owner, ar_util:encode(OwnerAddr)},
				{balance, Balance},
				{tx_cost, TX#tx.reward + TX#tx.quantity}
			]),
			handle_post_tx_exceed_balance_response();
		_ ->
			handle_post_tx(PeerIP, Node, TX, Height, MempoolTXIDs, WalletList)
	end.

handle_post_tx(PeerIP, Node, TX, Height, MempoolTXIDs, WalletList) ->
	Diff = ar_node:get_current_diff(Node),
	{ok, BlockTXPairs} = ar_node:get_block_txs_pairs(Node),
	case ar_tx_replay_pool:verify_tx(
		TX,
		Diff,
		Height,
		BlockTXPairs,
		MempoolTXIDs,
		WalletList
	) of
		{invalid, tx_verification_failed} ->
			handle_post_tx_verification_response();
		{invalid, last_tx_in_mempool} ->
			handle_post_tx_last_tx_in_mempool_response();
		{invalid, invalid_last_tx} ->
			handle_post_tx_verification_response();
		{invalid, tx_bad_anchor} ->
			handle_post_tx_bad_anchor_response();
		{invalid, tx_already_in_weave} ->
			handle_post_tx_already_in_weave_response();
		{invalid, tx_already_in_mempool} ->
			handle_post_tx_already_in_mempool_response();
		{valid, _, _} ->
			handle_post_tx_accepted(PeerIP, TX)
	end.

verify_mempool_txs_size(MempoolTXs, TX, Height) ->
	case ar_fork:height_1_8() of
		H when Height >= H ->
			verify_mempool_txs_size(MempoolTXs, TX);
		_ ->
			valid
	end.

verify_mempool_txs_size(MempoolTXs, TX) ->
	TotalSize = lists:foldl(
		fun(MempoolTX, Sum) ->
			Sum + byte_size(MempoolTX#tx.data)
		end,
		0,
		MempoolTXs
	),
	case byte_size(TX#tx.data) + TotalSize of
		Size when Size > ?TOTAL_WAITING_TXS_DATA_SIZE_LIMIT ->
			invalid;
		_ ->
			valid
	end.

handle_post_tx_accepted(PeerIP, TX) ->
	%% Exclude successful requests with valid transactions from the
	%% IP-based throttling, to avoid connectivity issues at the times
	%% of excessive transaction volumes.
	ar_blacklist_middleware:decrement_ip_addr(PeerIP),
	ar:info([
		ar_http_iface_handler,
		accepted_tx,
		{id, ar_util:encode(TX#tx.id)}
	]),
	ar_bridge:add_tx(whereis(http_bridge_node), TX),
	ok.

handle_post_tx_exceed_balance_response() ->
	{error_response, {400, #{}, <<"Waiting TXs exceed balance for wallet.">>}}.

handle_post_tx_verification_response() ->
	{error_response, {400, #{}, <<"Transaction verification failed.">>}}.

handle_post_tx_last_tx_in_mempool_response() ->
	{error_response, {400, #{}, <<"Invalid anchor (last_tx from mempool).">>}}.

handle_post_tx_no_mempool_space_response() ->
	ar:err([ar_http_iface_middleware, rejected_transaction, {reason, mempool_is_full}]),
	{error_response, {400, #{}, <<"Mempool is full.">>}}.

handle_post_tx_bad_anchor_response() ->
	{error_response, {400, #{}, <<"Invalid anchor (last_tx).">>}}.

handle_post_tx_already_in_weave_response() ->
	{error_response, {400, #{}, <<"Transaction is already on the weave.">>}}.

handle_post_tx_already_in_mempool_response() ->
	{error_response, {400, #{}, <<"Transaction is already in the mempool.">>}}.

check_internal_api_secret(Req) ->
	Reject = fun(Msg) ->
		log_internal_api_reject(Msg, Req),
		timer:sleep(rand:uniform(1000) + 1000), % Reduce efficiency of timing attacks by sleeping randomly between 1-2s.
		{reject, {421, #{}, <<"Internal API disabled or invalid internal API secret in request.">>}}
	end,
	case {ar_meta_db:get(internal_api_secret), cowboy_req:header(<<"x-internal-api-secret">>, Req)} of
		{not_set, _} ->
			Reject("Request to disabled internal API");
		{_Secret, _Secret} when is_binary(_Secret) ->
			pass;
		_ ->
			Reject("Invalid secret for internal API request")
	end.

log_internal_api_reject(Msg, Req) ->
	spawn(fun() ->
		Path = ar_http_iface_server:split_path(cowboy_req:path(Req)),
		{IpAddr, _Port} = cowboy_req:peer(Req),
		BinIpAddr = list_to_binary(inet:ntoa(IpAddr)),
		ar:warn("~s: IP address: ~s Path: ~p", [Msg, BinIpAddr, Path])
	end).

%% @doc Convert a blocks field with the given label into a string
block_field_to_string(<<"nonce">>, Res) -> Res;
block_field_to_string(<<"previous_block">>, Res) -> Res;
block_field_to_string(<<"timestamp">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"last_retarget">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"diff">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"height">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"hash">>, Res) -> Res;
block_field_to_string(<<"indep_hash">>, Res) -> Res;
block_field_to_string(<<"txs">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"hash_list">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"wallet_list">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"reward_addr">>, Res) -> Res.

%% @doc checks if hash is valid & if so returns filename.
hash_to_filename(Type, Hash) ->
	case ar_util:safe_decode(Hash) of
		{error, invalid} ->
			{error, invalid};
		{ok, ID} ->
			{Mod, Fun} = type_to_mf({Type, lookup_filename}),
			F = apply(Mod, Fun, [ID]),
			case F of
				unavailable ->
					{error, ID, unavailable};
				Filename ->
					{ok, Filename}
			end
	end.

%% @doc Return true if ID is a pending tx.
is_a_pending_tx(ID) ->
	lists:member(
		ID,
		ar_node:get_pending_txs(whereis(http_entrypoint_node), [id_only])
	).

%% @doc Given a request, returns a blockshadow.
request_to_struct_with_blockshadow(Req, BlockJSON) ->
	try
		{Struct} = ar_serialize:dejsonify(BlockJSON),
		JSONB = val_for_key(<<"new_block">>, Struct),
		BShadow = ar_serialize:json_struct_to_block(JSONB),
		{ok, {Struct, BShadow}, Req}
	catch
		Exception:Reason ->
			{error, {Exception, Reason}, Req}
	end.

%% @doc Generate and return an informative JSON object regarding
%% the state of the node.
return_info(Req) ->
	{Time, Current} =
		timer:tc(fun() -> ar_node:get_current_block_hash(whereis(http_entrypoint_node)) end),
	{Time2, Height} =
		timer:tc(fun() -> ar_node:get_height(whereis(http_entrypoint_node)) end),
	{200, #{},
		ar_serialize:jsonify(
			{
				[
					{network, list_to_binary(?NETWORK_NAME)},
					{version, ?CLIENT_VERSION},
					{release, ?RELEASE_NUMBER},
					{height,
						case Height of
							not_joined -> -1;
							H -> H
						end
					},
					{current,
						case is_atom(Current) of
							true -> atom_to_binary(Current, utf8);
							false -> ar_util:encode(Current)
						end
					},
					{blocks, ar_storage:blocks_on_disk()},
					{peers, length(ar_bridge:get_remote_peers(whereis(http_bridge_node)))},
					{queue_length,
						element(
							2,
							erlang:process_info(whereis(http_entrypoint_node), message_queue_len)
						)
					},
					{node_state_latency, (Time + Time2) div 2}
				]
			}
		),
	Req}.

%% @doc converts a tuple of atoms to a {Module, Function} tuple.
type_to_mf({tx, lookup_filename}) ->
	{ar_storage, lookup_tx_filename};
type_to_mf({block, lookup_filename}) ->
	{ar_storage, lookup_block_filename}.

%% @doc Convenience function for lists:keyfind(Key, 1, List).
%% returns Value not {Key, Value}.
val_for_key(K, L) ->
	case lists:keyfind(K, 1, L) of
		false -> false;
		{K, V} -> V
	end.

%% @doc Handle multiple steps of POST /block. First argument is a subcommand,
%% second the argument for that subcommand.
post_block(request, {Req, Pid}) ->
	OrigPeer = arweave_peer(Req),
	case ar_blacklist_middleware:is_peer_banned(OrigPeer) of
		not_banned ->
			post_block(read_blockshadow, OrigPeer, {Req, Pid});
		banned ->
			{403, #{}, <<"IP address blocked due to previous request.">>, Req}
	end.
post_block(read_blockshadow, OrigPeer, {Req, Pid}) ->
	HeaderBlockHashKnown = case cowboy_req:header(<<"arweave-block-hash">>, Req, not_set) of
		not_set ->
			false;
		EncodedBH ->
			case ar_util:safe_decode(EncodedBH) of
				{ok, BH} ->
					ar_bridge:is_id_ignored(BH);
				{error, invalid} ->
					false
			end
	end,
	case HeaderBlockHashKnown of
		true ->
			{208, <<"Block already processed.">>, Req};
		false ->
			case read_complete_body(Req, Pid) of
				{ok, BlockJSON, Req2} ->
					case request_to_struct_with_blockshadow(Req2, BlockJSON) of
						{error, {_, _}, ReadReq} ->
							{400, #{}, <<"Invalid block.">>, ReadReq};
						{ok, {ReqStruct, BShadow}, ReadReq} ->
							post_block(check_data_segment_processed, {ReqStruct, BShadow, OrigPeer}, ReadReq)
					end;
				{error, body_size_too_large} ->
					{413, #{}, <<"Payload too large">>, Req}
			end
	end;
post_block(check_data_segment_processed, {ReqStruct, BShadow, OrigPeer}, Req) ->
	% Check if block is already known.
	case lists:keyfind(<<"block_data_segment">>, 1, ReqStruct) of
		{_, BDSEncoded} ->
			BDS = ar_util:decode(BDSEncoded),
			case ar_bridge:is_id_ignored(BDS) of
				true ->
					{208, #{}, <<"Block Data Segment already processed.">>, Req};
				false ->
					post_block(check_indep_hash_processed, {ReqStruct, BShadow, OrigPeer, BDS}, Req)
			end;
		false ->
			post_block_reject_warn(BShadow, block_data_segment_missing, OrigPeer),
			{400, #{}, <<"block_data_segment missing.">>, Req}
	end;
post_block(check_indep_hash_processed, {ReqStruct, BShadow, OrigPeer, BDS}, Req) ->
	case ar_bridge:is_id_ignored(BShadow#block.indep_hash) of
		true ->
			{208, <<"Block already processed.">>, Req};
		false ->
			ar_bridge:ignore_id(BShadow#block.indep_hash),
			post_block(check_is_joined, {ReqStruct, BShadow, OrigPeer, BDS}, Req)
	end;
post_block(check_is_joined, {ReqStruct, BShadow, OrigPeer, BDS}, Req) ->
	% Check if node is joined.
	case ar_node:is_joined(whereis(http_entrypoint_node)) of
		false ->
			{503, #{}, <<"Not joined.">>, Req};
		true ->
			post_block(check_height, {ReqStruct, BShadow, OrigPeer, BDS}, Req)
	end;
post_block(check_height, {ReqStruct, BShadow, OrigPeer, BDS}, Req) ->
	CurrentHeight = ar_node:get_height(whereis(http_entrypoint_node)),
	case BShadow#block.height of
		H when H < CurrentHeight - ?STORE_BLOCKS_BEHIND_CURRENT ->
			{400, #{}, <<"Height is too far behind">>, Req};
		H when H > CurrentHeight + ?STORE_BLOCKS_BEHIND_CURRENT ->
			{400, #{}, <<"Height is too far ahead">>, Req};
		_ ->
			post_block(check_difficulty, {ReqStruct, BShadow, OrigPeer, BDS}, Req)
	end;
%% The min difficulty check is filtering out blocks from smaller networks, e.g.
%% testnets. Therefor, we don't want to log when this check or any check above
%% rejects the block because there are potentially a lot of rejections.
post_block(check_difficulty, {ReqStruct, BShadow, OrigPeer, BDS}, Req) ->
	case BShadow#block.diff >= ar_mine:min_difficulty(BShadow#block.height) of
		true ->
			post_block(check_pow, {ReqStruct, BShadow, OrigPeer, BDS}, Req);
		_ ->
			{400, #{}, <<"Difficulty too low">>, Req}
	end;
%% Note! Checking PoW should be as cheap as possible. All slow steps should
%% be after the PoW check to reduce the possibility of doing a DOS attack on
%% the network.
post_block(check_pow, {ReqStruct, BShadow, OrigPeer, BDS}, Req) ->
	case ar_mine:validate(BDS, BShadow#block.nonce, BShadow#block.diff, BShadow#block.height) of
		{invalid, _} ->
			post_block_reject_warn(BShadow, check_pow, OrigPeer),
			ar_blacklist_middleware:ban_peer(OrigPeer, ?BAD_POW_BAN_TIME),
			{400, #{}, <<"Invalid Block Proof of Work">>, Req};
		{valid, _} ->
			ar_bridge:ignore_id(BDS),
			post_block(check_timestamp, {ReqStruct, BShadow, OrigPeer, BDS}, Req)
	end;
post_block(check_timestamp, {ReqStruct, BShadow, OrigPeer, BDS}, Req) ->
	%% Verify the timestamp of the block shadow.
	case ar_block:verify_timestamp(BShadow) of
		false ->
			post_block_reject_warn(
				BShadow,
				check_timestamp,
				OrigPeer,
				[{block_time, BShadow#block.timestamp},
				 {current_time, os:system_time(seconds)}]
			),
			{400, #{}, <<"Invalid timestamp.">>, Req};
		true ->
			post_block(post_block, {ReqStruct, BShadow, OrigPeer, BDS}, Req)
	end;
post_block(post_block, {ReqStruct, BShadow, OrigPeer, BDS}, Req) ->
	%% The ar_block:generate_block_from_shadow/2 call is potentially slow. Since
	%% all validation steps already passed, we can do the rest in a separate
	spawn(fun() ->
		Recall = 
			case val_for_key(<<"recall_block">>, ReqStruct) of
				false -> BShadow#block.poa;
				RecallH ->
					RecallSize = val_for_key(<<"recall_size">>, ReqStruct),
					Key = ar_util:decode(val_for_key(<<"key">>, ReqStruct)),
					Nonce = ar_util:decode(val_for_key(<<"nonce">>, ReqStruct)),
					{RecallH, RecallSize, Key, Nonce}
			end,
		ar:info([{
			sending_external_block_to_bridge,
			ar_util:encode(BShadow#block.indep_hash)
		}]),
		ar:info([
			ar_http_iface_handler,
			accepted_block,
			{indep_hash, ar_util:encode(BShadow#block.indep_hash)}
		]),
		ar_bridge:add_block(
			whereis(http_bridge_node),
			OrigPeer,
			BShadow,
			BDS,
			Recall
		)
	end),
	{200, #{}, <<"OK">>, Req}.

post_block_reject_warn(BShadow, Step, Peer) ->
	ar:warn([
		{post_block_rejected, ar_util:encode(BShadow#block.indep_hash)},
		Step,
		{peer, ar_util:format_peer(Peer)}
	]).

post_block_reject_warn(BShadow, Step, Peer, Params) ->
	ar:warn([
		{post_block_rejected, ar_util:encode(BShadow#block.indep_hash)},
		{Step, Params},
		{peer, ar_util:format_peer(Peer)}
	]).

%% @doc Return the block hash list associated with a block.
process_request(get_block, [Type, ID, <<"hash_list">>], Req) ->
	CurrentBI = ar_node:get_block_index(whereis(http_entrypoint_node)),
	case is_block_known(Type, ID, CurrentBI) of
		true ->
			Hash =
				case Type of
					<<"height">> ->
						B =
							ar_node:get_block(whereis(http_entrypoint_node),
							ID,
							CurrentBI),
						B#block.indep_hash;
					<<"hash">> -> ID
				end,
			BlockHL = ar_block:generate_hash_list_for_block(Hash, CurrentBI),
			{200, #{},
				ar_serialize:jsonify(
					lists:map(fun ar_util:encode/1, BlockHL)
				),
			Req};
		false ->
			{404, #{}, <<"Block not found.">>, Req}
	end;
%% @doc Return the wallet list associated with a block (as referenced by hash
%% or height).
process_request(get_block, [_Type, ID, <<"wallet_list">>], Req) ->
	case ar_block_index:get_block_filename(ID) of
		unavailable ->
			{404, #{}, <<"Block not found.">>, Req};
		Filename ->
			{ok, Binary} = file:read_file(Filename),
			B = ar_serialize:json_struct_to_block(Binary),
			WLHash = B#block.wallet_list,
			WLFilepath = ar_storage:wallet_list_filepath(WLHash),
			case filelib:is_file(WLFilepath) of
				true ->
					{200, #{}, sendfile(WLFilepath), Req};
				false ->
					{404, #{}, <<"Block not found.">>, Req}
			end
	end;
%% @doc Return a given field for the the blockshadow corresponding to the block height, 'height'.
%% GET request to endpoint /block/hash/{hash|height}/{field}
%%
%% {field} := { nonce | previous_block | timestamp | last_retarget | diff | height | hash | indep_hash
%%				txs | hash_list | wallet_list | reward_addr | tags | reward_pool }
%%
process_request(get_block, [Type, ID, Field], Req) ->
	CurrentBI = ar_node:get_block_index(whereis(http_entrypoint_node)),
	case ar_meta_db:get(subfield_queries) of
		true ->
			case find_block(Type, ID, CurrentBI) of
				unavailable ->
					{404, #{}, <<"Not Found.">>, Req};
				B ->
					{BLOCKJSON} = ar_serialize:block_to_json_struct(B),
					{_, Res} = lists:keyfind(list_to_existing_atom(binary_to_list(Field)), 1, BLOCKJSON),
					Result = block_field_to_string(Field, Res),
					{200, #{}, Result, Req}
			end;
		_ ->
			{421, #{}, <<"Subfield block querying is disabled on this node.">>, Req}
	end.

validate_get_block_type_id(<<"height">>, ID) ->
	try binary_to_integer(ID) of
		Int -> {ok, Int}
	catch _:_ ->
		{error, {400, #{}, <<"Invalid height.">>}}
	end;
validate_get_block_type_id(<<"hash">>, ID) ->
	case ar_util:safe_decode(ID) of
		{ok, Hash} -> {ok, Hash};
		{error, invalid} -> {error, {400, #{}, <<"Invalid hash.">>}}
	end.

%% @doc Take a block type specifier, an ID, and a BI, returning whether the
%% given block is part of the BI.
is_block_known(<<"height">>, RawHeight, BI) when is_binary(RawHeight) ->
	is_block_known(<<"height">>, binary_to_integer(RawHeight), BI);
is_block_known(<<"height">>, Height, BI) ->
	Height < length(BI);
is_block_known(<<"hash">>, ID, BI) ->
	lists:member(ID, ?BI_TO_BHL(BI)).

%% @doc Find a block, given a type and a specifier.
find_block(<<"height">>, RawHeight, BI) ->
	ar_node:get_block(
		whereis(http_entrypoint_node),
		binary_to_integer(RawHeight),
		BI
	);
find_block(<<"hash">>, ID, BI) ->
	ar_storage:read_block(ID, BI).

post_tx_parse_id({Req, Pid}) ->
	post_tx_parse_id(check_header, {Req, Pid}).

post_tx_parse_id(check_header, {Req, Pid}) ->
	case cowboy_req:header(<<"arweave-tx-id">>, Req, not_set) of
		not_set ->
			post_tx_parse_id(check_body, {Req, Pid});
		EncodedTXID ->
			case ar_util:safe_decode(EncodedTXID) of
				{ok, TXID} ->
					post_tx_parse_id(check_ignore_list, {TXID, Req, Pid, <<>>});
				{error, invalid} ->
					{error, invalid_hash, Req}
			end
	end;
post_tx_parse_id(check_body, {Req, Pid}) ->
	{_, Chunk, Req2} = read_body_chunk(Req, Pid, 100, 10),
	case re:run(Chunk, <<"\"id\":\s*\"(?<ID>[A-Za-z0-9_-]{43})\"">>, [{capture, ['ID']}]) of
		{match, [Part]} ->
			TXID = ar_util:decode(binary:part(Chunk, Part)),
			post_tx_parse_id(check_ignore_list, {TXID, Req2, Pid, Chunk});
		_ ->
			post_tx_parse_id(read_body, {not_set, Req2, Pid, <<>>})
	end;
post_tx_parse_id(check_ignore_list, {TXID, Req, Pid, FirstChunk}) ->
	case ar_bridge:is_id_ignored(TXID) of
		true ->
			{error, tx_already_processed, Req};
		false ->
			ar_bridge:ignore_id(TXID),
			post_tx_parse_id(read_body, {TXID, Req, Pid, FirstChunk})
	end;
post_tx_parse_id(read_body, {TXID, Req, Pid, FirstChunk}) ->
	case read_complete_body(Req, Pid) of
		{ok, SecondChunk, Req2} ->
			Body = iolist_to_binary([FirstChunk | SecondChunk]),
			post_tx_parse_id(parse_json, {TXID, Req2, Body});
		{error, body_size_too_large} ->
			{error, body_size_too_large, Req}
	end;
post_tx_parse_id(parse_json, {TXID, Req, Body}) ->
	case catch ar_serialize:json_struct_to_tx(Body) of
		{'EXIT', _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_bridge:unignore_id(TXID)
			end,
			{error, invalid_json, Req};
		TX ->
			post_tx_parse_id(verify_id_match, {TXID, Req, TX})
	end;
post_tx_parse_id(verify_id_match, {MaybeTXID, Req, TX}) ->
	TXID = TX#tx.id,
	case MaybeTXID of
		TXID ->
			{ok, TX};
		MaybeNotSet ->
			case MaybeNotSet of
				not_set ->
					noop;
				MismatchingTXID ->
					ar_bridge:unignore(MismatchingTXID)
			end,
			case ar_bridge:is_id_ignored(TXID) of
				true ->
					{error, tx_already_processed, Req};
				false ->
					ar_bridge:ignore_id(TXID),
					{ok, TX}
			end
	end.

read_complete_body(Req, Pid) ->
	Pid ! {read_complete_body, self(), Req},
	receive
		{read_complete_body, Term} -> Term
	end.

read_body_chunk(Req, Pid, Size, Timeout) ->
	Pid ! {read_body_chunk, self(), Req, Size, Timeout},
	receive
		{read_body_chunk, Term} -> Term
	end.
