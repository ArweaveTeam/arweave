-module(ar_http_iface_middleware).

-behaviour(cowboy_middleware).

-export([execute/2, read_body_chunk/4]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").
-include_lib("arweave/include/ar_mine.hrl").

-define(HANDLER_TIMEOUT, 55000).

-define(MAX_SERIALIZED_RECENT_HASH_LIST_DIFF, 2400). % 50 * 48.

-define(MAX_SERIALIZED_MISSING_TX_INDICES, 125). % Every byte encodes 8 positions.

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
%% @end
loop(TimeoutRef) ->
	receive
		{handled, {Status, Headers, Body, HandledReq}} ->
			timer:cancel(TimeoutRef),
			CowboyStatus = handle_custom_codes(Status),
			RepliedReq = cowboy_req:reply(CowboyStatus, Headers, Body, HandledReq),
			{stop, RepliedReq};
		{read_complete_body, From, Req, SizeLimit} ->
			Term = ar_http_req:body(Req, SizeLimit),
			From ! {read_complete_body, Term},
			loop(TimeoutRef);
		{read_body_chunk, From, Req, Size, Timeout} ->
			Term = ar_http_req:read_body_chunk(Req, Size, Timeout),
			From ! {read_body_chunk, Term},
			loop(TimeoutRef);
		{timeout, HandlerPid, InitialReq} ->
			unlink(HandlerPid),
			exit(HandlerPid, handler_timeout),
			?LOG_WARNING([
				{event, handler_timeout},
				{method, cowboy_req:method(InitialReq)},
				{path, cowboy_req:path(InitialReq)}
			]),
			RepliedReq = cowboy_req:reply(500, #{}, <<"Handler timeout">>, InitialReq),
			{stop, RepliedReq}
	end.

handle(Req, Pid) ->
	Peer = ar_http_util:arweave_peer(Req),
	handle(Peer, Req, Pid).

handle(Peer, Req, Pid) ->
	Method = cowboy_req:method(Req),
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(http_logging, Config#config.enable) of
		true ->
			?LOG_INFO([
				{event, http_request},
				{method, Method},
				{path, SplitPath},
				{peer, ar_util:format_peer(Peer)}
			]);
		_ ->
			do_nothing
	end,
	case handle4(Method, SplitPath, Req, Pid) of
		{Status, Hdrs, Body, HandledReq} ->
			{Status, maps:merge(?CORS_HEADERS, Hdrs), Body, HandledReq};
		{Status, Body, HandledReq} ->
			{Status, ?CORS_HEADERS, Body, HandledReq}
	end.

-ifdef(TESTNET).
handle4(<<"POST">>, [<<"mine">>], Req, _Pid) ->
	ar_node:mine(),
	{200, #{}, <<>>, Req};

handle4(<<"GET">>, [<<"tx">>, <<"ready_for_mining">>], Req, _Pid) ->
	{200, #{},
			ar_serialize:jsonify(
				lists:map(
					fun ar_util:encode/1,
					ar_node:get_ready_for_mining_txs([id_only])
				)
			),
	Req};

handle4(Method, SplitPath, Req, Pid) ->
	handle(Method, SplitPath, Req, Pid).
-else.
handle4(Method, SplitPath, Req, Pid) ->
	handle(Method, SplitPath, Req, Pid).
-endif.

%% Return network information from a given node.
%% GET request to endpoint /info.
handle(<<"GET">>, [], Req, _Pid) ->
	return_info(Req);

handle(<<"GET">>, [<<"info">>], Req, _Pid) ->
	return_info(Req);

%% Some load balancers use 'HEAD's rather than 'GET's to tell if a node
%% is alive. Appease them.
handle(<<"HEAD">>, [], Req, _Pid) ->
	{200, #{}, <<>>, Req};
handle(<<"HEAD">>, [<<"info">>], Req, _Pid) ->
	{200, #{}, <<>>, Req};

%% Return permissive CORS headers for all endpoints.
handle(<<"OPTIONS">>, [<<"block">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"tx">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"peer">> | _], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, [<<"arql">>], Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
			<<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>, Req};
handle(<<"OPTIONS">>, _, Req, _Pid) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET">>}, <<"OK">>, Req};

%% Return the current universal time in seconds.
handle(<<"GET">>, [<<"time">>], Req, _Pid) ->
	{200, #{}, integer_to_binary(os:system_time(second)), Req};

%% Return all mempool transactions.
%% GET request to endpoint /tx/pending.
handle(<<"GET">>, [<<"tx">>, <<"pending">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			{200, #{},
					ar_serialize:jsonify(
						%% Should encode
						lists:map(
							fun ar_util:encode/1,
							ar_node:get_pending_txs([id_only])
						)
					),
			Req}
	end;

%% Return outgoing transaction priority queue.
%% GET request to endpoint /queue.
%% @deprecated
handle(<<"GET">>, [<<"queue">>], Req, _Pid) ->
	{200, #{}, <<"[]">>, Req};

%% Return additional information about the transaction with the given identifier (hash).
%% GET request to endpoint /tx/{hash}/status.
handle(<<"GET">>, [<<"tx">>, Hash, <<"status">>], Req, _Pid) ->
	ar_semaphore:acquire(arql_semaphore(Req), 5000),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			handle_get_tx_status(Hash, Req)
	end;

%% Return a JSON-encoded transaction.
%% GET request to endpoint /tx/{hash}.
handle(<<"GET">>, [<<"tx">>, Hash], Req, _Pid) ->
	handle_get_tx(Hash, Req, json);

%% Return a binary-encoded transaction.
%% GET request to endpoint /tx2/{hash}.
handle(<<"GET">>, [<<"tx2">>, Hash], Req, _Pid) ->
	handle_get_tx(Hash, Req, binary);

%% Return a possibly unconfirmed JSON-encoded transaction.
%% GET request to endpoint /unconfirmed_tx/{hash}.
handle(<<"GET">>, [<<"unconfirmed_tx">>, Hash], Req, _Pid) ->
	handle_get_unconfirmed_tx(Hash, Req, json);

%% Return a possibly unconfirmed binary-encoded transaction.
%% GET request to endpoint /unconfirmed_tx2/{hash}.
handle(<<"GET">>, [<<"unconfirmed_tx2">>, Hash], Req, _Pid) ->
	handle_get_unconfirmed_tx(Hash, Req, binary);

%% Return the transaction IDs of all txs where the tags in post match the given set
%% of key value pairs. POST request to endpoint /arql with body of request being a logical
%% expression valid in ar_parser.
%%
%% Example logical expression.
%%	{
%%		op:		{ and | or | equals }
%%		expr1:	{ string | logical expression }
%%		expr2:	{ string | logical expression }
%%	}
handle(<<"POST">>, [<<"arql">>], Req, Pid) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(serve_arql, Config#config.enable) of
		true ->
			ar_semaphore:acquire(arql_semaphore(Req), 5000),
			case ar_node:is_joined() of
				false ->
					not_joined(Req);
				true ->
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
					end
			end;
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return the data field of the transaction specified via the transaction ID (hash)
%% served as HTML.
%% GET request to endpoint /tx/{hash}/data.html
handle(<<"GET">>, [<<"tx">>, Hash, << "data.", _/binary >>], Req, _Pid) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(serve_html_data, Config#config.disable) of
		true ->
			{421, #{}, <<"Serving HTML data is disabled on this node.">>, Req};
		_ ->
			case ar_util:safe_decode(Hash) of
				{error, invalid} ->
					{400, #{}, <<"Invalid hash.">>, Req};
				{ok, ID} ->
					case ar_storage:read_tx(ID) of
						unavailable ->
							{404, #{}, sendfile("data/not_found.html"), Req};
						#tx{} = TX ->
							serve_tx_html_data(Req, TX)
					end
			end
	end;

handle(<<"GET">>, [<<"sync_buckets">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			ok = ar_semaphore:acquire(get_sync_record, infinity),
			case ar_sync_record:get_serialized_sync_buckets(ar_data_sync) of
				{ok, Binary} ->
					{200, #{}, Binary, Req};
				{error, not_initialized} ->
					{500, #{}, jiffy:encode(#{ error => not_initialized }), Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end;

handle(<<"GET">>, [<<"data_sync_record">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			Format =
				case cowboy_req:header(<<"content-type">>, Req) of
					<<"application/json">> ->
						json;
					_ ->
						etf
			end,
			ok = ar_semaphore:acquire(get_sync_record, infinity),
			Options = #{ format => Format, random_subset => true },
			case ar_sync_record:get_record(Options, ar_data_sync) of
				{ok, Binary} ->
					{200, #{}, Binary, Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end;

handle(<<"GET">>, [<<"data_sync_record">>, EncodedStart, EncodedLimit], Req, _Pid) ->
	case catch binary_to_integer(EncodedStart) of
		{'EXIT', _} ->
			{400, #{}, jiffy:encode(#{ error => invalid_start_encoding }), Req};
		Start ->
			case catch binary_to_integer(EncodedLimit) of
				{'EXIT', _} ->
					{400, #{}, jiffy:encode(#{ error => invalid_limit_encoding }), Req};
				Limit ->
					case Limit > ?MAX_SHARED_SYNCED_INTERVALS_COUNT of
						true ->
							{400, #{}, jiffy:encode(#{ error => limit_too_big }), Req};
						false ->
							ok = ar_semaphore:acquire(get_sync_record, infinity),
							handle_get_data_sync_record(Start, Limit, Req)
					end
			end
	end;

handle(<<"GET">>, [<<"chunk">>, OffsetBinary], Req, _Pid) ->
	handle_get_chunk(OffsetBinary, Req, json);

handle(<<"GET">>, [<<"chunk2">>, OffsetBinary], Req, _Pid) ->
	handle_get_chunk(OffsetBinary, Req, binary);

handle(<<"GET">>, [<<"tx">>, EncodedID, <<"offset">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_util:safe_decode(EncodedID) of
				{error, invalid} ->
					{400, #{}, jiffy:encode(#{ error => invalid_address }), Req};
				{ok, ID} ->
					case ar_data_sync:get_tx_offset(ID) of
						{ok, {Offset, Size}} ->
							ResponseBody = jiffy:encode(#{
								offset => integer_to_binary(Offset),
								size => integer_to_binary(Size)
							}),
							{200, #{}, ResponseBody, Req};
						{error, not_found} ->
							{404, #{}, <<>>, Req};
						{error, failed_to_read_offset} ->
							{500, #{}, <<>>, Req};
						{error, timeout} ->
							{503, #{}, jiffy:encode(#{ error => timeout }), Req}
					end
			end
	end;

handle(<<"POST">>, [<<"chunk">>], Req, Pid) ->
	Joined =
		case ar_node:is_joined() of
			false ->
				not_joined(Req);
			true ->
				ok
		end,
	Semaphore =
		case Joined of
			ok ->
				case ar_semaphore:acquire(post_chunk, 5000) of
					ok ->
						ok;
					{error, timeout} ->
						{503, #{}, jiffy:encode(#{ error => timeout }), Req}
				end;
			Reply ->
				Reply
		end,
	DataRootKnown =
		case Semaphore of
			ok ->
				case get_data_root_from_headers(Req) of
					not_set ->
						ok;
					{ok, {DataRoot, DataSize}} ->
						case ar_data_sync:has_data_root(DataRoot, DataSize) of
							true ->
								ok;
							false ->
								{400, #{}, jiffy:encode(#{ error => data_root_not_found }),
										Req}
						end
				end;
			Reply2 ->
				Reply2
		end,
	ParseChunk =
		case DataRootKnown of
			ok ->
				parse_chunk(Req, Pid);
			Reply3 ->
				Reply3
		end,
	case ParseChunk of
		{ok, {Proof, Req2}} ->
			handle_post_chunk(Proof, Req2);
		Reply4 ->
			Reply4
	end;

%% Accept an announcement of a block. Reply 412 (no previous block),
%% 200 (optionally specifying missing transactions and chunk in the response)
%% or 208 (already processing the block).
handle(<<"POST">>, [<<"block_announcement">>], Req, Pid) ->
	case read_complete_body(Req, Pid) of
		{ok, Body, Req2} ->
			case catch ar_serialize:binary_to_block_announcement(Body) of
				{ok, #block_announcement{ indep_hash = H, previous_block = PrevH,
						tx_prefixes = Prefixes, recall_byte = RecallByte }} ->
					case ar_ignore_registry:member(H) of
						true ->
							{208, #{}, <<>>, Req};
						false ->
							case ar_node:get_block_shadow_from_cache(PrevH) of
								not_found ->
									{412, #{}, <<>>, Req};
								_ ->
									Indices = collect_missing_tx_indices(Prefixes),
									MissingChunk =
										case RecallByte of
											undefined ->
												true;
											_ ->
												prometheus_counter:inc(
														block_announcement_reported_chunks),
												case ar_sync_record:is_recorded(RecallByte + 1,
														ar_data_sync) of
													{true, spora_2_5} ->
														false;
													_ ->
														prometheus_counter:inc(
															block_announcement_missing_chunks),
														true
												end
										end,
									prometheus_counter:inc(
											block_announcement_reported_transactions,
											length(Prefixes)),
									prometheus_counter:inc(
											block_announcement_missing_transactions,
											length(Indices)),
									Response = #block_announcement_response{
											missing_chunk = MissingChunk,
											missing_tx_indices = Indices },
									{200, #{}, ar_serialize:block_announcement_response_to_binary(Response), Req2}
							end
					end;
				{'EXIT', _Reason} ->
					{400, #{}, <<>>, Req2};
				{error, _Reason} ->
					{400, #{}, <<>>, Req2}
			end;
		{error, body_size_too_large} ->
			{400, #{}, <<>>, Req}
	end;

%% Accept a JSON-encoded block with Base64Url encoded fields.
handle(<<"POST">>, [<<"block">>], Req, Pid) ->
	post_block(request, {Req, Pid, json}, erlang:timestamp());

%% Accept a binary-encoded block.
handle(<<"POST">>, [<<"block2">>], Req, Pid) ->
	erlang:put(post_block2, true),
	post_block(request, {Req, Pid, binary}, erlang:timestamp());

%% Generate a wallet and receive a secret key identifying it.
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

%% Accept a new JSON-encoded transaction.
%% POST request to endpoint /tx.
handle(<<"POST">>, [<<"tx">>], Req, Pid) ->
	handle_post_tx({Req, Pid, json});

%% Accept a new binary-encoded transaction.
%% POST request to endpoint /tx2.
handle(<<"POST">>, [<<"tx2">>], Req, Pid) ->
	handle_post_tx({Req, Pid, binary});

%% Sign and send a tx to the network.
%% Fetches the wallet by the provided key generated via POST /wallet.
%% Requires internal_api_secret startup option to be set.
%% WARNING: only use it if you really really know what you are doing.
handle(<<"POST">>, [<<"unsigned_tx">>], Req, Pid) ->
	case {ar_node:is_joined(), check_internal_api_secret(Req)} of
		{false, _} ->
			not_joined(Req);
		{true, pass} ->
			case read_complete_body(Req, Pid) of
				{ok, Body, Req2} ->
					{UnsignedTXProps} = ar_serialize:dejsonify(Body),
					WalletAccessCode =
						proplists:get_value(<<"wallet_access_code">>, UnsignedTXProps),
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
					Data = UnsignedTX#tx.data,
					DataSize = byte_size(Data),
					DataRoot = case DataSize > 0 of
						true ->
							TreeTX = ar_tx:generate_chunk_tree(#tx{ data = Data }),
							TreeTX#tx.data_root;
						false ->
							<<>>
					end,
					Format2TX = UnsignedTX#tx{
						format = 2,
						data_size = DataSize,
						data_root = DataRoot
					},
					SignedTX = ar_tx:sign(Format2TX, KeyPair),
					Peer = ar_http_util:arweave_peer(Req),
					Reply = ar_serialize:jsonify({[{<<"id">>,
							ar_util:encode(SignedTX#tx.id)}]}),
					case handle_post_tx(Req2, Peer, SignedTX) of
						ok ->
							{200, #{}, Reply, Req2};
						{error_response, {Status, Headers, ErrBody}} ->
							{Status, Headers, ErrBody, Req2}
					end;
				{error, body_size_too_large} ->
					{413, #{}, <<"Payload too large">>, Req}
			end;
		{true, {reject, {Status, Headers, Body}}} ->
			{Status, Headers, Body, Req}
	end;

%% Return the list of peers held by the node.
%% GET request to endpoint /peers.
handle(<<"GET">>, [<<"peers">>], Req, _Pid) ->
	{200, #{},
		ar_serialize:jsonify(
			[
				list_to_binary(ar_util:format_peer(P))
			||
				P <- ar_peers:get_peers(),
				P /= ar_http_util:arweave_peer(Req),
				ar_peers:is_public_peer(P)
			]
		),
	Req};

%% Return the estimated transaction fee not including a new wallet fee.
%% GET request to endpoint /price/{bytes}.
handle(<<"GET">>, [<<"price">>, SizeInBytesBinary], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case catch binary_to_integer(SizeInBytesBinary) of
				{'EXIT', _} ->
					{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
				Size ->
					Fee = estimate_tx_fee(Size, no_wallet),
					{200, #{}, integer_to_binary(Fee), Req}
			end
	end;

%% Return the estimated transaction fee not including a new wallet fee.
%% GET request to endpoint /price/{bytes}/{address}.
handle(<<"GET">>, [<<"price">>, SizeInBytesBinary, Addr], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, AddrOK} ->
					case catch binary_to_integer(SizeInBytesBinary) of
						{'EXIT', _} ->
							{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
						Size ->
							Fee = estimate_tx_fee(Size, AddrOK),
							{200, #{}, integer_to_binary(Fee), Req}
					end
			end
	end;

%% Return the current JSON-encoded hash list held by the node.
%% GET request to endpoint /block_index.
handle(<<"GET">>, [<<"hash_list">>], Req, _Pid) ->
	handle(<<"GET">>, [<<"block_index">>], Req, _Pid);

handle(<<"GET">>, [<<"block_index">>], Req, _Pid) ->
	ok = ar_semaphore:acquire(get_block_index, infinity),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			BI = ar_node:get_block_index(),
			{200, #{},
				ar_serialize:jsonify(
					ar_serialize:block_index_to_json_struct(format_bi_for_peer(BI, Req))
				),
			Req}
	end;

%% Return the current binary-encoded block index held by the node.
%% GET request to endpoint /block_index2.
handle(<<"GET">>, [<<"block_index2">>], Req, _Pid) ->
	ok = ar_semaphore:acquire(get_block_index, infinity),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			BI = ar_node:get_block_index(),
			Bin = ar_serialize:block_index_to_binary(BI),
			{200, #{}, Bin, Req}
	end;

handle(<<"GET">>, [<<"hash_list">>, From, To], Req, _Pid) ->
	handle(<<"GET">>, [<<"block_index">>, From, To], Req, _Pid);

handle(<<"GET">>, [<<"block_index">>, From, To], Req, _Pid) ->
	ok = ar_semaphore:acquire(get_block_index, infinity),
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			try
				FromHeight = binary_to_integer(From),
				ToHeight = binary_to_integer(To),
				{Height, BI} = ar_node:get_block_index_and_height(),
				{_, BI2} = lists:split(Height - ToHeight, BI),
				BI3 = lists:sublist(BI2, ToHeight - FromHeight + 1),
				{200, #{},
					ar_serialize:jsonify(
						ar_serialize:block_index_to_json_struct(format_bi_for_peer(BI3, Req))
					), Req}
			catch _:_ ->
				{400, #{}, jiffy:encode(#{ error => invalid_range }), Req}
			end
	end;

handle(<<"GET">>, [<<"recent_hash_list">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			Encoded = [ar_util:encode(H) || H <- ar_node:get_block_anchors()],
			{200, #{}, ar_serialize:jsonify(Encoded), Req}
	end;

%% Accept the list of independent block hashes ordered from oldest to newest
%% and return the deviation of our hash list from the given one.
%% Peers may use this endpoint to make sure they did not miss blocks or learn
%% about the missed blocks and their transactions so that they can catch up quickly.
handle(<<"GET">>, [<<"recent_hash_list_diff">>], Req, Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case read_complete_body(Req, Pid, ?MAX_SERIALIZED_RECENT_HASH_LIST_DIFF) of
				{ok, Body, Req2} ->
					case decode_recent_hash_list(Body) of
						{ok, ReverseHL} ->
							BlockTXPairs = ar_node:get_block_txs_pairs(),
							case get_recent_hash_list_diff(ReverseHL,
									lists:reverse(BlockTXPairs)) of
								no_intersection ->
									{404, #{}, <<>>, Req2};
								Bin ->
									{200, #{}, Bin, Req2}
							end;
						error ->
							{400, #{}, <<>>, Req2}
					end;
				{error, body_size_too_large} ->
					{413, #{}, <<"Payload too large">>, Req}
			end
	end;

%% Return the current wallet list held by the node.
%% GET request to endpoint /wallet_list.
handle(<<"GET">>, [<<"wallet_list">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			H = ar_node:get_current_block_hash(),
			process_request(get_block, [<<"hash">>, ar_util:encode(H), <<"wallet_list">>], Req)
	end;

%% Return a bunch of wallets, up to ?WALLET_LIST_CHUNK_SIZE, from the tree with
%% the given root hash. The wallet addresses are picked in the ascending alphabetical order.
handle(<<"GET">>, [<<"wallet_list">>, EncodedRootHash], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			process_get_wallet_list_chunk(EncodedRootHash, first, Req)
	end;

%% Return a bunch of wallets, up to ?WALLET_LIST_CHUNK_SIZE, from the tree with
%% the given root hash, starting with the provided cursor, taken the wallet addresses
%% are picked in the ascending alphabetical order.
handle(<<"GET">>, [<<"wallet_list">>, EncodedRootHash, EncodedCursor], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			process_get_wallet_list_chunk(EncodedRootHash, EncodedCursor, Req)
	end;

%% Return the balance of the given address from the wallet tree with the given root hash.
handle(<<"GET">>, [<<"wallet_list">>, EncodedRootHash, EncodedAddr, <<"balance">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case {ar_util:safe_decode(EncodedRootHash), ar_util:safe_decode(EncodedAddr)} of
				{{error, invalid}, _} ->
					{400, #{}, jiffy:encode(#{ error => invalid_root_hash_encoding }), Req};
				{_, {error, invalid}} ->
					{400, #{}, jiffy:encode(#{ error => invalid_address_encoding }), Req};
				{{ok, RootHash}, {ok, Addr}} ->
					case ar_wallets:get_balance(RootHash, Addr) of
						{error, not_found} ->
							{404, #{}, jiffy:encode(#{ error => root_hash_not_found }), Req};
						Balance when is_integer(Balance) ->
							{200, #{}, integer_to_binary(Balance), Req};
						_Error ->
							{500, #{}, <<>>, Req}
					end
			end
	end;

%% Share your IP with another peer.
%% @deprecated To make a node learn your IP, you can make any request to it.
handle(<<"POST">>, [<<"peers">>], Req, _Pid) ->
	{200, #{}, <<>>, Req};

%% Return the balance of the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/balance.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"balance">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, AddrOK} ->
					%% ar_node:get_balance/2 can time out which is not suitable for this
					%% use-case. It would be better if it never timed out so that Cowboy
					%% would handle the timeout instead.
					case ar_node:get_balance(AddrOK) of
						node_unavailable ->
							{503, #{}, <<"Internal timeout.">>, Req};
						Balance ->
							{200, #{}, integer_to_binary(Balance), Req}
					end
			end
	end;

%% Return the last transaction ID (hash) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/last_tx.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"last_tx">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
				{error, invalid} ->
					{400, #{}, <<"Invalid address.">>, Req};
				{ok, AddrOK} ->
					{200, #{},
						ar_util:encode(
							?OK(ar_node:get_last_tx(AddrOK))
						),
					Req}
			end
	end;

%% Return a block anchor to use for building transactions.
handle(<<"GET">>, [<<"tx_anchor">>], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			List = ar_node:get_block_anchors(),
			SuggestedAnchor =
				lists:nth(min(length(List), (?MAX_TX_ANCHOR_DEPTH)) div 2 + 1, List),
			{200, #{}, ar_util:encode(SuggestedAnchor), Req}
	end;

%% Return transaction identifiers (hashes) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txs.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"txs">>], Req, _Pid) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			ar_semaphore:acquire(arql_semaphore(Req), 5000),
			{Status, Headers, Body} = handle_get_wallet_txs(Addr, none),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return transaction identifiers (hashes) starting from the earliest_tx for the wallet
%% specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txs/{earliest_tx}.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"txs">>, EarliestTX], Req, _Pid) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(serve_wallet_txs, Config#config.enable) of
		true ->
			ar_semaphore:acquire(arql_semaphore(Req), 5000),
			{Status, Headers, Body} = handle_get_wallet_txs(Addr, ar_util:decode(EarliestTX)),
			{Status, Headers, Body, Req};
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return identifiers (hashes) of transfer transactions depositing to the given
%% wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/deposits.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"deposits">>], Req, _Pid) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(serve_wallet_deposits, Config#config.enable) of
		true ->
			ar_semaphore:acquire(arql_semaphore(Req), 5000),
			case catch ar_arql_db:select_txs_by([{to, [Addr]}]) of
				TXMaps when is_list(TXMaps) ->
					TXIDs = lists:map(fun(#{ id := ID }) -> ID end, TXMaps),
					{200, #{}, ar_serialize:jsonify(TXIDs), Req};
				{'EXIT', {timeout, {gen_server, call, [ar_arql_db, _]}}} ->
					{503, #{}, <<"ArQL unavailable.">>, Req}
			end;
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return identifiers (hashes) of transfer transactions depositing to the given
%% wallet_address starting from the earliest_deposit.
%% GET request to endpoint /wallet/{wallet_address}/deposits/{earliest_deposit}.
handle(<<"GET">>, [<<"wallet">>, Addr, <<"deposits">>, EarliestDeposit], Req, _Pid) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(serve_wallet_deposits, Config#config.enable) of
		true ->
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
		false ->
			{421, #{}, jiffy:encode(#{ error => endpoint_not_enabled }), Req}
	end;

%% Return the JSON-encoded block with the given height or hash.
%% GET request to endpoint /block/{height|hash}/{height|hash}.
handle(<<"GET">>, [<<"block">>, Type, ID], Req, Pid)
		when Type == <<"height">> orelse Type == <<"hash">> ->
	handle_get_block(Type, ID, Req, Pid, json);

%% Return the binary-encoded block with the given height or hash.
%% GET request to endpoint /block2/{height|hash}/{height|hash}.
%% Optionally accept an HTTP body, up to 125 bytes - the encoded
%% transaction indices where the Nth bit being 1 asks to include
%% the Nth transaction in the alphabetical order (not just its identifier)
%% in the response. The node only includes transactions in the response
%% when the corresponding indices are present in the request and those
%% transactions are found in the block cache - the motivation is to keep
%% the endpoint lightweight.
handle(<<"GET">>, [<<"block2">>, Type, ID], Req, Pid)
		when Type == <<"height">> orelse Type == <<"hash">> ->
	handle_get_block(Type, ID, Req, Pid, binary);

%% Return block or block field.
handle(<<"GET">>, [<<"block">>, Type, ID, Field], Req, _Pid)
		when Type == <<"height">> orelse Type == <<"hash">> ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			process_request(get_block, [Type, ID, Field], Req)
	end;

%% Return the current block.
%% GET request to endpoint /block/current.
handle(<<"GET">>, [<<"block">>, <<"current">>], Req, Pid) ->
	case ar_node:get_current_block_hash() of
		not_joined ->
			not_joined(Req);
		H when is_binary(H) ->
			handle(<<"GET">>, [<<"block">>, <<"hash">>, ar_util:encode(H)], Req, Pid)
	end;

%% DEPRECATED (12/07/2018)
handle(<<"GET">>, [<<"current_block">>], Req, Pid) ->
	handle(<<"GET">>, [<<"block">>, <<"current">>], Req, Pid);

%% Return a given field of the transaction specified by the transaction ID (hash).
%% GET request to endpoint /tx/{hash}/{field}
%%
%% {field} := { id | last_tx | owner | tags | target | quantity | data | signature | reward }
handle(<<"GET">>, [<<"tx">>, Hash, Field], Req, _Pid) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			ReadTX =
				case ar_util:safe_decode(Hash) of
					{error, invalid} ->
						{reply, {400, #{}, <<"Invalid hash.">>, Req}};
					{ok, ID} ->
						{ar_storage:read_tx(ID), ID}
				end,
			case ReadTX of
				{unavailable, TXID} ->
					case is_a_pending_tx(TXID) of
						true ->
							{202, #{}, <<"Pending">>, Req};
						false ->
							{404, #{}, <<"Not Found.">>, Req}
					end;
				{reply, Reply} ->
					Reply;
				{#tx{} = TX, _} ->
					case Field of
						<<"tags">> ->
							{200, #{}, ar_serialize:jsonify(lists:map(
									fun({Name, Value}) ->
										{[{name, ar_util:encode(Name)},
												{value, ar_util:encode(Value)}]}
									end,
									TX#tx.tags)), Req};
						<<"data">> ->
							serve_tx_data(Req, TX);
						_ ->
							case catch binary_to_existing_atom(Field) of
								{'EXIT', _} ->
									{400, #{}, jiffy:encode(#{ error => invalid_field }), Req};
								FieldAtom ->
									{TXJSON} = ar_serialize:tx_to_json_struct(TX),
									case catch val_for_key(FieldAtom, TXJSON) of
										{'EXIT', _} ->
											{400, #{}, jiffy:encode(#{ error => invalid_field }),
													Req};
										Val ->
											{200, #{}, Val, Req}
									end
							end
					end
			end
	end;

%% Return the current block hieght, or 500.
handle(Method, [<<"height">>], Req, _Pid)
		when (Method == <<"GET">>) or (Method == <<"HEAD">>) ->
	case ar_node:get_height() of
		-1 -> not_joined(Req);
		H -> {200, #{}, integer_to_binary(H), Req}
	end;

%% If we are given a hash with no specifier (block, tx, etc), assume that
%% the user is requesting the data from the TX associated with that hash.
%% Optionally allow a file extension.
handle(<<"GET">>, [<<Hash:43/binary, MaybeExt/binary>>], Req, Pid) ->
	handle(<<"GET">>, [<<"tx">>, Hash, <<"data.", MaybeExt/binary>>], Req, Pid);

%% Catch case for requests made to unknown endpoints.
%% Returns error code 400 - Request type not found.
handle(_, _, Req, _Pid) ->
	not_found(Req).

%% Cowlib does not yet support status codes 208 and 419 properly.
%% See https://github.com/ninenines/cowlib/pull/79
handle_custom_codes(208) -> <<"208 Already Reported">>;
handle_custom_codes(419) -> <<"419 Missing Chunk">>;
handle_custom_codes(Status) -> Status.

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

not_joined(Req) ->
	{503, #{}, jiffy:encode(#{ error => not_joined }), Req}.

handle_get_tx_status(EncodedTXID, Req) ->
	case ar_util:safe_decode(EncodedTXID) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>, Req};
		{ok, TXID} ->
			case is_a_pending_tx(TXID) of
				true ->
					{202, #{}, <<"Pending">>, Req};
				false ->
					case ar_storage:get_tx_confirmation_data(TXID) of
						{ok, {Height, BH}} ->
							PseudoTags = [
								{<<"block_height">>, Height},
								{<<"block_indep_hash">>, ar_util:encode(BH)}
							],
							case ar_block_index:get_element_by_height(Height) of
								not_found ->
									{404, #{}, <<"Not Found.">>, Req};
								{BH, _, _} ->
									CurrentHeight = ar_node:get_height(),
									%% First confirmation is when the TX is
									%% in the latest block.
									NumberOfConfirmations = CurrentHeight - Height + 1,
									Status = PseudoTags
											++ [{<<"number_of_confirmations">>,
												NumberOfConfirmations}],
									{200, #{}, ar_serialize:jsonify({Status}), Req};
								_ ->
									{404, #{}, <<"Not Found.">>, Req}
							end;
						not_found ->
							{404, #{}, <<"Not Found.">>, Req};
						{error, timeout} ->
							{503, #{}, <<"ArQL unavailable.">>, Req}
					end
			end
	end.

handle_get_tx(Hash, Req, Encoding) ->
	case ar_util:safe_decode(Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>, Req};
		{ok, ID} ->
			case ar_storage:read_tx(ID) of
				unavailable ->
					maybe_tx_is_pending_response(ID, Req);
				#tx{} = TX ->
					Body =
						case Encoding of
							json ->
								ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX));
							binary ->
								ar_serialize:tx_to_binary(TX)
						end,
					{200, #{}, Body, Req}
			end
	end.

handle_get_unconfirmed_tx(Hash, Req, Encoding) ->
	case ar_util:safe_decode(Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>, Req};
		{ok, TXID} ->
			case ets:lookup(node_state, {tx, TXID}) of
				[{_, TX}] ->
					Body =
						case Encoding of
							json ->
								ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX));
							binary ->
								ar_serialize:tx_to_binary(TX)
						end,
					{200, #{}, Body, Req};
				[] ->
					handle_get_tx(Hash, Req, Encoding)
			end
	end.

maybe_tx_is_pending_response(ID, Req) ->
	case is_a_pending_tx(ID) of
		true ->
			{202, #{}, <<"Pending">>, Req};
		false ->
			case ar_tx_db:get_error_codes(ID) of
				{ok, ErrorCodes} ->
					ErrorBody = list_to_binary(lists:join(" ", ErrorCodes)),
					{410, #{}, ErrorBody, Req};
				not_found ->
					{404, #{}, <<"Not Found.">>, Req}
			end
	end.

serve_tx_data(Req, #tx{ format = 1 } = TX) ->
	{200, #{}, ar_util:encode(TX#tx.data), Req};
serve_tx_data(Req, #tx{ format = 2, id = ID } = TX) ->
	DataFilename = ar_storage:tx_data_filepath(TX),
	case filelib:is_file(DataFilename) of
		true ->
			{200, #{}, sendfile(DataFilename), Req};
		false ->
			ok = ar_semaphore:acquire(get_tx_data, infinity),
			case ar_data_sync:get_tx_data(ID) of
				{ok, Data} ->
					{200, #{}, ar_util:encode(Data), Req};
				{error, tx_data_too_big} ->
					{400, #{}, jiffy:encode(#{ error => tx_data_too_big }), Req};
				{error, not_found} ->
					{200, #{}, <<>>, Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end.

serve_tx_html_data(Req, TX) ->
	serve_tx_html_data(Req, TX, ar_http_util:get_tx_content_type(TX)).

serve_tx_html_data(Req, #tx{ format = 1 } = TX, {valid, ContentType}) ->
	{200, #{ <<"content-type">> => ContentType }, TX#tx.data, Req};
serve_tx_html_data(Req, #tx{ format = 1 } = TX, none) ->
	{200, #{ <<"content-type">> => <<"text/html">> }, TX#tx.data, Req};
serve_tx_html_data(Req, #tx{ format = 2 } = TX, {valid, ContentType}) ->
	serve_format_2_html_data(Req, ContentType, TX);
serve_tx_html_data(Req, #tx{ format = 2 } = TX, none) ->
	serve_format_2_html_data(Req, <<"text/html">>, TX);
serve_tx_html_data(Req, _TX, invalid) ->
	{421, #{}, <<>>, Req}.

serve_format_2_html_data(Req, ContentType, TX) ->
	case ar_storage:read_tx_data(TX) of
		{ok, Data} ->
			{200, #{ <<"content-type">> => ContentType }, Data, Req};
		{error, enoent} ->
			ok = ar_semaphore:acquire(get_tx_data, infinity),
			case ar_data_sync:get_tx_data(TX#tx.id) of
				{ok, Data} ->
					{200, #{ <<"content-type">> => ContentType }, Data, Req};
				{error, tx_data_too_big} ->
					{400, #{}, jiffy:encode(#{ error => tx_data_too_big }), Req};
				{error, not_found} ->
					{200, #{ <<"content-type">> => ContentType }, <<>>, Req};
				{error, timeout} ->
					{503, #{}, jiffy:encode(#{ error => timeout }), Req}
			end
	end.

estimate_tx_fee(Size, Addr) ->
	Props =
		ets:select(
			node_state,
			[{{'$1', '$2'},
				[{'or',
					{'==', '$1', height},
					{'==', '$1', wallet_list},
					{'==', '$1', usd_to_ar_rate},
					{'==', '$1', scheduled_usd_to_ar_rate}}], ['$_']}]
		),
	Height = proplists:get_value(height, Props),
	CurrentRate = proplists:get_value(usd_to_ar_rate, Props),
	ScheduledRate = proplists:get_value(scheduled_usd_to_ar_rate, Props),
	%% Of the two rates - the currently active one and the one scheduled to be
	%% used soon - pick the one that leads to a higher fee in AR to make sure the
	%% transaction does not become underpaid.
	Rate = ar_fraction:maximum(CurrentRate, ScheduledRate),
	RootHash = proplists:get_value(wallet_list, Props),
	PaidSize = ar_tx:get_weave_size_increase(Size, Height + 1),
	estimate_tx_fee(PaidSize, Rate, Height + 1, Addr, RootHash).

estimate_tx_fee(Size, Rate, Height, Addr, RootHash) ->
	Timestamp = os:system_time(second),
	case Addr of
		no_wallet ->
			ar_tx:get_tx_fee(Size, Rate, Height, Timestamp);
		_ ->
			Wallets = ar_wallets:get(RootHash, Addr),
			ar_tx:get_tx_fee(Size, Rate, Height, Wallets, Addr, Timestamp)
	end.

handle_get_wallet_txs(Addr, EarliestTXID) ->
	case ar_wallet:base64_address_with_optional_checksum_to_decoded_address_safe(Addr) of
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
%% @end
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

handle_get_block(Type, ID, Req, Pid, Encoding) ->
	case Type of
		<<"hash">> ->
			case ar_util:safe_decode(ID) of
				{error, invalid} ->
					{404, #{}, <<"Block not found.">>, Req};
				{ok, H} ->
					handle_get_block(H, Req, Pid, Encoding)
			end;
		<<"height">> ->
			case ar_node:is_joined() of
				false ->
					not_joined(Req);
				true ->
					CurrentHeight = ar_node:get_height(),
					try binary_to_integer(ID) of
						Height when Height < 0 ->
							{400, #{}, <<"Invalid height.">>, Req};
						Height when Height > CurrentHeight ->
							{404, #{}, <<"Block not found.">>, Req};
						Height ->
							case ar_block_index:get_element_by_height(Height) of
								not_found ->
									{404, #{}, <<"Block not found.">>, Req};
								{H, _, _} ->
									handle_get_block(<<"hash">>, ar_util:encode(H), Req, Pid,
											Encoding)
							end
					catch _:_ ->
						{400, #{}, <<"Invalid height.">>, Req}
					end
			end
	end.

handle_get_block(H, Req, Pid, Encoding) ->
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			handle_get_block2(H, Req, Encoding);
		B ->
			case {Encoding, lists:any(fun(TX) -> is_binary(TX) end, B#block.txs)} of
				{binary, false} ->
					%% We have found the block in the block cache. Therefore, we can
					%% include the requested transactions without doing disk lookups.
					case read_complete_body(Req, Pid, ?MAX_SERIALIZED_MISSING_TX_INDICES) of
						{ok, Body, Req2} ->
							case parse_missing_tx_indices(Body) of
								error ->
									{400, #{}, <<>>, Req2};
								Indices ->
									SortedTXs = lists:sort(B#block.txs),
									Map = collect_missing_transactions(SortedTXs, Indices),
									TXs2 = [maps:get(TX#tx.id, Map, TX#tx.id)
											|| TX <- B#block.txs],
									handle_get_block3(B#block{ txs = TXs2 }, Req2, binary)
							end;
						{error, body_size_too_large} ->
							{413, #{}, <<"Payload too large">>, Req}
					end;
				_ ->
					handle_get_block3(B, Req, Encoding)
			end
	end.

handle_get_block2(H, Req, Encoding) ->
	ReadB =
		case ar_randomx_state:get_key_block(H) of
			not_found ->
				ar_storage:read_block(H);
			{ok, B} ->
				B
		end,
	case ReadB of
		unavailable ->
			{404, #{}, <<"Block not found.">>, Req};
		#block{} ->
			handle_get_block3(ReadB, Req, Encoding)
	end.

handle_get_block3(B, Req, Encoding) ->
	Bin =
		case Encoding of
			json ->
				ar_serialize:jsonify(ar_serialize:block_to_json_struct(B));
			binary ->
				ar_serialize:block_to_binary(B)
		end,
	{200, #{}, Bin, Req}.

parse_missing_tx_indices(Input) ->
	parse_missing_tx_indices(Input, 0).

parse_missing_tx_indices(<< 0:1, Rest/bitstring >>, N) ->
	parse_missing_tx_indices(Rest, N + 1);
parse_missing_tx_indices(<< 1:1, Rest/bitstring >>, N) ->
	case parse_missing_tx_indices(Rest, N + 1) of
		error ->
			error;
		Indices ->
			[N | Indices]
	end;
parse_missing_tx_indices(<<>>, _N) ->
	[];
parse_missing_tx_indices(_BadInput, _N) ->
	error.

collect_missing_transactions(TXs, Indices) ->
	collect_missing_transactions(TXs, Indices, 0).

collect_missing_transactions([#tx{ id = TXID } = TX | TXs], [N | Indices], N) ->
	maps:put(TXID, TX, collect_missing_transactions(TXs, Indices, N + 1));
collect_missing_transactions([_TX | TXs], Indices, N) ->
	collect_missing_transactions(TXs, Indices, N + 1);
collect_missing_transactions(_TXs, [], _N) ->
	#{};
collect_missing_transactions([], _Indices, _N) ->
	#{}.

handle_post_tx({Req, Pid, Encoding}) ->
	case ar_node:is_joined() of
		false ->
			not_joined(Req);
		true ->
			{ok, Config} = application:get_env(arweave, config),
			case ar_semaphore:acquire(post_tx, Config#config.post_tx_timeout * 1000) of
				{error, timeout} ->
					{503, #{}, <<>>, Req};
				ok ->
					case post_tx_parse_id({Req, Pid, Encoding}) of
						{error, invalid_hash, Req2} ->
							{400, #{}, <<"Invalid hash.">>, Req2};
						{error, tx_already_processed, _TXID, Req2} ->
							{208, #{}, <<"Transaction already processed.">>, Req2};
						{error, invalid_json, Req2} ->
							{400, #{}, <<"Invalid JSON.">>, Req2};
						{error, body_size_too_large, Req2} ->
							{413, #{}, <<"Payload too large">>, Req2};
						{ok, TX} ->
							Peer = ar_http_util:arweave_peer(Req),
							case handle_post_tx(Req, Peer, TX) of
								ok ->
									{200, #{}, <<"OK">>, Req};
								{error_response, {Status, Headers, Body}} ->
									ar_ignore_registry:remove_temporary(TX#tx.id),
									{Status, Headers, Body, Req}
							end
					end
			end
	end.

handle_post_tx(Req, Peer, TX) ->
	case ar_tx_validator:validate(TX) of
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
		{invalid, invalid_data_root_size} ->
			handle_post_tx_invalid_data_root_response();
		{valid, TX2} ->
			ar_data_sync:add_data_root_to_disk_pool(TX2#tx.data_root, TX2#tx.data_size,
					TX#tx.id),
			handle_post_tx_accepted(Req, TX, Peer)
	end.

handle_post_tx_accepted(Req, TX, Peer) ->
	%% Exclude successful requests with valid transactions from the
	%% IP-based throttling, to avoid connectivity issues at the times
	%% of excessive transaction volumes.
	{A, B, C, D, _} = Peer,
	ar_blacklist_middleware:decrement_ip_addr({A, B, C, D}, Req),
	ar_events:send(peer, {gossiped_tx, Peer, erlang:get(read_body_time),
			erlang:get(body_size)}),
	ar_events:send(tx, {new, TX, Peer}),
	TXID = TX#tx.id,
	ar_ignore_registry:remove_temporary(TXID),
	ar_ignore_registry:add_temporary(TXID, 10 * 60 * 1000),
	ok.

handle_post_tx_verification_response() ->
	{error_response, {400, #{}, <<"Transaction verification failed.">>}}.

handle_post_tx_last_tx_in_mempool_response() ->
	{error_response, {400, #{}, <<"Invalid anchor (last_tx from mempool).">>}}.

handle_post_tx_bad_anchor_response() ->
	{error_response, {400, #{}, <<"Invalid anchor (last_tx).">>}}.

handle_post_tx_already_in_weave_response() ->
	{error_response, {400, #{}, <<"Transaction is already on the weave.">>}}.

handle_post_tx_already_in_mempool_response() ->
	{error_response, {400, #{}, <<"Transaction is already in the mempool.">>}}.

handle_post_tx_invalid_data_root_response() ->
	{error_response, {400, #{}, <<"The attached data is split in an unknown way.">>}}.

handle_get_data_sync_record(Start, Limit, Req) ->
	Format =
		case cowboy_req:header(<<"content-type">>, Req) of
			<<"application/json">> ->
				json;
			_ ->
				etf
		end,
	Options = #{ start => Start, limit => Limit, format => Format },
	case ar_sync_record:get_record(Options, ar_data_sync) of
		{ok, Binary} ->
			{200, #{}, Binary, Req};
		{error, timeout} ->
			{503, #{}, jiffy:encode(#{ error => timeout }), Req}
	end.

handle_get_chunk(OffsetBinary, Req, Encoding) ->
	case catch binary_to_integer(OffsetBinary) of
		Offset when is_integer(Offset) ->
			case << Offset:(?NOTE_SIZE * 8) >> of
				%% A positive number represented by =< ?NOTE_SIZE bytes.
				<< Offset:(?NOTE_SIZE * 8) >> ->
					RequestedPacking =
						case cowboy_req:header(<<"x-packing">>, Req, not_set) of
							not_set ->
								unpacked;
							<<"unpacked">> ->
								unpacked;
							<<"spora_2_5">> ->
								spora_2_5;
							_ ->
								any
						end,
					IsBucketBasedOffset =
						case cowboy_req:header(<<"x-bucket-based-offset">>, Req, not_set) of
							not_set ->
								false;
							_ ->
								true
						end,
					{ReadPacking, CheckRecords} =
						case ar_sync_record:is_recorded(Offset, ar_data_sync) of
							false ->
								{none, {reply, {404, #{}, <<>>, Req}}};
							{true, RequestedPacking} ->
								ok = ar_semaphore:acquire(get_chunk, infinity),
								{RequestedPacking, ok};
							{true, Packing} when RequestedPacking == any ->
								ok = ar_semaphore:acquire(get_chunk, infinity),
								{Packing, ok};
							{true, _} ->
								ok = ar_semaphore:acquire(get_and_pack_chunk, infinity),
								{RequestedPacking, ok}
						end,
					case CheckRecords of
						{reply, Reply} ->
							Reply;
						ok ->
							Args = #{ packing => ReadPacking,
									bucket_based_offset => IsBucketBasedOffset },
							case ar_data_sync:get_chunk(Offset, Args) of
								{ok, Proof} ->
									Proof2 = Proof#{ packing => ReadPacking },
									Reply =
										case Encoding of
											json ->
												jiffy:encode(
													ar_serialize:chunk_proof_to_json_map(
															Proof2));
											binary ->
												ar_serialize:poa_to_binary(Proof2)
										end,
									{200, #{}, Reply, Req};
								{error, chunk_not_found} ->
									{404, #{}, <<>>, Req};
								{error, not_joined} ->
									not_joined(Req);
								{error, failed_to_read_chunk} ->
									{500, #{}, <<>>, Req}
							end
					end;
				_ ->
					{400, #{}, jiffy:encode(#{ error => offset_out_of_bounds }), Req}
			end;
		_ ->
			{400, #{}, jiffy:encode(#{ error => invalid_offset }), Req}
	end.

get_data_root_from_headers(Req) ->
	case {cowboy_req:header(<<"arweave-data-root">>, Req, not_set),
			cowboy_req:header(<<"arweave-data-size">>, Req, not_set)} of
		{not_set, _} ->
			not_set;
		{_, not_set} ->
			not_set;
		{EncodedDataRoot, EncodedDataSize} when byte_size(EncodedDataRoot) == 43 ->
			case catch binary_to_integer(EncodedDataSize) of
				DataSize when is_integer(DataSize) ->
					case ar_util:safe_decode(EncodedDataRoot) of
						{ok, DataRoot} ->
							{ok, {DataRoot, DataSize}};
						_ ->
							not_set
					end;
				_ ->
					not_set
			end;
		_ ->
			not_set
	end.

parse_chunk(Req, Pid) ->
	case read_complete_body(Req, Pid, ?MAX_SERIALIZED_CHUNK_PROOF_SIZE) of
		{ok, Body, Req2} ->
			case ar_serialize:json_decode(Body, [{return_maps, true}]) of
				{ok, JSON} ->
					case catch ar_serialize:json_map_to_chunk_proof(JSON) of
						{'EXIT', _} ->
							{400, #{}, jiffy:encode(#{ error => invalid_json }), Req2};
						Proof ->
							{ok, {Proof, Req2}}
					end;
				{error, _} ->
					{400, #{}, jiffy:encode(#{ error => invalid_json }), Req2}
			end;
		{error, body_size_too_large} ->
			{413, #{}, <<"Payload too large">>, Req}
	end.

handle_post_chunk(Proof, Req) ->
	handle_post_chunk(check_data_size, Proof, Req).

handle_post_chunk(check_data_size, Proof, Req) ->
	case maps:get(data_size, Proof) > trunc(math:pow(2, ?NOTE_SIZE * 8)) - 1 of
		true ->
			{400, #{}, jiffy:encode(#{ error => data_size_too_big }), Req};
		false ->
			handle_post_chunk(check_chunk_size, Proof, Req)
	end;
handle_post_chunk(check_chunk_size, Proof, Req) ->
	case byte_size(maps:get(chunk, Proof)) > ?DATA_CHUNK_SIZE of
		true ->
			{400, #{}, jiffy:encode(#{ error => chunk_too_big }), Req};
		false ->
			handle_post_chunk(check_data_path_size, Proof, Req)
	end;
handle_post_chunk(check_data_path_size, Proof, Req) ->
	case byte_size(maps:get(data_path, Proof)) > ?MAX_PATH_SIZE of
		true ->
			{400, #{}, jiffy:encode(#{ error => data_path_too_big }), Req};
		false ->
			handle_post_chunk(check_offset_field, Proof, Req)
	end;
handle_post_chunk(check_offset_field, Proof, Req) ->
	case maps:is_key(offset, Proof) of
		false ->
			{400, #{}, jiffy:encode(#{ error => offset_field_required }), Req};
		true ->
			handle_post_chunk(check_offset_size, Proof, Req)
	end;
handle_post_chunk(check_offset_size, Proof, Req) ->
	case maps:get(offset, Proof) > trunc(math:pow(2, ?NOTE_SIZE * 8)) - 1 of
		true ->
			{400, #{}, jiffy:encode(#{ error => offset_too_big }), Req};
		false ->
			handle_post_chunk(check_chunk_proof_ratio, Proof, Req)
	end;
handle_post_chunk(check_chunk_proof_ratio, Proof, Req) ->
	DataPath = maps:get(data_path, Proof),
	Chunk = maps:get(chunk, Proof),
	DataSize = maps:get(data_size, Proof),
	case ar_data_sync:is_chunk_proof_ratio_attractive(byte_size(Chunk), DataSize, DataPath) of
		false ->
			{400, #{}, jiffy:encode(#{ error => chunk_proof_ratio_not_attractive }), Req};
		true ->
			handle_post_chunk(validate_proof, Proof, Req)
	end;
handle_post_chunk(validate_proof, Proof, Req) ->
	Parent = self(),
	spawn(fun() -> Parent ! ar_data_sync:add_chunk(Proof) end),
	receive
		ok ->
			{200, #{}, <<>>, Req};
		{error, data_root_not_found} ->
			{400, #{}, jiffy:encode(#{ error => data_root_not_found }), Req};
		{error, exceeds_disk_pool_size_limit} ->
			{400, #{}, jiffy:encode(#{ error => exceeds_disk_pool_size_limit }), Req};
		{error, disk_full} ->
			{400, #{}, jiffy:encode(#{ error => disk_full }), Req};
		{error, failed_to_store_chunk} ->
			{500, #{}, <<>>, Req};
		{error, invalid_proof} ->
			{400, #{}, jiffy:encode(#{ error => invalid_proof }), Req}
	end.

check_internal_api_secret(Req) ->
	Reject = fun(Msg) ->
		log_internal_api_reject(Msg, Req),
		%% Reduce efficiency of timing attacks by sleeping randomly between 1-2s.
		timer:sleep(rand:uniform(1000) + 1000),
		{reject,
			{421, #{}, <<"Internal API disabled or invalid internal API secret in request.">>}}
	end,
	{ok, Config} = application:get_env(arweave, config),
	case {Config#config.internal_api_secret,
			cowboy_req:header(<<"x-internal-api-secret">>, Req)} of
		{not_set, _} ->
			Reject("Request to disabled internal API");
		{Secret, Secret} when is_binary(Secret) ->
			pass;
		_ ->
			Reject("Invalid secret for internal API request")
	end.

log_internal_api_reject(Msg, Req) ->
	spawn(fun() ->
		Path = ar_http_iface_server:split_path(cowboy_req:path(Req)),
		{IpAddr, _Port} = cowboy_req:peer(Req),
		BinIpAddr = list_to_binary(inet:ntoa(IpAddr)),
		?LOG_WARNING("~s: IP address: ~s Path: ~p", [Msg, BinIpAddr, Path])
	end).

%% @doc Convert a blocks field with the given label into a string.
block_field_to_string(<<"timestamp">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"last_retarget">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"diff">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"cumulative_diff">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"height">>, Res) -> integer_to_list(Res);
block_field_to_string(<<"txs">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"hash_list">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"wallet_list">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"usd_to_ar_rate">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"scheduled_usd_to_ar_rate">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(<<"poa">>, Res) -> ar_serialize:jsonify(Res);
block_field_to_string(_, Res) -> Res.

%% @doc Return true if ID is a pending tx.
is_a_pending_tx(ID) ->
	ar_node:is_a_pending_tx(ID).

decode_block(JSON, json) ->
	try
		{Struct} = ar_serialize:dejsonify(JSON),
		JSONB = val_for_key(<<"new_block">>, Struct),
		BShadow = ar_serialize:json_struct_to_block(JSONB),
		{ok, BShadow}
	catch
		Exception:Reason ->
			{error, {Exception, Reason}}
	end;
decode_block(Bin, binary) ->
	try
		ar_serialize:binary_to_block(Bin)
	catch
		Exception:Reason ->
			{error, {Exception, Reason}}
	end.

%% @doc Generate and return an informative JSON object regarding the state of the node.
return_info(Req) ->
	{Time, Current} =
		timer:tc(fun() -> ar_node:get_current_block_hash() end),
	{Time2, Height} =
		timer:tc(fun() -> ar_node:get_height() end),
	[{_, BlockCount}] = ets:lookup(ar_header_sync, synced_blocks),
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
					{blocks, BlockCount},
					{peers, prometheus_gauge:value(arweave_peer_count)},
					{queue_length,
						element(
							2,
							erlang:process_info(whereis(ar_node_worker), message_queue_len)
						)
					},
					{node_state_latency, (Time + Time2) div 2}
				]
			}
		),
	Req}.

%% @doc Convenience function for lists:keyfind(Key, 1, List). Returns Value, not {Key, Value}.
val_for_key(K, L) ->
	case lists:keyfind(K, 1, L) of
		false -> false;
		{K, V} -> V
	end.

collect_missing_tx_indices(Prefixes) ->
	collect_missing_tx_indices(Prefixes, [], 0).

collect_missing_tx_indices([], Indices, _N) ->
	lists:reverse(Indices);
collect_missing_tx_indices([Prefix | Prefixes], Indices, N) ->
	case ets:member(tx_prefixes, Prefix) of
		false ->
			collect_missing_tx_indices(Prefixes, [N | Indices], N + 1);
		true ->
			collect_missing_tx_indices(Prefixes, Indices, N + 1)
	end.

%% @doc Handle multiple steps of POST /block. First argument is a subcommand,
%% second the argument for that subcommand.
post_block(request, {Req, Pid, Encoding}, ReceiveTimestamp) ->
	OrigPeer = ar_http_util:arweave_peer(Req),
	case ar_blacklist_middleware:is_peer_banned(OrigPeer) of
		not_banned ->
			case ar_node:is_joined() of
				true ->
					post_block(check_block_hash_header, OrigPeer, {Req, Pid, Encoding},
									ReceiveTimestamp);
				false ->
					%% The node is not ready to validate and accept blocks.
					%% If the network adopts this block, ar_poller will catch up.
					{503, #{}, <<"Not joined.">>, Req}
			end;
		banned ->
			{403, #{}, <<"IP address blocked due to previous request.">>, Req}
	end.

post_block(check_block_hash_header, OrigPeer, {Req, Pid, Encoding}, ReceiveTimestamp) ->
	%% Look up the block hash in the "already processing/processed" registry
	%% for POST /block. We are gradually moving to using POST /block2 - its handler
	%% does the check in post_block_process_announcement/4.
	case cowboy_req:header(<<"arweave-block-hash">>, Req, not_set) of
		not_set ->
			post_block(read_body, OrigPeer, {Req, Pid, Encoding}, ReceiveTimestamp);
		EncodedBH ->
			case ar_util:safe_decode(EncodedBH) of
				{ok, BH} when byte_size(BH) =< 48 ->
					case ar_ignore_registry:member(BH) of
						true ->
							{208, #{}, <<"Block already processed.">>, Req};
						false ->
							post_block(read_body, OrigPeer, {Req, Pid, Encoding},
									ReceiveTimestamp)
					end;
				_ ->
					post_block(read_body, OrigPeer, {Req, Pid, Encoding}, ReceiveTimestamp)
			end
	end;
post_block(read_body, OrigPeer, {Req, Pid, Encoding}, ReceiveTimestamp) ->
	case read_complete_body(Req, Pid) of
		{ok, Body, Req2} ->
			case decode_block(Body, Encoding) of
				{error, _} ->
					{400, #{}, <<"Invalid block.">>, Req2};
				{ok, BShadow} ->
					ReadBodyTime = timer:now_diff(erlang:timestamp(), ReceiveTimestamp),
					erlang:put(read_body_time, ReadBodyTime),
					erlang:put(body_size, byte_size(term_to_binary(BShadow))),
					case byte_size(BShadow#block.indep_hash) > 48 of
						true ->
							{400, #{}, <<"Invalid block.">>, Req2};
						false ->
							post_block(check_indep_hash_processed, {BShadow, OrigPeer}, Req2,
									ReceiveTimestamp)
					end
			end;
		{error, body_size_too_large} ->
			{413, #{}, <<"Payload too large">>, Req}
	end;
post_block(check_indep_hash_processed, {BShadow, OrigPeer}, Req, ReceiveTimestamp) ->
	case ar_ignore_registry:member(BShadow#block.indep_hash) of
		true ->
			{208, #{}, <<"Block already processed.">>, Req};
		false ->
			post_block(check_transactions_are_present, {BShadow, OrigPeer}, Req,
					ReceiveTimestamp)
	end;
post_block(check_transactions_are_present, {BShadow, OrigPeer}, Req, ReceiveTimestamp) ->
	case erlang:get(post_block2) of
		true ->
			case get_missing_tx_identifiers(BShadow#block.txs) of
				[] ->
					post_block(may_be_fetch_chunk, {BShadow, OrigPeer}, Req, ReceiveTimestamp);
				{error, tx_list_too_long} ->
					{400, #{}, <<>>, Req};
				MissingTXIDs ->
					{418, #{}, encode_txids(MissingTXIDs), Req}
			end;
		_ -> % POST /block; do not reject for backwards-compatibility
			post_block(may_be_fetch_chunk, {BShadow, OrigPeer}, Req, ReceiveTimestamp)
	end;
post_block(may_be_fetch_chunk, {#block{ poa = #poa{ chunk = <<>> } } = BShadow, OrigPeer},
		Req, ReceiveTimestamp) ->
	case cowboy_req:header(<<"arweave-recall-byte">>, Req, not_set) of
		not_set ->
			post_block(check_indep_hash, {BShadow, OrigPeer}, Req, ReceiveTimestamp);
		Bin ->
			case catch binary_to_integer(Bin) of
				{'EXIT', _} ->
					post_block(check_indep_hash, {BShadow, OrigPeer}, Req, ReceiveTimestamp);
				RecallByte ->
					case ar_data_sync:get_chunk(RecallByte + 1, #{ pack => false,
							packing => spora_2_5, bucket_based_offset => true }) of
						{ok, #{ chunk := Chunk, data_path := DataPath, tx_path := TXPath }} ->
							erlang:put(fetched_chunk, true),
							post_block(check_indep_hash, {BShadow#block{
									poa = #poa{ chunk = Chunk, data_path = DataPath,
											tx_path = TXPath } }, OrigPeer}, Req,
											ReceiveTimestamp);
						_ ->
							{419, #{}, <<>>, Req}
					end
			end
	end;
post_block(may_be_fetch_chunk, {BShadow, OrigPeer}, Req, ReceiveTimestamp) ->
	post_block(check_indep_hash, {BShadow, OrigPeer}, Req, ReceiveTimestamp);
post_block(check_indep_hash, {BShadow, OrigPeer}, Req, ReceiveTimestamp) ->
	BH = BShadow#block.indep_hash,
	PrevH = BShadow#block.previous_block,
	case ar_node:get_block_shadow_from_cache(PrevH) of
		not_found ->
			%% We have not seen the previous block yet - might happen if two
			%% successive blocks are distributed at the same time. Do not
			%% ban the peer as the block might be valid. If the network adopts
			%% this block, ar_poller will catch up.
			{412, #{}, <<>>, Req};
		#block{ height = PrevHeight } = PrevB ->
			case BShadow#block.height == PrevHeight + 1 of
				false ->
					{400, #{}, <<"Invalid block.">>, Req};
				true ->
					case catch compute_hash(BShadow) of
						{BDS, BH} ->
							ar_ignore_registry:add_temporary(BH, 5000),
							post_block(
								check_timestamp,
								{BShadow, OrigPeer, BDS, PrevB},
								Req,
								ReceiveTimestamp
							);
						_ ->
							post_block_reject_warn(BShadow, check_indep_hash, OrigPeer),
							{400, #{}, <<"Invalid Block Hash">>, Req}
					end
			end
	end;
post_block(check_timestamp, {BShadow, OrigPeer, BDS, PrevB}, Req, ReceiveTimestamp) ->
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
			ar_ignore_registry:remove_temporary(BShadow#block.indep_hash),
			%% If the network actually applies this block, but we received it
			%% late for some reason, ar_poller will fetch and apply it.
			{400, #{}, <<"Invalid timestamp.">>, Req};
		true ->
			post_block(check_difficulty, {BShadow, OrigPeer, BDS, PrevB}, Req, ReceiveTimestamp)
	end;
%% The min difficulty check is filtering out blocks from smaller networks, e.g.
%% testnets. Therefore, we don't want to log when this check or any check above
%% rejects the block because there are potentially a lot of rejections.
post_block(check_difficulty, {BShadow, OrigPeer, BDS, PrevB}, Req, ReceiveTimestamp) ->
	case BShadow#block.diff >= ar_mine:min_difficulty(BShadow#block.height) of
		true ->
			post_block(check_pow, {BShadow, OrigPeer, BDS, PrevB}, Req, ReceiveTimestamp);
		_ ->
			ar_ignore_registry:remove_temporary(BShadow#block.indep_hash),
			{400, #{}, <<"Difficulty too low">>, Req}
	end;
%% Note! Checking PoW should be as cheap as possible. All slow steps should
%% be after the PoW check to reduce the possibility of doing a DOS attack on
%% the network.
post_block(check_pow, {BShadow, OrigPeer, BDS, PrevB}, Req, ReceiveTimestamp) ->
	#block{ indep_hash = PrevH } = PrevB,
	MaybeValid =
		case ar_node:get_recent_search_space_upper_bound_by_prev_h(PrevH) of
			not_found ->
				{reply, {412, #{}, <<>>, Req}};
			SearchSpaceUpperBound ->
				validate_spora_pow(BShadow, PrevB, BDS, SearchSpaceUpperBound)
		end,
	case MaybeValid of
		{reply, Reply} ->
			Reply;
		{true, RecallByte} ->
			post_block(post_block, {BShadow, OrigPeer, RecallByte}, Req, ReceiveTimestamp);
		false ->
			post_block_reject_warn(BShadow, check_pow, OrigPeer),
			ar_blacklist_middleware:ban_peer(OrigPeer, ?BAD_POW_BAN_TIME),
			{400, #{}, <<"Invalid Block Proof of Work">>, Req}
	end;
post_block(post_block, {BShadow, OrigPeer, RecallByte}, Req, ReceiveTimestamp) ->
	record_block_pre_validation_time(ReceiveTimestamp),
	?LOG_INFO([{event, ar_http_iface_handler_accepted_block},
			{indep_hash, ar_util:encode(BShadow#block.indep_hash)}]),
	%% Include all transactions found in the mempool in place of the corresponding
	%% transaction identifiers so that we can gossip them to peers who miss them
	%% along with the block.
	B = BShadow#block{ txs = include_transactions(BShadow#block.txs) },
	ar_events:send(block, {new, B, #{ source => {peer, OrigPeer},
			recall_byte => RecallByte }}),
	ar_events:send(peer, {gossiped_block, OrigPeer, erlang:get(read_body_time),
			erlang:get(body_size)}),
	prometheus_counter:inc(block2_received_transactions,
			count_received_transactions(BShadow#block.txs)),
	case erlang:get(fetched_chunk) of
		true ->
			prometheus_counter:inc(block2_fetched_chunks);
		_ ->
			ok
	end,
	{200, #{}, <<"OK">>, Req}.

encode_txids([]) ->
	<<>>;
encode_txids([TXID | TXIDs]) ->
	<< TXID/binary, (encode_txids(TXIDs))/binary >>.

get_missing_tx_identifiers(TXIDs) ->
	get_missing_tx_identifiers(TXIDs, [], 0).

get_missing_tx_identifiers([], MissingTXIDs, _N) ->
	MissingTXIDs;
get_missing_tx_identifiers([_ | _], _, N) when N == ?BLOCK_TX_COUNT_LIMIT ->
	{error, tx_list_too_long};
get_missing_tx_identifiers([#tx{} | TXIDs], MissingTXIDs, N) ->
	get_missing_tx_identifiers(TXIDs, MissingTXIDs, N + 1);
get_missing_tx_identifiers([TXID | TXIDs], MissingTXIDs, N) ->
	case ar_node_worker:is_mempool_or_block_cache_tx(TXID) of
		true ->
			get_missing_tx_identifiers(TXIDs, MissingTXIDs, N + 1);
		false ->
			get_missing_tx_identifiers(TXIDs, [TXID | MissingTXIDs], N + 1)
	end.

compute_hash(B) ->
	BDS = ar_block:generate_block_data_segment(B),
	Hash = B#block.hash,
	Nonce = B#block.nonce,
	{BDS, ar_weave:indep_hash(BDS, Hash, Nonce, B#block.poa)}.

validate_spora_pow(B, PrevB, BDS, SearchSpaceUpperBound) ->
	#block{ height = PrevHeight, indep_hash = PrevH } = PrevB,
	#block{ height = Height, nonce = Nonce, timestamp = Timestamp,
			poa = #poa{ chunk = Chunk } = SPoA } = B,
	Root = ar_block:compute_hash_list_merkle(PrevB),
	case {Root, PrevHeight + 1} == {B#block.hash_list_merkle, Height} of
		false ->
			false;
		true ->
			{H0, Entropy} = ar_mine:spora_h0_with_entropy(BDS, Nonce, Height),
			{ComputeSolutionHash, RecallByte} =
				case ar_mine:pick_recall_byte(H0, PrevH, SearchSpaceUpperBound) of
					{error, weave_size_too_small} ->
						case SPoA == #poa{} of
							false ->
								{false, undefined};
							true ->
								{ar_mine:spora_solution_hash(PrevH, Timestamp, H0, <<>>,
										Height), undefined}
						end;
					{ok, Byte} ->
						PackingThreshold = ar_block:get_packing_threshold(PrevB,
								SearchSpaceUpperBound),
						case verify_packing_threshold(B, PackingThreshold) of
							false ->
								{false, Byte};
							true ->
								case Byte >= PackingThreshold of
									true ->
										{ar_mine:spora_solution_hash_with_entropy(
												PrevH, Timestamp, H0, Chunk, Entropy,
												Height), Byte};
									false ->
										{ar_mine:spora_solution_hash(PrevH, Timestamp,
												H0, Chunk, Height), Byte}
								end
						end
				end,
			case ComputeSolutionHash of
				false ->
					false;
				SolutionHash ->
					case ar_mine:validate(SolutionHash, B#block.diff, Height)
							andalso SolutionHash == B#block.hash of
						false ->
							false;
						true ->
							{true, RecallByte}
					end
			end
	end.

verify_packing_threshold(_B, undefined) ->
	true;
verify_packing_threshold(#block{ packing_2_5_threshold = PackingThreshold }, PackingThreshold) ->
	true;
verify_packing_threshold(_, _) ->
	false.

post_block_reject_warn(BShadow, Step, Peer) ->
	?LOG_WARNING([
		{event, post_block_rejected},
		{hash, ar_util:encode(BShadow#block.indep_hash)},
		{step, Step},
		{peer, ar_util:format_peer(Peer)}
	]).

post_block_reject_warn(BShadow, Step, Peer, Params) ->
	?LOG_WARNING([
		{event, post_block_rejected},
		{hash, ar_util:encode(BShadow#block.indep_hash)},
		{step, Step},
		{params, Params},
		{peer, ar_util:format_peer(Peer)}
	]).

record_block_pre_validation_time(ReceiveTimestamp) ->
	TimeMs = timer:now_diff(erlang:timestamp(), ReceiveTimestamp) / 1000,
	prometheus_histogram:observe(block_pre_validation_time, TimeMs).

include_transactions([#tx{} = TX | TXs]) ->
	[TX | include_transactions(TXs)];
include_transactions([]) ->
	[];
include_transactions([TXID | TXs]) ->
	case ets:lookup(node_state, {tx, TXID}) of
		[] ->
			[TXID | include_transactions(TXs)];
		[{_, TX}] ->
			[TX | include_transactions(TXs)]
	end.

count_received_transactions(TXs) ->
	count_received_transactions(TXs, 0).

count_received_transactions([#tx{} | TXs], N) ->
	count_received_transactions(TXs, N + 1);
count_received_transactions([_ | TXs], N) ->
	count_received_transactions(TXs, N);
count_received_transactions([], N) ->
	N.

decode_recent_hash_list(<<>>) ->
	{ok, []};
decode_recent_hash_list(<< H:48/binary, Rest/binary >>) ->
	case decode_recent_hash_list(Rest) of
		error ->
			error;
		{ok, HL} ->
			{ok, [H | HL]}
	end;
decode_recent_hash_list(_Rest) ->
	error.

get_recent_hash_list_diff([H | HL], [{H, _TXIDs} | BlockTXPairs]) ->
	get_recent_hash_list_diff(HL, BlockTXPairs, H);
get_recent_hash_list_diff(_, _) ->
	no_intersection.

get_recent_hash_list_diff([H | HL], [{H, _SizeTaggedTXs} | BlockTXPairs], _PrevH) ->
	get_recent_hash_list_diff(HL, BlockTXPairs, H);
get_recent_hash_list_diff(_HL, BlockTXPairs, PrevH) ->
	<< PrevH/binary, (get_recent_hash_list_diff(BlockTXPairs))/binary >>.

get_recent_hash_list_diff([{H, SizeTaggedTXs} | BlockTXPairs]) ->
	{Len, TXIDs} = get_txids_from_size_tagged_txs(SizeTaggedTXs),
	<< H:48/binary, Len:16,
			(iolist_to_binary([TXID || TXID <- TXIDs]))/binary,
			(get_recent_hash_list_diff(BlockTXPairs))/binary >>;
get_recent_hash_list_diff([]) ->
	<<>>.

get_txids_from_size_tagged_txs([{{TXID, _}, _} | SizeTaggedTXs]) ->
	{N, L} = get_txids_from_size_tagged_txs(SizeTaggedTXs),
	{N + 1, [TXID | L]};
get_txids_from_size_tagged_txs([]) ->
	{0, []}.

%% Return the block hash list associated with a block.
process_request(get_block, [Type, ID, <<"hash_list">>], Req) ->
	case find_block(Type, ID) of
		{error, height_not_integer} ->
			{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
		unavailable ->
			{404, #{}, <<"Not Found.">>, Req};
		B ->
			ok = ar_semaphore:acquire(get_block_index, infinity),
			CurrentBI = ar_node:get_block_index(),
			HL = ar_block:generate_hash_list_for_block(B#block.indep_hash, CurrentBI),
			{200, #{}, ar_serialize:jsonify(lists:map(fun ar_util:encode/1, HL)), Req}
	end;
%% @doc Return the wallet list associated with a block.
process_request(get_block, [Type, ID, <<"wallet_list">>], Req) ->
	case find_block(Type, ID) of
		{error, height_not_integer} ->
			{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
		unavailable ->
			{404, #{}, <<"Not Found.">>, Req};
		B ->
			{ok, Config} = application:get_env(arweave, config),
			case {B#block.height >= ar_fork:height_2_2(),
					lists:member(serve_wallet_lists, Config#config.disable)} of
				{true, true} ->
					{400, #{},
						jiffy:encode(#{ error => does_not_serve_blocks_after_2_2_fork }),
						Req};
				{true, _} ->
					ok = ar_semaphore:acquire(get_wallet_list, infinity),
					case ar_storage:read_wallet_list(B#block.wallet_list) of
						{ok, Tree} ->
							{200, #{}, ar_serialize:jsonify(
								ar_serialize:wallet_list_to_json_struct(
									B#block.reward_addr, false, Tree
								)), Req};
						_ ->
							{404, #{}, <<"Block not found.">>, Req}
					end;
				_ ->
					WLFilepath = ar_storage:wallet_list_filepath(B#block.wallet_list),
					case filelib:is_file(WLFilepath) of
						true ->
							{200, #{}, sendfile(WLFilepath), Req};
						false ->
							{404, #{}, <<"Block not found.">>, Req}
					end
			end
	end;

%% Return a requested field of a given block.
%% GET request to endpoint /block/hash/{hash|height}/{field}.
%%
%% field :: nonce | previous_block | timestamp | last_retarget | diff | height | hash |
%%			indep_hash | txs | hash_list | wallet_list | reward_addr | tags | reward_pool
process_request(get_block, [Type, ID, Field], Req) ->
	{ok, Config} = application:get_env(arweave, config),
	case lists:member(subfield_queries, Config#config.enable) of
		true ->
			case find_block(Type, ID) of
				{error, height_not_integer} ->
					{400, #{}, jiffy:encode(#{ error => size_must_be_an_integer }), Req};
				unavailable ->
					{404, #{}, <<"Not Found.">>, Req};
				B ->
					{BLOCKJSON} = ar_serialize:block_to_json_struct(B),
					case catch list_to_existing_atom(binary_to_list(Field)) of
						{'EXIT', _} ->
							{404, #{}, <<"Not Found.">>, Req};
						Atom ->
							case lists:keyfind(Atom, 1, BLOCKJSON) of
								{_, Res} ->
									Result = block_field_to_string(Field, Res),
									{200, #{}, Result, Req};
								_ ->
									{404, #{}, <<"Not Found.">>, Req}
							end
					end
			end;
		_ ->
			{421, #{}, <<"Subfield block querying is disabled on this node.">>, Req}
	end.

process_get_wallet_list_chunk(EncodedRootHash, EncodedCursor, Req) ->
	DecodeCursorResult =
		case EncodedCursor of
			first ->
				{ok, first};
			_ ->
				ar_util:safe_decode(EncodedCursor)
		end,
	case {ar_util:safe_decode(EncodedRootHash), DecodeCursorResult} of
		{{error, invalid}, _} ->
			{400, #{}, <<"Invalid root hash.">>, Req};
		{_, {error, invalid}} ->
			{400, #{}, <<"Invalid root hash.">>, Req};
		{{ok, RootHash}, {ok, Cursor}} ->
			case ar_node:get_wallet_list_chunk(RootHash, Cursor) of
				{ok, {NextCursor, Wallets}} ->
					SerializeFn = case cowboy_req:header(<<"content-type">>, Req) of
						<<"application/json">> -> fun wallet_list_chunk_to_json/1;
						<<"application/etf">> -> fun erlang:term_to_binary/1;
						_ -> fun erlang:term_to_binary/1
					end,
					Reply = SerializeFn(#{ next_cursor => NextCursor, wallets => Wallets }),
					{200, #{}, Reply, Req};
				{error, root_hash_not_found} ->
					{404, #{}, <<"Root hash not found.">>, Req}
			end
	end.

wallet_list_chunk_to_json(#{ next_cursor := NextCursor, wallets := Wallets }) ->
	SerializedWallets =
		lists:map(
			fun({Addr, {Balance, LastTX}}) ->
				ar_serialize:wallet_to_json_struct({Addr, Balance, LastTX})
			end,
			Wallets
		),
	case NextCursor of
		last ->
			jiffy:encode(#{ wallets => SerializedWallets });
		Cursor when is_binary(Cursor) ->
			jiffy:encode(#{
				next_cursor => ar_util:encode(Cursor),
				wallets => SerializedWallets
			})
	end.

%% @doc Find a block, given a type and a specifier.
find_block(<<"height">>, RawHeight) ->
	case catch binary_to_integer(RawHeight) of
		{'EXIT', _} ->
			{error, height_not_integer};
		Height ->
			case ar_block_index:get_element_by_height(Height) of
				not_found ->
					unavailable;
				{H, _, _} ->
					ar_storage:read_block(H)
			end
	end;
find_block(<<"hash">>, ID) ->
	case ar_util:safe_decode(ID) of
		{ok, H} ->
			ar_storage:read_block(H);
		_ ->
			unavailable
	end.

is_tx_already_processed(TXID) ->
	case ar_ignore_registry:member(TXID) of
		true ->
			true;
		false ->
			ets:member(node_state, {tx, TXID})
	end.

post_tx_parse_id({Req, Pid, Encoding}) ->
	post_tx_parse_id(check_header, {Req, Pid, Encoding}).

post_tx_parse_id(check_header, {Req, Pid, Encoding}) ->
	case cowboy_req:header(<<"arweave-tx-id">>, Req, not_set) of
		not_set ->
			post_tx_parse_id(read_body, {not_set, Req, Pid, Encoding});
		EncodedTXID ->
			case ar_util:safe_decode(EncodedTXID) of
				{ok, TXID} when byte_size(TXID) =< 32 ->
					post_tx_parse_id(check_ignore_list, {TXID, Req, Pid, Encoding});
				_ ->
					{error, invalid_hash, Req}
			end
	end;
post_tx_parse_id(check_ignore_list, {TXID, Req, Pid, Encoding}) ->
	case is_tx_already_processed(TXID) of
		true ->
			{error, tx_already_processed, TXID, Req};
		false ->
			ar_ignore_registry:add_temporary(TXID, 5000),
			post_tx_parse_id(read_body, {TXID, Req, Pid, Encoding})
	end;
post_tx_parse_id(read_body, {TXID, Req, Pid, Encoding}) ->
	Timestamp = erlang:timestamp(),
	case read_complete_body(Req, Pid) of
		{ok, Body, Req2} ->
			case Encoding of
				json ->
					post_tx_parse_id(parse_json, {TXID, Req2, Body, Timestamp});
				binary ->
					post_tx_parse_id(parse_binary, {TXID, Req2, Body, Timestamp})
			end;
		{error, body_size_too_large} ->
			{error, body_size_too_large, Req}
	end;
post_tx_parse_id(parse_json, {TXID, Req, Body, Timestamp}) ->
	case catch ar_serialize:json_struct_to_tx(Body) of
		{'EXIT', _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_ignore_registry:remove_temporary(TXID)
			end,
			{error, invalid_json, Req};
		{error, _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_ignore_registry:remove_temporary(TXID)
			end,
			{error, invalid_json, Req};
		TX ->
			Time = timer:now_diff(erlang:timestamp(), Timestamp),
			erlang:put(read_body_time, Time),
			erlang:put(body_size, byte_size(term_to_binary(TX))),
			post_tx_parse_id(verify_id_match, {TXID, Req, TX})
	end;
post_tx_parse_id(parse_binary, {TXID, Req, Body, Timestamp}) ->
	case catch ar_serialize:binary_to_tx(Body) of
		{'EXIT', _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_ignore_registry:remove_temporary(TXID)
			end,
			{error, invalid_json, Req};
		{error, _} ->
			case TXID of
				not_set ->
					noop;
				_ ->
					ar_ignore_registry:remove_temporary(TXID)
			end,
			{error, invalid_json, Req};
		{ok, TX} ->
			Time = timer:now_diff(erlang:timestamp(), Timestamp),
			erlang:put(read_body_time, Time),
			erlang:put(body_size, byte_size(term_to_binary(TX))),
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
					ar_ignore_registry:remove_temporary(MismatchingTXID)
			end,
			case byte_size(TXID) > 32 of
				true ->
					{error, invalid_hash, Req};
				false ->
					case is_tx_already_processed(TXID) of
						true ->
							{error, tx_already_processed, TXID, Req};
						false ->
							ar_ignore_registry:add_temporary(TXID, 5000),
							{ok, TX}
					end
			end
	end.

read_complete_body(Req, Pid) ->
	read_complete_body(Req, Pid, ?MAX_BODY_SIZE).

read_complete_body(Req, Pid, SizeLimit) ->
	Pid ! {read_complete_body, self(), Req, SizeLimit},
	receive
		{read_complete_body, Term} -> Term
	end.

read_body_chunk(Req, Pid, Size, Timeout) ->
	Pid ! {read_body_chunk, self(), Req, Size, Timeout},
	receive
		{read_body_chunk, Term} -> Term
	end.
