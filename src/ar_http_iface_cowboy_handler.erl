-module(ar_http_iface_cowboy_handler).
-behaviour(cowboy_handler).
-export([init/2]).
-include("ar.hrl").

%%%===================================================================
%%% Cowboy handler callbacks.
%%%===================================================================

init(Req0, State) ->
	{Status, Headers, Body} = do_handle(Req0),
	CowboyStatus = handle208(Status),
	Req1 = cowboy_req:reply(CowboyStatus, Headers, Body, Req0),
	{ok, Req1, State}.

%%%===================================================================
%%% Private functions.
%%%===================================================================

do_handle(Req) ->
	%% Inform ar_bridge about new peer, performance rec will be updated from ar_metrics
	%% (this is leftover from update_performance_list)
	Peer = arweave_peer(Req),
	do_handle(Req, Peer).

do_handle(Req, Peer) ->
	Method = cowboy_req:method(Req),
	SplitPath = split_path(cowboy_req:path(Req)),
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
	case do_handle(Method, SplitPath, Req) of
		{Status, Hdrs, Body} ->
			{Status, maps:merge(?DEFAULT_RESPONSE_HEADERS, Hdrs), Body};
		{Status, Body} ->
			{Status, ?DEFAULT_RESPONSE_HEADERS, Body}
	end.

%% @doc Return network information from a given node.
%% GET request to endpoint /info
do_handle(<<"GET">>, [], _Req) ->
	return_info();
do_handle(<<"GET">>, [<<"info">>], _Req) ->
	return_info();

%% @doc Some load balancers use 'HEAD's rather than 'GET's to tell if a node
%% is alive. Appease them.
do_handle(<<"HEAD">>, [], _Req) ->
	{200, #{}, <<>>};
do_handle(<<"HEAD">>, [<<"info">>], _Req) ->
	{200, #{}, <<>>};

%% @doc Return permissive CORS headers for all endpoints
do_handle(<<"OPTIONS">>, [<<"block">>], _) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
		    <<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>};
do_handle(<<"OPTIONS">>, [<<"tx">>], _) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
		    <<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>};
do_handle(<<"OPTIONS">>, [<<"peer">>|_], _) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
		    <<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>};
do_handle(<<"OPTIONS">>, [<<"arql">>], _) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET, POST">>,
		    <<"access-control-allow-headers">> => <<"Content-Type">>}, <<"OK">>};
do_handle(<<"OPTIONS">>, _, _) ->
	{200, #{<<"access-control-allow-methods">> => <<"GET">>}, <<"OK">>};

%% @doc Return the current universal time in seconds.
do_handle(<<"GET">>, [<<"time">>], _Req) ->
	{200, #{}, integer_to_binary(os:system_time(second))};

%% @doc Return all transactions from node that are waiting to be mined into a block.
%% GET request to endpoint /tx/pending
do_handle(<<"GET">>, [<<"tx">>, <<"pending">>], _Req) ->
	{200, #{},
			ar_serialize:jsonify(
				%% Should encode
				lists:map(
					fun ar_util:encode/1,
					ar_node:get_pending_txs(whereis(http_entrypoint_node))
				)
			)
	};

%% @doc Return additional information about the transaction with the given identifier (hash).
%% GET request to endpoint /tx/{hash}.
do_handle(<<"GET">>, [<<"tx">>, Hash, <<"status">>], _Req) ->
	case get_tx_filename(Hash) of
		{ok, _} ->
			TagsToInclude = [
				<<"block_height">>,
				<<"block_indep_hash">>
			],
			Tags = lists:filter(
				fun(Tag) ->
					{Name, _} = Tag,
					lists:member(Name, TagsToInclude)
				end,
				?OK(
					ar_tx_search:get_tags_by_id(
						whereis(http_search_node),
						ar_util:decode(Hash),
						infinity
					)
				)
			),
			CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
			[TXIndepHashEncoded] = proplists:get_all_values(<<"block_indep_hash">>, Tags),
			TXIndepHash = ar_util:decode(TXIndepHashEncoded),
			case lists:member(TXIndepHash, CurrentBHL) of
				false ->
					{404, #{}, <<"Not Found.">>};
				true ->
					CurrentHeight = ar_node:get_height(whereis(http_entrypoint_node)),
					[TXHeight] = proplists:get_all_values(<<"block_height">>, Tags),
					%% First confirmation is when the TX is in the latest block.
					NumberOfConfirmations = CurrentHeight - TXHeight + 1,
					Status = Tags ++ [{<<"number_of_confirmations">>, NumberOfConfirmations}],
					{200, #{}, ar_serialize:jsonify({Status})}
			end;
		{response, Response} ->
			Response
	end;


% @doc Return a transaction specified via the the transaction id (hash)
%% GET request to endpoint /tx/{hash}
do_handle(<<"GET">>, [<<"tx">>, Hash], _Req) ->
	case get_tx_filename(Hash) of
		{ok, Filename} ->
			{200, #{}, sendfile(Filename)};
		{response, Response} ->
			Response
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
do_handle(<<"POST">>, [<<"arql">>], Req) ->
	{QueryJson, _} = read_complete_body(Req),
	Query = ar_serialize:json_struct_to_query(
		binary_to_list(QueryJson)
	),
	TXs = ar_util:unique(ar_parser:eval(Query)),
	case TXs of
		[] -> {200, #{}, []};
		Set ->
			{
				200,
				#{},
				ar_serialize:jsonify(
					ar_serialize:hash_list_to_json_struct(Set)
				)
			}
	end;

%% @doc Return the data field of the transaction specified via the transaction ID (hash) served as HTML.
%% GET request to endpoint /tx/{hash}/data.html
do_handle(<<"GET">>, [<<"tx">>, Hash, << "data.", _/binary >>], _Req) ->
	case hash_to_filename(tx, Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>};
		{error, _, unavailable} ->
			{404, #{}, sendfile("data/not_found.html")};
		{ok, Filename} ->
			T = ar_storage:do_read_tx(Filename),
			{
				200,
				#{
					<<"content-type">> => proplists:get_value(
						<<"Content-Type">>, T#tx.tags, "text/html"
					)
				}
				,
				T#tx.data
			}
	end;

%% @doc Share a new block to a peer.
%% POST request to endpoint /block with the body of the request being a JSON encoded block
%% as specified in ar_serialize.
do_handle(<<"POST">>, [<<"block">>], Req) ->
	post_block(request, Req);

%% @doc Generate a wallet and receive a secret key identifying it.
%% Requires internal_api_secret startup option to be set.
%% WARNING: only use it if you really really know what you are doing.
do_handle(<<"POST">>, [<<"wallet">>], Req) ->
	case check_internal_api_secret(Req) of
		pass ->
			WalletAccessCode = ar_util:encode(crypto:strong_rand_bytes(32)),
			{{_, PubKey}, _} = ar_wallet:new_keyfile(WalletAccessCode),
			ResponseProps = [
				{<<"wallet_address">>, ar_util:encode(ar_wallet:to_address(PubKey))},
				{<<"wallet_access_code">>, WalletAccessCode}
			],
			{200, #{}, ar_serialize:jsonify({ResponseProps})};
		{reject, Response} ->
			Response
	end;

%% @doc Share a new transaction with a peer.
%% POST request to endpoint /tx with the body of the request being a JSON encoded tx as
%% specified in ar_serialize.
do_handle(<<"POST">>, [<<"tx">>], Req) ->
	{TXJSON, _} = read_complete_body(Req),
	TX = ar_serialize:json_struct_to_tx(TXJSON),
	case handle_post_tx(TX) of
		ok ->
			{200, #{}, <<"OK">>};
		{error_response, Response} ->
			Response
	end;

%% @doc Sign and send a tx to the network.
%% Fetches the wallet by the provided key generated via POST /wallet.
%% Requires internal_api_secret startup option to be set.
%% WARNING: only use it if you really really know what you are doing.
do_handle(<<"POST">>, [<<"unsigned_tx">>], Req) ->
	case check_internal_api_secret(Req) of
		pass ->
			{Body, _} = read_complete_body(Req),
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
			SignedTX = ar_tx:sign(UnsignedTX, KeyPair),
			case handle_post_tx(SignedTX) of
				ok ->
					{200, #{}, ar_serialize:jsonify({[{<<"id">>, ar_util:encode(SignedTX#tx.id)}]})};
				{error_response, Response} ->
					Response
			end;
		{reject, Response} ->
			Response
	end;

%% @doc Return the list of peers held by the node.
%% GET request to endpoint /peers
do_handle(<<"GET">>, [<<"peers">>], Req) ->
	{200, #{},
		ar_serialize:jsonify(
			[
				list_to_binary(ar_util:format_peer(P))
			||
				P <- ar_bridge:get_remote_peers(whereis(http_bridge_node)),
				P /= arweave_peer(Req)
			]
		)
	};

%% @doc Return the estimated reward cost of transactions with a data body size of 'bytes'.
%% GET request to endpoint /price/{bytes}
%% TODO: Change so current block does not need to be pulled to calculate cost
do_handle(<<"GET">>, [<<"price">>, SizeInBytes], _Req) ->
	{200, #{},
		integer_to_binary(
			ar_tx:calculate_min_tx_cost(
				binary_to_integer(SizeInBytes),
				ar_node:get_current_diff(whereis(http_entrypoint_node))
			)
		)
	};

%% @doc Return the estimated reward cost of transactions with a data body size of 'bytes'.
%% GET request to endpoint /price/{bytes}/{address}
%% TODO: Change so current block does not need to be pulled to calculate cost
do_handle(<<"GET">>, [<<"price">>, SizeInBytes, Addr], _Req) ->
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, AddrOK} ->
			{200, #{},
				integer_to_binary(
					ar_tx:calculate_min_tx_cost(
						binary_to_integer(SizeInBytes),
						ar_node:get_current_diff(whereis(http_entrypoint_node)),
						ar_node:get_wallet_list(whereis(http_entrypoint_node)),
						AddrOK
					)
				)
			}
	end;

%% @doc Return the current hash list held by the node.
%% GET request to endpoint /hash_list
do_handle(<<"GET">>, [<<"hash_list">>], _Req) ->
	HashList = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	{200, #{},
		ar_serialize:jsonify(
			ar_serialize:hash_list_to_json_struct(HashList)
		)
	};

%% @doc Return the current wallet list held by the node.
%% GET request to endpoint /wallet_list
do_handle(<<"GET">>, [<<"wallet_list">>], _Req) ->
	Node = whereis(http_entrypoint_node),
	WalletList = ar_node:get_wallet_list(Node),
	{200, #{},
		ar_serialize:jsonify(
			ar_serialize:wallet_list_to_json_struct(WalletList)
		)
	};

%% @doc Share your nodes IP with another peer.
%% POST request to endpoint /peers with the body of the request being your
%% nodes network information JSON encoded as specified in ar_serialize.
% NOTE: Consider returning remaining timeout on a failed request
do_handle(<<"POST">>, [<<"peers">>], Req) ->
	{BlockJSON, ReadReq} = read_complete_body(Req),
	case ar_serialize:dejsonify(BlockJSON) of
		{Struct} ->
			{<<"network">>, NetworkName} = lists:keyfind(<<"network">>, 1, Struct),
			case (NetworkName == <<?NETWORK_NAME>>) of
				false ->
					{400, #{}, <<"Wrong network.">>};
				true ->
					Peer = arweave_peer(ReadReq),
					case ar_meta_db:get({peer, Peer}) of
						not_found ->
							ar_bridge:add_remote_peer(whereis(http_bridge_node), Peer);
						X -> X
					end,
					{200, #{}, []}
			end;
		_ -> {400, #{}, "Wrong network"}
	end;
%% @doc Return the balance of the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/balance
do_handle(<<"GET">>, [<<"wallet">>, Addr, <<"balance">>], _Req) ->
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, AddrOK} ->
			%% ar_node:get_balance/2 can time out which is not suitable for this
			%% use-case. It would be better if it never timed out so that Elli
			%% would handle the timeout instead.
			case ar_node:get_balance(whereis(http_entrypoint_node), AddrOK) of
				node_unavailable ->
					{503, #{}, <<"Internal timeout.">>};
				Balance ->
					{200, #{}, integer_to_binary(Balance)}
			end
	end;

%% @doc Return the last transaction ID (hash) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/last_tx
do_handle(<<"GET">>, [<<"wallet">>, Addr, <<"last_tx">>], _Req) ->
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, AddrOK} ->
			{200, #{},
				ar_util:encode(
					?OK(ar_node:get_last_tx(whereis(http_entrypoint_node), AddrOK))
				)
			}
	end;

%% @doc Return transaction identifiers (hashes) for the wallet specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txs
do_handle(<<"GET">>, [<<"wallet">>, Addr, <<"txs">>], _Req) ->
	handle_get_wallet_txs(Addr, none);

%% @doc Return transaction identifiers (hashes) starting from the earliest_tx for the wallet
%% specified via wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/txs/{earliest_tx}
do_handle(<<"GET">>, [<<"wallet">>, Addr, <<"txs">>, EarliestTX], _Req) ->
	handle_get_wallet_txs(Addr, ar_util:decode(EarliestTX));

%% @doc Return identifiers (hashes) of transfer transactions depositing to the given wallet_address.
%% GET request to endpoint /wallet/{wallet_address}/deposits
do_handle(<<"GET">>, [<<"wallet">>, Addr, <<"deposits">>], _Req) ->
	TXIDs = lists:reverse(
		lists:map(fun ar_util:encode/1, ar_tx_search:get_entries(<<"to">>, Addr))
	),
	{200, #{}, ar_serialize:jsonify(TXIDs)};

%% @doc Return identifiers (hashes) of transfer transactions depositing to the given wallet_address
%% starting from the earliest_deposit.
%% GET request to endpoint /wallet/{wallet_address}/deposits/{earliest_deposit}
do_handle(<<"GET">>, [<<"wallet">>, Addr, <<"deposits">>, EarliestDeposit], _Req) ->
	TXIDs = lists:reverse(
		lists:map(fun ar_util:encode/1, ar_tx_search:get_entries(<<"to">>, Addr))
	),
	{Before, After} = lists:splitwith(fun(T) -> T /= EarliestDeposit end, TXIDs),
	FilteredTXs = case After of
		[] ->
			Before;
		[EarliestDeposit | _] ->
			Before ++ [EarliestDeposit]
	end,
	{200, #{}, ar_serialize:jsonify(FilteredTXs)};

%% @doc Return the encrypted blockshadow corresponding to the indep_hash.
%% GET request to endpoint /block/hash/{indep_hash}/encrypted
%do_handle(<<"GET">>, [<<"block">>, <<"hash">>, Hash, <<"encrypted">>], _Req) ->
	%ar:d({resp_block_hash, Hash}),
	%ar:report_console([{resp_getting_block_by_hash, Hash}, {path, split_path(cowboy_req:path(Req))}]),
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
do_handle(<<"GET">>, [<<"block">>, Type, ID], Req) ->
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
			{400, #{}, <<"Invalid height.">>};
		invalid_height ->
			{400, #{}, <<"Invalid hash.">>};
		unavailable ->
			{404, #{}, <<"Block not found.">>};
		_  ->
			case {ar_meta_db:get(api_compat), cowboy_req:header(<<"x-block-format">>, Req, <<"2">>)} of
				{false, <<"1">>} ->
					{426, #{}, <<"Client version incompatible.">>};
				{_, <<"1">>} ->
					% Supprt for legacy nodes (pre-1.5).
					BHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
					try ar_storage:do_read_block(Filename, BHL) of
						B ->
							{JSONStruct} =
								ar_serialize:block_to_json_struct(
									B#block {
										txs =
											[
												if is_binary(TX) -> TX; true -> TX#tx.id end
											||
												TX <- B#block.txs
											]
									}
								),
							{200, #{},
								ar_serialize:jsonify(
									{
										[
											{
												<<"hash_list">>,
												ar_serialize:hash_list_to_json_struct(B#block.hash_list)
											}
										|
											JSONStruct
										]
									}
								)
							}
					catch error:cannot_generate_block_hash_list ->
						{404, #{}, <<"Requested block not found on block hash list.">>}
					end;
				{_, _} ->
					{200, #{}, sendfile(Filename)}
			end
	end;

%% @doc Return block or block field.
do_handle(<<"GET">>, [<<"block">>, Type, IDBin, Field], _Req) ->
	case validate_get_block_type_id(Type, IDBin) of
		{error, Response} ->
			Response;
		{ok, ID} ->
			process_request(get_block, [Type, ID, Field])
	end;

%% @doc Return the current block.
%% GET request to endpoint /current_block
%% GET request to endpoint /block/current
do_handle(<<"GET">>, [<<"block">>, <<"current">>], Req) ->
	case ar_node:get_hash_list(whereis(http_entrypoint_node)) of
		[] -> {404, #{}, <<"Block not found.">>};
		[IndepHash|_] ->
			do_handle(<<"GET">>, [<<"block">>, <<"hash">>, ar_util:encode(IndepHash)], Req)
	end;

%% DEPRECATED (12/07/2018)
do_handle(<<"GET">>, [<<"current_block">>], Req) ->
	do_handle(<<"GET">>, [<<"block">>, <<"current">>], Req);

%% @doc Return a list of known services.
%% GET request to endpoint /services
do_handle(<<"GET">>, [<<"services">>], _Req) ->
	{200, #{},
		ar_serialize:jsonify(
			{
				[
					{
						[
							{"name", Name},
							{"host", ar_util:format_peer(Host)},
							{"expires", Expires}
						]
					}
				||
					#service {
						name = Name,
						host = Host,
						expires = Expires
					} <- ar_services:get(whereis(http_service_node))
				]
			}
		)
	};

%% @doc Return a given field of the transaction specified by the transaction ID (hash).
%% GET request to endpoint /tx/{hash}/{field}
%%
%% {field} := { id | last_tx | owner | tags | target | quantity | data | signature | reward }
%%
do_handle(<<"GET">>, [<<"tx">>, Hash, Field], _Req) ->
	case hash_to_filename(tx, Hash) of
		{error, invalid} ->
			{400, #{}, <<"Invalid hash.">>};
		{error, ID, unavailable} ->
			case is_a_pending_tx(ID) of
				true ->
					{202, #{}, <<"Pending">>};
				false ->
					{404, #{}, <<"Not Found.">>}
			end;
		{ok, Filename} ->
			case Field of
				<<"tags">> ->
					TX = ar_storage:do_read_tx(Filename),
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
					)};
				_ ->
					{ok, JSONBlock} = file:read_file(Filename),
					{TXJSON} = ar_serialize:dejsonify(JSONBlock),
					Res = val_for_key(Field, TXJSON),
					{200, #{}, Res}
			end
	end;

%% @doc Share the location of a given service with a peer.
%% POST request to endpoint /services where the body of the request is a JSON encoded serivce as
%% specified in ar_serialize.
do_handle(<<"POST">>, [<<"services">>], Req) ->
	{BodyBin, _} = read_complete_body(Req),
	{ServicesJSON} = ar_serialize:jsonify(BodyBin),
	ar_services:add(
		whereis(http_services_node),
		lists:map(
			fun({Vals}) ->
				{<<"name">>, Name} = lists:keyfind(<<"name">>, 1, Vals),
				{<<"host">>, Host} = lists:keyfind(<<"host">>, 1, Vals),
				{<<"expires">>, Expiry} = lists:keyfind(<<"expires">>, 1, Vals),
				#service { name = Name, host = Host, expires = Expiry }
			end,
			ServicesJSON
		)
	),
	{200, #{}, "OK"};

%% @doc Return the current block hieght, or 500
do_handle(Method, [<<"height">>], _Req)
		when (Method == <<"GET">>) or (Method == <<"HEAD">>) ->
	case ar_node:get_height(whereis(http_entrypoint_node)) of
		-1 -> {503, #{}, <<"Node has not joined the network yet.">>};
		H -> {200, #{}, integer_to_binary(H)}
	end;

%% @doc If we are given a hash with no specifier (block, tx, etc), assume that
%% the user is requesting the data from the TX associated with that hash.
%% Optionally allow a file extension.
do_handle(<<"GET">>, [<< Hash:43/binary, MaybeExt/binary >>], Req) ->
	case MaybeExt of
		<< ".", Part/binary >> ->
			do_handle(<<"GET">>, [<<"tx">>, Hash, <<"data.", Part/binary>>], Req);
		<<>> ->
			do_handle(<<"GET">>, [<<"tx">>, Hash, <<"data.html">>], Req);
		_ ->
			not_found()
	end;

%% @doc Catch case for requests made to unknown endpoints.
%% Returns error code 400 - Request type not found.
do_handle(_, _, _) ->
	not_found().

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

split_path(Path) ->
	lists:filter(
		fun(C) -> C /= <<>> end,
		string:split(Path, <<"/">>, all)
	).

sendfile(Filename) ->
	{sendfile, 0, filelib:file_size(Filename), Filename}.

read_complete_body(Req) ->
	read_complete_body(Req, <<>>).

read_complete_body(UnreadReq, Acc) ->
	case cowboy_req:read_body(UnreadReq) of
		{more, Data, ReadReq} ->
			read_complete_body(ReadReq, <<Acc/binary, Data/binary>>);
		{ok, Data, ReadReq} ->
			{<<Acc/binary, Data/binary>>, ReadReq}
	end.

not_found() ->
	{400, #{}, <<"Request type not found.">>}.

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

handle_get_wallet_txs(Addr, EarliestTXID) ->
	case safe_decode(Addr) of
		{error, invalid} ->
			{400, #{}, <<"Invalid address.">>};
		{ok, AddrOK} ->
			{ok, LastTXID} = ar_node:get_last_tx(whereis(http_entrypoint_node), AddrOK),
			EncodedTXIDs = lists:map(fun ar_util:encode/1, get_wallet_txs(EarliestTXID, LastTXID)),
			{200, #{}, ar_serialize:jsonify(EncodedTXIDs)}
	end.

%% @doc Returns a list of all TX IDs starting with LastTXID to EarliestTXID (inclusive)
%% for the same wallet.
get_wallet_txs(EarliestTXID, LatestTXID) ->
	get_wallet_txs(EarliestTXID, LatestTXID, []).

get_wallet_txs(EarliestTXID, PreviousTXID, Acc) ->
	case PreviousTXID of
		<<>> ->
			lists:reverse(Acc);
		EarliestTXID ->
			lists:reverse([EarliestTXID | Acc]);
		_ ->
			TX = ar_storage:read_tx(PreviousTXID),
			get_wallet_txs(EarliestTXID, TX#tx.last_tx, [PreviousTXID | Acc])
	end.

handle_post_tx(TX) ->
	% Check whether the TX is already ignored, ignore it if it is not
	% (and then pass to processing steps).
	case ar_bridge:is_id_ignored(TX#tx.id) of
		true -> {error_response, {208, <<"Transaction already processed.">>}};
		false ->
			ar_bridge:ignore_id(TX#tx.id),
			case ar_node:get_current_diff(whereis(http_entrypoint_node)) of
				Diff ->
					% Validate that the waiting TXs in the pool for a wallet do not exceed the balance.
					FloatingWalletList = ar_node:get_wallet_list(whereis(http_entrypoint_node)),
					WaitingTXs = ar_node:get_all_known_txs(whereis(http_entrypoint_node)),
					OwnerAddr = ar_wallet:to_address(TX#tx.owner),
					WinstonInQueue =
						lists:sum(
							[
								T#tx.quantity + T#tx.reward
							||
								T <- WaitingTXs, OwnerAddr == ar_wallet:to_address(T#tx.owner)
							]
						),
					case [ Balance || {Addr, Balance, _} <- FloatingWalletList, OwnerAddr == Addr ] of
						[B|_] when ((WinstonInQueue + TX#tx.reward + TX#tx.quantity) > B) ->
							ar:info(
								[
									{offered_txs_for_wallet_exceed_balance, ar_util:encode(OwnerAddr)},
									{balance, B},
									{in_queue, WinstonInQueue},
									{cost, TX#tx.reward + TX#tx.quantity}
								]
							),
							{error_response, {400, #{}, <<"Waiting TXs exceed balance for wallet.">>}};
						_ ->
							% Finally, validate the veracity of the TX.
							case ar_tx:verify(TX, Diff, FloatingWalletList) of
								false ->
									%ar:d({rejected_tx , ar_util:encode(TX#tx.id)}),
									{error_response, {400, #{}, <<"Transaction verification failed.">>}};
								true ->
									%ar:d({accepted_tx , ar_util:encode(TX#tx.id)}),
									ar_bridge:add_tx(whereis(http_bridge_node), TX),%, OrigPeer),
									ok
							end
					end
			end
	end.

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
		Path = split_path(cowboy_req:path(Req)),
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
	case safe_decode(Hash) of
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
	lists:member(ID, ar_node:get_pending_txs(whereis(http_entrypoint_node))).

%% @doc Given a request, returns a blockshadow.
request_to_struct_with_blockshadow(Req) ->
	try
		{BlockJSON, _} = read_complete_body(Req),
		{Struct} = ar_serialize:dejsonify(BlockJSON),
		JSONB = val_for_key(<<"new_block">>, Struct),
		BShadow = ar_serialize:json_struct_to_block(JSONB),
		{ok, {Struct, BShadow}}
	catch
		Exception:Reason ->
			{error, {Exception, Reason}}
	end.

%% @doc Generate and return an informative JSON object regarding
%% the state of the node.
return_info() ->
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
		)
	}.

%% @doc wrapper aound util:decode catching exceptions for invalid base64url encoding.
safe_decode(X) ->
	try
		D = ar_util:decode(X),
		{ok, D}
	catch
		_:_ ->
			{error, invalid}
	end.

%% @doc converts a tuple of atoms to a {Module, Function} tuple.
type_to_mf({tx, lookup_filename}) ->
	{ar_storage, lookup_tx_filename};
type_to_mf({block, lookup_filename}) ->
	{ar_storage, lookup_block_filename}.

%% @doc Convenience function for lists:keyfind(Key, 1, List).
%% returns Value not {Key, Value}.
val_for_key(K, L) ->
	{K, V} = lists:keyfind(K, 1, L),
	V.

%% @doc Handle multiple steps of POST /block. First argument is a subcommand,
%% second the argument for that subcommand.
post_block(request, Req) ->
	% Convert request to struct and block shadow.
	case request_to_struct_with_blockshadow(Req) of
		{error, {_, _}} ->
			{400, #{}, <<"Invalid block.">>};
		{ok, {ReqStruct, BShadow}} ->
			OrigPeer = arweave_peer(Req),
			post_block(check_data_segment_processed, {ReqStruct, BShadow, OrigPeer})
	end;
%% TODO: Make block_data_segment mandatory when all nodes are posting it.
post_block(check_data_segment_processed, {ReqStruct, BShadow, OrigPeer}) ->
	% Check if block is already known.
	case lists:keyfind(<<"block_data_segment">>, 1, ReqStruct) of
		{_, BDSEncoded} ->
			BDS = ar_util:decode(BDSEncoded),
			case ar_bridge:is_id_ignored(BDS) of
				true ->
					{208, #{}, <<"Block Data Segment already processed.">>};
				false ->
					post_block(check_indep_hash_processed, {ReqStruct, BShadow, OrigPeer, BDS})
			end;
		_ ->
			post_block(check_indep_hash_processed, {ReqStruct, BShadow, OrigPeer, no_data_segment})
	end;
%% TODO: Remove the check_is_ignored clause when all nodes send block_data_segment.
post_block(check_indep_hash_processed, {ReqStruct, BShadow, OrigPeer, BDS}) ->
	case ar_bridge:is_id_ignored(BShadow#block.indep_hash) of
		true ->
			{208, <<"Block already processed.">>};
		false ->
			ar_bridge:ignore_id(BShadow#block.indep_hash),
			post_block(check_is_joined, {ReqStruct, BShadow, OrigPeer, BDS})
	end;
post_block(check_is_joined, {ReqStruct, BShadow, OrigPeer, BDS}) ->
	% Check if node is joined.
	case ar_node:is_joined(whereis(http_entrypoint_node)) of
		false ->
			{503, #{}, <<"Not joined.">>};
		true ->
			post_block(check_difficulty, {ReqStruct, BShadow, OrigPeer, BDS})
	end;
%% The MIN_DIFF check is filtering out blocks from smaller networks, e.g.
%% testnets. Therefor, we don't want to log when this check or any check above
%% rejects the block because there are potentially a lot of rejections.
post_block(check_difficulty, {ReqStruct, BShadow, OrigPeer, BDS}) ->
	case BShadow#block.diff >= ?MIN_DIFF of
		true ->
			post_block(check_pow, {BShadow, ReqStruct, OrigPeer, BDS});
		_ ->
			{400, #{}, <<"Difficulty too low">>}
	end;
%% TODO: Enable check_timestamp when all nodes are running the new miner which
%% updates the timestamp regularly. Also re-enable:
%% ar_http_iface_tests:add_external_block_with_invalid_timestamp_test/0
%% post_block(check_timestamp, {ReqStruct, BShadow, OrigPeer, BDS}) ->
%% 	% Verify the timestamp of the block shadow.
%% 	case ar_block:verify_timestamp(BShadow) of
%% 		false ->
%% 			post_block_reject_warn(
%% 				BShadow,
%% 				check_timestamp,
%% 				[{block_time, BShadow#block.timestamp},
%% 				 {current_time, os:system_time(seconds)}]
%% 			),
%% 			{400, #{}, <<"Invalid timestamp.">>};
%% 		true ->
%% 			post_block(check_pow, {ReqStruct, BShadow, OrigPeer, BDS})
%% 	end;
%% Note! Checking PoW should be as cheap as possible. All slow steps should
%% be after the PoW check to reduce the possibility of doing a DOS attack on
%% the network.
post_block(check_pow, {BShadow, ReqStruct, OrigPeer, BDS}) ->
	case BDS of
		no_data_segment ->
			post_block(post_block, {BShadow, ReqStruct, OrigPeer, BDS});
		_ ->
			case ar_mine:validate(BDS, BShadow#block.nonce, BShadow#block.diff) of
				{invalid, _} ->
					post_block_reject_warn(BShadow, check_pow),
					{400, #{}, <<"Invalid Block Proof of Work">>};
				{valid, _} ->
					ar_bridge:ignore_id(BDS),
					post_block(post_block, {BShadow, ReqStruct, OrigPeer, BDS})
			end
	end;
post_block(post_block, {BShadow, ReqStruct, OrigPeer, BDS}) ->
	%% The ar_block:generate_block_from_shadow/2 call is potentially slow. Since
	%% all validation steps already passed, we can do the rest in a separate
	spawn(fun() ->
		RecallSize = val_for_key(<<"recall_size">>, ReqStruct),
		B = ar_block:generate_block_from_shadow(BShadow, RecallSize),
		RecallIndepHash = ar_util:decode(val_for_key(<<"recall_block">>, ReqStruct)),
		Key = ar_util:decode(val_for_key(<<"key">>, ReqStruct)),
		Nonce = ar_util:decode(val_for_key(<<"nonce">>, ReqStruct)),
		ar:info([{
			sending_external_block_to_bridge,
			ar_util:encode(B#block.indep_hash)
		}]),
		ar_bridge:add_block(
			whereis(http_bridge_node),
			OrigPeer,
			B,
			BDS,
			{RecallIndepHash, RecallSize, Key, Nonce}
		)
	end),
	{200, #{}, <<"OK">>}.

post_block_reject_warn(BShadow, Step) ->
	ar:warn([
		{post_block_rejected, ar_util:encode(BShadow#block.indep_hash)},
		Step
	]).

%% post_block_reject_warn(BShadow, Step, Params) ->
%% 	ar:warn([
%% 		{post_block_rejected, ar_util:encode(BShadow#block.indep_hash)},
%% 		{Step, Params}
%% 	]).

%% @doc Return the block hash list associated with a block.
process_request(get_block, [Type, ID, <<"hash_list">>]) ->
	CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	case is_block_known(Type, ID, CurrentBHL) of
		true ->
			Hash =
				case Type of
					<<"height">> ->
						B =
							ar_node:get_block(whereis(http_entrypoint_node),
							ID,
							CurrentBHL),
						B#block.indep_hash;
					<<"hash">> -> ID
				end,
			BlockBHL = ar_block:generate_hash_list_for_block(Hash, CurrentBHL),
			{200, #{},
				ar_serialize:jsonify(
					ar_serialize:hash_list_to_json_struct(
						BlockBHL
					)
				)
			};
		false ->
			{404, #{}, <<"Block not found.">>}
	end;
%% @doc Return the wallet list associated with a block (as referenced by hash
%% or height).
process_request(get_block, [Type, ID, <<"wallet_list">>]) ->
	HTTPEntryPointPid = whereis(http_entrypoint_node),
	CurrentBHL = ar_node:get_hash_list(HTTPEntryPointPid),
	case is_block_known(Type, ID, CurrentBHL) of
		false -> {404, #{}, <<"Block not found.">>};
		true ->
			B = find_block(Type, ID, CurrentBHL),
			case ?IS_BLOCK(B) of
				true ->
					{200, #{},
						ar_serialize:jsonify(
							ar_serialize:wallet_list_to_json_struct(
								B#block.wallet_list
							)
						)
					};
				false ->
					{404, #{}, <<"Block not found.">>}
			end
	end;
%% @doc Return a given field for the the blockshadow corresponding to the block height, 'height'.
%% GET request to endpoint /block/hash/{hash|height}/{field}
%%
%% {field} := { nonce | previous_block | timestamp | last_retarget | diff | height | hash | indep_hash
%%				txs | hash_list | wallet_list | reward_addr | tags | reward_pool }
%%
process_request(get_block, [Type, ID, Field]) ->
	CurrentBHL = ar_node:get_hash_list(whereis(http_entrypoint_node)),
	case ar_meta_db:get(subfield_queries) of
		true ->
			case find_block(Type, ID, CurrentBHL) of
				unavailable ->
						{404, #{}, <<"Not Found.">>};
				B ->
					{BLOCKJSON} = ar_serialize:block_to_json_struct(B),
					{_, Res} = lists:keyfind(list_to_existing_atom(binary_to_list(Field)), 1, BLOCKJSON),
					Result = block_field_to_string(Field, Res),
					{200, #{}, Result}
			end;
		_ ->
			{421, #{}, <<"Subfield block querying is disabled on this node.">>}
	end.

validate_get_block_type_id(<<"height">>, ID) ->
	try binary_to_integer(ID) of
		Int -> {ok, Int}
	catch _:_ ->
		{error, {400, #{}, <<"Invalid height.">>}}
	end;
validate_get_block_type_id(<<"hash">>, ID) ->
	case safe_decode(ID) of
		{ok, Hash} -> {ok, Hash};
		invalid    -> {error, {400, #{}, <<"Invalid hash.">>}}
	end.

%% @doc Take a block type specifier, an ID, and a BHL, returning whether the
%% given block is part of the BHL.
is_block_known(<<"height">>, RawHeight, BHL) when is_binary(RawHeight) ->
	is_block_known(<<"height">>, binary_to_integer(RawHeight), BHL);
is_block_known(<<"height">>, Height, BHL) ->
	Height < length(BHL);
is_block_known(<<"hash">>, ID, BHL) ->
	lists:member(ID, BHL).

%% @doc Find a block, given a type and a specifier.
find_block(<<"height">>, RawHeight, BHL) ->
	ar_node:get_block(
		whereis(http_entrypoint_node),
		binary_to_integer(RawHeight),
		BHL
	);
find_block(<<"hash">>, ID, BHL) ->
	ar_storage:read_block(ID, BHL).
