%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface_client).

-export([send_new_block/4, send_new_tx/2, get_block/3, get_block/4]).
-export([get_block_shadow/2]).
-export([get_tx/3, get_tx/4, get_tx_data/2, get_tx_from_remote_peer/3]).
-export([get_block_subfield/3, add_peer/1]).
-export([get_info/1, get_info/2, get_peers/1, get_peers/2, get_pending_txs/1]).
-export([get_time/2, get_height/1]).
-export([get_wallet_list/2, get_block_index/1, get_block_index/2]).

-include("ar.hrl").

%% @doc Send a new transaction to an Arweave HTTP node.
send_new_tx(Peer, TX) ->
	case byte_size(TX#tx.data) of
		_Size when _Size < ?TX_SEND_WITHOUT_ASKING_SIZE_LIMIT ->
			do_send_new_tx(Peer, TX);
		_ ->
			case has_tx(Peer, TX#tx.id) of
				doesnt_have_tx -> do_send_new_tx(Peer, TX);
				has_tx -> not_sent;
				error -> not_sent
			end
	end.

do_send_new_tx(Peer, TX) ->
	TXSize = byte_size(TX#tx.data),
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/tx",
		p2p_headers() ++ [{<<"arweave-tx-id">>, ar_util:encode(TX#tx.id)}],
		ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
		500,
		max(3, min(60, TXSize * 8 div ?TX_PROPAGATION_BITS_PER_SECOND)) * 1000
	).

%% @doc Check whether a peer has a given transaction
has_tx(Peer, ID) ->
	case
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/tx/" ++ binary_to_list(ar_util:encode(ID)) ++ "/id",
			p2p_headers() ++ ar_tx:tx_format_to_http_header(?WITH_TX_HEADERS),
			[],
			500,
			3 * 1000
		)
	of
		{ok, {{<<"200">>, _}, _, _, _, _}} -> has_tx;
		{ok, {{<<"202">>, _}, _, _, _, _}} -> has_tx; % In the mempool
		{ok, {{<<"404">>, _}, _, _, _, _}} -> doesnt_have_tx;
		_ -> error
	end.


%% @doc Distribute a newly found block to remote nodes.
send_new_block(Peer, NewB, BDS, Recall) ->
	ShortHashList =
		lists:map(
			fun ar_util:encode/1,
			lists:sublist(NewB#block.hash_list, ?STORE_BLOCKS_BEHIND_CURRENT)
		),
	{SmallBlockProps} =
		ar_serialize:block_to_json_struct(
			NewB#block{ wallet_list = [] }
		),
	BlockShadowProps =
		[{<<"hash_list">>, ShortHashList} | SmallBlockProps],
	PostProps = [
		{<<"new_block">>, {BlockShadowProps}},
		%% Add the P2P port field to be backwards compatible with nodes
		%% running the old version of the P2P port feature.
		{<<"port">>, ?DEFAULT_HTTP_IFACE_PORT},
		{<<"block_data_segment">>, ar_util:encode(BDS)}
	] ++
	case Recall of
		{RecallIndepHash, RecallSize, Key, Nonce} ->
			[
				{<<"recall_block">>, ar_util:encode(RecallIndepHash)},
				{<<"recall_size">>, RecallSize},
				{<<"key">>, ar_util:encode(Key)},
				{<<"nonce">>, ar_util:encode(Nonce)}
			];
		_ -> []
	end,
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/block",
		p2p_headers() ++ [{<<"arweave-block-hash">>, ar_util:encode(NewB#block.indep_hash)}],
		ar_serialize:jsonify({PostProps}),
		3 * 1000
	).

%% @doc Request to be added as a peer to a remote host.
add_peer(Peer) ->
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/peers",
		p2p_headers(),
		ar_serialize:jsonify(
			{
				[
					{network, list_to_binary(?NETWORK_NAME)}
				]
			}
		),
		3 * 1000
	).

%% @doc Retreive a block by height or hash from a remote peer.
get_block(Peer, ID, BI) ->
	get_block(Peer, ID, BI, ?WITH_TX_DATA).

get_block([], _ID, _BI, _TXFormat) ->
	unavailable;
get_block(Peers = [_|_], ID, BI, TXFormat) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	case handle_block_response(
		Peer,
		Peers,
		ar_httpc:request(
			<<"GET">>,
			Peer,
			prepare_block_id(ID),
			p2p_headers(),
			[],
			10 * 1000
		),
		{full_block, BI}, TXFormat
	) of
		unavailable ->
			get_block(Peers -- [Peer], ID, BI, TXFormat);
		not_found ->
			get_block(Peers -- [Peer], ID, BI, TXFormat);
		B ->
			{Peer, B}
	end;
get_block(Peer, ID, BI, TXFormat) ->
	get_block([Peer], ID, BI, TXFormat).

%% @doc Get a specified subfield from the block with the given hash
get_block_subfield(Peer, Hash, Subfield) when is_binary(Hash) ->
	handle_block_field_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/" ++ Subfield,
			p2p_headers()
		)
	);
%% @doc Get a specified subfield from the block with the given height
get_block_subfield(Peer, Height, Subfield) when is_integer(Height) ->
	handle_block_field_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/height/" ++integer_to_list(Height) ++ "/" ++ Subfield,
			p2p_headers()
		)
	).

%% @doc Generate an appropriate URL for a block by its identifier.
prepare_block_id({ID, _, _}) ->
	prepare_block_id(ID);
prepare_block_id(ID) when is_binary(ID) ->
	"/block/hash/" ++ binary_to_list(ar_util:encode(ID));
prepare_block_id(ID) when is_integer(ID) ->
	"/block/height/" ++ integer_to_list(ID).

%% @doc Retreive a block shadow by hash or height from remote peers.
get_block_shadow([], _ID) ->
	unavailable;
get_block_shadow(Peers, ID) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	case handle_block_response(
		Peer,
		Peers,
		ar_httpc:request(
			<<"GET">>,
			Peer,
			prepare_block_id(ID),
			p2p_headers(),
			[],
			10 * 1000
		),
		block_shadow
	) of
		unavailable ->
			get_block_shadow(Peers -- [Peer], ID);
		not_found ->
			get_block_shadow(Peers -- [Peer], ID);
		B ->
			{Peer, B}
	end.

%% @doc Get a wallet list (by its hash) from the external peer.
get_wallet_list(Peer, Hash) ->
	Response =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/wallet_list",
			p2p_headers()
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:json_struct_to_wallet_list(Body);
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found;
		_ -> unavailable
	end.

%% @doc Get a block hash list (by its hash) from the external peer.
get_block_index(Peer) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/hash_list",
			p2p_headers()
		),
	case ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(Body)) of
		BI = [{_H, _WS, _TXRoot} | _] -> BI;
		Hashes -> [{H, not_set, not_set} || H <- Hashes]
	end.

get_block_index(Peer, Hash) ->
	Response =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/hash_list",
			p2p_headers()
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:dejsonify(ar_serialize:json_struct_to_block_index(Body));
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found
	end.

%% @doc Return the current height of a remote node.
get_height(Peer) ->
	Response =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/height",
			p2p_headers()
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} -> binary_to_integer(Body);
		{ok, {{<<"500">>, _}, _, _, _, _}} -> not_joined
	end.

get_txs(_Peers, _MempoolTXs, _B = #block{txs = TXIDs}, ?NO_TX) -> {ok, TXIDs};
get_txs(Peers, MempoolTXs, B, TXFormat) ->
	case B#block.txs of
		TXIDs when length(TXIDs) > ?BLOCK_TX_COUNT_LIMIT ->
			{error, txs_count_exceeds_limit};
		TXIDs ->
			get_txs(Peers, MempoolTXs, TXIDs, [], 0, TXFormat)
	end.

get_txs(_Peers, _MempoolTXs, [], TXs, _TotalSize, _TXFormat) ->
	{ok, lists:reverse(TXs)};
get_txs(Peers, MempoolTXs, [TXID | Rest], TXs, TotalSize, TXFormat) ->
	case get_tx(Peers, TXID, MempoolTXs, TXFormat) of
		TX when is_record(TX, tx) ->
			case TotalSize + byte_size(TX#tx.data) of
				NewTotalSize when NewTotalSize > ?BLOCK_TX_DATA_SIZE_LIMIT ->
					{error, txs_exceed_block_size_limit};
				NewTotalSize ->
					get_txs(Peers, MempoolTXs, Rest, [TX | TXs], NewTotalSize, TXFormat)
			end;
		_ ->
			{error, tx_not_found}
	end.

%% @doc Retreive a tx by ID from the memory pool, disk, or a remote peer.
get_tx(Peers, TXID, MempoolTXs) ->
	get_tx(Peers, TXID, MempoolTXs, ?WITH_TX_DATA).
get_tx(Peers, TXID, MempoolTXs, TXFormat) ->
	case list_search(
		fun(TX) ->
			TXID == TX#tx.id
		end,
		MempoolTXs
	) of
		{value, TX} ->
			TX;
		false ->
			get_tx_from_disk_or_peer(Peers, TXID, TXFormat)
	end.

get_tx_from_disk_or_peer(Peers, TXID, TXFormat) ->
	case ar_storage:read_tx(TXID) of
		unavailable ->
			get_tx_from_remote_peer(Peers, TXID, TXFormat);
		TX ->
			TX
	end.

get_tx_from_remote_peer([], _TXID, _TXFormat) ->
	not_found;
get_tx_from_remote_peer(Peers, TXID, TXFormat) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	case handle_tx_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/tx/" ++ binary_to_list(ar_util:encode(TXID)),
			p2p_headers() ++ ar_tx:tx_format_to_http_header(TXFormat),
			[],
			500,
			10 * 1000
		)
	) of
		not_found ->
			get_tx_from_remote_peer(Peers -- [Peer], TXID, TXFormat);
		pending ->
			get_tx_from_remote_peer(Peers -- [Peer], TXID, TXFormat);
		gone ->
			get_tx_from_remote_peer(Peers -- [Peer], TXID, TXFormat);
		ShouldBeTX ->
			ShouldBeTX
	end.

list_search(_Pred, []) ->
	false;
list_search(Pred, [Head | Rest]) ->
	case Pred(Head) of
		false ->
			list_search(Pred, Rest);
		true ->
			{value, Head}
	end.

%% @doc Retreive only the data associated with a transaction.
get_tx_data(Peer, Hash) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/" ++ binary_to_list(ar_util:encode(Hash)),
			p2p_headers(),
			[],
			10 * 1000
		),
	Body.

%% @doc Retreive the current universal time as claimed by a foreign node.
get_time(Peer, Timeout) ->
	Parent = self(),
	Ref = make_ref(),
	Child = spawn_link(
		fun() ->
			Resp = ar_httpc:request(<<"GET">>, Peer, "/time", p2p_headers(), <<>>, Timeout + 100),
			Parent ! {get_time_response, Ref, Resp}
		end
	),
	receive
		{get_time_response, Ref, Response} ->
			case Response of
				{ok, {{<<"200">>, _}, _, Body, Start, End}} ->
					Time = binary_to_integer(Body),
					RequestTime = ceil((End - Start) / 1000000),
					%% The timestamp returned by the HTTP daemon is floored second precision. Thus the
					%% upper bound is increased by 1.
					{ok, {Time - RequestTime, Time + RequestTime + 1}};
				_ ->
					{error, Response}
			end
		after Timeout ->
			%% Note: the fusco client (used in ar_httpc:request) is not shutdown properly this way.
			exit(Child, {shutdown, timeout}),
			{error, timeout}
	end.

%% @doc Retreive all valid transactions held that have not yet been mined into
%% a block from a remote peer.
get_pending_txs(Peer) ->
	try
		begin
			{ok, {{200, _}, _, Body, _, _}} =
				ar_httpc:request(
					<<"GET">>,
					Peer,
					"/tx/pending",
					p2p_headers()
				),
			PendingTxs = ar_serialize:dejsonify(Body),
			[list_to_binary(P) || P <- PendingTxs]
		end
	catch _:_ -> []
	end.

%% @doc Retreive information from a peer. Optionally, filter the resulting
%% keyval list for required information.
get_info(Peer, Type) ->
	case get_info(Peer) of
		info_unavailable -> info_unavailable;
		Info ->
			{Type, X} = lists:keyfind(Type, 1, Info),
			X
	end.
get_info(Peer) ->
	case
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/info",
			p2p_headers()
		)
	of
		{ok, {{<<"200">>, _}, _, JSON, _, _}} -> process_get_info_json(JSON);
		_ -> info_unavailable
	end.

%% @doc Return a list of parsed peer IPs for a remote server.
get_peers(Peer) -> get_peers(Peer, default_timeout).

get_peers(Peer, Timeout) ->
	try
		begin
			{ok, {{<<"200">>, _}, _, Body, _, _}} =
				ar_httpc:request(
					<<"GET">>,
					Peer,
					"/peers",
					p2p_headers(),
					[],
					Timeout
				),
			PeerArray = ar_serialize:dejsonify(Body),
			lists:map(fun ar_util:parse_peer/1, PeerArray)
		end
	catch _:_ -> unavailable
	end.

%% @doc Produce a key value list based on a /info response.
process_get_info_json(JSON) ->
	case ar_serialize:json_decode(JSON) of
		{ok, {Props}} ->
			process_get_info(Props);
		{error, _} ->
			info_unavailable
	end.

process_get_info(Props) ->
	Keys = [<<"network">>, <<"version">>, <<"height">>, <<"blocks">>, <<"peers">>],
	case safe_get_vals(Keys, Props) of
		error ->
			info_unavailable;
		{ok, [NetworkName, ClientVersion, Height, Blocks, Peers]} ->
			ReleaseNumber =
				case lists:keyfind(<<"release">>, 1, Props) of
					false -> 0;
					R -> R
				end,
			[
				{name, NetworkName},
				{version, ClientVersion},
				{height, Height},
				{blocks, Blocks},
				{peers, Peers},
				{release, ReleaseNumber}
			]
	end.

%% @doc Process the response of an /block call.
handle_block_response(_, _, {error, _}, _) -> unavailable;
handle_block_response(_, _, {ok, {{<<"400">>, _}, _, _, _, _}}, _) -> unavailable;
handle_block_response(_, _, {ok, {{<<"404">>, _}, _, _, _, _}}, _) -> not_found;
handle_block_response(_, _, {ok, {{<<"500">>, _}, _, _, _, _}}, _) -> unavailable;
handle_block_response(Peer, _Peers, {ok, {{<<"200">>, _}, _, Body, _, _}}, block_shadow) ->
	case catch ar_serialize:json_struct_to_block(Body) of
		{'EXIT', Reason} ->
			ar:info([
				"Failed to parse block response.",
				{peer, Peer},
				{reason, Reason}
			]),
			unavailable;
		Handled ->
			Handled
	end;
handle_block_response(_, _, Response, _) ->
	ar:warn([{unexpected_block_response, Response}]),
	unavailable.

handle_block_response(Peer, Peers, {ok, {{<<"200">>, _}, _, Body, _, _}}, {full_block, BI}, TXFormat) ->
	case catch reconstruct_full_block(Peer, Peers, Body, BI, TXFormat) of
		{'EXIT', Reason} ->
			ar:info([
				"Failed to parse block response.",
				{peer, Peer},
				{reason, Reason}
			]),
			unavailable;
		Handled ->
			Handled
	end;
handle_block_response(Peer, Peers, Response, Context, _TXFormat) ->
	handle_block_response(Peer, Peers, Response, Context).

reconstruct_full_block(Peer, Peers, Body, BI, TXFormat) ->
	B = ar_serialize:json_struct_to_block(Body),
	case ?IS_BLOCK(B) of
		true ->
			WalletList =
				case B#block.wallet_list of
					WL when is_list(WL) -> WL;
					WL when is_binary(WL) ->
						case ar_storage:read_wallet_list(WL) of
							{ok, ReadWL} ->
								ReadWL;
							{error, _} ->
								get_wallet_list(Peer, B#block.indep_hash)
						end
				end,
			HashList =
				case B#block.hash_list of
					unset ->
						ar_block:generate_hash_list_for_block(
							B,
							BI
						);
					HL -> HL
				end,
			MempoolTXs = ar_node:get_pending_txs(whereis(http_entrypoint_node)),
			TXFormatByHeight =
				case B#block.height >= ar_fork:height_2_0() of
					true -> TXFormat;
					false -> ?WITH_TX_DATA
				end,
			case {get_txs(Peers, MempoolTXs, B, TXFormatByHeight), WalletList} of
				{{ok, TXs}, MaybeWalletList} when is_list(MaybeWalletList) ->
					B#block {
						txs = TXs,
						hash_list = HashList,
						wallet_list = WalletList
					};
				_ ->
					unavailable
			end;
		false -> B
	end.

%% @doc Process the response of a /block/[{Height}|{Hash}]/{Subfield} call.
handle_block_field_response(Response) ->
	case catch handle_block_field_response1(Response) of
		{'EXIT', _} -> unavailable;
		Handled -> Handled
	end.

handle_block_field_response1({"timestamp", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	binary_to_integer(Body);
handle_block_field_response1({"last_retarget", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	binary_to_integer(Body);
handle_block_field_response1({"diff", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	binary_to_integer(Body);
handle_block_field_response1({"height", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	binary_to_integer(Body);
handle_block_field_response1({"txs", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	ar_serialize:json_struct_to_tx(Body);
handle_block_field_response1({"hash_list", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	lists:map(fun ar_util:decode/1, ar_serialize:dejsonify(Body));
handle_block_field_response1({"wallet_list", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	ar_serialize:json_struct_to_wallet_list(Body);
handle_block_field_response1({_Subfield, {ok, {{<<"200">>, _}, _, Body, _, _}}}) -> Body;
handle_block_field_response1({error, _}) -> unavailable;
handle_block_field_response1({ok, {{<<"404">>, _}, _, _}}) -> not_found;
handle_block_field_response1({ok, {{<<"500">>, _}, _, _}}) -> unavailable;
handle_block_field_response1(Response) ->
	ar:warn([{unexpected_block_field_response, Response}]),
	unavailable.

%% @doc Process the response of a /tx call.
handle_tx_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch ar_serialize:json_struct_to_tx(Body) of
		{'EXIT', _} -> not_found;
		TX -> TX
	end;
handle_tx_response({ok, {{<<"202">>, _}, _, _, _, _}}) -> pending;
handle_tx_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_tx_response({ok, {{<<"410">>, _}, _, _, _, _}}) -> gone;
handle_tx_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> not_found;
handle_tx_response(Response) ->
	ar:warn([{unexpected_tx_response, Response}]),
	not_found.

p2p_headers() ->
	[{<<"X-P2p-Port">>, integer_to_binary(ar_meta_db:get(port))}].

%% @doc Return values for keys - or error if any key is missing.
safe_get_vals(Keys, Props) ->
	case lists:foldl(fun
			(_, error) -> error;
			(Key, Acc) ->
				case lists:keyfind(Key, 1, Props) of
					{_, Val} -> [Val | Acc];
					_        -> error
				end
			end, [], Keys) of
		error -> error;
		Vals  -> {ok, lists:reverse(Vals)}
	end.
