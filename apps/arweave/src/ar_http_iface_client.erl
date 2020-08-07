%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface_client).

-export([send_new_block/3, send_new_tx/2, get_block/2]).
-export([get_block_shadow/2]).
-export([get_tx/3, get_txs/3, get_tx_from_remote_peer/2, get_tx_data/2]).
-export([add_peer/1]).
-export([get_info/1, get_info/2, get_peers/1, get_pending_txs/1]).
-export([get_time/2, get_height/1]).
-export([get_block_index/1, get_block_index/2]).
-export([get_sync_record/1, get_chunk/2]).

-include("ar.hrl").
-include("ar_data_sync.hrl").

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
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/tx",
		headers => p2p_headers() ++ [{<<"arweave-tx-id">>, ar_util:encode(TX#tx.id)}],
		body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
		connect_timeout => 500,
		timeout => max(3, min(60, TXSize * 8 div ?TX_PROPAGATION_BITS_PER_SECOND)) * 1000
	}).

%% @doc Check whether a peer has a given transaction
has_tx(Peer, ID) ->
	case
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/tx/" ++ binary_to_list(ar_util:encode(ID)) ++ "/id",
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 3 * 1000
		})
	of
		{ok, {{<<"200">>, _}, _, _, _, _}} -> has_tx;
		{ok, {{<<"202">>, _}, _, _, _, _}} -> has_tx; % In the mempool
		{ok, {{<<"404">>, _}, _, _, _, _}} -> doesnt_have_tx;
		_ -> error
	end.


%% @doc Distribute a newly found block to remote nodes.
send_new_block(Peer, NewB, BDS) ->
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
	],
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/block",
		headers => p2p_headers() ++ [{<<"arweave-block-hash">>, ar_util:encode(NewB#block.indep_hash)}],
		body => ar_serialize:jsonify({PostProps}),
		timeout => 3 * 1000
	}).

%% @doc Request to be added as a peer to a remote host.
add_peer(Peer) ->
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/peers",
		headers => p2p_headers(),
		body => ar_serialize:jsonify({[{network, list_to_binary(?NETWORK_NAME)}]}),
		timeout => 3 * 1000
	}).

%% @doc Retreive a block by hash from disk or a remote peer.
get_block(Peers, H) when is_list(Peers) ->
	case ar_storage:read_block(H) of
		unavailable ->
			get_block_from_remote_peers(Peers, H);
		B ->
			case catch reconstruct_full_block(Peers, B) of
				{'EXIT', Reason} ->
					ar:info([
						{event, failed_to_construct_full_block_from_shadow},
						{reason, Reason}
					]),
					unavailable;
				Handled ->
					Handled
			end
	end;
get_block(Peer, H) ->
	get_block([Peer], H).

get_block_from_remote_peers([], _H) ->
	unavailable;
get_block_from_remote_peers(Peers = [_ | _], H) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	case handle_block_response(
		Peer,
		Peers,
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => prepare_block_id(H),
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 30 * 1000,
			limit => ?MAX_BODY_SIZE
		}),
		full_block
	) of
		unavailable ->
			get_block_from_remote_peers(Peers -- [Peer], H);
		not_found ->
			get_block_from_remote_peers(Peers -- [Peer], H);
		B ->
			B
	end.

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
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => prepare_block_id(ID),
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 30 * 1000,
			limit => ?MAX_BODY_SIZE
		}),
		block_shadow
	) of
		unavailable ->
			get_block_shadow(Peers -- [Peer], ID);
		not_found ->
			get_block_shadow(Peers -- [Peer], ID);
		B ->
			{Peer, B}
	end.

%% @doc Get a wallet list by its hash from external peers.
get_wallet_list([], _H) ->
	not_found;
get_wallet_list([Peer | Peers], H) ->
	case get_wallet_list(Peer, H) of
		unavailable ->
			get_wallet_list(Peers, H);
		not_found ->
			get_wallet_list(Peers, H);
		WL ->
			WL
	end;
get_wallet_list(Peer, H) ->
	Response =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/block/hash/" ++ binary_to_list(ar_util:encode(H)) ++ "/wallet_list",
			headers => p2p_headers()
		}),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:json_struct_to_wallet_list(Body);
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found;
		_ -> unavailable
	end.

%% @doc Get a block hash list (by its hash) from the external peer.
get_block_index(Peer) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/hash_list",
			timeout => 120 * 1000,
			headers => p2p_headers()
		}),
	ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(Body)).

get_block_index(Peer, Hash) ->
	Response =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/hash_list",
			headers => p2p_headers()
		}),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:dejsonify(ar_serialize:json_struct_to_block_index(Body));
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found
	end.

get_sync_record(Peer) ->
	handle_sync_record_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/data_sync_record",
		timeout => 5 * 1000,
		connect_timeout => 500,
		limit => ?MAX_ETF_SYNC_RECORD_SIZE,
		headers => [{<<"Content-Type">>, <<"application/etf">>} | p2p_headers()]
	})).

get_chunk(Peer, Offset) ->
	handle_chunk_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/chunk/" ++ integer_to_binary(Offset),
		timeout => 30 * 1000,
		connect_timeout => 500,
		limit => ?MAX_SERIALIZED_CHUNK_PROOF_SIZE,
		headers => p2p_headers()
	})).

handle_sync_record_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_intervals:safe_from_etf(Body);
handle_sync_record_response(_) ->
	{error, not_found}.

handle_chunk_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch ar_serialize:json_map_to_chunk_proof(jiffy:decode(Body, [return_maps])) of
		{'EXIT', Reason} ->
			{error, Reason};
		Proof ->
			{ok, Proof}
	end;
handle_chunk_response(Response) ->
	{error, Response}.

%% @doc Return the current height of a remote node.
get_height(Peer) ->
	Response =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/height",
			headers => p2p_headers()
		}),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} -> binary_to_integer(Body);
		{ok, {{<<"500">>, _}, _, _, _, _}} -> not_joined
	end.

get_txs(Peers, MempoolTXs, B) ->
	case B#block.txs of
		TXIDs when length(TXIDs) > ?BLOCK_TX_COUNT_LIMIT ->
			ar:err([{event, downloaded_txs_count_exceeds_limit}]),
			{error, txs_count_exceeds_limit};
		TXIDs ->
			get_txs(Peers, MempoolTXs, TXIDs, [], 0)
	end.

get_txs(_Peers, _MempoolTXs, [], TXs, _TotalSize) ->
	{ok, lists:reverse(TXs)};
get_txs(Peers, MempoolTXs, [TXID | Rest], TXs, TotalSize) ->
	case get_tx(Peers, TXID, MempoolTXs) of
		#tx{ format = 2 } = TX ->
			get_txs(Peers, MempoolTXs, Rest, [TX | TXs], TotalSize);
		#tx{ format = 1 } = TX ->
			case TotalSize + TX#tx.data_size of
				NewTotalSize when NewTotalSize > ?BLOCK_TX_DATA_SIZE_LIMIT ->
					ar:err([{event, downloaded_txs_exceed_block_size_limit}]),
					{error, txs_exceed_block_size_limit};
				NewTotalSize ->
					get_txs(Peers, MempoolTXs, Rest, [TX | TXs], NewTotalSize)
			end;
		_ ->
			{error, tx_not_found}
	end.

%% @doc Retreive a tx by ID from the memory pool, disk, or a remote peer.
get_tx(Peers, TXID, MempoolTXs) ->
	case maps:get(TXID, MempoolTXs, not_in_mempool) of
		{TX, _} ->
			TX;
		not_in_mempool ->
			get_tx_from_disk_or_peer(Peers, TXID)
	end.

get_tx_from_disk_or_peer(Peers, TXID) ->
	case ar_storage:read_tx(TXID) of
		unavailable ->
			get_tx_from_remote_peer(Peers, TXID);
		TX ->
			TX
	end.

get_tx_from_remote_peer([], _TXID) ->
	not_found;
get_tx_from_remote_peer(Peers, TXID) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	case handle_tx_response(
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/tx/" ++ binary_to_list(ar_util:encode(TXID)),
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 60 * 1000,
			limit => ?MAX_BODY_SIZE
		})
	) of
		not_found ->
			get_tx_from_remote_peer(Peers -- [Peer], TXID);
		pending ->
			get_tx_from_remote_peer(Peers -- [Peer], TXID);
		gone ->
			get_tx_from_remote_peer(Peers -- [Peer], TXID);
		ShouldBeTX ->
			ShouldBeTX
	end.

%% @doc Retreive only the data associated with a transaction.
%% The function must only be used when it is known that the transaction
%% has data.
get_tx_data([], _Hash) ->
	unavailable;
get_tx_data(Peers, Hash) when is_list(Peers) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	case get_tx_data(Peer, Hash) of
		unavailable ->
			get_tx_data(Peers -- [Peer], Hash);
		Data ->
			Data
	end;
get_tx_data(Peer, Hash) ->
	Reply =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/tx/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/data",
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 120 * 1000,
			limit => ?MAX_BODY_SIZE
		}),
	case Reply of
		{ok, {{<<"200">>, _}, _, <<>>, _, _}} ->
			unavailable;
		{ok, {{<<"200">>, _}, _, EncodedData, _, _}} ->
			case ar_util:safe_decode(EncodedData) of
				{ok, Data} ->
					Data;
				{error, invalid} ->
					unavailable
			end;
		_ ->
			unavailable
	end.

%% @doc Retreive the current universal time as claimed by a foreign node.
get_time(Peer, Timeout) ->
	case ar_http:req(#{method => get, peer => Peer, path => "/time", headers => p2p_headers(), timeout => Timeout + 100}) of
		{ok, {{<<"200">>, _}, _, Body, Start, End}} ->
			Time = binary_to_integer(Body),
			RequestTime = ceil((End - Start) / 1000000),
			%% The timestamp returned by the HTTP daemon is floored second precision. Thus the
			%% upper bound is increased by 1.
			{ok, {Time - RequestTime, Time + RequestTime + 1}};
		Other ->
			{error, Other}
	end.

%% @doc Retreive all valid transactions held that have not yet been mined into
%% a block from a remote peer.
get_pending_txs(Peer) ->
	try
		begin
			{ok, {{200, _}, _, Body, _, _}} =
				ar_http:req(#{
					method => get,
					peer => Peer,
					path => "/tx/pending",
					headers => p2p_headers()
				}),
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
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/info",
			headers => p2p_headers()
		})
	of
		{ok, {{<<"200">>, _}, _, JSON, _, _}} -> process_get_info_json(JSON);
		_ -> info_unavailable
	end.

%% @doc Return a list of parsed peer IPs for a remote server.
get_peers(Peer) ->
	try
		begin
			{ok, {{<<"200">>, _}, _, Body, _, _}} =
				ar_http:req(#{
					method => get,
					peer => Peer,
					path => "/peers",
					headers => p2p_headers()
				}),
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
		B when is_record(B, block) ->
			B;
		Error ->
			ar:info([
				"Failed to parse block response.",
				{peer, Peer},
				{error, Error}
			]),
			unavailable
	end;
handle_block_response(Peer, Peers, {ok, {{<<"200">>, _}, _, Body, _, _}}, full_block) ->
	case catch reconstruct_full_block(Peers, Body) of
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
handle_block_response(_, _, _, _) ->
	unavailable.

reconstruct_full_block(Peers, Body) when is_binary(Body) ->
	case ar_serialize:json_struct_to_block(Body) of
		B when is_record(B, block) ->
			reconstruct_full_block(Peers, B);
		B ->
			B
	end;
reconstruct_full_block(Peers, B) when is_record(B, block) ->
	WalletList =
		case B#block.wallet_list of
			WL when is_list(WL) -> WL;
			WL when is_binary(WL) ->
				case ar_storage:read_wallet_list(WL) of
					{ok, ReadWL} ->
						ReadWL;
					{error, _} ->
						get_wallet_list(Peers, B#block.indep_hash)
				end
		end,
	MempoolTXs = ar_node:get_pending_txs(whereis(http_entrypoint_node), [as_map]),
	case {get_txs(Peers, MempoolTXs, B), WalletList} of
		{{ok, TXs}, MaybeWalletList} when is_list(MaybeWalletList) ->
			B#block {
				txs = TXs,
				wallet_list = WalletList
			};
		_ ->
			unavailable
	end.

%% @doc Process the response of a /tx call.
handle_tx_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch ar_serialize:json_struct_to_tx(Body) of
		TX when is_record(TX, tx) -> TX;
		_ -> not_found
	end;
handle_tx_response({ok, {{<<"202">>, _}, _, _, _, _}}) -> pending;
handle_tx_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_tx_response({ok, {{<<"410">>, _}, _, _, _, _}}) -> gone;
handle_tx_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> not_found;
handle_tx_response(_Response) ->
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
