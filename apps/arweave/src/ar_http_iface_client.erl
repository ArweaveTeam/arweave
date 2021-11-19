%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface_client).

-export([send_new_block/3, send_new_tx/2, get_block/2, get_block_shadow/2, get_tx/3, get_txs/3,
		get_tx_from_remote_peer/2, get_tx_data/2, get_wallet_list_chunk/2,
		get_wallet_list_chunk/3, get_wallet_list/2, add_peer/1, get_info/1, get_info/2,
		get_peers/1, get_time/2, get_height/1, get_block_index/1, get_block_index/2,
		get_sync_record/1, get_sync_record/3, get_chunk/3, get_mempool/1, get_sync_buckets/1]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").
-include_lib("arweave/include/ar_wallets.hrl").

%% @doc Send a new transaction to an Arweave HTTP node.
send_new_tx(Peer, TX) ->
	TXID = TX#tx.id,
	TXSize = byte_size(TX#tx.data),
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/tx",
		headers => p2p_headers() ++ [{<<"arweave-tx-id">>, ar_util:encode(TXID)}],
		body => ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
		connect_timeout => 500,
		timeout => max(3, min(60, TXSize * 8 div ?TX_PROPAGATION_BITS_PER_SECOND)) * 1000
	}).

%% @doc Distribute a newly found block to remote nodes.
send_new_block(Peer, #block{ height = Height } = NewB, BDS) ->
	{BlockProps} = ar_serialize:block_to_json_struct(NewB),
	BlockShadowProps =
		case Height >= ar_fork:height_2_4() of
			true ->
				BlockProps;
			false ->
				case NewB#block.hash_list of
					unset ->
						BlockProps;
					_ ->
						ShortHashList =
							lists:map(
								fun ar_util:encode/1,
								lists:sublist(NewB#block.hash_list, ?STORE_BLOCKS_BEHIND_CURRENT)
							),
						[{<<"hash_list">>, ShortHashList} | BlockProps]
				end
		end,
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
		headers =>
			p2p_headers() ++ [{<<"arweave-block-hash">>, ar_util:encode(NewB#block.indep_hash)}],
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
		timeout => 3 * 1000,
		connect_timeout => 500
	}).

%% @doc Retreive a block by hash from disk or a remote peer.
get_block(Peers, H) when is_list(Peers) ->
	case ar_storage:read_block(H) of
		unavailable ->
			get_block_from_remote_peers(Peers, H);
		B ->
			case catch reconstruct_full_block(Peers, B) of
				{'EXIT', Reason} ->
					?LOG_INFO([
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
			connect_timeout => 2000,
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

%% @doc Get a bunch of wallets by the given root hash from external peers.
get_wallet_list_chunk(Peers, H) ->
	get_wallet_list_chunk(Peers, H, start).

get_wallet_list_chunk([], _H, _Cursor) ->
	{error, not_found};
get_wallet_list_chunk([Peer | Peers], H, Cursor) ->
	BasePath = "/wallet_list/" ++ binary_to_list(ar_util:encode(H)),
	Path =
		case Cursor of
			start ->
				BasePath;
			_ ->
				BasePath ++ "/" ++ binary_to_list(ar_util:encode(Cursor))
		end,
	Response =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => Path,
			headers => p2p_headers(),
			limit => ?MAX_SERIALIZED_WALLET_LIST_CHUNK_SIZE,
			timeout => 10 * 1000,
			connect_timeout => 1000
		}),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case ar_serialize:etf_to_wallet_chunk_response(Body) of
				{ok, #{ next_cursor := NextCursor, wallets := Wallets }} ->
					{ok, {NextCursor, Wallets}};
				DeserializationResult ->
					?LOG_ERROR([
						{event, got_unexpected_wallet_list_chunk_deserialization_result},
						{deserialization_result, DeserializationResult}
					]),
					get_wallet_list_chunk(Peers, H, Cursor)
			end;
		Response ->
			get_wallet_list_chunk(Peers, H, Cursor)
	end.

%% @doc Get a wallet list by the given block hash from external peers.
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
			{ok, ar_serialize:json_struct_to_wallet_list(Body)};
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found;
		_ -> unavailable
	end.

%% @doc Get a block hash list (by its hash) from the external peer.
get_block_index([]) ->
	unavailable;
get_block_index(Peers) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	ar:console("Downloading block index from ~s.~n", [ar_util:format_peer(Peer)]),
	Reply =
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/hash_list",
			timeout => 300 * 1000,
			headers => p2p_headers()
		}),
	case Reply of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case catch ar_serialize:json_struct_to_block_index(ar_serialize:dejsonify(Body)) of
				{'EXIT', Reason} ->
					ar:console("Failed to parse block index.~n", []),
					?LOG_WARNING([
						{event, failed_to_parse_block_index_from_peer},
						{peer, ar_util:format_peer(Peer)},
						{reason, io_lib:format("~p", [Reason])}
					]),
					get_block_index(Peers -- [Peer]);
				BI ->
					ar:console("Downloaded block index successfully.~n", []),
					BI
			end;
		_ ->
			ar:console("Failed to download block index.~n", []),
			?LOG_WARNING([
				{event, failed_to_fetch_block_index_from_peer},
				{peer, ar_util:format_peer(Peer)},
				{reply, io_lib:format("~p", [Reply])}
			]),
			get_block_index(Peers -- [Peer])
	end.

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
	Headers = [{<<"Content-Type">>, <<"application/etf">>}],
	handle_sync_record_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/data_sync_record",
		timeout => 15 * 1000,
		connect_timeout => 500,
		limit => ?MAX_ETF_SYNC_RECORD_SIZE,
		headers => Headers
	})).

get_sync_record(Peer, Start, Limit) ->
	Headers = [{<<"Content-Type">>, <<"application/etf">>}],
	handle_sync_record_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/data_sync_record/" ++ integer_to_list(Start) ++ "/" ++ integer_to_list(Limit),
		timeout => 5 * 1000,
		connect_timeout => 500,
		limit => ?MAX_ETF_SYNC_RECORD_SIZE,
		headers => Headers
	}), Start, Limit).

get_chunk(Peer, Offset, RequestedPacking) ->
	Headers = [{<<"x-packing">>, atom_to_binary(RequestedPacking)},
			%% The nodes not upgraded to the 2.5 version would ignore this header.
			%% It is fine because all offsets before 2.5 are not bucket-based.
			%% Client libraries do not send this header - normally they do not need
			%% bucket-based offsets. Bucket-based offsets are required in mining
			%% after the fork 2.5 and it is convenient to use them for syncing,
			%% thus setting the header here. A bucket-based offset corresponds to
			%% the chunk that ends in the same 256 KiB bucket starting from the
			%% 2.5 block. In most cases a bucket-based offset would correspond to
			%% the same chunk as the normal offset except for the offsets of the
			%% last and second last chunks of the transactions when these chunks
			%% are smaller than 256 KiB.
			{<<"x-bucket-based-offset">>, <<"true">>}],
	handle_chunk_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/chunk/" ++ integer_to_binary(Offset),
		timeout => 30 * 1000,
		connect_timeout => 2000,
		limit => ?MAX_SERIALIZED_CHUNK_PROOF_SIZE,
		headers => p2p_headers() ++ Headers
	})).

get_mempool(Peer) ->
	handle_mempool_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/tx/pending",
		timeout => 5 * 1000,
		connect_timeout => 500,
		%% Sufficient for a JSON-encoded list of the transaction identifiers
		%% from a mempool with 250 MiB worth of transaction headers with no data.
		limit => 3000000,
		headers => p2p_headers()
	})).

get_sync_buckets(Peer) ->
	handle_get_sync_buckets_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/sync_buckets",
		timeout => 5 * 1000,
		connect_timeout => 500,
		limit => ?MAX_SYNC_BUCKETS_SIZE,
		headers => p2p_headers()
	})).

handle_sync_record_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_intervals:safe_from_etf(Body);
handle_sync_record_response(Reply) ->
	{error, Reply}.

handle_sync_record_response({ok, {{<<"200">>, _}, _, Body, _, _}}, Start, Limit) ->
	case ar_intervals:safe_from_etf(Body) of
		{ok, Intervals} ->
			case ar_intervals:count(Intervals) > Limit of
				true ->
					{error, too_many_intervals};
				false ->
					case ar_intervals:is_empty(Intervals) of
						true ->
							{ok, Intervals};
						false ->
							case element(1, ar_intervals:smallest(Intervals)) < Start of
								true ->
									{error, intervals_do_not_match_cursor};
								false ->
									{ok, Intervals}
							end
					end
			end;
		Error ->
			Error
	end;
handle_sync_record_response(Reply, _, _) ->
	{error, Reply}.

handle_chunk_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch ar_serialize:json_map_to_chunk_proof(jiffy:decode(Body, [return_maps])) of
		{'EXIT', Reason} ->
			{error, Reason};
		Proof ->
			case maps:get(chunk, Proof) of
				<<>> ->
					{error, empty_chunk};
				Chunk when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
					{error, chunk_bigger_than_256kib};
				_ ->
					{ok, Proof}
			end
	end;
handle_chunk_response(Response) ->
	{error, Response}.

handle_mempool_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch jiffy:decode(Body) of
		{'EXIT', Error} ->
			?LOG_WARNING([{event, failed_to_parse_peer_mempool},
				{error, io_lib:format("~p", [Error])}]),
			{error, invalid_json};
		L when is_list(L) ->
			lists:foldr(
				fun	(_, {error, Reason}) ->
						{error, Reason};
					(EncodedTXID, {ok, Acc}) ->
						case ar_util:safe_decode(EncodedTXID) of
							{ok, TXID} when byte_size(TXID) /= 32 ->
								?LOG_WARNING([{event, failed_to_parse_peer_mempool},
									{reason, invalid_txid},
									{txid, io_lib:format("~p", [EncodedTXID])}]),
								{error, invalid_txid};
							{ok, TXID} ->
								{ok, [TXID | Acc]};
							{error, invalid} ->
								?LOG_WARNING([{event, failed_to_parse_peer_mempool},
									{reason, invalid_txid},
									{txid, io_lib:format("~p", [EncodedTXID])}]),
								{error, invalid_txid}
						end
				end,
				{ok, []},
				L
			);
		NotList ->
			?LOG_WARNING([{event, failed_to_parse_peer_mempool}, {reason, invalid_format},
				{reply, io_lib:format("~p", [NotList])}]),
			{error, invalid_format}
	end;
handle_mempool_response(Response) ->
	{error, Response}.

handle_get_sync_buckets_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case ar_sync_buckets:deserialize(Body) of
		{ok, Buckets} ->
			{ok, Buckets};
		{'EXIT', Reason} ->
			{error, Reason};
		_ ->
			{error, invalid_response_type}
	end;
handle_get_sync_buckets_response({ok, {{<<"400">>, _}, _,
		<<"Request type not found.">>, _, _}}) ->
	{error, request_type_not_found};
handle_get_sync_buckets_response(Response) ->
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
			?LOG_ERROR([{event, downloaded_txs_count_exceeds_limit}]),
			{error, txs_count_exceeds_limit};
		TXIDs ->
			get_txs(B#block.height, Peers, MempoolTXs, TXIDs, [], 0)
	end.

get_txs(_Height, _Peers, _MempoolTXs, [], TXs, _TotalSize) ->
	{ok, lists:reverse(TXs)};
get_txs(Height, Peers, MempoolTXs, [TXID | Rest], TXs, TotalSize) ->
	Fork_2_0 = ar_fork:height_2_0(),
	case get_tx(Peers, TXID, MempoolTXs) of
		#tx{ format = 2 } = TX ->
			get_txs(Height, Peers, MempoolTXs, Rest, [TX | TXs], TotalSize);
		#tx{} = TX when Height < Fork_2_0 ->
			get_txs(Height, Peers, MempoolTXs, Rest, [TX | TXs], TotalSize);
		#tx{ format = 1 } = TX ->
			case TotalSize + TX#tx.data_size of
				NewTotalSize when NewTotalSize > ?BLOCK_TX_DATA_SIZE_LIMIT ->
					?LOG_ERROR([{event, downloaded_txs_exceed_block_size_limit}]),
					{error, txs_exceed_block_size_limit};
				NewTotalSize ->
					get_txs(Height, Peers, MempoolTXs, Rest, [TX | TXs], NewTotalSize)
			end;
		_ ->
			{error, tx_not_found}
	end.

%% @doc Retreive a tx by ID from the memory pool, disk, or a remote peer.
get_tx(Peers, TXID, MempoolTXs) ->
	case maps:get(TXID, MempoolTXs, not_in_mempool) of
		not_in_mempool ->
			get_tx_from_disk_or_peer(Peers, TXID);
		_Status ->
			case ets:lookup(node_state, {tx, TXID}) of
				[{_, TX}] ->
					TX;
				_ ->
					get_tx_from_disk_or_peer(Peers, TXID)
			end
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
			path => "/unconfirmed_tx/" ++ binary_to_list(ar_util:encode(TXID)),
			headers => p2p_headers(),
			connect_timeout => 1000,
			timeout => 30 * 1000,
			limit => ?MAX_BODY_SIZE
		})
	) of
		not_found ->
			get_tx_from_remote_peer(Peers -- [Peer], TXID);
		{ok, #tx{} = TX} ->
			case ar_tx:verify_tx_id(TXID, TX) of
				false ->
					?LOG_WARNING([
						{event, peer_served_invalid_tx},
						{peer, ar_util:format_peer(Peer)},
						{tx, ar_util:encode(TXID)}
					]),
					get_tx_from_remote_peer(Peers -- [Peer], TXID);
				true ->
					TX
			end
	end.

%% @doc Retreive only the data associated with a transaction.
%% The function must only be used when it is known that the transaction
%% has data.
%% @end
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

%% @doc Retreive information from a peer. Optionally, filter the resulting
%% keyval list for required information.
%% @end
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
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 2 * 1000
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
					headers => p2p_headers(),
					connect_timeout => 500,
					timeout => 2 * 1000
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
			?LOG_INFO(
				"event: failed_to_parse_block_response, peer: ~s, reason: ~p",
				[ar_util:format_peer(Peer), Reason]
			),
			unavailable;
		B when is_record(B, block) ->
			B;
		Error ->
			?LOG_INFO(
				"event: failed_to_parse_block_response, peer: ~s, error: ~p",
				[ar_util:format_peer(Peer), Error]
			),
			unavailable
	end;
handle_block_response(Peer, Peers, {ok, {{<<"200">>, _}, _, Body, _, _}}, full_block) ->
	case catch reconstruct_full_block(Peers, Body) of
		{'EXIT', Reason} ->
			?LOG_INFO(
				"event: failed_to_parse_block_response, peer: ~s, reason: ~p",
				[ar_util:format_peer(Peer), Reason]
			),
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
	MempoolTXs = ar_node:get_pending_txs([as_map]),
	case get_txs(Peers, MempoolTXs, B) of
		{ok, TXs} ->
			B#block {
				txs = TXs
			};
		_ ->
			unavailable
	end.

%% @doc Process the response of a GET /unconfirmed_tx call.
handle_tx_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch ar_serialize:json_struct_to_tx(Body) of
		TX when is_record(TX, tx) ->
			case TX#tx.format == 1 of true -> {ok, TX}; _ -> {ok, TX#tx{ data = <<>> }} end;
		_ ->
			not_found
	end;
handle_tx_response(_Response) ->
	not_found.

p2p_headers() ->
	{ok, Config} = application:get_env(arweave, config),
	[{<<"X-P2p-Port">>, integer_to_binary(Config#config.port)}].

%% @doc Return values for keys - or error if any key is missing.
safe_get_vals(Keys, Props) ->
	case lists:foldl(fun
			(_, error) -> error;
			(Key, Acc) ->
				case lists:keyfind(Key, 1, Props) of
					{_, Val} -> [Val | Acc];
					_		 -> error
				end
			end, [], Keys) of
		error -> error;
		Vals  -> {ok, lists:reverse(Vals)}
	end.
