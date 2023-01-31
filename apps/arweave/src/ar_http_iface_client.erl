%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface_client).

-export([send_block_json/3, send_block_binary/3, send_block_binary/4, send_tx_json/3,
		send_tx_binary/3, send_block_announcement/2, get_block_shadow/2, get_block_shadow/3,
		get_block/3, get_tx/3, get_txs/3, get_tx_from_remote_peer/2, get_tx_data/2,
		get_wallet_list_chunk/2, get_wallet_list_chunk/3, get_wallet_list/2,
		add_peer/1, get_info/1, get_info/2, get_peers/1, get_time/2, get_height/1,
		get_block_index/3, get_sync_record/1, get_sync_record/3,
		get_chunk_json/3, get_chunk_binary/3, get_mempool/1, get_sync_buckets/1,
		get_recent_hash_list/1, get_recent_hash_list_diff/2, get_reward_history/3,
		push_nonce_limiter_update/2]).

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_pricing.hrl").
-include_lib("arweave/include/ar_config.hrl").
-include_lib("arweave/include/ar_data_sync.hrl").
-include_lib("arweave/include/ar_data_discovery.hrl").
-include_lib("arweave/include/ar_wallets.hrl").

%% @doc Send a JSON-encoded transaction to the given Peer.
send_tx_json(Peer, TXID, Bin) ->
	case is_testnet_peer(Peer) of
		false ->
			ok;
		true ->
			send_tx_json2(Peer, TXID, Bin)
	end.

send_tx_json2(Peer, TXID, Bin) ->
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/tx",
		headers => [{<<"arweave-tx-id">>, ar_util:encode(TXID)} | p2p_headers()],
		body => Bin,
		connect_timeout => 5000,
		timeout => 30 * 1000
	}).

%% @doc Send a binary-encoded transaction to the given Peer.
send_tx_binary(Peer, TXID, Bin) ->
	case is_testnet_peer(Peer) of
		false ->
			ok;
		true ->
			send_tx_binary2(Peer, TXID, Bin)
	end.

send_tx_binary2(Peer, TXID, Bin) ->
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/tx2",
		headers => [{<<"arweave-tx-id">>, ar_util:encode(TXID)} | p2p_headers()],
		body => Bin,
		connect_timeout => 5000,
		timeout => 30 * 1000
	}).

%% @doc Announce a block to Peer.
send_block_announcement(Peer, Announcement) ->
	case is_testnet_peer(Peer) of
		false ->
			ok;
		true ->
			send_block_announcement2(Peer, Announcement)
	end.

send_block_announcement2(Peer, Announcement) ->
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/block_announcement",
		headers => p2p_headers(),
		body => ar_serialize:block_announcement_to_binary(Announcement),
		timeout => 10 * 1000
	}).

%% @doc Send the given JSON-encoded block to the given peer.
send_block_json(Peer, H, Payload) ->
	case is_testnet_peer(Peer) of
		false ->
			ok;
		true ->
			send_block_json2(Peer, H, Payload)
	end.

-ifdef(DEBUG).
is_testnet_peer(_Peer) ->
	true.
-else.
is_testnet_peer(Peer) ->
	case ets:lookup(node_state, {is_testnet_peer, Peer}) of
		[{_, TrueOrFalse}] ->
			TrueOrFalse;
		[] ->
			case ar_http_iface_client:get_info(Peer, name) of
				<<"arweave.2.6.testnet">> ->
					ets:insert(node_state, {{is_testnet_peer, Peer}, true}),
					true;
				Network when is_binary(Network) ->
					ets:insert(node_state, {{is_testnet_peer, Peer}, false}),
					false;
				_ ->
					false
			end
	end.
-endif.

send_block_json2(Peer, H, Payload) ->
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/block",
		headers => [{<<"arweave-block-hash">>, ar_util:encode(H)} | p2p_headers()],
		body => Payload,
		connect_timeout => 5000,
		timeout => 20 * 1000
	}).

%% @doc Send the given binary-encoded block to the given peer.
send_block_binary(Peer, H, Payload) ->
	send_block_binary(Peer, H, Payload, undefined).

send_block_binary(Peer, H, Payload, RecallByte) ->
	case is_testnet_peer(Peer) of
		false ->
			ok;
		true ->
			send_block_binary2(Peer, H, Payload, RecallByte)
	end.

send_block_binary2(Peer, H, Payload, RecallByte) ->
	Headers = [{<<"arweave-block-hash">>, ar_util:encode(H)} | p2p_headers()],
	%% The way of informing the recipient about the recall byte used before the fork
	%% 2.6. Since the fork 2.6 blocks have a "recall_byte" field.
	Headers2 = case RecallByte of undefined -> Headers; _ ->
			[{<<"arweave-recall-byte">>, integer_to_binary(RecallByte)} | Headers] end,
	Reply = ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/block2",
		headers => Headers2,
		body => Payload,
		timeout => 20 * 1000
	}),
	case Reply of
		{ok, {{<<"200">>, _}, _, _, _, _}} ->
			Reply;
		{ok, {{<<"208">>, _}, _, _, _, _}} ->
			Reply;
		_ ->
			?LOG_WARNING(
				"event: failed_to_send_block, peer: ~ts, block: ~ts, reply: ~p",
				[ar_util:format_peer(Peer), binary_to_list(ar_util:encode(H)), Reply]
			),
			Reply
	end.

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

%% @doc Retrieve a block. We request the peer to include complete
%% transactions at the given positions (in the sorted transaction list).
get_block(Peer, H, TXIndices) ->
	case handle_block_response(Peer, binary,
			ar_http:req(#{
				method => get,
				peer => Peer,
				path => "/block2/hash/" ++ binary_to_list(ar_util:encode(H)),
				headers => p2p_headers(),
				connect_timeout => 500,
				timeout => 15 * 1000,
				body => ar_util:encode_list_indices(TXIndices),
				limit => ?MAX_BODY_SIZE
			})) of
		not_found ->
			not_found;
		{ok, B, Time, Size} ->
			{B, Time, Size}
	end.

%% @doc Retreive a block shadow by hash or height from one of the given peers.
get_block_shadow([], _ID) ->
	unavailable;
get_block_shadow(Peers, ID) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	case get_block_shadow(ID, Peer, binary) of
		not_found ->
			get_block_shadow(Peers -- [Peer], ID);
		{ok, B, Time, Size} ->
			{Peer, B, Time, Size}
	end.

%% @doc Retreive a block shadow by hash or height from the given peer.
get_block_shadow(ID, Peer, Encoding) ->
	handle_block_response(Peer, Encoding,
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => get_block_path(ID, Encoding),
			headers => p2p_headers(),
			connect_timeout => 500,
			timeout => 30 * 1000,
			limit => ?MAX_BODY_SIZE
		})).

%% @doc Generate an appropriate URL for a block by its identifier.
get_block_path({ID, _, _}, Encoding) ->
	get_block_path(ID, Encoding);
get_block_path(ID, Encoding) when is_binary(ID) ->
	case Encoding of
		binary ->
			"/block2/hash/" ++ binary_to_list(ar_util:encode(ID));
		json ->
			"/block/hash/" ++ binary_to_list(ar_util:encode(ID))
	end;
get_block_path(ID, Encoding) when is_integer(ID) ->
	case Encoding of
		binary ->
			"/block2/height/" ++ integer_to_list(ID);
		json ->
			"/block/height/" ++ integer_to_list(ID)
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

get_block_index(Peer, Start, End) ->
	get_block_index(Peer, Start, End, binary).

get_block_index(Peer, Start, End, Encoding) ->
	StartList = integer_to_list(Start),
	EndList = integer_to_list(End),
	Root = case Encoding of binary -> "/block_index2/"; json -> "/block_index/" end,
	case ar_http:req(#{
				method => get,
				peer => Peer,
				path => Root ++ StartList ++ "/" ++ EndList,
				timeout => 20000,
				connect_timeout => 5000,
				headers => p2p_headers()
			}) of
		{ok, {{<<"400">>, _}, _, <<"Request type not found.">>, _, _}}
				when Encoding == binary ->
			get_block_index(Peer, Start, End, json);
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case decode_block_index(Body, Encoding) of
				{ok, BI} ->
					{ok, BI};
				Error ->
					?LOG_WARNING([{event, failed_to_decode_block_index_range}, Error]),
					Error
			end;
		Error ->
			?LOG_WARNING([{event, failed_to_fetch_block_index_range},
					{error, io_lib:format("~p", [Error])}]),
			{error, Error}
	end.

decode_block_index(Bin, binary) ->
	ar_serialize:binary_to_block_index(Bin);
decode_block_index(Bin, json) ->
	case ar_serialize:json_decode(Bin) of
		{ok, Struct} ->
			case catch ar_serialize:json_struct_to_block_index(Struct) of
				{'EXIT', _} = Exc ->
					{error, Exc};
				BI ->
					{ok, BI}
			end;
		Error ->
			Error
	end.

get_sync_record(Peer) ->
	Headers = [{<<"Content-Type">>, <<"application/etf">>}],
	handle_sync_record_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/data_sync_record",
		timeout => 15 * 1000,
		connect_timeout => 2000,
		limit => ?MAX_ETF_SYNC_RECORD_SIZE,
		headers => Headers
	})).

get_sync_record(Peer, Start, Limit) ->
	Headers = [{<<"Content-Type">>, <<"application/etf">>}],
	handle_sync_record_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/data_sync_record/" ++ integer_to_list(Start) ++ "/" ++ integer_to_list(Limit),
		timeout => 10 * 1000,
		connect_timeout => 5000,
		limit => ?MAX_ETF_SYNC_RECORD_SIZE,
		headers => Headers
	}), Start, Limit).

get_chunk_json(Peer, Offset, RequestedPacking) ->
	get_chunk(Peer, Offset, RequestedPacking, json).

get_chunk_binary(Peer, Offset, RequestedPacking) ->
	get_chunk(Peer, Offset, RequestedPacking, binary).

get_chunk(Peer, Offset, RequestedPacking, Encoding) ->
	PackingBinary =
		case RequestedPacking of
			any ->
				<<"any">>;
			unpacked ->
				<<"unpacked">>;
			spora_2_5 ->
				<<"spora_2_5">>;
			{spora_2_6, Addr} ->
				iolist_to_binary([<<"spora_2_6_">>, ar_util:encode(Addr)])
		end,
	Headers = [{<<"x-packing">>, PackingBinary},
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
	handle_chunk_response(Encoding, ar_http:req(#{
		peer => Peer,
		method => get,
		path => get_chunk_path(Offset, Encoding),
		timeout => 20 * 1000,
		connect_timeout => 5000,
		limit => ?MAX_SERIALIZED_CHUNK_PROOF_SIZE,
		headers => p2p_headers() ++ Headers
	})).

get_chunk_path(Offset, json) ->
	"/chunk/" ++ integer_to_binary(Offset);
get_chunk_path(Offset, binary) ->
	"/chunk2/" ++ integer_to_binary(Offset).

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
		timeout => 10 * 1000,
		connect_timeout => 2000,
		limit => ?MAX_SYNC_BUCKETS_SIZE,
		headers => p2p_headers()
	})).

get_recent_hash_list(Peer) ->
	handle_get_recent_hash_list_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/recent_hash_list",
		timeout => 2 * 1000,
		connect_timeout => 1000,
		limit => 3400,
		headers => p2p_headers()
	})).

get_recent_hash_list_diff(Peer, HL) ->
	ReverseHL = lists:reverse(HL),
	handle_get_recent_hash_list_diff_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/recent_hash_list_diff",
		timeout => 10 * 1000,
		connect_timeout => 1000,
		%%        PrevH H    Len        TXID
		limit => (48 + (48 + 2 + 1000 * 32) * 49), % 1570498 bytes,
													% very pessimistic case.
		body => iolist_to_binary(ReverseHL),
		headers => p2p_headers()
	}), HL, Peer).

%% @doc Fetch the reward history from one of the given peers. The reward history
%% must contain ?REWARD_HISTORY_BLOCKS + ?STORE_BLOCKS_BEHIND_CURRENT elements or
%% one per every block since the fork 2.6 block, whatever is smaller. The reward history
%% hashes are validated against the given ExpectedRewardHistoryHashes. Return not_found
%% if we fail to fetch a reward history of the expected length from any of the peers.
get_reward_history([Peer | Peers], B, ExpectedRewardHistoryHashes) ->
	#block{ height = Height, indep_hash = H } = B,
	Fork_2_6 = ar_fork:height_2_6(),
	true = Height >= Fork_2_6,
	ExpectedLength = min(Height - Fork_2_6 + 1,
			?REWARD_HISTORY_BLOCKS + ?STORE_BLOCKS_BEHIND_CURRENT),
	true = length(ExpectedRewardHistoryHashes) == min(Height - Fork_2_6 + 1,
			?STORE_BLOCKS_BEHIND_CURRENT),
	SizeLimit = (33 * 2) * (?REWARD_HISTORY_BLOCKS + ?STORE_BLOCKS_BEHIND_CURRENT),
	case ar_http:req(#{
				peer => Peer,
				method => get,
				path => "/reward_history/" ++ binary_to_list(ar_util:encode(H)),
				timeout => 30000,
				limit => SizeLimit,
				headers => p2p_headers()
			}) of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case ar_serialize:binary_to_reward_history(Body) of
				{ok, RewardHistory} when length(RewardHistory) == ExpectedLength ->
					case validate_reward_history_hashes(RewardHistory,
							ExpectedRewardHistoryHashes) of
						true ->
							{ok, RewardHistory};
						false ->
							?LOG_WARNING([{event, received_invalid_reward_history},
									{peer, ar_util:format_peer(Peer)}]),
							get_reward_history(Peers, B, ExpectedRewardHistoryHashes)
					end;
				{ok, L} ->
					?LOG_WARNING([{event, received_reward_history_of_unexpected_length},
							{expected_length, ExpectedLength}, {received_length, length(L)},
							{peer, ar_util:format_peer(Peer)}]),
					get_reward_history(Peers, B, ExpectedRewardHistoryHashes);
				{error, _} ->
					?LOG_WARNING([{event, failed_to_parse_reward_history},
							{peer, ar_util:format_peer(Peer)}]),
					get_reward_history(Peers, B, ExpectedRewardHistoryHashes)
			end;
		Reply ->
			?LOG_WARNING([{event, failed_to_fetch_reward_history},
					{peer, ar_util:format_peer(Peer)},
					{reply, io_lib:format("~p", [Reply])}]),
			get_reward_history(Peers, B, ExpectedRewardHistoryHashes)
	end;
get_reward_history([], _B, _RewardHistoryHashes) ->
	not_found.

push_nonce_limiter_update(Peer, Update) ->
	Body = ar_serialize:nonce_limiter_update_to_binary(Update),
	case ar_http:req(#{
				peer => Peer,
				method => post,
				path => "/vdf",
				body => Body,
				timeout => 2000,
				limit => 100,
				headers => p2p_headers()
			}) of
		{ok, {{<<"200">>, _}, _, <<>>, _, _}} ->
			ok;
		{ok, {{<<"202">>, _}, _, ResponseBody, _, _}} ->
			ar_serialize:binary_to_nonce_limiter_update_response(ResponseBody);
		{ok, {{Status, _}, _, ResponseBody, _, _}} ->
			{error, {Status, ResponseBody}};
		Reply ->
			Reply
	end.

validate_reward_history_hashes(_RewardHistory, []) ->
	true;
validate_reward_history_hashes(RewardHistory, [H | ExpectedRewardHistoryHashes]) ->
	case ar_block:validate_reward_history_hash(H, RewardHistory) of
		true ->
			validate_reward_history_hashes(tl(RewardHistory), ExpectedRewardHistoryHashes);
		false ->
			false
	end.

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

handle_chunk_response(Encoding, {ok, {{<<"200">>, _}, _, Body, Start, End}}) ->
	DecodeFun =
		case Encoding of
			json ->
				fun(Bin) ->
					ar_serialize:json_map_to_chunk_proof(jiffy:decode(Bin, [return_maps]))
				end;
			binary ->
				fun(Bin) ->
					case ar_serialize:binary_to_poa(Bin) of
						{ok, Reply} ->
							Reply;
						{error, Reason} ->
							{error, Reason}
					end
				end
		end,
	case catch DecodeFun(Body) of
		{'EXIT', Reason} ->
			{error, Reason};
		{error, Reason} ->
			{error, Reason};
		Proof ->
			case maps:get(chunk, Proof) of
				<<>> ->
					{error, empty_chunk};
				Chunk when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
					{error, chunk_bigger_than_256kib};
				_ ->
					{ok, Proof, End - Start, byte_size(term_to_binary(Proof))}
			end
	end;
handle_chunk_response(_Encoding, Response) ->
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

handle_get_recent_hash_list_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case ar_serialize:json_decode(Body) of
		{ok, HL} when is_list(HL) ->
			decode_hash_list(HL);
		{ok, _} ->
			{error, invalid_hash_list};
		Error ->
			Error
	end;
handle_get_recent_hash_list_response({ok, {{<<"400">>, _}, _,
		<<"Request type not found.">>, _, _}}) ->
	{error, request_type_not_found};
handle_get_recent_hash_list_response(Response) ->
	{error, Response}.

handle_get_recent_hash_list_diff_response({ok, {{<<"200">>, _}, _, Body, _, _}}, HL, Peer) ->
	case parse_recent_hash_list_diff(Body, HL) of
		{error, invalid_input} ->
			ar_events:send(peer, {bad_response, {Peer, recent_hash_list_diff, invalid_input}}),
			{error, invalid_input};
		{error, unknown_base} ->
			ar_events:send(peer, {bad_response, {Peer, recent_hash_list_diff, unknown_base}}),
			{error, unknown_base};
		{ok, Reply} ->
			{ok, Reply}
	end;
handle_get_recent_hash_list_diff_response({ok, {{<<"404">>, _}, _,
		_, _, _}}, _HL, _Peer) ->
	{error, not_found};
handle_get_recent_hash_list_diff_response({ok, {{<<"400">>, _}, _,
		<<"Request type not found.">>, _, _}}, _HL, _Peer) ->
	{error, request_type_not_found};
handle_get_recent_hash_list_diff_response(Response, _HL, _Peer) ->
	{error, Response}.

decode_hash_list(HL) ->
	decode_hash_list(HL, []).

decode_hash_list([H | HL], DecodedHL) ->
	case ar_util:safe_decode(H) of
		{ok, DecodedH} ->
			decode_hash_list(HL, [DecodedH | DecodedHL]);
		Error ->
			Error
	end;
decode_hash_list([], DecodedHL) ->
	{ok, lists:reverse(DecodedHL)}.

parse_recent_hash_list_diff(<< PrevH:48/binary, Rest/binary >>, HL) ->
	case lists:member(PrevH, HL) of
		true ->
			parse_recent_hash_list_diff(Rest);
		false ->
			{error, unknown_base}
	end;
parse_recent_hash_list_diff(_Input, _HL) ->
	{error, invalid_input}.

parse_recent_hash_list_diff(<<>>) ->
	{ok, in_sync};
parse_recent_hash_list_diff(<< H:48/binary, Len:16, TXIDs:(32 * Len)/binary, Rest/binary >>)
		when Len =< ?BLOCK_TX_COUNT_LIMIT ->
	case ar_block_cache:get(block_cache, H) of
		not_found ->
			case count_blocks_on_top(Rest) of
				{ok, N} ->
					{ok, {H, parse_txids(TXIDs), N}};
				Error ->
					Error
			end;
		_ ->
			parse_recent_hash_list_diff(Rest)
	end;
parse_recent_hash_list_diff(_Input) ->
	{error, invalid_input}.

count_blocks_on_top(Bin) ->
	count_blocks_on_top(Bin, 0).

count_blocks_on_top(<<>>, N) ->
	{ok, N};
count_blocks_on_top(<< _H:48/binary, Len:16, _TXIDs:(32 * Len)/binary, Rest/binary >>, N) ->
	count_blocks_on_top(Rest, N + 1);
count_blocks_on_top(_Bin, _N) ->
	{error, invalid_input}.

parse_txids(<< TXID:32/binary, Rest/binary >>) ->
	[TXID | parse_txids(Rest)];
parse_txids(<<>>) ->
	[].

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
get_tx(_Peers, #tx{} = TX, _MempoolTXs) ->
	TX;
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
	Release = ar_peers:get_peer_release(Peer),
	Encoding = case Release >= 52 of true -> binary; _ -> json end,
	case handle_tx_response(Peer, Encoding,
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => get_tx_path(TXID, Encoding),
			headers => p2p_headers(),
			connect_timeout => 1000,
			timeout => 30 * 1000,
			limit => ?MAX_BODY_SIZE
		})
	) of
		not_found ->
			get_tx_from_remote_peer(Peers -- [Peer], TXID);
		{ok, #tx{} = TX, Time, Size} ->
			case ar_tx:verify_tx_id(TXID, TX) of
				false ->
					?LOG_WARNING([
						{event, peer_served_invalid_tx},
						{peer, ar_util:format_peer(Peer)},
						{tx, ar_util:encode(TXID)}
					]),
					ar_events:send(peer, {bad_response, {Peer, tx, invalid}}),
					get_tx_from_remote_peer(Peers -- [Peer], TXID);
				true ->
					ar_events:send(peer, {served_tx, Peer, Time, Size}),
					TX
			end
	end.

get_tx_path(TXID, json) ->
	"/unconfirmed_tx/" ++ binary_to_list(ar_util:encode(TXID));
get_tx_path(TXID, binary) ->
	"/unconfirmed_tx2/" ++ binary_to_list(ar_util:encode(TXID)).

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
	case ar_http:req(#{method => get, peer => Peer, path => "/time",
			headers => p2p_headers(), timeout => Timeout + 100}) of
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
handle_block_response(_Peer, _Encoding, {ok, {{<<"400">>, _}, _, _, _, _}}) ->
	not_found;
handle_block_response(_Peer, _Encoding, {ok, {{<<"404">>, _}, _, _, _, _}}) ->
	not_found;
handle_block_response(Peer, Encoding, {ok, {{<<"200">>, _}, _, Body, Start, End}}) ->
	DecodeFun = case Encoding of json ->
			fun(Input) ->
				ar_serialize:json_struct_to_block(ar_serialize:dejsonify(Input))
			end; binary -> fun ar_serialize:binary_to_block/1 end,
	case catch DecodeFun(Body) of
		{'EXIT', Reason} ->
			?LOG_INFO(
				"event: failed_to_parse_block_response, peer: ~s, reason: ~p",
				[ar_util:format_peer(Peer), Reason]),
			ar_events:send(peer, {bad_response, {Peer, block, Reason}}),
			not_found;
		{ok, B} ->
			{ok, B, End - Start, byte_size(term_to_binary(B))};
		B when is_record(B, block) ->
			{ok, B, End - Start, byte_size(term_to_binary(B))};
		Error ->
			?LOG_INFO(
				"event: failed_to_parse_block_response, peer: ~s, error: ~p",
				[ar_util:format_peer(Peer), Error]),
			ar_events:send(peer, {bad_response, {Peer, block, Error}}),
			not_found
	end;
handle_block_response(Peer, _Encoding, Response) ->
	ar_events:send(peer, {bad_response, {Peer, block, Response}}),
	not_found.

%% @doc Process the response of a GET /unconfirmed_tx call.
handle_tx_response(_Peer, _Encoding, {ok, {{<<"404">>, _}, _, _, _, _}}) ->
	not_found;
handle_tx_response(_Peer, _Encoding, {ok, {{<<"400">>, _}, _, _, _, _}}) ->
	not_found;
handle_tx_response(Peer, Encoding, {ok, {{<<"200">>, _}, _, Body, Start, End}}) ->
	DecodeFun = case Encoding of json -> fun ar_serialize:json_struct_to_tx/1;
			binary -> fun ar_serialize:binary_to_tx/1 end,
	case catch DecodeFun(Body) of
		{ok, TX} ->
			Size = byte_size(term_to_binary(TX)),
			case TX#tx.format == 1 of
				true ->
					{ok, TX, End - Start, Size};
				_ ->
					DataSize = byte_size(TX#tx.data),
					{ok, TX#tx{ data = <<>> }, End - Start, Size - DataSize}
			end;
		TX when is_record(TX, tx) ->
			Size = byte_size(term_to_binary(TX)),
			case TX#tx.format == 1 of
				true ->
					{ok, TX, End - Start, Size};
				_ ->
					DataSize = byte_size(TX#tx.data),
					{ok, TX#tx{ data = <<>> }, End - Start, Size - DataSize}
			end;
		{'EXIT', Reason} ->
			ar_events:send(peer, {bad_response, {Peer, tx, Reason}}),
			not_found;
		Reply ->
			ar_events:send(peer, {bad_response, {Peer, tx, Reply}}),
			not_found
	end;
handle_tx_response(Peer, _Encoding, Response) ->
	ar_events:send(peer, {bad_response, {Peer, tx, Response}}),
	not_found.

p2p_headers() ->
	{ok, Config} = application:get_env(arweave, config),
	[{<<"X-P2p-Port">>, integer_to_binary(Config#config.port)},
			{<<"X-Release">>, integer_to_binary(?RELEASE_NUMBER)}].

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
