%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface_client).

-export([send_tx_json/3, send_tx_json/4, send_tx_binary/3, send_tx_binary/4]).
-export([send_block_json/3, send_block_binary/3, send_block_binary/4,
	send_block_announcement/2,
	get_block/3, get_tx/2, get_txs/2, get_tx_from_remote_peers/3,
	get_tx_data/2, get_wallet_list_chunk/2, get_wallet_list_chunk/3,
	get_wallet_list/2, add_peer/1, get_info/1, get_info/2, get_peers/1,
	get_time/2, get_height/1, get_block_index/3,
	get_sync_record/1, get_sync_record/3, get_sync_record/4, get_footprints/3,
	get_chunk_binary/3, get_mempool/1,
	get_sync_buckets/1, get_footprint_buckets/1, get_recent_hash_list/1,
	get_recent_hash_list_diff/2, get_reward_history/3,
	get_block_time_history/3, push_nonce_limiter_update/3,
	get_vdf_update/1, get_vdf_session/1, get_previous_vdf_session/1,
	get_cm_partition_table/1, cm_h1_send/2, cm_h2_send/2,
	cm_publish_send/2, get_jobs/2, post_partial_solution/2,
	get_pool_cm_jobs/2, post_pool_cm_jobs/2,
	post_cm_partition_table_to_pool/2, get_data_roots/2]).
-export([get_block_shadow/2, get_block_shadow/3, get_block_shadow/4]).
-export([log_failed_request/2]).

%% -- Testing exports
-export([get_tx_from_remote_peer/3]).
%% -- End of testing exports

-include("ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").
-include("ar_consensus.hrl").
-include("ar_data_sync.hrl").
-include("ar_sync_buckets.hrl").
-include("ar_data_discovery.hrl").
-include("ar_mining.hrl").
-include("ar_wallets.hrl").
-include("ar_pool.hrl").

%%--------------------------------------------------------------------
%% @doc Send a JSON-encoded transaction to the given Peer with default
%% parameters.
%%
%% == Examples ==
%%
%% ```
%% Host = {127,0,0,1},
%% Port = 1984,
%% Peer = {Host, Port},
%% TXID = <<0:256>>,
%% Bin = ar_serialize:tx_to_binary(#tx{}),
%% send_tx_json(Peer, TXID, Bin).
%% '''
%%
%% @see send_tx_json/4
%% @end
%%--------------------------------------------------------------------
send_tx_json(Peer, TXID, Bin) ->
	send_tx_json(Peer, TXID, Bin, #{}).

%%--------------------------------------------------------------------
%% @doc Send a JSON-encoded transaction to the given Peer.
%%
%% == Examples ==
%%
%% ```
%% Host = {127,0,0,1},
%% Port = 1984,
%% Peer = {Host, Port},
%% TXID = <<0:256>>,
%% Bin = ar_serialize:tx_to_binary(#tx{}),
%% Opts = #{ connect_timeout => 5
%%         , timeout => 30
%%         },
%% send_tx_json(Peer, TXID, Bin, Opts).
%% '''
%%
%% @end
%%--------------------------------------------------------------------
send_tx_json(Peer, TXID, Bin, Opts) ->
	ConnectTimeout = maps:get(connect_timeout, Opts, 5),
	Timeout = maps:get(timeout, Opts, 30),
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/tx",
		headers => add_header(<<"arweave-tx-id">>, ar_util:encode(TXID), p2p_headers()),
		body => Bin,
		connect_timeout => ConnectTimeout * 1000,
		timeout => Timeout * 1000
	}).

%%--------------------------------------------------------------------
%% @doc Send a binary-encoded transaction to the given Peer with
%% default parameters.
%% @see send_tx_binary/4
%% @end
%%--------------------------------------------------------------------
send_tx_binary(Peer, TXID, Bin) ->
	send_tx_binary(Peer, TXID, Bin, #{}).

%%--------------------------------------------------------------------
%% @doc Send a binary-encoded transaction to the given Peer.
%% @end
%%--------------------------------------------------------------------
send_tx_binary(Peer, TXID, Bin, Opts) ->
	ConnectTimeout = maps:get(connect_timeout, Opts, 5),
	Timeout = maps:get(timeout, Opts, 30),
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/tx2",
		headers => add_header(<<"arweave-tx-id">>, ar_util:encode(TXID), p2p_headers()),
		body => Bin,
		connect_timeout => ConnectTimeout * 1000,
		timeout => Timeout * 1000
	}).

%% @doc Announce a block to Peer.
send_block_announcement(Peer, Announcement) ->
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
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/block",
		headers => add_header(<<"arweave-block-hash">>, ar_util:encode(H), p2p_headers()),
		body => Payload,
		connect_timeout => 5000,
		timeout => 120 * 1000
	}).

%% @doc Send the given binary-encoded block to the given peer.
send_block_binary(Peer, H, Payload) ->
	send_block_binary(Peer, H, Payload, undefined).

send_block_binary(Peer, H, Payload, RecallByte) ->
	Headers = add_header(<<"arweave-block-hash">>, ar_util:encode(H), p2p_headers()),
	%% The way of informing the recipient about the recall byte used before the fork
	%% 2.6. Since the fork 2.6 blocks have a "recall_byte" field.
	Headers2 = case RecallByte of undefined -> Headers; _ ->
			add_header(<<"arweave-recall-byte">>, integer_to_binary(RecallByte), Headers) end,
	ar_http:req(#{
		method => post,
		peer => Peer,
		path => "/block2",
		headers => Headers2,
		body => Payload,
		timeout => 20 * 1000
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

%% @doc Retrieve a block. We request the peer to include complete
%% transactions at the given positions (in the sorted transaction list).
get_block(Peer, H, TXIndices) ->
	case handle_block_response(Peer, binary,
			ar_http:req(#{
				method => get,
				peer => Peer,
				path => "/block2/hash/" ++ binary_to_list(ar_util:encode(H)),
				headers => p2p_headers(),
				connect_timeout => 1000,
				timeout => 15 * 1000,
				body => ar_util:encode_list_indices(TXIndices),
				limit => ?MAX_BODY_SIZE
			})) of
		not_found ->
			not_found;
		{ok, B, Time, Size} ->
			{B, Time, Size}
	end.

%%--------------------------------------------------------------------
%% @doc get a block shadow using default parameter.
%% @end
%%--------------------------------------------------------------------
get_block_shadow(Peers, ID) ->
	get_block_shadow(Peers, ID, #{}).

%%--------------------------------------------------------------------
%% @doc Retrieve a block shadow by hash or height from one of the given
%%      peers. Some options can be modified like `rand_min',
%%      `connect_timeout' and `timeout'.
%% @see get_block_shadow/4
%% @end
%%--------------------------------------------------------------------
get_block_shadow([], _ID, _Opts) ->
	unavailable;
get_block_shadow(Peers, ID, Opts) ->
	RandMin = maps:get(rand_min, Opts, 5),
	Random = rand:uniform(min(RandMin, length(Peers))),
	Peer = lists:nth(Random, Peers),
	case get_block_shadow(ID, Peer, binary, Opts) of
		not_found ->
			get_block_shadow(Peers -- [Peer], ID, Opts);
		{ok, B, Time, Size} ->
			{Peer, B, Time, Size}
	end.

%%--------------------------------------------------------------------
%% @doc Retrieve a block shadow by hash or height from the given peer.
%% @end
%%--------------------------------------------------------------------
get_block_shadow(ID, Peer, Encoding, _Opts) ->
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
		timeout => 30 * 1000,
		connect_timeout => 2000,
		limit => ?MAX_ETF_SYNC_RECORD_SIZE,
		headers => Headers
	})).

get_sync_record(Peer, Start, Limit) ->
	Headers = [{<<"Content-Type">>, <<"application/etf">>}],
	handle_sync_record_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/data_sync_record/" ++ integer_to_list(Start) ++ "/"
				++ integer_to_list(Limit),
		timeout => 30 * 1000,
		connect_timeout => 5000,
		limit => ?MAX_ETF_SYNC_RECORD_SIZE,
		headers => Headers
	}), Start, Limit).

get_sync_record(Peer, Start, End, Limit) ->
	Headers = [{<<"Content-Type">>, <<"application/etf">>}],
	handle_sync_record_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/data_sync_record/" ++ integer_to_list(Start) ++ "/"
				++ integer_to_list(End) ++ "/" ++ integer_to_list(Limit),
		timeout => 30 * 1000,
		connect_timeout => 5000,
		limit => ?MAX_ETF_SYNC_RECORD_SIZE,
		headers => Headers
	}), Start, Limit).

get_footprints(Peer, Partition, Footprint) ->
	handle_footprints_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/footprints/" ++ integer_to_list(Partition) ++ "/" ++ integer_to_list(Footprint),
		timeout => 10_000,
		connect_timeout => 5_000,
		limit => ?MAX_FOOTPRINT_PAYLOAD_SIZE,
		headers => p2p_headers()
	})).

get_chunk_binary(Peer, Offset, RequestedPacking) ->
	PackingBinary = iolist_to_binary(ar_serialize:encode_packing(RequestedPacking, false)),
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
	StartTime = erlang:monotonic_time(),
	Response = ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/chunk2/" ++ integer_to_binary(Offset),
		timeout => 120 * 1000,
		connect_timeout => 5000,
		limit => ?MAX_SERIALIZED_CHUNK_PROOF_SIZE,
		headers => p2p_headers() ++ Headers
	}),
	prometheus_histogram:observe(
		http_client_get_chunk_duration_seconds,
		[
			ar_metrics:get_status_class(Response),
			ar_util:format_peer(Peer)
		],
		erlang:monotonic_time() - StartTime),

	handle_chunk_response(Response, RequestedPacking, Peer).

get_mempool([]) ->
	{error, not_found};
get_mempool([Peer | Peers]) ->
    case get_mempool(Peer) of
		{{ok, TXIDs}, Peer} ->
			{{ok, TXIDs}, Peer};
		{error, Error} ->
			log_failed_request(Error, [{event, failed_to_get_mempool_txids_from_peer},
					{peer, ar_util:format_peer(Peer)},
					{error, io_lib:format("~p", [Error])}]),
			get_mempool(Peers -- [Peer])
    end;

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
	}), Peer).

get_sync_buckets(Peer) ->
	handle_get_sync_buckets_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/sync_buckets",
		timeout => 10 * 1000,
		connect_timeout => 2000,
		limit => ?MAX_SYNC_BUCKETS_SIZE,
		headers => p2p_headers()
	}), ?DEFAULT_SYNC_BUCKET_SIZE).

get_footprint_buckets(Peer) ->
	handle_get_sync_buckets_response(ar_http:req(#{
		peer => Peer,
		method => get,
		path => "/footprint_buckets",
		timeout => 10 * 1000,
		connect_timeout => 2000,
		limit => ?MAX_SYNC_BUCKETS_SIZE,
		headers => p2p_headers()
	}), ?NETWORK_FOOTPRINT_BUCKET_SIZE).

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
%% must contain ar_rewards:buffered_reward_history_length/1 elements. The reward history
%% hashes are validated against the given ExpectedRewardHistoryHashes. Return not_found
%% if we fail to fetch a reward history of the expected length from any of the peers.
get_reward_history([Peer | Peers], B, ExpectedRewardHistoryHashes) ->
	#block{ height = Height, indep_hash = H } = B,
	ExpectedLength = ar_rewards:buffered_reward_history_length(Height),
	DoubleCheckLength = ar_rewards:expected_hashes_length(Height),
	true = length(ExpectedRewardHistoryHashes) == min(
													Height - ar_fork:height_2_6() + 1,
													DoubleCheckLength),
	case ar_http:req(#{
				peer => Peer,
				method => get,
				path => "/reward_history/" ++ binary_to_list(ar_util:encode(H)),
				timeout => 30000,
				headers => p2p_headers()
			}) of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case ar_serialize:binary_to_reward_history(Body) of
				{ok, RewardHistory} -> % when length(RewardHistory) == ExpectedLength ->
					case ar_rewards:validate_reward_history_hashes(Height, RewardHistory,
							ExpectedRewardHistoryHashes) of
						true ->
							?LOG_DEBUG([
								{event, received_valid_reward_history},
								{peer, ar_util:format_peer(Peer)},
								{height, Height},
								{expected_length, ExpectedLength},
								{length, length(RewardHistory)}
							]),
							{ok, RewardHistory};
						false ->
							?LOG_WARNING([{event, received_invalid_reward_history},
									{peer, ar_util:format_peer(Peer)}]),
							get_reward_history(Peers, B, ExpectedRewardHistoryHashes)
					end;
				% {ok, L} ->
				%	?LOG_WARNING([{event, received_reward_history_of_unexpected_length},
				%			{expected_length, ExpectedLength}, {received_length, length(L)},
				%			{peer, ar_util:format_peer(Peer)}]),
				%	get_reward_history(Peers, B, ExpectedRewardHistoryHashes);
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

get_block_time_history([Peer | Peers], B, ExpectedBlockTimeHistoryHashes) ->
	#block{ height = Height, indep_hash = H } = B,
	Fork_2_7 = ar_fork:height_2_7(),
	true = Height >= Fork_2_7,
	ExpectedLength = min(Height - Fork_2_7 + 1,
			ar_block_time_history:history_length() + ar_block:get_consensus_window_size()),
	true = length(ExpectedBlockTimeHistoryHashes) == min(Height - Fork_2_7 + 1,
			ar_block:get_consensus_window_size()),
	case ar_http:req(#{
				peer => Peer,
				method => get,
				path => "/block_time_history/" ++ binary_to_list(ar_util:encode(H)),
				timeout => 30000,
				headers => p2p_headers()
			}) of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			case ar_serialize:binary_to_block_time_history(Body) of
				{ok, BlockTimeHistory} when length(BlockTimeHistory) == ExpectedLength ->
					case ar_block_time_history:validate_hashes(BlockTimeHistory,
							ExpectedBlockTimeHistoryHashes) of
						true ->
							{ok, BlockTimeHistory};
						false ->
							?LOG_WARNING([{event, received_invalid_block_time_history},
									{peer, ar_util:format_peer(Peer)}]),
							get_block_time_history(Peers, B, ExpectedBlockTimeHistoryHashes)
					end;
				{ok, L} ->
					?LOG_WARNING([{event, received_block_time_history_of_unexpected_length},
							{expected_length, ExpectedLength}, {received_length, length(L)},
							{peer, ar_util:format_peer(Peer)}]),
					get_block_time_history(Peers, B, ExpectedBlockTimeHistoryHashes);
				{error, _} ->
					?LOG_WARNING([{event, failed_to_parse_block_time_history},
							{peer, ar_util:format_peer(Peer)}]),
					get_block_time_history(Peers, B, ExpectedBlockTimeHistoryHashes)
			end;
		Reply ->
			?LOG_WARNING([{event, failed_to_fetch_block_time_history},
					{peer, ar_util:format_peer(Peer)},
					{reply, io_lib:format("~p", [Reply])}]),
			get_block_time_history(Peers, B, ExpectedBlockTimeHistoryHashes)
	end;
get_block_time_history([], _B, _RewardHistoryHashes) ->
	not_found.

push_nonce_limiter_update(Peer, Update, Format) ->
	Body = ar_serialize:nonce_limiter_update_to_binary(Format, Update),
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

get_vdf_update(Peer) ->
	case ar_http:req(#{ peer => Peer, method => get, path => "/vdf2",
			timeout => 2000, headers => p2p_headers()
			}) of
		{ok, {{<<"200">>, _}, _, Bin, _, _}} ->
			ar_serialize:binary_to_nonce_limiter_update(2, Bin);
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			{error, not_found};
		{ok, {{Status, _}, _, ResponseBody, _, _}} ->
			{error, {Status, ResponseBody}};
		Reply ->
			Reply
	end.

get_vdf_session(Peer) ->
	{Path, Format} =
		case ar_config:compute_own_vdf() of
			true ->
				%% If we compute our own VDF, we need to know the VDF difficulties
				%% so that we can continue extending the new session. The VDF difficulties
				%% have been introduced in the format number 4.
				{"/vdf4/session", 4};
			false ->
				{"/vdf3/session", 3}
		end,
	case ar_http:req(#{ peer => Peer, method => get, path => Path,
			timeout => 10000, headers => p2p_headers() }) of
		{ok, {{<<"200">>, _}, _, Bin, _, _}} ->
			ar_serialize:binary_to_nonce_limiter_update(Format, Bin);
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			{error, not_found};
		{ok, {{Status, _}, _, ResponseBody, _, _}} ->
			{error, {Status, ResponseBody}};
		Reply ->
			Reply
	end.

get_previous_vdf_session(Peer) ->
	{Path, Format} =
		case ar_config:compute_own_vdf() of
			true ->
				%% If we compute our own VDF, we need to know the VDF difficulties
				%% so that we can continue extending the new session. The VDF difficulties
				%% have been introduced in the format number 4.
				{"/vdf4/previous_session", 4};
			false ->
				{"/vdf2/previous_session", 2}
		end,
	case ar_http:req(#{ peer => Peer, method => get, path => Path,
			timeout => 10000, headers => p2p_headers() }) of
		{ok, {{<<"200">>, _}, _, Bin, _, _}} ->
			ar_serialize:binary_to_nonce_limiter_update(Format, Bin);
		{ok, {{<<"404">>, _}, _, _, _, _}} ->
			{error, not_found};
		{ok, {{Status, _}, _, ResponseBody, _, _}} ->
			{error, {Status, ResponseBody}};
		Reply ->
			Reply
	end.

%% -----------------------------------------------------------------------------
%% Coordinated Mining and Pool Request
%% -----------------------------------------------------------------------------

get_cm_partition_table(Peer) ->
	Req = build_cm_or_pool_request(get, Peer, "/coordinated_mining/partition_table"),
	handle_cm_partition_table_response(ar_http:req(Req)).

cm_h1_send(Peer, Candidate) ->
	JSON = ar_serialize:jsonify(ar_serialize:candidate_to_json_struct(Candidate)),
	Req = build_cm_or_pool_request(post, Peer, "/coordinated_mining/h1", JSON),
	handle_cm_noop_response(ar_http:req(Req)).

cm_h2_send(Peer, Candidate) ->
	JSON = ar_serialize:jsonify(ar_serialize:candidate_to_json_struct(Candidate)),
	Req = build_cm_or_pool_request(post, Peer, "/coordinated_mining/h2", JSON),
	handle_cm_noop_response(ar_http:req(Req)).

cm_publish_send(Peer, Solution) ->
	?LOG_DEBUG([{event, cm_publish_send}, {peer, ar_util:format_peer(Peer)},
		{solution, ar_util:encode(Solution#mining_solution.solution_hash)},
		{step_number, Solution#mining_solution.step_number},
		{start_interval_number, Solution#mining_solution.start_interval_number},
		{seed, ar_util:encode(Solution#mining_solution.seed)}]),
	JSON = ar_serialize:jsonify(ar_serialize:solution_to_json_struct(Solution)),
	Req = build_cm_or_pool_request(post, Peer, "/coordinated_mining/publish", JSON),
	handle_cm_noop_response(ar_http:req(Req)).

%% @doc Fetch the jobs from the pool or coordinated mining exit peer.
get_jobs(Peer, PrevOutput) ->
	prometheus_counter:inc(pool_job_request_count),
	Req = build_cm_or_pool_request(get, Peer,
		"/jobs/" ++ binary_to_list(ar_util:encode(PrevOutput))),
	handle_get_jobs_response(ar_http:req(Req)).

%% @doc Post the partial solution to the pool or coordinated mining exit peer.
post_partial_solution(Peer, Solution) ->
	Payload =
		case is_binary(Solution) of
			true ->
				Solution;
			false ->
				ar_serialize:jsonify(ar_serialize:solution_to_json_struct(Solution))
		end,
	Req = build_cm_or_pool_request(post, Peer, "/partial_solution", Payload),
	handle_post_partial_solution_response(ar_http:req(Req#{
		timeout => 20 * 1000,
		connect_timeout => 5 * 1000
	})).

get_pool_cm_jobs(Peer, Jobs) ->
	JSON = ar_serialize:jsonify(ar_serialize:pool_cm_jobs_to_json_struct(Jobs)),
	Req = build_cm_or_pool_request(post, Peer, "/pool_cm_jobs", JSON),
	handle_get_pool_cm_jobs_response(ar_http:req(Req#{
		connect_timeout => 1000
	})).

post_pool_cm_jobs(Peer, Payload) ->
	Req = build_cm_or_pool_request(post, Peer, "/pool_cm_jobs", Payload),
	handle_post_pool_cm_jobs_response(ar_http:req(Req#{
		timeout => 10 * 1000,
		connect_timeout => 2000
	})).

post_cm_partition_table_to_pool(Peer, Payload) ->
	Req = build_cm_or_pool_request(post, Peer, "/coordinated_mining/partition_table", Payload),
	handle_cm_partition_table_response(ar_http:req(Req#{
		timeout => 10 * 1000,
		connect_timeout => 2000
	})).

%% @doc Fetch data_root metadata for the block that starts at or before the given offset,
%% and validate it against the local block index. Also recompute the TXRoot from entries.
get_data_roots(Peer, Offset) ->
	Path = "/data_roots/" ++ integer_to_list(Offset),
	Response = ar_http:req(#{
			peer => Peer,
			method => get,
			path => Path,
			timeout => 10 * 1000,
			connect_timeout => 2000,
			headers => p2p_headers()
		}),
	handle_get_data_roots_response(Response, Offset).

handle_get_data_roots_response({ok, {{<<"200">>, _}, _, Body, _, _}}, Offset) ->
	case ar_serialize:binary_to_data_roots(Body) of
		{ok, {TXRoot, BlockSize, Entries}} ->
			handle_get_data_roots_response(TXRoot, BlockSize, Entries, Offset);
		_ ->
			{error, invalid_response}
	end;
handle_get_data_roots_response({ok, {{<<"404">>, _}, _, _, _, _}}, _Offset) ->
	{error, not_found};
handle_get_data_roots_response(Other, _Offset) ->
	{error, Other}.

handle_get_data_roots_response(TXRoot, BlockSize, Entries, Offset) ->
	{BlockStart, BlockEnd, ExpectedTXRoot} = ar_block_index:get_block_bounds(Offset),
	CheckBlockBounds =
		case Offset >= BlockStart andalso Offset < BlockEnd of
			false ->
				{error, invalid_block_bounds};
			true ->
				ok
		end,
	CheckBlockSize =
		case CheckBlockBounds of
			ok ->
				case BlockSize == BlockEnd - BlockStart of
					false ->
						{error, invalid_block_size};
					true ->
						ok
				end;
			Error ->
				Error
		end,
	PrepareDataRootPairs =
		case CheckBlockSize of
			ok ->
				prepare_data_root_pairs(Entries, BlockStart, BlockSize);
			Error2 ->
				Error2
		end,
	ValidateTXRoot =
		case PrepareDataRootPairs of
			{ok, Triplets} ->
				case TXRoot == ExpectedTXRoot of
					false ->
						{error, invalid_tx_root};
					true ->
						{ok, Triplets}
				end;
			{error, _} = Error3 ->
				Error3
		end,
	case ValidateTXRoot of
		{ok, Triplets2} ->
			case verify_tx_paths(Triplets2, TXRoot, BlockStart, BlockEnd, 0) of
				ok ->
					{ok, {TXRoot, BlockSize, Entries}};
				Error4 ->
					Error4
			end;
		Error5 ->
			Error5
	end.

prepare_data_root_pairs(Entries, BlockStart, BlockSize) ->
	Result = lists:foldr(
		fun
			(_, {error, _} = Error) ->
				Error;
			({_DataRoot, 0, _TXStartOffset, _TXPath}, _Acc) ->
				{error, invalid_zero_tx_size};
			({DataRoot, TXSize, TXStartOffset, TXPath}, {ok, {Total, Acc}}) ->
				MerkleLabel = TXStartOffset + TXSize - BlockStart,
				case MerkleLabel >= 0 of
					true ->
						PaddedSize = get_padded_size(TXSize, BlockStart),
						{ok, {Total + PaddedSize, [{DataRoot, MerkleLabel, TXPath} | Acc]}};
					false ->
						{error, invalid_entry_merkle_label}
				end
		end,
		{ok, {0,[]}},
		Entries
	),
	case Result of
		{ok, {Total, Entries2}} ->
			case Total == BlockSize of
				true ->
					{ok, lists:reverse(Entries2)};
				false ->
					{error, invalid_total_tx_size}
			end;
		Error6 ->
			Error6
	end.

get_padded_size(TXSize, BlockStart) ->
	case BlockStart >= ar_block:strict_data_split_threshold() of
		true ->
			ar_poa:get_padded_offset(TXSize, 0);
		false ->
			TXSize
	end.

verify_tx_paths([], _TXRoot, _BlockStart, _BlockEnd, _TXStartOffset) ->
	ok;
verify_tx_paths([Entry | Entries], TXRoot, BlockStart, BlockEnd, TXStartOffset) ->
	{DataRoot, TXEndOffset, TXPath} = Entry,
	BlockSize = BlockEnd - BlockStart,
	case ar_merkle:validate_path(TXRoot, TXEndOffset - 1, BlockSize, TXPath) of
		false ->
			{error, invalid_tx_path};
		{DataRoot, TXStartOffset, TXEndOffset} ->
			PaddedEndOffset = get_padded_size(TXEndOffset, BlockStart),
			verify_tx_paths(Entries, TXRoot, BlockStart, BlockEnd, PaddedEndOffset);
		_ ->
			{error, invalid_tx_path}
	end.

get_peer_and_path_from_url(URL) ->
	#{ host := Host, path := P } = Parsed = uri_string:parse(URL),
	Peer = case maps:get(port, Parsed, undefined) of
		undefined ->
			case maps:get(scheme, Parsed, undefined) of
				"https" ->
					{binary_to_list(Host), 443};
				_ ->
					{binary_to_list(Host), 1984}
			end;
		Port ->
			{binary_to_list(Host), Port}
	end,
	{Peer, binary_to_list(P)}.

build_cm_or_pool_request(Method, Peer, Path) ->
	build_cm_or_pool_request(Method, Peer, Path, <<>>).
build_cm_or_pool_request(Method, Peer, Path, Body) ->
	{Peer3, Headers, BasePath, IsPeerRequest} =
		case Peer of
			{pool, URL} ->
				{Peer2, Path2} = get_peer_and_path_from_url(URL),
				{Peer2, pool_client_headers(), Path2, false};
			_ ->
				{Peer, cm_p2p_headers(), "", true}
		end,
	Headers2 = case Method of
		get ->
			Headers;
		_ ->
			add_header(<<"content-type">>, <<"application/json">>, Headers)
	end,
	#{
		peer => Peer3,
		method => Method,
		path => BasePath ++ Path,
		timeout => 5 * 1000,
		connect_timeout => 500,
		headers => Headers2,
		body => Body,
		is_peer_request => IsPeerRequest
	}.

handle_get_pool_cm_jobs_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch ar_serialize:json_map_to_pool_cm_jobs(
			element(2, ar_serialize:json_decode(Body, [return_maps]))) of
		{'EXIT', _} ->
			{error, invalid_json};
		Jobs ->
			{ok, Jobs}
	end;
handle_get_pool_cm_jobs_response(Reply) ->
	{error, Reply}.

handle_post_pool_cm_jobs_response({ok, {{<<"200">>, _}, _, _, _, _}}) ->
	ok;
handle_post_pool_cm_jobs_response(Reply) ->
	{error, Reply}.

handle_post_partial_solution_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch jiffy:decode(Body, [return_maps]) of
		{'EXIT', _} ->
			{error, invalid_json};
		Response ->
			{ok, Response}
	end;
handle_post_partial_solution_response(Reply) ->
	{error, Reply}.

handle_get_jobs_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch ar_serialize:json_struct_to_jobs(ar_serialize:dejsonify(Body)) of
		{'EXIT', _} ->
			{error, invalid_json};
		Jobs ->
			prometheus_counter:inc(pool_total_job_got_count, length(Jobs#jobs.jobs)),
			{ok, Jobs}
	end;
handle_get_jobs_response(Reply) ->
	{error, Reply}.

handle_sync_record_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_intervals:safe_from_etf(Body);
handle_sync_record_response({ok, {{<<"429">>, _}, _, _, _, _}}) ->
	{error, too_many_requests};
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
handle_sync_record_response({ok, {{<<"429">>, _}, _, _, _, _}}, _, _) ->
	{error, too_many_requests};
handle_sync_record_response(Reply, _, _) ->
	{error, Reply}.

handle_footprints_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch ar_serialize:json_map_to_footprint(jiffy:decode(Body, [return_maps])) of
		{'EXIT', Reason} ->
			{error, Reason};
		Footprint ->
			{ok, Footprint}
	end;
handle_footprints_response({ok, {{<<"404">>, _}, _, _, _, _}}) ->
	not_found;
handle_footprints_response({ok, {{<<"400">>, _}, _, Body, _, _}}) ->
	case catch jiffy:decode(Body, [return_maps]) of
		{'EXIT', Reason} ->
			{error, Reason};
		#{ <<"error">> := <<"footprint_number_too_large">> } ->
			{error, footprint_number_too_large};
		#{ <<"error">> := <<"negative_footprint_number">> } ->
			{error, negative_footprint_number};
		#{ <<"error">> := <<"negative_partition_number">> } ->
			{error, negative_partition_number};
		#{ <<"error">> := <<"invalid_footprint_number_encoding">> } ->
			{error, invalid_footprint_number_encoding};
		Response ->
			{error, Response}
	end;
handle_footprints_response(Reply) ->
	{error, Reply}.

handle_chunk_response({ok, {{<<"200">>, _}, _, Body, Start, End}}, RequestedPacking, Peer) ->
	case catch ar_serialize:binary_to_poa(Body) of
		{'EXIT', Reason} ->
			{error, Reason};
		{error, Reason} ->
			{error, Reason};
		{ok, #{ packing := Packing } = Proof} ->
			CheckPacking =
				case RequestedPacking of
					any ->
						true;
					Packing ->
						true;
					_ ->
						false
				end,
			case CheckPacking of
				true ->
					case maps:get(chunk, Proof) of
						<<>> ->
							{error, empty_chunk};
						Chunk when byte_size(Chunk) > ?DATA_CHUNK_SIZE ->
							{error, chunk_bigger_than_256kib};
						_ ->
							{ok, Proof, End - Start, byte_size(term_to_binary(Proof))}
					end;
				false ->
					?LOG_WARNING([{event, peer_served_proof_with_wrong_packing},
						{requested_packing, ar_serialize:encode_packing(RequestedPacking, false)},
						{got_packing, ar_serialize:encode_packing(Packing, false)},
						{peer, ar_util:format_peer(Peer)}]),
					{error, wrong_packing}
			end
	end;
handle_chunk_response({error, _} = Response, _RequestedPacking, _Peer) ->
	Response;
handle_chunk_response(Response, _RequestedPacking, _Peer) ->
	{error, Response}.

handle_mempool_response({ok, {{<<"200">>, _}, _, Body, _, _}}, Peer) ->
	case catch jiffy:decode(Body) of
		{'EXIT', Error} ->
			?LOG_WARNING([{event, failed_to_parse_peer_mempool},
				{error, io_lib:format("~p", [Error])}]),
			{error, invalid_json};
		L when is_list(L) ->
			Result = lists:foldr(
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
			),
			case Result of
				{ok, TXIDs} ->
					{{ok, TXIDs}, Peer};
				{error, Reason2} ->
					{error, Reason2}
			end;
		NotList ->
			?LOG_WARNING([{event, failed_to_parse_peer_mempool}, {reason, invalid_format},
				{reply, io_lib:format("~p", [NotList])}]),
			{error, invalid_format}
	end;
handle_mempool_response(Response, _Peer) ->
	{error, Response}.

handle_get_sync_buckets_response({ok, {{<<"200">>, _}, _, Body, _, _}}, BucketSize) ->
	case ar_sync_buckets:deserialize(Body, BucketSize) of
		{ok, Buckets} ->
			{ok, Buckets};
		{'EXIT', Reason} ->
			{error, Reason};
		_ ->
			{error, invalid_response_type}
	end;
handle_get_sync_buckets_response({ok, {{<<"400">>, _}, _,
		<<"Request type not found.">>, _, _}}, _BucketSize) ->
	{error, request_type_not_found};
handle_get_sync_buckets_response(Response, _BucketSize) ->
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
			ar_peers:issue_warning(Peer, recent_hash_list_diff, invalid_input),
			{error, invalid_input};
		{error, unknown_base} ->
			ar_peers:issue_warning(Peer, recent_hash_list_diff, unknown_base),
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
	{error, invalid_input4}.

count_blocks_on_top(Bin) ->
	count_blocks_on_top(Bin, 0).

count_blocks_on_top(<<>>, N) ->
	{ok, N};
count_blocks_on_top(<< _H:48/binary, Len:16, _TXIDs:(32 * Len)/binary, Rest/binary >>, N) ->
	count_blocks_on_top(Rest, N + 1);
count_blocks_on_top(_Bin, _N) ->
	{error, invalid_input5}.

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

get_txs(Peers, B) ->
	case B#block.txs of
		TXIDs when length(TXIDs) > ?BLOCK_TX_COUNT_LIMIT ->
			?LOG_ERROR([{event, downloaded_txs_count_exceeds_limit}]),
			{error, txs_count_exceeds_limit};
		TXIDs ->
			get_txs(B#block.height, Peers, TXIDs, [], 0)
	end.

get_txs(_Height, _Peers, [], TXs, _TotalSize) ->
	{ok, lists:reverse(TXs)};
get_txs(Height, Peers, [TXID | Rest], TXs, TotalSize) ->
	Fork_2_0 = ar_fork:height_2_0(),
	case get_tx(Peers, TXID) of
		#tx{ format = 2 } = TX ->
			get_txs(Height, Peers, Rest, [TX | TXs], TotalSize);
		#tx{} = TX when Height < Fork_2_0 ->
			get_txs(Height, Peers, Rest, [TX | TXs], TotalSize);
		#tx{ format = 1 } = TX ->
			case TotalSize + TX#tx.data_size of
				NewTotalSize when NewTotalSize > ?BLOCK_TX_DATA_SIZE_LIMIT ->
					?LOG_ERROR([{event, downloaded_txs_exceed_block_size_limit}]),
					{error, txs_exceed_block_size_limit};
				NewTotalSize ->
					get_txs(Height, Peers, Rest, [TX | TXs], NewTotalSize)
			end;
		_ ->
			{error, tx_not_found}
	end.

%% @doc Retreive a tx by ID from the memory pool, disk, or a remote peer.
get_tx(Peer, TX) when not is_list(Peer) ->
	get_tx([Peer], TX);
get_tx(_Peers, #tx{} = TX) ->
	TX;
get_tx(Peers, TXID) ->
	case ar_mempool:get_tx(TXID) of
		not_found ->
			get_tx_from_disk_or_peers(Peers, TXID);
		TX ->
			TX
	end.

get_tx_from_disk_or_peers(Peers, TXID) ->
	case ar_storage:read_tx(TXID) of
		unavailable ->
			case get_tx_from_remote_peers(Peers, TXID) of
				not_found ->
					not_found;
				{TX, _Peer, _Time, _Size} ->
					TX
			end;
		TX ->
			TX
	end.

get_tx_from_remote_peers(Peers, TXID) ->
	get_tx_from_remote_peers(Peers, TXID, true).

get_tx_from_remote_peers([], _TXID, _RatePeer) ->
	not_found;
get_tx_from_remote_peers(Peers, TXID, RatePeer) ->
	Peer = lists:nth(rand:uniform(min(5, length(Peers))), Peers),
	case get_tx_from_remote_peer(Peer, TXID, RatePeer) of
		{#tx{} = TX, Peer, Time, Size} ->
			{TX, Peer, Time, Size};
		_ ->
			get_tx_from_remote_peers(Peers -- [Peer], TXID, RatePeer)
	end.

get_tx_from_remote_peer(Peer, TXID, RatePeer) ->
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
		{ok, #tx{} = TX, Time, Size} ->
			case ar_tx:verify_tx_id(TXID, TX) of
				false ->
					?LOG_WARNING([
						{event, peer_served_invalid_tx},
						{peer, ar_util:format_peer(Peer)},
						{tx, ar_util:encode(TXID)}
					]),
					ar_peers:issue_warning(Peer, tx, invalid),
					{error, invalid_tx};
				true ->
					case RatePeer of
						true ->
							ar_peers:rate_fetched_data(Peer, tx, Time, Size);
						false ->
							ok
					end,
					{TX, Peer, Time, Size}
			end;
		Error ->
			Error
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
			maps:get(atom_to_binary(Type), Info, info_unavailable)
	end.
get_info(Peer) ->
	case
		ar_http:req(#{
			method => get,
			peer => Peer,
			path => "/info",
			headers => p2p_headers(),
			connect_timeout => 1000,
			timeout => 2 * 1000
		})
	of
		{ok, {{<<"200">>, _}, _, JSON, _, _}} ->
			case ar_serialize:json_decode(JSON, [return_maps]) of
				{ok, JsonMap} ->
					JsonMap;
				{error, _} ->
					info_unavailable
			end;
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
			PeersList = lists:map(fun ar_util:parse_peer/1, PeerArray),
			lists:flatten(PeersList)
		end
	catch _:_ -> unavailable
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
			ar_peers:issue_warning(Peer, block, Reason),
			not_found;
		{ok, B} ->
			{ok, B, End - Start, byte_size(term_to_binary(B))};
		B when is_record(B, block) ->
			{ok, B, End - Start, byte_size(term_to_binary(B))};
		Error ->
			?LOG_INFO(
				"event: failed_to_parse_block_response, peer: ~s, error: ~p",
				[ar_util:format_peer(Peer), Error]),
			ar_peers:issue_warning(Peer, block, Error),
			not_found
	end;
handle_block_response(Peer, _Encoding, Response) ->
	ar_peers:issue_warning(Peer, block, Response),
	not_found.

%% @doc Process the response of a GET /unconfirmed_tx call.
handle_tx_response(_Peer, _Encoding, {ok, {{<<"404">>, _}, _, _, _, _}}) ->
	{error, not_found};
handle_tx_response(_Peer, _Encoding, {ok, {{<<"400">>, _}, _, _, _, _}}) ->
	{error, bad_request};
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
			ar_peers:issue_warning(Peer, tx, Reason),
			{error, Reason};
		Reply ->
			ar_peers:issue_warning(Peer, tx, Reply),
			Reply
	end;
handle_tx_response(Peer, _Encoding, Response) ->
	ar_peers:issue_warning(Peer, tx, Response),
	{error, Response}.

handle_cm_partition_table_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	case catch jiffy:decode(Body) of
		{'EXIT', Error} ->
			?LOG_WARNING([{event, failed_to_parse_cm_partition_table},
				{error, io_lib:format("~p", [Error])}]),
			{error, invalid_json};
		L when is_list(L) ->
			lists:foldr(
				fun	(_, {error, Reason}) ->
						{error, Reason};
					(Partition, {ok, Acc}) ->
						case Partition of
							{[
								{<<"bucket">>, Bucket},
								{<<"bucketsize">>, BucketSize},
								{<<"addr">>, EncodedAddr}
							]} ->
								DecodedPartition = {
									Bucket,
									BucketSize,
									ar_util:decode(EncodedAddr),
									0
								},
								{ok, [DecodedPartition | Acc]};
							{[
								{<<"bucket">>, Bucket},
								{<<"bucketsize">>, BucketSize},
								{<<"addr">>, EncodedAddr},
								{<<"pdiff">>, PackingDifficulty}
							]} when is_integer(PackingDifficulty) andalso (
								(PackingDifficulty >= 1
									andalso PackingDifficulty =< ?MAX_PACKING_DIFFICULTY)
										orelse
									(PackingDifficulty == ?REPLICA_2_9_PACKING_DIFFICULTY)) ->
								DecodedPartition = {
									Bucket,
									BucketSize,
									ar_util:decode(EncodedAddr),
									PackingDifficulty
								},
								{ok, [DecodedPartition | Acc]};
							_ ->
								?LOG_WARNING([{event, failed_to_parse_cm_partition_table},
									{reason, invalid_partition},
									{txid, io_lib:format("~p", [Partition])}]),
								{error, invalid_partition}
						end
				end,
				{ok, []},
				L
			);
		NotList ->
			?LOG_WARNING([{event, failed_to_parse_cm_partition_table}, {reason, invalid_format},
				{reply, io_lib:format("~p", [NotList])}]),
			{error, invalid_format}
	end;
handle_cm_partition_table_response(Response) ->
	{error, Response}.

handle_cm_noop_response({ok, {{<<"200">>, _}, _, _Body, _, _}}) ->
	{ok, []};
handle_cm_noop_response(Response) ->
	{error, Response}.

p2p_headers() ->
	{ok, Config} = arweave_config:get_env(),
	[{<<"x-p2p-port">>, integer_to_binary(Config#config.port)},
			{<<"x-release">>, integer_to_binary(?RELEASE_NUMBER)}].

cm_p2p_headers() ->
	{ok, Config} = arweave_config:get_env(),
	add_header(<<"x-cm-api-secret">>, Config#config.cm_api_secret, p2p_headers()).

pool_client_headers() ->
	{ok, Config} = arweave_config:get_env(),
	Headers = add_header(<<"x-pool-api-key">>, Config#config.pool_api_key, p2p_headers()),
	case Config#config.pool_worker_name of
		not_set ->
			Headers;
		WorkerName ->
			add_header(<<"worker">>, WorkerName, Headers)
	end.

add_header(Name, Value, Headers) when is_binary(Name) andalso is_binary(Value) ->
	[{Name, Value} | Headers];
add_header(Name, Value, Headers) ->
	?LOG_ERROR([{event, invalid_header}, {name, Name}, {value, Value}]),
	Headers.

%% @doc Utility to filter out some log spam. We generally don't want to log a failed HTTP
%% request if the peer just times out or is no longer taking connections.
log_failed_request(Reason, Log) ->
	case Reason of
		{error,{shutdown,econnrefused}} -> ok;
		{error,{shutdown,timeout}} -> ok;
		{error,timeout} -> ok;
		{error,{shutdown,ehostunreach}} -> ok;
		{error,{stream_error,closed}} -> ok;
		{error,{stream_error,closing}} -> ok;
		_ -> ?LOG_DEBUG(Log)
	end.
