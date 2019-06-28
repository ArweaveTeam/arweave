%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface_client).

-export([send_new_block/4, send_new_tx/2, get_block/3]).
-export([get_tx/2, get_tx_data/2, get_full_block/3, get_block_subfield/3, add_peer/1]).
-export([get_tx_reward/2]).
-export([get_encrypted_block/2, get_encrypted_full_block/2]).
-export([get_info/1, get_info/2, get_peers/1, get_peers/2, get_pending_txs/1]).
-export([get_time/2, get_height/1]).
-export([get_wallet_list/2, get_hash_list/1, get_hash_list/2]).
-export([get_current_block/1, get_current_block/2]).

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
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/tx",
		p2p_headers(),
		ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX)),
		3 * 1000
	).

%% @doc Check whether a peer has a given transaction
has_tx(Peer, ID) ->
	case
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/tx/" ++ binary_to_list(ar_util:encode(ID)) ++ "/id",
			p2p_headers(),
			[],
			3 * 1000
		)
	of
		{ok, {{<<"200">>, _}, _, _, _, _}} -> has_tx;
		{ok, {{<<"202">>, _}, _, _, _, _}} -> has_tx; % In the mempool
		{ok, {{<<"404">>, _}, _, _, _, _}} -> doesnt_have_tx;
		_ -> error
	end.


%% @doc Distribute a newly found block to remote nodes.
send_new_block(Peer, NewB, BDS, {RecallIndepHash, RecallSize, Key, Nonce}) ->
	ShortHashList =
		lists:map(
			fun ar_util:encode/1,
			lists:sublist(
				NewB#block.hash_list,
				1,
				?STORE_BLOCKS_BEHIND_CURRENT
			)
		),
	{SmallBlockProps} =
		ar_serialize:block_to_json_struct(
			NewB#block { wallet_list = [] }
		),
	BlockShadowProps = [{<<"hash_list">>, ShortHashList} | SmallBlockProps],
	PostProps = [
		{<<"new_block">>, {BlockShadowProps}},
		{<<"recall_block">>, ar_util:encode(RecallIndepHash)},
		{<<"recall_size">>, RecallSize},
		%% Add the P2P port field to be backwards compatible with nodes
		%% running the old version of the P2P port feature.
		{<<"port">>, ?DEFAULT_HTTP_IFACE_PORT},
		{<<"key">>, ar_util:encode(Key)},
		{<<"nonce">>, ar_util:encode(Nonce)},
		{<<"block_data_segment">>, ar_util:encode(BDS)}
	],
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/block",
		p2p_headers(),
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
		)
	).

%% @doc Get a peers current, top block.
get_current_block(Peer) ->
	get_current_block(Peer, get_hash_list(Peer)).
get_current_block(Peer, BHL) ->
	get_full_block(Peer, hd(BHL), BHL).

%% @doc Get the minimum cost that a remote peer would charge for
%% a transaction of the given data size in bytes.
get_tx_reward(Peer, Size) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/price/" ++ integer_to_list(Size),
			p2p_headers()
		),
	binary_to_integer(Body).

%% @doc Retreive a block by height or hash from a remote peer.
get_block(Peer, ID, BHL) ->
	get_full_block(Peer, ID, BHL).

%% @doc Get an encrypted block from a remote peer.
%% Used when the next block is the recall block.
get_encrypted_block(Peer, Hash) when is_binary(Hash) ->
	handle_encrypted_block_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/encrypted",
			p2p_headers()
		)
	).

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
prepare_block_id(ID) when is_binary(ID) ->
	"/block/hash/" ++ binary_to_list(ar_util:encode(ID));
prepare_block_id(ID) when is_integer(ID) ->
	"/block/height/" ++ integer_to_list(ID).

%% @doc Retreive a full block (full transactions included in body)
%% by hash from a remote peer.
get_full_block(Peer, ID, BHL) ->
	handle_block_response(
		Peer,
		ar_httpc:request(
			<<"GET">>,
			Peer,
			prepare_block_id(ID),
			p2p_headers(),
			[],
			10 * 1000
		),
		BHL
	).

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
get_hash_list(Peer) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/hash_list",
			p2p_headers()
		),
	ar_serialize:json_struct_to_hash_list(ar_serialize:dejsonify(Body)).

get_hash_list(Peer, Hash) ->
	Response =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/hash_list",
			p2p_headers()
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:dejsonify(ar_serialize:json_struct_to_hash_list(Body));
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

%% @doc Retreive a full block (full transactions included in body)
%% by hash from a remote peer in an encrypted form
get_encrypted_full_block(Peer, Hash) when is_binary(Hash) ->
	handle_encrypted_full_block_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/all/encrypted",
			p2p_headers()
		)
	).

%% @doc Retreive a tx by hash from a remote peer
get_tx(Peer, Hash) ->
	handle_tx_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/tx/" ++ binary_to_list(ar_util:encode(Hash)),
			p2p_headers(),
			[],
			10 * 1000
		)
	).

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
	{_, NetworkName} = lists:keyfind(<<"network">>, 1, Props),
	{_, ClientVersion} = lists:keyfind(<<"version">>, 1, Props),
	ReleaseNumber =
		case lists:keyfind(<<"release">>, 1, Props) of
			false -> 0;
			R -> R
		end,
	{_, Height} = lists:keyfind(<<"height">>, 1, Props),
	{_, Blocks} = lists:keyfind(<<"blocks">>, 1, Props),
	{_, Peers} = lists:keyfind(<<"peers">>, 1, Props),
	[
		{name, NetworkName},
		{version, ClientVersion},
		{height, Height},
		{blocks, Blocks},
		{peers, Peers},
		{release, ReleaseNumber}
	].

%% @doc Process the response of an /block call.
handle_block_response(Peer, {ok, {{<<"200">>, _}, _, Body, _, _}}, BHL) ->
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
						ar_block:generate_hash_list_for_block(B, BHL);
					HL -> HL
				end,
			FullB =
				B#block {
					txs = [ get_tx(Peer, TXID) || TXID <- B#block.txs ],
					hash_list = HashList,
					wallet_list = WalletList
				},
			case lists:any(fun erlang:is_atom/1, FullB#block.txs) or is_atom(WalletList) of
				false -> FullB;
				true -> unavailable
			end;
		false -> B
	end;
handle_block_response(_, {error, _}, _) -> unavailable;
handle_block_response(_, {ok, {{<<"400">>, _}, _, _, _, _}}, _) -> unavailable;
handle_block_response(_, {ok, {{<<"404">>, _}, _, _, _, _}}, _) -> not_found;
handle_block_response(_, {ok, {{<<"500">>, _}, _, _, _, _}}, _) -> unavailable.

%% @doc Process the response of a /block/.../encrypted call.
handle_encrypted_block_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_util:decode(Body);
handle_encrypted_block_response({error, _}) -> unavailable;
handle_encrypted_block_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_encrypted_block_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> unavailable.

%% @doc Process the response of a /block/.../all/encrypted call.
handle_encrypted_full_block_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_util:decode(Body);
handle_encrypted_full_block_response({error, _}) -> unavailable;
handle_encrypted_full_block_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_encrypted_full_block_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> unavailable.

%% @doc Process the response of a /block/[{Height}|{Hash}]/{Subfield} call.
handle_block_field_response({"timestamp", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	binary_to_integer(Body);
handle_block_field_response({"last_retarget", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	binary_to_integer(Body);
handle_block_field_response({"diff", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	binary_to_integer(Body);
handle_block_field_response({"height", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	binary_to_integer(Body);
handle_block_field_response({"txs", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	ar_serialize:json_struct_to_tx(Body);
handle_block_field_response({"hash_list", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	ar_serialize:json_struct_to_hash_list(ar_serialize:dejsonify(Body));
handle_block_field_response({"wallet_list", {ok, {{<<"200">>, _}, _, Body, _, _}}}) ->
	ar_serialize:json_struct_to_wallet_list(Body);
handle_block_field_response({_Subfield, {ok, {{<<"200">>, _}, _, Body, _, _}}}) -> Body;
handle_block_field_response({error, _}) -> unavailable;
handle_block_field_response({ok, {{<<"404">>, _}, _, _}}) -> not_found;
handle_block_field_response({ok, {{<<"500">>, _}, _, _}}) -> unavailable.

%% @doc Process the response of a /tx call.
handle_tx_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_serialize:json_struct_to_tx(Body);
handle_tx_response({ok, {{<<"202">>, _}, _, _, _, _}}) -> pending;
handle_tx_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_tx_response({ok, {{<<"410">>, _}, _, _, _, _}}) -> gone;
handle_tx_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> not_found.

p2p_headers() ->
	[{<<"X-P2p-Port">>, integer_to_binary(ar_meta_db:get(port))}].
