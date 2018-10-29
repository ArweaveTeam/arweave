%%%
%%% @doc Exposes access to an internal Arweave client to external nodes on the network.
%%%

-module(ar_http_iface_client).

-export([send_new_block/3, send_new_block/4, send_new_block/6, send_new_tx/2, get_block/3]).
-export([send_new_block_with_bds/5]).
-export([get_tx/2, get_full_block/3, get_block_subfield/3, add_peer/1]).
-export([get_encrypted_block/2, get_encrypted_full_block/2]).
-export([get_info/1, get_info/2, get_peers/1, get_pending_txs/1, has_tx/2]).
-export([get_time/1]).
-export([get_wallet_list/2, get_hash_list/1, get_hash_list/2]).
-export([get_current_block/1, get_current_block/2]).

-include("ar.hrl").

%%%
%%% Public API.
%%%

%% @doc Send a new transaction to an Arweave HTTP node.
send_new_tx(Peer, TX) ->
	if
		byte_size(TX#tx.data) < 50000 ->
			do_send_new_tx(Peer, TX);
		true ->
			case has_tx(Peer, TX#tx.id) of
				false -> do_send_new_tx(Peer, TX);
				true -> not_sent
			end
	end.

do_send_new_tx(Peer, TX) ->
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/tx",
		ar_serialize:jsonify(ar_serialize:tx_to_json_struct(TX))
	).

%% @doc Check whether a peer has a given transaction
has_tx(Peer, ID) ->
	case
		ar_httpc:request(
				<<"GET">>,
				Peer,
				"/tx/" ++ binary_to_list(ar_util:encode(ID)) ++ "/id",
				[]
		)
	of
		{ok, {{<<"200">>, _}, _, _, _, _}} -> true;
		{ok, {{<<"202">>, _}, _, _, _, _}} -> true;
		_ -> false
	end.


%% @doc Distribute a newly found block to remote nodes.
send_new_block(IP, NewB, RecallB) ->
	send_new_block(IP, ?DEFAULT_HTTP_IFACE_PORT, NewB, RecallB).
send_new_block(Peer, Port, NewB, RecallB) ->
	RecallBHash =
		case ?IS_BLOCK(RecallB) of
			true ->  RecallB#block.indep_hash;
			false -> <<>>
		end,
	{Key, Nonce} = case ar_key_db:get(RecallBHash) of
		[{K, N}] -> {K, N};
		_        -> {<<>>, <<>>}
	end,
	send_new_block(Peer, Port, NewB, RecallB, Key, Nonce).
send_new_block(Peer, Port, NewB, RecallB, Key, Nonce) ->
	BDS = ar_block:generate_block_data_segment(
		ar_storage:read_block(
			NewB#block.previous_block,
			ar_node:get_hash_list(whereis(http_entrypoint_node))
		),
		RecallB,
		lists:map(fun ar_storage:read_tx/1, NewB#block.txs),
		NewB#block.reward_addr,
		NewB#block.timestamp,
		NewB#block.tags
    ),
	RecallBHash =
		case ?IS_BLOCK(RecallB) of
			true ->  RecallB#block.indep_hash;
			false -> <<>>
		end,
	HashList =
		lists:map(
			fun ar_util:encode/1,
			lists:sublist(
				NewB#block.hash_list,
				1,
				?STORE_BLOCKS_BEHIND_CURRENT
			)
		),
	{TempJSONStruct} =
		ar_serialize:block_to_json_struct(
			NewB#block { wallet_list = [] }
		),
	BlockJSON =
		{
			[{<<"hash_list">>, HashList }|TempJSONStruct]
		},
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/block",
		ar_serialize:jsonify(
			{
				[
					{<<"block_data_segment">>, ar_util:encode(BDS)},
					{<<"new_block">>, BlockJSON},
					{<<"recall_block">>, ar_util:encode(RecallBHash)},
					{<<"recall_size">>, RecallB#block.block_size},
					{<<"port">>, Port},
					{<<"key">>, ar_util:encode(Key)},
					{<<"nonce">>, ar_util:encode(Nonce)}
				]
			}
		)
	).

%% @doc As send_new_block but with block_data_segment in POST json.
%% only used in tests.
send_new_block_with_bds(Peer, Port, NewB, RecallB, BDS) ->
	RecallBHash =
		case ?IS_BLOCK(RecallB) of
			true ->  RecallB#block.indep_hash;
			false -> <<>>
		end,
	{Key, Nonce} = case ar_key_db:get(RecallBHash) of
		[{K, N}] -> {K, N};
		_        -> {<<>>, <<>>}
	end,
	HashList =
		lists:map(
			fun ar_util:encode/1,
			lists:sublist(
				NewB#block.hash_list,
				1,
				?STORE_BLOCKS_BEHIND_CURRENT
			)
		),
	{TempJSONStruct} =
		ar_serialize:block_to_json_struct(
			NewB#block { wallet_list = [] }
		),
	BlockJSON =
		{
			[{<<"hash_list">>, HashList }|TempJSONStruct]
		},
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/block",
		ar_serialize:jsonify(
			{
				[
					{<<"block_data_segment">>, ar_util:encode(BDS)},
					{<<"new_block">>, BlockJSON},
					{<<"recall_block">>, ar_util:encode(RecallBHash)},
					{<<"recall_size">>, RecallB#block.block_size},
					{<<"port">>, Port},
					{<<"key">>, ar_util:encode(Key)},
					{<<"nonce">>, ar_util:encode(Nonce)}
				]
			}
		)
	).

%% @doc Request to be added as a peer to a remote host.
add_peer(Peer) ->
	ar_httpc:request(
		<<"POST">>,
		Peer,
		"/peers",
		ar_serialize:jsonify(
			{
				[
					{network, list_to_binary(?NETWORK_NAME)}
				]
			}
		)
	).

%% @doc Get a peer's current, top block.
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
			[]
		),
	binary_to_integer(Body).

%% @doc Retreive a block by height or hash from a remote peer.
get_block(Peer, ID, BHL) ->
	get_full_block(Peer, ID, BHL).

%% @doc Get an encrypted block from a remote peer.
%% Used when the next block is the recall block.
get_encrypted_block(Peer, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_encrypted_block_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/encrypted",
			[]
		)
	).

%% @doc Get a specified subfield from the block with the given hash
get_block_subfield(Peer, Hash, Subfield) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host,[] Host}, {hash, Hash}]),
	handle_block_field_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/" ++ Subfield,
			[]
		)
	);
%% @doc Get a specified subfield from the block with the given height
get_block_subfield(Peer, Height, Subfield) when is_integer(Height) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_block_field_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/height/" ++integer_to_list(Height) ++ "/" ++ Subfield,
			[]
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
			[]
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
			[]
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:json_struct_to_wallet_list(Body);
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found
	end.

%% @doc Get a block hash list (by its hash) from the external peer.
get_hash_list(Peer) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/hash_list",
			[]
		),
	ar_serialize:json_struct_to_hash_list(ar_serialize:dejsonify(Body)).

get_hash_list(Peer, Hash) ->
	Response =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/hash_list",
			[]
		),
	case Response of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			ar_serialize:dejsonify(ar_serialize:json_struct_to_hash_list(Body));
		{ok, {{<<"404">>, _}, _, _, _, _}} -> not_found
	end.

%% @doc Retreive a full block (full transactions included in body)
%% by hash from a remote peer in an encrypted form
get_encrypted_full_block(Peer, Hash) when is_binary(Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_block, {host, Host}, {hash, Hash}]),
	handle_encrypted_full_block_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/block/hash/" ++ binary_to_list(ar_util:encode(Hash)) ++ "/all/encrypted",
			[]
		)
	).

%% @doc Retreive a tx by hash from a remote peer
get_tx(Peer, Hash) ->
	%ar:report_console([{req_getting_block_by_hash, Hash}]),
	%ar:d([getting_new_block, {host, Host}, {hash, Hash}]),
	handle_tx_response(
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/tx/" ++ binary_to_list(ar_util:encode(Hash)),
			[]
		)
	).

%% @doc Retreive only the data associated with a transaction.
get_tx_data(Peer, Hash) ->
	{ok, {{<<"200">>, _}, _, Body, _, _}} =
		ar_httpc:request(
			<<"GET">>,
			Peer,
			"/" ++ binary_to_list(ar_util:encode(Hash)),
			[]
		),
	Body.

%% @doc Retreive the current universal time as claimed by a foreign node.
get_time(Peer) ->
	case ar_httpc:request(<<"GET">>, Peer, "/time", []) of
		{ok, {{<<"200">>, _}, _, Body, _, _}} ->
			binary_to_integer(Body);
		_ -> unknown
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
					[]
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
			[]
		)
	of
		{ok, {{<<"200">>, _}, _, Body, _, _}} -> process_get_info(Body);
		_ -> info_unavailable
	end.

%% @doc Return a list of parsed peer IPs for a remote server.
get_peers(Peer) ->
	try
		begin
			{ok, {{<<"200">>, _}, _, Body, _, _}} =
				ar_httpc:request(
				<<"GET">>,
				Peer,
				"/peers",
				[]
				),
			PeerArray = ar_serialize:dejsonify(Body),
			lists:map(fun ar_util:parse_peer/1, PeerArray)
		end
	catch _:_ -> []
	end.

%% @doc Produce a key value list based on a /info response.
process_get_info(Body) ->
	{Struct} = ar_serialize:dejsonify(Body),
	{_, NetworkName} = lists:keyfind(<<"network">>, 1, Struct),
	{_, ClientVersion} = lists:keyfind(<<"version">>, 1, Struct),
	ReleaseNumber =
		case lists:keyfind(<<"release">>, 1, Struct) of
			false -> 0;
			R -> R
		end,
	{_, Height} = lists:keyfind(<<"height">>, 1, Struct),
	{_, Blocks} = lists:keyfind(<<"blocks">>, 1, Struct),
	{_, Peers} = lists:keyfind(<<"peers">>, 1, Struct),
	[
		{name, NetworkName},
		{version, ClientVersion},
		{height, Height},
		{blocks, Blocks},
		{peers, Peers},
		{release, ReleaseNumber}
	].

%% @doc Process the response of a /block call.
handle_block_response(Peer, {ok, {{<<"200">>, _}, _, Body, _, _}}, BHL) ->
	B = ar_serialize:json_struct_to_block(Body),
	case ?IS_BLOCK(B) of
		true ->
			WalletList =
				case is_binary(WL = B#block.wallet_list) of
					true ->
						case ar_storage:read_wallet_list(WL) of
							{error, _} ->
								get_wallet_list(Peer, B#block.indep_hash);
							ReadWL -> ReadWL
						end;
					false -> WL
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

%% @doc Process the response of a /block/.../all call.
handle_full_block_response({ok, {{<<"200">>, _}, _, Body, _, _}}) ->
	ar_serialize:json_struct_to_full_block(Body);
handle_full_block_response({error, _}) -> unavailable;
handle_full_block_response({ok, {{<<"404">>, _}, _, _, _, _}}) -> not_found;
handle_full_block_response({ok, {{<<"500">>, _}, _, _, _, _}}) -> unavailable.

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
