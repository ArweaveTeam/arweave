-module(ar_webhook_tests).

-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave/include/ar_config.hrl").

-import(ar_test_node, [
		wait_until_height/2, read_block_when_stored/1]).

init(Req, State) ->
	SplitPath = ar_http_iface_server:split_path(cowboy_req:path(Req)),
	handle(SplitPath, Req, State).

handle([<<"tx">>], Req, State) ->
	{ok, Reply, _} = cowboy_req:read_body(Req),
	JSON = jiffy:decode(Reply, [return_maps]),
	TX = maps:get(<<"transaction">>, JSON),
	ets:insert(?MODULE, {{tx, maps:get(<<"id">>, TX)}, TX}),
	{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};

handle([<<"block">>], Req, State) ->
	{ok, Reply, _} = cowboy_req:read_body(Req),
	JSON = jiffy:decode(Reply, [return_maps]),
	B = maps:get(<<"block">>, JSON),
	ets:insert(?MODULE, {{block, maps:get(<<"height">>, B)}, B}),
	{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};

handle([<<"txdata">>], Req, State) ->
	{ok, Reply, _} = cowboy_req:read_body(Req),
	JSON = jiffy:decode(Reply, [return_maps]),
	ets:insert(?MODULE, {{tx_data_payload, maps:get(<<"txid">>, JSON)}, JSON}),
	{ok, cowboy_req:reply(200, #{}, <<>>, Req), State};

handle([<<"solution">>], Req, State) ->
	{ok, Reply, _} = cowboy_req:read_body(Req),
	JSON = jiffy:decode(Reply, [return_maps]),
	case maps:get(<<"event">>, JSON, not_found) of
		<<"solution_accepted">> ->
			ets:update_counter(?MODULE, accepted_solutions, {2, 1}, {accepted_solutions, 0});
		_ ->
			ok
	end,
	{ok, cowboy_req:reply(200, #{}, <<>>, Req), State}.

webhooks_test_() ->
	{timeout, 120, fun test_webhooks/0}.

test_webhooks() ->
	{_, Pub} = Wallet = ar_wallet:new(),
	[B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
	{ok, Config} = arweave_config:get_env(),
	try
		Port = ar_test_node:get_unused_port(),
		PortBinary = integer_to_binary(Port),
		TXBlacklistFilename = random_tx_blacklist_filename(),
		Addr = ar_wallet:to_address(ar_wallet:new_keyfile()),
		Config2 = Config#config{
			webhooks = [
				#config_webhook{
					url = <<"http://127.0.0.1:", PortBinary/binary, "/tx">>,
					events = [transaction]
				},
				#config_webhook{
					url = <<"http://127.0.0.1:", PortBinary/binary, "/block">>,
					events = [block]
				},
				#config_webhook{
					url = <<"http://127.0.0.1:", PortBinary/binary, "/txdata">>,
					events = [transaction_data]
				},
				#config_webhook{
					url = <<"http://127.0.0.1:", PortBinary/binary, "/solution">>,
					events = [solution]
				}
			],
			transaction_blacklist_files = [TXBlacklistFilename]
		},
		ar_test_node:start(#{ b0 => B0, addr => Addr, config => Config2,
				%% Replica 2.9 modules do not support updates.
				storage_modules =>[{10 * 1024 * 1024, 0, {composite, Addr, 1}}] }),
		%% Setup a server that would be listening for the webhooks and registering
		%% them in the ETS table.
		ets:new(?MODULE, [named_table, set, public]),
		Routes = [{"/[...]", ar_webhook_tests, []}],
		cowboy:start_clear(
			ar_webhook_test_listener,
			[{port, Port}],
			#{ env => #{ dispatch => cowboy_router:compile([{'_', Routes}]) } }
		),
		{V2TX, Proofs} = create_v2_tx(Wallet),
		TXs =
			lists:map(
				fun(Height) ->
					SignedTX =
						case Height rem 2 == 1 of
							true ->
								Data = crypto:strong_rand_bytes(262144 * 2 + 10),
								ar_test_node:sign_v1_tx(main, Wallet, #{ data => Data });
							false ->
								case Height == 2 of
									true ->
										V2TX;
									false ->
										ar_test_node:sign_tx(main, Wallet, #{})
								end
						end,
					ar_test_node:assert_post_tx_to_peer(main, SignedTX),
					ar_test_node:mine(),
					wait_until_height(main, Height),
					[{_, AcceptedSolutionCount}] = ets:lookup(?MODULE, accepted_solutions),
					?assert(AcceptedSolutionCount >= Height),
					SignedTX
				end,
				lists:seq(1, 10)
			),
		UnconfirmedTX = ar_test_node:sign_tx(main, Wallet, #{}),
		ar_test_node:assert_post_tx_to_peer(main, UnconfirmedTX),
		lists:foreach(
			fun(Height) ->
				TX = lists:nth(Height, TXs),
				true = ar_util:do_until(
					fun() ->
						case ets:lookup(?MODULE, {block, Height}) of
							[{_, B}] ->
								{H, _, _} = ar_node:get_block_index_entry(Height),
								B2 = read_block_when_stored(H),
								Struct = ar_serialize:block_to_json_struct(B2),
								Expected =
									maps:remove(
										<<"wallet_list">>,
										jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
									),
								?assertEqual(Expected, B),
								true;	
							_ ->
								false
						end
					end,
					200,
					10000
				),
				true = ar_util:do_until(
					fun() ->
						case ets:lookup(?MODULE, {tx, ar_util:encode(TX#tx.id)}) of
							[{_, TX2}] ->
								Struct = ar_serialize:tx_to_json_struct(TX),
								Expected =
									maps:remove(
										<<"data">>,
										jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
									),
								?assertEqual(Expected, TX2),
								true;
							_ ->
								false
						end
					end,
					200,
					10000
				),
				case Height < 8 andalso Height rem 2 == 1 of
					false ->
						%% Do not expect events about data from the latest blocks because it
						%% stays in the disk pool.
						ok;
					true ->
						assert_transaction_data_synced(TX#tx.id)
				end
			end,
			lists:seq(1, 10)
		),
		true = ar_util:do_until(
			fun() ->
				case ets:lookup(?MODULE, {tx, ar_util:encode(UnconfirmedTX#tx.id)}) of
					[{_, TX}] ->
						Struct = ar_serialize:tx_to_json_struct(UnconfirmedTX),
						Expected =
							maps:remove(
								<<"data">>,
								jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
							),
						?assertEqual(Expected, TX),
						true;
					_ ->
						false
				end
			end,
			200,
			2000
		),
		V2TXID = (V2TX)#tx.id,
		upload_chunks(Proofs),
		assert_transaction_data_synced(V2TXID),
		FirstTXID = (hd(TXs))#tx.id,
		append_txid_to_file(FirstTXID, TXBlacklistFilename),
		assert_transaction_data_removed(FirstTXID),
		SecondTXID = (lists:nth(3, TXs))#tx.id, % The second v1 transaction with data.
		append_second_chunk_to_file(SecondTXID, TXBlacklistFilename),
		assert_transaction_data_removed(SecondTXID),
		append_second_chunk_to_file(V2TXID, TXBlacklistFilename),
		assert_transaction_data_removed(V2TXID),
		empty_file(TXBlacklistFilename),
		%% Wait until the new blacklisting policy (=no blacklisting) takes effect.
		timer:sleep(3000),
		upload_chunks(Proofs),
		assert_transaction_data_synced(V2TXID),
		cowboy:stop_listener(ar_webhook_test_listener)
	after
		arweave_config:set_env(Config#config{ webhooks = [] })
	end.

create_v2_tx(Wallet) ->
	DataSize = 3 * ?DATA_CHUNK_SIZE + 11,
	Chunks = ar_tx:chunk_binary(?DATA_CHUNK_SIZE, crypto:strong_rand_bytes(DataSize)),
	SizeTaggedChunks = ar_tx:chunks_to_size_tagged_chunks(Chunks),
	SizedChunkIDs = ar_tx:sized_chunks_to_sized_chunk_ids(SizeTaggedChunks),
	{DataRoot, DataTree} = ar_merkle:generate_tree(SizedChunkIDs),
	TX = ar_test_node:sign_tx(main, Wallet,
			#{ format => 2, data_root => DataRoot, data_size => DataSize, reward => ?AR(1) }),
	Proofs = [encode_proof(#{ data_root => DataRoot, chunk => Chunk,
				data_path => ar_merkle:generate_path(DataRoot, Offset - 1, DataTree),
				offset => Offset - 1, data_size => DataSize })
			|| {Chunk, Offset} <- SizeTaggedChunks],
	{TX, Proofs}.

encode_proof(Proof) ->
	ar_serialize:jsonify(#{
		chunk => ar_util:encode(maps:get(chunk, Proof)),
		data_path => ar_util:encode(maps:get(data_path, Proof)),
		data_root => ar_util:encode(maps:get(data_root, Proof)),
		data_size => integer_to_binary(maps:get(data_size, Proof)),
		offset => integer_to_binary(maps:get(offset, Proof))
	}).

assert_transaction_data_synced(TXID) ->
	EncodedTXID = ar_util:encode(TXID),
	true = ar_util:do_until(
		fun() ->
			case ets:lookup(?MODULE, {tx_data_payload, EncodedTXID}) of
				[{_, JSON}] ->
					maps:get(<<"event">>, JSON) == <<"transaction_data_synced">>;
				_ ->
					false
			end
		end,
		1000,
		30000
	).

upload_chunks([]) ->
	ok;
upload_chunks([Proof | Proofs]) ->
	{ok, {{<<"200">>, _}, _, _, _, _}} = ar_test_node:post_chunk(main, Proof),
	upload_chunks(Proofs).

random_tx_blacklist_filename() ->
	{ok, Config} = arweave_config:get_env(),
	filename:join(Config#config.data_dir,
		"ar-webhook-tests-transaction-blacklist-"
		++
		binary_to_list(ar_util:encode(crypto:strong_rand_bytes(32)))).

append_txid_to_file(TXID, Filename) ->
	{ok, F} = file:open(Filename, [append]),
	ok = file:write(F, io_lib:format("~s~n", [ar_util:encode(TXID)])),
	file:close(F).

assert_transaction_data_removed(TXID) ->
	EncodedTXID = ar_util:encode(TXID),
	true = ar_util:do_until(
		fun() ->
			[{_, JSON}] = ets:lookup(?MODULE, {tx_data_payload, EncodedTXID}),
			maps:get(<<"event">>, JSON) == <<"transaction_data_removed">>
		end,
		100,
		60000
	).

append_second_chunk_to_file(TXID, Filename) ->
	{ok, {EndOffset, Size}} = ar_data_sync:get_tx_offset(TXID),
	SecondChunkStart = EndOffset - Size + ?DATA_CHUNK_SIZE,
	SecondChunkEnd = SecondChunkStart + ?DATA_CHUNK_SIZE,
	{ok, F} = file:open(Filename, [append]),
	ok = file:write(F, io_lib:format("~B,~B~n", [SecondChunkStart, SecondChunkEnd])),
	file:close(F).

empty_file(Filename) ->
	{ok, F} = file:open(Filename, [write]),
	ok = file:write(F, <<" ">>),
	file:close(F).
