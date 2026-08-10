-module(ar_webhook_tests).


-export([init/2]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("arweave/include/ar.hrl").
-include_lib("arweave_config/include/arweave_config.hrl").

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

%% Mock `ar_tx_blacklist:refresh_interval_ms/0' so blacklist refreshes
%% on a test-friendly cadence — the second-chunk blacklisting step
%% relies on a refresh firing within the assertion's 60s window, and
%% the production interval is 10 minutes.
webhooks_test_() ->
    ar_test_node:test_with_all_nodes_mocked(
        [{ar_tx_blacklist, refresh_interval_ms, fun() -> 2000 end}],
        fun test_webhooks/0,
        ?TEST_NODE_TIMEOUT
    ).

test_webhooks() ->
    {_, Pub} = Wallet = ar_wallet:new(),
    [B0] = ar_weave:init([{ar_wallet:to_address(Pub), ?AR(10000), <<>>}]),
    arweave_config:with_test_config(fun() ->
        test_webhooks_body(Wallet, B0)
    end).

test_webhooks_body(Wallet, B0) ->
    Port = ar_test_node:get_unused_port(),
        PortBinary = integer_to_binary(Port),
        TXBlacklistFilename = random_tx_blacklist_filename(),
        Addr = ar_test_node:generate_address(main),
        Webhooks = [
            #{
                url => <<"http://127.0.0.1:", PortBinary/binary, "/tx">>,
                events => [<<"transaction">>]
            },
            #{
                url => <<"http://127.0.0.1:", PortBinary/binary, "/block">>,
                events => [<<"block">>]
            },
            #{
                url => <<"http://127.0.0.1:", PortBinary/binary, "/txdata">>,
                events => [<<"transaction_data">>]
            },
            #{
                url => <<"http://127.0.0.1:", PortBinary/binary, "/solution">>,
                events => [<<"solution">>]
            }
        ],
        Overrides = #{
            [transactions, blocklist, files] =>
                [list_to_binary(TXBlacklistFilename)]
        },
        ar_test_node:start(#{ b0 => B0, addr => Addr, config => Overrides,
                [webhooks] => [Webhook#{enabled => true} || Webhook <- Webhooks],
                    %% Use SPoRA; replica 2.9 modules do not support updates.
                    [storage_modules] => [
                        {0, 10 * ?MiB, {spora_2_6, Addr}}
                    ] }),
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
                    ?assertMatch({ok, _}, ar_test_await:node_height(main, Height)),
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
                await_webhook_event(webhook_block_event, {block, Height},
                    fun(B) ->
                        {H, _, _} = ar_node:get_block_index_entry(Height),
                        B2 = ar_test_await:block_stored(H),
                        Struct = ar_serialize:block_to_json_struct(B2),
                        Expected =
                            maps:remove(
                                <<"wallet_list">>,
                                jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
                            ),
                        ?assertEqual(Expected, B),
                        true
                    end,
                    10000
                ),
                await_webhook_event(webhook_tx_event, {tx, arweave_util:encode(TX#tx.id)},
                    fun(TX2) ->
                        Struct = ar_serialize:tx_to_json_struct(TX),
                        Expected =
                            maps:remove(
                                <<"data">>,
                                jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
                            ),
                        ?assertEqual(Expected, TX2),
                        true
                    end,
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
        await_webhook_event(webhook_unconfirmed_tx_event,
            {tx, arweave_util:encode(UnconfirmedTX#tx.id)},
            fun(TX) ->
                Struct = ar_serialize:tx_to_json_struct(UnconfirmedTX),
                Expected =
                    maps:remove(
                        <<"data">>,
                        jiffy:decode(ar_serialize:jsonify(Struct), [return_maps])
                    ),
                ?assertEqual(Expected, TX),
                true
            end,
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
        cowboy:stop_listener(ar_webhook_test_listener).

%% @doc Poll the test's ETS receiver table until the entry at Key is present
%% and its stored JSON satisfies MatchFun.
await_webhook_event(Name, Key, MatchFun, Timeout) ->
    ok = ar_test_await:until(Name,
        fun() ->
            case ets:lookup(?MODULE, Key) of
                [{_, JSON}] ->
                    MatchFun(JSON);
                _ ->
                    false
            end
        end,
        Timeout
    ).

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
        chunk => arweave_util:encode(maps:get(chunk, Proof)),
        data_path => arweave_util:encode(maps:get(data_path, Proof)),
        data_root => arweave_util:encode(maps:get(data_root, Proof)),
        data_size => integer_to_binary(maps:get(data_size, Proof)),
        offset => integer_to_binary(maps:get(offset, Proof))
    }).

assert_transaction_data_synced(TXID) ->
    EncodedTXID = arweave_util:encode(TXID),
    await_webhook_event(webhook_tx_data_synced,
        {tx_data_payload, EncodedTXID},
        fun(JSON) ->
            maps:get(<<"event">>, JSON) == <<"transaction_data_synced">>
        end,
        30000
    ).

upload_chunks([]) ->
    ok;
upload_chunks([Proof | Proofs]) ->
    {ok, {{<<"200">>, _}, _, _, _, _}} = ar_test_node:post_chunk(main, Proof),
    upload_chunks(Proofs).

random_tx_blacklist_filename() ->
    DataDir = arweave_config:get([data_dir]),
    filename:join(DataDir,
        "ar-webhook-tests-transaction-blacklist-"
        ++
        binary_to_list(arweave_util:encode(crypto:strong_rand_bytes(32)))).

append_txid_to_file(TXID, Filename) ->
    {ok, F} = file:open(Filename, [append]),
    ok = file:write(F, io_lib:format("~s~n", [arweave_util:encode(TXID)])),
    file:close(F).

assert_transaction_data_removed(TXID) ->
    EncodedTXID = arweave_util:encode(TXID),
    await_webhook_event(webhook_tx_data_removed,
        {tx_data_payload, EncodedTXID},
        fun(JSON) ->
            maps:get(<<"event">>, JSON) == <<"transaction_data_removed">>
        end,
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
