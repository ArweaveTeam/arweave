%%% @doc Tests for the HTTP client interface against a stub HTTP
%%% server (`ar_test_http_server') rather than a full node.
%%% 
%%% Since we aren't querying a node running our own code, we
%%% can mock any 
-module(ar_http_iface_client_tests).
-test_category([fast]).

-include_lib("eunit/include/eunit.hrl").

get_peers_test_() ->
    {timeout, 30,
     {foreach, fun setup/0, fun cleanup/1,
      [
       fun(Config) -> fun() -> test_get_peers(Config) end end
      ]}}.

setup() ->
    {ok, Peer, Ref, Table} = ar_test_http_server:start(#{}),
    [{peer, Peer}, {ref, Ref}, {table, Table}].

cleanup(Config) ->
    Ref = proplists:get_value(ref, Config),
    Table = proplists:get_value(table, Config),
    ar_test_http_server:stop(Ref, Table),
    ok.

%% Test cases
test_get_peers(Config) ->
    %% Valid
    Peer = proplists:get_value(peer, Config),
    Table = proplists:get_value(table, Config),
    ok = mock_get_peers_response(
             Table,
             ar_serialize:jsonify([<<"127.0.0.1:1984">>, <<"10.0.0.1:1985">>])),
    %% A bit of a self check here: webservice returns 200.
    ?assertMatch({ok, {{<<"200">>, _}, _, _Body, _, _}},
                 ar_http:req(#{method => get,
                               peer => Peer,
                               path => "/peers",
                               connect_timeout => 500,
                               timeout => 2 * 1000
                              })),
    %% IPs are parsed
    ?assertEqual([{127, 0, 0, 1, 1984}, {10, 0, 0, 1, 1985}],
                 ar_http_iface_client:get_peers(Peer)),

    %% 128 items accepted
    IPList = lists:map(fun(_) ->
                           <<"127.0.0.1:1984">>
                       end, lists:seq(1,128)),
    Body128 = ar_serialize:jsonify(IPList),
    ok = mock_get_peers_response(Table, Body128),
    Result128 = lists:map(fun(_) ->
                           {127,0,0,1,1984}
                       end, lists:seq(1,128)),
    ?assertEqual(Result128, ar_http_iface_client:get_peers(Peer)),

    %% One is too long
    ok = mock_get_peers_response(
             Table,
             ar_serialize:jsonify([<<"127.0.0.1:1984">>, <<"12312312310.1231230.1231230.1231231:11231231985">>])),
    ?assertEqual([{127,0,0,1,1984}], ar_http_iface_client:get_peers(Peer)),
    
    %% One can't be parsed into IP+Port tuple
    ok = mock_get_peers_response(
             Table,
             ar_serialize:jsonify([<<"127.0.0.1:1984">>, <<"randomstuff">>])),
    ?assertEqual([{127,0,0,1,1984}], ar_http_iface_client:get_peers(Peer)),

    %% One can't be parsed into IP+Port tuple
    ok = mock_get_peers_response(
             Table,
             ar_serialize:jsonify([<<"127.0.0.1:1984">>, <<"127.0.0.1asd:1984">>])),
    ?assertEqual([{127,0,0,1,1984}], ar_http_iface_client:get_peers(Peer)),

    %% One weird one
    ok = mock_get_peers_response(
             Table,
             ar_serialize:jsonify([<<"127.0.0.1:1984">>, <<"127.0.0.1111:1984">>])),
    ?assertEqual([{127,0,0,1,1984}], ar_http_iface_client:get_peers(Peer)),

    %% List too long
    IPListTooLong = lists:map(fun(_) ->
                           <<"127.0.0.1:1984">>
                       end, lists:seq(1,129)),
    BodyTooLong = ar_serialize:jsonify(IPListTooLong),
    ok = mock_get_peers_response(Table, BodyTooLong),
    %% It will cut the list to 128. - the same as the valid one.
    ?assertEqual(Result128, ar_http_iface_client:get_peers(Peer)),

    ok.

%% Private
mock_get_peers_response(Table, Body) ->
    ar_test_http_server:set_route(
        Table,
        {<<"GET">>, <<"/peers">>},
        {200, #{<<"content-type">> => <<"application/json">>}, Body}).
