%%%=============================================================================
%%% @copyright (C) 1999-2013, Erlang Solutions Ltd
%%% @author Diana Corbacho <diana.corbacho@erlang-solutions.com>
%%% @doc
%%% @end
%%%=============================================================================
-module(fusco_protocol_tests).
-copyright("2013, Erlang Solutions Ltd.").

-include_lib("eunit/include/eunit.hrl").
-include("fusco.hrl").

-export([test_decode_header/0]).

fusco_protocol_test_() ->
    [{"HTTP version", ?_test(http_version())},
     {"Cookies", ?_test(cookies())},
     {"Decode header", ?_test(decode_header())}].

http_version() ->
    L = {_, _, Socket} = test_utils:start_listener(cookie_message()),
    _ = test_utils:send_message(Socket),
    ?assertMatch(#response{version = {1,1},
                           status_code = <<"200">>,
                           reason = <<"OK">>,
                           body = <<"Great success!">>},
                 fusco_protocol:recv(Socket, false)),
    test_utils:stop_listener(L).

cookies() ->
    L = {_, _, Socket} = test_utils:start_listener(cookie_message()),
    _ = test_utils:send_message(Socket),
    Recv = fusco_protocol:recv(Socket, false),
    test_utils:stop_listener(L),
    ?assertMatch(#response{version = {1,1},
                           status_code = <<"200">>,
                           reason = <<"OK">>,
                           headers = [{<<"set-cookie">>,
                                       <<"name2=value2; Expires=Wed, ",
                                         "09 Jun 2021 10:18:14 GMT">>},
                                       {<<"set-cookie">>,<<"name=value">>} | _],
                           body = <<"Great success!">>},
         Recv).

decode_header() ->
    ?assertMatch(#response{
        headers = [
            {<<"set-cookie">>,
             <<"name2=value2; Expires=Wed, 09 Jun 2021 10:18:14 GMT">>},
            {<<"set-cookie">>,<<"name=value">>},
            {<<"content-length">>, <<"14">>},
            {<<"content-type">>,<<"text/plain">>}],
        body = <<"Great success!">>},
        test_decode_header()).

test_decode_header() ->
    fusco_protocol:decode_header(header(), <<>>, #response{}).

header() ->
    <<"Content-type: text/plain\r\nContent-length: 14\r\nSet-Cookie: ",
      "name=value\r\nSet-Cookie: name2=value2; Expires=Wed, 09 Jun 2021 ",
      "10:18:14 GMT\r\n\r\nGreat success!">>.

cookie_message() ->
    [
     "HTTP/1.1 200 OK\r\n"
     "Content-type: text/plain\r\nContent-length: 14\r\n"
     "Set-Cookie: name=value\r\n"
     "Set-Cookie: name2=value2; Expires=Wed, 09 Jun 2021 10:18:14 GMT\r\n"
     "\r\n"
     "Great success!"
    ].
