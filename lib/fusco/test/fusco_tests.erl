%%% ----------------------------------------------------------------------------
%%% Copyright (c) 2009, Erlang Training and Consulting Ltd.
%%% All rights reserved.
%%%
%%% Redistribution and use in source and binary forms, with or without
%%% modification, are permitted provided that the following conditions are met:
%%%    * Redistributions of source code must retain the above copyright
%%%      notice, this list of conditions and the following disclaimer.
%%%    * Redistributions in binary form must reproduce the above copyright
%%%      notice, this list of conditions and the following disclaimer in the
%%%      documentation and/or other materials provided with the distribution.
%%%    * Neither the name of Erlang Training and Consulting Ltd. nor the
%%%      names of its contributors may be used to endorse or promote products
%%%      derived from this software without specific prior written permission.
%%%
%%% THIS SOFTWARE IS PROVIDED BY Erlang Training and Consulting Ltd. ''AS IS''
%%% AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
%%% IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
%%% ARE DISCLAIMED. IN NO EVENT SHALL Erlang Training and Consulting Ltd. BE
%%% LIABLE SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
%%% BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
%%% WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR
%%% OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
%%% ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
%%% ----------------------------------------------------------------------------

%%% @author Oscar Hellström <oscar@hellstrom.st>
-module(fusco_tests).

-export([test_no/2]).

-include_lib("eunit/include/eunit.hrl").

test_no(N, Tests) ->
    setelement(2, Tests,
        setelement(4, element(2, Tests),
            lists:nth(N, element(4, element(2, Tests))))).

%%% Eunit setup stuff

start_app() ->
    [application:start(App) || App <- apps()].

apps() ->
    [crypto, asn1, public_key, ssl].

stop_app(_) ->
    [application:stop(App) || App <- lists:reverse(apps())].

tcp_test_() ->
    {inorder,
        {setup, fun start_app/0, fun stop_app/1, [
                ?_test(get_with_connect_options()),
                ?_test(no_content_length()),
                ?_test(no_content_length_1_0()),
                ?_test(pre_1_1_server_connection()),
                ?_test(pre_1_1_server_keep_alive()),
                ?_test(post_100_continue()),
                ?_test(request_timeout()),
                ?_test(trailing_space_header()),
                ?_test(closed_after_timeout())
            ]}
    }.

options_test() ->
    invalid_options().

get_with_connect_options() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp, [fun webserver_utils:empty_body/5]),
    URL = url(Port),
    Options = [{connect_options, [{ip, {127, 0, 0, 1}}, {port, 0}]}],
    {ok, Client} = fusco:start(URL, Options),
    {ok, Response} =
        fusco:request(Client, <<"/empty">>, "GET", [], [], 1, 1000),
    ?assertEqual({<<"200">>, <<"OK">>}, status(Response)),
    ?assertEqual(<<>>, body(Response)).

no_content_length() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp, [fun webserver_utils:no_content_length/5]),
    URL = url(Port),
    {ok, Client} = fusco:start(URL, []),
    {ok, Response} = fusco:request(Client, <<"/no_cl">>, "GET", [], [], 1000),
    ?assertEqual({<<"200">>, <<"OK">>}, status(Response)),
    ?assertEqual(list_to_binary(webserver_utils:default_string()),
                 body(Response)).

no_content_length_1_0() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp, [fun webserver_utils:no_content_length_1_0/5]),
    URL = url(Port),
    {ok, Client} = fusco:start(URL, []),
    {ok, Response} = fusco:request(Client, <<"/no_cl">>, "GET", [], [], 1000),
    ?assertEqual({<<"200">>, <<"OK">>}, status(Response)),
    ?assertEqual(list_to_binary(webserver_utils:default_string()),
                 body(Response)).

%% Check the header value is trimming spaces on header values
%% which can cause crash in fusco_client:body_type when Content-Length
%% is converted from list to integer
trailing_space_header() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp, [fun webserver_utils:trailing_space_header/5]),
    URL = url(Port),
    {ok, Client} = fusco:start(URL, []),
    {ok, Response} = fusco:request(Client, <<"/no_cl">>, "GET", [], [], 1000),
    Headers = headers(Response),
    ContentLength = fusco_lib:header_value(<<"content-length">>, Headers),
    ?assertEqual(<<"14">>, ContentLength).

pre_1_1_server_connection() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp, [fun webserver_utils:pre_1_1_server/5]),
    URL = url(Port),
    Body = pid_to_list(self()),
    {ok, Client} = fusco:start(URL, []),
    {ok, _} = fusco:request(Client, <<"/close">>, "PUT", [], Body, 1000),
    % Wait for the server to see that socket has been closed.
    % The socket should be closed by us since the server responded with a
    % 1.0 version, and not the Connection: keep-alive header.
    receive closed -> ok end.

pre_1_1_server_keep_alive() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp,
                        [fun webserver_utils:pre_1_1_server_keep_alive/5,
                         fun webserver_utils:pre_1_1_server/5]),
    URL = url(Port),
    Body = pid_to_list(self()),
    {ok, Client} = fusco:start(URL, []),
    {ok, Response1} = fusco:request(Client, <<"/close">>, "GET", [], [], 1000),
    {ok, Response2} =
        fusco:request(Client, <<"/close">>, "PUT", [], Body, 1000),
    ?assertEqual({<<"200">>, <<"OK">>}, status(Response1)),
    ?assertEqual({<<"200">>, <<"OK">>}, status(Response2)),
    ?assertEqual(list_to_binary(webserver_utils:default_string()),
                 body(Response1)),
    ?assertEqual(list_to_binary(webserver_utils:default_string()),
                 body(Response2)),
    % Wait for the server to see that socket has been closed.
    % The socket should be closed by us since the server responded with a
    % 1.0 version, and not the Connection: keep-alive header.
    receive closed -> ok end.

post_100_continue() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp,
                        [fun webserver_utils:copy_body_100_continue/5]),
    URL = url(Port),
    {X, Y, Z} = erlang:timestamp(),
    Body = [
        "This is a rather simple post :)",
        integer_to_list(X),
        integer_to_list(Y),
        integer_to_list(Z)
    ],
    {ok, Client} = fusco:start(URL, []),
    {ok, Response} = fusco:request(Client, <<"/post">>, "POST", [], Body, 1000),
    {StatusCode, ReasonPhrase} = status(Response),
    ?assertEqual(<<"200">>, StatusCode),
    ?assertEqual(<<"OK">>, ReasonPhrase),
    ?assertEqual(iolist_to_binary(Body), body(Response)).

request_timeout() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp, [fun webserver_utils:very_slow_response/5]),
    URL = url(Port),
    {ok, Client} = fusco:start(URL, []),
    ?assertEqual({error, timeout},
                 fusco:request(Client, <<"/slow">>, "GET", [], [], 50)).

invalid_options() ->
    URL = url(5050),
    ?assertError({bad_option, {connect_timeout, "bad_option"}},
        fusco:start(URL, [{connect_timeout, "bad_option"},
                          {connect_timeout, "bar"}])),
    ?assertError({bad_option, {connect_timeout, "bar"}},
        fusco:start(URL, [{connect_timeout, "bar"},
                          {connect_timeout, "bad_option"}])).

closed_after_timeout() ->
    {ok, _, _, Port} =
        webserver:start(gen_tcp,
                        [fun webserver_utils:no_response/5, stay_open]),
    URL = url(Port),
    {ok, Client} = fusco:start(URL, []),
    _ = fusco:request(Client, <<"/slow">>, "GET", [], [], 50),
    fusco:disconnect(Client),
    wait_for_exit(10, Client),
    ?assertEqual(false,erlang:is_process_alive(Client)).

wait_for_exit(0, _) ->
    ok;
wait_for_exit(N, Proc) ->
    timer:sleep(50),
    case is_process_alive(Proc) of
        false ->
            ok;
        true ->
            wait_for_exit(N - 1, Proc)
    end.

url(Port) ->
    url(inet, Port).

url(inet, Port) ->
    "http://localhost:" ++ integer_to_list(Port).
% url(inet6, Port) ->
%     "http://[::1]:" ++ integer_to_list(Port).

status({Status, _, _, _, _}) ->
    Status.

body({_, _, Body, _, _}) ->
    Body.

headers({_, Headers, _, _, _}) ->
    Headers.
