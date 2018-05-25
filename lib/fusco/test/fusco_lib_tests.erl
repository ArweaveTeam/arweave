%%%-----------------------------------------------------------------------------
%%% @copyright (C) 1999-2013, Erlang Solutions Ltd
%%% @author Oscar Hellström <oscar@hellstrom.st>
%%% @author Diana Parra Corbacho <diana.corbacho@erlang-solutions.com>
%%% @doc
%%% @end
%%%-----------------------------------------------------------------------------
-module(fusco_lib_tests).
-copyright("2013, Erlang Solutions Ltd.").

-include("../include/fusco_types.hrl").
-include("../include/fusco.hrl").
-include_lib("eunit/include/eunit.hrl").

parse_url_test_() ->
    [
        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 80,
                         path = "/",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://host")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 80,
                         path = "/",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://host/")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 443,
                         path = "/",
                         is_ssl = true,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("https://host")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 443,
                         path = "/",
                         is_ssl = true,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("https://host/")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 180,
                         path = "/",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://host:180")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 180,
                         path = "/",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://host:180/")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 180,
                         path = "/foo",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://host:180/foo")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 180,
                         path = "/foo/bar",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://host:180/foo/bar")),

        ?_assertEqual(
            #fusco_url{
                 host = "host",
                 port = 180,
                 path = "/foo/bar",
                 is_ssl = false,
                 user = "joe",
                 password = "erlang"
                },
            fusco_lib:parse_url("http://joe:erlang@host:180/foo/bar")),


        ?_assertEqual(
            #fusco_url{
                 host = "host",
                 port = 180,
                 path = "/foo/bar",
                 is_ssl = false,
                 user = "joe",
                 password = "erl@ng"
                },
            fusco_lib:parse_url("http://joe:erl%40ng@host:180/foo/bar")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 180,
                         path = "/foo/bar",
                         is_ssl = false,
                         user = "joe",
                         password = ""
                        },
                      fusco_lib:parse_url("http://joe@host:180/foo/bar")),

        ?_assertEqual(#fusco_url{
                         host = "host",
                         port = 180,
                         path = "/foo/bar",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://@host:180/foo/bar")),

        ?_assertEqual(
            #fusco_url{
                 host = "host",
                 port = 180,
                 path = "/foo/bar",
                 is_ssl = false,
                 user = "joe:arm",
                 password = "erlang"
                },
            fusco_lib:parse_url("http://joe%3Aarm:erlang@host:180/foo/bar")),

        ?_assertEqual(
            #fusco_url{
                 host = "host",
                 port = 180,
                 path = "/foo/bar",
                 is_ssl = false,
                 user = "joe:arm",
                 password = "erlang/otp"
                },
            fusco_lib:parse_url(
                "http://joe%3aarm:erlang%2Fotp@host:180/foo/bar")),

        ?_assertEqual(#fusco_url{
                         host = "::1",
                         port = 80,
                         path = "/foo/bar",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://[::1]/foo/bar")),

        ?_assertEqual(#fusco_url{
                         host = "::1",
                         port = 180,
                         path = "/foo/bar",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://[::1]:180/foo/bar")),

        ?_assertEqual(
            #fusco_url{
                 host = "::1",
                 port = 180,
                 path = "/foo/bar",
                 is_ssl = false,
                 user = "joe",
                 password = "erlang"
                },
            fusco_lib:parse_url("http://joe:erlang@[::1]:180/foo/bar")),

        ?_assertEqual(
            #fusco_url{
                 host = "1080:0:0:0:8:800:200c:417a",
                 port = 180,
                 path = "/foo/bar",
                 is_ssl = false,
                 user = "joe",
                 password = "erlang"
                },
            fusco_lib:parse_url(
                "http://joe:erlang@[1080:0:0:0:8:800:200C:417A]:180/foo/bar")),

        ?_assertEqual(#fusco_url{
                         host = "www.example.com",
                         port = 80,
                         path = "/?a=b",
                         is_ssl = false,
                         user = "",
                         password = ""
                        },
                      fusco_lib:parse_url("http://www.example.com?a=b"))
    ].
