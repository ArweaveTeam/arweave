-module(arweave_metrics_render_SUITE).
-test_category([fast]).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [text_format, encoding_negotiation].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_metrics),
    ok = ar_metrics:register(),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = ar_metrics:cleanup(),
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

%%====================================================================
%% Test cases
%%====================================================================

text_format(_Config) ->
    ?assert(arweave_metrics_render:is_text_format(<<"text/plain">>)),
    ?assert(arweave_metrics_render:is_text_format(<<"*/*">>)),
    ?assertNot(
        arweave_metrics_render:is_text_format(
            <<
                "application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;"
                "encoding=delimited"
            >>
        )
    ).

encoding_negotiation(_Config) ->
    ?assertEqual(
        <<"gzip">>, arweave_metrics_render:negotiate_encoding(<<"gzip">>)
    ),
    ?assertEqual(
        <<"identity">>, arweave_metrics_render:negotiate_encoding(undefined)
    ),
    ?assertEqual(
        <<"deflate">>, arweave_metrics_render:negotiate_encoding(<<"deflate">>)
    ),
    ?assertEqual(
        <<"gzip">>,
        arweave_metrics_render:negotiate_encoding(<<"deflate, gzip">>)
    ).
