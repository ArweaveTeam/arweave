-module(arweave_metrics_cache_SUITE).
-compile([export_all, nowarn_export_all]).
-include_lib("eunit/include/eunit.hrl").

suite() -> [{timetrap, {seconds, 60}}].

all() -> [prerendered_body, uncached_registry].

init_per_suite(Config) ->
    {ok, Apps} = application:ensure_all_started(arweave_metrics),
    ok = ar_metrics:register(),
    [{started_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = arweave_metrics:internal_cleanup(),
    ok = ar_metrics:cleanup(),
    lists:foreach(
        fun application:stop/1,
        lists:reverse(proplists:get_value(started_apps, Config))
    ).

%%====================================================================
%% Test cases
%%====================================================================

prerendered_body(_Config) ->
    ok = arweave_metrics_cache:render_now(),
    Cache = arweave_metrics_cache:lookup(default),
    ?assertMatch(#{content_type := _, identity := _, gzip := _}, Cache),
    #{content_type := ContentType, identity := Identity, gzip := Gzip} = Cache,
    ?assertMatch(
        {0, _},
        binary:match(
            iolist_to_binary(ContentType),
            <<"text/plain">>
        )
    ),
    %% The gzipped body round-trips to the plain body.
    ?assertEqual(Identity, zlib:gunzip(Gzip)),
    %% The plain body is a real exposition carrying arweave series.
    ?assertNotEqual(nomatch, binary:match(Identity, <<"# TYPE">>)).

uncached_registry(_Config) ->
    ?assertEqual(
        not_cached, arweave_metrics_cache:lookup(nonexistent_registry)
    ).
